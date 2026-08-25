"""Standalone startup profiler copied into a captured Python environment.

This module must remain standard-library-only: capture copies it as
``sitecustomize.py`` so a workload does not need Contrail installed.
"""

from __future__ import annotations

import atexit
import dis
import importlib
import json
import os
import socket
import struct
import sys
import threading
import time
from types import CodeType, FrameType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import _semantic_capture_bootstrap as _semantic_capture
else:
    _semantic_capture = importlib.import_module(
        f"{__package__}._semantic_capture_bootstrap"
        if __package__
        else "_semantic_capture_bootstrap"
    )

_DIRECTORY_ENV = "_CONTRAIL_DEEP_PROFILE_DIRECTORY"
_SOCKET_ENV = "_CONTRAIL_PROFILE_SNAPSHOT_SOCKET"
_CHECKPOINT_INTERVAL_SECONDS = 0.5
_CHECKPOINT_INTERVAL_NS = 500_000_000
_FIRST_CHECKPOINT_SECONDS = 0.05
_FIRST_CHECKPOINT_NS = 50_000_000
_MAX_FUNCTIONS = 2_000
_MAX_NATIVE_FUNCTIONS = 2_000
_MAX_EDGES = 10_000
_MAX_NATIVE_EDGES = 10_000
_MAX_STACK_DEPTH = 4_096
_MAX_TEXT_CHARACTERS = 1_024
_MAX_REPORT_BYTES = 16 * 1024 * 1024
_SNAPSHOT_PROTOCOL_MAGIC = b"CTRP0002"
_SNAPSHOT_HEADER = struct.Struct("!8sQQQQ")
_SNAPSHOT_KIND_REGISTRATION = 1
_SNAPSHOT_KIND_CHECKPOINT = 2
_SNAPSHOT_KIND_FINAL = 3
_SNAPSHOT_SOCKET_TIMEOUT_SECONDS = 0.1
_FINAL_SNAPSHOT_SOCKET_TIMEOUT_SECONDS = 1.0
_PUBLICATION_METRICS_VERSION = 1
_OBSERVER_INTEGRITY_VERSION = 1
_MAX_PUBLICATION_FALLBACK_BYTES = 256
_SEMANTIC_OBSERVER_MODULE = "_semantic_capture_bootstrap"
_CALLER_WRAPPER_MODULES = frozenset({"subprocess", "asyncio.subprocess", "asyncio.base_subprocess"})
_THREAD_PROFILE_HOOK_SETTERS = frozenset({"setprofile", "setprofile_all_threads"})
_THREAD_TRACE_HOOK_SETTERS = frozenset({"settrace", "settrace_all_threads"})
_UVICORN_HTTP_ADAPTERS = {
    "uvicorn.protocols.http.h11_impl": "uvicorn.h11",
    "uvicorn.protocols.http.httptools_impl": "uvicorn.httptools",
}
_NATIVE_FILENAME = "<native>"
_PYTHON_EXCEPTION_FILTER_VERSION = 1
_FILTERED_CONTROL_FLOW_EXCEPTION_TYPES = (
    GeneratorExit,
    StopAsyncIteration,
    StopIteration,
)
_FILTERED_CONTROL_FLOW_EXCEPTION_TYPE_NAMES = (
    "GeneratorExit",
    "StopAsyncIteration",
    "StopIteration",
)

type _FunctionKey = tuple[str, str, str, int, str]
type _StackEntry = tuple[_FunctionKey | None, int, int, bool]
type _WsgiBoundary = tuple[int | None, _semantic_capture.ActiveToken | None]

_IGNORED_OBSERVER_KEY: _FunctionKey = (
    "<contrail>",
    "semantic-observer",
    "<contrail>",
    0,
    "runtime",
)

_aggregates: dict[_FunctionKey, list[int]] = {}
_native_aggregates: dict[_FunctionKey, list[int]] = {}
_call_edges: dict[tuple[_FunctionKey, _FunctionKey], list[int]] = {}
_native_call_edges: dict[tuple[_FunctionKey, _FunctionKey], list[int]] = {}
_code_keys: dict[CodeType, _FunctionKey] = {}
_tracked_function_keys: set[_FunctionKey] = set()
_tracked_native_function_keys: set[_FunctionKey] = set()
_stacks: dict[int, list[_StackEntry]] = {}
_wsgi_boundaries: dict[int, _WsgiBoundary] = {}
_active_wsgi_frames: dict[int, list[int]] = {}
_attributed_wsgi_frames: set[int] = set()
_asgi_boundaries: dict[int, int | None] = {}
_asgi_cycle_frames: dict[int, int] = {}
_asgi_frame_cycles: dict[int, int] = {}
_asgi_final_send_frames: dict[int, int] = {}
_attributed_asgi_frames: set[int] = set()
_dropped_call_count = 0
_dropped_edge_count = 0
_dropped_exception_event_count = 0
_dropped_non_control_flow_exception_event_count = 0
_callback_error_count = 0
_profile_hook_setter_call_count = 0
_trace_hook_setter_call_count = 0
_observer_active = False
_started_at_ns = time.time_ns()
_stop_event = threading.Event()
_checkpoint_thread: threading.Thread | None = None
_runtime_prefixes = tuple(
    dict.fromkeys(
        os.path.realpath(prefix)
        for prefix in (
            sys.base_prefix,
            sys.prefix,
            *(
                entry
                for entry in sys.path
                if isinstance(entry, str)
                and entry
                and any(
                    component in {"site-packages", "dist-packages"}
                    for component in os.path.normpath(entry).split(os.sep)
                )
            ),
        )
        if isinstance(prefix, str) and prefix
    )
)


def _bounded_text(value: object, fallback: str) -> str:
    text = value if isinstance(value, str) and value else fallback
    return text.encode("utf-8", "backslashreplace").decode("utf-8")[:_MAX_TEXT_CHARACTERS]


def _scope(filename: str) -> str:
    if filename.startswith("<frozen "):
        return "runtime"
    if filename.startswith("<"):
        return "application"
    resolved = os.path.realpath(filename)
    for prefix in _runtime_prefixes:
        try:
            if os.path.commonpath((resolved, prefix)) == prefix:
                return "library"
        except ValueError:
            continue
    return "application"


def _function_key(frame: FrameType) -> _FunctionKey:
    code = frame.f_code
    existing = _code_keys.get(code)
    if existing is not None:
        return existing
    filename = _bounded_text(code.co_filename, "<unknown>")
    key = (
        _bounded_text(frame.f_globals.get("__name__"), "<unknown>"),
        _bounded_text(code.co_qualname, code.co_name or "<unknown>"),
        filename,
        max(0, code.co_firstlineno),
        _scope(filename),
    )
    if len(_code_keys) < _MAX_FUNCTIONS:
        _code_keys[code] = key
    return key


def _native_function_key(value: object) -> _FunctionKey:
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    name = getattr(value, "__name__", None)
    owner = type(getattr(value, "__self__", None))
    if not isinstance(module, str) or not module:
        module = getattr(owner, "__module__", None)
    if not isinstance(qualname, str) or not qualname:
        owner_name = getattr(owner, "__qualname__", None)
        if isinstance(owner_name, str) and owner_name and isinstance(name, str) and name:
            qualname = f"{owner_name}.{name}"
        else:
            qualname = name
    bounded_module = _bounded_text(module, "<native>")
    bounded_qualname = _bounded_text(qualname, "<unknown>")
    top_level_module = bounded_module.partition(".")[0]
    scope = "runtime" if top_level_module in sys.stdlib_module_names else "library"
    return bounded_module, bounded_qualname, _NATIVE_FILENAME, 0, scope


def _preferred_caller(keys: list[_FunctionKey]) -> _FunctionKey | None:
    for key in reversed(keys):
        if key[4] == "application":
            return key
    for key in reversed(keys):
        if key[0] not in _CALLER_WRAPPER_MODULES and key != _IGNORED_OBSERVER_KEY:
            return key
    return next((key for key in reversed(keys) if key != _IGNORED_OBSERVER_KEY), None)


def _semantic_caller() -> tuple[str, str, str, int, str, str] | None:
    stack = _stacks.get(threading.get_ident())
    if not stack:
        return None
    caller = _preferred_caller([entry[0] for entry in stack if entry[0] is not None])
    return None if caller is None else (*caller, "exact")


def _is_wsgi_handler_frame(frame: FrameType, method: str) -> bool:
    module_name = frame.f_globals.get("__name__")
    return (
        isinstance(module_name, str)
        and module_name == "wsgiref.handlers"
        and frame.f_code.co_qualname == f"BaseHandler.{method}"
    )


def _start_wsgi_request(frame: FrameType, thread_id: int) -> None:
    frame_id = id(frame)
    boundary = _semantic_capture.begin_wsgi_request()
    _wsgi_boundaries[frame_id] = boundary
    _active_wsgi_frames.setdefault(thread_id, []).append(frame_id)


def _record_wsgi_status(frame: FrameType, thread_id: int) -> None:
    active = _active_wsgi_frames.get(thread_id)
    if not active:
        return
    boundary = _wsgi_boundaries.get(active[-1])
    if boundary is not None:
        _semantic_capture.record_wsgi_status(boundary[0], frame.f_locals.get("status"))


def _attribute_wsgi_request(thread_id: int, key: _FunctionKey) -> None:
    active = _active_wsgi_frames.get(thread_id)
    if not active or key[4] != "application":
        return
    frame_id = active[-1]
    if frame_id in _attributed_wsgi_frames:
        return
    _semantic_capture.attribute_active_caller(thread_id, (*key, "exact"))
    _attributed_wsgi_frames.add(frame_id)


def _finish_wsgi_request(frame: FrameType, thread_id: int) -> None:
    frame_id = id(frame)
    boundary = _wsgi_boundaries.pop(frame_id, None)
    active = _active_wsgi_frames.get(thread_id)
    if active:
        if active[-1] == frame_id:
            active.pop()
        elif frame_id in active:
            active.remove(frame_id)
        if not active:
            _active_wsgi_frames.pop(thread_id, None)
    _attributed_wsgi_frames.discard(frame_id)
    if boundary is not None:
        _semantic_capture.finish_wsgi_request(*boundary)


def _uvicorn_http_adapter(frame: FrameType, method: str) -> str | None:
    module_name = frame.f_globals.get("__name__")
    if not isinstance(module_name, str):
        return None
    adapter = _UVICORN_HTTP_ADAPTERS.get(module_name)
    if adapter is None or frame.f_code.co_qualname != f"RequestResponseCycle.{method}":
        return None
    return adapter


def _start_asgi_request(frame: FrameType, adapter: str) -> None:
    frame_id = id(frame)
    if frame_id in _asgi_boundaries:
        return
    cycle = frame.f_locals.get("self")
    if cycle is None:
        return
    cycle_id = id(cycle)
    if cycle_id in _asgi_cycle_frames:
        return
    identifier = _semantic_capture.begin_asgi_request(adapter)
    _asgi_boundaries[frame_id] = identifier
    _asgi_cycle_frames[cycle_id] = frame_id
    _asgi_frame_cycles[frame_id] = cycle_id


def _attribute_asgi_request(frame: FrameType, key: _FunctionKey) -> None:
    if key[4] != "application":
        return
    ancestor = frame.f_back
    while ancestor is not None:
        frame_id = id(ancestor)
        if frame_id in _asgi_boundaries:
            if frame_id not in _attributed_asgi_frames:
                _semantic_capture.attribute_logical_operation_caller(
                    _asgi_boundaries[frame_id],
                    (*key, "exact"),
                )
                _attributed_asgi_frames.add(frame_id)
            return
        ancestor = ancestor.f_back


def _observe_asgi_send(frame: FrameType) -> None:
    cycle = frame.f_locals.get("self")
    if cycle is None:
        return
    request_frame_id = _asgi_cycle_frames.get(id(cycle))
    if request_frame_id is None:
        return
    message = frame.f_locals.get("message")
    if type(message) is not dict:
        return
    message_type = message.get("type")
    if message_type == "http.response.start":
        _semantic_capture.record_asgi_status(
            _asgi_boundaries.get(request_frame_id),
            message.get("status"),
        )
    elif message_type == "http.response.body" and message.get("more_body", False) is False:
        _asgi_final_send_frames[id(frame)] = request_frame_id


def _coroutine_suspended(frame: FrameType) -> bool:
    instruction = frame.f_lasti
    code = frame.f_code.co_code
    if not 0 <= instruction < len(code):
        return False
    operation = dis.opname[code[instruction]]
    if operation in {"YIELD_FROM", "YIELD_VALUE"}:
        return True
    return (
        operation == "RESUME"
        and instruction >= 2
        and dis.opname[code[instruction - 2]] == "YIELD_VALUE"
    )


def _finish_asgi_request(request_frame_id: int) -> None:
    if request_frame_id not in _asgi_boundaries:
        return
    identifier = _asgi_boundaries.pop(request_frame_id)
    cycle_id = _asgi_frame_cycles.pop(request_frame_id, None)
    if cycle_id is not None and _asgi_cycle_frames.get(cycle_id) == request_frame_id:
        _asgi_cycle_frames.pop(cycle_id, None)
    _attributed_asgi_frames.discard(request_frame_id)
    _semantic_capture.finish_asgi_request(identifier)


def _profile(frame: FrameType, event: str, argument: object) -> None:
    global _callback_error_count, _dropped_call_count, _dropped_edge_count
    global _profile_hook_setter_call_count, _trace_hook_setter_call_count
    try:
        if _observer_active:
            if event == "c_call":
                if argument is sys.setprofile and frame.f_globals.get("__name__") != "threading":
                    _profile_hook_setter_call_count += 1
                elif argument is sys.settrace and frame.f_globals.get("__name__") != "threading":
                    _trace_hook_setter_call_count += 1
            elif event == "call":
                function_name = frame.f_code.co_name
                if (
                    function_name in _THREAD_PROFILE_HOOK_SETTERS
                    and frame.f_globals.get("__name__") == "threading"
                ):
                    _profile_hook_setter_call_count += 1
                elif (
                    function_name in _THREAD_TRACE_HOOK_SETTERS
                    and frame.f_globals.get("__name__") == "threading"
                ):
                    _trace_hook_setter_call_count += 1
        thread_id = threading.get_ident()
        stack = _stacks.setdefault(thread_id, [])
        if event in {"call", "c_call"}:
            if event == "call":
                if _is_wsgi_handler_frame(frame, "run"):
                    _start_wsgi_request(frame, thread_id)
                elif _is_wsgi_handler_frame(frame, "start_response"):
                    _record_wsgi_status(frame, thread_id)
                else:
                    asgi_adapter = _uvicorn_http_adapter(frame, "run_asgi")
                    if asgi_adapter is not None:
                        _start_asgi_request(frame, asgi_adapter)
                    elif _uvicorn_http_adapter(frame, "send") is not None:
                        _observe_asgi_send(frame)
            tracked_key: _FunctionKey | None
            native = event == "c_call"
            if (stack and stack[-1][0] is _IGNORED_OBSERVER_KEY) or frame.f_globals.get(
                "__name__"
            ) == _SEMANTIC_OBSERVER_MODULE:
                tracked_key = _IGNORED_OBSERVER_KEY
                native = False
            else:
                key = _native_function_key(argument) if native else _function_key(frame)
                tracked_keys = _tracked_native_function_keys if native else _tracked_function_keys
                limit = _MAX_NATIVE_FUNCTIONS if native else _MAX_FUNCTIONS
                if key in tracked_keys or len(tracked_keys) < limit:
                    tracked_keys.add(key)
                    tracked_key = key
                else:
                    tracked_key = None
            if tracked_key is not None and tracked_key is not _IGNORED_OBSERVER_KEY:
                _attribute_wsgi_request(thread_id, tracked_key)
                if event == "call":
                    _attribute_asgi_request(frame, tracked_key)
            if len(stack) >= _MAX_STACK_DEPTH:
                tracked_key = None
            stack.append((tracked_key, time.perf_counter_ns(), 0, native))
            return
        if event == "return":
            if _is_wsgi_handler_frame(frame, "run"):
                _finish_wsgi_request(frame, thread_id)
            elif _uvicorn_http_adapter(frame, "send") is not None:
                send_frame_id = id(frame)
                request_frame_id = _asgi_final_send_frames.get(send_frame_id)
                if request_frame_id is not None and not _coroutine_suspended(frame):
                    _asgi_final_send_frames.pop(send_frame_id, None)
                    _finish_asgi_request(request_frame_id)
            elif _uvicorn_http_adapter(frame, "run_asgi") is not None and not (
                _coroutine_suspended(frame)
            ):
                _finish_asgi_request(id(frame))
        if event not in {"return", "c_return", "c_exception"} or not stack:
            return
        key, started_at_ns, child_ns, native = stack.pop()
        elapsed_ns = max(0, time.perf_counter_ns() - started_at_ns)
        if stack:
            parent_key, parent_started_at_ns, parent_child_ns, parent_native = stack[-1]
            stack[-1] = (
                parent_key,
                parent_started_at_ns,
                parent_child_ns + elapsed_ns,
                parent_native,
            )
        if key is _IGNORED_OBSERVER_KEY:
            return
        if key is None:
            _dropped_call_count += 1
            return
        aggregates = _native_aggregates if native else _aggregates
        aggregate = aggregates.setdefault(key, [0, 0, 0, 0, 0, 0])
        aggregate[0] += 1
        aggregate[1] += elapsed_ns
        aggregate[2] += max(0, elapsed_ns - child_ns)
        aggregate[3] = max(aggregate[3], elapsed_ns)
        aggregate[4] += event == "c_exception"
        if not stack:
            return
        parent_key = stack[-1][0]
        if parent_key is None or parent_key == key:
            return
        edge_key = (parent_key, key)
        edges = _native_call_edges if native or stack[-1][3] else _call_edges
        edge = edges.get(edge_key)
        if edge is None:
            limit = _MAX_NATIVE_EDGES if edges is _native_call_edges else _MAX_EDGES
            if len(edges) >= limit:
                _dropped_edge_count += 1
                return
            edge = [0, 0]
            edges[edge_key] = edge
        edge[0] += 1
        edge[1] += elapsed_ns
    except BaseException:
        # Profiling must never turn an observed workload into a failed workload.
        _callback_error_count += 1


def _is_builtin_iterator_control_flow(argument: object) -> bool:
    if not isinstance(argument, tuple) or not argument:
        return False
    exception_type = argument[0]
    return any(exception_type is candidate for candidate in _FILTERED_CONTROL_FLOW_EXCEPTION_TYPES)


def _trace(frame: FrameType, event: str, argument: object) -> object:
    """Count propagation and separately exclude built-in iterator control flow."""
    global _callback_error_count, _dropped_exception_event_count
    global _dropped_non_control_flow_exception_event_count
    try:
        frame.f_trace_lines = False
        frame.f_trace_opcodes = False
        if event != "exception":
            return _trace
        if frame.f_globals.get("__name__") == _SEMANTIC_OBSERVER_MODULE:
            return _trace
        control_flow = _is_builtin_iterator_control_flow(argument)
        key = _function_key(frame)
        if key not in _tracked_function_keys:
            if len(_tracked_function_keys) >= _MAX_FUNCTIONS:
                _dropped_exception_event_count += 1
                if not control_flow:
                    _dropped_non_control_flow_exception_event_count += 1
                return _trace
            _tracked_function_keys.add(key)
        aggregate = _aggregates.setdefault(key, [0, 0, 0, 0, 0, 0])
        aggregate[4] += 1
        aggregate[5] += not control_flow
    except BaseException:
        # Tracing must never turn an observed workload into a failed workload.
        _callback_error_count += 1
    return _trace


def _checkpoint_loop() -> None:
    sys.setprofile(None)
    sys.settrace(None)
    if _stop_event.wait(_FIRST_CHECKPOINT_SECONDS):
        return
    _publish_report("checkpoint")
    while not _stop_event.wait(_CHECKPOINT_INTERVAL_SECONDS):
        _publish_report("checkpoint")


def _start_checkpoint_thread() -> None:
    global _checkpoint_thread
    _checkpoint_thread = threading.Thread(
        target=_checkpoint_loop,
        name="contrail-python-profile-checkpoint",
        daemon=True,
    )
    _checkpoint_thread.start()


def _reset_after_fork() -> None:
    global _callback_error_count, _dropped_call_count, _dropped_edge_count, _started_at_ns
    global _dropped_exception_event_count, _dropped_non_control_flow_exception_event_count
    global _observer_active, _profile_hook_setter_call_count, _stop_event
    global _trace_hook_setter_call_count
    _observer_active = False
    sys.setprofile(None)
    threading.setprofile(None)
    sys.settrace(None)
    threading.settrace(None)
    _aggregates.clear()
    _native_aggregates.clear()
    _call_edges.clear()
    _native_call_edges.clear()
    _code_keys.clear()
    _tracked_function_keys.clear()
    _tracked_native_function_keys.clear()
    _stacks.clear()
    _wsgi_boundaries.clear()
    _active_wsgi_frames.clear()
    _attributed_wsgi_frames.clear()
    _asgi_boundaries.clear()
    _asgi_cycle_frames.clear()
    _asgi_frame_cycles.clear()
    _asgi_final_send_frames.clear()
    _attributed_asgi_frames.clear()
    _dropped_call_count = 0
    _dropped_edge_count = 0
    _dropped_exception_event_count = 0
    _dropped_non_control_flow_exception_event_count = 0
    _callback_error_count = 0
    _profile_hook_setter_call_count = 0
    _trace_hook_setter_call_count = 0
    _started_at_ns = time.time_ns()
    _stop_event = threading.Event()
    _semantic_capture.reset_after_fork()
    _publish_report("checkpoint", registration_only=True)
    _start_checkpoint_thread()
    threading.setprofile(_profile)
    threading.settrace(_trace)
    sys.setprofile(_profile)
    sys.settrace(_trace)
    _observer_active = True


def _document() -> dict[str, object]:
    ordered_functions = sorted(
        (*_aggregates.items(), *_native_aggregates.items()),
        key=lambda item: (-item[1][2], -item[1][1], item[0]),
    )
    identifiers = {key: index for index, (key, _) in enumerate(ordered_functions)}
    functions = [
        {
            "id": identifiers[key],
            "module": key[0],
            "qualname": key[1],
            "filename": key[2],
            "firstlineno": key[3],
            "scope": key[4],
            "call_count": values[0],
            "total_ns": values[1],
            "self_ns": values[2],
            "max_ns": values[3],
            "native": key[2] == _NATIVE_FILENAME,
            "exception_count": values[4],
            "non_control_flow_exception_count": values[5],
        }
        for key, values in ordered_functions
    ]
    edges = [
        {
            "source_id": identifiers[source],
            "target_id": identifiers[target],
            "call_count": values[0],
            "total_ns": values[1],
        }
        for (source, target), values in sorted(
            (*_call_edges.items(), *_native_call_edges.items()),
            key=lambda item: (-item[1][1], item[0]),
        )
        if source in identifiers and target in identifiers
    ]
    current_thread = threading.get_ident()
    open_call_count = sum(
        sum(1 for key, _, _, _ in stack if key is not _IGNORED_OBSERVER_KEY)
        for thread_id, stack in _stacks.items()
        if thread_id != current_thread
    )
    return {
        "format_version": 1,
        "pid": os.getpid(),
        "python_version": sys.version.split()[0],
        "publication_metrics_version": _PUBLICATION_METRICS_VERSION,
        "started_at_ns": _started_at_ns,
        "finished_at_ns": time.time_ns(),
        "limits": {
            "max_functions": _MAX_FUNCTIONS,
            "max_native_functions": _MAX_NATIVE_FUNCTIONS,
            "max_edges": _MAX_EDGES,
            "max_native_edges": _MAX_NATIVE_EDGES,
            "max_stack_depth": _MAX_STACK_DEPTH,
        },
        "truncated": bool(
            _dropped_call_count
            or _dropped_edge_count
            or _dropped_exception_event_count
            or _callback_error_count
            or _profile_hook_setter_call_count
            or open_call_count
        ),
        "dropped_call_count": _dropped_call_count,
        "dropped_edge_count": _dropped_edge_count,
        "dropped_exception_event_count": _dropped_exception_event_count,
        "dropped_non_control_flow_exception_event_count": (
            _dropped_non_control_flow_exception_event_count
        ),
        "python_exception_filter": {
            "format_version": _PYTHON_EXCEPTION_FILTER_VERSION,
            "event_semantics": "exact_type_identity",
            "filtered_exception_types": list(_FILTERED_CONTROL_FLOW_EXCEPTION_TYPE_NAMES),
            "exception_type_identity_inspected": True,
            "exception_types_captured": False,
        },
        "callback_error_count": _callback_error_count,
        "observer_integrity": {
            "format_version": _OBSERVER_INTEGRITY_VERSION,
            "profile_hook_setter_call_count": _profile_hook_setter_call_count,
            "trace_hook_setter_call_count": _trace_hook_setter_call_count,
        },
        "open_call_count": open_call_count,
        "functions": functions,
        "edges": edges,
    }


def _encoded_report(snapshot_kind: str, *, registration_only: bool = False) -> bytes:
    document = _document()
    document["semantic_capture"] = _semantic_capture.snapshot(registration_only=registration_only)
    document["snapshot_kind"] = snapshot_kind
    document["checkpoint_interval_ns"] = _CHECKPOINT_INTERVAL_NS
    document["first_checkpoint_delay_ns"] = _FIRST_CHECKPOINT_NS
    if registration_only:
        document["registration_only"] = True
    encoded = json.dumps(document, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_REPORT_BYTES - _MAX_PUBLICATION_FALLBACK_BYTES:
        document["functions"] = []
        document["edges"] = []
        document["truncated"] = True
        document["report_oversized"] = True
        encoded = json.dumps(document, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return encoded


def _fallback_report(
    encoded: bytes,
    *,
    socket_attempted: bool,
    socket_failure_ns: int,
    snapshot_kind: int,
) -> bytes:
    kind = {
        _SNAPSHOT_KIND_REGISTRATION: "registration",
        _SNAPSHOT_KIND_CHECKPOINT: "checkpoint",
        _SNAPSHOT_KIND_FINAL: "final",
    }[snapshot_kind]
    fallback = json.dumps(
        {
            "socket_attempted": socket_attempted,
            "socket_failure_ns": socket_failure_ns,
            "snapshot_kind": kind,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if not encoded.endswith(b"}"):
        raise ValueError("profile report must be a JSON object")
    result = encoded[:-1] + b',"publication_fallback":' + fallback + b"}"
    if len(result) > _MAX_REPORT_BYTES:
        raise ValueError("profile fallback report exceeds its byte limit")
    return result


def _send_to_collector(
    encoded: bytes,
    serialization_ns: int,
    snapshot_kind: int,
) -> bool:
    socket_path = os.environ.get(_SOCKET_ENV)
    if not socket_path or not os.path.isabs(socket_path):
        return False
    suppression_token = _semantic_capture._begin_network_suppression()
    try:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(
                    _FINAL_SNAPSHOT_SOCKET_TIMEOUT_SECONDS
                    if snapshot_kind == _SNAPSHOT_KIND_FINAL
                    else _SNAPSHOT_SOCKET_TIMEOUT_SECONDS
                )
                connection.connect(socket_path)
                connection.sendall(
                    _SNAPSHOT_HEADER.pack(
                        _SNAPSHOT_PROTOCOL_MAGIC,
                        os.getpid(),
                        len(encoded),
                        serialization_ns,
                        snapshot_kind,
                    )
                )
                connection.sendall(encoded)
            return True
        except OSError:
            return False
    finally:
        _semantic_capture._end_network_suppression(suppression_token)


def _write_file(encoded: bytes) -> bool:
    directory = os.environ.get(_DIRECTORY_ENV)
    if not directory or not os.path.isabs(directory):
        return False
    temporary = os.path.join(directory, f".profile-{os.getpid()}.tmp")
    destination = os.path.join(directory, f"profile-{os.getpid()}.json")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                return False
            view = view[written:]
        os.close(descriptor)
        descriptor = None
        os.replace(temporary, destination)
        return True
    except OSError:
        return False
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        try:
            os.unlink(temporary)
        except OSError:
            pass


def _publish_report(snapshot_kind: str, *, registration_only: bool = False) -> None:
    global _callback_error_count
    try:
        started_at_ns = time.perf_counter_ns()
        encoded = _encoded_report(snapshot_kind, registration_only=registration_only)
        serialization_ns = max(0, time.perf_counter_ns() - started_at_ns)
        transport_kind = (
            _SNAPSHOT_KIND_REGISTRATION
            if registration_only
            else (
                _SNAPSHOT_KIND_CHECKPOINT if snapshot_kind == "checkpoint" else _SNAPSHOT_KIND_FINAL
            )
        )
        socket_path = os.environ.get(_SOCKET_ENV)
        socket_attempted = bool(socket_path and os.path.isabs(socket_path))
        socket_started_at_ns = time.perf_counter_ns()
        sent = _send_to_collector(encoded, serialization_ns, transport_kind)
        socket_failure_ns = (
            max(0, time.perf_counter_ns() - socket_started_at_ns) if socket_attempted else 0
        )
        if not sent and not _write_file(
            _fallback_report(
                encoded,
                socket_attempted=socket_attempted,
                socket_failure_ns=socket_failure_ns,
                snapshot_kind=transport_kind,
            )
        ):
            _callback_error_count += 1
    except BaseException:
        _callback_error_count += 1


def _write_report() -> None:
    global _callback_error_count, _observer_active
    _observer_active = False
    sys.setprofile(None)
    threading.setprofile(None)
    sys.settrace(None)
    threading.settrace(None)
    _stop_event.set()
    if _checkpoint_thread is not None:
        _checkpoint_thread.join(timeout=max(1.0, _CHECKPOINT_INTERVAL_SECONDS * 2))
        if _checkpoint_thread.is_alive():
            _callback_error_count += 1
    _publish_report("final")


def _activate() -> None:
    global _observer_active
    directory = os.environ.get(_DIRECTORY_ENV)
    if not directory or not os.path.isabs(directory):
        return
    _semantic_capture.activate(
        _semantic_caller,
        instrument_logical_operations=True,
    )
    atexit.register(_write_report)
    if hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=_reset_after_fork)
    _publish_report("checkpoint", registration_only=True)
    _start_checkpoint_thread()
    threading.setprofile(_profile)
    threading.settrace(_trace)
    sys.setprofile(_profile)
    sys.settrace(_trace)
    _observer_active = True


_activate()
