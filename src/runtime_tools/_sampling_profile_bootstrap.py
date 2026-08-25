"""Standalone startup sampler copied into a captured Python environment.

This module must remain standard-library-only: capture copies it as
``sitecustomize.py`` so a workload does not need Contrail installed.
"""

from __future__ import annotations

import atexit
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

_DIRECTORY_ENV = "_CONTRAIL_SAMPLE_PROFILE_DIRECTORY"
_SOCKET_ENV = "_CONTRAIL_PROFILE_SNAPSHOT_SOCKET"
_INTERVAL_SECONDS = 0.01
_INTERVAL_NS = 10_000_000
_CHECKPOINT_INTERVAL_SECONDS = 0.5
_CHECKPOINT_INTERVAL_NS = 500_000_000
_FIRST_CHECKPOINT_SECONDS = 0.05
_FIRST_CHECKPOINT_NS = 50_000_000
_MAX_FUNCTIONS = 2_000
_MAX_EDGES = 10_000
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
_MAX_PUBLICATION_FALLBACK_BYTES = 256
_SEMANTIC_OBSERVER_MODULE = "_semantic_capture_bootstrap"
_CALLER_WRAPPER_MODULES = frozenset({"subprocess", "asyncio.subprocess", "asyncio.base_subprocess"})

type _FunctionKey = tuple[str, str, str, int, str]

_aggregates: dict[_FunctionKey, list[int]] = {}
_stack_edges: dict[tuple[_FunctionKey, _FunctionKey], int] = {}
_code_keys: dict[CodeType, _FunctionKey] = {}
_sample_count = 0
_thread_sample_count = 0
_dropped_frame_sample_count = 0
_dropped_edge_sample_count = 0
_callback_error_count = 0
_started_at_ns = time.time_ns()
_stop_event = threading.Event()
_sampler_thread: threading.Thread | None = None
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


def _preferred_caller(keys: list[_FunctionKey]) -> _FunctionKey | None:
    for key in reversed(keys):
        if key[4] == "application":
            return key
    for key in reversed(keys):
        if key[0] not in _CALLER_WRAPPER_MODULES:
            return key
    return keys[-1] if keys else None


def _record_stack(thread_id: int, frame: FrameType) -> None:
    global _dropped_edge_sample_count, _dropped_frame_sample_count
    frames: list[FrameType] = []
    current: FrameType | None = frame
    while current is not None and len(frames) < _MAX_STACK_DEPTH:
        frames.append(current)
        current = current.f_back
    if current is not None:
        _dropped_frame_sample_count += 1

    tracked: list[_FunctionKey | None] = []
    caller_keys: list[_FunctionKey] = []
    observer_suppressed = False
    for sampled_frame in reversed(frames):
        if sampled_frame.f_globals.get("__name__") == _SEMANTIC_OBSERVER_MODULE:
            observer_suppressed = True
            caller = _preferred_caller(caller_keys)
            if caller is not None:
                _semantic_capture.attribute_active_caller(
                    thread_id,
                    (*caller, "sampled"),
                )
            break
        key = _function_key(sampled_frame)
        caller_keys.append(key)
        if key not in _aggregates and len(_aggregates) >= _MAX_FUNCTIONS:
            tracked.append(None)
            _dropped_frame_sample_count += 1
            continue
        aggregate = _aggregates.setdefault(key, [0, 0])
        aggregate[0] += 1
        tracked.append(key)

    if tracked and tracked[-1] is not None and not observer_suppressed:
        _aggregates[tracked[-1]][1] += 1
    for source, target in zip(tracked, tracked[1:], strict=False):
        if source is None or target is None or source == target:
            continue
        edge_key = (source, target)
        if edge_key not in _stack_edges and len(_stack_edges) >= _MAX_EDGES:
            _dropped_edge_sample_count += 1
            continue
        _stack_edges[edge_key] = _stack_edges.get(edge_key, 0) + 1


def _sample_once() -> None:
    global _callback_error_count, _sample_count, _thread_sample_count
    try:
        if _stop_event.is_set():
            return
        frames = sys._current_frames()
        if _stop_event.is_set():
            return
        sampler_id = threading.get_ident()
        _sample_count += 1
        for thread_id, frame in frames.items():
            if _stop_event.is_set():
                return
            if thread_id == sampler_id:
                continue
            _thread_sample_count += 1
            _record_stack(thread_id, frame)
    except BaseException:
        # Sampling must never turn an observed workload into a failed workload.
        _callback_error_count += 1


def _sampling_loop() -> None:
    next_checkpoint = time.monotonic() + _FIRST_CHECKPOINT_SECONDS
    while not _stop_event.wait(_INTERVAL_SECONDS):
        _sample_once()
        now = time.monotonic()
        if now >= next_checkpoint:
            _publish_report("checkpoint")
            next_checkpoint = now + _CHECKPOINT_INTERVAL_SECONDS


def _start_sampler() -> None:
    global _sampler_thread
    _sampler_thread = threading.Thread(
        target=_sampling_loop,
        name="contrail-python-sampler",
        daemon=True,
    )
    _sampler_thread.start()


def _reset_after_fork() -> None:
    global _callback_error_count, _dropped_edge_sample_count
    global _dropped_frame_sample_count, _sample_count, _started_at_ns
    global _stop_event, _thread_sample_count
    _aggregates.clear()
    _stack_edges.clear()
    _code_keys.clear()
    _sample_count = 0
    _thread_sample_count = 0
    _dropped_frame_sample_count = 0
    _dropped_edge_sample_count = 0
    _callback_error_count = 0
    _started_at_ns = time.time_ns()
    _stop_event = threading.Event()
    _semantic_capture.reset_after_fork()
    _publish_report("checkpoint", registration_only=True)
    _start_sampler()


def _document() -> dict[str, object]:
    ordered_functions = sorted(
        _aggregates.items(),
        key=lambda item: (-item[1][1], -item[1][0], item[0]),
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
            "sample_count": values[0],
            "leaf_sample_count": values[1],
        }
        for key, values in ordered_functions
    ]
    edges = [
        {
            "source_id": identifiers[source],
            "target_id": identifiers[target],
            "sample_count": sample_count,
        }
        for (source, target), sample_count in sorted(
            _stack_edges.items(),
            key=lambda item: (-item[1], item[0]),
        )
        if source in identifiers and target in identifiers
    ]
    return {
        "format_version": 1,
        "mode": "sample",
        "pid": os.getpid(),
        "python_version": sys.version.split()[0],
        "publication_metrics_version": _PUBLICATION_METRICS_VERSION,
        "started_at_ns": _started_at_ns,
        "finished_at_ns": time.time_ns(),
        "interval_ns": _INTERVAL_NS,
        "limits": {
            "max_functions": _MAX_FUNCTIONS,
            "max_edges": _MAX_EDGES,
            "max_stack_depth": _MAX_STACK_DEPTH,
        },
        "truncated": bool(
            _dropped_frame_sample_count or _dropped_edge_sample_count or _callback_error_count
        ),
        "sample_count": _sample_count,
        "thread_sample_count": _thread_sample_count,
        "dropped_frame_sample_count": _dropped_frame_sample_count,
        "dropped_edge_sample_count": _dropped_edge_sample_count,
        "callback_error_count": _callback_error_count,
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
    global _callback_error_count
    _stop_event.set()
    if _sampler_thread is not None:
        _sampler_thread.join(timeout=max(0.1, _INTERVAL_SECONDS * 4))
        if _sampler_thread.is_alive():
            _callback_error_count += 1
    _publish_report("final")


def _activate() -> None:
    directory = os.environ.get(_DIRECTORY_ENV)
    if not directory or not os.path.isabs(directory):
        return
    _semantic_capture.activate()
    atexit.register(_write_report)
    if hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=_reset_after_fork)
    _publish_report("checkpoint", registration_only=True)
    _start_sampler()


_activate()
