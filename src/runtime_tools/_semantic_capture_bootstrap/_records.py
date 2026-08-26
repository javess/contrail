"""Standalone semantic observer copied beside a capture ``sitecustomize``.

The module must remain standard-library-only. It deliberately records no
subprocess arguments, environment values, working directories, or output, and
no HTTP server names, URLs, headers, request or response bodies, or credentials.
Deep capture also observes selected database, cache, queue, broker, executor,
and scheduler operation boundaries, including executor task submission-to-completion
intervals, without retaining statements, parameters, keys, destinations,
payloads, queue items, callables, awaitables, task names, context values,
arguments, or return values.
"""

from __future__ import annotations

import os
import threading
import time
from typing import cast

from ._constants import (
    MAX_CALLER_TEXT_CHARACTERS,
    MAX_COUNTER,
    MAX_EXECUTABLE_CHARACTERS,
    MAX_SUBPROCESS_RECORDS,
    ActiveToken,
    CallerIdentity,
    NetworkSuppressionToken,
)
from ._state import STATE


def _increment(value: int) -> int:
    return min(MAX_COUNTER, value + 1)


def _record_error(key: str) -> None:
    try:
        with STATE._lock:
            STATE._callback_error_counts[key] = _increment(STATE._callback_error_counts[key])
    except BaseException:
        pass


def _record_callback_error() -> None:
    _record_error("callback_error_count")


def _record_caller_callback_error() -> None:
    _record_error("caller_callback_error_count")


def _record_http_callback_error() -> None:
    _record_error("http_callback_error_count")


def _record_http_caller_callback_error() -> None:
    _record_error("http_caller_callback_error_count")


def _record_network_callback_error() -> None:
    _record_error("network_callback_error_count")


def _record_network_caller_callback_error() -> None:
    _record_error("network_caller_callback_error_count")


def _record_network_setup_callback_error() -> None:
    _record_error("network_setup_callback_error_count")


def _record_network_setup_caller_callback_error() -> None:
    _record_error("network_setup_caller_callback_error_count")


def _record_logical_operation_callback_error() -> None:
    _record_error("logical_operation_callback_error_count")


def _record_logical_operation_caller_callback_error() -> None:
    _record_error("logical_operation_caller_callback_error_count")


def _begin_network_suppression() -> NetworkSuppressionToken | None:
    if STATE._network_capture_suppressed.get():
        return None
    return STATE._network_capture_suppressed.set(True)


def _end_network_suppression(token: NetworkSuppressionToken | None) -> None:
    if token is not None:
        STATE._network_capture_suppressed.reset(token)


def _caller_value(value: object) -> dict[str, object] | None:
    if type(value) is not tuple or len(value) != 6:
        return None
    module, qualname, filename, firstlineno, scope, observation = value
    if (
        type(module) is not str
        or not module
        or type(qualname) is not str
        or not qualname
        or type(filename) is not str
        or not filename
        or type(firstlineno) is not int
        or not 0 <= firstlineno <= MAX_COUNTER
        or type(scope) is not str
        or scope not in {"application", "library", "runtime"}
        or type(observation) is not str
        or observation not in {"exact", "sampled"}
    ):
        return None
    return {
        "module": module[:MAX_CALLER_TEXT_CHARACTERS],
        "qualname": qualname[:MAX_CALLER_TEXT_CHARACTERS],
        "filename": filename[:MAX_CALLER_TEXT_CHARACTERS],
        "firstlineno": firstlineno,
        "scope": scope,
        "observation": observation,
    }


def _capture_caller(
    *,
    http_boundary: bool = False,
    logical_operation_boundary: bool = False,
    network_boundary: bool = False,
    network_setup_boundary: bool = False,
) -> dict[str, object] | None:
    provider = STATE._caller_provider
    if provider is None:
        return None
    try:
        provided = provider()
    except BaseException:
        if http_boundary:
            _record_http_caller_callback_error()
        elif logical_operation_boundary:
            _record_logical_operation_caller_callback_error()
        elif network_setup_boundary:
            _record_network_setup_caller_callback_error()
        elif network_boundary:
            _record_network_caller_callback_error()
        else:
            _record_caller_callback_error()
        return None
    if provided is None:
        return None
    caller = _caller_value(provided)
    if caller is None:
        if http_boundary:
            _record_http_caller_callback_error()
        elif logical_operation_boundary:
            _record_logical_operation_caller_callback_error()
        elif network_setup_boundary:
            _record_network_setup_caller_callback_error()
        elif network_boundary:
            _record_network_caller_callback_error()
        else:
            _record_caller_callback_error()
    return caller


def _primitive_text(value: object) -> str | None:
    if type(value) is str:
        return value
    if type(value) is bytes:
        return os.fsdecode(value)
    return None


def _executable_name(
    arguments: object,
    positional: tuple[object, ...],
    keywords: dict[str, object],
) -> tuple[str, bool | None]:
    raw_shell = keywords.get("shell", positional[7] if len(positional) > 7 else False)
    shell = (
        raw_shell
        if type(raw_shell) is bool
        else bool(raw_shell)
        if type(raw_shell) is int
        else None
    )
    if shell is True:
        return "<shell>", True
    if shell is None:
        return "<command>", None
    executable = keywords.get(
        "executable",
        positional[1] if len(positional) > 1 else None,
    )
    candidate: object = executable
    if candidate is None:
        if type(arguments) is list:
            argument_list = cast(list[object], arguments)
            candidate = argument_list[0] if argument_list else arguments
        elif type(arguments) is tuple:
            argument_tuple = arguments
            candidate = argument_tuple[0] if argument_tuple else arguments
        else:
            candidate = arguments
    text = _primitive_text(candidate)
    if not text or any(character.isspace() for character in text):
        return "<command>", shell
    name = os.path.basename(text) or "<unknown>"
    return name[:MAX_EXECUTABLE_CHARACTERS], shell


def _reserve_record(
    arguments: object,
    positional: tuple[object, ...],
    keywords: dict[str, object],
) -> int | None:
    name, shell = _executable_name(arguments, positional, keywords)
    caller = _capture_caller()
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        if len(STATE._records) >= MAX_SUBPROCESS_RECORDS:
            STATE._dropped_subprocess_count = _increment(STATE._dropped_subprocess_count)
            return None
        identifier = STATE._next_identifier
        STATE._next_identifier = _increment(STATE._next_identifier)
        STATE._records[identifier] = {
            "id": identifier,
            "name": name,
            "parent_pid": os.getpid(),
            "child_pid": None,
            "shell": shell,
            "started_at_ns": started_at_ns,
            "_started_monotonic_ns": started_monotonic_ns,
            "duration_ns": None,
            "exit_code": None,
            "outcome": "unknown",
            "caller": caller,
        }
        return identifier


def _record_child(identifier: int | None, process: object) -> None:
    if identifier is None:
        return
    try:
        process_values = object.__getattribute__(process, "__dict__")
    except BaseException:
        return
    child_pid = process_values.get("pid") if isinstance(process_values, dict) else None
    if not isinstance(child_pid, int) or isinstance(child_pid, bool) or child_pid <= 0:
        return
    with STATE._lock:
        record = STATE._records.get(identifier)
        if record is not None:
            record["child_pid"] = child_pid


def _finish_record(
    identifier: int | None,
    *,
    exit_code: int | None,
    outcome: str,
    error_type: str | None = None,
) -> None:
    if identifier is None:
        return
    finished_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        record = STATE._records.get(identifier)
        if record is None or record.get("duration_ns") is not None:
            return
        started_monotonic_ns = record.get("_started_monotonic_ns")
        if not isinstance(started_monotonic_ns, int):
            return
        record["duration_ns"] = max(0, finished_monotonic_ns - started_monotonic_ns)
        record["exit_code"] = exit_code
        record["outcome"] = outcome
        if error_type is not None:
            record["error_type"] = error_type[:MAX_EXECUTABLE_CHARACTERS]


def _associate_record(process: object, identifier: int | None) -> None:
    process_identity = id(process)
    with STATE._lock:
        if identifier is None:
            STATE._process_record_ids.pop(process_identity, None)
        else:
            STATE._process_record_ids[process_identity] = identifier


def _record_identifier(process: object) -> int | None:
    with STATE._lock:
        return STATE._process_record_ids.get(id(process))


def _release_record(process: object) -> None:
    with STATE._lock:
        STATE._process_record_ids.pop(id(process), None)


def _begin_active(process: object) -> ActiveToken | None:
    thread_id = threading.get_ident()
    with STATE._lock:
        identifier = STATE._process_record_ids.get(id(process))
        if identifier is None:
            return None
        key = ("subprocess", identifier)
        STATE._active_record_keys.setdefault(thread_id, []).append(key)
    return thread_id, key


def _end_active(token: ActiveToken | None) -> None:
    if token is None:
        return
    thread_id, key = token
    with STATE._lock:
        active = STATE._active_record_keys.get(thread_id)
        if not active:
            return
        if active[-1] == key:
            active.pop()
        else:
            for index in range(len(active) - 1, -1, -1):
                if active[index] == key:
                    del active[index]
                    break
        if not active:
            STATE._active_record_keys.pop(thread_id, None)


def attribute_active_caller(thread_id: int, caller: CallerIdentity) -> None:
    """Attach one caller observed by the existing sampler to an active wait."""

    with STATE._lock:
        active = STATE._active_record_keys.get(thread_id)
        active_key = active[-1] if active else None
    if active_key is None:
        return
    value = _caller_value(caller)
    if value is None:
        if active_key[0] == "http":
            _record_http_caller_callback_error()
        elif active_key[0] == "logical_operation":
            _record_logical_operation_caller_callback_error()
        elif active_key[0] == "network_setup":
            _record_network_setup_caller_callback_error()
        elif active_key[0] == "network":
            _record_network_caller_callback_error()
        else:
            _record_caller_callback_error()
        return
    with STATE._lock:
        active = STATE._active_record_keys.get(thread_id)
        if not active:
            return
        kind, identifier = active[-1]
        record = (
            STATE._records.get(identifier)
            if kind == "subprocess"
            else STATE._http_records.get(identifier)
            if kind == "http"
            else STATE._network_records.get(identifier)
            if kind == "network"
            else STATE._network_setup_records.get(identifier)
            if kind == "network_setup"
            else STATE._logical_operation_records.get(identifier)
            if kind == "logical_operation"
            else None
        )
        if record is not None and record.get("caller") is None:
            record["caller"] = value


def attribute_logical_operation_caller(
    identifier: int | None,
    caller: CallerIdentity,
) -> None:
    """Attach one exact caller directly to a concurrent logical operation."""

    if identifier is None:
        return
    value = _caller_value(caller)
    if value is None:
        _record_logical_operation_caller_callback_error()
        return
    with STATE._lock:
        record = STATE._logical_operation_records.get(identifier)
        if record is not None and record.get("caller") is None:
            record["caller"] = value
