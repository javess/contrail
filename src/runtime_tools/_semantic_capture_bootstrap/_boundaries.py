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

import contextvars
import functools
import http.client
import os
import socket
import threading
import time
from collections.abc import Awaitable, Callable
from typing import cast

from ._constants import (
    HTTP_ADAPTERS,
    LOGICAL_OPERATION_ADAPTER_CATEGORIES,
    LOGICAL_OPERATION_ADAPTERS,
    MAX_EXECUTABLE_CHARACTERS,
    MAX_HTTP_METHOD_CHARACTERS,
    MAX_HTTP_REQUEST_RECORDS,
    MAX_LOGICAL_OPERATION_RECORDS,
    MAX_NETWORK_CONNECTION_RECORDS,
    MAX_NETWORK_SETUP_RECORDS,
    NETWORK_ADAPTERS,
    NETWORK_SETUP_ADAPTERS,
    ActiveToken,
)
from ._records import (
    _capture_caller,
    _end_active,
    _increment,
    _record_logical_operation_callback_error,
    _record_logical_operation_caller_callback_error,
)
from ._state import STATE


def _http_method(value: object) -> str:
    if type(value) is bytes:
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return "<method>"
    if type(value) is not str:
        return "<method>"
    method = value
    if (
        not 0 < len(method) <= MAX_HTTP_METHOD_CHARACTERS
        or not method.isascii()
        or not all(character in "ABCDEFGHIJKLMNOPQRSTUVWXYZ-" for character in method)
    ):
        return "<method>"
    return method


def _http_connection_values(connection: object) -> dict[object, object]:
    try:
        values = object.__getattribute__(connection, "__dict__")
    except BaseException:
        return {}
    return cast(dict[object, object], values) if type(values) is dict else {}


def _http_scheme(value: object) -> str | None:
    if type(value) is bytes:
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return None
    return value if type(value) is str and value in {"http", "https"} else None


def _http_port(value: object, scheme: str) -> int | None:
    if type(value) is int and 0 < value <= 65_535:
        return value
    if value is None:
        return 443 if scheme == "https" else 80
    return None


def _reserve_http_boundary(
    method: object,
    *,
    scheme: str,
    server_port: object,
    adapter: str,
) -> int | None:
    if scheme not in {"http", "https"} or adapter not in HTTP_ADAPTERS:
        return None
    caller = _capture_caller(http_boundary=True)
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        if len(STATE._http_records) >= MAX_HTTP_REQUEST_RECORDS:
            STATE._dropped_http_request_count = _increment(STATE._dropped_http_request_count)
            return None
        identifier = STATE._next_http_identifier
        STATE._next_http_identifier = _increment(STATE._next_http_identifier)
        STATE._http_records[identifier] = {
            "id": identifier,
            "method": _http_method(method),
            "scheme": scheme,
            "server_address": None,
            "server_port": _http_port(server_port, scheme),
            "server_identity_policy": "redact",
            "adapter": adapter,
            "parent_pid": os.getpid(),
            "started_at_ns": started_at_ns,
            "_started_monotonic_ns": started_monotonic_ns,
            "duration_ns": None,
            "status_code": None,
            "outcome": "unknown",
            "caller": caller,
        }
        return identifier


def _reserve_http_record(connection: object, method: object) -> int | None:
    connection_values = _http_connection_values(connection)
    raw_port = connection_values.get("port")
    scheme = "https" if isinstance(connection, http.client.HTTPSConnection) else "http"
    connection_identity = id(connection)
    with STATE._lock:
        previous = STATE._connection_record_ids.get(connection_identity)
    if previous is not None:
        _finish_http_record(previous, status_code=None, outcome="closed")
    identifier = _reserve_http_boundary(
        method,
        scheme=scheme,
        server_port=raw_port,
        adapter="stdlib.http.client",
    )
    with STATE._lock:
        if identifier is None:
            STATE._connection_record_ids.pop(connection_identity, None)
        else:
            STATE._connection_record_ids[connection_identity] = identifier
    return identifier


def _http_record_identifier(connection: object) -> int | None:
    with STATE._lock:
        return STATE._connection_record_ids.get(id(connection))


def _release_http_record(connection: object) -> None:
    with STATE._lock:
        STATE._connection_record_ids.pop(id(connection), None)


def _finish_http_record(
    identifier: int | None,
    *,
    status_code: int | None,
    outcome: str,
    error_type: str | None = None,
) -> None:
    if identifier is None:
        return
    finished_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        record = STATE._http_records.get(identifier)
        if record is None:
            return
        already_finished = record.get("duration_ns") is not None
        if already_finished and not (
            record.get("outcome") == "closed" and outcome in {"request_error", "response"}
        ):
            return
        if not already_finished:
            started_monotonic_ns = record.get("_started_monotonic_ns")
            if not isinstance(started_monotonic_ns, int):
                return
            record["duration_ns"] = max(0, finished_monotonic_ns - started_monotonic_ns)
        record["status_code"] = status_code
        record["outcome"] = outcome
        if error_type is not None:
            record["error_type"] = error_type[:MAX_EXECUTABLE_CHARACTERS]


def _begin_active_http(connection: object) -> ActiveToken | None:
    with STATE._lock:
        identifier = STATE._connection_record_ids.get(id(connection))
    return _begin_active_identifier("http", identifier)


def _begin_active_identifier(family: str, identifier: int | None) -> ActiveToken | None:
    if identifier is None:
        return None
    thread_id = threading.get_ident()
    with STATE._lock:
        key = (family, identifier)
        STATE._active_record_keys.setdefault(thread_id, []).append(key)
    return thread_id, key


def _http_status(response: object) -> int | None:
    values = _http_connection_values(response)
    status = values.get("status")
    if type(status) is int and 100 <= status <= 999:
        return status
    return None


def _network_family(value: object) -> str:
    if not isinstance(value, int) or isinstance(value, bool):
        return "unknown"
    if value == socket.AF_INET:
        return "ipv4"
    if value == socket.AF_INET6:
        return "ipv6"
    if hasattr(socket, "AF_UNIX") and value == socket.AF_UNIX:
        return "unix"
    return "unknown"


def _network_target(
    connection: object,
    address: object,
) -> tuple[str, str, int | None] | None:
    if not isinstance(connection, socket.socket):
        return None
    try:
        timeout = socket.socket.gettimeout(connection)
        family = object.__getattribute__(connection, "family")
        socket_type = object.__getattribute__(connection, "type")
    except BaseException:
        return None
    if timeout == 0.0 or not isinstance(socket_type, int) or isinstance(socket_type, bool):
        return None
    if socket_type & socket.SOCK_STREAM != socket.SOCK_STREAM:
        return None
    family_name = _network_family(family)
    if family_name == "unix":
        return "unix", family_name, None
    if family_name not in {"ipv4", "ipv6"}:
        return None
    server_port: int | None = None
    if type(address) is tuple and len(address) >= 2:
        raw_port = address[1]
        if type(raw_port) is int and 0 < raw_port <= 65_535:
            server_port = raw_port
    return "tcp", family_name, server_port


def _reserve_network_boundary(
    *,
    transport: str,
    family: str,
    server_port: int | None,
    adapter: str,
    tls_requested: bool | None,
) -> int | None:
    if (
        transport not in {"tcp", "unix"}
        or family not in {"ipv4", "ipv6", "unix", "unknown"}
        or adapter not in NETWORK_ADAPTERS
        or (server_port is not None and not 0 < server_port <= 65_535)
    ):
        return None
    caller = _capture_caller(network_boundary=True)
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        if len(STATE._network_records) >= MAX_NETWORK_CONNECTION_RECORDS:
            STATE._dropped_network_connection_count = _increment(
                STATE._dropped_network_connection_count
            )
            return None
        identifier = STATE._next_network_identifier
        STATE._next_network_identifier = _increment(STATE._next_network_identifier)
        STATE._network_records[identifier] = {
            "id": identifier,
            "adapter": adapter,
            "transport": transport,
            "address_family": family,
            "server_address": None,
            "server_port": server_port,
            "server_identity_policy": "redact",
            "tls_requested": tls_requested,
            "parent_pid": os.getpid(),
            "started_at_ns": started_at_ns,
            "_started_monotonic_ns": started_monotonic_ns,
            "duration_ns": None,
            "outcome": "unknown",
            "caller": caller,
        }
        return identifier


def _finish_timed_record(
    records: dict[int, dict[str, object]],
    identifier: int | None,
    *,
    outcome: str,
    error_type: str | None = None,
) -> None:
    if identifier is None:
        return
    finished_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        record = records.get(identifier)
        if record is None or record.get("duration_ns") is not None:
            return
        started_monotonic_ns = record.get("_started_monotonic_ns")
        if not isinstance(started_monotonic_ns, int):
            return
        record["duration_ns"] = max(0, finished_monotonic_ns - started_monotonic_ns)
        record["outcome"] = outcome
        if error_type is not None:
            record["error_type"] = error_type[:MAX_EXECUTABLE_CHARACTERS]


def _finish_network_record(
    identifier: int | None,
    *,
    outcome: str,
    error_type: str | None = None,
) -> None:
    _finish_timed_record(STATE._network_records, identifier, outcome=outcome, error_type=error_type)


def _reserve_network_setup_boundary(*, phase: str, adapter: str) -> int | None:
    if phase not in {"dns", "tls"} or adapter not in NETWORK_SETUP_ADAPTERS:
        return None
    caller = _capture_caller(network_setup_boundary=True)
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        if len(STATE._network_setup_records) >= MAX_NETWORK_SETUP_RECORDS:
            STATE._dropped_network_setup_count = _increment(STATE._dropped_network_setup_count)
            return None
        identifier = STATE._next_network_setup_identifier
        STATE._next_network_setup_identifier = _increment(STATE._next_network_setup_identifier)
        STATE._network_setup_records[identifier] = {
            "id": identifier,
            "phase": phase,
            "adapter": adapter,
            "parent_pid": os.getpid(),
            "started_at_ns": started_at_ns,
            "_started_monotonic_ns": started_monotonic_ns,
            "duration_ns": None,
            "outcome": "unknown",
            "caller": caller,
        }
        return identifier


def _finish_network_setup_record(
    identifier: int | None,
    *,
    outcome: str,
    error_type: str | None = None,
) -> None:
    _finish_timed_record(
        STATE._network_setup_records, identifier, outcome=outcome, error_type=error_type
    )


def _reserve_logical_operation_boundary(
    *,
    category: str,
    operation: str,
    adapter: str,
    capture_caller: bool = True,
) -> int | None:
    valid_operations = {
        "broker": {"consume", "publish"},
        "cache": {"batch", "command"},
        "database": {"commit", "execute", "executemany", "executescript", "rollback"},
        "executor": {"task"},
        "queue": {"get", "put"},
        "scheduler": {"task"},
        "server": {"request"},
    }
    if (
        operation not in valid_operations.get(category, set())
        or LOGICAL_OPERATION_ADAPTER_CATEGORIES.get(adapter) != category
    ):
        return None
    caller = _capture_caller(logical_operation_boundary=True) if capture_caller else None
    if category == "queue" and caller is not None and caller.get("scope") != "application":
        return None
    if category == "scheduler" and (caller is None or caller.get("scope") != "application"):
        return None
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with STATE._lock:
        if len(STATE._logical_operation_records) >= MAX_LOGICAL_OPERATION_RECORDS:
            STATE._dropped_logical_operation_count = _increment(
                STATE._dropped_logical_operation_count
            )
            return None
        identifier = STATE._next_logical_operation_identifier
        STATE._next_logical_operation_identifier = _increment(
            STATE._next_logical_operation_identifier
        )
        STATE._logical_operation_records[identifier] = {
            "id": identifier,
            "category": category,
            "operation": operation,
            "adapter": adapter,
            "parent_pid": os.getpid(),
            "started_at_ns": started_at_ns,
            "_started_monotonic_ns": started_monotonic_ns,
            "duration_ns": None,
            "outcome": "unknown",
            "caller": caller,
        }
        return identifier


def _finish_logical_operation_record(
    identifier: int | None,
    *,
    outcome: str,
    error_type: str | None = None,
) -> None:
    _finish_timed_record(
        STATE._logical_operation_records, identifier, outcome=outcome, error_type=error_type
    )


def _run_logical_operation(
    original: Callable[..., object],
    instance: object,
    positional: tuple[object, ...],
    keywords: dict[str, object],
    *,
    category: str,
    operation: str,
    adapter: str,
    sampled_caller: bool,
) -> object:
    if STATE._logical_operation_capture_suppressed.get():
        return original(instance, *positional, **keywords)
    identifier: int | None = None
    active_token: ActiveToken | None = None
    suppression_token: contextvars.Token[bool] | None = None
    try:
        suppression_token = STATE._logical_operation_capture_suppressed.set(True)
        identifier = _reserve_logical_operation_boundary(
            category=category,
            operation=operation,
            adapter=adapter,
        )
        if sampled_caller:
            active_token = _begin_active_identifier("logical_operation", identifier)
    except BaseException:
        _record_logical_operation_callback_error()
    try:
        result = original(instance, *positional, **keywords)
    except BaseException as error:
        try:
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type=type(error).__name__,
            )
        except BaseException:
            _record_logical_operation_callback_error()
        raise
    finally:
        try:
            _end_active(active_token)
        except BaseException:
            _record_logical_operation_caller_callback_error()
        if suppression_token is not None:
            try:
                STATE._logical_operation_capture_suppressed.reset(suppression_token)
            except BaseException:
                _record_logical_operation_caller_callback_error()
    try:
        _finish_logical_operation_record(identifier, outcome="completed")
    except BaseException:
        _record_logical_operation_callback_error()
    return result


async def _run_async_logical_operation(
    original: Callable[..., Awaitable[object]],
    instance: object,
    positional: tuple[object, ...],
    keywords: dict[str, object],
    *,
    category: str,
    operation: str,
    adapter: str,
) -> object:
    if STATE._logical_operation_capture_suppressed.get():
        return await original(instance, *positional, **keywords)
    identifier: int | None = None
    suppression_token: contextvars.Token[bool] | None = None
    try:
        suppression_token = STATE._logical_operation_capture_suppressed.set(True)
        identifier = _reserve_logical_operation_boundary(
            category=category,
            operation=operation,
            adapter=adapter,
        )
    except BaseException:
        _record_logical_operation_callback_error()
    try:
        result = await original(instance, *positional, **keywords)
    except BaseException as error:
        try:
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type=type(error).__name__,
            )
        except BaseException:
            _record_logical_operation_callback_error()
        raise
    finally:
        if suppression_token is not None:
            try:
                STATE._logical_operation_capture_suppressed.reset(suppression_token)
            except BaseException:
                _record_logical_operation_caller_callback_error()
    try:
        _finish_logical_operation_record(identifier, outcome="completed")
    except BaseException:
        _record_logical_operation_callback_error()
    return result


def _finish_executor_future(identifier: int | None, future: object) -> None:
    try:
        cancelled = getattr(future, "cancelled", None)
        exception = getattr(future, "exception", None)
        if not callable(cancelled) or not callable(exception):
            _record_logical_operation_callback_error()
            return
        if cancelled():
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type="CancelledError",
            )
            return
        error = exception()
        if error is None:
            _finish_logical_operation_record(identifier, outcome="completed")
        else:
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type=type(error).__name__,
            )
    except BaseException:
        _record_logical_operation_callback_error()


def _run_executor_submit(
    original: Callable[..., object],
    instance: object,
    positional: tuple[object, ...],
    keywords: dict[str, object],
    *,
    adapter: str,
) -> object:
    if STATE._logical_operation_capture_suppressed.get():
        return original(instance, *positional, **keywords)
    identifier: int | None = None
    suppression_token: contextvars.Token[bool] | None = None
    try:
        suppression_token = STATE._logical_operation_capture_suppressed.set(True)
        identifier = _reserve_logical_operation_boundary(
            category="executor",
            operation="task",
            adapter=adapter,
        )
    except BaseException:
        _record_logical_operation_callback_error()
    try:
        future = original(instance, *positional, **keywords)
    except BaseException as error:
        try:
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type=type(error).__name__,
            )
        except BaseException:
            _record_logical_operation_callback_error()
        raise
    finally:
        if suppression_token is not None:
            try:
                STATE._logical_operation_capture_suppressed.reset(suppression_token)
            except BaseException:
                _record_logical_operation_caller_callback_error()
    try:
        add_done_callback = getattr(future, "add_done_callback", None)
        if not callable(add_done_callback):
            _record_logical_operation_callback_error()
        else:
            add_done_callback(
                functools.partial(_finish_executor_future, identifier),
            )
    except BaseException:
        _record_logical_operation_callback_error()
    return future


def _finish_scheduled_task(identifier: int | None, task: object) -> None:
    try:
        cancelled = getattr(task, "cancelled", None)
        if not callable(cancelled):
            _record_logical_operation_callback_error()
            return
        if cancelled():
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type="CancelledError",
            )
            return
        error = object.__getattribute__(task, "_exception")
        if error is None:
            _finish_logical_operation_record(identifier, outcome="completed")
        elif isinstance(error, BaseException):
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type=type(error).__name__,
            )
        else:
            _record_logical_operation_callback_error()
    except BaseException:
        _record_logical_operation_callback_error()


def _run_asyncio_create_task(
    original: Callable[..., object],
    positional: tuple[object, ...],
    keywords: dict[str, object],
    *,
    adapter: str,
) -> object:
    if STATE._logical_operation_capture_suppressed.get():
        return original(*positional, **keywords)
    identifier: int | None = None
    try:
        identifier = _reserve_logical_operation_boundary(
            category="scheduler",
            operation="task",
            adapter=adapter,
        )
    except BaseException:
        _record_logical_operation_callback_error()
    try:
        task = original(*positional, **keywords)
    except BaseException as error:
        try:
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type=type(error).__name__,
            )
        except BaseException:
            _record_logical_operation_callback_error()
        raise
    if identifier is None:
        return task
    try:
        add_done_callback = getattr(task, "add_done_callback", None)
        if not callable(add_done_callback):
            _record_logical_operation_callback_error()
        else:
            add_done_callback(functools.partial(_finish_scheduled_task, identifier))
    except BaseException:
        _record_logical_operation_callback_error()
    return task


def _begin_server_request(
    adapter: str,
    *,
    active: bool,
) -> tuple[int | None, ActiveToken | None]:
    identifier: int | None = None
    token: ActiveToken | None = None
    try:
        identifier = _reserve_logical_operation_boundary(
            category="server",
            operation="request",
            adapter=adapter,
            capture_caller=False,
        )
        if active:
            token = _begin_active_identifier("logical_operation", identifier)
        with STATE._lock:
            if adapter in LOGICAL_OPERATION_ADAPTERS:
                STATE._logical_operation_adapters.add(adapter)
    except BaseException:
        _record_logical_operation_callback_error()
    return identifier, token


def begin_wsgi_request() -> tuple[int | None, ActiveToken | None]:
    """Start one Deep-only standard WSGI request boundary."""

    return _begin_server_request("stdlib.wsgiref", active=True)


def begin_asgi_request(adapter: str) -> int | None:
    """Start one Deep-only Uvicorn HTTP request boundary."""

    identifier, _ = _begin_server_request(adapter, active=False)
    return identifier


def _record_server_status(identifier: int | None, status_code: object) -> None:
    if (
        identifier is None
        or not isinstance(status_code, int)
        or isinstance(status_code, bool)
        or not 100 <= status_code <= 999
    ):
        return
    with STATE._lock:
        record = STATE._logical_operation_records.get(identifier)
        if record is not None and record.get("duration_ns") is None:
            record["status_code"] = status_code


def record_wsgi_status(identifier: int | None, status: object) -> None:
    """Retain only the numeric status from a WSGI ``start_response`` call."""

    if identifier is None or type(status) is not str:
        return
    status_text = status
    if len(status_text) < 4 or status_text[3] != " " or not status_text[:3].isdigit():
        return
    status_code = int(status_text[:3])
    if not 100 <= status_code <= 999:
        return
    try:
        _record_server_status(identifier, status_code)
    except BaseException:
        _record_logical_operation_callback_error()


def record_asgi_status(identifier: int | None, status: object) -> None:
    """Retain only the numeric status from an ASGI response-start message."""

    try:
        _record_server_status(identifier, status)
    except BaseException:
        _record_logical_operation_callback_error()


def _finish_server_request(identifier: int | None) -> None:
    status_code: int | None = None
    with STATE._lock:
        record = (
            STATE._logical_operation_records.get(identifier) if identifier is not None else None
        )
        raw_status_code = record.get("status_code") if record is not None else None
        if isinstance(raw_status_code, int) and not isinstance(raw_status_code, bool):
            status_code = raw_status_code
    _finish_logical_operation_record(
        identifier,
        outcome=(
            "operation_error" if status_code is not None and status_code >= 500 else "completed"
        ),
        error_type=("HTTPStatusError" if status_code is not None and status_code >= 500 else None),
    )


def finish_wsgi_request(identifier: int | None, token: ActiveToken | None) -> None:
    """Finish one WSGI request without inspecting its environment or body."""

    try:
        _finish_server_request(identifier)
    except BaseException:
        _record_logical_operation_callback_error()
    finally:
        try:
            _end_active(token)
        except BaseException:
            _record_logical_operation_caller_callback_error()


def finish_asgi_request(identifier: int | None) -> None:
    """Finish one ASGI HTTP request without retaining its message or scope."""

    try:
        _finish_server_request(identifier)
    except BaseException:
        _record_logical_operation_callback_error()
