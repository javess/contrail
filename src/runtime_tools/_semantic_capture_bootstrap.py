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
import importlib.abc
import importlib.machinery
import os
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.parse
from collections.abc import Awaitable, Callable, Sequence
from types import ModuleType
from typing import cast

FORMAT_VERSION = 2
MAX_SUBPROCESS_RECORDS = 256
MAX_HTTP_REQUEST_RECORDS = 256
MAX_NETWORK_CONNECTION_RECORDS = 256
MAX_NETWORK_SETUP_RECORDS = 256
MAX_LOGICAL_OPERATION_RECORDS = 256
MAX_EXECUTABLE_CHARACTERS = 256
MAX_HTTP_METHOD_CHARACTERS = 32
MAX_CALLER_TEXT_CHARACTERS = 1_024
MAX_COUNTER = (1 << 63) - 1
_LOGICAL_WRAPPER_MARKER = "__contrail_logical_operation_wrapper__"
_ASYNCIO_ENSURE_FUTURE_ENGINE_MARKER = "__contrail_ensure_future_engine_wrapper__"
_ASYNCIO_ENSURE_FUTURE_ENTRY_MARKER = "__contrail_ensure_future_entry_wrapper__"
HTTP_ADAPTERS = frozenset(
    {
        "stdlib.http.client",
        "httpcore.sync",
        "httpcore.async",
        "aiohttp.async",
    }
)
OPTIONAL_HTTP_MODULES = frozenset({"httpcore", "aiohttp.client"})
NETWORK_ADAPTERS = frozenset(
    {
        "stdlib.socket.connect",
        "asyncio.create_connection",
        "asyncio.create_unix_connection",
    }
)
NETWORK_SETUP_ADAPTERS = frozenset(
    {
        "stdlib.socket.getaddrinfo",
        "stdlib.ssl.SSLObject.do_handshake",
        "stdlib.ssl.SSLSocket.do_handshake",
    }
)
LOGICAL_OPERATION_ADAPTER_CATEGORIES = {
    "stdlib.asyncio.TaskGroup": "scheduler",
    "stdlib.asyncio.create_task": "scheduler",
    "stdlib.asyncio.ensure_future": "scheduler",
    "stdlib.asyncio.gather": "scheduler",
    "stdlib.wsgiref": "server",
    "uvicorn.h11": "server",
    "uvicorn.httptools": "server",
    "stdlib.concurrent.futures.ProcessPoolExecutor": "executor",
    "stdlib.concurrent.futures.ThreadPoolExecutor": "executor",
    "stdlib.asyncio.Queue": "queue",
    "stdlib.queue.Queue": "queue",
    "stdlib.sqlite3.Connection": "database",
    "stdlib.sqlite3.Cursor": "database",
    "sqlalchemy.engine.Connection": "database",
    "sqlalchemy.orm.Session": "database",
    "sqlalchemy.ext.asyncio.AsyncConnection": "database",
    "sqlalchemy.ext.asyncio.AsyncSession": "database",
    "redis.Redis": "cache",
    "redis.Pipeline": "cache",
    "redis.asyncio.Redis": "cache",
    "redis.asyncio.Pipeline": "cache",
    "pika.BlockingChannel": "broker",
    "aiokafka.AIOKafkaProducer": "broker",
    "aiokafka.AIOKafkaConsumer": "broker",
}
LOGICAL_OPERATION_ADAPTERS = frozenset(LOGICAL_OPERATION_ADAPTER_CATEGORIES)
OPTIONAL_LOGICAL_OPERATION_MODULES = frozenset(
    {
        "aiokafka.consumer.consumer",
        "aiokafka.producer.producer",
        "concurrent.futures.process",
        "concurrent.futures.thread",
        "pika.adapters.blocking_connection",
        "redis.asyncio.client",
        "redis.client",
        "sqlalchemy.engine.base",
        "sqlalchemy.ext.asyncio.engine",
        "sqlalchemy.ext.asyncio.session",
        "sqlalchemy.orm.session",
    }
)
STDLIB_LOGICAL_OPERATION_MODULES = frozenset(
    {
        "asyncio",
        "asyncio.queues",
        "asyncio.taskgroups",
        "asyncio.tasks",
        "queue",
        "sqlite3",
        "sqlite3.dbapi2",
    }
)
LOGICAL_OPERATION_MODULES = OPTIONAL_LOGICAL_OPERATION_MODULES | STDLIB_LOGICAL_OPERATION_MODULES
LAZY_ADAPTER_MODULES = (
    OPTIONAL_HTTP_MODULES
    | LOGICAL_OPERATION_MODULES
    | {
        "asyncio.base_events",
        "asyncio.unix_events",
    }
)

type CallerIdentity = tuple[str, str, str, int, str, str]
type CallerProvider = Callable[[], CallerIdentity | None]
type ActiveRecordKey = tuple[str, int]
type ActiveToken = tuple[int, ActiveRecordKey]
type NetworkSuppressionToken = contextvars.Token[bool]

_original_init = cast(Callable[..., None], subprocess.Popen.__init__)
_original_wait = cast(Callable[..., int], subprocess.Popen.wait)
_original_poll = cast(Callable[..., int | None], subprocess.Popen.poll)
_original_communicate = cast(Callable[..., object], subprocess.Popen.communicate)
_original_http_putrequest = cast(Callable[..., None], http.client.HTTPConnection.putrequest)
_original_http_endheaders = cast(Callable[..., None], http.client.HTTPConnection.endheaders)
_original_http_getresponse = cast(
    Callable[..., http.client.HTTPResponse],
    http.client.HTTPConnection.getresponse,
)
_original_http_close = cast(Callable[..., None], http.client.HTTPConnection.close)
_original_socket_connect = cast(Callable[..., None], socket.socket.connect)
_original_getaddrinfo = cast(Callable[..., object], socket.getaddrinfo)
_original_ssl_object_handshake = cast(Callable[..., None], ssl.SSLObject.do_handshake)
_original_ssl_socket_handshake = cast(Callable[..., None], ssl.SSLSocket.do_handshake)
_lock = threading.RLock()
_records: dict[int, dict[str, object]] = {}
_process_record_ids: dict[int, int] = {}
_http_records: dict[int, dict[str, object]] = {}
_network_records: dict[int, dict[str, object]] = {}
_network_setup_records: dict[int, dict[str, object]] = {}
_logical_operation_records: dict[int, dict[str, object]] = {}
_tls_record_ids: dict[int, tuple[object, int | None]] = {}
_connection_record_ids: dict[int, int] = {}
_active_record_keys: dict[int, list[ActiveRecordKey]] = {}
_caller_provider: CallerProvider | None = None
_next_identifier = 0
_next_http_identifier = 0
_next_network_identifier = 0
_next_network_setup_identifier = 0
_next_logical_operation_identifier = 0
_dropped_subprocess_count = 0
_dropped_http_request_count = 0
_dropped_network_connection_count = 0
_dropped_network_setup_count = 0
_dropped_logical_operation_count = 0
_callback_error_count = 0
_caller_callback_error_count = 0
_http_callback_error_count = 0
_http_caller_callback_error_count = 0
_network_callback_error_count = 0
_network_caller_callback_error_count = 0
_network_setup_callback_error_count = 0
_network_setup_caller_callback_error_count = 0
_logical_operation_callback_error_count = 0
_logical_operation_caller_callback_error_count = 0
_http_adapters = {"stdlib.http.client"}
_network_adapters = {"stdlib.socket.connect"}
_network_setup_adapters: set[str] = set()
_logical_operation_adapters: set[str] = set()
_logical_operation_capture_enabled = False
_network_capture_suppressed = contextvars.ContextVar(
    "contrail_network_capture_suppressed",
    default=False,
)
_logical_operation_capture_suppressed = contextvars.ContextVar(
    "contrail_logical_operation_capture_suppressed",
    default=False,
)
_asyncio_task_scheduling_state = threading.local()
_active = False


def _increment(value: int) -> int:
    return min(MAX_COUNTER, value + 1)


def _record_callback_error() -> None:
    global _callback_error_count
    try:
        with _lock:
            _callback_error_count = _increment(_callback_error_count)
    except BaseException:
        pass


def _record_caller_callback_error() -> None:
    global _caller_callback_error_count
    try:
        with _lock:
            _caller_callback_error_count = _increment(_caller_callback_error_count)
    except BaseException:
        pass


def _record_http_callback_error() -> None:
    global _http_callback_error_count
    try:
        with _lock:
            _http_callback_error_count = _increment(_http_callback_error_count)
    except BaseException:
        pass


def _record_http_caller_callback_error() -> None:
    global _http_caller_callback_error_count
    try:
        with _lock:
            _http_caller_callback_error_count = _increment(_http_caller_callback_error_count)
    except BaseException:
        pass


def _record_network_callback_error() -> None:
    global _network_callback_error_count
    try:
        with _lock:
            _network_callback_error_count = _increment(_network_callback_error_count)
    except BaseException:
        pass


def _record_network_caller_callback_error() -> None:
    global _network_caller_callback_error_count
    try:
        with _lock:
            _network_caller_callback_error_count = _increment(_network_caller_callback_error_count)
    except BaseException:
        pass


def _record_network_setup_callback_error() -> None:
    global _network_setup_callback_error_count
    try:
        with _lock:
            _network_setup_callback_error_count = _increment(_network_setup_callback_error_count)
    except BaseException:
        pass


def _record_network_setup_caller_callback_error() -> None:
    global _network_setup_caller_callback_error_count
    try:
        with _lock:
            _network_setup_caller_callback_error_count = _increment(
                _network_setup_caller_callback_error_count
            )
    except BaseException:
        pass


def _record_logical_operation_callback_error() -> None:
    global _logical_operation_callback_error_count
    try:
        with _lock:
            _logical_operation_callback_error_count = _increment(
                _logical_operation_callback_error_count
            )
    except BaseException:
        pass


def _record_logical_operation_caller_callback_error() -> None:
    global _logical_operation_caller_callback_error_count
    try:
        with _lock:
            _logical_operation_caller_callback_error_count = _increment(
                _logical_operation_caller_callback_error_count
            )
    except BaseException:
        pass


def _begin_network_suppression() -> NetworkSuppressionToken | None:
    if _network_capture_suppressed.get():
        return None
    return _network_capture_suppressed.set(True)


def _end_network_suppression(token: NetworkSuppressionToken | None) -> None:
    if token is not None:
        _network_capture_suppressed.reset(token)


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
    provider = _caller_provider
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
    global _dropped_subprocess_count, _next_identifier
    name, shell = _executable_name(arguments, positional, keywords)
    caller = _capture_caller()
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with _lock:
        if len(_records) >= MAX_SUBPROCESS_RECORDS:
            _dropped_subprocess_count = _increment(_dropped_subprocess_count)
            return None
        identifier = _next_identifier
        _next_identifier = _increment(_next_identifier)
        _records[identifier] = {
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
    with _lock:
        record = _records.get(identifier)
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
    with _lock:
        record = _records.get(identifier)
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
    with _lock:
        if identifier is None:
            _process_record_ids.pop(process_identity, None)
        else:
            _process_record_ids[process_identity] = identifier


def _record_identifier(process: object) -> int | None:
    with _lock:
        return _process_record_ids.get(id(process))


def _release_record(process: object) -> None:
    with _lock:
        _process_record_ids.pop(id(process), None)


def _begin_active(process: object) -> ActiveToken | None:
    thread_id = threading.get_ident()
    with _lock:
        identifier = _process_record_ids.get(id(process))
        if identifier is None:
            return None
        key = ("subprocess", identifier)
        _active_record_keys.setdefault(thread_id, []).append(key)
    return thread_id, key


def _end_active(token: ActiveToken | None) -> None:
    if token is None:
        return
    thread_id, key = token
    with _lock:
        active = _active_record_keys.get(thread_id)
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
            _active_record_keys.pop(thread_id, None)


def attribute_active_caller(thread_id: int, caller: CallerIdentity) -> None:
    """Attach one caller observed by the existing sampler to an active wait."""

    with _lock:
        active = _active_record_keys.get(thread_id)
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
    with _lock:
        active = _active_record_keys.get(thread_id)
        if not active:
            return
        kind, identifier = active[-1]
        record = (
            _records.get(identifier)
            if kind == "subprocess"
            else _http_records.get(identifier)
            if kind == "http"
            else _network_records.get(identifier)
            if kind == "network"
            else _network_setup_records.get(identifier)
            if kind == "network_setup"
            else _logical_operation_records.get(identifier)
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
    with _lock:
        record = _logical_operation_records.get(identifier)
        if record is not None and record.get("caller") is None:
            record["caller"] = value


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
    global _dropped_http_request_count, _next_http_identifier
    if scheme not in {"http", "https"} or adapter not in HTTP_ADAPTERS:
        return None
    caller = _capture_caller(http_boundary=True)
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with _lock:
        if len(_http_records) >= MAX_HTTP_REQUEST_RECORDS:
            _dropped_http_request_count = _increment(_dropped_http_request_count)
            return None
        identifier = _next_http_identifier
        _next_http_identifier = _increment(_next_http_identifier)
        _http_records[identifier] = {
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
    with _lock:
        previous = _connection_record_ids.get(connection_identity)
    if previous is not None:
        _finish_http_record(previous, status_code=None, outcome="closed")
    identifier = _reserve_http_boundary(
        method,
        scheme=scheme,
        server_port=raw_port,
        adapter="stdlib.http.client",
    )
    with _lock:
        if identifier is None:
            _connection_record_ids.pop(connection_identity, None)
        else:
            _connection_record_ids[connection_identity] = identifier
    return identifier


def _http_record_identifier(connection: object) -> int | None:
    with _lock:
        return _connection_record_ids.get(id(connection))


def _release_http_record(connection: object) -> None:
    with _lock:
        _connection_record_ids.pop(id(connection), None)


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
    with _lock:
        record = _http_records.get(identifier)
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
    with _lock:
        identifier = _connection_record_ids.get(id(connection))
    return _begin_active_http_identifier(identifier)


def _begin_active_http_identifier(identifier: int | None) -> ActiveToken | None:
    if identifier is None:
        return None
    thread_id = threading.get_ident()
    with _lock:
        key = ("http", identifier)
        _active_record_keys.setdefault(thread_id, []).append(key)
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
    global _dropped_network_connection_count, _next_network_identifier
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
    with _lock:
        if len(_network_records) >= MAX_NETWORK_CONNECTION_RECORDS:
            _dropped_network_connection_count = _increment(_dropped_network_connection_count)
            return None
        identifier = _next_network_identifier
        _next_network_identifier = _increment(_next_network_identifier)
        _network_records[identifier] = {
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


def _finish_network_record(
    identifier: int | None,
    *,
    outcome: str,
    error_type: str | None = None,
) -> None:
    if identifier is None:
        return
    finished_monotonic_ns = time.perf_counter_ns()
    with _lock:
        record = _network_records.get(identifier)
        if record is None or record.get("duration_ns") is not None:
            return
        started_monotonic_ns = record.get("_started_monotonic_ns")
        if not isinstance(started_monotonic_ns, int):
            return
        record["duration_ns"] = max(0, finished_monotonic_ns - started_monotonic_ns)
        record["outcome"] = outcome
        if error_type is not None:
            record["error_type"] = error_type[:MAX_EXECUTABLE_CHARACTERS]


def _begin_active_network_identifier(identifier: int | None) -> ActiveToken | None:
    if identifier is None:
        return None
    thread_id = threading.get_ident()
    with _lock:
        key = ("network", identifier)
        _active_record_keys.setdefault(thread_id, []).append(key)
    return thread_id, key


def _reserve_network_setup_boundary(*, phase: str, adapter: str) -> int | None:
    global _dropped_network_setup_count, _next_network_setup_identifier
    if phase not in {"dns", "tls"} or adapter not in NETWORK_SETUP_ADAPTERS:
        return None
    caller = _capture_caller(network_setup_boundary=True)
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    with _lock:
        if len(_network_setup_records) >= MAX_NETWORK_SETUP_RECORDS:
            _dropped_network_setup_count = _increment(_dropped_network_setup_count)
            return None
        identifier = _next_network_setup_identifier
        _next_network_setup_identifier = _increment(_next_network_setup_identifier)
        _network_setup_records[identifier] = {
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
    if identifier is None:
        return
    finished_monotonic_ns = time.perf_counter_ns()
    with _lock:
        record = _network_setup_records.get(identifier)
        if record is None or record.get("duration_ns") is not None:
            return
        started_monotonic_ns = record.get("_started_monotonic_ns")
        if not isinstance(started_monotonic_ns, int):
            return
        record["duration_ns"] = max(0, finished_monotonic_ns - started_monotonic_ns)
        record["outcome"] = outcome
        if error_type is not None:
            record["error_type"] = error_type[:MAX_EXECUTABLE_CHARACTERS]


def _begin_active_network_setup_identifier(identifier: int | None) -> ActiveToken | None:
    if identifier is None:
        return None
    thread_id = threading.get_ident()
    with _lock:
        key = ("network_setup", identifier)
        _active_record_keys.setdefault(thread_id, []).append(key)
    return thread_id, key


def _reserve_logical_operation_boundary(
    *,
    category: str,
    operation: str,
    adapter: str,
    capture_caller: bool = True,
) -> int | None:
    global _dropped_logical_operation_count, _next_logical_operation_identifier
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
    with _lock:
        if len(_logical_operation_records) >= MAX_LOGICAL_OPERATION_RECORDS:
            _dropped_logical_operation_count = _increment(_dropped_logical_operation_count)
            return None
        identifier = _next_logical_operation_identifier
        _next_logical_operation_identifier = _increment(_next_logical_operation_identifier)
        _logical_operation_records[identifier] = {
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
    if identifier is None:
        return
    finished_monotonic_ns = time.perf_counter_ns()
    with _lock:
        record = _logical_operation_records.get(identifier)
        if record is None or record.get("duration_ns") is not None:
            return
        started_monotonic_ns = record.get("_started_monotonic_ns")
        if not isinstance(started_monotonic_ns, int):
            return
        record["duration_ns"] = max(0, finished_monotonic_ns - started_monotonic_ns)
        record["outcome"] = outcome
        if error_type is not None:
            record["error_type"] = error_type[:MAX_EXECUTABLE_CHARACTERS]


def _begin_active_logical_operation(identifier: int | None) -> ActiveToken | None:
    if identifier is None:
        return None
    thread_id = threading.get_ident()
    with _lock:
        key = ("logical_operation", identifier)
        _active_record_keys.setdefault(thread_id, []).append(key)
    return thread_id, key


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
    if _logical_operation_capture_suppressed.get():
        return original(instance, *positional, **keywords)
    identifier: int | None = None
    active_token: ActiveToken | None = None
    suppression_token: contextvars.Token[bool] | None = None
    try:
        suppression_token = _logical_operation_capture_suppressed.set(True)
        identifier = _reserve_logical_operation_boundary(
            category=category,
            operation=operation,
            adapter=adapter,
        )
        if sampled_caller:
            active_token = _begin_active_logical_operation(identifier)
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
                _logical_operation_capture_suppressed.reset(suppression_token)
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
    if _logical_operation_capture_suppressed.get():
        return await original(instance, *positional, **keywords)
    identifier: int | None = None
    suppression_token: contextvars.Token[bool] | None = None
    try:
        suppression_token = _logical_operation_capture_suppressed.set(True)
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
                _logical_operation_capture_suppressed.reset(suppression_token)
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
    if _logical_operation_capture_suppressed.get():
        return original(instance, *positional, **keywords)
    identifier: int | None = None
    suppression_token: contextvars.Token[bool] | None = None
    try:
        suppression_token = _logical_operation_capture_suppressed.set(True)
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
                _logical_operation_capture_suppressed.reset(suppression_token)
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
    if _logical_operation_capture_suppressed.get():
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
            token = _begin_active_logical_operation(identifier)
        with _lock:
            if adapter in LOGICAL_OPERATION_ADAPTERS:
                _logical_operation_adapters.add(adapter)
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
    with _lock:
        record = _logical_operation_records.get(identifier)
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
    with _lock:
        record = _logical_operation_records.get(identifier) if identifier is not None else None
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


def _asyncio_task_scheduling_origin() -> str | None:
    origins = getattr(_asyncio_task_scheduling_state, "origins", None)
    if not isinstance(origins, list) or not origins:
        return None
    origin = origins[-1]
    return origin if isinstance(origin, str) else None


def _run_asyncio_task_scheduling_entry_point(
    original: Callable[..., object],
    positional: tuple[object, ...],
    keywords: dict[str, object],
    *,
    adapter: str,
) -> object:
    origins = getattr(_asyncio_task_scheduling_state, "origins", None)
    if not isinstance(origins, list):
        origins = []
        _asyncio_task_scheduling_state.origins = origins
    origins.append(adapter)
    try:
        return original(*positional, **keywords)
    finally:
        origins.pop()


def _run_asyncio_ensure_future(
    original: Callable[..., object],
    positional: tuple[object, ...],
    keywords: dict[str, object],
    *,
    default_adapter: str | None,
) -> object:
    adapter = _asyncio_task_scheduling_origin() or default_adapter
    if adapter is None or _logical_operation_capture_suppressed.get():
        return original(*positional, **keywords)
    candidate = positional[0] if positional else keywords.get("coro_or_future")
    try:
        task = original(*positional, **keywords)
    except BaseException as error:
        identifier: int | None = None
        try:
            identifier = _reserve_logical_operation_boundary(
                category="scheduler",
                operation="task",
                adapter=adapter,
            )
            _finish_logical_operation_record(
                identifier,
                outcome="operation_error",
                error_type=type(error).__name__,
            )
        except BaseException:
            _record_logical_operation_callback_error()
        raise
    if task is candidate:
        return task
    identifier = None
    try:
        identifier = _reserve_logical_operation_boundary(
            category="scheduler",
            operation="task",
            adapter=adapter,
        )
        if identifier is None:
            return task
        add_done_callback = getattr(task, "add_done_callback", None)
        if not callable(add_done_callback):
            _record_logical_operation_callback_error()
        else:
            add_done_callback(functools.partial(_finish_scheduled_task, identifier))
    except BaseException:
        _record_logical_operation_callback_error()
    return task


@functools.wraps(_original_getaddrinfo)
def _observed_getaddrinfo(
    host: object,
    port: object,
    *positional: object,
    **keywords: object,
) -> object:
    identifier: int | None = None
    active_token: ActiveToken | None = None
    try:
        if host is not None:
            identifier = _reserve_network_setup_boundary(
                phase="dns",
                adapter="stdlib.socket.getaddrinfo",
            )
            active_token = _begin_active_network_setup_identifier(identifier)
    except BaseException:
        _record_network_setup_callback_error()
    try:
        result = _original_getaddrinfo(host, port, *positional, **keywords)
    except BaseException as error:
        try:
            _finish_network_setup_record(
                identifier,
                outcome="setup_error",
                error_type=type(error).__name__,
            )
        except BaseException:
            _record_network_setup_callback_error()
        raise
    finally:
        try:
            _end_active(active_token)
        except BaseException:
            _record_network_setup_caller_callback_error()
    try:
        _finish_network_setup_record(identifier, outcome="completed")
    except BaseException:
        _record_network_setup_callback_error()
    return result


def _tls_record_identifier(connection: object, *, adapter: str) -> int | None:
    connection_identity = id(connection)
    with _lock:
        active = _tls_record_ids.get(connection_identity)
        if active is not None and active[0] is connection:
            return active[1]
    identifier = _reserve_network_setup_boundary(phase="tls", adapter=adapter)
    with _lock:
        _tls_record_ids[connection_identity] = (connection, identifier)
    return identifier


def _release_tls_record(connection: object) -> None:
    connection_identity = id(connection)
    with _lock:
        active = _tls_record_ids.get(connection_identity)
        if active is not None and active[0] is connection:
            _tls_record_ids.pop(connection_identity, None)


def _observe_tls_handshake(
    original: Callable[..., None],
    adapter: str,
    connection: object,
    positional: tuple[object, ...],
    keywords: dict[str, object],
) -> None:
    identifier: int | None = None
    active_token: ActiveToken | None = None
    try:
        identifier = _tls_record_identifier(connection, adapter=adapter)
        active_token = _begin_active_network_setup_identifier(identifier)
    except BaseException:
        _record_network_setup_callback_error()
    try:
        original(connection, *positional, **keywords)
    except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
        raise
    except BaseException as error:
        try:
            _finish_network_setup_record(
                identifier,
                outcome="setup_error",
                error_type=type(error).__name__,
            )
            _release_tls_record(connection)
        except BaseException:
            _record_network_setup_callback_error()
        raise
    finally:
        try:
            _end_active(active_token)
        except BaseException:
            _record_network_setup_caller_callback_error()
    try:
        _finish_network_setup_record(identifier, outcome="completed")
        _release_tls_record(connection)
    except BaseException:
        _record_network_setup_callback_error()


@functools.wraps(_original_ssl_object_handshake)
def _observed_ssl_object_handshake(
    connection: object,
    *positional: object,
    **keywords: object,
) -> None:
    _observe_tls_handshake(
        _original_ssl_object_handshake,
        "stdlib.ssl.SSLObject.do_handshake",
        connection,
        positional,
        keywords,
    )


@functools.wraps(_original_ssl_socket_handshake)
def _observed_ssl_socket_handshake(
    connection: object,
    *positional: object,
    **keywords: object,
) -> None:
    _observe_tls_handshake(
        _original_ssl_socket_handshake,
        "stdlib.ssl.SSLSocket.do_handshake",
        connection,
        positional,
        keywords,
    )


@functools.wraps(_original_socket_connect)
def _observed_socket_connect(
    connection: object,
    address: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier: int | None = None
    active_token: ActiveToken | None = None
    try:
        target = None if _network_capture_suppressed.get() else _network_target(connection, address)
        if target is not None:
            transport, family, server_port = target
            identifier = _reserve_network_boundary(
                transport=transport,
                family=family,
                server_port=server_port,
                adapter="stdlib.socket.connect",
                tls_requested=None,
            )
            active_token = _begin_active_network_identifier(identifier)
    except BaseException:
        _record_network_callback_error()
    try:
        _original_socket_connect(connection, address, *positional, **keywords)
    except BaseException as error:
        try:
            _finish_network_record(
                identifier,
                outcome="connect_error",
                error_type=type(error).__name__,
            )
        except BaseException:
            _record_network_callback_error()
        raise
    finally:
        try:
            _end_active(active_token)
        except BaseException:
            _record_network_caller_callback_error()
    try:
        _finish_network_record(identifier, outcome="connected")
    except BaseException:
        _record_network_callback_error()


@functools.wraps(_original_http_putrequest)
def _observed_http_putrequest(
    connection: object,
    method: object,
    url: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier: int | None = None
    try:
        identifier = _reserve_http_record(connection, method)
    except BaseException:
        _record_http_callback_error()
    try:
        _original_http_putrequest(connection, method, url, *positional, **keywords)
    except BaseException as exc:
        try:
            _finish_http_record(
                identifier,
                status_code=None,
                outcome="request_error",
                error_type=type(exc).__name__,
            )
            _release_http_record(connection)
        except BaseException:
            _record_http_callback_error()
        raise


@functools.wraps(_original_http_endheaders)
def _observed_http_endheaders(
    connection: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier = _http_record_identifier(connection)
    token: ActiveToken | None = None
    network_token: NetworkSuppressionToken | None = None
    try:
        token = _begin_active_http(connection)
        network_token = _begin_network_suppression()
    except BaseException:
        _record_http_caller_callback_error()
    try:
        _original_http_endheaders(connection, *positional, **keywords)
    except BaseException as exc:
        try:
            _finish_http_record(
                identifier,
                status_code=None,
                outcome="request_error",
                error_type=type(exc).__name__,
            )
            _release_http_record(connection)
        except BaseException:
            _record_http_callback_error()
        raise
    finally:
        try:
            _end_network_suppression(network_token)
            _end_active(token)
        except BaseException:
            _record_http_caller_callback_error()


@functools.wraps(_original_http_getresponse)
def _observed_http_getresponse(
    connection: object,
    *positional: object,
    **keywords: object,
) -> http.client.HTTPResponse:
    identifier = _http_record_identifier(connection)
    token: ActiveToken | None = None
    network_token: NetworkSuppressionToken | None = None
    try:
        token = _begin_active_http(connection)
        network_token = _begin_network_suppression()
    except BaseException:
        _record_http_caller_callback_error()
    try:
        response = _original_http_getresponse(connection, *positional, **keywords)
    except BaseException as exc:
        try:
            _finish_http_record(
                identifier,
                status_code=None,
                outcome="request_error",
                error_type=type(exc).__name__,
            )
            _release_http_record(connection)
        except BaseException:
            _record_http_callback_error()
        raise
    finally:
        try:
            _end_network_suppression(network_token)
            _end_active(token)
        except BaseException:
            _record_http_caller_callback_error()
    try:
        _finish_http_record(
            identifier,
            status_code=_http_status(response),
            outcome="response",
        )
        _release_http_record(connection)
    except BaseException:
        _record_http_callback_error()
    return response


@functools.wraps(_original_http_close)
def _observed_http_close(
    connection: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier = _http_record_identifier(connection)
    try:
        _original_http_close(connection, *positional, **keywords)
    finally:
        try:
            _finish_http_record(identifier, status_code=None, outcome="closed")
            _release_http_record(connection)
        except BaseException:
            _record_http_callback_error()


def _exact_attribute(value: object, name: str) -> object:
    values = _http_connection_values(value)
    if name in values:
        return values[name]
    try:
        return object.__getattribute__(value, name)
    except BaseException:
        return None


def _httpcore_request_values(
    request: object,
    *,
    request_type: type[object],
    url_type: type[object],
) -> tuple[object, str, object] | None:
    if type(request) is not request_type:
        return None
    method = _exact_attribute(request, "method")
    url = _exact_attribute(request, "url")
    if type(url) is not url_type:
        return None
    scheme = _http_scheme(_exact_attribute(url, "scheme"))
    if scheme is None:
        return None
    return method, scheme, _exact_attribute(url, "port")


def _typed_response_status(response: object, response_type: type[object]) -> int | None:
    if type(response) is not response_type:
        return None
    status = _exact_attribute(response, "status")
    return status if type(status) is int and 100 <= status <= 999 else None


def _finish_optional_http_error(identifier: int | None, error: BaseException) -> None:
    try:
        _finish_http_record(
            identifier,
            status_code=None,
            outcome="request_error",
            error_type=type(error).__name__,
        )
    except BaseException:
        _record_http_callback_error()


def _patch_httpcore_sync(
    owner: type[object],
    *,
    request_type: type[object],
    url_type: type[object],
    response_type: type[object],
) -> bool:
    method_name = "handle_request"
    try:
        raw_original = type.__getattribute__(owner, method_name)
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., object], raw_original)

    @functools.wraps(original)
    def observed(
        instance: object,
        request: object,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        token: ActiveToken | None = None
        network_token: NetworkSuppressionToken | None = None
        try:
            request_values = _httpcore_request_values(
                request,
                request_type=request_type,
                url_type=url_type,
            )
            if request_values is not None:
                method, scheme, server_port = request_values
                identifier = _reserve_http_boundary(
                    method,
                    scheme=scheme,
                    server_port=server_port,
                    adapter="httpcore.sync",
                )
                token = _begin_active_http_identifier(identifier)
                network_token = _begin_network_suppression()
        except BaseException:
            _record_http_callback_error()
        try:
            response = original(instance, request, *positional, **keywords)
        except BaseException as exc:
            _finish_optional_http_error(identifier, exc)
            raise
        finally:
            try:
                _end_network_suppression(network_token)
                _end_active(token)
            except BaseException:
                _record_http_caller_callback_error()
        try:
            _finish_http_record(
                identifier,
                status_code=_typed_response_status(response, response_type),
                outcome="response",
            )
        except BaseException:
            _record_http_callback_error()
        return response

    try:
        type.__setattr__(owner, method_name, observed)
    except BaseException:
        return False
    with _lock:
        _http_adapters.add("httpcore.sync")
    return True


def _patch_httpcore_async(
    owner: type[object],
    *,
    request_type: type[object],
    url_type: type[object],
    response_type: type[object],
) -> bool:
    method_name = "handle_async_request"
    try:
        raw_original = type.__getattribute__(owner, method_name)
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., Awaitable[object]], raw_original)

    @functools.wraps(original)
    async def observed(
        instance: object,
        request: object,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        network_token: NetworkSuppressionToken | None = None
        try:
            request_values = _httpcore_request_values(
                request,
                request_type=request_type,
                url_type=url_type,
            )
            if request_values is not None:
                method, scheme, server_port = request_values
                identifier = _reserve_http_boundary(
                    method,
                    scheme=scheme,
                    server_port=server_port,
                    adapter="httpcore.async",
                )
                network_token = _begin_network_suppression()
        except BaseException:
            _record_http_callback_error()
        try:
            response = await original(instance, request, *positional, **keywords)
        except BaseException as exc:
            _finish_optional_http_error(identifier, exc)
            raise
        finally:
            try:
                _end_network_suppression(network_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_http_record(
                identifier,
                status_code=_typed_response_status(response, response_type),
                outcome="response",
            )
        except BaseException:
            _record_http_callback_error()
        return response

    try:
        type.__setattr__(owner, method_name, observed)
    except BaseException:
        return False
    with _lock:
        _http_adapters.add("httpcore.async")
    return True


def _module_values(module: ModuleType) -> dict[str, object]:
    try:
        values = ModuleType.__getattribute__(module, "__dict__")
    except BaseException:
        return {}
    return cast(dict[str, object], values) if type(values) is dict else {}


def _patch_httpcore(module: ModuleType) -> bool:
    values = _module_values(module)
    request_type = values.get("Request")
    url_type = values.get("URL")
    response_type = values.get("Response")
    sync_pool = values.get("ConnectionPool")
    async_pool = values.get("AsyncConnectionPool")
    if not all(isinstance(value, type) for value in (request_type, url_type, response_type)):
        return False
    typed_request = cast(type[object], request_type)
    typed_url = cast(type[object], url_type)
    typed_response = cast(type[object], response_type)
    sync_patched = isinstance(sync_pool, type) and _patch_httpcore_sync(
        sync_pool,
        request_type=typed_request,
        url_type=typed_url,
        response_type=typed_response,
    )
    async_patched = isinstance(async_pool, type) and _patch_httpcore_async(
        async_pool,
        request_type=typed_request,
        url_type=typed_url,
        response_type=typed_response,
    )
    return sync_patched and async_patched


def _url_scheme_and_port(value: object) -> tuple[str, object] | None:
    if type(value) is str:
        try:
            parsed = urllib.parse.urlsplit(value)
            scheme = _http_scheme(parsed.scheme)
            return None if scheme is None else (scheme, parsed.port)
        except (UnicodeError, ValueError):
            return None
    value_type = type(value)
    try:
        module_name = type.__getattribute__(value_type, "__module__")
    except BaseException:
        return None
    if type(module_name) is not str or not module_name.startswith("yarl"):
        return None
    scheme = _http_scheme(_exact_attribute(value, "scheme"))
    return None if scheme is None else (scheme, _exact_attribute(value, "port"))


def _aiohttp_request_values(
    session: object,
    method: object,
    url: object,
) -> tuple[object, str, object] | None:
    target = _url_scheme_and_port(url)
    if target is None:
        target = _url_scheme_and_port(_exact_attribute(session, "_base_url"))
    if target is None:
        return None
    scheme, server_port = target
    return method, scheme, server_port


def _aiohttp_response_status(response: object) -> int | None:
    try:
        module_name = type.__getattribute__(type(response), "__module__")
    except BaseException:
        return None
    if type(module_name) is not str or not module_name.startswith("aiohttp"):
        return None
    status = _exact_attribute(response, "status")
    return status if type(status) is int and 100 <= status <= 999 else None


def _patch_aiohttp(module: ModuleType) -> bool:
    values = _module_values(module)
    session_type = values.get("ClientSession")
    if not isinstance(session_type, type):
        return False
    method_name = "_request"
    try:
        raw_original = type.__getattribute__(session_type, method_name)
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., Awaitable[object]], raw_original)

    @functools.wraps(original)
    async def observed(
        session: object,
        method: object,
        url: object,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        network_token: NetworkSuppressionToken | None = None
        try:
            request_values = _aiohttp_request_values(session, method, url)
            if request_values is not None:
                safe_method, scheme, server_port = request_values
                identifier = _reserve_http_boundary(
                    safe_method,
                    scheme=scheme,
                    server_port=server_port,
                    adapter="aiohttp.async",
                )
                network_token = _begin_network_suppression()
        except BaseException:
            _record_http_callback_error()
        try:
            response = await original(session, method, url, *positional, **keywords)
        except BaseException as exc:
            _finish_optional_http_error(identifier, exc)
            raise
        finally:
            try:
                _end_network_suppression(network_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_http_record(
                identifier,
                status_code=_aiohttp_response_status(response),
                outcome="response",
            )
        except BaseException:
            _record_http_callback_error()
        return response

    try:
        type.__setattr__(session_type, method_name, observed)
    except BaseException:
        return False
    with _lock:
        _http_adapters.add("aiohttp.async")
    return True


def _patch_asyncio(module: ModuleType) -> bool:
    values = _module_values(module)
    loop_type = values.get("BaseEventLoop")
    if not isinstance(loop_type, type):
        return False

    try:
        raw_tcp_original = type.__getattribute__(loop_type, "create_connection")
    except BaseException:
        return False
    if not callable(raw_tcp_original):
        return False
    tcp_original = cast(Callable[..., Awaitable[object]], raw_tcp_original)

    @functools.wraps(tcp_original)
    async def observed_tcp(
        loop: object,
        protocol_factory: object,
        host: object = None,
        port: object = None,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        suppression_token: NetworkSuppressionToken | None = None
        should_observe = (
            not _network_capture_suppressed.get()
            and host is not None
            and keywords.get("sock") is None
        )
        try:
            if should_observe:
                server_port = port if type(port) is int and 0 < port <= 65_535 else None
                identifier = _reserve_network_boundary(
                    transport="tcp",
                    family=_network_family(keywords.get("family")),
                    server_port=server_port,
                    adapter="asyncio.create_connection",
                    tls_requested=(
                        keywords.get("ssl") is not None and keywords.get("ssl") is not False
                    ),
                )
                suppression_token = _begin_network_suppression()
        except BaseException:
            _record_network_callback_error()
        try:
            result = await tcp_original(
                loop,
                protocol_factory,
                host,
                port,
                *positional,
                **keywords,
            )
        except BaseException as error:
            try:
                _finish_network_record(
                    identifier,
                    outcome="connect_error",
                    error_type=type(error).__name__,
                )
            except BaseException:
                _record_network_callback_error()
            raise
        finally:
            try:
                _end_network_suppression(suppression_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_network_record(identifier, outcome="connected")
        except BaseException:
            _record_network_callback_error()
        return result

    try:
        type.__setattr__(loop_type, "create_connection", observed_tcp)
    except BaseException:
        return False
    with _lock:
        _network_adapters.add("asyncio.create_connection")
    return True


def _patch_asyncio_unix(module: ModuleType) -> bool:
    values = _module_values(module)
    loop_type = values.get("_UnixSelectorEventLoop")
    if not isinstance(loop_type, type):
        return False
    try:
        raw_original = type.__getattribute__(loop_type, "create_unix_connection")
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., Awaitable[object]], raw_original)

    @functools.wraps(original)
    async def observed(
        loop: object,
        protocol_factory: object,
        path: object = None,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        suppression_token: NetworkSuppressionToken | None = None
        should_observe = (
            not _network_capture_suppressed.get()
            and path is not None
            and keywords.get("sock") is None
        )
        try:
            if should_observe:
                identifier = _reserve_network_boundary(
                    transport="unix",
                    family="unix",
                    server_port=None,
                    adapter="asyncio.create_unix_connection",
                    tls_requested=(
                        keywords.get("ssl") is not None and keywords.get("ssl") is not False
                    ),
                )
                suppression_token = _begin_network_suppression()
        except BaseException:
            _record_network_callback_error()
        try:
            result = await original(
                loop,
                protocol_factory,
                path,
                *positional,
                **keywords,
            )
        except BaseException as error:
            try:
                _finish_network_record(
                    identifier,
                    outcome="connect_error",
                    error_type=type(error).__name__,
                )
            except BaseException:
                _record_network_callback_error()
            raise
        finally:
            try:
                _end_network_suppression(suppression_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_network_record(identifier, outcome="connected")
        except BaseException:
            _record_network_callback_error()
        return result

    try:
        type.__setattr__(loop_type, "create_unix_connection", observed)
    except BaseException:
        return False
    with _lock:
        _network_adapters.add("asyncio.create_unix_connection")
    return True


def _logical_sync_wrapper(
    original: Callable[..., object],
    *,
    category: str,
    operation: str,
    adapter: str,
    sampled_caller: bool = True,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(instance: object, *positional: object, **keywords: object) -> object:
        return _run_logical_operation(
            original,
            instance,
            positional,
            keywords,
            category=category,
            operation=operation,
            adapter=adapter,
            sampled_caller=sampled_caller,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _executor_submit_wrapper(
    original: Callable[..., object],
    *,
    adapter: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(instance: object, *positional: object, **keywords: object) -> object:
        return _run_executor_submit(
            original,
            instance,
            positional,
            keywords,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _asyncio_create_task_wrapper(
    original: Callable[..., object],
    *,
    adapter: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(*positional: object, **keywords: object) -> object:
        return _run_asyncio_create_task(
            original,
            positional,
            keywords,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _asyncio_task_scheduling_entry_point_wrapper(
    original: Callable[..., object],
    *,
    adapter: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(*positional: object, **keywords: object) -> object:
        return _run_asyncio_task_scheduling_entry_point(
            original,
            positional,
            keywords,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _asyncio_ensure_future_wrapper(
    original: Callable[..., object],
    *,
    adapter: str | None,
    marker: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(*positional: object, **keywords: object) -> object:
        return _run_asyncio_ensure_future(
            original,
            positional,
            keywords,
            default_adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    setattr(observed, marker, True)
    return observed


def _patch_executor(
    module: ModuleType,
    *,
    owner_name: str,
    adapter: str,
) -> bool:
    executor_type = getattr(module, owner_name, None)
    if not isinstance(executor_type, type):
        return False
    original = getattr(executor_type, "submit", None)
    if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
        return True
    if not callable(original):
        return False
    wrapped = _executor_submit_wrapper(
        cast(Callable[..., object], original),
        adapter=adapter,
    )
    try:
        type.__setattr__(executor_type, "submit", wrapped)
    except BaseException:
        return False
    with _lock:
        _logical_operation_adapters.add(adapter)
    return True


def _patch_asyncio_create_task(module: ModuleType) -> bool:
    original = getattr(module, "create_task", None)
    if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
        with _lock:
            _logical_operation_adapters.add("stdlib.asyncio.create_task")
        return True
    if not callable(original):
        return False
    adapter = "stdlib.asyncio.create_task"
    wrapped = _asyncio_create_task_wrapper(
        cast(Callable[..., object], original),
        adapter=adapter,
    )
    try:
        ModuleType.__setattr__(module, "create_task", wrapped)
    except BaseException:
        return False
    with _lock:
        _logical_operation_adapters.add(adapter)
    return True


def _patch_asyncio_implicit_tasks(module: ModuleType) -> bool:
    module_name = getattr(module, "__name__", None)
    names_and_adapters = {"gather": "stdlib.asyncio.gather"}
    originals: dict[str, object] = {}
    wrappers: dict[str, object] = {}
    for name, adapter in names_and_adapters.items():
        original = getattr(module, name, None)
        if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
            continue
        if not callable(original):
            return False
        originals[name] = original
        wrappers[name] = _asyncio_task_scheduling_entry_point_wrapper(
            cast(Callable[..., object], original),
            adapter=adapter,
        )
    ensure_future = getattr(module, "ensure_future", None)
    ensure_future_marker = (
        _ASYNCIO_ENSURE_FUTURE_ENGINE_MARKER
        if module_name == "asyncio.tasks"
        else _ASYNCIO_ENSURE_FUTURE_ENTRY_MARKER
    )
    if getattr(ensure_future, ensure_future_marker, False) is not True:
        if not callable(ensure_future):
            return False
        originals["ensure_future"] = ensure_future
        wrappers["ensure_future"] = _asyncio_ensure_future_wrapper(
            cast(Callable[..., object], ensure_future),
            adapter=(None if module_name == "asyncio.tasks" else "stdlib.asyncio.ensure_future"),
            marker=ensure_future_marker,
        )
    try:
        for name, wrapper in wrappers.items():
            ModuleType.__setattr__(module, name, wrapper)
    except BaseException:
        for name, original in originals.items():
            try:
                ModuleType.__setattr__(module, name, original)
            except BaseException:
                pass
        return False
    with _lock:
        _logical_operation_adapters.update(
            {"stdlib.asyncio.ensure_future", "stdlib.asyncio.gather"}
        )
    return True


def _patch_asyncio_task_scheduling(module: ModuleType) -> bool:
    return _patch_asyncio_create_task(module) and _patch_asyncio_implicit_tasks(module)


def _patch_asyncio_task_group(module: ModuleType) -> bool:
    task_group_type = getattr(module, "TaskGroup", None)
    if not isinstance(task_group_type, type):
        return False
    try:
        original = type.__getattribute__(task_group_type, "create_task")
    except BaseException:
        return False
    if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
        with _lock:
            _logical_operation_adapters.add("stdlib.asyncio.TaskGroup")
        return True
    if not callable(original):
        return False
    adapter = "stdlib.asyncio.TaskGroup"
    wrapped = _asyncio_create_task_wrapper(
        cast(Callable[..., object], original),
        adapter=adapter,
    )
    try:
        type.__setattr__(task_group_type, "create_task", wrapped)
    except BaseException:
        return False
    with _lock:
        _logical_operation_adapters.add(adapter)
    return True


def _patch_sync_queue(module: ModuleType) -> bool:
    adapter = "stdlib.queue.Queue"
    queue_type = getattr(module, "Queue", None)
    if not isinstance(queue_type, type):
        return False
    if all(
        getattr(getattr(queue_type, operation, None), _LOGICAL_WRAPPER_MARKER, False) is True
        for operation in ("put", "get")
    ):
        return True
    originals: dict[str, Callable[..., object]] = {}
    for operation in ("put", "get"):
        try:
            original = type.__getattribute__(queue_type, operation)
        except BaseException:
            return False
        if not callable(original):
            return False
        originals[operation] = cast(Callable[..., object], original)
    try:
        for operation, original in originals.items():
            type.__setattr__(
                queue_type,
                operation,
                _logical_sync_wrapper(
                    original,
                    category="queue",
                    operation=operation,
                    adapter=adapter,
                ),
            )
    except BaseException:
        for operation, original in originals.items():
            try:
                type.__setattr__(queue_type, operation, original)
            except BaseException:
                pass
        return False
    with _lock:
        _logical_operation_adapters.add(adapter)
    return True


def _logical_async_wrapper(
    original: Callable[..., Awaitable[object]],
    *,
    category: str,
    operation: str,
    adapter: str,
) -> Callable[..., Awaitable[object]]:
    @functools.wraps(original)
    async def observed(instance: object, *positional: object, **keywords: object) -> object:
        return await _run_async_logical_operation(
            original,
            instance,
            positional,
            keywords,
            category=category,
            operation=operation,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _patch_async_queue(module: ModuleType) -> bool:
    adapter = "stdlib.asyncio.Queue"
    queue_type = getattr(module, "Queue", None)
    if not isinstance(queue_type, type):
        return False
    if all(
        getattr(getattr(queue_type, operation, None), _LOGICAL_WRAPPER_MARKER, False) is True
        for operation in ("put", "get")
    ):
        return True
    originals: dict[str, Callable[..., Awaitable[object]]] = {}
    for operation in ("put", "get"):
        try:
            original = type.__getattribute__(queue_type, operation)
        except BaseException:
            return False
        if not callable(original):
            return False
        originals[operation] = cast(Callable[..., Awaitable[object]], original)
    try:
        for operation, original in originals.items():
            type.__setattr__(
                queue_type,
                operation,
                _logical_async_wrapper(
                    original,
                    category="queue",
                    operation=operation,
                    adapter=adapter,
                ),
            )
    except BaseException:
        for operation, original in originals.items():
            try:
                type.__setattr__(queue_type, operation, original)
            except BaseException:
                pass
        return False
    with _lock:
        _logical_operation_adapters.add(adapter)
    return True


def _patch_sqlite3(module: ModuleType) -> bool:
    connection_adapter = "stdlib.sqlite3.Connection"
    cursor_adapter = "stdlib.sqlite3.Cursor"
    connection_type = getattr(module, "Connection", None)
    cursor_type = getattr(module, "Cursor", None)
    original_connect = getattr(module, "connect", None)
    dbapi2 = sys.modules.get("sqlite3.dbapi2")
    dbapi2_connection = getattr(dbapi2, "Connection", None)
    dbapi2_cursor = getattr(dbapi2, "Cursor", None)
    dbapi2_connect = getattr(dbapi2, "connect", None)
    if (
        not isinstance(connection_type, type)
        or not isinstance(cursor_type, type)
        or not callable(original_connect)
    ):
        return False
    if (
        getattr(connection_type, _LOGICAL_WRAPPER_MARKER, False) is True
        and getattr(cursor_type, _LOGICAL_WRAPPER_MARKER, False) is True
        and getattr(original_connect, _LOGICAL_WRAPPER_MARKER, False) is True
    ):
        return True
    connection_operations = ("execute", "executemany", "executescript", "commit", "rollback")
    cursor_operations = ("execute", "executemany", "executescript")
    connection_methods: dict[str, object] = {}
    cursor_methods: dict[str, object] = {}
    for operation in connection_operations:
        original = getattr(connection_type, operation, None)
        if not callable(original):
            return False
        connection_methods[operation] = _logical_sync_wrapper(
            cast(Callable[..., object], original),
            category="database",
            operation=operation,
            adapter=connection_adapter,
        )
    for operation in cursor_operations:
        original = getattr(cursor_type, operation, None)
        if not callable(original):
            return False
        cursor_methods[operation] = _logical_sync_wrapper(
            cast(Callable[..., object], original),
            category="database",
            operation=operation,
            adapter=cursor_adapter,
        )
    try:
        observed_cursor_type = type(
            "Cursor",
            (cursor_type,),
            {
                "__module__": "sqlite3",
                _LOGICAL_WRAPPER_MARKER: True,
                **cursor_methods,
            },
        )
        original_cursor = cast(Callable[..., object], connection_type.cursor)

        @functools.wraps(original_cursor)
        def observed_cursor(
            instance: object,
            *positional: object,
            **keywords: object,
        ) -> object:
            if positional or "factory" in keywords:
                return original_cursor(instance, *positional, **keywords)
            return original_cursor(instance, observed_cursor_type)

        observed_connection_type = type(
            "Connection",
            (connection_type,),
            {
                "__module__": "sqlite3",
                _LOGICAL_WRAPPER_MARKER: True,
                "cursor": observed_cursor,
                **connection_methods,
            },
        )
        original_connect_callable = cast(Callable[..., object], original_connect)

        @functools.wraps(original_connect_callable)
        def observed_connect(*positional: object, **keywords: object) -> object:
            if len(positional) >= 6 or "factory" in keywords:
                return original_connect_callable(*positional, **keywords)
            return original_connect_callable(
                *positional,
                factory=observed_connection_type,
                **keywords,
            )

        setattr(observed_connect, _LOGICAL_WRAPPER_MARKER, True)

        ModuleType.__setattr__(module, "Connection", observed_connection_type)
        ModuleType.__setattr__(module, "Cursor", observed_cursor_type)
        ModuleType.__setattr__(module, "connect", observed_connect)
        if isinstance(dbapi2, ModuleType):
            ModuleType.__setattr__(dbapi2, "Connection", observed_connection_type)
            ModuleType.__setattr__(dbapi2, "Cursor", observed_cursor_type)
            ModuleType.__setattr__(dbapi2, "connect", observed_connect)
    except BaseException:
        try:
            ModuleType.__setattr__(module, "Connection", connection_type)
            ModuleType.__setattr__(module, "Cursor", cursor_type)
            ModuleType.__setattr__(module, "connect", original_connect)
            if isinstance(dbapi2, ModuleType):
                ModuleType.__setattr__(dbapi2, "Connection", dbapi2_connection)
                ModuleType.__setattr__(dbapi2, "Cursor", dbapi2_cursor)
                ModuleType.__setattr__(dbapi2, "connect", dbapi2_connect)
        except BaseException:
            pass
        return False
    with _lock:
        _logical_operation_adapters.update({connection_adapter, cursor_adapter})
    return True


def _patch_optional_logical_type(
    module: ModuleType,
    *,
    owner_name: str,
    adapter: str,
    methods: dict[str, tuple[str, str, bool]],
) -> bool:
    owner = _module_values(module).get(owner_name)
    if not isinstance(owner, type):
        return False
    originals: dict[str, object] = {}
    wrappers: dict[str, object] = {}
    for method_name, (category, operation, is_async) in methods.items():
        try:
            original = type.__getattribute__(owner, method_name)
        except BaseException:
            return False
        if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
            continue
        if not callable(original):
            return False
        originals[method_name] = original
        wrappers[method_name] = (
            _logical_async_wrapper(
                cast(Callable[..., Awaitable[object]], original),
                category=category,
                operation=operation,
                adapter=adapter,
            )
            if is_async
            else _logical_sync_wrapper(
                cast(Callable[..., object], original),
                category=category,
                operation=operation,
                adapter=adapter,
                sampled_caller=False,
            )
        )
    try:
        for method_name, wrapper in wrappers.items():
            type.__setattr__(owner, method_name, wrapper)
    except BaseException:
        for method_name, original in originals.items():
            try:
                type.__setattr__(owner, method_name, original)
            except BaseException:
                pass
        return False
    with _lock:
        _logical_operation_adapters.add(adapter)
    return True


def _patch_optional_logical_operations(module_name: str, module: ModuleType) -> bool:
    if module_name == "concurrent.futures.thread":
        return _patch_executor(
            module,
            owner_name="ThreadPoolExecutor",
            adapter="stdlib.concurrent.futures.ThreadPoolExecutor",
        )
    if module_name == "concurrent.futures.process":
        return _patch_executor(
            module,
            owner_name="ProcessPoolExecutor",
            adapter="stdlib.concurrent.futures.ProcessPoolExecutor",
        )
    if module_name == "sqlalchemy.engine.base":
        return _patch_optional_logical_type(
            module,
            owner_name="Connection",
            adapter="sqlalchemy.engine.Connection",
            methods={
                "commit": ("database", "commit", False),
                "execute": ("database", "execute", False),
                "exec_driver_sql": ("database", "execute", False),
                "rollback": ("database", "rollback", False),
            },
        )
    if module_name == "sqlalchemy.orm.session":
        return _patch_optional_logical_type(
            module,
            owner_name="Session",
            adapter="sqlalchemy.orm.Session",
            methods={
                "commit": ("database", "commit", False),
                "execute": ("database", "execute", False),
                "rollback": ("database", "rollback", False),
            },
        )
    if module_name == "sqlalchemy.ext.asyncio.engine":
        return _patch_optional_logical_type(
            module,
            owner_name="AsyncConnection",
            adapter="sqlalchemy.ext.asyncio.AsyncConnection",
            methods={
                "commit": ("database", "commit", True),
                "execute": ("database", "execute", True),
                "exec_driver_sql": ("database", "execute", True),
                "rollback": ("database", "rollback", True),
            },
        )
    if module_name == "sqlalchemy.ext.asyncio.session":
        return _patch_optional_logical_type(
            module,
            owner_name="AsyncSession",
            adapter="sqlalchemy.ext.asyncio.AsyncSession",
            methods={
                "commit": ("database", "commit", True),
                "execute": ("database", "execute", True),
                "rollback": ("database", "rollback", True),
            },
        )
    if module_name == "redis.client":
        outcomes = (
            _patch_optional_logical_type(
                module,
                owner_name="Redis",
                adapter="redis.Redis",
                methods={"execute_command": ("cache", "command", False)},
            ),
            _patch_optional_logical_type(
                module,
                owner_name="Pipeline",
                adapter="redis.Pipeline",
                methods={"execute": ("cache", "batch", False)},
            ),
        )
        return all(outcomes)
    if module_name == "redis.asyncio.client":
        outcomes = (
            _patch_optional_logical_type(
                module,
                owner_name="Redis",
                adapter="redis.asyncio.Redis",
                methods={"execute_command": ("cache", "command", True)},
            ),
            _patch_optional_logical_type(
                module,
                owner_name="Pipeline",
                adapter="redis.asyncio.Pipeline",
                methods={"execute": ("cache", "batch", True)},
            ),
        )
        return all(outcomes)
    if module_name == "pika.adapters.blocking_connection":
        return _patch_optional_logical_type(
            module,
            owner_name="BlockingChannel",
            adapter="pika.BlockingChannel",
            methods={
                "basic_get": ("broker", "consume", False),
                "basic_publish": ("broker", "publish", False),
            },
        )
    if module_name == "aiokafka.producer.producer":
        return _patch_optional_logical_type(
            module,
            owner_name="AIOKafkaProducer",
            adapter="aiokafka.AIOKafkaProducer",
            methods={"send_and_wait": ("broker", "publish", True)},
        )
    if module_name == "aiokafka.consumer.consumer":
        return _patch_optional_logical_type(
            module,
            owner_name="AIOKafkaConsumer",
            adapter="aiokafka.AIOKafkaConsumer",
            methods={
                "getmany": ("broker", "consume", True),
                "getone": ("broker", "consume", True),
            },
        )
    return False


def _activate_lazy_adapter(module_name: str, module: ModuleType) -> None:
    try:
        patched = (
            _patch_httpcore(module)
            if module_name == "httpcore"
            else _patch_aiohttp(module)
            if module_name == "aiohttp.client"
            else _patch_asyncio(module)
            if module_name == "asyncio.base_events"
            else _patch_asyncio_unix(module)
            if module_name == "asyncio.unix_events"
            else _patch_asyncio_task_scheduling(module)
            if module_name in {"asyncio", "asyncio.tasks"} and _logical_operation_capture_enabled
            else _patch_asyncio_task_group(module)
            if module_name == "asyncio.taskgroups" and _logical_operation_capture_enabled
            else _patch_sync_queue(module)
            if module_name == "queue" and _logical_operation_capture_enabled
            else _patch_async_queue(module)
            if module_name == "asyncio.queues" and _logical_operation_capture_enabled
            else _patch_sqlite3(module)
            if module_name in {"sqlite3", "sqlite3.dbapi2"} and _logical_operation_capture_enabled
            else _patch_optional_logical_operations(module_name, module)
            if module_name in OPTIONAL_LOGICAL_OPERATION_MODULES
            and _logical_operation_capture_enabled
            else True
        )
        if not patched:
            if module_name in LOGICAL_OPERATION_MODULES:
                _record_logical_operation_callback_error()
            elif module_name in {"asyncio.base_events", "asyncio.unix_events"}:
                _record_network_callback_error()
            else:
                _record_http_callback_error()
    except BaseException:
        if module_name in LOGICAL_OPERATION_MODULES:
            _record_logical_operation_callback_error()
        elif module_name in {"asyncio.base_events", "asyncio.unix_events"}:
            _record_network_callback_error()
        else:
            _record_http_callback_error()


class _OptionalAdapterLoader(importlib.abc.Loader):
    def __init__(
        self,
        loader: importlib.abc.Loader,
        module_name: str,
    ) -> None:
        self._loader = loader
        self._module_name = module_name

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType | None:
        return self._loader.create_module(spec)

    def exec_module(self, module: ModuleType) -> None:
        self._loader.exec_module(module)
        try:
            spec = module.__spec__
            if spec is not None:
                spec.loader = self._loader
            module.__loader__ = self._loader
        except BaseException:
            if self._module_name in LOGICAL_OPERATION_MODULES:
                _record_logical_operation_callback_error()
            elif self._module_name.startswith("asyncio."):
                _record_network_callback_error()
            else:
                _record_http_callback_error()
        _activate_lazy_adapter(self._module_name, module)

    def __getattr__(self, name: str) -> object:
        return getattr(self._loader, name)


class _OptionalAdapterFinder(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if fullname not in LAZY_ADAPTER_MODULES:
            return None
        if fullname in LOGICAL_OPERATION_MODULES and not _logical_operation_capture_enabled:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        if spec is None or spec.loader is None:
            return spec
        spec.loader = _OptionalAdapterLoader(spec.loader, fullname)
        return spec


_optional_adapter_finder = _OptionalAdapterFinder()


def _install_lazy_adapters() -> None:
    for module_name in LAZY_ADAPTER_MODULES:
        loaded = sys.modules.get(module_name)
        if isinstance(loaded, ModuleType):
            _activate_lazy_adapter(module_name, loaded)
    if any(finder is _optional_adapter_finder for finder in sys.meta_path):
        return
    insert_at = next(
        (
            index
            for index, finder in enumerate(sys.meta_path)
            if finder is importlib.machinery.PathFinder
        ),
        len(sys.meta_path),
    )
    sys.meta_path.insert(insert_at, _optional_adapter_finder)


@functools.wraps(_original_init)
def _observed_init(
    process: object,
    arguments: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier: int | None = None
    try:
        identifier = _reserve_record(arguments, positional, keywords)
        _associate_record(process, identifier)
    except BaseException:
        _record_callback_error()
    try:
        _original_init(process, arguments, *positional, **keywords)
    except BaseException as exc:
        try:
            _finish_record(
                identifier,
                exit_code=None,
                outcome="launch_error",
                error_type=type(exc).__name__,
            )
            _release_record(process)
        except BaseException:
            _record_callback_error()
        raise
    try:
        _record_child(identifier, process)
    except BaseException:
        _record_callback_error()


@functools.wraps(_original_wait)
def _observed_wait(process: object, *positional: object, **keywords: object) -> int:
    token: ActiveToken | None = None
    try:
        token = _begin_active(process)
    except BaseException:
        _record_caller_callback_error()
    try:
        result = _original_wait(process, *positional, **keywords)
    finally:
        try:
            _end_active(token)
        except BaseException:
            _record_caller_callback_error()
    try:
        _finish_record(
            _record_identifier(process),
            exit_code=result,
            outcome="exited",
        )
        _release_record(process)
    except BaseException:
        _record_callback_error()
    return result


@functools.wraps(_original_communicate)
def _observed_communicate(
    process: object,
    *positional: object,
    **keywords: object,
) -> object:
    token: ActiveToken | None = None
    try:
        token = _begin_active(process)
    except BaseException:
        _record_caller_callback_error()
    try:
        return _original_communicate(process, *positional, **keywords)
    finally:
        try:
            _end_active(token)
        except BaseException:
            _record_caller_callback_error()


@functools.wraps(_original_poll)
def _observed_poll(process: object, *positional: object, **keywords: object) -> int | None:
    result = _original_poll(process, *positional, **keywords)
    if result is None:
        return None
    try:
        _finish_record(
            _record_identifier(process),
            exit_code=result,
            outcome="exited",
        )
        _release_record(process)
    except BaseException:
        _record_callback_error()
    return result


def activate(
    caller_provider: CallerProvider | None = None,
    *,
    instrument_logical_operations: bool = False,
) -> None:
    """Install bounded boundary observers for the selected capture level."""

    global _active, _caller_provider, _logical_operation_capture_enabled
    with _lock:
        if caller_provider is not None:
            _caller_provider = caller_provider
        _logical_operation_capture_enabled = instrument_logical_operations
        if _active:
            return
        try:
            type.__setattr__(subprocess.Popen, "__init__", _observed_init)
            type.__setattr__(subprocess.Popen, "wait", _observed_wait)
            type.__setattr__(subprocess.Popen, "poll", _observed_poll)
            type.__setattr__(subprocess.Popen, "communicate", _observed_communicate)
            type.__setattr__(http.client.HTTPConnection, "putrequest", _observed_http_putrequest)
            type.__setattr__(http.client.HTTPConnection, "endheaders", _observed_http_endheaders)
            type.__setattr__(http.client.HTTPConnection, "getresponse", _observed_http_getresponse)
            type.__setattr__(http.client.HTTPConnection, "close", _observed_http_close)
            type.__setattr__(socket.socket, "connect", _observed_socket_connect)
            ModuleType.__setattr__(socket, "getaddrinfo", _observed_getaddrinfo)
            type.__setattr__(
                ssl.SSLObject,
                "do_handshake",
                _observed_ssl_object_handshake,
            )
            type.__setattr__(
                ssl.SSLSocket,
                "do_handshake",
                _observed_ssl_socket_handshake,
            )
        except BaseException:
            try:
                type.__setattr__(subprocess.Popen, "__init__", _original_init)
                type.__setattr__(subprocess.Popen, "wait", _original_wait)
                type.__setattr__(subprocess.Popen, "poll", _original_poll)
                type.__setattr__(subprocess.Popen, "communicate", _original_communicate)
                type.__setattr__(
                    http.client.HTTPConnection, "putrequest", _original_http_putrequest
                )
                type.__setattr__(
                    http.client.HTTPConnection, "endheaders", _original_http_endheaders
                )
                type.__setattr__(
                    http.client.HTTPConnection,
                    "getresponse",
                    _original_http_getresponse,
                )
                type.__setattr__(http.client.HTTPConnection, "close", _original_http_close)
                type.__setattr__(socket.socket, "connect", _original_socket_connect)
                ModuleType.__setattr__(socket, "getaddrinfo", _original_getaddrinfo)
                type.__setattr__(
                    ssl.SSLObject,
                    "do_handshake",
                    _original_ssl_object_handshake,
                )
                type.__setattr__(
                    ssl.SSLSocket,
                    "do_handshake",
                    _original_ssl_socket_handshake,
                )
            except BaseException:
                pass
            _record_callback_error()
            return
        _active = True
        _network_setup_adapters.update(NETWORK_SETUP_ADAPTERS)
        try:
            _install_lazy_adapters()
        except BaseException:
            _record_http_callback_error()
            _record_network_callback_error()
            if instrument_logical_operations:
                _record_logical_operation_callback_error()


def reset_after_fork() -> None:
    """Discard inherited parent evidence in a forked Python process."""

    global _callback_error_count, _caller_callback_error_count
    global _dropped_http_request_count, _dropped_network_connection_count
    global _dropped_logical_operation_count, _dropped_network_setup_count
    global _dropped_subprocess_count
    global _http_callback_error_count, _http_caller_callback_error_count
    global _network_callback_error_count, _network_caller_callback_error_count
    global _network_setup_callback_error_count
    global _network_setup_caller_callback_error_count
    global _logical_operation_callback_error_count
    global _logical_operation_caller_callback_error_count
    global _asyncio_task_scheduling_state, _lock
    global _next_http_identifier, _next_identifier, _next_network_identifier
    global _next_logical_operation_identifier, _next_network_setup_identifier
    _lock = threading.RLock()
    _asyncio_task_scheduling_state = threading.local()
    _records.clear()
    _process_record_ids.clear()
    _http_records.clear()
    _network_records.clear()
    _network_setup_records.clear()
    _logical_operation_records.clear()
    _tls_record_ids.clear()
    _connection_record_ids.clear()
    _active_record_keys.clear()
    _next_identifier = 0
    _next_http_identifier = 0
    _next_network_identifier = 0
    _next_network_setup_identifier = 0
    _next_logical_operation_identifier = 0
    _dropped_subprocess_count = 0
    _dropped_http_request_count = 0
    _dropped_network_connection_count = 0
    _dropped_network_setup_count = 0
    _dropped_logical_operation_count = 0
    _callback_error_count = 0
    _caller_callback_error_count = 0
    _http_callback_error_count = 0
    _http_caller_callback_error_count = 0
    _network_callback_error_count = 0
    _network_caller_callback_error_count = 0
    _network_setup_callback_error_count = 0
    _network_setup_caller_callback_error_count = 0
    _logical_operation_callback_error_count = 0
    _logical_operation_caller_callback_error_count = 0
    _network_capture_suppressed.set(False)
    _logical_operation_capture_suppressed.set(False)


def snapshot(*, registration_only: bool = False) -> dict[str, object]:
    """Return one bounded, JSON-compatible semantic snapshot."""

    if registration_only:
        records: list[dict[str, object]] = []
        http_records: list[dict[str, object]] = []
        network_records: list[dict[str, object]] = []
        network_setup_records: list[dict[str, object]] = []
        logical_operation_records: list[dict[str, object]] = []
        dropped_subprocess_count = 0
        dropped_http_request_count = 0
        dropped_network_connection_count = 0
        dropped_network_setup_count = 0
        dropped_logical_operation_count = 0
        callback_error_count = 0
        caller_callback_error_count = 0
        http_callback_error_count = 0
        http_caller_callback_error_count = 0
        network_callback_error_count = 0
        network_caller_callback_error_count = 0
        network_setup_callback_error_count = 0
        network_setup_caller_callback_error_count = 0
        logical_operation_callback_error_count = 0
        logical_operation_caller_callback_error_count = 0
    else:
        with _lock:
            records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(_records.items())
            ]
            http_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(_http_records.items())
            ]
            network_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(_network_records.items())
            ]
            network_setup_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(_network_setup_records.items())
            ]
            logical_operation_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(_logical_operation_records.items())
            ]
            dropped_subprocess_count = _dropped_subprocess_count
            dropped_http_request_count = _dropped_http_request_count
            dropped_network_connection_count = _dropped_network_connection_count
            dropped_network_setup_count = _dropped_network_setup_count
            dropped_logical_operation_count = _dropped_logical_operation_count
            callback_error_count = _callback_error_count
            caller_callback_error_count = _caller_callback_error_count
            http_callback_error_count = _http_callback_error_count
            http_caller_callback_error_count = _http_caller_callback_error_count
            network_callback_error_count = _network_callback_error_count
            network_caller_callback_error_count = _network_caller_callback_error_count
            network_setup_callback_error_count = _network_setup_callback_error_count
            network_setup_caller_callback_error_count = _network_setup_caller_callback_error_count
            logical_operation_callback_error_count = _logical_operation_callback_error_count
            logical_operation_caller_callback_error_count = (
                _logical_operation_caller_callback_error_count
            )
    with _lock:
        http_adapters = sorted(_http_adapters)
        network_adapters = sorted(_network_adapters)
        network_setup_adapters = sorted(_network_setup_adapters)
        logical_operation_adapters = sorted(_logical_operation_adapters)
        logical_operation_capture_enabled = _logical_operation_capture_enabled
    return {
        "format_version": FORMAT_VERSION,
        "observer": "python-runtime-boundary-wrapper",
        "subprocess_count": len(records),
        "dropped_subprocess_count": dropped_subprocess_count,
        "callback_error_count": callback_error_count,
        "caller_callback_error_count": caller_callback_error_count,
        "http_request_count": len(http_records),
        "dropped_http_request_count": dropped_http_request_count,
        "http_callback_error_count": http_callback_error_count,
        "http_caller_callback_error_count": http_caller_callback_error_count,
        "http_adapters": http_adapters,
        "network_connection_count": len(network_records),
        "dropped_network_connection_count": dropped_network_connection_count,
        "network_callback_error_count": network_callback_error_count,
        "network_caller_callback_error_count": network_caller_callback_error_count,
        "network_adapters": network_adapters,
        "network_setup_count": len(network_setup_records),
        "dropped_network_setup_count": dropped_network_setup_count,
        "network_setup_callback_error_count": network_setup_callback_error_count,
        "network_setup_caller_callback_error_count": (network_setup_caller_callback_error_count),
        "network_setup_adapters": network_setup_adapters,
        "logical_operation_capture_enabled": logical_operation_capture_enabled,
        "logical_operation_count": len(logical_operation_records),
        "dropped_logical_operation_count": dropped_logical_operation_count,
        "logical_operation_callback_error_count": logical_operation_callback_error_count,
        "logical_operation_caller_callback_error_count": (
            logical_operation_caller_callback_error_count
        ),
        "logical_operation_adapters": logical_operation_adapters,
        "server_identity_policy": "redact",
        "limits": {
            "max_subprocesses": MAX_SUBPROCESS_RECORDS,
            "max_http_requests": MAX_HTTP_REQUEST_RECORDS,
            "max_network_connections": MAX_NETWORK_CONNECTION_RECORDS,
            "max_network_setup_phases": MAX_NETWORK_SETUP_RECORDS,
            "max_logical_operations": MAX_LOGICAL_OPERATION_RECORDS,
        },
        "subprocesses": records,
        "http_requests": http_records,
        "network_connections": network_records,
        "network_setup_phases": network_setup_records,
        "logical_operations": logical_operation_records,
    }
