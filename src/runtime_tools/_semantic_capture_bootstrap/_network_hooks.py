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

import functools
import http.client
import ssl
from collections.abc import Callable

from ._boundaries import (
    _begin_active_http,
    _begin_active_identifier,
    _finish_http_record,
    _finish_logical_operation_record,
    _finish_network_record,
    _finish_network_setup_record,
    _finish_scheduled_task,
    _http_record_identifier,
    _http_status,
    _network_target,
    _release_http_record,
    _reserve_http_record,
    _reserve_logical_operation_boundary,
    _reserve_network_boundary,
    _reserve_network_setup_boundary,
)
from ._constants import ActiveToken, NetworkSuppressionToken
from ._records import (
    _begin_network_suppression,
    _end_active,
    _end_network_suppression,
    _record_http_callback_error,
    _record_http_caller_callback_error,
    _record_logical_operation_callback_error,
    _record_network_callback_error,
    _record_network_caller_callback_error,
    _record_network_setup_callback_error,
    _record_network_setup_caller_callback_error,
)
from ._state import STATE


def _asyncio_task_scheduling_origin() -> str | None:
    origins = getattr(STATE._asyncio_task_scheduling_state, "origins", None)
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
    origins = getattr(STATE._asyncio_task_scheduling_state, "origins", None)
    if not isinstance(origins, list):
        origins = []
        STATE._asyncio_task_scheduling_state.origins = origins
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
    if adapter is None or STATE._logical_operation_capture_suppressed.get():
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


@functools.wraps(STATE._original_getaddrinfo)
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
            active_token = _begin_active_identifier("network_setup", identifier)
    except BaseException:
        _record_network_setup_callback_error()
    try:
        result = STATE._original_getaddrinfo(host, port, *positional, **keywords)
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
    with STATE._lock:
        active = STATE._tls_record_ids.get(connection_identity)
        if active is not None and active[0] is connection:
            return active[1]
    identifier = _reserve_network_setup_boundary(phase="tls", adapter=adapter)
    with STATE._lock:
        STATE._tls_record_ids[connection_identity] = (connection, identifier)
    return identifier


def _release_tls_record(connection: object) -> None:
    connection_identity = id(connection)
    with STATE._lock:
        active = STATE._tls_record_ids.get(connection_identity)
        if active is not None and active[0] is connection:
            STATE._tls_record_ids.pop(connection_identity, None)


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
        active_token = _begin_active_identifier("network_setup", identifier)
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


@functools.wraps(STATE._original_ssl_object_handshake)
def _observed_ssl_object_handshake(
    connection: object,
    *positional: object,
    **keywords: object,
) -> None:
    _observe_tls_handshake(
        STATE._original_ssl_object_handshake,
        "stdlib.ssl.SSLObject.do_handshake",
        connection,
        positional,
        keywords,
    )


@functools.wraps(STATE._original_ssl_socket_handshake)
def _observed_ssl_socket_handshake(
    connection: object,
    *positional: object,
    **keywords: object,
) -> None:
    _observe_tls_handshake(
        STATE._original_ssl_socket_handshake,
        "stdlib.ssl.SSLSocket.do_handshake",
        connection,
        positional,
        keywords,
    )


@functools.wraps(STATE._original_socket_connect)
def _observed_socket_connect(
    connection: object,
    address: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier: int | None = None
    active_token: ActiveToken | None = None
    try:
        target = (
            None
            if STATE._network_capture_suppressed.get()
            else _network_target(connection, address)
        )
        if target is not None:
            transport, family, server_port = target
            identifier = _reserve_network_boundary(
                transport=transport,
                family=family,
                server_port=server_port,
                adapter="stdlib.socket.connect",
                tls_requested=None,
            )
            active_token = _begin_active_identifier("network", identifier)
    except BaseException:
        _record_network_callback_error()
    try:
        STATE._original_socket_connect(connection, address, *positional, **keywords)
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


@functools.wraps(STATE._original_http_putrequest)
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
        STATE._original_http_putrequest(connection, method, url, *positional, **keywords)
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


@functools.wraps(STATE._original_http_endheaders)
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
        STATE._original_http_endheaders(connection, *positional, **keywords)
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


@functools.wraps(STATE._original_http_getresponse)
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
        response = STATE._original_http_getresponse(connection, *positional, **keywords)
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


@functools.wraps(STATE._original_http_close)
def _observed_http_close(
    connection: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier = _http_record_identifier(connection)
    try:
        STATE._original_http_close(connection, *positional, **keywords)
    finally:
        try:
            _finish_http_record(identifier, status_code=None, outcome="closed")
            _release_http_record(connection)
        except BaseException:
            _record_http_callback_error()
