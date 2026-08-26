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

import http.client
import socket
import ssl
import subprocess
import threading
from types import ModuleType

from ._boundaries import (
    begin_asgi_request,
    begin_wsgi_request,
    finish_asgi_request,
    finish_wsgi_request,
    record_asgi_status,
    record_wsgi_status,
)
from ._constants import (
    FORMAT_VERSION,
    MAX_HTTP_REQUEST_RECORDS,
    MAX_LOGICAL_OPERATION_RECORDS,
    MAX_NETWORK_CONNECTION_RECORDS,
    MAX_NETWORK_SETUP_RECORDS,
    MAX_SUBPROCESS_RECORDS,
    NETWORK_SETUP_ADAPTERS,
    ActiveToken,
    CallerProvider,
)
from ._importer import _install_lazy_adapters
from ._network_hooks import (
    _observed_getaddrinfo,
    _observed_http_close,
    _observed_http_endheaders,
    _observed_http_getresponse,
    _observed_http_putrequest,
    _observed_socket_connect,
    _observed_ssl_object_handshake,
    _observed_ssl_socket_handshake,
)
from ._records import (
    _begin_network_suppression,
    _end_network_suppression,
    _record_callback_error,
    _record_http_callback_error,
    _record_logical_operation_callback_error,
    _record_network_callback_error,
    attribute_active_caller,
    attribute_logical_operation_caller,
)
from ._state import STATE
from ._subprocess_hooks import _observed_communicate, _observed_init, _observed_poll, _observed_wait

__all__ = [
    "ActiveToken",
    "activate",
    "attribute_active_caller",
    "attribute_logical_operation_caller",
    "begin_asgi_request",
    "begin_wsgi_request",
    "finish_asgi_request",
    "finish_wsgi_request",
    "record_asgi_status",
    "record_wsgi_status",
    "reset_after_fork",
    "snapshot",
    "_begin_network_suppression",
    "_end_network_suppression",
]


def activate(
    caller_provider: CallerProvider | None = None,
    *,
    instrument_logical_operations: bool = False,
) -> None:
    """Install bounded boundary observers for the selected capture level."""

    with STATE._lock:
        if caller_provider is not None:
            STATE._caller_provider = caller_provider
        STATE._logical_operation_capture_enabled = instrument_logical_operations
        if STATE._active:
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
                type.__setattr__(subprocess.Popen, "__init__", STATE._original_init)
                type.__setattr__(subprocess.Popen, "wait", STATE._original_wait)
                type.__setattr__(subprocess.Popen, "poll", STATE._original_poll)
                type.__setattr__(subprocess.Popen, "communicate", STATE._original_communicate)
                type.__setattr__(
                    http.client.HTTPConnection, "putrequest", STATE._original_http_putrequest
                )
                type.__setattr__(
                    http.client.HTTPConnection, "endheaders", STATE._original_http_endheaders
                )
                type.__setattr__(
                    http.client.HTTPConnection,
                    "getresponse",
                    STATE._original_http_getresponse,
                )
                type.__setattr__(http.client.HTTPConnection, "close", STATE._original_http_close)
                type.__setattr__(socket.socket, "connect", STATE._original_socket_connect)
                ModuleType.__setattr__(socket, "getaddrinfo", STATE._original_getaddrinfo)
                type.__setattr__(
                    ssl.SSLObject,
                    "do_handshake",
                    STATE._original_ssl_object_handshake,
                )
                type.__setattr__(
                    ssl.SSLSocket,
                    "do_handshake",
                    STATE._original_ssl_socket_handshake,
                )
            except BaseException:
                pass
            _record_callback_error()
            return
        STATE._active = True
        STATE._network_setup_adapters.update(NETWORK_SETUP_ADAPTERS)
        try:
            _install_lazy_adapters()
        except BaseException:
            _record_http_callback_error()
            _record_network_callback_error()
            if instrument_logical_operations:
                _record_logical_operation_callback_error()


def reset_after_fork() -> None:
    """Discard inherited parent evidence in a forked Python process."""

    STATE._lock = threading.RLock()
    STATE._asyncio_task_scheduling_state = threading.local()
    STATE._records.clear()
    STATE._process_record_ids.clear()
    STATE._http_records.clear()
    STATE._network_records.clear()
    STATE._network_setup_records.clear()
    STATE._logical_operation_records.clear()
    STATE._tls_record_ids.clear()
    STATE._connection_record_ids.clear()
    STATE._active_record_keys.clear()
    STATE._next_identifier = 0
    STATE._next_http_identifier = 0
    STATE._next_network_identifier = 0
    STATE._next_network_setup_identifier = 0
    STATE._next_logical_operation_identifier = 0
    STATE._dropped_subprocess_count = 0
    STATE._dropped_http_request_count = 0
    STATE._dropped_network_connection_count = 0
    STATE._dropped_network_setup_count = 0
    STATE._dropped_logical_operation_count = 0
    for key in STATE._callback_error_counts:
        STATE._callback_error_counts[key] = 0
    STATE._network_capture_suppressed.set(False)
    STATE._logical_operation_capture_suppressed.set(False)


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
        callback_errors = {key: 0 for key in STATE._callback_error_counts}
    else:
        with STATE._lock:
            records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(STATE._records.items())
            ]
            http_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(STATE._http_records.items())
            ]
            network_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(STATE._network_records.items())
            ]
            network_setup_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(STATE._network_setup_records.items())
            ]
            logical_operation_records = [
                {key: value for key, value in record.items() if not key.startswith("_")}
                for _, record in sorted(STATE._logical_operation_records.items())
            ]
            dropped_subprocess_count = STATE._dropped_subprocess_count
            dropped_http_request_count = STATE._dropped_http_request_count
            dropped_network_connection_count = STATE._dropped_network_connection_count
            dropped_network_setup_count = STATE._dropped_network_setup_count
            dropped_logical_operation_count = STATE._dropped_logical_operation_count
            callback_errors = dict(STATE._callback_error_counts)
    with STATE._lock:
        http_adapters = sorted(STATE._http_adapters)
        network_adapters = sorted(STATE._network_adapters)
        network_setup_adapters = sorted(STATE._network_setup_adapters)
        logical_operation_adapters = sorted(STATE._logical_operation_adapters)
        logical_operation_capture_enabled = STATE._logical_operation_capture_enabled
    return {
        "format_version": FORMAT_VERSION,
        "observer": "python-runtime-boundary-wrapper",
        "subprocess_count": len(records),
        "dropped_subprocess_count": dropped_subprocess_count,
        "http_request_count": len(http_records),
        "dropped_http_request_count": dropped_http_request_count,
        "http_adapters": http_adapters,
        "network_connection_count": len(network_records),
        "dropped_network_connection_count": dropped_network_connection_count,
        "network_adapters": network_adapters,
        "network_setup_count": len(network_setup_records),
        "dropped_network_setup_count": dropped_network_setup_count,
        "network_setup_adapters": network_setup_adapters,
        "logical_operation_capture_enabled": logical_operation_capture_enabled,
        "logical_operation_count": len(logical_operation_records),
        "dropped_logical_operation_count": dropped_logical_operation_count,
        **callback_errors,
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
