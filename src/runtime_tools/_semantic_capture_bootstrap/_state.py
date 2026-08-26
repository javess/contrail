"""Mutable state for the standalone semantic observer."""

from __future__ import annotations

import contextvars
import http.client
import socket
import ssl
import subprocess
import threading
from collections.abc import Callable
from typing import cast

from ._constants import ActiveRecordKey, CallerProvider


class SemanticState:
    """Own the observer state shared by independently patchable adapters."""

    def __init__(self) -> None:
        self._original_init = cast(Callable[..., None], subprocess.Popen.__init__)
        self._original_wait = cast(Callable[..., int], subprocess.Popen.wait)
        self._original_poll = cast(Callable[..., int | None], subprocess.Popen.poll)
        self._original_communicate = cast(Callable[..., object], subprocess.Popen.communicate)
        self._original_http_putrequest = cast(
            Callable[..., None], http.client.HTTPConnection.putrequest
        )
        self._original_http_endheaders = cast(
            Callable[..., None], http.client.HTTPConnection.endheaders
        )
        self._original_http_getresponse = cast(
            Callable[..., http.client.HTTPResponse], http.client.HTTPConnection.getresponse
        )
        self._original_http_close = cast(Callable[..., None], http.client.HTTPConnection.close)
        self._original_socket_connect = cast(Callable[..., None], socket.socket.connect)
        self._original_getaddrinfo = cast(Callable[..., object], socket.getaddrinfo)
        self._original_ssl_object_handshake = cast(Callable[..., None], ssl.SSLObject.do_handshake)
        self._original_ssl_socket_handshake = cast(Callable[..., None], ssl.SSLSocket.do_handshake)
        self._lock = threading.RLock()
        self._records: dict[int, dict[str, object]] = {}
        self._process_record_ids: dict[int, int] = {}
        self._http_records: dict[int, dict[str, object]] = {}
        self._network_records: dict[int, dict[str, object]] = {}
        self._network_setup_records: dict[int, dict[str, object]] = {}
        self._logical_operation_records: dict[int, dict[str, object]] = {}
        self._tls_record_ids: dict[int, tuple[object, int | None]] = {}
        self._connection_record_ids: dict[int, int] = {}
        self._active_record_keys: dict[int, list[ActiveRecordKey]] = {}
        self._caller_provider: CallerProvider | None = None
        self._next_identifier = 0
        self._next_http_identifier = 0
        self._next_network_identifier = 0
        self._next_network_setup_identifier = 0
        self._next_logical_operation_identifier = 0
        self._dropped_subprocess_count = 0
        self._dropped_http_request_count = 0
        self._dropped_network_connection_count = 0
        self._dropped_network_setup_count = 0
        self._dropped_logical_operation_count = 0
        self._callback_error_counts = {
            key: 0
            for key in (
                "callback_error_count",
                "caller_callback_error_count",
                "http_callback_error_count",
                "http_caller_callback_error_count",
                "network_callback_error_count",
                "network_caller_callback_error_count",
                "network_setup_callback_error_count",
                "network_setup_caller_callback_error_count",
                "logical_operation_callback_error_count",
                "logical_operation_caller_callback_error_count",
            )
        }
        self._http_adapters = {"stdlib.http.client"}
        self._network_adapters = {"stdlib.socket.connect"}
        self._network_setup_adapters: set[str] = set()
        self._logical_operation_adapters: set[str] = set()
        self._logical_operation_capture_enabled = False
        self._network_capture_suppressed = contextvars.ContextVar(
            "contrail_network_capture_suppressed", default=False
        )
        self._logical_operation_capture_suppressed = contextvars.ContextVar(
            "contrail_logical_operation_capture_suppressed", default=False
        )
        self._asyncio_task_scheduling_state = threading.local()
        self._active = False


STATE = SemanticState()
