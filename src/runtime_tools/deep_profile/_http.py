"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import heapq
from typing import Literal

from runtime_tools.deep_profile._common import (
    MAX_SEMANTIC_EXECUTABLE_CHARACTERS,
    MAX_SEMANTIC_HTTP_METHOD_CHARACTERS,
    MAX_SEMANTIC_HTTP_REQUEST_EVENTS,
    MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS,
    SEMANTIC_HTTP_ADAPTERS,
    DeepProfileError,
    _bounded_sum,
    _process_role,
)
from runtime_tools.deep_profile._evidence import (
    _HTTP_CALLER_CONTRACT,
    _HttpCaptureEvidence,
    _HttpRequestRecord,
    _SubprocessCaller,
)
from runtime_tools.deep_profile._parsing import (
    _caller_capture_status,
    _caller_event_id,
    _caller_evidence,
    _optional_nonnegative_integer,
    _parse_document,
    _retained_capture_status,
    _subprocess_caller,
)
from runtime_tools.deep_profile._session import _boolean, _integer, _list, _object, _text
from runtime_tools.model import Event, JsonValue


def _http_request_record(value: object, *, document_pid: int) -> _HttpRequestRecord:
    item = _object(value, "semantic HTTP request record")
    identifier = _integer(item.get("id"), "semantic HTTP request id")
    method = _text(item.get("method"), "semantic HTTP method")
    if len(method) > MAX_SEMANTIC_HTTP_METHOD_CHARACTERS or (
        method != "<method>"
        and (
            not method.isascii()
            or not method
            or not all(character in "ABCDEFGHIJKLMNOPQRSTUVWXYZ-" for character in method)
        )
    ):
        raise DeepProfileError("semantic HTTP method is unsupported")
    raw_scheme = item.get("scheme")
    scheme: Literal["http", "https"]
    if raw_scheme == "http":
        scheme = "http"
    elif raw_scheme == "https":
        scheme = "https"
    else:
        raise DeepProfileError("semantic HTTP scheme is unsupported")
    if item.get("server_address") is not None or item.get("server_identity_policy") != "redact":
        raise DeepProfileError("semantic HTTP server identity is not redacted")
    adapter = item.get("adapter", "stdlib.http.client")
    if not isinstance(adapter, str) or adapter not in SEMANTIC_HTTP_ADAPTERS:
        raise DeepProfileError("semantic HTTP adapter is unsupported")
    server_port = _optional_nonnegative_integer(
        item.get("server_port"),
        "semantic HTTP server port",
    )
    if server_port is not None and not 0 < server_port <= 65_535:
        raise DeepProfileError("semantic HTTP server port is invalid")
    parent_pid = _integer(item.get("parent_pid"), "semantic HTTP parent process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic HTTP parent process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic HTTP request start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic HTTP request start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic HTTP request duration",
    )
    status_code = _optional_nonnegative_integer(
        item.get("status_code"),
        "semantic HTTP response status",
    )
    if status_code is not None and not 100 <= status_code <= 999:
        raise DeepProfileError("semantic HTTP response status is invalid")
    raw_outcome = item.get("outcome")
    outcome: Literal["response", "request_error", "closed", "unknown"]
    if raw_outcome == "response":
        outcome = "response"
    elif raw_outcome == "request_error":
        outcome = "request_error"
    elif raw_outcome == "closed":
        outcome = "closed"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic HTTP request outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic HTTP request error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic HTTP request error type exceeds its character limit")
    if outcome == "response" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("semantic HTTP response evidence is inconsistent")
    if outcome == "request_error" and (
        duration_ns is None or status_code is not None or error_type is None
    ):
        raise DeepProfileError("failed semantic HTTP request evidence is inconsistent")
    if outcome == "closed" and (
        duration_ns is None or status_code is not None or error_type is not None
    ):
        raise DeepProfileError("closed semantic HTTP request evidence is inconsistent")
    if outcome == "unknown" and (
        duration_ns is not None or status_code is not None or error_type is not None
    ):
        raise DeepProfileError("unfinished semantic HTTP request evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic HTTP request finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _HttpRequestRecord(
        identifier,
        method,
        scheme,
        server_port,
        parent_pid,
        started_at_ns,
        duration_ns,
        status_code,
        outcome,
        error_type,
        caller,
        caller_status,
        adapter,
    )


def _http_request_event_id(record: _HttpRequestRecord) -> str:
    return f"semantic:http:{record.parent_pid}:{record.identifier}"


def _http_request_event(
    record: _HttpRequestRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-http-client-wrapper",
        "adapter": record.adapter,
        "method": record.method,
        "scheme": record.scheme,
        "server_address": None,
        "server_port": record.server_port,
        "server_identity_policy": "redact",
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "headers_captured": False,
        "body_captured": False,
        "path_captured": False,
        "query_captured": False,
        "response_body_captured": False,
        "duration_boundary": "response_headers",
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _caller_event_id(
            _HTTP_CALLER_CONTRACT, record.caller.identity
        )
    if record.status_code is not None:
        attributes["status_code"] = record.status_code
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_http_request_event_id(record),
        kind="http.client.request",
        name=f"HTTP {record.method}",
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _http_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _HttpCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _HttpRequestRecord]] = []
    process_count = 0
    dropped_request_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_request_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if semantic.get("format_version") == 1:
                saw_missing = True
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic HTTP capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_http_requests"),
                    "semantic HTTP request limit",
                )
                != MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS
            ):
                raise DeepProfileError("semantic HTTP request limit is unsupported")
            if semantic.get("server_identity_policy") != "redact":
                raise DeepProfileError("semantic HTTP server identity policy is unsupported")
            raw_adapters = _list(
                semantic.get("http_adapters", ["stdlib.http.client"]),
                "semantic HTTP adapters",
            )
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic HTTP adapter")
                if adapter not in SEMANTIC_HTTP_ADAPTERS or adapter in document_adapters:
                    raise DeepProfileError("semantic HTTP adapters are unsupported")
                document_adapters.add(adapter)
            if "stdlib.http.client" not in document_adapters:
                raise DeepProfileError("semantic HTTP standard-library adapter is missing")
            pid = _integer(document.get("pid"), "semantic HTTP capture process id")
            raw_records = _list(semantic.get("http_requests"), "semantic HTTP requests")
            declared_count = _integer(
                semantic.get("http_request_count"),
                "semantic HTTP request count",
            )
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS
            ):
                raise DeepProfileError("semantic HTTP request count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_http_request_count"),
                "semantic dropped HTTP request count",
            )
            document_callback_errors = _integer(
                semantic.get("http_callback_error_count"),
                "semantic HTTP callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("http_caller_callback_error_count"),
                "semantic HTTP caller callback error count",
            )
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic HTTP registration contains evidence")
            records = tuple(
                _http_request_record(raw_record, document_pid=pid) for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic HTTP request adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic HTTP ids must be unique per process")
                identifiers.add(record.identifier)
            process_count += 1
            dropped_request_count = _bounded_sum(
                dropped_request_count,
                document_dropped_count,
                "semantic dropped HTTP request count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic HTTP callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic HTTP caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_request_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = saw_incomplete or (
                    record.outcome in {"closed", "unknown"}
                    or record.method == "<method>"
                    or (record.outcome == "response" and record.status_code is None)
                )
                important = int(
                    record.outcome != "response"
                    or (record.status_code is not None and record.status_code >= 500)
                )
                rank = (
                    important,
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_HTTP_REQUEST_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _HttpCaptureEvidence(
            (),
            (),
            (),
            "invalid",
            process_count,
            0,
            0,
            "invalid",
            0,
            0,
            invalid_caller_count,
            0,
            (),
        )
    if process_count == 0:
        return _HttpCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_request_count = _bounded_sum(
        dropped_request_count,
        valid_request_count - len(selected_records),
        "semantic dropped HTTP request count",
    )
    status = _retained_capture_status(
        partial=saw_missing or saw_checkpoint or saw_incomplete,
        dropped_count=dropped_request_count,
        callback_error_count=callback_error_count,
    )
    caller_events, caller_edges, attributed_request_count, unattributed_request_count = (
        _caller_evidence(
            selected_records,
            contract=_HTTP_CALLER_CONTRACT,
            entity_id=entity_id,
            target_event_id=_http_request_event_id,
        )
    )
    caller_status = _caller_capture_status(
        invalid_count=invalid_caller_count,
        partial=saw_missing or saw_checkpoint,
        attributed_count=attributed_request_count,
        unattributed_count=unattributed_request_count,
        callback_error_count=caller_callback_error_count,
        dropped_count=dropped_request_count,
    )
    return _HttpCaptureEvidence(
        tuple(
            _http_request_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_request_count,
        callback_error_count,
        caller_status,
        attributed_request_count,
        unattributed_request_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )
