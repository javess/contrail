"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from typing import Literal

from runtime_tools.batchscope.analysis._boundary_models import HttpCaptureSummary, HttpRequest
from runtime_tools.batchscope.analysis._common import (
    HTTP_CAPTURE_ADAPTERS,
    MAX_HTTP_REQUEST_SUMMARIES,
)
from runtime_tools.batchscope.analysis._profile_summary import (
    _caller_attribution_counts,
    _capture_status,
    _semantic_count,
)
from runtime_tools.batchscope.analysis._subprocess import _HTTP_CALLER_CONTRACT, _operation_callers
from runtime_tools.model import CausalEdge, Event, JsonValue


def _http_requests(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[HttpRequest, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_request, invalid_caller_count = _operation_callers(
        events, edges, _HTTP_CALLER_CONTRACT
    )
    requests: list[HttpRequest] = []
    invalid_event_count = 0
    for event in events:
        if event.kind != "http.client.request":
            continue
        method = event.attributes.get("method")
        adapter = event.attributes.get("adapter", "stdlib.http.client")
        raw_scheme = event.attributes.get("scheme")
        server_port = event.attributes.get("server_port")
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        raw_outcome = event.attributes.get("outcome")
        status_code = event.attributes.get("status_code")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        if (
            event.started_at_ns is None
            or event.attributes.get("source") != "python-http-client-wrapper"
            or not isinstance(method, str)
            or not method
            or len(method) > 32
            or (
                method != "<method>"
                and (
                    not method.isascii()
                    or not all(character in "ABCDEFGHIJKLMNOPQRSTUVWXYZ-" for character in method)
                )
            )
            or event.name != f"HTTP {method}"
            or not isinstance(adapter, str)
            or adapter not in HTTP_CAPTURE_ADAPTERS
            or raw_scheme not in {"http", "https"}
            or event.attributes.get("server_address") is not None
            or event.attributes.get("server_identity_policy") != "redact"
            or (
                server_port is not None
                and (
                    not isinstance(server_port, int)
                    or isinstance(server_port, bool)
                    or not 0 < server_port <= 65_535
                )
            )
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or raw_outcome not in {"response", "request_error", "closed", "unknown"}
            or (
                status_code is not None
                and (
                    not isinstance(status_code, int)
                    or isinstance(status_code, bool)
                    or not 100 <= status_code <= 999
                )
            )
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
            or event.attributes.get("headers_captured") is not False
            or event.attributes.get("body_captured") is not False
            or event.attributes.get("path_captured") is not False
            or event.attributes.get("query_captured") is not False
            or event.attributes.get("response_body_captured") is not False
            or event.attributes.get("duration_boundary") != "response_headers"
        ):
            invalid_event_count += 1
            continue
        role: Literal["root", "descendant", "unknown"]
        if raw_role == "root":
            role = "root"
        elif raw_role == "descendant":
            role = "descendant"
        else:
            role = "unknown"
        scheme: Literal["http", "https"] = "http" if raw_scheme == "http" else "https"
        outcome: Literal["response", "request_error", "closed", "unknown"]
        if raw_outcome == "response":
            outcome = "response"
        elif raw_outcome == "request_error":
            outcome = "request_error"
        elif raw_outcome == "closed":
            outcome = "closed"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "response"
                and (event.finished_at_ns is None or error_type is not None or error is True)
            )
            or (
                outcome == "request_error"
                and (
                    event.finished_at_ns is None
                    or status_code is not None
                    or error_type is None
                    or error is not True
                )
            )
            or (
                outcome == "closed"
                and (
                    event.finished_at_ns is None
                    or status_code is not None
                    or error_type is not None
                    or error is True
                )
            )
            or (
                outcome == "unknown"
                and (
                    event.finished_at_ns is not None
                    or status_code is not None
                    or error_type is not None
                    or error is True
                )
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        requests.append(
            HttpRequest(
                event.id,
                method,
                scheme,
                server_port,
                parent_pid,
                role,
                outcome,
                status_code,
                error_type,
                event.started_at_ns,
                duration_seconds,
                caller_by_request.get(event.id),
                adapter,
            )
        )
    requests.sort(key=lambda request: (request.started_at_ns, request.parent_pid, request.event_id))
    return (
        tuple(requests[:MAX_HTTP_REQUEST_SUMMARIES]),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_request.values()}),
        len(caller_by_request),
        tuple(sorted({request.adapter for request in requests})),
    )


def _http_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    request_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_request_count: int,
    invalid_caller_count: int,
    observed_adapters: tuple[str, ...],
) -> HttpCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    raw_http = instrumentation.get("http_capture")
    if not isinstance(raw_http, dict):
        return None
    status = _capture_status(raw_http.get("status"))

    raw_counts = {
        key: _semantic_count(raw_http.get(key))
        for key in (
            "process_count",
            "request_count",
            "dropped_request_count",
            "callback_error_count",
        )
    }
    raw_adapters = raw_http.get("adapters", ["stdlib.http.client"])
    adapters: tuple[str, ...] = ()
    adapters_invalid = not isinstance(raw_adapters, list)
    if isinstance(raw_adapters, list):
        adapter_values = [adapter for adapter in raw_adapters if isinstance(adapter, str)]
        if len(raw_adapters) > len(HTTP_CAPTURE_ADAPTERS) or len(adapter_values) != len(
            raw_adapters
        ):
            adapters_invalid = True
        else:
            adapters = tuple(sorted(adapter_values))
            adapters_invalid = (
                len(adapters) != len(set(adapters))
                or "stdlib.http.client" not in adapters
                or any(adapter not in HTTP_CAPTURE_ADAPTERS for adapter in adapters)
                or any(adapter not in adapters for adapter in observed_adapters)
            )
    declared_request_count = raw_counts["request_count"]
    if (
        raw_http.get("observer") != "python-http-client-wrapper"
        or raw_http.get("zero_code") is not True
        or raw_http.get("server_identity_policy") != "redact"
        or raw_http.get("method_captured") is not True
        or raw_http.get("scheme_captured") is not True
        or raw_http.get("server_address_captured") is not False
        or raw_http.get("path_captured") is not False
        or raw_http.get("query_captured") is not False
        or raw_http.get("headers_captured") is not False
        or raw_http.get("body_captured") is not False
        or raw_http.get("response_body_captured") is not False
        or any(value is None for value in raw_counts.values())
        or adapters_invalid
        or declared_request_count != request_event_count
        or invalid_event_count
    ):
        status = "invalid"
    caller = _caller_attribution_counts(
        raw_http.get("caller_attribution"),
        target_name="request",
        target_count=request_event_count,
        caller_count=caller_count,
        attributed_count=attributed_request_count,
        invalid_count=invalid_caller_count,
    )

    return HttpCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_request_count or 0,
        raw_counts["dropped_request_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller.status,
        caller.caller_count,
        caller.attributed_count,
        caller.unattributed_count,
        caller.invalid_count,
        caller.callback_error_count,
        adapters,
    )
