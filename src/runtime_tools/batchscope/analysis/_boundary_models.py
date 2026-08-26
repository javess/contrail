"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from runtime_tools.batchscope.analysis._common import LogicalOperationCategory, LogicalOperationName
from runtime_tools.json_support import JsonValueModel


@dataclass(frozen=True, slots=True)
class CallerAttribution(JsonValueModel):
    event_id: str
    name: str
    module: str
    qualname: str
    filename: str
    firstlineno: int
    scope: Literal["application", "library", "runtime"]
    observation: Literal["exact", "sampled"]
    confidence: float


SubprocessCaller = CallerAttribution


HttpCaller = CallerAttribution


NetworkCaller = CallerAttribution


@dataclass(frozen=True, slots=True)
class SubprocessCall(JsonValueModel):
    event_id: str
    name: str
    parent_pid: int
    role: Literal["root", "descendant", "unknown"]
    child_pid: int | None
    child_observed_in_process_tree: bool
    shell: bool | None
    outcome: Literal["exited", "launch_error", "unknown"]
    exit_code: int | None
    error_type: str | None
    started_at_ns: int
    duration_seconds: float | None
    caller: CallerAttribution | None = None


@dataclass(frozen=True, slots=True)
class HttpCaptureSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    process_count: int
    request_count: int
    dropped_request_count: int
    callback_error_count: int
    invalid_event_count: int
    caller_attribution_status: Literal[
        "complete", "partial", "truncated", "unavailable", "invalid"
    ] = "unavailable"
    caller_count: int = 0
    attributed_request_count: int = 0
    unattributed_request_count: int = 0
    invalid_caller_count: int = 0
    caller_callback_error_count: int = 0
    adapters: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class HttpRequest(JsonValueModel):
    event_id: str
    method: str
    scheme: Literal["http", "https"]
    server_port: int | None
    parent_pid: int
    role: Literal["root", "descendant", "unknown"]
    outcome: Literal["response", "request_error", "closed", "unknown"]
    status_code: int | None
    error_type: str | None
    started_at_ns: int
    duration_seconds: float | None
    caller: CallerAttribution | None = None
    adapter: str = "stdlib.http.client"


@dataclass(frozen=True, slots=True)
class NetworkCaptureSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    process_count: int
    connection_count: int
    dropped_connection_count: int
    callback_error_count: int
    invalid_event_count: int
    caller_attribution_status: Literal[
        "complete", "partial", "truncated", "unavailable", "invalid"
    ] = "unavailable"
    caller_count: int = 0
    attributed_connection_count: int = 0
    unattributed_connection_count: int = 0
    invalid_caller_count: int = 0
    caller_callback_error_count: int = 0
    adapters: tuple[str, ...] = ()
    connection_hotspot_count: int = 0


@dataclass(frozen=True, slots=True)
class NetworkSetupCaptureSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    process_count: int
    phase_count: int
    dropped_phase_count: int
    callback_error_count: int
    invalid_event_count: int
    caller_attribution_status: Literal[
        "complete", "partial", "truncated", "unavailable", "invalid"
    ] = "unavailable"
    caller_count: int = 0
    attributed_phase_count: int = 0
    unattributed_phase_count: int = 0
    invalid_caller_count: int = 0
    caller_callback_error_count: int = 0
    adapters: tuple[str, ...] = ()
    hotspot_count: int = 0


@dataclass(frozen=True, slots=True)
class NetworkSetupPhase(JsonValueModel):
    event_id: str
    phase: Literal["dns", "tls"]
    adapter: str
    parent_pid: int
    role: Literal["root", "descendant", "unknown"]
    outcome: Literal["completed", "setup_error", "unknown"]
    error_type: str | None
    started_at_ns: int
    duration_seconds: float | None
    caller: CallerAttribution | None = None


@dataclass(frozen=True, slots=True)
class NetworkSetupHotspot(JsonValueModel):
    phase: Literal["dns", "tls"]
    adapter: str
    phase_count: int
    completed_phase_count: int
    failed_phase_count: int
    unfinished_phase_count: int
    total_duration_seconds: float
    max_duration_seconds: float
    caller: CallerAttribution | None = None


@dataclass(slots=True)
class _NetworkSetupHotspotAggregate:
    phase: Literal["dns", "tls"]
    adapter: str
    caller: CallerAttribution | None
    phase_count: int = 0
    completed_phase_count: int = 0
    failed_phase_count: int = 0
    unfinished_phase_count: int = 0
    total_duration_seconds: float = 0.0
    max_duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class LogicalOperationCaptureSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    process_count: int
    operation_count: int
    dropped_operation_count: int
    callback_error_count: int
    invalid_event_count: int
    caller_attribution_status: Literal[
        "complete", "partial", "truncated", "unavailable", "invalid"
    ] = "unavailable"
    caller_count: int = 0
    attributed_operation_count: int = 0
    unattributed_operation_count: int = 0
    invalid_caller_count: int = 0
    caller_callback_error_count: int = 0
    adapters: tuple[str, ...] = ()
    hotspot_count: int = 0


@dataclass(frozen=True, slots=True)
class LogicalOperation(JsonValueModel):
    event_id: str
    category: LogicalOperationCategory
    operation: LogicalOperationName
    adapter: str
    parent_pid: int
    role: Literal["root", "descendant", "unknown"]
    outcome: Literal["completed", "operation_error", "unknown"]
    error_type: str | None
    status_code: int | None
    started_at_ns: int
    duration_seconds: float | None
    caller: CallerAttribution | None = None


@dataclass(frozen=True, slots=True)
class LogicalOperationHotspot(JsonValueModel):
    category: LogicalOperationCategory
    operation: LogicalOperationName
    adapter: str
    operation_count: int
    completed_operation_count: int
    failed_operation_count: int
    unfinished_operation_count: int
    total_duration_seconds: float
    max_duration_seconds: float
    caller: CallerAttribution | None = None


@dataclass(slots=True)
class _LogicalOperationHotspotAggregate:
    category: LogicalOperationCategory
    operation: LogicalOperationName
    adapter: str
    caller: CallerAttribution | None
    operation_count: int = 0
    completed_operation_count: int = 0
    failed_operation_count: int = 0
    unfinished_operation_count: int = 0
    total_duration_seconds: float = 0.0
    max_duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class NetworkConnection(JsonValueModel):
    event_id: str
    adapter: str
    transport: Literal["tcp", "unix"]
    address_family: Literal["ipv4", "ipv6", "unix", "unknown"]
    server_port: int | None
    tls_requested: bool | None
    parent_pid: int
    role: Literal["root", "descendant", "unknown"]
    outcome: Literal["connected", "connect_error", "unknown"]
    error_type: str | None
    started_at_ns: int
    duration_seconds: float | None
    caller: CallerAttribution | None = None


@dataclass(frozen=True, slots=True)
class NetworkConnectionHotspot(JsonValueModel):
    adapter: str
    connection_count: int
    connected_connection_count: int
    failed_connection_count: int
    unfinished_connection_count: int
    total_duration_seconds: float
    max_duration_seconds: float
    caller: CallerAttribution | None = None


@dataclass(slots=True)
class _NetworkConnectionHotspotAggregate:
    adapter: str
    caller: CallerAttribution | None
    connection_count: int = 0
    connected_connection_count: int = 0
    failed_connection_count: int = 0
    unfinished_connection_count: int = 0
    total_duration_seconds: float = 0.0
    max_duration_seconds: float = 0.0


@dataclass(slots=True)
class _ProcessResourceAggregate:
    name: str
    pid: int
    parent_name: str | None
    sample_count: int = 0
    peak_rss_bytes: float = 0.0
    cpu_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class _ObservedProcessIdentity:
    name: str
    parent_name: str | None
