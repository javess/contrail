"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

from runtime_tools.deep_profile._common import (
    CallerObservation,
    LogicalOperationCategory,
    LogicalOperationName,
    SemanticCaptureStatus,
)
from runtime_tools.model import CausalEdge, Event


@dataclass(frozen=True, slots=True)
class _FunctionIdentity:
    module: str
    qualname: str
    filename: str
    firstlineno: int
    scope: str

    @property
    def name(self) -> str:
        return f"{self.module}.{self.qualname}"

    @property
    def native(self) -> bool:
        return self.filename == "<native>"


@dataclass(frozen=True, slots=True)
class _SubprocessRecord:
    identifier: int
    name: str
    parent_pid: int
    child_pid: int | None
    shell: bool | None
    started_at_ns: int
    duration_ns: int | None
    exit_code: int | None
    outcome: Literal["exited", "launch_error", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _SubprocessCaller:
    identity: _FunctionIdentity
    observation: CallerObservation


class _CallerRecord(Protocol):
    @property
    def caller(self) -> _SubprocessCaller | None: ...


@dataclass(frozen=True, slots=True)
class _CallerEventContract:
    namespace: str
    source: str
    count_attribute: str
    relation: str


_SUBPROCESS_CALLER_CONTRACT = _CallerEventContract(
    "semantic:caller", "python-subprocess-wrapper", "subprocess_count", "launches"
)


_HTTP_CALLER_CONTRACT = _CallerEventContract(
    "semantic:http-caller", "python-http-client-wrapper", "http_request_count", "requests"
)


_NETWORK_CALLER_CONTRACT = _CallerEventContract(
    "semantic:network-caller",
    "python-network-connection-wrapper",
    "network_connection_count",
    "connects",
)


_NETWORK_SETUP_CALLER_CONTRACT = _CallerEventContract(
    "semantic:network-setup-caller",
    "python-network-setup-wrapper",
    "network_setup_phase_count",
    "resolves",
)


_LOGICAL_OPERATION_CALLER_CONTRACT = _CallerEventContract(
    "semantic:logical-operation-caller",
    "python-logical-operation-wrapper",
    "logical_operation_count",
    "performs",
)


@dataclass(frozen=True, slots=True)
class _SemanticCaptureEvidence:
    subprocess_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_subprocess_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_subprocess_count: int
    unattributed_subprocess_count: int
    invalid_caller_count: int
    caller_callback_error_count: int


@dataclass(frozen=True, slots=True)
class _HttpRequestRecord:
    identifier: int
    method: str
    scheme: Literal["http", "https"]
    server_port: int | None
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    status_code: int | None
    outcome: Literal["response", "request_error", "closed", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]
    adapter: str


@dataclass(frozen=True, slots=True)
class _HttpCaptureEvidence:
    request_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_request_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_request_count: int
    unattributed_request_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _NetworkConnectionRecord:
    identifier: int
    adapter: str
    transport: Literal["tcp", "unix"]
    address_family: Literal["ipv4", "ipv6", "unix", "unknown"]
    server_port: int | None
    tls_requested: bool | None
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    outcome: Literal["connected", "connect_error", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _NetworkCaptureEvidence:
    connection_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_connection_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_connection_count: int
    unattributed_connection_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _NetworkSetupRecord:
    identifier: int
    phase: Literal["dns", "tls"]
    adapter: str
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    outcome: Literal["completed", "setup_error", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _NetworkSetupCaptureEvidence:
    phase_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_phase_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_phase_count: int
    unattributed_phase_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _LogicalOperationRecord:
    identifier: int
    category: LogicalOperationCategory
    operation: LogicalOperationName
    adapter: str
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    outcome: Literal["completed", "operation_error", "unknown"]
    error_type: str | None
    status_code: int | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _LogicalOperationCaptureEvidence:
    operation_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_operation_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_operation_count: int
    unattributed_operation_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _BoundaryEvidence:
    semantic: _SemanticCaptureEvidence
    http: _HttpCaptureEvidence
    network: _NetworkCaptureEvidence
    network_setup: _NetworkSetupCaptureEvidence
    logical_operation: _LogicalOperationCaptureEvidence

    @property
    def events(self) -> tuple[Event, ...]:
        return (
            self.semantic.caller_events
            + self.semantic.subprocess_events
            + self.http.caller_events
            + self.http.request_events
            + self.network.caller_events
            + self.network.connection_events
            + self.network_setup.caller_events
            + self.network_setup.phase_events
            + self.logical_operation.caller_events
            + self.logical_operation.operation_events
        )

    @property
    def edges(self) -> tuple[CausalEdge, ...]:
        return (
            self.semantic.caller_edges
            + self.http.caller_edges
            + self.network.caller_edges
            + self.network_setup.caller_edges
            + self.logical_operation.caller_edges
        )
