"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from runtime_tools.batchscope.analysis._common import CaptureStatus
from runtime_tools.json_support import JsonValueModel


@dataclass(frozen=True, slots=True)
class _CallerAttributionCounts:
    status: CaptureStatus
    caller_count: int
    attributed_count: int
    unattributed_count: int
    invalid_count: int
    callback_error_count: int


@dataclass(frozen=True, slots=True)
class LifecyclePhase(JsonValueModel):
    name: str
    duration_seconds: float
    source: Literal["explicit", "derived"]


@dataclass(frozen=True, slots=True)
class CriticalPath(JsonValueModel):
    duration_seconds: float
    active_seconds: float
    waiting_seconds: float
    parallel_slack_seconds: float
    event_ids: tuple[str, ...]
    event_names: tuple[str, ...]
    certainty: Literal["observed", "inferred"]
    cycle_detected: bool


@dataclass(frozen=True, slots=True)
class Throughput(JsonValueModel):
    completed: float
    total: float
    rate_per_second: float | None
    remaining: float
    estimated_drain_seconds: float | None
    compute_finished_at_ns: int | None
    remaining_at_compute_completion: float | None
    post_compute_seconds: float | None
    post_compute_rate_per_second: float | None


@dataclass(frozen=True, slots=True)
class _ProgressSample:
    timestamp_ns: int
    uncertainty_ns: int | None
    sequence: int | None
    completed: float
    total: float


@dataclass(frozen=True, slots=True)
class Bottleneck(JsonValueModel):
    classification: str
    evidence: str
    confidence: float


@dataclass(frozen=True, slots=True)
class PythonProfileCoverageGap(JsonValueModel):
    pid: int
    process_name: str
    parent_name: str | None


@dataclass(frozen=True, slots=True)
class PythonProfileCoverage(JsonValueModel):
    status: Literal["complete", "partial", "unavailable", "invalid"]
    profiled_process_count: int
    observed_python_process_count: int | None
    matched_process_count: int
    unprofiled_process_count: int
    unprofiled_processes: tuple[PythonProfileCoverageGap, ...]
    unobserved_profile_process_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ProfileSnapshotMetrics(JsonValueModel):
    status: Literal["available", "unavailable", "invalid"]
    message_count: int
    payload_bytes: int
    max_payload_bytes: int
    serialization_seconds: float
    max_serialization_seconds: float
    checkpoint_message_count: int
    checkpoint_payload_bytes: int
    max_checkpoint_payload_bytes: int
    checkpoint_serialization_seconds: float
    max_checkpoint_serialization_seconds: float


@dataclass(frozen=True, slots=True)
class ProfilePublicationMetrics(JsonValueModel):
    status: Literal["available", "unavailable", "invalid"]
    fallback_process_count: int
    fallback_process_ids: tuple[int, ...]
    socket_attempted_process_count: int
    socket_failure_seconds: float
    max_socket_failure_seconds: float


@dataclass(frozen=True, slots=True)
class ProfileNormalizationMetrics(JsonValueModel):
    status: Literal["available", "unavailable", "invalid"]
    duration_seconds: float
    ranking_database_peak_bytes: int
    ranking_database_limit_bytes: int


@dataclass(frozen=True, slots=True)
class NativeCallCaptureSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    function_count: int
    call_count: int
    exception_count: int
    max_functions_per_process: int
    max_edges_per_process: int


@dataclass(frozen=True, slots=True)
class PythonExceptionControlFlowFilterSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    non_control_flow_function_count: int
    non_control_flow_event_count: int
    filtered_event_count: int
    dropped_non_control_flow_event_count: int
    dropped_filtered_event_count: int


@dataclass(frozen=True, slots=True)
class PythonExceptionCaptureSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    function_count: int
    event_count: int
    dropped_event_count: int
    max_functions_per_process: int
    control_flow_filter: PythonExceptionControlFlowFilterSummary | None = None


@dataclass(frozen=True, slots=True)
class ObserverIntegritySummary(JsonValueModel):
    status: Literal["complete", "partial", "unavailable", "invalid"]
    process_count: int
    missing_process_count: int
    profile_hook_setter_call_count: int
    profile_hook_setter_process_count: int
    trace_hook_setter_call_count: int
    trace_hook_setter_process_count: int


@dataclass(frozen=True, slots=True)
class DeepProfileSummary(JsonValueModel):
    status: str
    process_count: int
    function_count: int
    edge_count: int
    truncated: bool
    dropped_call_count: int
    open_call_count: int
    checkpoint_process_count: int
    registration_only_process_count: int
    dropped_profile_process_count: int
    dropped_profile_process_count_truncated: bool
    first_checkpoint_delay_seconds: float
    checkpoint_interval_seconds: float
    transport: str
    collector_error_count: int
    snapshot_metrics: ProfileSnapshotMetrics
    publication_metrics: ProfilePublicationMetrics
    normalization_metrics: ProfileNormalizationMetrics
    process_ids: tuple[int, ...]
    process_coverage: PythonProfileCoverage
    native_call_capture: NativeCallCaptureSummary | None = None
    python_exception_capture: PythonExceptionCaptureSummary | None = None
    observer_integrity: ObserverIntegritySummary | None = None


@dataclass(frozen=True, slots=True)
class SampleProfileSummary(JsonValueModel):
    status: str
    process_count: int
    function_count: int
    edge_count: int
    truncated: bool
    sample_count: int
    thread_sample_count: int
    dropped_frame_sample_count: int
    interval_seconds: float
    checkpoint_process_count: int
    registration_only_process_count: int
    dropped_profile_process_count: int
    dropped_profile_process_count_truncated: bool
    first_checkpoint_delay_seconds: float
    checkpoint_interval_seconds: float
    transport: str
    collector_error_count: int
    snapshot_metrics: ProfileSnapshotMetrics
    publication_metrics: ProfilePublicationMetrics
    normalization_metrics: ProfileNormalizationMetrics
    process_ids: tuple[int, ...]
    process_coverage: PythonProfileCoverage


@dataclass(frozen=True, slots=True)
class PythonCallProcessContribution(JsonValueModel):
    pid: int
    role: Literal["root", "descendant", "unknown"]
    process_name: str | None
    parent_name: str | None
    observed_in_process_tree: bool
    call_count: int
    total_seconds: float
    self_seconds: float
    max_seconds: float
    exception_count: int = 0
    non_control_flow_exception_count: int | None = None


@dataclass(frozen=True, slots=True)
class PythonHotspot(JsonValueModel):
    name: str
    filename: str
    firstlineno: int
    scope: str
    call_count: int
    total_seconds: float
    self_seconds: float
    max_seconds: float
    process_attribution_status: Literal["complete", "unavailable", "invalid"]
    processes: tuple[PythonCallProcessContribution, ...]
    implementation: Literal["python", "native"] = "python"
    exception_count: int = 0
    non_control_flow_exception_count: int | None = None


@dataclass(frozen=True, slots=True)
class PythonSampleProcessContribution(JsonValueModel):
    pid: int
    role: Literal["root", "descendant", "unknown"]
    process_name: str | None
    parent_name: str | None
    observed_in_process_tree: bool
    sample_count: int
    leaf_sample_count: int
    estimated_total_seconds: float
    estimated_leaf_seconds: float


@dataclass(frozen=True, slots=True)
class PythonSampleHotspot(JsonValueModel):
    name: str
    filename: str
    firstlineno: int
    scope: str
    sample_count: int
    leaf_sample_count: int
    estimated_total_seconds: float
    estimated_leaf_seconds: float
    process_attribution_status: Literal["complete", "unavailable", "invalid"]
    processes: tuple[PythonSampleProcessContribution, ...]


@dataclass(frozen=True, slots=True)
class ProcessObserverSummary(JsonValueModel):
    status: str
    process_count: int
    descendant_process_count: int
    sample_count: int
    poll_count: int
    truncated: bool
    dropped_process_count: int
    interval_seconds: float
    error: str | None


@dataclass(frozen=True, slots=True)
class ProcessResourceHotspot(JsonValueModel):
    name: str
    pid: int
    parent_name: str | None
    sample_count: int
    peak_rss_bytes: float
    cpu_seconds: float


@dataclass(frozen=True, slots=True)
class SemanticCaptureSummary(JsonValueModel):
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    process_count: int
    subprocess_count: int
    dropped_subprocess_count: int
    callback_error_count: int
    invalid_event_count: int
    caller_attribution_status: Literal[
        "complete", "partial", "truncated", "unavailable", "invalid"
    ] = "unavailable"
    caller_count: int = 0
    attributed_subprocess_count: int = 0
    unattributed_subprocess_count: int = 0
    invalid_caller_count: int = 0
    caller_callback_error_count: int = 0
