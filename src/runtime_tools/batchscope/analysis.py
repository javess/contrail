"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Literal

from runtime_tools.deep_profile import (
    FILTERED_CONTROL_FLOW_EXCEPTION_TYPES,
    PYTHON_EXCEPTION_FILTER_VERSION,
)
from runtime_tools.inspect import ExecutionSummary, inspect_reader
from runtime_tools.json_support import output_document
from runtime_tools.model import CausalEdge, Event, JsonValue
from runtime_tools.storage import RunpackReader, resolve_runpack_path

MAX_PROFILE_PROCESS_CONTRIBUTIONS = 128
MAX_PROFILE_COVERAGE_GAPS = 100
MAX_SUBPROCESS_CALL_SUMMARIES = 100
MAX_HTTP_REQUEST_SUMMARIES = 100
MAX_NETWORK_CONNECTION_SUMMARIES = 100
MAX_NETWORK_SETUP_SUMMARIES = 100
MAX_LOGICAL_OPERATION_SUMMARIES = 100
MAX_NATIVE_FUNCTIONS_PER_PROCESS = 2_000
MAX_NATIVE_EDGES_PER_PROCESS = 10_000
MAX_PYTHON_FUNCTIONS_PER_PROCESS = 2_000
MAX_PYTHON_HOTSPOTS = 100
PYTHON_EXCEPTION_CHURN_MIN_EVENTS = 10
PYTHON_EXCEPTION_CHURN_MIN_EVENTS_PER_CALL = 0.5
NETWORK_CHURN_MIN_CONNECTIONS = 10
NETWORK_CHURN_MIN_RATE_PER_SECOND = 5.0
NETWORK_SETUP_MIN_SECONDS = 0.05
NETWORK_SETUP_MIN_RUN_RATIO = 0.25
LOGICAL_OPERATION_MIN_SECONDS = 0.05
LOGICAL_OPERATION_MIN_RUN_RATIO = 0.25
HTTP_CAPTURE_ADAPTERS = frozenset(
    {
        "stdlib.http.client",
        "httpcore.sync",
        "httpcore.async",
        "aiohttp.async",
    }
)
NETWORK_CAPTURE_ADAPTERS = frozenset(
    {
        "stdlib.socket.connect",
        "asyncio.create_connection",
        "asyncio.create_unix_connection",
    }
)
NETWORK_SETUP_CAPTURE_ADAPTERS = frozenset(
    {
        "stdlib.socket.getaddrinfo",
        "stdlib.ssl.SSLObject.do_handshake",
        "stdlib.ssl.SSLSocket.do_handshake",
    }
)
LOGICAL_OPERATION_CAPTURE_ADAPTER_CATEGORIES = {
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
LOGICAL_OPERATION_CAPTURE_ADAPTERS = frozenset(LOGICAL_OPERATION_CAPTURE_ADAPTER_CATEGORIES)

type LogicalOperationCategory = Literal[
    "broker", "cache", "database", "executor", "queue", "scheduler", "server"
]
type LogicalOperationName = Literal[
    "batch",
    "command",
    "commit",
    "consume",
    "execute",
    "executemany",
    "executescript",
    "get",
    "publish",
    "put",
    "rollback",
    "request",
    "task",
]


@dataclass(frozen=True, slots=True)
class LifecyclePhase:
    name: str
    duration_seconds: float
    source: Literal["explicit", "derived"]

    def as_json_value(self) -> dict[str, JsonValue]:
        return {"name": self.name, "duration_seconds": self.duration_seconds, "source": self.source}


@dataclass(frozen=True, slots=True)
class CriticalPath:
    duration_seconds: float
    active_seconds: float
    waiting_seconds: float
    parallel_slack_seconds: float
    event_ids: tuple[str, ...]
    event_names: tuple[str, ...]
    certainty: Literal["observed", "inferred"]
    cycle_detected: bool

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "duration_seconds": self.duration_seconds,
            "active_seconds": self.active_seconds,
            "waiting_seconds": self.waiting_seconds,
            "parallel_slack_seconds": self.parallel_slack_seconds,
            "event_ids": list(self.event_ids),
            "event_names": list(self.event_names),
            "certainty": self.certainty,
            "cycle_detected": self.cycle_detected,
        }


@dataclass(frozen=True, slots=True)
class Throughput:
    completed: float
    total: float
    rate_per_second: float | None
    remaining: float
    estimated_drain_seconds: float | None
    compute_finished_at_ns: int | None
    remaining_at_compute_completion: float | None
    post_compute_seconds: float | None
    post_compute_rate_per_second: float | None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "completed": self.completed,
            "total": self.total,
            "rate_per_second": self.rate_per_second,
            "remaining": self.remaining,
            "estimated_drain_seconds": self.estimated_drain_seconds,
            "compute_finished_at_ns": self.compute_finished_at_ns,
            "remaining_at_compute_completion": self.remaining_at_compute_completion,
            "post_compute_seconds": self.post_compute_seconds,
            "post_compute_rate_per_second": self.post_compute_rate_per_second,
        }


@dataclass(frozen=True, slots=True)
class _ProgressSample:
    timestamp_ns: int
    uncertainty_ns: int | None
    sequence: int | None
    completed: float
    total: float


@dataclass(frozen=True, slots=True)
class Bottleneck:
    classification: str
    evidence: str
    confidence: float

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "classification": self.classification,
            "evidence": self.evidence,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class PythonProfileCoverageGap:
    pid: int
    process_name: str
    parent_name: str | None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "pid": self.pid,
            "process_name": self.process_name,
            "parent_name": self.parent_name,
        }


@dataclass(frozen=True, slots=True)
class PythonProfileCoverage:
    status: Literal["complete", "partial", "unavailable", "invalid"]
    profiled_process_count: int
    observed_python_process_count: int | None
    matched_process_count: int
    unprofiled_process_count: int
    unprofiled_processes: tuple[PythonProfileCoverageGap, ...]
    unobserved_profile_process_ids: tuple[int, ...]

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "profiled_process_count": self.profiled_process_count,
            "observed_python_process_count": self.observed_python_process_count,
            "matched_process_count": self.matched_process_count,
            "unprofiled_process_count": self.unprofiled_process_count,
            "unprofiled_processes": [item.as_json_value() for item in self.unprofiled_processes],
            "unobserved_profile_process_ids": list(self.unobserved_profile_process_ids),
        }


@dataclass(frozen=True, slots=True)
class ProfileSnapshotMetrics:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "message_count": self.message_count,
            "payload_bytes": self.payload_bytes,
            "max_payload_bytes": self.max_payload_bytes,
            "serialization_seconds": self.serialization_seconds,
            "max_serialization_seconds": self.max_serialization_seconds,
            "checkpoint_message_count": self.checkpoint_message_count,
            "checkpoint_payload_bytes": self.checkpoint_payload_bytes,
            "max_checkpoint_payload_bytes": self.max_checkpoint_payload_bytes,
            "checkpoint_serialization_seconds": self.checkpoint_serialization_seconds,
            "max_checkpoint_serialization_seconds": (self.max_checkpoint_serialization_seconds),
        }


@dataclass(frozen=True, slots=True)
class ProfilePublicationMetrics:
    status: Literal["available", "unavailable", "invalid"]
    fallback_process_count: int
    fallback_process_ids: tuple[int, ...]
    socket_attempted_process_count: int
    socket_failure_seconds: float
    max_socket_failure_seconds: float

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "fallback_process_count": self.fallback_process_count,
            "fallback_process_ids": list(self.fallback_process_ids),
            "socket_attempted_process_count": self.socket_attempted_process_count,
            "socket_failure_seconds": self.socket_failure_seconds,
            "max_socket_failure_seconds": self.max_socket_failure_seconds,
        }


@dataclass(frozen=True, slots=True)
class ProfileNormalizationMetrics:
    status: Literal["available", "unavailable", "invalid"]
    duration_seconds: float
    ranking_database_peak_bytes: int
    ranking_database_limit_bytes: int

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "duration_seconds": self.duration_seconds,
            "ranking_database_peak_bytes": self.ranking_database_peak_bytes,
            "ranking_database_limit_bytes": self.ranking_database_limit_bytes,
        }


@dataclass(frozen=True, slots=True)
class NativeCallCaptureSummary:
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    function_count: int
    call_count: int
    exception_count: int
    max_functions_per_process: int
    max_edges_per_process: int

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "enabled": True,
            "deep_only": True,
            "function_count": self.function_count,
            "call_count": self.call_count,
            "exception_count": self.exception_count,
            "arguments_captured": False,
            "return_values_captured": False,
            "exception_messages_captured": False,
            "max_functions_per_process": self.max_functions_per_process,
            "max_edges_per_process": self.max_edges_per_process,
        }


@dataclass(frozen=True, slots=True)
class PythonExceptionControlFlowFilterSummary:
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    non_control_flow_function_count: int
    non_control_flow_event_count: int
    filtered_event_count: int
    dropped_non_control_flow_event_count: int
    dropped_filtered_event_count: int

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "format_version": PYTHON_EXCEPTION_FILTER_VERSION,
            "status": self.status,
            "event_semantics": "exact_type_identity",
            "filtered_exception_types": list(FILTERED_CONTROL_FLOW_EXCEPTION_TYPES),
            "exception_type_identity_inspected": True,
            "exception_types_captured": False,
            "non_control_flow_function_count": self.non_control_flow_function_count,
            "non_control_flow_event_count": self.non_control_flow_event_count,
            "filtered_event_count": self.filtered_event_count,
            "dropped_non_control_flow_event_count": (self.dropped_non_control_flow_event_count),
            "dropped_filtered_event_count": self.dropped_filtered_event_count,
        }


@dataclass(frozen=True, slots=True)
class PythonExceptionCaptureSummary:
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    function_count: int
    event_count: int
    dropped_event_count: int
    max_functions_per_process: int
    control_flow_filter: PythonExceptionControlFlowFilterSummary | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "status": self.status,
            "enabled": True,
            "deep_only": True,
            "event_semantics": "per_propagated_frame",
            "function_count": self.function_count,
            "event_count": self.event_count,
            "dropped_event_count": self.dropped_event_count,
            "arguments_captured": False,
            "locals_captured": False,
            "exception_types_captured": False,
            "exception_values_captured": False,
            "exception_messages_captured": False,
            "tracebacks_captured": False,
            "line_events_enabled": False,
            "opcode_events_enabled": False,
            "max_functions_per_process": self.max_functions_per_process,
        }
        if self.control_flow_filter is not None:
            value["control_flow_filter"] = self.control_flow_filter.as_json_value()
        return value


@dataclass(frozen=True, slots=True)
class ObserverIntegritySummary:
    status: Literal["complete", "partial", "unavailable", "invalid"]
    process_count: int
    missing_process_count: int
    profile_hook_setter_call_count: int
    profile_hook_setter_process_count: int
    trace_hook_setter_call_count: int
    trace_hook_setter_process_count: int

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "format_version": 1,
            "status": self.status,
            "process_count": self.process_count,
            "missing_process_count": self.missing_process_count,
            "profile_hook_setter_call_count": self.profile_hook_setter_call_count,
            "profile_hook_setter_process_count": self.profile_hook_setter_process_count,
            "trace_hook_setter_call_count": self.trace_hook_setter_call_count,
            "trace_hook_setter_process_count": self.trace_hook_setter_process_count,
            "arguments_captured": False,
            "locals_captured": False,
            "hook_values_captured": False,
        }


@dataclass(frozen=True, slots=True)
class DeepProfileSummary:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "process_count": self.process_count,
            "function_count": self.function_count,
            "edge_count": self.edge_count,
            "truncated": self.truncated,
            "dropped_call_count": self.dropped_call_count,
            "open_call_count": self.open_call_count,
            "checkpoint_process_count": self.checkpoint_process_count,
            "registration_only_process_count": self.registration_only_process_count,
            "dropped_profile_process_count": self.dropped_profile_process_count,
            "dropped_profile_process_count_truncated": (
                self.dropped_profile_process_count_truncated
            ),
            "first_checkpoint_delay_seconds": self.first_checkpoint_delay_seconds,
            "checkpoint_interval_seconds": self.checkpoint_interval_seconds,
            "transport": self.transport,
            "collector_error_count": self.collector_error_count,
            "snapshot_metrics": self.snapshot_metrics.as_json_value(),
            "publication_metrics": self.publication_metrics.as_json_value(),
            "normalization_metrics": self.normalization_metrics.as_json_value(),
            "intrusive": True,
            "process_coverage": self.process_coverage.as_json_value(),
            "native_call_capture": (
                self.native_call_capture.as_json_value()
                if self.native_call_capture is not None
                else None
            ),
            "python_exception_capture": (
                self.python_exception_capture.as_json_value()
                if self.python_exception_capture is not None
                else None
            ),
            "observer_integrity": (
                self.observer_integrity.as_json_value()
                if self.observer_integrity is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class SampleProfileSummary:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "process_count": self.process_count,
            "function_count": self.function_count,
            "edge_count": self.edge_count,
            "truncated": self.truncated,
            "sample_count": self.sample_count,
            "thread_sample_count": self.thread_sample_count,
            "dropped_frame_sample_count": self.dropped_frame_sample_count,
            "interval_seconds": self.interval_seconds,
            "checkpoint_process_count": self.checkpoint_process_count,
            "registration_only_process_count": self.registration_only_process_count,
            "dropped_profile_process_count": self.dropped_profile_process_count,
            "dropped_profile_process_count_truncated": (
                self.dropped_profile_process_count_truncated
            ),
            "first_checkpoint_delay_seconds": self.first_checkpoint_delay_seconds,
            "checkpoint_interval_seconds": self.checkpoint_interval_seconds,
            "transport": self.transport,
            "collector_error_count": self.collector_error_count,
            "snapshot_metrics": self.snapshot_metrics.as_json_value(),
            "publication_metrics": self.publication_metrics.as_json_value(),
            "normalization_metrics": self.normalization_metrics.as_json_value(),
            "estimated": True,
            "intrusive": True,
            "per_call": False,
            "process_coverage": self.process_coverage.as_json_value(),
        }


@dataclass(frozen=True, slots=True)
class PythonCallProcessContribution:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "pid": self.pid,
            "role": self.role,
            "process_name": self.process_name,
            "parent_name": self.parent_name,
            "observed_in_process_tree": self.observed_in_process_tree,
            "call_count": self.call_count,
            "total_seconds": self.total_seconds,
            "self_seconds": self.self_seconds,
            "max_seconds": self.max_seconds,
            "exception_count": self.exception_count,
        }
        if self.non_control_flow_exception_count is not None:
            value["non_control_flow_exception_count"] = self.non_control_flow_exception_count
        return value


@dataclass(frozen=True, slots=True)
class PythonHotspot:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "name": self.name,
            "filename": self.filename,
            "firstlineno": self.firstlineno,
            "scope": self.scope,
            "call_count": self.call_count,
            "total_seconds": self.total_seconds,
            "self_seconds": self.self_seconds,
            "max_seconds": self.max_seconds,
            "implementation": self.implementation,
            "exception_count": self.exception_count,
            "process_attribution_status": self.process_attribution_status,
            "processes": [process.as_json_value() for process in self.processes],
        }
        if self.non_control_flow_exception_count is not None:
            value["non_control_flow_exception_count"] = self.non_control_flow_exception_count
        return value


@dataclass(frozen=True, slots=True)
class PythonSampleProcessContribution:
    pid: int
    role: Literal["root", "descendant", "unknown"]
    process_name: str | None
    parent_name: str | None
    observed_in_process_tree: bool
    sample_count: int
    leaf_sample_count: int
    estimated_total_seconds: float
    estimated_leaf_seconds: float

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "pid": self.pid,
            "role": self.role,
            "process_name": self.process_name,
            "parent_name": self.parent_name,
            "observed_in_process_tree": self.observed_in_process_tree,
            "sample_count": self.sample_count,
            "leaf_sample_count": self.leaf_sample_count,
            "estimated_total_seconds": self.estimated_total_seconds,
            "estimated_leaf_seconds": self.estimated_leaf_seconds,
        }


@dataclass(frozen=True, slots=True)
class PythonSampleHotspot:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "name": self.name,
            "filename": self.filename,
            "firstlineno": self.firstlineno,
            "scope": self.scope,
            "sample_count": self.sample_count,
            "leaf_sample_count": self.leaf_sample_count,
            "estimated_total_seconds": self.estimated_total_seconds,
            "estimated_leaf_seconds": self.estimated_leaf_seconds,
            "process_attribution_status": self.process_attribution_status,
            "processes": [process.as_json_value() for process in self.processes],
        }


@dataclass(frozen=True, slots=True)
class ProcessObserverSummary:
    status: str
    process_count: int
    descendant_process_count: int
    sample_count: int
    poll_count: int
    truncated: bool
    dropped_process_count: int
    interval_seconds: float
    error: str | None

    def as_json_value(self) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "status": self.status,
            "process_count": self.process_count,
            "descendant_process_count": self.descendant_process_count,
            "sample_count": self.sample_count,
            "poll_count": self.poll_count,
            "truncated": self.truncated,
            "dropped_process_count": self.dropped_process_count,
            "interval_seconds": self.interval_seconds,
            "controller_side": True,
        }
        if self.error is not None:
            value["error"] = self.error
        return value


@dataclass(frozen=True, slots=True)
class ProcessResourceHotspot:
    name: str
    pid: int
    parent_name: str | None
    sample_count: int
    peak_rss_bytes: float
    cpu_seconds: float

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "name": self.name,
            "pid": self.pid,
            "parent_name": self.parent_name,
            "sample_count": self.sample_count,
            "peak_rss_bytes": self.peak_rss_bytes,
            "cpu_seconds": self.cpu_seconds,
        }


@dataclass(frozen=True, slots=True)
class SemanticCaptureSummary:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "observer": "python-subprocess-wrapper",
            "zero_code": True,
            "process_count": self.process_count,
            "subprocess_count": self.subprocess_count,
            "dropped_subprocess_count": self.dropped_subprocess_count,
            "callback_error_count": self.callback_error_count,
            "invalid_event_count": self.invalid_event_count,
            "arguments_captured": False,
            "environment_captured": False,
            "working_directory_captured": False,
            "caller_attribution": {
                "status": self.caller_attribution_status,
                "caller_count": self.caller_count,
                "attributed_subprocess_count": self.attributed_subprocess_count,
                "unattributed_subprocess_count": self.unattributed_subprocess_count,
                "invalid_caller_count": self.invalid_caller_count,
                "callback_error_count": self.caller_callback_error_count,
                "arguments_captured": False,
                "locals_captured": False,
            },
        }


@dataclass(frozen=True, slots=True)
class SubprocessCaller:
    event_id: str
    name: str
    module: str
    qualname: str
    filename: str
    firstlineno: int
    scope: Literal["application", "library", "runtime"]
    observation: Literal["exact", "sampled"]
    confidence: float

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "name": self.name,
            "module": self.module,
            "qualname": self.qualname,
            "filename": self.filename,
            "firstlineno": self.firstlineno,
            "scope": self.scope,
            "observation": self.observation,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SubprocessCall:
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
    caller: SubprocessCaller | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "name": self.name,
            "parent_pid": self.parent_pid,
            "role": self.role,
            "child_pid": self.child_pid,
            "child_observed_in_process_tree": self.child_observed_in_process_tree,
            "shell": self.shell,
            "outcome": self.outcome,
            "exit_code": self.exit_code,
            "error_type": self.error_type,
            "started_at_ns": self.started_at_ns,
            "duration_seconds": self.duration_seconds,
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(frozen=True, slots=True)
class HttpCaptureSummary:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "observer": "python-http-client-wrapper",
            "zero_code": True,
            "process_count": self.process_count,
            "request_count": self.request_count,
            "dropped_request_count": self.dropped_request_count,
            "callback_error_count": self.callback_error_count,
            "invalid_event_count": self.invalid_event_count,
            "adapters": list(self.adapters),
            "server_identity_policy": "redact",
            "method_captured": True,
            "scheme_captured": True,
            "server_address_captured": False,
            "path_captured": False,
            "query_captured": False,
            "headers_captured": False,
            "body_captured": False,
            "response_body_captured": False,
            "duration_boundary": "response_headers",
            "caller_attribution": {
                "status": self.caller_attribution_status,
                "caller_count": self.caller_count,
                "attributed_request_count": self.attributed_request_count,
                "unattributed_request_count": self.unattributed_request_count,
                "invalid_caller_count": self.invalid_caller_count,
                "callback_error_count": self.caller_callback_error_count,
                "arguments_captured": False,
                "locals_captured": False,
            },
        }


@dataclass(frozen=True, slots=True)
class HttpCaller:
    event_id: str
    name: str
    module: str
    qualname: str
    filename: str
    firstlineno: int
    scope: Literal["application", "library", "runtime"]
    observation: Literal["exact", "sampled"]
    confidence: float

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "name": self.name,
            "module": self.module,
            "qualname": self.qualname,
            "filename": self.filename,
            "firstlineno": self.firstlineno,
            "scope": self.scope,
            "observation": self.observation,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class HttpRequest:
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
    caller: HttpCaller | None = None
    adapter: str = "stdlib.http.client"

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "method": self.method,
            "adapter": self.adapter,
            "scheme": self.scheme,
            "server_address": None,
            "server_port": self.server_port,
            "server_identity_policy": "redact",
            "parent_pid": self.parent_pid,
            "role": self.role,
            "outcome": self.outcome,
            "status_code": self.status_code,
            "error_type": self.error_type,
            "started_at_ns": self.started_at_ns,
            "duration_seconds": self.duration_seconds,
            "duration_boundary": "response_headers",
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(frozen=True, slots=True)
class NetworkCaptureSummary:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "observer": "python-network-connection-wrapper",
            "zero_code": True,
            "process_count": self.process_count,
            "connection_count": self.connection_count,
            "dropped_connection_count": self.dropped_connection_count,
            "callback_error_count": self.callback_error_count,
            "invalid_event_count": self.invalid_event_count,
            "adapters": list(self.adapters),
            "connection_hotspot_count": self.connection_hotspot_count,
            "server_identity_policy": "redact",
            "server_address_captured": False,
            "path_captured": False,
            "credentials_captured": False,
            "duration_boundary": "connection_ready",
            "caller_attribution": {
                "status": self.caller_attribution_status,
                "caller_count": self.caller_count,
                "attributed_connection_count": self.attributed_connection_count,
                "unattributed_connection_count": self.unattributed_connection_count,
                "invalid_caller_count": self.invalid_caller_count,
                "callback_error_count": self.caller_callback_error_count,
                "arguments_captured": False,
                "locals_captured": False,
            },
        }


@dataclass(frozen=True, slots=True)
class NetworkCaller:
    event_id: str
    name: str
    module: str
    qualname: str
    filename: str
    firstlineno: int
    scope: Literal["application", "library", "runtime"]
    observation: Literal["exact", "sampled"]
    confidence: float

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "name": self.name,
            "module": self.module,
            "qualname": self.qualname,
            "filename": self.filename,
            "firstlineno": self.firstlineno,
            "scope": self.scope,
            "observation": self.observation,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class NetworkSetupCaptureSummary:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "observer": "python-network-setup-wrapper",
            "zero_code": True,
            "process_count": self.process_count,
            "phase_count": self.phase_count,
            "dropped_phase_count": self.dropped_phase_count,
            "callback_error_count": self.callback_error_count,
            "invalid_event_count": self.invalid_event_count,
            "adapters": list(self.adapters),
            "hotspot_count": self.hotspot_count,
            "hostname_captured": False,
            "server_address_captured": False,
            "sni_captured": False,
            "certificate_captured": False,
            "credentials_captured": False,
            "caller_attribution": {
                "status": self.caller_attribution_status,
                "caller_count": self.caller_count,
                "attributed_phase_count": self.attributed_phase_count,
                "unattributed_phase_count": self.unattributed_phase_count,
                "invalid_caller_count": self.invalid_caller_count,
                "callback_error_count": self.caller_callback_error_count,
                "arguments_captured": False,
                "locals_captured": False,
            },
        }


@dataclass(frozen=True, slots=True)
class NetworkSetupPhase:
    event_id: str
    phase: Literal["dns", "tls"]
    adapter: str
    parent_pid: int
    role: Literal["root", "descendant", "unknown"]
    outcome: Literal["completed", "setup_error", "unknown"]
    error_type: str | None
    started_at_ns: int
    duration_seconds: float | None
    caller: NetworkCaller | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "phase": self.phase,
            "adapter": self.adapter,
            "parent_pid": self.parent_pid,
            "role": self.role,
            "outcome": self.outcome,
            "error_type": self.error_type,
            "started_at_ns": self.started_at_ns,
            "duration_seconds": self.duration_seconds,
            "duration_boundary": self.phase,
            "hostname_captured": False,
            "server_address_captured": False,
            "sni_captured": False,
            "certificate_captured": False,
            "credentials_captured": False,
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(frozen=True, slots=True)
class NetworkSetupHotspot:
    phase: Literal["dns", "tls"]
    adapter: str
    phase_count: int
    completed_phase_count: int
    failed_phase_count: int
    unfinished_phase_count: int
    total_duration_seconds: float
    max_duration_seconds: float
    caller: NetworkCaller | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "phase": self.phase,
            "adapter": self.adapter,
            "phase_count": self.phase_count,
            "completed_phase_count": self.completed_phase_count,
            "failed_phase_count": self.failed_phase_count,
            "unfinished_phase_count": self.unfinished_phase_count,
            "total_duration_seconds": self.total_duration_seconds,
            "max_duration_seconds": self.max_duration_seconds,
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(slots=True)
class _NetworkSetupHotspotAggregate:
    phase: Literal["dns", "tls"]
    adapter: str
    caller: NetworkCaller | None
    phase_count: int = 0
    completed_phase_count: int = 0
    failed_phase_count: int = 0
    unfinished_phase_count: int = 0
    total_duration_seconds: float = 0.0
    max_duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class LogicalOperationCaptureSummary:
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "status": self.status,
            "observer": "python-logical-operation-wrapper",
            "zero_code": True,
            "deep_only": True,
            "process_count": self.process_count,
            "operation_count": self.operation_count,
            "dropped_operation_count": self.dropped_operation_count,
            "callback_error_count": self.callback_error_count,
            "invalid_event_count": self.invalid_event_count,
            "adapters": list(self.adapters),
            "hotspot_count": self.hotspot_count,
            "statement_captured": False,
            "parameters_captured": False,
            "payload_captured": False,
            "queue_item_captured": False,
            "queue_identity_captured": False,
            "callable_captured": False,
            "awaitable_captured": False,
            "task_name_captured": False,
            "context_captured": False,
            "arguments_captured": False,
            "return_value_captured": False,
            "exception_messages_captured": False,
            "http_method_captured": False,
            "route_captured": False,
            "url_captured": False,
            "headers_captured": False,
            "body_captured": False,
            "response_body_captured": False,
            "client_address_captured": False,
            "caller_attribution": {
                "status": self.caller_attribution_status,
                "caller_count": self.caller_count,
                "attributed_operation_count": self.attributed_operation_count,
                "unattributed_operation_count": self.unattributed_operation_count,
                "invalid_caller_count": self.invalid_caller_count,
                "callback_error_count": self.caller_callback_error_count,
                "arguments_captured": False,
                "locals_captured": False,
            },
        }


@dataclass(frozen=True, slots=True)
class LogicalOperation:
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
    caller: NetworkCaller | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "category": self.category,
            "operation": self.operation,
            "adapter": self.adapter,
            "parent_pid": self.parent_pid,
            "role": self.role,
            "outcome": self.outcome,
            "error_type": self.error_type,
            "status_code": self.status_code,
            "started_at_ns": self.started_at_ns,
            "duration_seconds": self.duration_seconds,
            "duration_boundary": (
                "submission_to_completion"
                if self.category == "executor"
                else "creation_to_completion"
                if self.category == "scheduler"
                else "request_to_response_completion"
                if self.category == "server"
                else "logical_operation"
            ),
            "statement_captured": False,
            "parameters_captured": False,
            "payload_captured": False,
            "queue_item_captured": False,
            "queue_identity_captured": False,
            "callable_captured": False,
            "awaitable_captured": False,
            "task_name_captured": False,
            "context_captured": False,
            "arguments_captured": False,
            "return_value_captured": False,
            "http_method_captured": False,
            "route_captured": False,
            "url_captured": False,
            "headers_captured": False,
            "body_captured": False,
            "response_body_captured": False,
            "client_address_captured": False,
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(frozen=True, slots=True)
class LogicalOperationHotspot:
    category: LogicalOperationCategory
    operation: LogicalOperationName
    adapter: str
    operation_count: int
    completed_operation_count: int
    failed_operation_count: int
    unfinished_operation_count: int
    total_duration_seconds: float
    max_duration_seconds: float
    caller: NetworkCaller | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "category": self.category,
            "operation": self.operation,
            "adapter": self.adapter,
            "operation_count": self.operation_count,
            "completed_operation_count": self.completed_operation_count,
            "failed_operation_count": self.failed_operation_count,
            "unfinished_operation_count": self.unfinished_operation_count,
            "total_duration_seconds": self.total_duration_seconds,
            "max_duration_seconds": self.max_duration_seconds,
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(slots=True)
class _LogicalOperationHotspotAggregate:
    category: LogicalOperationCategory
    operation: LogicalOperationName
    adapter: str
    caller: NetworkCaller | None
    operation_count: int = 0
    completed_operation_count: int = 0
    failed_operation_count: int = 0
    unfinished_operation_count: int = 0
    total_duration_seconds: float = 0.0
    max_duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class NetworkConnection:
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
    caller: NetworkCaller | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "event_id": self.event_id,
            "adapter": self.adapter,
            "transport": self.transport,
            "address_family": self.address_family,
            "server_address": None,
            "server_port": self.server_port,
            "server_identity_policy": "redact",
            "tls_requested": self.tls_requested,
            "parent_pid": self.parent_pid,
            "role": self.role,
            "outcome": self.outcome,
            "error_type": self.error_type,
            "started_at_ns": self.started_at_ns,
            "duration_seconds": self.duration_seconds,
            "duration_boundary": "connection_ready",
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(frozen=True, slots=True)
class NetworkConnectionHotspot:
    adapter: str
    connection_count: int
    connected_connection_count: int
    failed_connection_count: int
    unfinished_connection_count: int
    total_duration_seconds: float
    max_duration_seconds: float
    caller: NetworkCaller | None = None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "adapter": self.adapter,
            "connection_count": self.connection_count,
            "connected_connection_count": self.connected_connection_count,
            "failed_connection_count": self.failed_connection_count,
            "unfinished_connection_count": self.unfinished_connection_count,
            "total_duration_seconds": self.total_duration_seconds,
            "max_duration_seconds": self.max_duration_seconds,
            "caller": self.caller.as_json_value() if self.caller is not None else None,
        }


@dataclass(slots=True)
class _NetworkConnectionHotspotAggregate:
    adapter: str
    caller: NetworkCaller | None
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


@dataclass(frozen=True, slots=True)
class BatchAnalysis:
    execution_id: str
    name: str
    total_seconds: float | None
    lifecycle: tuple[LifecyclePhase, ...]
    critical_path: CriticalPath | None
    throughput: Throughput | None
    bottlenecks: tuple[Bottleneck, ...]
    deep_profile: DeepProfileSummary | None = None
    python_hotspots: tuple[PythonHotspot, ...] = ()
    sample_profile: SampleProfileSummary | None = None
    python_sample_hotspots: tuple[PythonSampleHotspot, ...] = ()
    process_observer: ProcessObserverSummary | None = None
    process_hotspots: tuple[ProcessResourceHotspot, ...] = ()
    semantic_capture: SemanticCaptureSummary | None = None
    subprocess_calls: tuple[SubprocessCall, ...] = ()
    http_capture: HttpCaptureSummary | None = None
    http_requests: tuple[HttpRequest, ...] = ()
    network_capture: NetworkCaptureSummary | None = None
    network_connections: tuple[NetworkConnection, ...] = ()
    network_connection_hotspots: tuple[NetworkConnectionHotspot, ...] = ()
    network_setup_capture: NetworkSetupCaptureSummary | None = None
    network_setup_phases: tuple[NetworkSetupPhase, ...] = ()
    network_setup_hotspots: tuple[NetworkSetupHotspot, ...] = ()
    logical_operation_capture: LogicalOperationCaptureSummary | None = None
    logical_operations: tuple[LogicalOperation, ...] = ()
    logical_operation_hotspots: tuple[LogicalOperationHotspot, ...] = ()

    def as_json_value(self) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "execution_id": self.execution_id,
            "name": self.name,
            "total_seconds": self.total_seconds,
            "lifecycle": [phase.as_json_value() for phase in self.lifecycle],
            "critical_path": self.critical_path.as_json_value() if self.critical_path else None,
            "throughput": self.throughput.as_json_value() if self.throughput else None,
            "bottlenecks": [item.as_json_value() for item in self.bottlenecks],
        }
        if self.deep_profile is not None:
            value["deep_profile"] = self.deep_profile.as_json_value()
            value["python_hotspots"] = [item.as_json_value() for item in self.python_hotspots]
        if self.sample_profile is not None:
            value["sample_profile"] = self.sample_profile.as_json_value()
            value["python_sample_hotspots"] = [
                item.as_json_value() for item in self.python_sample_hotspots
            ]
        if self.process_observer is not None:
            value["process_observer"] = self.process_observer.as_json_value()
            value["process_hotspots"] = [item.as_json_value() for item in self.process_hotspots]
        if self.semantic_capture is not None:
            value["semantic_capture"] = self.semantic_capture.as_json_value()
            value["subprocess_calls"] = [item.as_json_value() for item in self.subprocess_calls]
        if self.http_capture is not None:
            value["http_capture"] = self.http_capture.as_json_value()
            value["http_requests"] = [item.as_json_value() for item in self.http_requests]
        if self.network_capture is not None:
            value["network_capture"] = self.network_capture.as_json_value()
            value["network_connections"] = [
                item.as_json_value() for item in self.network_connections
            ]
            value["network_connection_hotspots"] = [
                item.as_json_value() for item in self.network_connection_hotspots
            ]
        if self.network_setup_capture is not None:
            value["network_setup_capture"] = self.network_setup_capture.as_json_value()
            value["network_setup_phases"] = [
                item.as_json_value() for item in self.network_setup_phases
            ]
            value["network_setup_hotspots"] = [
                item.as_json_value() for item in self.network_setup_hotspots
            ]
        if self.logical_operation_capture is not None:
            value["logical_operation_capture"] = self.logical_operation_capture.as_json_value()
            value["logical_operations"] = [item.as_json_value() for item in self.logical_operations]
            value["logical_operation_hotspots"] = [
                item.as_json_value() for item in self.logical_operation_hotspots
            ]
        return output_document("batchscope.inspect", value)


def _duration_ns(event: Event) -> int:
    if event.started_at_ns is None or event.finished_at_ns is None:
        return 0
    return max(0, event.finished_at_ns - event.started_at_ns)


def _merge_intervals(intervals: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            previous_start, previous_end = merged[-1]
            merged[-1] = (previous_start, max(previous_end, end))
    return tuple(merged)


@dataclass(frozen=True, slots=True)
class _Path:
    intervals: tuple[tuple[int, int], ...]
    event_id: str
    child: _Path | None
    length: int

    @property
    def active_ns(self) -> int:
        return sum(end - start for start, end in self.intervals)

    @property
    def duration_ns(self) -> int:
        if not self.intervals:
            return 0
        return self.intervals[-1][1] - self.intervals[0][0]

    def event_ids(self) -> tuple[str, ...]:
        result = []
        current: _Path | None = self
        while current is not None:
            result.append(current.event_id)
            current = current.child
        return tuple(result)


def _event_interval(event: Event) -> tuple[int, int]:
    if event.started_at_ns is None or event.finished_at_ns is None:
        raise ValueError("critical-path event requires a complete interval")
    return event.started_at_ns, event.finished_at_ns


def _has_complete_interval(event: Event) -> bool:
    return event.started_at_ns is not None and event.finished_at_ns is not None


def _path_key(path: _Path) -> tuple[bool, int, int, int]:
    return bool(path.intervals), path.duration_ns, path.active_ns, path.length


def _critical_path(
    events: tuple[Event, ...],
    edge_values: tuple[tuple[str, str, float], ...],
    total: float | None,
    *,
    clock_inconsistent: bool,
    causality_complete: bool,
) -> CriticalPath | None:
    nodes = {event.id: event for event in events}
    if not any(_has_complete_interval(event) for event in events):
        return None
    children: dict[str, list[str]] = {event_id: [] for event_id in nodes}
    confidence_by_pair: dict[tuple[str, str], float] = {}
    incoming: set[str] = set()
    for source, target, confidence in edge_values:
        if source in nodes and target in nodes:
            pair = (source, target)
            if pair not in confidence_by_pair:
                children[source].append(target)
            confidence_by_pair[pair] = max(confidence_by_pair.get(pair, 0.0), confidence)
            incoming.add(target)
    state: dict[str, int] = {}
    memo: dict[str, _Path] = {}
    cycle_detected = False

    def visit(event_id: str) -> _Path:
        nonlocal cycle_detected
        if event_id in memo:
            return memo[event_id]
        stack = [(event_id, False)]
        while stack:
            current_id, expanded = stack.pop()
            if current_id in memo:
                continue
            if not expanded:
                if state.get(current_id) == 1:
                    cycle_detected = True
                    continue
                state[current_id] = 1
                stack.append((current_id, True))
                for child_id in reversed(children[current_id]):
                    if child_id in memo:
                        continue
                    if state.get(child_id) == 1:
                        cycle_detected = True
                    else:
                        stack.append((child_id, False))
                continue

            event = nodes[current_id]
            event_interval = _event_interval(event) if _has_complete_interval(event) else None
            candidates = [
                _Path(
                    (
                        _merge_intervals((event_interval, *memo[child_id].intervals))
                        if event_interval is not None
                        else memo[child_id].intervals
                    ),
                    current_id,
                    memo[child_id],
                    memo[child_id].length + 1,
                )
                for child_id in children[current_id]
                if child_id in memo
            ]
            result = max(
                candidates,
                key=_path_key,
                default=_Path(
                    (event_interval,) if event_interval is not None else (),
                    current_id,
                    None,
                    1,
                ),
            )
            state[current_id] = 2
            memo[current_id] = result
        return memo[event_id]

    roots = [event_id for event_id in nodes if event_id not in incoming]
    candidates = [visit(root) for root in roots]
    for event_id in nodes:
        if event_id not in memo:
            candidates.append(visit(event_id))
    best_path = max(
        candidates,
        key=_path_key,
    )
    event_ids = best_path.event_ids()
    selected_confidences = tuple(
        confidence_by_pair[(source, target)]
        for source, target in zip(event_ids, event_ids[1:], strict=False)
    )
    edges_observed = all(confidence == 1.0 for confidence in selected_confidences)
    causal_structure_observed = len(nodes) == 1 or bool(selected_confidences)
    timing_complete = all(_has_complete_interval(nodes[event_id]) for event_id in event_ids)
    clock_domains = {nodes[event_id].clock_domain for event_id in event_ids}
    shared_clock_domain = len(clock_domains) == 1 and None not in clock_domains
    duration_seconds = best_path.duration_ns / 1_000_000_000
    return CriticalPath(
        duration_seconds=duration_seconds,
        active_seconds=best_path.active_ns / 1_000_000_000,
        waiting_seconds=(best_path.duration_ns - best_path.active_ns) / 1_000_000_000,
        parallel_slack_seconds=max(0.0, round((total or duration_seconds) - duration_seconds, 12)),
        event_ids=event_ids,
        event_names=tuple(nodes[event_id].name for event_id in event_ids),
        certainty=(
            "observed"
            if causal_structure_observed
            and edges_observed
            and timing_complete
            and shared_clock_domain
            and causality_complete
            and not clock_inconsistent
            and not cycle_detected
            else "inferred"
        ),
        cycle_detected=cycle_detected,
    )


def _common_parent_run(
    event_ids: set[str],
    events: tuple[Event, ...],
    parents_by_target: dict[str, set[str]],
) -> Event | None:
    events_by_id = {event.id: event for event in events}
    common_run_ids: set[str] | None = None
    for event_id in event_ids:
        ancestors: set[str] = set()
        seen = {event_id}
        pending = list(parents_by_target.get(event_id, ()))
        while pending:
            ancestor_id = pending.pop()
            if ancestor_id in seen:
                continue
            seen.add(ancestor_id)
            ancestor = events_by_id.get(ancestor_id)
            if ancestor is None:
                continue
            if ancestor.kind == "run":
                ancestors.add(ancestor_id)
            pending.extend(parents_by_target.get(ancestor_id, ()))
        common_run_ids = ancestors if common_run_ids is None else common_run_ids & ancestors
    if common_run_ids is None or len(common_run_ids) != 1:
        return None
    run = events_by_id[next(iter(common_run_ids))]
    return run if run.finished_at_ns is not None else None


def _parent_descendants(root_id: str, edges: tuple[CausalEdge, ...]) -> set[str]:
    children_by_parent: dict[str, list[str]] = {}
    for edge in edges:
        if edge.kind == "parent":
            children_by_parent.setdefault(edge.source_event_id, []).append(edge.target_event_id)
    descendants = {root_id}
    pending = [root_id]
    while pending:
        parent_id = pending.pop()
        for child_id in children_by_parent.get(parent_id, ()):
            if child_id not in descendants:
                descendants.add(child_id)
                pending.append(child_id)
    return descendants


def _throughput(
    events: tuple[Event, ...], edges: tuple[CausalEdge, ...], execution_finished_at_ns: int | None
) -> Throughput | None:
    parents_by_target: dict[str, set[str]] = {}
    for edge in edges:
        if edge.kind == "parent":
            parents_by_target.setdefault(edge.target_event_id, set()).add(edge.source_event_id)
    samples_by_series: dict[tuple[str, str], list[_ProgressSample]] = {}
    event_ids_by_series: dict[tuple[str, str], set[str]] = {}
    entities_by_series: dict[tuple[str, str], set[str | None]] = {}
    clock_domains_by_series: dict[tuple[str, str], set[str | None]] = {}
    for event in events:
        if event.kind != "progress" or event.started_at_ns is None:
            continue
        completed = event.attributes.get("completed")
        total = event.attributes.get("total")
        values = _progress_values(completed, total)
        if values is not None:
            explicit_series = event.attributes.get("series")
            parents = parents_by_target.get(event.id)
            if isinstance(explicit_series, str) and explicit_series:
                series = ("series", explicit_series)
            elif parents is not None and len(parents) == 1:
                series = ("parent", next(iter(parents)))
            elif parents:
                return None
            else:
                series = ("entity", event.entity_id or "unowned")
            samples_by_series.setdefault(series, []).append(
                _ProgressSample(
                    event.started_at_ns,
                    event.uncertainty_ns,
                    event.sequence,
                    *values,
                )
            )
            event_ids_by_series.setdefault(series, set()).add(event.id)
            entities_by_series.setdefault(series, set()).add(event.entity_id)
            clock_domains_by_series.setdefault(series, set()).add(event.clock_domain)
    if len(samples_by_series) != 1:
        return None
    series, samples = next(iter(samples_by_series.items()))
    if len(clock_domains_by_series[series]) != 1:
        return None
    progress_clock_domain = next(iter(clock_domains_by_series[series]))
    series_entities = entities_by_series[series]
    ordered_samples = _ordered_progress_samples(
        samples,
        use_sequence=len(series_entities) == 1 and None not in series_entities,
    )
    if ordered_samples is None:
        return None
    samples = ordered_samples
    latest = samples[-1]
    rate = _sample_rate(samples)
    remaining = max(0.0, latest.total - latest.completed)
    series_event_ids: set[str] | None = None
    series_finished_at_ns = execution_finished_at_ns
    if series[0] == "parent":
        series_event_ids = _parent_descendants(series[1], edges)
        series_parent = next((event for event in events if event.id == series[1]), None)
        if series_parent is not None and series_parent.finished_at_ns is not None:
            series_finished_at_ns = series_parent.finished_at_ns
    else:
        common_run = _common_parent_run(event_ids_by_series[series], events, parents_by_target)
        if common_run is not None:
            series_event_ids = _parent_descendants(common_run.id, edges)
            series_finished_at_ns = common_run.finished_at_ns
    compute_finishes = [
        event.finished_at_ns
        for event in events
        if event.kind == "stage"
        and event.finished_at_ns is not None
        and progress_clock_domain is not None
        and event.clock_domain == progress_clock_domain
        and len(series_entities) == 1
        and event.entity_id in series_entities
        and (series_event_ids is None or event.id in series_event_ids)
        and (
            "compute" in event.name.lower()
            or event.attributes.get("phase") == "compute"
            or event.attributes.get("phase") == "executing"
        )
    ]
    compute_finished_at_ns = max(compute_finishes) if compute_finishes else None
    remaining_at_compute: float | None = None
    post_compute_seconds: float | None = None
    post_compute_rate: float | None = None
    if compute_finished_at_ns is not None:
        before_compute = [
            sample for sample in samples if sample.timestamp_ns <= compute_finished_at_ns
        ]
        if before_compute:
            sample = before_compute[-1]
            remaining_at_compute = max(0.0, sample.total - sample.completed)
        if series_finished_at_ns is not None:
            post_compute_seconds = max(
                0.0, (series_finished_at_ns - compute_finished_at_ns) / 1_000_000_000
            )
        after_compute = [
            sample for sample in samples if sample.timestamp_ns >= compute_finished_at_ns
        ]
        post_compute_rate = _sample_rate(after_compute)
    estimated_drain = 0.0 if remaining == 0 else _finite_ratio(remaining, rate)
    return Throughput(
        latest.completed,
        latest.total,
        rate,
        remaining,
        estimated_drain,
        compute_finished_at_ns,
        remaining_at_compute,
        post_compute_seconds,
        post_compute_rate,
    )


def _progress_values(completed: object, total: object) -> tuple[float, float] | None:
    if (
        not isinstance(completed, (int, float))
        or isinstance(completed, bool)
        or not isinstance(total, (int, float))
        or isinstance(total, bool)
    ):
        return None
    try:
        completed_value = float(completed)
        total_value = float(total)
    except OverflowError:
        return None
    completed_is_inexact = isinstance(completed, int) and int(completed_value) != completed
    total_is_inexact = isinstance(total, int) and int(total_value) != total
    if completed_is_inexact or total_is_inexact:
        return None
    if not (
        math.isfinite(completed_value)
        and math.isfinite(total_value)
        and 0 <= completed_value <= total_value
    ):
        return None
    return completed_value, total_value


def _ordered_progress_samples(
    samples: list[_ProgressSample],
    *,
    use_sequence: bool,
) -> list[_ProgressSample] | None:
    sequences = [sample.sequence for sample in samples]
    if (
        use_sequence
        and all(sequence is not None for sequence in sequences)
        and len(set(sequences)) == len(sequences)
    ):
        return sorted(samples, key=lambda sample: sample.sequence or 0)

    samples_by_timestamp: dict[int, _ProgressSample] = {}
    for sample in samples:
        previous = samples_by_timestamp.get(sample.timestamp_ns)
        if previous is not None:
            if (previous.completed, previous.total) != (sample.completed, sample.total):
                return None
            uncertainty_ns = max(previous.uncertainty_ns or 0, sample.uncertainty_ns or 0)
            samples_by_timestamp[sample.timestamp_ns] = _ProgressSample(
                sample.timestamp_ns,
                uncertainty_ns,
                None,
                sample.completed,
                sample.total,
            )
        else:
            samples_by_timestamp[sample.timestamp_ns] = sample
    ordered = [samples_by_timestamp[timestamp] for timestamp in sorted(samples_by_timestamp)]
    return ordered if _timestamps_establish_order(ordered) else None


def _timestamps_establish_order(samples: list[_ProgressSample]) -> bool:
    return all(
        previous.timestamp_ns + (previous.uncertainty_ns or 0)
        < current.timestamp_ns - (current.uncertainty_ns or 0)
        for previous, current in zip(samples, samples[1:], strict=False)
    )


def _sample_rate(samples: list[_ProgressSample]) -> float | None:
    if len(samples) < 2:
        return None
    first_total = samples[0].total
    if any(sample.total != first_total for sample in samples[1:]):
        return None
    if any(
        current.completed < previous.completed
        for previous, current in zip(samples, samples[1:], strict=False)
    ):
        return None
    if not _timestamps_establish_order(samples):
        return None
    elapsed = (samples[-1].timestamp_ns - samples[0].timestamp_ns) / 1_000_000_000
    delta = samples[-1].completed - samples[0].completed
    return _finite_ratio(delta, elapsed)


def _finite_ratio(numerator: float, denominator: float | None) -> float | None:
    if denominator is None or numerator <= 0 or denominator <= 0:
        return None
    result = numerator / denominator
    return result if math.isfinite(result) else None


def _kubernetes_workload_events(
    events: tuple[Event, ...], edges: tuple[CausalEdge, ...]
) -> tuple[tuple[Event, ...], tuple[Event, ...], tuple[Event, ...]]:
    jobs = tuple(event for event in events if event.kind == "workload.job")
    pods = tuple(event for event in events if event.kind == "workload.pod")
    containers = tuple(event for event in events if event.kind == "workload.container")
    if not jobs or not pods:
        return jobs, pods, containers
    correlated_pods = {edge.source_event_id for edge in edges if edge.kind == "correlates"}
    correlated_jobs = {
        edge.source_event_id
        for edge in edges
        if edge.kind == "owns" and edge.target_event_id in correlated_pods
    }
    if len(correlated_jobs) == 1:
        job_id = next(iter(correlated_jobs))
    elif not correlated_jobs and len(jobs) == 1:
        job_id = jobs[0].id
    else:
        return (), (), ()
    jobs = tuple(event for event in jobs if event.id == job_id)
    pod_ids = {
        edge.target_event_id
        for edge in edges
        if edge.kind == "owns" and edge.source_event_id == job_id
    }
    if not pod_ids:
        return (), (), ()
    pods = tuple(event for event in pods if event.id in pod_ids)
    container_ids = {
        edge.target_event_id
        for edge in edges
        if edge.kind == "contains" and edge.source_event_id in {pod.id for pod in pods}
    }
    containers = tuple(event for event in containers if event.id in container_ids)
    return jobs, pods, containers


def _complete_cohort_start(events: tuple[Event, ...]) -> int | None:
    starts = [event.started_at_ns for event in events if event.started_at_ns is not None]
    return min(starts) if len(starts) == len(events) and starts else None


def _complete_cohort_finish(events: tuple[Event, ...]) -> int | None:
    finishes = [event.finished_at_ns for event in events if event.finished_at_ns is not None]
    return max(finishes) if len(finishes) == len(events) and finishes else None


def _lifecycle(
    events: tuple[Event, ...], edges: tuple[CausalEdge, ...], total: float | None
) -> tuple[LifecyclePhase, ...]:
    complete_stage_ids = {
        event.id for event in events if event.kind == "stage" and _duration_ns(event) > 0
    }
    nested_stage_ids = {
        edge.target_event_id
        for edge in edges
        if edge.kind == "parent"
        and edge.source_event_id in complete_stage_ids
        and edge.target_event_id in complete_stage_ids
    }
    explicit = tuple(
        LifecyclePhase(event.name, _duration_ns(event) / 1_000_000_000, "explicit")
        for event in events
        if event.id in complete_stage_ids and event.id not in nested_stage_ids
    )
    if explicit:
        return explicit
    jobs, pods, containers = _kubernetes_workload_events(events, edges)
    if jobs and pods:
        job_start = _complete_cohort_start(jobs)
        job_finish = _complete_cohort_finish(jobs)
        pod_start = _complete_cohort_start(pods)
        container_start = _complete_cohort_start(containers)
        container_finish = _complete_cohort_finish(containers)
        boundaries = (
            ("provisioning", job_start, pod_start),
            ("starting", pod_start, container_start),
            ("executing", container_start, container_finish),
            ("cleanup", container_finish, job_finish),
        )
        phases = tuple(
            LifecyclePhase(name, (finish - start) / 1_000_000_000, "derived")
            for name, start, finish in boundaries
            if start is not None and finish is not None and finish > start
        )
        return phases
    return (LifecyclePhase("executing", total, "derived"),) if total is not None else ()


def _selected_kubernetes_emitted_event_ids(
    events: tuple[Event, ...], edges: tuple[CausalEdge, ...]
) -> set[str] | None:
    has_jobs = any(event.kind == "workload.job" for event in events)
    has_pods = any(event.kind == "workload.pod" for event in events)
    jobs, pods, containers = _kubernetes_workload_events(events, edges)
    if not jobs or not pods:
        return None if not has_jobs or not has_pods else set()
    selected_lifecycle_ids = {event.id for event in (*jobs, *pods, *containers)}
    return {
        edge.target_event_id
        for edge in edges
        if edge.kind == "emits" and edge.source_event_id in selected_lifecycle_ids
    }


def _bottlenecks(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
    critical: CriticalPath | None,
    total: float | None,
) -> tuple[Bottleneck, ...]:
    if total is None or total <= 0:
        return ()
    findings: list[Bottleneck] = []
    selected_kubernetes_event_ids = _selected_kubernetes_emitted_event_ids(events, edges)
    failed_scheduling_count = sum(
        event.kind == "kubernetes.event"
        and event.name == "FailedScheduling"
        and (selected_kubernetes_event_ids is None or event.id in selected_kubernetes_event_ids)
        for event in events
    )
    if failed_scheduling_count:
        noun = "event" if failed_scheduling_count == 1 else "events"
        verb = "indicates" if failed_scheduling_count == 1 else "indicate"
        findings.append(
            Bottleneck(
                "capacity_starvation",
                (
                    f"{failed_scheduling_count} Kubernetes FailedScheduling {noun} "
                    f"{verb} placement failure"
                ),
                0.85,
            )
        )
    run_intervals = tuple(
        _event_interval(event)
        for event in events
        if event.kind == "run" and _has_complete_interval(event)
    )
    events_by_id = {event.id: event for event in events}
    parents_by_target: dict[str, set[str]] = {}
    for edge in edges:
        if edge.kind == "parent":
            parents_by_target.setdefault(edge.target_event_id, set()).add(edge.source_event_id)
    for event in events:
        concurrency = event.attributes.get("concurrency")
        if (
            event.kind != "stage"
            or not isinstance(concurrency, (int, float))
            or isinstance(concurrency, bool)
            or concurrency != 1
        ):
            continue
        duration = _duration_ns(event) / 1_000_000_000
        causal_run_found, comparison_window = _causal_run_window(
            event.id, events_by_id, parents_by_target
        )
        if causal_run_found and comparison_window is None:
            continue
        if comparison_window is None:
            enclosing_run_durations = tuple(
                (finish - start) / 1_000_000_000
                for start, finish in run_intervals
                if event.started_at_ns is not None
                and event.finished_at_ns is not None
                and start <= event.started_at_ns
                and finish >= event.finished_at_ns
            )
            comparison_window = min(enclosing_run_durations, default=total)
        if comparison_window > 0 and duration / comparison_window >= 0.25:
            findings.append(
                Bottleneck(
                    "serialized_stage",
                    f"{event.name} ran at concurrency 1 for {duration:.3f}s",
                    0.9,
                )
            )
    critical_ids = set(critical.event_ids) if critical is not None else set()
    client_intervals = tuple(
        _event_interval(event)
        for event in events
        if event.id in critical_ids and event.kind == "client.request" and _duration_ns(event) > 0
    )
    client_seconds = (
        sum(finish - start for start, finish in _merge_intervals(client_intervals)) / 1_000_000_000
    )
    if (
        critical is not None
        and critical.duration_seconds > 0
        and client_seconds / critical.duration_seconds >= 0.5
    ):
        findings.append(
            Bottleneck(
                "external_dependency",
                (
                    ("inferred " if critical.certainty == "inferred" else "")
                    + f"client operations occupy {client_seconds:.3f}s of a "
                    f"{critical.duration_seconds:.3f}s critical path"
                ),
                0.75 if critical.certainty == "observed" else 0.5,
            )
        )
    queue_wait = _queue_wait(events, events_by_id, parents_by_target, total)
    if queue_wait is not None:
        findings.append(queue_wait)
    retry_amplification = _retry_amplification(events)
    if retry_amplification is not None:
        findings.append(retry_amplification)
    straggler = _straggler_tail(events, total)
    if straggler is not None:
        findings.append(straggler)
    return tuple(findings)


def _causal_run_window(
    event_id: str,
    events_by_id: dict[str, Event],
    parents_by_target: dict[str, set[str]],
) -> tuple[bool, float | None]:
    """Return the nearest unambiguous causal run duration, when one exists."""
    seen = {event_id}
    frontier = {event_id}
    while frontier:
        parents = {
            parent_id
            for child_id in frontier
            for parent_id in parents_by_target.get(child_id, ())
            if parent_id not in seen
        }
        if not parents:
            return False, None
        runs = [
            events_by_id[parent_id]
            for parent_id in parents
            if events_by_id[parent_id].kind == "run"
        ]
        if runs:
            if len(runs) != 1 or not _has_complete_interval(runs[0]):
                return True, None
            return True, _duration_ns(runs[0]) / 1_000_000_000
        seen.update(parents)
        frontier = parents
    return False, None


def _queue_wait(
    events: tuple[Event, ...],
    events_by_id: dict[str, Event],
    parents_by_target: dict[str, set[str]],
    total: float,
) -> Bottleneck | None:
    candidates: list[tuple[float, str, float]] = []
    for event in events:
        if event.kind != "queue.wait" or not _has_complete_interval(event):
            continue
        duration = _duration_ns(event) / 1_000_000_000
        causal_run_found, comparison_window = _causal_run_window(
            event.id, events_by_id, parents_by_target
        )
        if causal_run_found and comparison_window is None:
            continue
        comparison_window = total if comparison_window is None else comparison_window
        if comparison_window <= 0 or duration / comparison_window < 0.25:
            continue
        activity_type = event.attributes.get("temporal.activity_type")
        name = activity_type if isinstance(activity_type, str) else event.name
        candidates.append((duration / comparison_window, name, duration))
    if not candidates:
        return None
    ratio, name, duration = max(candidates)
    return Bottleneck(
        "queue_wait",
        f"{name} waited {duration:.3f}s in queue ({ratio:.0%} of its run)",
        0.9,
    )


def _retry_amplification(events: tuple[Event, ...]) -> Bottleneck | None:
    candidates: list[tuple[int, str, str]] = []
    for event in events:
        if event.kind != "temporal.activity":
            continue
        attempt = event.attributes.get("temporal.attempt")
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt <= 1:
            continue
        outcome = event.attributes.get("temporal.outcome")
        state = outcome if isinstance(outcome, str) else "started"
        candidates.append((attempt, event.name, state))
    if not candidates:
        return None
    attempt, name, state = max(candidates)
    return Bottleneck(
        "retry_amplification",
        f"{name} {state} on Temporal attempt {attempt}",
        0.9,
    )


def _straggler_tail(events: tuple[Event, ...], total: float) -> Bottleneck | None:
    cohorts: dict[tuple[str, str, str], list[Event]] = {}
    for event in events:
        if event.kind not in {
            "operation",
            "message.consume",
            "server.request",
        } or not _has_complete_interval(event):
            continue
        domain = event.clock_domain or f"entity:{event.entity_id or 'unowned'}"
        cohorts.setdefault((event.kind, event.name, domain), []).append(event)
    candidates: list[tuple[float, str, float, float, int]] = []
    for (_, name, _), cohort in cohorts.items():
        if len(cohort) < 5:
            continue
        starts = [event.started_at_ns for event in cohort]
        if any(value is None for value in starts):
            continue
        known_starts = [value for value in starts if value is not None]
        start_spread = (max(known_starts) - min(known_starts)) / 1_000_000_000
        if start_spread > total * 0.1:
            continue
        durations = [_duration_ns(event) / 1_000_000_000 for event in cohort]
        typical = median(durations)
        longest = max(durations)
        excess = longest - typical
        if typical <= 0 or longest < typical * 2.5 or excess < total * 0.1:
            continue
        candidates.append((excess, name, longest, typical, len(cohort)))
    if not candidates:
        return None
    _, name, longest, typical, count = max(candidates)
    return Bottleneck(
        "straggler_tail",
        f"{name} max duration {longest:.3f}s versus {typical:.3f}s median "
        f"across {count} operations",
        0.8,
    )


def _metadata_count(value: JsonValue | None) -> int:
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= (1 << 63) - 1
        else 0
    )


def _semantic_count(value: JsonValue | None) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > (1 << 63) - 1:
        return None
    return value


def _profile_snapshot_metrics(instrumentation: dict[str, JsonValue]) -> ProfileSnapshotMetrics:
    raw = instrumentation.get("snapshot_metrics")
    unavailable = ProfileSnapshotMetrics("unavailable", 0, 0, 0, 0.0, 0.0, 0, 0, 0, 0.0, 0.0)
    invalid = ProfileSnapshotMetrics("invalid", 0, 0, 0, 0.0, 0.0, 0, 0, 0, 0.0, 0.0)
    if raw is None:
        return unavailable
    if not isinstance(raw, dict):
        return invalid
    status = raw.get("status")
    if status == "available":
        normalized_status: Literal["available", "unavailable"] = "available"
    elif status == "unavailable":
        normalized_status = "unavailable"
    else:
        return invalid
    values: dict[str, int] = {}
    for key in (
        "message_count",
        "payload_bytes",
        "max_payload_bytes",
        "serialization_ns",
        "max_serialization_ns",
        "checkpoint_message_count",
        "checkpoint_payload_bytes",
        "max_checkpoint_payload_bytes",
        "checkpoint_serialization_ns",
        "max_checkpoint_serialization_ns",
    ):
        value = raw.get(key)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > (1 << 63) - 1
        ):
            return invalid
        values[key] = value
    if (
        values["max_payload_bytes"] > values["payload_bytes"]
        or values["max_serialization_ns"] > values["serialization_ns"]
        or values["checkpoint_message_count"] > values["message_count"]
        or values["checkpoint_payload_bytes"] > values["payload_bytes"]
        or values["max_checkpoint_payload_bytes"] > values["checkpoint_payload_bytes"]
        or values["checkpoint_serialization_ns"] > values["serialization_ns"]
        or values["max_checkpoint_serialization_ns"] > values["checkpoint_serialization_ns"]
        or (normalized_status == "unavailable" and any(values.values()))
    ):
        return invalid
    return ProfileSnapshotMetrics(
        normalized_status,
        values["message_count"],
        values["payload_bytes"],
        values["max_payload_bytes"],
        values["serialization_ns"] / 1_000_000_000,
        values["max_serialization_ns"] / 1_000_000_000,
        values["checkpoint_message_count"],
        values["checkpoint_payload_bytes"],
        values["max_checkpoint_payload_bytes"],
        values["checkpoint_serialization_ns"] / 1_000_000_000,
        values["max_checkpoint_serialization_ns"] / 1_000_000_000,
    )


def _profile_publication_metrics(
    instrumentation: dict[str, JsonValue],
) -> ProfilePublicationMetrics:
    raw = instrumentation.get("publication_metrics")
    unavailable = ProfilePublicationMetrics("unavailable", 0, (), 0, 0.0, 0.0)
    invalid = ProfilePublicationMetrics("invalid", 0, (), 0, 0.0, 0.0)
    if raw is None:
        return unavailable
    if not isinstance(raw, dict):
        return invalid
    status = raw.get("status")
    if status == "available":
        normalized_status: Literal["available", "unavailable"] = "available"
    elif status == "unavailable":
        normalized_status = "unavailable"
    else:
        return invalid
    values: dict[str, int] = {}
    for key in (
        "fallback_process_count",
        "socket_attempted_process_count",
        "socket_failure_ns",
        "max_socket_failure_ns",
    ):
        value = raw.get(key)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > (1 << 63) - 1
        ):
            return invalid
        values[key] = value
    raw_process_ids = raw.get("fallback_process_ids")
    if (
        not isinstance(raw_process_ids, list)
        or len(raw_process_ids) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return invalid
    process_ids: list[int] = []
    seen: set[int] = set()
    for raw_process_id in raw_process_ids:
        if (
            not isinstance(raw_process_id, int)
            or isinstance(raw_process_id, bool)
            or raw_process_id <= 0
            or raw_process_id in seen
        ):
            return invalid
        seen.add(raw_process_id)
        process_ids.append(raw_process_id)
    if (
        values["fallback_process_count"] != len(process_ids)
        or values["socket_attempted_process_count"] > values["fallback_process_count"]
        or values["max_socket_failure_ns"] > values["socket_failure_ns"]
        or (
            values["socket_attempted_process_count"] == 0
            and (values["socket_failure_ns"] or values["max_socket_failure_ns"])
        )
        or (normalized_status == "unavailable" and (process_ids or any(values.values())))
    ):
        return invalid
    return ProfilePublicationMetrics(
        normalized_status,
        values["fallback_process_count"],
        tuple(process_ids),
        values["socket_attempted_process_count"],
        values["socket_failure_ns"] / 1_000_000_000,
        values["max_socket_failure_ns"] / 1_000_000_000,
    )


def _profile_normalization_metrics(
    instrumentation: dict[str, JsonValue],
) -> ProfileNormalizationMetrics:
    raw = instrumentation.get("normalization_metrics")
    unavailable = ProfileNormalizationMetrics("unavailable", 0.0, 0, 0)
    invalid = ProfileNormalizationMetrics("invalid", 0.0, 0, 0)
    if raw is None:
        return unavailable
    if not isinstance(raw, dict):
        return invalid
    status = raw.get("status")
    if status == "available":
        normalized_status: Literal["available", "unavailable"] = "available"
    elif status == "unavailable":
        normalized_status = "unavailable"
    else:
        return invalid
    values: dict[str, int] = {}
    for key in (
        "duration_ns",
        "ranking_database_peak_bytes",
        "ranking_database_limit_bytes",
    ):
        value = raw.get(key)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > (1 << 63) - 1
        ):
            return invalid
        values[key] = value
    if (
        values["ranking_database_limit_bytes"] <= 0
        or values["ranking_database_peak_bytes"] > values["ranking_database_limit_bytes"]
        or (
            normalized_status == "unavailable"
            and (values["duration_ns"] or values["ranking_database_peak_bytes"])
        )
    ):
        return invalid
    return ProfileNormalizationMetrics(
        normalized_status,
        values["duration_ns"] / 1_000_000_000,
        values["ranking_database_peak_bytes"],
        values["ranking_database_limit_bytes"],
    )


def _profile_process_ids(
    instrumentation: dict[str, JsonValue],
    process_count: int,
) -> tuple[Literal["complete", "unavailable", "invalid"], tuple[int, ...]]:
    raw_process_ids = instrumentation.get("process_ids")
    if raw_process_ids is None:
        return "unavailable", ()
    if (
        not isinstance(raw_process_ids, list)
        or len(raw_process_ids) != process_count
        or len(raw_process_ids) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return "invalid", ()
    process_ids: list[int] = []
    seen: set[int] = set()
    for raw_process_id in raw_process_ids:
        if (
            not isinstance(raw_process_id, int)
            or isinstance(raw_process_id, bool)
            or raw_process_id <= 0
            or raw_process_id in seen
        ):
            return "invalid", ()
        seen.add(raw_process_id)
        process_ids.append(raw_process_id)
    return "complete", tuple(sorted(process_ids))


def _is_python_process_name(name: str) -> bool:
    normalized = Path(name).name.casefold()
    return normalized.startswith("python") or normalized.startswith("pypy")


def _python_profile_coverage(
    instrumentation: dict[str, JsonValue],
    *,
    profile_status: str,
    process_count: int,
    process_observer: ProcessObserverSummary | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> tuple[tuple[int, ...], PythonProfileCoverage]:
    identity_status, process_ids = _profile_process_ids(instrumentation, process_count)
    if profile_status == "invalid" or identity_status == "invalid":
        return (), PythonProfileCoverage("invalid", process_count, None, 0, 0, (), ())
    if (
        identity_status == "unavailable"
        or process_observer is None
        or process_observer.status in {"unavailable", "invalid"}
    ):
        return process_ids, PythonProfileCoverage(
            "unavailable",
            process_count,
            None,
            0,
            0,
            (),
            (),
        )
    profile_id_set = set(process_ids)
    observed_python_ids = {
        pid
        for pid, identity in observed_processes.items()
        if pid in profile_id_set or _is_python_process_name(identity.name)
    }
    unprofiled_ids = observed_python_ids - profile_id_set
    unprofiled = tuple(
        PythonProfileCoverageGap(
            pid,
            observed_processes[pid].name,
            observed_processes[pid].parent_name,
        )
        for pid in sorted(unprofiled_ids)[:MAX_PROFILE_COVERAGE_GAPS]
    )
    unobserved_profile_ids = tuple(sorted(profile_id_set - observed_processes.keys()))
    coverage_status: Literal["complete", "partial"] = (
        "complete"
        if process_observer.status == "complete"
        and not unprofiled_ids
        and not unobserved_profile_ids
        else "partial"
    )
    return process_ids, PythonProfileCoverage(
        coverage_status,
        process_count,
        len(observed_python_ids),
        len(profile_id_set & observed_python_ids),
        len(unprofiled_ids),
        unprofiled,
        unobserved_profile_ids,
    )


def _native_call_capture_summary(
    instrumentation: dict[str, JsonValue],
    *,
    profile_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"],
    profile_function_count: int,
) -> NativeCallCaptureSummary | None:
    raw_capture = instrumentation.get("native_call_capture")
    if raw_capture is None:
        return None

    def invalid() -> NativeCallCaptureSummary:
        return NativeCallCaptureSummary(
            "invalid",
            0,
            0,
            0,
            MAX_NATIVE_FUNCTIONS_PER_PROCESS,
            MAX_NATIVE_EDGES_PER_PROCESS,
        )

    if not isinstance(raw_capture, dict):
        return invalid()
    function_count = raw_capture.get("function_count")
    call_count = raw_capture.get("call_count")
    exception_count = raw_capture.get("exception_count")
    limits = raw_capture.get("limits")
    if (
        raw_capture.get("enabled") is not True
        or raw_capture.get("deep_only") is not True
        or raw_capture.get("arguments_captured") is not False
        or raw_capture.get("return_values_captured") is not False
        or raw_capture.get("exception_messages_captured") is not False
        or not isinstance(function_count, int)
        or isinstance(function_count, bool)
        or not 0 <= function_count <= profile_function_count
        or not isinstance(call_count, int)
        or isinstance(call_count, bool)
        or call_count < function_count
        or not isinstance(exception_count, int)
        or isinstance(exception_count, bool)
        or not 0 <= exception_count <= call_count
        or not isinstance(limits, dict)
        or limits.get("max_functions_per_process") != MAX_NATIVE_FUNCTIONS_PER_PROCESS
        or limits.get("max_edges_per_process") != MAX_NATIVE_EDGES_PER_PROCESS
    ):
        return invalid()
    return NativeCallCaptureSummary(
        profile_status,
        function_count,
        call_count,
        exception_count,
        MAX_NATIVE_FUNCTIONS_PER_PROCESS,
        MAX_NATIVE_EDGES_PER_PROCESS,
    )


def _python_exception_capture_summary(
    instrumentation: dict[str, JsonValue],
    *,
    profile_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"],
    profile_function_count: int,
    observer_integrity: ObserverIntegritySummary | None,
) -> PythonExceptionCaptureSummary | None:
    raw_capture = instrumentation.get("python_exception_capture")
    if raw_capture is None:
        return None

    def invalid() -> PythonExceptionCaptureSummary:
        return PythonExceptionCaptureSummary(
            "invalid",
            0,
            0,
            0,
            MAX_PYTHON_FUNCTIONS_PER_PROCESS,
        )

    if not isinstance(raw_capture, dict):
        return invalid()
    function_count = raw_capture.get("function_count")
    event_count = raw_capture.get("event_count")
    dropped_event_count = raw_capture.get("dropped_event_count")
    limits = raw_capture.get("limits")
    if (
        raw_capture.get("enabled") is not True
        or raw_capture.get("deep_only") is not True
        or raw_capture.get("event_semantics") != "per_propagated_frame"
        or raw_capture.get("arguments_captured") is not False
        or raw_capture.get("locals_captured") is not False
        or raw_capture.get("exception_types_captured") is not False
        or raw_capture.get("exception_values_captured") is not False
        or raw_capture.get("exception_messages_captured") is not False
        or raw_capture.get("tracebacks_captured") is not False
        or raw_capture.get("line_events_enabled") is not False
        or raw_capture.get("opcode_events_enabled") is not False
        or not isinstance(function_count, int)
        or isinstance(function_count, bool)
        or not 0 <= function_count <= profile_function_count
        or not isinstance(event_count, int)
        or isinstance(event_count, bool)
        or event_count < function_count
        or not isinstance(dropped_event_count, int)
        or isinstance(dropped_event_count, bool)
        or dropped_event_count < 0
        or not isinstance(limits, dict)
        or limits.get("max_functions_per_process") != MAX_PYTHON_FUNCTIONS_PER_PROCESS
    ):
        return invalid()
    exception_status = profile_status
    if observer_integrity is not None:
        if observer_integrity.status == "invalid":
            exception_status = "invalid"
        elif observer_integrity.status == "partial" and (
            observer_integrity.trace_hook_setter_call_count
            or observer_integrity.missing_process_count
        ):
            exception_status = "partial"
    control_flow_filter: PythonExceptionControlFlowFilterSummary | None = None
    raw_control_flow_filter = raw_capture.get("control_flow_filter")
    if raw_control_flow_filter is not None:
        if not isinstance(raw_control_flow_filter, dict):
            return invalid()
        raw_filter_status = raw_control_flow_filter.get("status")
        non_control_flow_function_count = raw_control_flow_filter.get(
            "non_control_flow_function_count"
        )
        non_control_flow_event_count = raw_control_flow_filter.get("non_control_flow_event_count")
        filtered_event_count = raw_control_flow_filter.get("filtered_event_count")
        dropped_non_control_flow_event_count = raw_control_flow_filter.get(
            "dropped_non_control_flow_event_count"
        )
        dropped_filtered_event_count = raw_control_flow_filter.get("dropped_filtered_event_count")
        filter_counts = (
            non_control_flow_function_count,
            non_control_flow_event_count,
            filtered_event_count,
            dropped_non_control_flow_event_count,
            dropped_filtered_event_count,
        )
        if (
            raw_control_flow_filter.get("format_version") != PYTHON_EXCEPTION_FILTER_VERSION
            or raw_filter_status != profile_status
            or raw_control_flow_filter.get("event_semantics") != "exact_type_identity"
            or raw_control_flow_filter.get("filtered_exception_types")
            != list(FILTERED_CONTROL_FLOW_EXCEPTION_TYPES)
            or raw_control_flow_filter.get("exception_type_identity_inspected") is not True
            or raw_control_flow_filter.get("exception_types_captured") is not False
            or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 0
                for value in filter_counts
            )
        ):
            return invalid()
        assert isinstance(non_control_flow_function_count, int)
        assert isinstance(non_control_flow_event_count, int)
        assert isinstance(filtered_event_count, int)
        assert isinstance(dropped_non_control_flow_event_count, int)
        assert isinstance(dropped_filtered_event_count, int)
        if (
            non_control_flow_function_count > function_count
            or non_control_flow_event_count < non_control_flow_function_count
            or non_control_flow_event_count > event_count
            or filtered_event_count != event_count - non_control_flow_event_count
            or dropped_non_control_flow_event_count > dropped_event_count
            or dropped_filtered_event_count
            != dropped_event_count - dropped_non_control_flow_event_count
        ):
            return invalid()
        control_flow_filter = PythonExceptionControlFlowFilterSummary(
            exception_status,
            non_control_flow_function_count,
            non_control_flow_event_count,
            filtered_event_count,
            dropped_non_control_flow_event_count,
            dropped_filtered_event_count,
        )
    return PythonExceptionCaptureSummary(
        exception_status,
        function_count,
        event_count,
        dropped_event_count,
        MAX_PYTHON_FUNCTIONS_PER_PROCESS,
        control_flow_filter,
    )


def _observer_integrity_summary(
    instrumentation: dict[str, JsonValue],
    *,
    profile_process_count: int,
) -> ObserverIntegritySummary | None:
    raw_integrity = instrumentation.get("observer_integrity")
    if raw_integrity is None:
        return None

    def invalid() -> ObserverIntegritySummary:
        return ObserverIntegritySummary("invalid", 0, profile_process_count, 0, 0, 0, 0)

    if not isinstance(raw_integrity, dict):
        return invalid()
    raw_status = raw_integrity.get("status")
    process_count = raw_integrity.get("process_count")
    missing_process_count = raw_integrity.get("missing_process_count")
    profile_hook_setter_call_count = raw_integrity.get("profile_hook_setter_call_count")
    profile_hook_setter_process_count = raw_integrity.get("profile_hook_setter_process_count")
    trace_hook_setter_call_count = raw_integrity.get("trace_hook_setter_call_count")
    trace_hook_setter_process_count = raw_integrity.get("trace_hook_setter_process_count")
    values = (
        process_count,
        missing_process_count,
        profile_hook_setter_call_count,
        profile_hook_setter_process_count,
        trace_hook_setter_call_count,
        trace_hook_setter_process_count,
    )
    if (
        raw_integrity.get("format_version") != 1
        or raw_status not in {"complete", "partial", "unavailable"}
        or raw_integrity.get("arguments_captured") is not False
        or raw_integrity.get("locals_captured") is not False
        or raw_integrity.get("hook_values_captured") is not False
        or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values
        )
    ):
        return invalid()
    assert isinstance(process_count, int)
    assert isinstance(missing_process_count, int)
    assert isinstance(profile_hook_setter_call_count, int)
    assert isinstance(profile_hook_setter_process_count, int)
    assert isinstance(trace_hook_setter_call_count, int)
    assert isinstance(trace_hook_setter_process_count, int)
    expected_status = (
        "unavailable"
        if process_count == 0
        else (
            "partial"
            if (
                missing_process_count
                or profile_hook_setter_call_count
                or trace_hook_setter_call_count
            )
            else "complete"
        )
    )
    if (
        process_count + missing_process_count != profile_process_count
        or profile_hook_setter_process_count > process_count
        or trace_hook_setter_process_count > process_count
        or profile_hook_setter_call_count < profile_hook_setter_process_count
        or trace_hook_setter_call_count < trace_hook_setter_process_count
        or raw_status != expected_status
    ):
        return invalid()
    status: Literal["complete", "partial", "unavailable", "invalid"] = expected_status
    return ObserverIntegritySummary(
        status,
        process_count,
        missing_process_count,
        profile_hook_setter_call_count,
        profile_hook_setter_process_count,
        trace_hook_setter_call_count,
        trace_hook_setter_process_count,
    )


def _deep_profile_summary(
    metadata: dict[str, JsonValue],
    process_observer: ProcessObserverSummary | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> DeepProfileSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict) or instrumentation.get("mode") != "deep":
        return None
    raw_status = instrumentation.get("status")
    status = (
        raw_status
        if isinstance(raw_status, str)
        and raw_status in {"complete", "partial", "truncated", "unavailable", "invalid"}
        else "invalid"
    )
    process_count = _metadata_count(instrumentation.get("process_count"))
    checkpoint_interval_ns = _metadata_count(instrumentation.get("checkpoint_interval_ns"))
    first_checkpoint_delay_ns = _metadata_count(instrumentation.get("first_checkpoint_delay_ns"))
    raw_transport = instrumentation.get("transport")
    transport = raw_transport if isinstance(raw_transport, str) and raw_transport else "unknown"
    process_ids, process_coverage = _python_profile_coverage(
        instrumentation,
        profile_status=status,
        process_count=process_count,
        process_observer=process_observer,
        observed_processes=observed_processes,
    )
    native_call_capture = _native_call_capture_summary(
        instrumentation,
        profile_status=status,
        profile_function_count=_metadata_count(instrumentation.get("function_count")),
    )
    observer_integrity = _observer_integrity_summary(
        instrumentation,
        profile_process_count=process_count,
    )
    python_exception_capture = _python_exception_capture_summary(
        instrumentation,
        profile_status=status,
        profile_function_count=_metadata_count(instrumentation.get("function_count")),
        observer_integrity=observer_integrity,
    )
    return DeepProfileSummary(
        status=status,
        process_count=process_count,
        function_count=_metadata_count(instrumentation.get("function_count")),
        edge_count=_metadata_count(instrumentation.get("edge_count")),
        truncated=instrumentation.get("truncated") is True,
        dropped_call_count=_metadata_count(instrumentation.get("dropped_call_count")),
        open_call_count=_metadata_count(instrumentation.get("open_call_count")),
        checkpoint_process_count=_metadata_count(instrumentation.get("checkpoint_process_count")),
        registration_only_process_count=_metadata_count(
            instrumentation.get("registration_only_process_count")
        ),
        dropped_profile_process_count=_metadata_count(
            instrumentation.get("dropped_profile_process_count")
        ),
        dropped_profile_process_count_truncated=(
            instrumentation.get("dropped_profile_process_count_truncated") is True
        ),
        first_checkpoint_delay_seconds=first_checkpoint_delay_ns / 1_000_000_000,
        checkpoint_interval_seconds=checkpoint_interval_ns / 1_000_000_000,
        transport=transport,
        collector_error_count=_metadata_count(instrumentation.get("collector_error_count")),
        snapshot_metrics=_profile_snapshot_metrics(instrumentation),
        publication_metrics=_profile_publication_metrics(instrumentation),
        normalization_metrics=_profile_normalization_metrics(instrumentation),
        process_ids=process_ids,
        process_coverage=process_coverage,
        native_call_capture=native_call_capture,
        python_exception_capture=python_exception_capture,
        observer_integrity=observer_integrity,
    )


def _sample_profile_summary(
    metadata: dict[str, JsonValue],
    process_observer: ProcessObserverSummary | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> SampleProfileSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict) or instrumentation.get("mode") != "sample":
        return None
    raw_status = instrumentation.get("status")
    status = (
        raw_status
        if isinstance(raw_status, str)
        and raw_status in {"complete", "partial", "truncated", "unavailable", "invalid"}
        else "invalid"
    )
    interval_ns = _metadata_count(instrumentation.get("interval_ns"))
    checkpoint_interval_ns = _metadata_count(instrumentation.get("checkpoint_interval_ns"))
    first_checkpoint_delay_ns = _metadata_count(instrumentation.get("first_checkpoint_delay_ns"))
    raw_transport = instrumentation.get("transport")
    transport = raw_transport if isinstance(raw_transport, str) and raw_transport else "unknown"
    process_count = _metadata_count(instrumentation.get("process_count"))
    process_ids, process_coverage = _python_profile_coverage(
        instrumentation,
        profile_status=status,
        process_count=process_count,
        process_observer=process_observer,
        observed_processes=observed_processes,
    )
    return SampleProfileSummary(
        status=status,
        process_count=process_count,
        function_count=_metadata_count(instrumentation.get("function_count")),
        edge_count=_metadata_count(instrumentation.get("edge_count")),
        truncated=instrumentation.get("truncated") is True,
        sample_count=_metadata_count(instrumentation.get("sample_count")),
        thread_sample_count=_metadata_count(instrumentation.get("thread_sample_count")),
        dropped_frame_sample_count=_metadata_count(
            instrumentation.get("dropped_frame_sample_count")
        ),
        interval_seconds=interval_ns / 1_000_000_000,
        checkpoint_process_count=_metadata_count(instrumentation.get("checkpoint_process_count")),
        registration_only_process_count=_metadata_count(
            instrumentation.get("registration_only_process_count")
        ),
        dropped_profile_process_count=_metadata_count(
            instrumentation.get("dropped_profile_process_count")
        ),
        dropped_profile_process_count_truncated=(
            instrumentation.get("dropped_profile_process_count_truncated") is True
        ),
        first_checkpoint_delay_seconds=first_checkpoint_delay_ns / 1_000_000_000,
        checkpoint_interval_seconds=checkpoint_interval_ns / 1_000_000_000,
        transport=transport,
        collector_error_count=_metadata_count(instrumentation.get("collector_error_count")),
        snapshot_metrics=_profile_snapshot_metrics(instrumentation),
        publication_metrics=_profile_publication_metrics(instrumentation),
        normalization_metrics=_profile_normalization_metrics(instrumentation),
        process_ids=process_ids,
        process_coverage=process_coverage,
    )


def _process_observer_summary(metadata: dict[str, JsonValue]) -> ProcessObserverSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    observer = capture.get("process_observer")
    if not isinstance(observer, dict) or observer.get("requested") is not True:
        return None
    raw_status = observer.get("status")
    status = (
        raw_status
        if isinstance(raw_status, str)
        and raw_status in {"complete", "truncated", "partial", "unavailable", "invalid"}
        else "invalid"
    )
    interval_ns = _metadata_count(observer.get("interval_ns"))
    raw_error = observer.get("error")
    error = raw_error if isinstance(raw_error, str) and raw_error else None
    return ProcessObserverSummary(
        status,
        _metadata_count(observer.get("process_count")),
        _metadata_count(observer.get("descendant_process_count")),
        _metadata_count(observer.get("sample_count")),
        _metadata_count(observer.get("poll_count")),
        observer.get("truncated") is True,
        _metadata_count(observer.get("dropped_process_count")),
        interval_ns / 1_000_000_000,
        error,
    )


def _subprocess_callers(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[dict[str, SubprocessCaller], int]:
    by_id = {event.id: event for event in events}
    subprocesses = {event.id: event for event in events if event.kind == "subprocess.run"}
    callers: dict[str, SubprocessCaller] = {}
    invalid_targets: set[str] = set()
    for target_id, event in subprocesses.items():
        caller_event_id = event.attributes.get("caller_event_id")
        if caller_event_id is not None and (
            not isinstance(caller_event_id, str) or not caller_event_id
        ):
            invalid_targets.add(target_id)
    for edge in edges:
        source = by_id.get(edge.source_event_id)
        target = by_id.get(edge.target_event_id)
        if source is None or source.kind != "python.callsite":
            continue
        if edge.kind != "launches" or target is None or target.kind != "subprocess.run":
            if target is not None and target.kind == "subprocess.run":
                invalid_targets.add(target.id)
            continue
        module = source.attributes.get("module")
        qualname = source.attributes.get("qualname")
        filename = source.attributes.get("filename")
        firstlineno = source.attributes.get("firstlineno")
        subprocess_count = source.attributes.get("subprocess_count")
        raw_scope = source.attributes.get("scope")
        raw_observation = edge.attributes.get("observation")
        expected_confidence = 1.0 if raw_observation == "exact" else 0.9
        if (
            target.id in callers
            or target.attributes.get("caller_event_id") != source.id
            or source.attributes.get("source") != "python-subprocess-wrapper"
            or source.attributes.get("arguments_captured") is not False
            or source.attributes.get("locals_captured") is not False
            or not isinstance(module, str)
            or not module
            or not isinstance(qualname, str)
            or not qualname
            or not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(subprocess_count, int)
            or isinstance(subprocess_count, bool)
            or subprocess_count <= 0
            or raw_scope not in {"application", "library", "runtime"}
            or raw_observation not in {"exact", "sampled"}
            or edge.attributes.get("source") != "python-subprocess-wrapper"
            or not math.isclose(edge.confidence, expected_confidence)
            or source.name != f"{module}.{qualname}"
        ):
            invalid_targets.add(target.id)
            callers.pop(target.id, None)
            continue
        scope: Literal["application", "library", "runtime"]
        if raw_scope == "application":
            scope = "application"
        elif raw_scope == "library":
            scope = "library"
        else:
            scope = "runtime"
        observation: Literal["exact", "sampled"] = (
            "exact" if raw_observation == "exact" else "sampled"
        )
        callers[target.id] = SubprocessCaller(
            source.id,
            source.name,
            module,
            qualname,
            filename,
            firstlineno,
            scope,
            observation,
            edge.confidence,
        )
    targets_by_caller: dict[str, list[str]] = {}
    for target_id, caller in callers.items():
        targets_by_caller.setdefault(caller.event_id, []).append(target_id)
    for caller_event_id, target_ids in targets_by_caller.items():
        source = by_id[caller_event_id]
        if source.attributes.get("subprocess_count") != len(target_ids):
            invalid_targets.update(target_ids)
    for target_id, event in subprocesses.items():
        if event.attributes.get("caller_event_id") is not None and target_id not in callers:
            invalid_targets.add(target_id)
    for target_id in invalid_targets:
        callers.pop(target_id, None)
    return callers, len(invalid_targets)


def _subprocess_calls(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> tuple[tuple[SubprocessCall, ...], int, int, int, int]:
    caller_by_subprocess, invalid_caller_count = _subprocess_callers(events, edges)
    calls: list[SubprocessCall] = []
    invalid_event_count = 0
    for event in events:
        if event.kind != "subprocess.run":
            continue
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        child_pid = event.attributes.get("child_pid")
        shell = event.attributes.get("shell")
        raw_outcome = event.attributes.get("outcome")
        exit_code = event.attributes.get("exit_code")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        if (
            not event.name
            or event.started_at_ns is None
            or event.attributes.get("source") != "python-subprocess-wrapper"
            or event.attributes.get("arguments_captured") is not False
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or (
                child_pid is not None
                and (
                    not isinstance(child_pid, int) or isinstance(child_pid, bool) or child_pid <= 0
                )
            )
            or (shell is not None and not isinstance(shell, bool))
            or raw_outcome not in {"exited", "launch_error", "unknown"}
            or (
                exit_code is not None
                and (not isinstance(exit_code, int) or isinstance(exit_code, bool))
            )
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
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
        outcome: Literal["exited", "launch_error", "unknown"]
        if raw_outcome == "exited":
            outcome = "exited"
        elif raw_outcome == "launch_error":
            outcome = "launch_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "exited"
                and (
                    event.finished_at_ns is None
                    or child_pid is None
                    or exit_code is None
                    or (exit_code == 0 and (error_type is not None or error is True))
                    or (exit_code != 0 and (error_type != "subprocess_exit" or error is not True))
                )
            )
            or (
                outcome == "launch_error"
                and (
                    event.finished_at_ns is None
                    or child_pid is not None
                    or exit_code is not None
                    or error_type is None
                    or error is not True
                )
            )
            or (
                outcome == "unknown"
                and (
                    event.finished_at_ns is not None
                    or child_pid is None
                    or exit_code is not None
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
        calls.append(
            SubprocessCall(
                event.id,
                event.name,
                parent_pid,
                role,
                child_pid,
                child_pid in observed_processes if child_pid is not None else False,
                shell,
                outcome,
                exit_code,
                error_type,
                event.started_at_ns,
                duration_seconds,
                caller_by_subprocess.get(event.id),
            )
        )
    calls.sort(key=lambda call: (call.started_at_ns, call.parent_pid, call.event_id))
    return (
        tuple(calls[:MAX_SUBPROCESS_CALL_SUMMARIES]),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_subprocess.values()}),
        len(caller_by_subprocess),
    )


def _semantic_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    subprocess_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_subprocess_count: int,
    invalid_caller_count: int,
) -> SemanticCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    semantic = instrumentation.get("semantic_capture")
    if not isinstance(semantic, dict):
        return None
    raw_status = semantic.get("status")
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    if raw_status == "complete":
        status = "complete"
    elif raw_status == "partial":
        status = "partial"
    elif raw_status == "truncated":
        status = "truncated"
    elif raw_status == "unavailable":
        status = "unavailable"
    else:
        status = "invalid"
    raw_counts = {
        key: _semantic_count(semantic.get(key))
        for key in (
            "process_count",
            "subprocess_count",
            "dropped_subprocess_count",
            "callback_error_count",
        )
    }
    declared_subprocess_count = raw_counts["subprocess_count"]
    if (
        semantic.get("observer") != "python-subprocess-wrapper"
        or semantic.get("zero_code") is not True
        or semantic.get("arguments_captured") is not False
        or semantic.get("environment_captured") is not False
        or semantic.get("working_directory_captured") is not False
        or any(value is None for value in raw_counts.values())
        or declared_subprocess_count != subprocess_event_count
        or invalid_event_count
    ):
        status = "invalid"
    raw_caller = semantic.get("caller_attribution")
    caller_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    caller_counts = {
        "caller_count": 0,
        "attributed_subprocess_count": 0,
        "unattributed_subprocess_count": subprocess_event_count,
        "invalid_caller_count": 0,
        "callback_error_count": 0,
    }
    if raw_caller is None:
        caller_status = "unavailable"
    elif not isinstance(raw_caller, dict):
        caller_status = "invalid"
    else:
        raw_caller_status = raw_caller.get("status")
        if raw_caller_status == "complete":
            caller_status = "complete"
        elif raw_caller_status == "partial":
            caller_status = "partial"
        elif raw_caller_status == "truncated":
            caller_status = "truncated"
        elif raw_caller_status == "unavailable":
            caller_status = "unavailable"
        elif raw_caller_status == "invalid":
            caller_status = "invalid"
        else:
            caller_status = "invalid"
        parsed_caller_counts = {key: _semantic_count(raw_caller.get(key)) for key in caller_counts}
        if all(value is not None for value in parsed_caller_counts.values()):
            caller_counts = {key: value or 0 for key, value in parsed_caller_counts.items()}
        else:
            caller_status = "invalid"
        if (
            raw_caller.get("arguments_captured") is not False
            or raw_caller.get("locals_captured") is not False
            or caller_counts["caller_count"] != caller_count
            or caller_counts["attributed_subprocess_count"] != attributed_subprocess_count
            or caller_counts["attributed_subprocess_count"]
            + caller_counts["unattributed_subprocess_count"]
            != subprocess_event_count
            or caller_counts["invalid_caller_count"] < invalid_caller_count
            or invalid_caller_count
        ):
            caller_status = "invalid"
    return SemanticCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_subprocess_count or 0,
        raw_counts["dropped_subprocess_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller_status,
        caller_counts["caller_count"],
        caller_counts["attributed_subprocess_count"],
        caller_counts["unattributed_subprocess_count"],
        caller_counts["invalid_caller_count"],
        caller_counts["callback_error_count"],
    )


def _http_callers(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[dict[str, HttpCaller], int]:
    by_id = {event.id: event for event in events}
    requests = {event.id: event for event in events if event.kind == "http.client.request"}
    callers: dict[str, HttpCaller] = {}
    invalid_targets: set[str] = set()
    for target_id, event in requests.items():
        caller_event_id = event.attributes.get("caller_event_id")
        if caller_event_id is not None and (
            not isinstance(caller_event_id, str) or not caller_event_id
        ):
            invalid_targets.add(target_id)
    for edge in edges:
        source = by_id.get(edge.source_event_id)
        target = by_id.get(edge.target_event_id)
        if source is None or source.kind != "python.callsite":
            continue
        if edge.kind != "requests" or target is None or target.kind != "http.client.request":
            if target is not None and target.kind == "http.client.request":
                invalid_targets.add(target.id)
            continue
        module = source.attributes.get("module")
        qualname = source.attributes.get("qualname")
        filename = source.attributes.get("filename")
        firstlineno = source.attributes.get("firstlineno")
        request_count = source.attributes.get("http_request_count")
        raw_scope = source.attributes.get("scope")
        raw_observation = edge.attributes.get("observation")
        expected_confidence = 1.0 if raw_observation == "exact" else 0.9
        if (
            target.id in callers
            or target.attributes.get("caller_event_id") != source.id
            or source.attributes.get("source") != "python-http-client-wrapper"
            or source.attributes.get("arguments_captured") is not False
            or source.attributes.get("locals_captured") is not False
            or not isinstance(module, str)
            or not module
            or not isinstance(qualname, str)
            or not qualname
            or not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(request_count, int)
            or isinstance(request_count, bool)
            or request_count <= 0
            or raw_scope not in {"application", "library", "runtime"}
            or raw_observation not in {"exact", "sampled"}
            or edge.attributes.get("source") != "python-http-client-wrapper"
            or not math.isclose(edge.confidence, expected_confidence)
            or source.name != f"{module}.{qualname}"
        ):
            invalid_targets.add(target.id)
            callers.pop(target.id, None)
            continue
        scope: Literal["application", "library", "runtime"]
        if raw_scope == "application":
            scope = "application"
        elif raw_scope == "library":
            scope = "library"
        else:
            scope = "runtime"
        observation: Literal["exact", "sampled"] = (
            "exact" if raw_observation == "exact" else "sampled"
        )
        callers[target.id] = HttpCaller(
            source.id,
            source.name,
            module,
            qualname,
            filename,
            firstlineno,
            scope,
            observation,
            edge.confidence,
        )
    targets_by_caller: dict[str, list[str]] = {}
    for target_id, caller in callers.items():
        targets_by_caller.setdefault(caller.event_id, []).append(target_id)
    for caller_event_id, target_ids in targets_by_caller.items():
        source = by_id[caller_event_id]
        if source.attributes.get("http_request_count") != len(target_ids):
            invalid_targets.update(target_ids)
    for target_id, event in requests.items():
        if event.attributes.get("caller_event_id") is not None and target_id not in callers:
            invalid_targets.add(target_id)
    for target_id in invalid_targets:
        callers.pop(target_id, None)
    return callers, len(invalid_targets)


def _http_requests(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[HttpRequest, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_request, invalid_caller_count = _http_callers(events, edges)
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
    raw_status = raw_http.get("status")
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    if raw_status == "complete":
        status = "complete"
    elif raw_status == "partial":
        status = "partial"
    elif raw_status == "truncated":
        status = "truncated"
    elif raw_status == "unavailable":
        status = "unavailable"
    else:
        status = "invalid"
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
    raw_caller = raw_http.get("caller_attribution")
    caller_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    caller_counts = {
        "caller_count": 0,
        "attributed_request_count": 0,
        "unattributed_request_count": request_event_count,
        "invalid_caller_count": 0,
        "callback_error_count": 0,
    }
    if raw_caller is None:
        caller_status = "unavailable"
    elif not isinstance(raw_caller, dict):
        caller_status = "invalid"
    else:
        raw_caller_status = raw_caller.get("status")
        if raw_caller_status == "complete":
            caller_status = "complete"
        elif raw_caller_status == "partial":
            caller_status = "partial"
        elif raw_caller_status == "truncated":
            caller_status = "truncated"
        elif raw_caller_status == "unavailable":
            caller_status = "unavailable"
        elif raw_caller_status == "invalid":
            caller_status = "invalid"
        else:
            caller_status = "invalid"
        parsed_counts = {key: _semantic_count(raw_caller.get(key)) for key in caller_counts}
        if all(value is not None for value in parsed_counts.values()):
            caller_counts = {key: value or 0 for key, value in parsed_counts.items()}
        else:
            caller_status = "invalid"
        if (
            raw_caller.get("arguments_captured") is not False
            or raw_caller.get("locals_captured") is not False
            or caller_counts["caller_count"] != caller_count
            or caller_counts["attributed_request_count"] != attributed_request_count
            or caller_counts["attributed_request_count"]
            + caller_counts["unattributed_request_count"]
            != request_event_count
            or caller_counts["invalid_caller_count"] < invalid_caller_count
            or invalid_caller_count
        ):
            caller_status = "invalid"
    return HttpCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_request_count or 0,
        raw_counts["dropped_request_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller_status,
        caller_counts["caller_count"],
        caller_counts["attributed_request_count"],
        caller_counts["unattributed_request_count"],
        caller_counts["invalid_caller_count"],
        caller_counts["callback_error_count"],
        adapters,
    )


def _network_callers(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[dict[str, NetworkCaller], int]:
    by_id = {event.id: event for event in events}
    connections = {event.id: event for event in events if event.kind == "network.connect"}
    callers: dict[str, NetworkCaller] = {}
    invalid_targets: set[str] = set()
    for target_id, event in connections.items():
        caller_event_id = event.attributes.get("caller_event_id")
        if caller_event_id is not None and (
            not isinstance(caller_event_id, str) or not caller_event_id
        ):
            invalid_targets.add(target_id)
    for edge in edges:
        source = by_id.get(edge.source_event_id)
        target = by_id.get(edge.target_event_id)
        if source is None or source.kind != "python.callsite":
            continue
        if edge.kind != "connects" or target is None or target.kind != "network.connect":
            if target is not None and target.kind == "network.connect":
                invalid_targets.add(target.id)
            continue
        module = source.attributes.get("module")
        qualname = source.attributes.get("qualname")
        filename = source.attributes.get("filename")
        firstlineno = source.attributes.get("firstlineno")
        connection_count = source.attributes.get("network_connection_count")
        raw_scope = source.attributes.get("scope")
        raw_observation = edge.attributes.get("observation")
        expected_confidence = 1.0 if raw_observation == "exact" else 0.9
        if (
            target.id in callers
            or target.attributes.get("caller_event_id") != source.id
            or source.attributes.get("source") != "python-network-connection-wrapper"
            or source.attributes.get("arguments_captured") is not False
            or source.attributes.get("locals_captured") is not False
            or not isinstance(module, str)
            or not module
            or not isinstance(qualname, str)
            or not qualname
            or not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(connection_count, int)
            or isinstance(connection_count, bool)
            or connection_count <= 0
            or raw_scope not in {"application", "library", "runtime"}
            or raw_observation not in {"exact", "sampled"}
            or edge.attributes.get("source") != "python-network-connection-wrapper"
            or not math.isclose(edge.confidence, expected_confidence)
            or source.name != f"{module}.{qualname}"
        ):
            invalid_targets.add(target.id)
            callers.pop(target.id, None)
            continue
        scope: Literal["application", "library", "runtime"]
        if raw_scope == "application":
            scope = "application"
        elif raw_scope == "library":
            scope = "library"
        else:
            scope = "runtime"
        observation: Literal["exact", "sampled"] = (
            "exact" if raw_observation == "exact" else "sampled"
        )
        callers[target.id] = NetworkCaller(
            source.id,
            source.name,
            module,
            qualname,
            filename,
            firstlineno,
            scope,
            observation,
            edge.confidence,
        )
    targets_by_caller: dict[str, list[str]] = {}
    for target_id, caller in callers.items():
        targets_by_caller.setdefault(caller.event_id, []).append(target_id)
    for caller_event_id, target_ids in targets_by_caller.items():
        source = by_id[caller_event_id]
        if source.attributes.get("network_connection_count") != len(target_ids):
            invalid_targets.update(target_ids)
    for target_id, event in connections.items():
        if event.attributes.get("caller_event_id") is not None and target_id not in callers:
            invalid_targets.add(target_id)
    for target_id in invalid_targets:
        callers.pop(target_id, None)
    return callers, len(invalid_targets)


def _network_connections(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[NetworkConnection, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_connection, invalid_caller_count = _network_callers(events, edges)
    connections: list[NetworkConnection] = []
    invalid_event_count = 0
    for event in events:
        if event.kind != "network.connect":
            continue
        adapter = event.attributes.get("adapter")
        raw_transport = event.attributes.get("transport")
        raw_family = event.attributes.get("address_family")
        server_port = event.attributes.get("server_port")
        tls_requested = event.attributes.get("tls_requested")
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        raw_outcome = event.attributes.get("outcome")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        if (
            event.started_at_ns is None
            or event.attributes.get("source") != "python-network-connection-wrapper"
            or not isinstance(adapter, str)
            or adapter not in NETWORK_CAPTURE_ADAPTERS
            or not isinstance(raw_transport, str)
            or raw_transport not in {"tcp", "unix"}
            or not isinstance(raw_family, str)
            or raw_family not in {"ipv4", "ipv6", "unix", "unknown"}
            or (raw_transport == "unix") != (raw_family == "unix")
            or event.name != f"{str(raw_transport).upper()} connect"
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
            or (raw_transport == "unix" and server_port is not None)
            or (tls_requested is not None and not isinstance(tls_requested, bool))
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or raw_outcome not in {"connected", "connect_error", "unknown"}
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
            or event.attributes.get("path_captured") is not False
            or event.attributes.get("credentials_captured") is not False
            or event.attributes.get("duration_boundary") != "connection_ready"
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
        transport: Literal["tcp", "unix"] = "tcp" if raw_transport == "tcp" else "unix"
        address_family: Literal["ipv4", "ipv6", "unix", "unknown"]
        if raw_family == "ipv4":
            address_family = "ipv4"
        elif raw_family == "ipv6":
            address_family = "ipv6"
        elif raw_family == "unix":
            address_family = "unix"
        else:
            address_family = "unknown"
        outcome: Literal["connected", "connect_error", "unknown"]
        if raw_outcome == "connected":
            outcome = "connected"
        elif raw_outcome == "connect_error":
            outcome = "connect_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "connected"
                and (event.finished_at_ns is None or error_type is not None or error is True)
            )
            or (
                outcome == "connect_error"
                and (event.finished_at_ns is None or error_type is None or error is not True)
            )
            or (
                outcome == "unknown"
                and (event.finished_at_ns is not None or error_type is not None or error is True)
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        connections.append(
            NetworkConnection(
                event.id,
                adapter,
                transport,
                address_family,
                server_port,
                tls_requested,
                parent_pid,
                role,
                outcome,
                error_type,
                event.started_at_ns,
                duration_seconds,
                caller_by_connection.get(event.id),
            )
        )
    connections.sort(
        key=lambda connection: (
            connection.started_at_ns,
            connection.parent_pid,
            connection.event_id,
        )
    )
    return (
        tuple(connections),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_connection.values()}),
        len(caller_by_connection),
        tuple(sorted({connection.adapter for connection in connections})),
    )


def _network_setup_callers(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[dict[str, NetworkCaller], int]:
    by_id = {event.id: event for event in events}
    phases = {
        event.id: event
        for event in events
        if event.kind in {"network.resolve", "network.tls_handshake"}
    }
    callers: dict[str, NetworkCaller] = {}
    invalid_targets: set[str] = set()
    for target_id, event in phases.items():
        caller_event_id = event.attributes.get("caller_event_id")
        if caller_event_id is not None and (
            not isinstance(caller_event_id, str) or not caller_event_id
        ):
            invalid_targets.add(target_id)
    for edge in edges:
        source = by_id.get(edge.source_event_id)
        target = by_id.get(edge.target_event_id)
        if source is None or source.kind != "python.callsite":
            continue
        if target is None or target.id not in phases:
            continue
        expected_edge = (
            "resolves"
            if target.kind == "network.resolve"
            else "handshakes"
            if target.kind == "network.tls_handshake"
            else None
        )
        if expected_edge is None or edge.kind != expected_edge:
            invalid_targets.add(target.id)
            continue
        module = source.attributes.get("module")
        qualname = source.attributes.get("qualname")
        filename = source.attributes.get("filename")
        firstlineno = source.attributes.get("firstlineno")
        phase_count = source.attributes.get("network_setup_phase_count")
        raw_scope = source.attributes.get("scope")
        raw_observation = edge.attributes.get("observation")
        expected_confidence = 1.0 if raw_observation == "exact" else 0.9
        if (
            target.id in callers
            or target.attributes.get("caller_event_id") != source.id
            or source.attributes.get("source") != "python-network-setup-wrapper"
            or source.attributes.get("arguments_captured") is not False
            or source.attributes.get("locals_captured") is not False
            or not isinstance(module, str)
            or not module
            or not isinstance(qualname, str)
            or not qualname
            or not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(phase_count, int)
            or isinstance(phase_count, bool)
            or phase_count <= 0
            or raw_scope not in {"application", "library", "runtime"}
            or raw_observation not in {"exact", "sampled"}
            or edge.attributes.get("source") != "python-network-setup-wrapper"
            or not math.isclose(edge.confidence, expected_confidence)
            or source.name != f"{module}.{qualname}"
        ):
            invalid_targets.add(target.id)
            callers.pop(target.id, None)
            continue
        scope: Literal["application", "library", "runtime"]
        if raw_scope == "application":
            scope = "application"
        elif raw_scope == "library":
            scope = "library"
        else:
            scope = "runtime"
        observation: Literal["exact", "sampled"] = (
            "exact" if raw_observation == "exact" else "sampled"
        )
        callers[target.id] = NetworkCaller(
            source.id,
            source.name,
            module,
            qualname,
            filename,
            firstlineno,
            scope,
            observation,
            edge.confidence,
        )
    targets_by_caller: dict[str, list[str]] = {}
    for target_id, caller in callers.items():
        targets_by_caller.setdefault(caller.event_id, []).append(target_id)
    for caller_event_id, target_ids in targets_by_caller.items():
        source = by_id[caller_event_id]
        if source.attributes.get("network_setup_phase_count") != len(target_ids):
            invalid_targets.update(target_ids)
    for target_id, event in phases.items():
        if event.attributes.get("caller_event_id") is not None and target_id not in callers:
            invalid_targets.add(target_id)
    for target_id in invalid_targets:
        callers.pop(target_id, None)
    return callers, len(invalid_targets)


def _network_setup_phases(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[NetworkSetupPhase, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_phase, invalid_caller_count = _network_setup_callers(events, edges)
    phases: list[NetworkSetupPhase] = []
    invalid_event_count = 0
    for event in events:
        if event.kind not in {"network.resolve", "network.tls_handshake"}:
            continue
        raw_phase = event.attributes.get("phase")
        adapter = event.attributes.get("adapter")
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        raw_outcome = event.attributes.get("outcome")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        expected_kind = (
            "network.resolve"
            if raw_phase == "dns"
            else "network.tls_handshake"
            if raw_phase == "tls"
            else None
        )
        expected_name = (
            "DNS resolution"
            if raw_phase == "dns"
            else "TLS handshake"
            if raw_phase == "tls"
            else None
        )
        if (
            event.started_at_ns is None
            or event.attributes.get("source") != "python-network-setup-wrapper"
            or expected_kind != event.kind
            or expected_name != event.name
            or not isinstance(adapter, str)
            or adapter not in NETWORK_SETUP_CAPTURE_ADAPTERS
            or (raw_phase == "dns") != (adapter == "stdlib.socket.getaddrinfo")
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or raw_outcome not in {"completed", "setup_error", "unknown"}
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
            or event.attributes.get("hostname_captured") is not False
            or event.attributes.get("server_address_captured") is not False
            or event.attributes.get("sni_captured") is not False
            or event.attributes.get("certificate_captured") is not False
            or event.attributes.get("credentials_captured") is not False
            or event.attributes.get("duration_boundary") != raw_phase
        ):
            invalid_event_count += 1
            continue
        phase: Literal["dns", "tls"] = "dns" if raw_phase == "dns" else "tls"
        role: Literal["root", "descendant", "unknown"]
        if raw_role == "root":
            role = "root"
        elif raw_role == "descendant":
            role = "descendant"
        else:
            role = "unknown"
        outcome: Literal["completed", "setup_error", "unknown"]
        if raw_outcome == "completed":
            outcome = "completed"
        elif raw_outcome == "setup_error":
            outcome = "setup_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "completed"
                and (event.finished_at_ns is None or error_type is not None or error is True)
            )
            or (
                outcome == "setup_error"
                and (event.finished_at_ns is None or error_type is None or error is not True)
            )
            or (
                outcome == "unknown"
                and (event.finished_at_ns is not None or error_type is not None or error is True)
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        phases.append(
            NetworkSetupPhase(
                event.id,
                phase,
                adapter,
                parent_pid,
                role,
                outcome,
                error_type,
                event.started_at_ns,
                duration_seconds,
                caller_by_phase.get(event.id),
            )
        )
    phases.sort(key=lambda phase: (phase.started_at_ns, phase.parent_pid, phase.event_id))
    return (
        tuple(phases),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_phase.values()}),
        len(caller_by_phase),
        tuple(sorted({phase.adapter for phase in phases})),
    )


def _network_setup_hotspots(
    phases: tuple[NetworkSetupPhase, ...],
) -> tuple[NetworkSetupHotspot, ...]:
    aggregates: dict[
        tuple[str | None, Literal["dns", "tls"], str], _NetworkSetupHotspotAggregate
    ] = {}
    for phase in phases:
        caller_event_id = phase.caller.event_id if phase.caller is not None else None
        key = (caller_event_id, phase.phase, phase.adapter)
        aggregate = aggregates.get(key)
        if aggregate is None:
            aggregate = _NetworkSetupHotspotAggregate(
                phase.phase,
                phase.adapter,
                phase.caller,
            )
            aggregates[key] = aggregate
        aggregate.phase_count += 1
        if phase.outcome == "completed":
            aggregate.completed_phase_count += 1
        elif phase.outcome == "setup_error":
            aggregate.failed_phase_count += 1
        else:
            aggregate.unfinished_phase_count += 1
        if phase.duration_seconds is not None:
            aggregate.total_duration_seconds += phase.duration_seconds
            aggregate.max_duration_seconds = max(
                aggregate.max_duration_seconds,
                phase.duration_seconds,
            )
    hotspots = tuple(
        NetworkSetupHotspot(
            aggregate.phase,
            aggregate.adapter,
            aggregate.phase_count,
            aggregate.completed_phase_count,
            aggregate.failed_phase_count,
            aggregate.unfinished_phase_count,
            aggregate.total_duration_seconds,
            aggregate.max_duration_seconds,
            aggregate.caller,
        )
        for aggregate in aggregates.values()
    )
    return tuple(
        sorted(
            hotspots,
            key=lambda hotspot: (
                -hotspot.failed_phase_count,
                -hotspot.phase_count,
                -hotspot.total_duration_seconds,
                hotspot.phase,
                hotspot.caller.name if hotspot.caller is not None else "",
                hotspot.adapter,
            ),
        )
    )


def _network_setup_bottlenecks(
    phases: tuple[NetworkSetupPhase, ...],
    capture: NetworkSetupCaptureSummary | None,
    total_seconds: float | None,
) -> tuple[Bottleneck, ...]:
    if (
        capture is None
        or capture.status in {"unavailable", "invalid"}
        or total_seconds is None
        or total_seconds <= 0
    ):
        return ()
    findings: list[Bottleneck] = []
    for phase_name, label in (("dns", "DNS resolution"), ("tls", "TLS handshake")):
        matching = tuple(phase for phase in phases if phase.phase == phase_name)
        failed_count = sum(phase.outcome == "setup_error" for phase in matching)
        if failed_count:
            findings.append(
                Bottleneck(
                    f"{phase_name}_failures",
                    (
                        f"{failed_count:,} of {len(matching):,} retained "
                        f"{label.lower()} attempts failed"
                    ),
                    0.9 if capture.status == "complete" else 0.7,
                )
            )
        completed = tuple(
            phase
            for phase in matching
            if phase.outcome == "completed" and phase.duration_seconds is not None
        )
        if not completed:
            continue
        slowest = max(completed, key=lambda phase: phase.duration_seconds or 0.0)
        duration_seconds = slowest.duration_seconds or 0.0
        ratio = duration_seconds / total_seconds
        if duration_seconds >= NETWORK_SETUP_MIN_SECONDS and ratio >= NETWORK_SETUP_MIN_RUN_RATIO:
            findings.append(
                Bottleneck(
                    f"{phase_name}_latency",
                    f"{label} took {duration_seconds:.3f}s ({ratio:.0%} of the run)",
                    0.8,
                )
            )
    return tuple(findings)


_LOGICAL_OPERATION_IDENTITIES = {
    ("cache", "batch"): "Cache batch",
    ("cache", "command"): "Cache command",
    ("database", "commit"): "Database commit",
    ("broker", "consume"): "Broker consume",
    ("database", "execute"): "Database execute",
    ("database", "executemany"): "Database executemany",
    ("database", "executescript"): "Database script",
    ("queue", "get"): "Queue get",
    ("broker", "publish"): "Broker publish",
    ("queue", "put"): "Queue put",
    ("database", "rollback"): "Database rollback",
    ("executor", "task"): "Executor task",
    ("scheduler", "task"): "Async task",
    ("server", "request"): "Inbound HTTP request",
}


def _logical_operation_callers(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[dict[str, NetworkCaller], int]:
    by_id = {event.id: event for event in events}
    operations = {
        event.id: event
        for event in events
        if event.attributes.get("source") == "python-logical-operation-wrapper"
        and event.kind != "python.callsite"
    }
    callers: dict[str, NetworkCaller] = {}
    invalid_targets: set[str] = set()
    for target_id, event in operations.items():
        caller_event_id = event.attributes.get("caller_event_id")
        if caller_event_id is not None and (
            not isinstance(caller_event_id, str) or not caller_event_id
        ):
            invalid_targets.add(target_id)
    for edge in edges:
        source = by_id.get(edge.source_event_id)
        target = by_id.get(edge.target_event_id)
        if source is None or source.kind != "python.callsite":
            continue
        if target is None or target.id not in operations:
            continue
        if edge.kind != "performs":
            invalid_targets.add(target.id)
            continue
        module = source.attributes.get("module")
        qualname = source.attributes.get("qualname")
        filename = source.attributes.get("filename")
        firstlineno = source.attributes.get("firstlineno")
        operation_count = source.attributes.get("logical_operation_count")
        raw_scope = source.attributes.get("scope")
        raw_observation = edge.attributes.get("observation")
        expected_confidence = 1.0 if raw_observation == "exact" else 0.9
        if (
            target.id in callers
            or target.attributes.get("caller_event_id") != source.id
            or source.attributes.get("source") != "python-logical-operation-wrapper"
            or source.attributes.get("arguments_captured") is not False
            or source.attributes.get("locals_captured") is not False
            or not isinstance(module, str)
            or not module
            or not isinstance(qualname, str)
            or not qualname
            or not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(operation_count, int)
            or isinstance(operation_count, bool)
            or operation_count <= 0
            or raw_scope not in {"application", "library", "runtime"}
            or raw_observation not in {"exact", "sampled"}
            or edge.attributes.get("source") != "python-logical-operation-wrapper"
            or not math.isclose(edge.confidence, expected_confidence)
            or source.name != f"{module}.{qualname}"
        ):
            invalid_targets.add(target.id)
            callers.pop(target.id, None)
            continue
        scope: Literal["application", "library", "runtime"]
        if raw_scope == "application":
            scope = "application"
        elif raw_scope == "library":
            scope = "library"
        else:
            scope = "runtime"
        observation: Literal["exact", "sampled"] = (
            "exact" if raw_observation == "exact" else "sampled"
        )
        callers[target.id] = NetworkCaller(
            source.id,
            source.name,
            module,
            qualname,
            filename,
            firstlineno,
            scope,
            observation,
            edge.confidence,
        )
    targets_by_caller: dict[str, list[str]] = {}
    for target_id, caller in callers.items():
        targets_by_caller.setdefault(caller.event_id, []).append(target_id)
    for caller_event_id, target_ids in targets_by_caller.items():
        source = by_id[caller_event_id]
        if source.attributes.get("logical_operation_count") != len(target_ids):
            invalid_targets.update(target_ids)
    for target_id, event in operations.items():
        if event.attributes.get("caller_event_id") is not None and target_id not in callers:
            invalid_targets.add(target_id)
    for target_id in invalid_targets:
        callers.pop(target_id, None)
    return callers, len(invalid_targets)


def _logical_operations(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[LogicalOperation, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_operation, invalid_caller_count = _logical_operation_callers(events, edges)
    operations: list[LogicalOperation] = []
    invalid_event_count = 0
    for event in events:
        if event.attributes.get("source") != "python-logical-operation-wrapper" or event.kind == (
            "python.callsite"
        ):
            continue
        raw_category = event.attributes.get("category")
        raw_operation = event.attributes.get("operation")
        adapter = event.attributes.get("adapter")
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        raw_outcome = event.attributes.get("outcome")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        status_code = event.attributes.get("status_code")
        expected_kind = (
            f"{raw_category}.{raw_operation}"
            if isinstance(raw_category, str) and isinstance(raw_operation, str)
            else None
        )
        expected_name = (
            _LOGICAL_OPERATION_IDENTITIES.get((raw_category, raw_operation))
            if isinstance(raw_category, str) and isinstance(raw_operation, str)
            else None
        )
        if (
            event.started_at_ns is None
            or expected_kind != event.kind
            or expected_name != event.name
            or not isinstance(adapter, str)
            or adapter not in LOGICAL_OPERATION_CAPTURE_ADAPTERS
            or LOGICAL_OPERATION_CAPTURE_ADAPTER_CATEGORIES.get(adapter) != raw_category
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or raw_outcome not in {"completed", "operation_error", "unknown"}
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
            or event.attributes.get("statement_captured") is not False
            or event.attributes.get("parameters_captured") is not False
            or event.attributes.get("payload_captured") is not False
            or event.attributes.get("queue_item_captured") is not False
            or event.attributes.get("queue_identity_captured") is not False
            or event.attributes.get("return_value_captured") is not False
            or event.attributes.get("duration_boundary")
            != (
                "submission_to_completion"
                if raw_category == "executor"
                else "creation_to_completion"
                if raw_category == "scheduler"
                else "request_to_response_completion"
                if raw_category == "server"
                else "logical_operation"
            )
            or (
                raw_category == "executor"
                and (
                    event.attributes.get("callable_captured") is not False
                    or event.attributes.get("arguments_captured") is not False
                )
            )
            or (
                raw_category == "scheduler"
                and (
                    event.attributes.get("callable_captured") is not False
                    or event.attributes.get("awaitable_captured") is not False
                    or event.attributes.get("task_name_captured") is not False
                    or event.attributes.get("context_captured") is not False
                    or event.attributes.get("arguments_captured") is not False
                )
            )
            or (
                raw_category == "server"
                and (
                    event.attributes.get("http_method_captured") is not False
                    or event.attributes.get("route_captured") is not False
                    or event.attributes.get("url_captured") is not False
                    or event.attributes.get("headers_captured") is not False
                    or event.attributes.get("body_captured") is not False
                    or event.attributes.get("response_body_captured") is not False
                    or event.attributes.get("client_address_captured") is not False
                    or (
                        status_code is not None
                        and (
                            not isinstance(status_code, int)
                            or isinstance(status_code, bool)
                            or not 100 <= status_code <= 999
                        )
                    )
                    or (status_code is None and raw_outcome != "completed")
                    or (
                        isinstance(status_code, int)
                        and not isinstance(status_code, bool)
                        and status_code >= 500
                        and (raw_outcome != "operation_error" or error_type != "HTTPStatusError")
                    )
                    or (
                        isinstance(status_code, int)
                        and not isinstance(status_code, bool)
                        and status_code < 500
                        and raw_outcome != "completed"
                    )
                )
            )
            or (raw_category != "server" and status_code is not None)
        ):
            invalid_event_count += 1
            continue
        category_values: dict[str, LogicalOperationCategory] = {
            "broker": "broker",
            "cache": "cache",
            "database": "database",
            "executor": "executor",
            "queue": "queue",
            "scheduler": "scheduler",
            "server": "server",
        }
        if not isinstance(raw_category, str) or raw_category not in category_values:
            invalid_event_count += 1
            continue
        category = category_values[raw_category]
        operation_values: dict[str, LogicalOperationName] = {
            "batch": "batch",
            "command": "command",
            "commit": "commit",
            "consume": "consume",
            "execute": "execute",
            "executemany": "executemany",
            "executescript": "executescript",
            "get": "get",
            "publish": "publish",
            "put": "put",
            "rollback": "rollback",
            "request": "request",
            "task": "task",
        }
        if not isinstance(raw_operation, str) or raw_operation not in operation_values:
            invalid_event_count += 1
            continue
        operation = operation_values[raw_operation]
        role: Literal["root", "descendant", "unknown"]
        if raw_role == "root":
            role = "root"
        elif raw_role == "descendant":
            role = "descendant"
        else:
            role = "unknown"
        outcome: Literal["completed", "operation_error", "unknown"]
        if raw_outcome == "completed":
            outcome = "completed"
        elif raw_outcome == "operation_error":
            outcome = "operation_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "completed"
                and (event.finished_at_ns is None or error_type is not None or error is True)
            )
            or (
                outcome == "operation_error"
                and (event.finished_at_ns is None or error_type is None or error is not True)
            )
            or (
                outcome == "unknown"
                and (event.finished_at_ns is not None or error_type is not None or error is True)
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        operations.append(
            LogicalOperation(
                event.id,
                category,
                operation,
                adapter,
                parent_pid,
                role,
                outcome,
                error_type,
                status_code if isinstance(status_code, int) else None,
                event.started_at_ns,
                duration_seconds,
                caller_by_operation.get(event.id),
            )
        )
    operations.sort(
        key=lambda operation: (
            operation.started_at_ns,
            operation.parent_pid,
            operation.event_id,
        )
    )
    return (
        tuple(operations),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_operation.values()}),
        len(caller_by_operation),
        tuple(sorted({operation.adapter for operation in operations})),
    )


def _logical_operation_hotspots(
    operations: tuple[LogicalOperation, ...],
) -> tuple[LogicalOperationHotspot, ...]:
    aggregates: dict[tuple[str | None, str, str, str], _LogicalOperationHotspotAggregate] = {}
    for operation in operations:
        caller_event_id = operation.caller.event_id if operation.caller is not None else None
        key = (caller_event_id, operation.category, operation.operation, operation.adapter)
        aggregate = aggregates.get(key)
        if aggregate is None:
            aggregate = _LogicalOperationHotspotAggregate(
                operation.category,
                operation.operation,
                operation.adapter,
                operation.caller,
            )
            aggregates[key] = aggregate
        aggregate.operation_count += 1
        if operation.outcome == "completed":
            aggregate.completed_operation_count += 1
        elif operation.outcome == "operation_error":
            aggregate.failed_operation_count += 1
        else:
            aggregate.unfinished_operation_count += 1
        if operation.duration_seconds is not None:
            aggregate.total_duration_seconds += operation.duration_seconds
            aggregate.max_duration_seconds = max(
                aggregate.max_duration_seconds,
                operation.duration_seconds,
            )
    hotspots = tuple(
        LogicalOperationHotspot(
            aggregate.category,
            aggregate.operation,
            aggregate.adapter,
            aggregate.operation_count,
            aggregate.completed_operation_count,
            aggregate.failed_operation_count,
            aggregate.unfinished_operation_count,
            aggregate.total_duration_seconds,
            aggregate.max_duration_seconds,
            aggregate.caller,
        )
        for aggregate in aggregates.values()
    )
    return tuple(
        sorted(
            hotspots,
            key=lambda hotspot: (
                -hotspot.failed_operation_count,
                -hotspot.operation_count,
                -hotspot.total_duration_seconds,
                hotspot.category,
                hotspot.operation,
                hotspot.caller.name if hotspot.caller is not None else "",
                hotspot.adapter,
            ),
        )
    )


def _logical_operation_bottlenecks(
    operations: tuple[LogicalOperation, ...],
    capture: LogicalOperationCaptureSummary | None,
    total_seconds: float | None,
) -> tuple[Bottleneck, ...]:
    if (
        capture is None
        or capture.status in {"unavailable", "invalid"}
        or total_seconds is None
        or total_seconds <= 0
    ):
        return ()
    findings: list[Bottleneck] = []
    for category, label in (
        ("database", "Database"),
        ("cache", "Cache"),
        ("queue", "Queue"),
        ("broker", "Broker"),
        ("executor", "Executor"),
        ("scheduler", "Scheduler"),
        ("server", "Server"),
    ):
        matching = tuple(operation for operation in operations if operation.category == category)
        failed_count = sum(operation.outcome == "operation_error" for operation in matching)
        if failed_count:
            findings.append(
                Bottleneck(
                    f"{category}_operation_failures",
                    (
                        f"{failed_count:,} of {len(matching):,} retained "
                        f"{label.lower()} operations failed"
                    ),
                    0.9 if capture.status == "complete" else 0.7,
                )
            )
        completed = tuple(
            operation
            for operation in matching
            if operation.outcome == "completed" and operation.duration_seconds is not None
        )
        if not completed:
            continue
        slowest = max(completed, key=lambda operation: operation.duration_seconds or 0.0)
        duration_seconds = slowest.duration_seconds or 0.0
        ratio = duration_seconds / total_seconds
        if (
            duration_seconds >= LOGICAL_OPERATION_MIN_SECONDS
            and ratio >= LOGICAL_OPERATION_MIN_RUN_RATIO
        ):
            findings.append(
                Bottleneck(
                    f"{category}_operation_latency",
                    (
                        f"{label} {slowest.operation} took {duration_seconds:.3f}s "
                        f"({ratio:.0%} of the run)"
                    ),
                    0.8,
                )
            )
    return tuple(findings)


def _network_bottlenecks(
    connections: tuple[NetworkConnection, ...],
    capture: NetworkCaptureSummary | None,
    total_seconds: float | None,
) -> tuple[Bottleneck, ...]:
    if (
        capture is None
        or capture.status in {"unavailable", "invalid"}
        or total_seconds is None
        or total_seconds <= 0
    ):
        return ()
    findings: list[Bottleneck] = []
    failed_count = sum(connection.outcome == "connect_error" for connection in connections)
    if failed_count:
        attempt_label = "attempt" if capture.connection_count == 1 else "attempts"
        findings.append(
            Bottleneck(
                "connection_failures",
                (
                    f"{failed_count:,} of {capture.connection_count:,} retained outbound "
                    f"connection {attempt_label} failed"
                ),
                0.9 if capture.status == "complete" else 0.7,
            )
        )
    completed_connections = tuple(
        connection
        for connection in connections
        if connection.outcome == "connected" and connection.duration_seconds is not None
    )
    if completed_connections:
        slowest = max(
            completed_connections,
            key=lambda connection: connection.duration_seconds or 0.0,
        )
        duration_seconds = slowest.duration_seconds or 0.0
        ratio = duration_seconds / total_seconds
        if duration_seconds >= NETWORK_SETUP_MIN_SECONDS and ratio >= NETWORK_SETUP_MIN_RUN_RATIO:
            target = (
                f"{slowest.transport}://<redacted>:{slowest.server_port}"
                if slowest.server_port is not None
                else f"{slowest.transport}://<redacted>"
            )
            findings.append(
                Bottleneck(
                    "connection_setup",
                    (
                        f"{target} took {duration_seconds:.3f}s to become ready "
                        f"({ratio:.0%} of the run)"
                    ),
                    0.8,
                )
            )
    connection_rate = capture.connection_count / total_seconds
    if (
        capture.connection_count >= NETWORK_CHURN_MIN_CONNECTIONS
        and connection_rate >= NETWORK_CHURN_MIN_RATE_PER_SECOND
    ):
        qualifier = "at least " if capture.status != "complete" else ""
        findings.append(
            Bottleneck(
                "connection_churn",
                (
                    f"{qualifier}{capture.connection_count:,} outbound connection attempts "
                    f"occurred ({connection_rate:,.1f}/s); inspect pooling or retry behavior"
                ),
                0.65 if capture.status == "complete" else 0.5,
            )
        )
    return tuple(findings)


def _network_connection_hotspots(
    connections: tuple[NetworkConnection, ...],
) -> tuple[NetworkConnectionHotspot, ...]:
    aggregates: dict[tuple[str | None, str], _NetworkConnectionHotspotAggregate] = {}
    for connection in connections:
        caller_event_id = connection.caller.event_id if connection.caller is not None else None
        key = (caller_event_id, connection.adapter)
        aggregate = aggregates.get(key)
        if aggregate is None:
            aggregate = _NetworkConnectionHotspotAggregate(
                connection.adapter,
                connection.caller,
            )
            aggregates[key] = aggregate
        aggregate.connection_count += 1
        if connection.outcome == "connected":
            aggregate.connected_connection_count += 1
        elif connection.outcome == "connect_error":
            aggregate.failed_connection_count += 1
        else:
            aggregate.unfinished_connection_count += 1
        if connection.duration_seconds is not None:
            aggregate.total_duration_seconds += connection.duration_seconds
            aggregate.max_duration_seconds = max(
                aggregate.max_duration_seconds,
                connection.duration_seconds,
            )
    hotspots = tuple(
        NetworkConnectionHotspot(
            aggregate.adapter,
            aggregate.connection_count,
            aggregate.connected_connection_count,
            aggregate.failed_connection_count,
            aggregate.unfinished_connection_count,
            aggregate.total_duration_seconds,
            aggregate.max_duration_seconds,
            aggregate.caller,
        )
        for aggregate in aggregates.values()
    )
    return tuple(
        sorted(
            hotspots,
            key=lambda hotspot: (
                -hotspot.failed_connection_count,
                -hotspot.connection_count,
                -hotspot.total_duration_seconds,
                hotspot.caller.name if hotspot.caller is not None else "",
                hotspot.adapter,
            ),
        )
    )


def _network_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    connection_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_connection_count: int,
    invalid_caller_count: int,
    observed_adapters: tuple[str, ...],
    connection_hotspot_count: int,
) -> NetworkCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    raw_network = instrumentation.get("network_capture")
    if not isinstance(raw_network, dict):
        return None
    raw_status = raw_network.get("status")
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    if raw_status == "complete":
        status = "complete"
    elif raw_status == "partial":
        status = "partial"
    elif raw_status == "truncated":
        status = "truncated"
    elif raw_status == "unavailable":
        status = "unavailable"
    else:
        status = "invalid"
    raw_counts = {
        key: _semantic_count(raw_network.get(key))
        for key in (
            "process_count",
            "connection_count",
            "dropped_connection_count",
            "callback_error_count",
        )
    }
    raw_adapters = raw_network.get("adapters", ["stdlib.socket.connect"])
    adapters: tuple[str, ...] = ()
    adapters_invalid = not isinstance(raw_adapters, list)
    if isinstance(raw_adapters, list):
        adapter_values = [adapter for adapter in raw_adapters if isinstance(adapter, str)]
        if len(raw_adapters) > len(NETWORK_CAPTURE_ADAPTERS) or len(adapter_values) != len(
            raw_adapters
        ):
            adapters_invalid = True
        else:
            adapters = tuple(sorted(adapter_values))
            adapters_invalid = (
                len(adapters) != len(set(adapters))
                or "stdlib.socket.connect" not in adapters
                or any(adapter not in NETWORK_CAPTURE_ADAPTERS for adapter in adapters)
                or any(adapter not in adapters for adapter in observed_adapters)
            )
    declared_connection_count = raw_counts["connection_count"]
    if (
        raw_network.get("observer") != "python-network-connection-wrapper"
        or raw_network.get("zero_code") is not True
        or raw_network.get("server_identity_policy") != "redact"
        or raw_network.get("server_address_captured") is not False
        or raw_network.get("path_captured") is not False
        or raw_network.get("credentials_captured") is not False
        or any(value is None for value in raw_counts.values())
        or adapters_invalid
        or declared_connection_count != connection_event_count
        or invalid_event_count
    ):
        status = "invalid"
    raw_caller = raw_network.get("caller_attribution")
    caller_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    caller_counts = {
        "caller_count": 0,
        "attributed_connection_count": 0,
        "unattributed_connection_count": connection_event_count,
        "invalid_caller_count": 0,
        "callback_error_count": 0,
    }
    if raw_caller is None:
        caller_status = "unavailable"
    elif not isinstance(raw_caller, dict):
        caller_status = "invalid"
    else:
        raw_caller_status = raw_caller.get("status")
        if raw_caller_status == "complete":
            caller_status = "complete"
        elif raw_caller_status == "partial":
            caller_status = "partial"
        elif raw_caller_status == "truncated":
            caller_status = "truncated"
        elif raw_caller_status == "unavailable":
            caller_status = "unavailable"
        elif raw_caller_status == "invalid":
            caller_status = "invalid"
        else:
            caller_status = "invalid"
        parsed_counts = {key: _semantic_count(raw_caller.get(key)) for key in caller_counts}
        if all(value is not None for value in parsed_counts.values()):
            caller_counts = {key: value or 0 for key, value in parsed_counts.items()}
        else:
            caller_status = "invalid"
        if (
            raw_caller.get("arguments_captured") is not False
            or raw_caller.get("locals_captured") is not False
            or caller_counts["caller_count"] != caller_count
            or caller_counts["attributed_connection_count"] != attributed_connection_count
            or caller_counts["attributed_connection_count"]
            + caller_counts["unattributed_connection_count"]
            != connection_event_count
            or caller_counts["invalid_caller_count"] < invalid_caller_count
            or invalid_caller_count
        ):
            caller_status = "invalid"
    return NetworkCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_connection_count or 0,
        raw_counts["dropped_connection_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller_status,
        caller_counts["caller_count"],
        caller_counts["attributed_connection_count"],
        caller_counts["unattributed_connection_count"],
        caller_counts["invalid_caller_count"],
        caller_counts["callback_error_count"],
        adapters,
        connection_hotspot_count,
    )


def _network_setup_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    phase_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_phase_count: int,
    invalid_caller_count: int,
    observed_adapters: tuple[str, ...],
    hotspot_count: int,
) -> NetworkSetupCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    raw_setup = instrumentation.get("network_setup_capture")
    if not isinstance(raw_setup, dict):
        return None
    raw_status = raw_setup.get("status")
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    if raw_status == "complete":
        status = "complete"
    elif raw_status == "partial":
        status = "partial"
    elif raw_status == "truncated":
        status = "truncated"
    elif raw_status == "unavailable":
        status = "unavailable"
    else:
        status = "invalid"
    raw_counts = {
        key: _semantic_count(raw_setup.get(key))
        for key in (
            "process_count",
            "phase_count",
            "dropped_phase_count",
            "callback_error_count",
        )
    }
    raw_adapters = raw_setup.get("adapters")
    adapters: tuple[str, ...] = ()
    adapters_invalid = not isinstance(raw_adapters, list)
    if isinstance(raw_adapters, list):
        adapter_values = [adapter for adapter in raw_adapters if isinstance(adapter, str)]
        if len(raw_adapters) > len(NETWORK_SETUP_CAPTURE_ADAPTERS) or len(adapter_values) != len(
            raw_adapters
        ):
            adapters_invalid = True
        else:
            adapters = tuple(sorted(adapter_values))
            adapters_invalid = (
                len(adapters) != len(set(adapters))
                or "stdlib.socket.getaddrinfo" not in adapters
                or any(adapter not in NETWORK_SETUP_CAPTURE_ADAPTERS for adapter in adapters)
                or any(adapter not in adapters for adapter in observed_adapters)
            )
    declared_phase_count = raw_counts["phase_count"]
    if (
        raw_setup.get("observer") != "python-network-setup-wrapper"
        or raw_setup.get("zero_code") is not True
        or raw_setup.get("hostname_captured") is not False
        or raw_setup.get("server_address_captured") is not False
        or raw_setup.get("sni_captured") is not False
        or raw_setup.get("certificate_captured") is not False
        or raw_setup.get("credentials_captured") is not False
        or any(value is None for value in raw_counts.values())
        or adapters_invalid
        or declared_phase_count != phase_event_count
        or invalid_event_count
    ):
        status = "invalid"
    raw_caller = raw_setup.get("caller_attribution")
    caller_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    caller_counts = {
        "caller_count": 0,
        "attributed_phase_count": 0,
        "unattributed_phase_count": phase_event_count,
        "invalid_caller_count": 0,
        "callback_error_count": 0,
    }
    if raw_caller is None:
        caller_status = "unavailable"
    elif not isinstance(raw_caller, dict):
        caller_status = "invalid"
    else:
        raw_caller_status = raw_caller.get("status")
        if raw_caller_status == "complete":
            caller_status = "complete"
        elif raw_caller_status == "partial":
            caller_status = "partial"
        elif raw_caller_status == "truncated":
            caller_status = "truncated"
        elif raw_caller_status == "unavailable":
            caller_status = "unavailable"
        elif raw_caller_status == "invalid":
            caller_status = "invalid"
        else:
            caller_status = "invalid"
        parsed_counts = {key: _semantic_count(raw_caller.get(key)) for key in caller_counts}
        if all(value is not None for value in parsed_counts.values()):
            caller_counts = {key: value or 0 for key, value in parsed_counts.items()}
        else:
            caller_status = "invalid"
        if (
            raw_caller.get("arguments_captured") is not False
            or raw_caller.get("locals_captured") is not False
            or caller_counts["caller_count"] != caller_count
            or caller_counts["attributed_phase_count"] != attributed_phase_count
            or caller_counts["attributed_phase_count"] + caller_counts["unattributed_phase_count"]
            != phase_event_count
            or caller_counts["invalid_caller_count"] < invalid_caller_count
            or invalid_caller_count
        ):
            caller_status = "invalid"
    return NetworkSetupCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_phase_count or 0,
        raw_counts["dropped_phase_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller_status,
        caller_counts["caller_count"],
        caller_counts["attributed_phase_count"],
        caller_counts["unattributed_phase_count"],
        caller_counts["invalid_caller_count"],
        caller_counts["callback_error_count"],
        adapters,
        hotspot_count,
    )


def _logical_operation_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    operation_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_operation_count: int,
    invalid_caller_count: int,
    observed_adapters: tuple[str, ...],
    hotspot_count: int,
) -> LogicalOperationCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    raw_operation = instrumentation.get("logical_operation_capture")
    if not isinstance(raw_operation, dict):
        return None
    raw_status = raw_operation.get("status")
    status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    if raw_status == "complete":
        status = "complete"
    elif raw_status == "partial":
        status = "partial"
    elif raw_status == "truncated":
        status = "truncated"
    elif raw_status == "unavailable":
        status = "unavailable"
    else:
        status = "invalid"
    raw_counts = {
        key: _semantic_count(raw_operation.get(key))
        for key in (
            "process_count",
            "operation_count",
            "dropped_operation_count",
            "callback_error_count",
        )
    }
    raw_adapters = raw_operation.get("adapters")
    adapters: tuple[str, ...] = ()
    adapters_invalid = not isinstance(raw_adapters, list)
    if isinstance(raw_adapters, list):
        adapter_values = [adapter for adapter in raw_adapters if isinstance(adapter, str)]
        if len(raw_adapters) > len(LOGICAL_OPERATION_CAPTURE_ADAPTERS) or len(
            adapter_values
        ) != len(raw_adapters):
            adapters_invalid = True
        else:
            adapters = tuple(sorted(adapter_values))
            adapters_invalid = (
                len(adapters) != len(set(adapters))
                or any(adapter not in LOGICAL_OPERATION_CAPTURE_ADAPTERS for adapter in adapters)
                or any(adapter not in adapters for adapter in observed_adapters)
            )
    declared_operation_count = raw_counts["operation_count"]
    if (
        raw_operation.get("observer") != "python-logical-operation-wrapper"
        or raw_operation.get("zero_code") is not True
        or raw_operation.get("deep_only") is not True
        or raw_operation.get("statement_captured") is not False
        or raw_operation.get("parameters_captured") is not False
        or raw_operation.get("payload_captured") is not False
        or raw_operation.get("queue_item_captured") is not False
        or raw_operation.get("queue_identity_captured") is not False
        or raw_operation.get("callable_captured", False) is not False
        or raw_operation.get("awaitable_captured", False) is not False
        or raw_operation.get("task_name_captured", False) is not False
        or raw_operation.get("context_captured", False) is not False
        or raw_operation.get("arguments_captured", False) is not False
        or raw_operation.get("return_value_captured") is not False
        or raw_operation.get("exception_messages_captured", False) is not False
        or raw_operation.get("http_method_captured", False) is not False
        or raw_operation.get("route_captured", False) is not False
        or raw_operation.get("url_captured", False) is not False
        or raw_operation.get("headers_captured", False) is not False
        or raw_operation.get("body_captured", False) is not False
        or raw_operation.get("response_body_captured", False) is not False
        or raw_operation.get("client_address_captured", False) is not False
        or any(value is None for value in raw_counts.values())
        or adapters_invalid
        or declared_operation_count != operation_event_count
        or invalid_event_count
        or (
            status == "unavailable"
            and (
                any((value or 0) != 0 for value in raw_counts.values())
                or bool(adapters)
                or operation_event_count
            )
        )
    ):
        status = "invalid"
    raw_caller = raw_operation.get("caller_attribution")
    caller_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"]
    caller_counts = {
        "caller_count": 0,
        "attributed_operation_count": 0,
        "unattributed_operation_count": operation_event_count,
        "invalid_caller_count": 0,
        "callback_error_count": 0,
    }
    if raw_caller is None:
        caller_status = "unavailable"
    elif not isinstance(raw_caller, dict):
        caller_status = "invalid"
    else:
        raw_caller_status = raw_caller.get("status")
        if raw_caller_status == "complete":
            caller_status = "complete"
        elif raw_caller_status == "partial":
            caller_status = "partial"
        elif raw_caller_status == "truncated":
            caller_status = "truncated"
        elif raw_caller_status == "unavailable":
            caller_status = "unavailable"
        elif raw_caller_status == "invalid":
            caller_status = "invalid"
        else:
            caller_status = "invalid"
        parsed_counts = {key: _semantic_count(raw_caller.get(key)) for key in caller_counts}
        if all(value is not None for value in parsed_counts.values()):
            caller_counts = {key: value or 0 for key, value in parsed_counts.items()}
        else:
            caller_status = "invalid"
        if (
            raw_caller.get("arguments_captured") is not False
            or raw_caller.get("locals_captured") is not False
            or caller_counts["caller_count"] != caller_count
            or caller_counts["attributed_operation_count"] != attributed_operation_count
            or caller_counts["attributed_operation_count"]
            + caller_counts["unattributed_operation_count"]
            != operation_event_count
            or caller_counts["invalid_caller_count"] < invalid_caller_count
            or invalid_caller_count
        ):
            caller_status = "invalid"
    return LogicalOperationCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_operation_count or 0,
        raw_counts["dropped_operation_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller_status,
        caller_counts["caller_count"],
        caller_counts["attributed_operation_count"],
        caller_counts["unattributed_operation_count"],
        caller_counts["invalid_caller_count"],
        caller_counts["callback_error_count"],
        adapters,
        hotspot_count,
    )


def _process_resource_evidence(
    reader: RunpackReader,
) -> tuple[tuple[ProcessResourceHotspot, ...], dict[int, _ObservedProcessIdentity]]:
    entities = {entity.id: entity for entity in reader.entities()}
    aggregates: dict[str, _ProcessResourceAggregate] = {}
    for measurement in reader.measurements():
        if measurement.name not in {"process.memory.rss", "process.cpu.total"}:
            continue
        if measurement.attributes.get("source") != "process-observer":
            continue
        entity_id = measurement.entity_id
        pid = measurement.attributes.get("pid")
        process_name = measurement.attributes.get("process_name")
        if (
            entity_id is None
            or not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not isinstance(process_name, str)
            or not process_name
        ):
            continue
        aggregate = aggregates.get(entity_id)
        if aggregate is None:
            entity = entities.get(entity_id)
            parent = (
                entities.get(entity.parent_entity_id)
                if entity is not None and entity.parent_entity_id is not None
                else None
            )
            aggregate = _ProcessResourceAggregate(
                process_name,
                pid,
                parent.name if parent is not None else None,
            )
            aggregates[entity_id] = aggregate
        if measurement.name == "process.memory.rss":
            aggregate.sample_count += 1
            aggregate.peak_rss_bytes = max(aggregate.peak_rss_bytes, measurement.value)
        else:
            aggregate.cpu_seconds = max(aggregate.cpu_seconds, measurement.value)
    hotspots = tuple(
        ProcessResourceHotspot(
            aggregate.name,
            aggregate.pid,
            aggregate.parent_name,
            aggregate.sample_count,
            aggregate.peak_rss_bytes,
            aggregate.cpu_seconds,
        )
        for aggregate in aggregates.values()
    )
    identities: dict[int, _ObservedProcessIdentity] = {}
    ambiguous_pids: set[int] = set()
    for hotspot in hotspots:
        if hotspot.pid in identities:
            identities.pop(hotspot.pid)
            ambiguous_pids.add(hotspot.pid)
        elif hotspot.pid not in ambiguous_pids:
            identities[hotspot.pid] = _ObservedProcessIdentity(
                hotspot.name,
                hotspot.parent_name,
            )
    return (
        tuple(
            sorted(
                hotspots,
                key=lambda item: (-item.cpu_seconds, -item.peak_rss_bytes, item.name, item.pid),
            )[:100]
        ),
        identities,
    )


def _event_number(event: Event, key: str) -> float | None:
    return _nonnegative_number(event.attributes.get(key))


def _nonnegative_number(value: JsonValue | None) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    result = float(value)
    return result if math.isfinite(result) and result >= 0 else None


def _process_identity(
    pid: int,
    role: Literal["root", "descendant", "unknown"],
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[str | None, str | None, bool]:
    identity = observed_processes.get(pid)
    if identity is not None:
        return identity.name, identity.parent_name, True
    return (root_process_name if role == "root" else None), None, False


def _process_role(value: JsonValue | None) -> Literal["root", "descendant", "unknown"] | None:
    if value == "root":
        return "root"
    if value == "descendant":
        return "descendant"
    if value == "unknown":
        return "unknown"
    return None


def _deep_process_contributions(
    event: Event,
    *,
    implementation: Literal["python", "native"],
    call_count: int,
    total_seconds: float,
    self_seconds: float,
    max_seconds: float,
    exception_count: int,
    non_control_flow_exception_count: int | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[
    Literal["complete", "unavailable", "invalid"],
    tuple[PythonCallProcessContribution, ...],
]:
    raw_processes = event.attributes.get("processes")
    if raw_processes is None:
        return "unavailable", ()
    if (
        not isinstance(raw_processes, list)
        or not raw_processes
        or len(raw_processes) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return "invalid", ()
    raw_process_count = event.attributes.get("process_count")
    if (
        not isinstance(raw_process_count, int)
        or isinstance(raw_process_count, bool)
        or raw_process_count != len(raw_processes)
    ):
        return "invalid", ()
    contributions: list[PythonCallProcessContribution] = []
    seen_pids: set[int] = set()
    root_count = 0
    for raw_process in raw_processes:
        if not isinstance(raw_process, dict):
            return "invalid", ()
        pid = raw_process.get("pid")
        role = _process_role(raw_process.get("role"))
        process_call_count = raw_process.get("call_count")
        process_total = _nonnegative_number(raw_process.get("total_seconds"))
        process_self = _nonnegative_number(raw_process.get("self_seconds"))
        process_max = _nonnegative_number(raw_process.get("max_seconds"))
        process_exception_count = raw_process.get("exception_count", 0)
        process_non_control_flow_exception_count = raw_process.get(
            "non_control_flow_exception_count"
        )
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or pid in seen_pids
            or role is None
            or not isinstance(process_call_count, int)
            or isinstance(process_call_count, bool)
            or process_call_count < 0
            or not isinstance(process_exception_count, int)
            or isinstance(process_exception_count, bool)
            or process_exception_count < 0
            or (implementation == "native" and process_exception_count > process_call_count)
            or (
                non_control_flow_exception_count is None
                and process_non_control_flow_exception_count is not None
            )
            or (
                non_control_flow_exception_count is not None
                and (
                    not isinstance(process_non_control_flow_exception_count, int)
                    or isinstance(process_non_control_flow_exception_count, bool)
                    or not 0 <= process_non_control_flow_exception_count <= process_exception_count
                )
            )
            or process_total is None
            or process_self is None
            or process_max is None
            or process_self > process_total
            or process_max > process_total
        ):
            return "invalid", ()
        seen_pids.add(pid)
        root_count += role == "root"
        if root_count > 1:
            return "invalid", ()
        process_name, parent_name, observed = _process_identity(
            pid,
            role,
            observed_processes,
            root_process_name,
        )
        contributions.append(
            PythonCallProcessContribution(
                pid,
                role,
                process_name,
                parent_name,
                observed,
                process_call_count,
                process_total,
                process_self,
                process_max,
                process_exception_count,
                (
                    process_non_control_flow_exception_count
                    if isinstance(process_non_control_flow_exception_count, int)
                    and not isinstance(process_non_control_flow_exception_count, bool)
                    else None
                ),
            )
        )
    if (
        sum(item.call_count for item in contributions) != call_count
        or sum(item.exception_count for item in contributions) != exception_count
        or (
            non_control_flow_exception_count is not None
            and sum(item.non_control_flow_exception_count or 0 for item in contributions)
            != non_control_flow_exception_count
        )
        or not math.isclose(
            sum(item.total_seconds for item in contributions),
            total_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
        or not math.isclose(
            sum(item.self_seconds for item in contributions),
            self_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
        or not math.isclose(
            max(item.max_seconds for item in contributions),
            max_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
    ):
        return "invalid", ()
    contributions.sort(key=lambda item: (-item.self_seconds, -item.total_seconds, item.pid))
    return "complete", tuple(contributions)


def _sample_process_contributions(
    event: Event,
    *,
    sample_count: int,
    leaf_sample_count: int,
    estimated_total_seconds: float,
    estimated_leaf_seconds: float,
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[
    Literal["complete", "unavailable", "invalid"],
    tuple[PythonSampleProcessContribution, ...],
]:
    raw_processes = event.attributes.get("processes")
    if raw_processes is None:
        return "unavailable", ()
    if (
        not isinstance(raw_processes, list)
        or not raw_processes
        or len(raw_processes) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return "invalid", ()
    raw_process_count = event.attributes.get("process_count")
    if (
        not isinstance(raw_process_count, int)
        or isinstance(raw_process_count, bool)
        or raw_process_count != len(raw_processes)
    ):
        return "invalid", ()
    contributions: list[PythonSampleProcessContribution] = []
    seen_pids: set[int] = set()
    root_count = 0
    for raw_process in raw_processes:
        if not isinstance(raw_process, dict):
            return "invalid", ()
        pid = raw_process.get("pid")
        role = _process_role(raw_process.get("role"))
        process_sample_count = raw_process.get("sample_count")
        process_leaf_count = raw_process.get("leaf_sample_count")
        process_total = _nonnegative_number(raw_process.get("estimated_total_seconds"))
        process_leaf = _nonnegative_number(raw_process.get("estimated_leaf_seconds"))
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or pid in seen_pids
            or role is None
            or not isinstance(process_sample_count, int)
            or isinstance(process_sample_count, bool)
            or process_sample_count < 0
            or not isinstance(process_leaf_count, int)
            or isinstance(process_leaf_count, bool)
            or not 0 <= process_leaf_count <= process_sample_count
            or process_total is None
            or process_leaf is None
            or process_leaf > process_total
        ):
            return "invalid", ()
        seen_pids.add(pid)
        root_count += role == "root"
        if root_count > 1:
            return "invalid", ()
        process_name, parent_name, observed = _process_identity(
            pid,
            role,
            observed_processes,
            root_process_name,
        )
        contributions.append(
            PythonSampleProcessContribution(
                pid,
                role,
                process_name,
                parent_name,
                observed,
                process_sample_count,
                process_leaf_count,
                process_total,
                process_leaf,
            )
        )
    if (
        sum(item.sample_count for item in contributions) != sample_count
        or sum(item.leaf_sample_count for item in contributions) != leaf_sample_count
        or not math.isclose(
            sum(item.estimated_total_seconds for item in contributions),
            estimated_total_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
        or not math.isclose(
            sum(item.estimated_leaf_seconds for item in contributions),
            estimated_leaf_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
    ):
        return "invalid", ()
    contributions.sort(key=lambda item: (-item.leaf_sample_count, -item.sample_count, item.pid))
    return "complete", tuple(contributions)


def _python_hotspots(
    events: tuple[Event, ...],
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[PythonHotspot, ...]:
    hotspots: list[PythonHotspot] = []
    for event in events:
        if event.kind != "python.call.aggregate":
            continue
        filename = event.attributes.get("filename")
        firstlineno = event.attributes.get("firstlineno")
        scope = event.attributes.get("scope")
        call_count = event.attributes.get("call_count")
        implementation = event.attributes.get("implementation", "python")
        exception_count = event.attributes.get("exception_count", 0)
        non_control_flow_exception_count = event.attributes.get("non_control_flow_exception_count")
        total_seconds = _event_number(event, "total_seconds")
        self_seconds = _event_number(event, "self_seconds")
        max_seconds = _event_number(event, "max_seconds")
        if (
            not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(scope, str)
            or scope not in {"application", "library", "runtime"}
            or not isinstance(call_count, int)
            or isinstance(call_count, bool)
            or call_count < 0
            or implementation not in {"python", "native"}
            or (implementation == "native") != (filename == "<native>")
            or not isinstance(exception_count, int)
            or isinstance(exception_count, bool)
            or exception_count < 0
            or (implementation == "native" and exception_count > call_count)
            or (
                non_control_flow_exception_count is not None
                and (
                    implementation != "python"
                    or not isinstance(non_control_flow_exception_count, int)
                    or isinstance(non_control_flow_exception_count, bool)
                    or not 0 <= non_control_flow_exception_count <= exception_count
                )
            )
            or total_seconds is None
            or self_seconds is None
            or max_seconds is None
        ):
            continue
        implementation_value: Literal["python", "native"] = (
            "native" if implementation == "native" else "python"
        )
        attribution_status, processes = _deep_process_contributions(
            event,
            implementation=implementation_value,
            call_count=call_count,
            total_seconds=total_seconds,
            self_seconds=self_seconds,
            max_seconds=max_seconds,
            exception_count=exception_count,
            non_control_flow_exception_count=(
                non_control_flow_exception_count
                if isinstance(non_control_flow_exception_count, int)
                and not isinstance(non_control_flow_exception_count, bool)
                else None
            ),
            observed_processes=observed_processes,
            root_process_name=root_process_name,
        )
        hotspots.append(
            PythonHotspot(
                event.name,
                filename,
                firstlineno,
                scope,
                call_count,
                total_seconds,
                self_seconds,
                max_seconds,
                attribution_status,
                processes,
                implementation_value,
                exception_count,
                (
                    non_control_flow_exception_count
                    if isinstance(non_control_flow_exception_count, int)
                    and not isinstance(non_control_flow_exception_count, bool)
                    else None
                ),
            )
        )
    hotspots.sort(
        key=lambda item: (
            item.scope != "application",
            -item.self_seconds,
            -item.total_seconds,
            item.name,
        )
    )
    return tuple(hotspots)


def _python_sample_hotspots(
    events: tuple[Event, ...],
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[PythonSampleHotspot, ...]:
    hotspots: list[PythonSampleHotspot] = []
    for event in events:
        if event.kind != "python.stack.sample":
            continue
        filename = event.attributes.get("filename")
        firstlineno = event.attributes.get("firstlineno")
        scope = event.attributes.get("scope")
        sample_count = event.attributes.get("sample_count")
        leaf_sample_count = event.attributes.get("leaf_sample_count")
        estimated_total_seconds = _event_number(event, "estimated_total_seconds")
        estimated_leaf_seconds = _event_number(event, "estimated_leaf_seconds")
        if (
            not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(scope, str)
            or scope not in {"application", "library", "runtime"}
            or not isinstance(sample_count, int)
            or isinstance(sample_count, bool)
            or sample_count < 0
            or not isinstance(leaf_sample_count, int)
            or isinstance(leaf_sample_count, bool)
            or not 0 <= leaf_sample_count <= sample_count
            or estimated_total_seconds is None
            or estimated_leaf_seconds is None
        ):
            continue
        attribution_status, processes = _sample_process_contributions(
            event,
            sample_count=sample_count,
            leaf_sample_count=leaf_sample_count,
            estimated_total_seconds=estimated_total_seconds,
            estimated_leaf_seconds=estimated_leaf_seconds,
            observed_processes=observed_processes,
            root_process_name=root_process_name,
        )
        hotspots.append(
            PythonSampleHotspot(
                event.name,
                filename,
                firstlineno,
                scope,
                sample_count,
                leaf_sample_count,
                estimated_total_seconds,
                estimated_leaf_seconds,
                attribution_status,
                processes,
            )
        )
    hotspots.sort(
        key=lambda item: (
            item.scope != "application",
            -item.leaf_sample_count,
            -item.sample_count,
            item.name,
        )
    )
    return tuple(hotspots[:100])


def _python_hotspot_bottleneck(
    hotspots: tuple[PythonHotspot, ...], total_seconds: float | None
) -> Bottleneck | None:
    application = next((item for item in hotspots if item.scope == "application"), None)
    if (
        application is None
        or total_seconds is None
        or total_seconds <= 0
        or application.self_seconds / total_seconds < 0.25
    ):
        return None
    return Bottleneck(
        "python_hotspot",
        f"{application.name} accumulated {application.self_seconds:.3f}s self time "
        f"across {application.call_count} calls under intrusive deep capture",
        0.6,
    )


def _python_exception_churn_bottleneck(
    hotspots: tuple[PythonHotspot, ...],
    profile: DeepProfileSummary | None,
) -> Bottleneck | None:
    if (
        profile is None
        or profile.status != "complete"
        or profile.truncated
        or profile.dropped_call_count != 0
        or profile.python_exception_capture is None
        or profile.python_exception_capture.status != "complete"
        or profile.python_exception_capture.dropped_event_count != 0
        or profile.python_exception_capture.control_flow_filter is None
        or profile.python_exception_capture.control_flow_filter.status != "complete"
        or profile.python_exception_capture.control_flow_filter.dropped_non_control_flow_event_count
        != 0
        or profile.observer_integrity is None
        or profile.observer_integrity.status != "complete"
    ):
        return None
    python_hotspots = tuple(item for item in hotspots if item.implementation == "python")
    exception_capture = profile.python_exception_capture
    control_flow_filter = exception_capture.control_flow_filter
    assert control_flow_filter is not None
    diagnostic_hotspots = tuple(
        (item, item.non_control_flow_exception_count)
        for item in python_hotspots
        if item.non_control_flow_exception_count is not None
    )
    if (
        len(hotspots) != profile.function_count
        or sum(item.exception_count for item in python_hotspots) != exception_capture.event_count
        or sum(item.exception_count > 0 for item in python_hotspots)
        != exception_capture.function_count
        or len(diagnostic_hotspots) != len(python_hotspots)
        or sum(count for _, count in diagnostic_hotspots)
        != control_flow_filter.non_control_flow_event_count
        or sum(count > 0 for _, count in diagnostic_hotspots)
        != control_flow_filter.non_control_flow_function_count
    ):
        return None
    candidates = tuple(
        (item, count)
        for item, count in diagnostic_hotspots
        if item.scope == "application"
        and item.call_count > 0
        and count >= PYTHON_EXCEPTION_CHURN_MIN_EVENTS
        and count / item.call_count >= PYTHON_EXCEPTION_CHURN_MIN_EVENTS_PER_CALL
    )
    if not candidates:
        return None
    hotspot, non_control_flow_exception_count = min(
        candidates,
        key=lambda candidate: (
            -candidate[1],
            -(candidate[1] / candidate[0].call_count),
            -candidate[0].self_seconds,
            candidate[0].name,
        ),
    )
    events_per_call = non_control_flow_exception_count / hotspot.call_count
    return Bottleneck(
        "python_exception_churn",
        f"{hotspot.name} recorded {non_control_flow_exception_count} non-control-flow Python "
        f"exception propagation events across {hotspot.call_count} calls "
        f"({events_per_call:.2f} per call); built-in iterator completion is excluded and "
        "events are not unique failures",
        0.6,
    )


def _python_sample_hotspot_bottleneck(
    hotspots: tuple[PythonSampleHotspot, ...],
) -> Bottleneck | None:
    application = next((item for item in hotspots if item.scope == "application"), None)
    total_leaf_samples = sum(item.leaf_sample_count for item in hotspots)
    if (
        application is None
        or application.leaf_sample_count < 3
        or total_leaf_samples <= 0
        or application.leaf_sample_count / total_leaf_samples < 0.25
    ):
        return None
    return Bottleneck(
        "python_sample_hotspot",
        f"{application.name} was the leaf Python frame in "
        f"{application.leaf_sample_count} / {total_leaf_samples} thread samples",
        0.5,
    )


def analyze_reader(reader: RunpackReader, summary: ExecutionSummary | None = None) -> BatchAnalysis:
    """Analyze one run from the reader's stable snapshot."""
    summary = inspect_reader(reader) if summary is None else summary
    all_events = reader.events()
    all_edges = reader.causal_edges()
    execution_metadata = reader.execution().metadata
    process_observer = _process_observer_summary(execution_metadata)
    if process_observer is None:
        process_hotspots: tuple[ProcessResourceHotspot, ...] = ()
        observed_processes: dict[int, _ObservedProcessIdentity] = {}
    else:
        process_hotspots, observed_processes = _process_resource_evidence(reader)
    (
        subprocess_calls,
        invalid_subprocess_event_count,
        invalid_caller_count,
        caller_count,
        attributed_subprocess_count,
    ) = _subprocess_calls(all_events, all_edges, observed_processes)
    subprocess_event_count = sum(event.kind == "subprocess.run" for event in all_events)
    semantic_capture = _semantic_capture_summary(
        execution_metadata,
        subprocess_event_count=subprocess_event_count,
        invalid_event_count=invalid_subprocess_event_count,
        caller_count=caller_count,
        attributed_subprocess_count=attributed_subprocess_count,
        invalid_caller_count=invalid_caller_count,
    )
    (
        http_requests,
        invalid_http_event_count,
        invalid_http_caller_count,
        http_caller_count,
        attributed_http_request_count,
        observed_http_adapters,
    ) = _http_requests(all_events, all_edges)
    http_request_event_count = sum(event.kind == "http.client.request" for event in all_events)
    http_capture = _http_capture_summary(
        execution_metadata,
        request_event_count=http_request_event_count,
        invalid_event_count=invalid_http_event_count,
        caller_count=http_caller_count,
        attributed_request_count=attributed_http_request_count,
        invalid_caller_count=invalid_http_caller_count,
        observed_adapters=observed_http_adapters,
    )
    (
        all_network_connections,
        invalid_network_event_count,
        invalid_network_caller_count,
        network_caller_count,
        attributed_network_connection_count,
        observed_network_adapters,
    ) = _network_connections(all_events, all_edges)
    network_connection_event_count = sum(event.kind == "network.connect" for event in all_events)
    all_network_connection_hotspots = _network_connection_hotspots(all_network_connections)
    network_capture = _network_capture_summary(
        execution_metadata,
        connection_event_count=network_connection_event_count,
        invalid_event_count=invalid_network_event_count,
        caller_count=network_caller_count,
        attributed_connection_count=attributed_network_connection_count,
        invalid_caller_count=invalid_network_caller_count,
        observed_adapters=observed_network_adapters,
        connection_hotspot_count=len(all_network_connection_hotspots),
    )
    network_connections = all_network_connections[:MAX_NETWORK_CONNECTION_SUMMARIES]
    network_connection_hotspots = all_network_connection_hotspots[:MAX_NETWORK_CONNECTION_SUMMARIES]
    (
        all_network_setup_phases,
        invalid_network_setup_event_count,
        invalid_network_setup_caller_count,
        network_setup_caller_count,
        attributed_network_setup_count,
        observed_network_setup_adapters,
    ) = _network_setup_phases(all_events, all_edges)
    network_setup_event_count = sum(
        event.kind in {"network.resolve", "network.tls_handshake"} for event in all_events
    )
    all_network_setup_hotspots = _network_setup_hotspots(all_network_setup_phases)
    network_setup_capture = _network_setup_capture_summary(
        execution_metadata,
        phase_event_count=network_setup_event_count,
        invalid_event_count=invalid_network_setup_event_count,
        caller_count=network_setup_caller_count,
        attributed_phase_count=attributed_network_setup_count,
        invalid_caller_count=invalid_network_setup_caller_count,
        observed_adapters=observed_network_setup_adapters,
        hotspot_count=len(all_network_setup_hotspots),
    )
    network_setup_phases = all_network_setup_phases[:MAX_NETWORK_SETUP_SUMMARIES]
    network_setup_hotspots = all_network_setup_hotspots[:MAX_NETWORK_SETUP_SUMMARIES]
    (
        all_logical_operations,
        invalid_logical_operation_event_count,
        invalid_logical_operation_caller_count,
        logical_operation_caller_count,
        attributed_logical_operation_count,
        observed_logical_operation_adapters,
    ) = _logical_operations(all_events, all_edges)
    logical_operation_event_count = sum(
        event.attributes.get("source") == "python-logical-operation-wrapper"
        and event.kind != "python.callsite"
        for event in all_events
    )
    all_logical_operation_hotspots = _logical_operation_hotspots(all_logical_operations)
    logical_operation_capture = _logical_operation_capture_summary(
        execution_metadata,
        operation_event_count=logical_operation_event_count,
        invalid_event_count=invalid_logical_operation_event_count,
        caller_count=logical_operation_caller_count,
        attributed_operation_count=attributed_logical_operation_count,
        invalid_caller_count=invalid_logical_operation_caller_count,
        observed_adapters=observed_logical_operation_adapters,
        hotspot_count=len(all_logical_operation_hotspots),
    )
    logical_operations = all_logical_operations[:MAX_LOGICAL_OPERATION_SUMMARIES]
    logical_operation_hotspots = all_logical_operation_hotspots[:MAX_LOGICAL_OPERATION_SUMMARIES]
    deep_profile = _deep_profile_summary(
        execution_metadata,
        process_observer,
        observed_processes,
    )
    sample_profile = _sample_profile_summary(
        execution_metadata,
        process_observer,
        observed_processes,
    )
    root_process_name = Path(summary.command[0]).name if summary.command else None
    all_python_hotspots = _python_hotspots(
        all_events,
        observed_processes,
        root_process_name,
    )
    python_hotspots = all_python_hotspots[:MAX_PYTHON_HOTSPOTS]
    python_sample_hotspots = _python_sample_hotspots(
        all_events,
        observed_processes,
        root_process_name,
    )
    events = tuple(
        event
        for event in all_events
        if event.kind
        not in {
            "log.record",
            "python.call.aggregate",
            "python.stack.sample",
            "python.callsite",
            "subprocess.run",
            "http.client.request",
            "network.connect",
            "network.resolve",
            "network.tls_handshake",
        }
        and event.attributes.get("source") != "python-logical-operation-wrapper"
    )
    edges = all_edges
    clock_inconsistent = reader.clock_inconsistency_count() > 0
    event_ids = {event.id for event in events}
    edges = tuple(
        edge
        for edge in edges
        if edge.source_event_id in event_ids and edge.target_event_id in event_ids
    )
    edge_values = tuple(
        (edge.source_event_id, edge.target_event_id, edge.confidence) for edge in edges
    )
    critical = _critical_path(
        events,
        edge_values,
        summary.wall_time_seconds,
        clock_inconsistent=clock_inconsistent,
        causality_complete=(
            summary.missing_causal_references == 0 and summary.annotation_error is None
        ),
    )
    bottlenecks = list(_bottlenecks(events, edges, critical, summary.wall_time_seconds))
    bottlenecks.extend(
        _network_bottlenecks(
            all_network_connections,
            network_capture,
            summary.wall_time_seconds,
        )
    )
    bottlenecks.extend(
        _network_setup_bottlenecks(
            all_network_setup_phases,
            network_setup_capture,
            summary.wall_time_seconds,
        )
    )
    bottlenecks.extend(
        _logical_operation_bottlenecks(
            all_logical_operations,
            logical_operation_capture,
            summary.wall_time_seconds,
        )
    )
    python_hotspot = _python_hotspot_bottleneck(python_hotspots, summary.wall_time_seconds)
    if python_hotspot is not None:
        bottlenecks.append(python_hotspot)
    python_exception_churn = _python_exception_churn_bottleneck(
        all_python_hotspots,
        deep_profile,
    )
    if python_exception_churn is not None:
        bottlenecks.append(python_exception_churn)
    python_sample_hotspot = _python_sample_hotspot_bottleneck(python_sample_hotspots)
    if python_sample_hotspot is not None:
        bottlenecks.append(python_sample_hotspot)
    return BatchAnalysis(
        execution_id=summary.id,
        name=summary.name,
        total_seconds=summary.wall_time_seconds,
        lifecycle=_lifecycle(events, edges, summary.wall_time_seconds),
        critical_path=critical,
        throughput=_throughput(events, edges, summary.finished_at_ns),
        bottlenecks=tuple(bottlenecks),
        deep_profile=deep_profile,
        python_hotspots=python_hotspots,
        sample_profile=sample_profile,
        python_sample_hotspots=python_sample_hotspots,
        process_observer=process_observer,
        process_hotspots=process_hotspots,
        semantic_capture=semantic_capture,
        subprocess_calls=subprocess_calls,
        http_capture=http_capture,
        http_requests=http_requests,
        network_capture=network_capture,
        network_connections=network_connections,
        network_connection_hotspots=network_connection_hotspots,
        network_setup_capture=network_setup_capture,
        network_setup_phases=network_setup_phases,
        network_setup_hotspots=network_setup_hotspots,
        logical_operation_capture=logical_operation_capture,
        logical_operations=logical_operations,
        logical_operation_hotspots=logical_operation_hotspots,
    )


def analyze_runpack(path: Path) -> BatchAnalysis:
    path = resolve_runpack_path(path)
    with RunpackReader(path) as reader:
        return analyze_reader(reader)
