"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from dataclasses import dataclass

from runtime_tools.batchscope.analysis._boundary_models import (
    HttpCaptureSummary,
    HttpRequest,
    LogicalOperation,
    LogicalOperationCaptureSummary,
    LogicalOperationHotspot,
    NetworkCaptureSummary,
    NetworkConnection,
    NetworkConnectionHotspot,
    NetworkSetupCaptureSummary,
    NetworkSetupHotspot,
    NetworkSetupPhase,
    SubprocessCall,
)
from runtime_tools.batchscope.analysis._profile_models import (
    Bottleneck,
    CriticalPath,
    DeepProfileSummary,
    LifecyclePhase,
    ProcessObserverSummary,
    ProcessResourceHotspot,
    PythonHotspot,
    PythonSampleHotspot,
    SampleProfileSummary,
    SemanticCaptureSummary,
    Throughput,
)
from runtime_tools.json_support import JsonDocumentModel


@dataclass(frozen=True, slots=True)
class BatchAnalysis(JsonDocumentModel):
    document_type = "batchscope.inspect"

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
