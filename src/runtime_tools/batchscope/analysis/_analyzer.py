"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.batchscope.analysis._boundary_models import _ObservedProcessIdentity
from runtime_tools.batchscope.analysis._common import (
    MAX_LOGICAL_OPERATION_SUMMARIES,
    MAX_NETWORK_CONNECTION_SUMMARIES,
    MAX_NETWORK_SETUP_SUMMARIES,
    MAX_PYTHON_HOTSPOTS,
)
from runtime_tools.batchscope.analysis._hotspots import (
    _python_exception_churn_bottleneck,
    _python_hotspot_bottleneck,
    _python_hotspots,
    _python_sample_hotspot_bottleneck,
    _python_sample_hotspots,
)
from runtime_tools.batchscope.analysis._http import _http_capture_summary, _http_requests
from runtime_tools.batchscope.analysis._lifecycle import (
    _bottlenecks,
    _critical_path,
    _lifecycle,
    _throughput,
)
from runtime_tools.batchscope.analysis._logical import (
    _logical_operation_bottlenecks,
    _logical_operation_capture_summary,
    _logical_operation_hotspots,
    _logical_operations,
)
from runtime_tools.batchscope.analysis._network import (
    _network_bottlenecks,
    _network_capture_summary,
    _network_connection_hotspots,
    _network_connections,
    _network_setup_bottlenecks,
    _network_setup_capture_summary,
    _network_setup_hotspots,
    _network_setup_phases,
)
from runtime_tools.batchscope.analysis._profile_models import ProcessResourceHotspot
from runtime_tools.batchscope.analysis._profile_summary import (
    _deep_profile_summary,
    _process_observer_summary,
    _sample_profile_summary,
)
from runtime_tools.batchscope.analysis._resources import _process_resource_evidence
from runtime_tools.batchscope.analysis._result import BatchAnalysis
from runtime_tools.batchscope.analysis._subprocess import (
    _semantic_capture_summary,
    _subprocess_calls,
)
from runtime_tools.inspect import ExecutionSummary, inspect_reader
from runtime_tools.storage import RunpackReader, resolve_runpack_path


def _analyze_reader(
    reader: RunpackReader, summary: ExecutionSummary | None = None
) -> BatchAnalysis:
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


class BatchAnalyzer:
    """Analyze one immutable runpack snapshot."""

    __slots__ = ("_reader",)

    def __init__(self, reader: RunpackReader) -> None:
        self._reader = reader

    def analyze(self, summary: ExecutionSummary | None = None) -> BatchAnalysis:
        return _analyze_reader(self._reader, summary)


def analyze_reader(reader: RunpackReader, summary: ExecutionSummary | None = None) -> BatchAnalysis:
    return BatchAnalyzer(reader).analyze(summary)


def analyze_runpack(path: Path) -> BatchAnalysis:
    path = resolve_runpack_path(path)
    with RunpackReader(path) as reader:
        return analyze_reader(reader)
