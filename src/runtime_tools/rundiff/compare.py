"""Structured RunDiff facts, independent of presentation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import computed_field

from runtime_tools.batchscope.analysis import analyze_reader
from runtime_tools.batchscope.analysis._boundary_models import (
    HttpCaptureSummary,
    LogicalOperationCaptureSummary,
    NetworkCaptureSummary,
    NetworkSetupCaptureSummary,
)
from runtime_tools.batchscope.analysis._profile_models import (
    SemanticCaptureSummary,
)
from runtime_tools.inspect import ExecutionSummary, inspect_reader
from runtime_tools.json_support import JsonDocumentModel, JsonValueModel
from runtime_tools.model import JsonValue
from runtime_tools.storage import RunpackError, RunpackReader, resolve_runpack_path

type Outcome = Literal["equivalent", "different", "unknown"]


@dataclass(frozen=True, slots=True)
class ValueChange(JsonValueModel):
    baseline: float | None
    candidate: float | None
    percent: float | None


@dataclass(frozen=True, slots=True)
class EntityCountChange(JsonValueModel):
    entity_kind: str
    entity_name: str
    baseline: int
    candidate: int

    @computed_field
    @property
    def change_kind(self) -> Literal["added", "removed", "changed"]:
        if self.baseline == 0:
            return "added"
        if self.candidate == 0:
            return "removed"
        return "changed"


@dataclass(frozen=True, slots=True)
class OperationCountChange(JsonValueModel):
    entity_kind: str
    entity_name: str
    operation_kind: str
    operation_name: str
    baseline: int
    candidate: int
    percent: float | None


@dataclass(frozen=True, slots=True)
class OperationConcurrencyChange(JsonValueModel):
    entity_kind: str
    entity_name: str
    operation_kind: str
    operation_name: str
    baseline: int
    candidate: int
    percent: float | None


@dataclass(frozen=True, slots=True)
class OperationDurationChange(JsonValueModel):
    entity_kind: str
    entity_name: str
    operation_kind: str
    operation_name: str
    baseline_seconds: float
    candidate_seconds: float
    percent: float | None


@dataclass(frozen=True, slots=True)
class EdgeCountChange(JsonValueModel):
    source_kind: str
    source_name: str
    target_kind: str
    target_name: str
    relation: str
    baseline: int
    candidate: int

    @computed_field
    @property
    def change_kind(self) -> Literal["new", "removed", "changed"]:
        if self.baseline == 0:
            return "new"
        if self.candidate == 0:
            return "removed"
        return "changed"


@dataclass(frozen=True, slots=True)
class EnvironmentChange(JsonValueModel):
    variable: str
    baseline_present: bool
    candidate_present: bool

    @computed_field
    @property
    def change_kind(self) -> Literal["added", "removed", "changed"]:
        if not self.baseline_present:
            return "added"
        if not self.candidate_present:
            return "removed"
        return "changed"


@dataclass(frozen=True, slots=True)
class ExecutionReference(JsonValueModel):
    id: str
    name: str
    exit_code: int | None


@dataclass(frozen=True, slots=True)
class InstrumentationComparison(JsonValueModel):
    baseline_mode: str
    candidate_mode: str
    timing_comparable: bool


@dataclass(frozen=True, slots=True)
class ExecutionDiff(JsonDocumentModel):
    document_type = "rundiff.compare"

    baseline: ExecutionReference
    candidate: ExecutionReference
    instrumentation: InstrumentationComparison
    match_level: Literal["exact", "structural", "aggregate"]
    baseline_annotation_error: str | None
    candidate_annotation_error: str | None
    baseline_missing_causal_references: int | None
    candidate_missing_causal_references: int | None
    baseline_dropped_attribute_count: int | None
    candidate_dropped_attribute_count: int | None
    baseline_semantic_capture_status: str | None
    candidate_semantic_capture_status: str | None
    baseline_dropped_subprocess_count: int | None
    candidate_dropped_subprocess_count: int | None
    baseline_stdout_relay_error: str | None
    baseline_stderr_relay_error: str | None
    candidate_stdout_relay_error: str | None
    candidate_stderr_relay_error: str | None
    baseline_incomplete_streams: tuple[str, ...]
    candidate_incomplete_streams: tuple[str, ...]
    outcome: Outcome
    exit_code_equivalent: bool | None
    output_equivalent: bool | None
    stderr_equivalent: bool | None
    operation_errors_equivalent: bool | None
    wall_time: ValueChange
    cpu_time: ValueChange
    critical_path: ValueChange
    baseline_critical_path_certainty: str | None
    candidate_critical_path_certainty: str | None
    peak_memory: ValueChange
    entity_count_changes: tuple[EntityCountChange, ...]
    operation_count_changes: tuple[OperationCountChange, ...]
    operation_error_count_changes: tuple[OperationCountChange, ...]
    operation_concurrency_changes: tuple[OperationConcurrencyChange, ...]
    operation_duration_changes: tuple[OperationDurationChange, ...]
    edge_count_changes: tuple[EdgeCountChange, ...]
    environment_changes: tuple[EnvironmentChange, ...]
    baseline_http_capture_status: str | None = None
    candidate_http_capture_status: str | None = None
    baseline_dropped_http_request_count: int | None = None
    candidate_dropped_http_request_count: int | None = None
    baseline_network_capture_status: str | None = None
    candidate_network_capture_status: str | None = None
    baseline_dropped_network_connection_count: int | None = None
    candidate_dropped_network_connection_count: int | None = None
    baseline_network_setup_capture_status: str | None = None
    candidate_network_setup_capture_status: str | None = None
    baseline_dropped_network_setup_phase_count: int | None = None
    candidate_dropped_network_setup_phase_count: int | None = None
    baseline_logical_operation_capture_status: str | None = None
    candidate_logical_operation_capture_status: str | None = None
    baseline_dropped_logical_operation_count: int | None = None
    candidate_dropped_logical_operation_count: int | None = None


def _percent(baseline: float | int | None, candidate: float | int | None) -> float | None:
    if baseline is None or candidate is None or baseline == 0:
        return None
    percent = (candidate - baseline) / baseline * 100
    return percent if math.isfinite(percent) else None


def _value_change(baseline: float | int | None, candidate: float | int | None) -> ValueChange:
    return ValueChange(
        float(baseline) if baseline is not None else None,
        float(candidate) if candidate is not None else None,
        _percent(baseline, candidate),
    )


def _known_equivalence(baseline: object | None, candidate: object | None) -> bool | None:
    if baseline is None or candidate is None:
        return None
    return baseline == candidate


def _cpu_time(summary: ExecutionSummary) -> float | None:
    if summary.cpu_user_seconds is None or summary.cpu_system_seconds is None:
        return None
    total = summary.cpu_user_seconds + summary.cpu_system_seconds
    return total if math.isfinite(total) else None


def _output_equivalence(
    baseline_bytes: int | None,
    baseline_digest: str | None,
    candidate_bytes: int | None,
    candidate_digest: str | None,
    baseline_complete: bool | None,
    candidate_complete: bool | None,
) -> bool | None:
    if (
        baseline_bytes is None
        or baseline_digest is None
        or candidate_bytes is None
        or candidate_digest is None
        or baseline_complete is not True
        or candidate_complete is not True
    ):
        return None
    return (baseline_bytes, baseline_digest) == (candidate_bytes, candidate_digest)


def _incomplete_streams(summary: ExecutionSummary) -> tuple[str, ...]:
    return tuple(
        stream for stream in ("stdout", "stderr") if getattr(summary, f"{stream}_complete") is False
    )


def _semantic_capture_observation(
    summary: SemanticCaptureSummary | None,
) -> tuple[str | None, int | None]:
    if summary is None:
        return None, None
    return summary.status, summary.dropped_subprocess_count


def _http_capture_observation(
    summary: HttpCaptureSummary | None,
) -> tuple[str | None, int | None]:
    if summary is None:
        return None, None
    return summary.status, summary.dropped_request_count


def _network_capture_observation(
    summary: NetworkCaptureSummary | None,
) -> tuple[str | None, int | None]:
    if summary is None:
        return None, None
    return summary.status, summary.dropped_connection_count


def _network_setup_capture_observation(
    summary: NetworkSetupCaptureSummary | None,
) -> tuple[str | None, int | None]:
    if summary is None:
        return None, None
    return summary.status, summary.dropped_phase_count


def _logical_operation_capture_observation(
    summary: LogicalOperationCaptureSummary | None,
) -> tuple[str | None, int | None]:
    if summary is None:
        return None, None
    return summary.status, summary.dropped_operation_count


def _optional_semantic_observation_complete(
    baseline_status: str | None,
    candidate_status: str | None,
) -> bool:
    if baseline_status in {None, "unavailable"} and candidate_status in {None, "unavailable"}:
        return True
    return baseline_status == candidate_status == "complete"


def _semantic_observation_complete(
    baseline_status: str | None,
    candidate_status: str | None,
) -> bool:
    if baseline_status is None and candidate_status is None:
        return True
    return baseline_status == candidate_status == "complete"


def _outcome(*equivalences: bool | None) -> Outcome:
    if any(value is False for value in equivalences):
        return "different"
    if all(value is True for value in equivalences):
        return "equivalent"
    return "unknown"


def _operation_changes(
    baseline: dict[tuple[str, str, str, str], int],
    candidate: dict[tuple[str, str, str, str], int],
) -> tuple[OperationCountChange, ...]:
    changes = []
    for key in baseline.keys() | candidate.keys():
        before = baseline.get(key, 0)
        after = candidate.get(key, 0)
        if before == after:
            continue
        changes.append(OperationCountChange(*key, before, after, _percent(before, after)))
    changes.sort(
        key=lambda change: (
            -abs(change.candidate - change.baseline),
            change.entity_kind,
            change.entity_name,
            change.operation_kind,
            change.operation_name,
        )
    )
    return tuple(changes)


def _entity_changes(
    baseline: dict[tuple[str, str], int],
    candidate: dict[tuple[str, str], int],
) -> tuple[EntityCountChange, ...]:
    changes = [
        EntityCountChange(*key, baseline.get(key, 0), candidate.get(key, 0))
        for key in baseline.keys() | candidate.keys()
        if baseline.get(key, 0) != candidate.get(key, 0)
    ]
    changes.sort(
        key=lambda change: (
            {"added": 0, "removed": 1, "changed": 2}[change.change_kind],
            -abs(change.candidate - change.baseline),
            change.entity_kind,
            change.entity_name,
        )
    )
    return tuple(changes)


def _concurrency_changes(
    baseline: dict[tuple[str, str, str, str], int],
    candidate: dict[tuple[str, str, str, str], int],
    baseline_counts: dict[tuple[str, str, str, str], int],
    candidate_counts: dict[tuple[str, str, str, str], int],
) -> tuple[OperationConcurrencyChange, ...]:
    changes = []
    for key in baseline.keys() | candidate.keys():
        if (key not in baseline and baseline_counts.get(key, 0) > 0) or (
            key not in candidate and candidate_counts.get(key, 0) > 0
        ):
            continue
        before = baseline.get(key, 0)
        after = candidate.get(key, 0)
        if before == after:
            continue
        changes.append(OperationConcurrencyChange(*key, before, after, _percent(before, after)))
    changes.sort(
        key=lambda change: (
            -abs(change.candidate - change.baseline),
            change.entity_kind,
            change.entity_name,
            change.operation_kind,
            change.operation_name,
        )
    )
    return tuple(changes)


def _edge_changes(
    baseline: dict[tuple[str, str, str, str, str], int],
    candidate: dict[tuple[str, str, str, str, str], int],
) -> tuple[EdgeCountChange, ...]:
    changes = []
    for key in baseline.keys() | candidate.keys():
        before = baseline.get(key, 0)
        after = candidate.get(key, 0)
        if before != after:
            changes.append(EdgeCountChange(*key, before, after))
    changes.sort(
        key=lambda change: (
            {"new": 0, "removed": 1, "changed": 2}[change.change_kind],
            change.source_name,
            change.target_name,
            change.relation,
        )
    )
    return tuple(changes)


def _duration_changes(
    baseline: dict[tuple[str, str, str, str], float],
    candidate: dict[tuple[str, str, str, str], float],
    baseline_counts: dict[tuple[str, str, str, str], int],
    candidate_counts: dict[tuple[str, str, str, str], int],
) -> tuple[OperationDurationChange, ...]:
    changes = []
    for key in baseline.keys() | candidate.keys():
        if (key not in baseline and baseline_counts.get(key, 0) > 0) or (
            key not in candidate and candidate_counts.get(key, 0) > 0
        ):
            continue
        before = baseline.get(key, 0.0)
        after = candidate.get(key, 0.0)
        if before == after:
            continue
        changes.append(OperationDurationChange(*key, before, after, _percent(before, after)))
    changes.sort(
        key=lambda change: (
            -abs(change.candidate_seconds - change.baseline_seconds),
            change.entity_kind,
            change.entity_name,
            change.operation_kind,
            change.operation_name,
        )
    )
    return tuple(changes)


def _all_edge_counts(reader: RunpackReader) -> dict[tuple[str, str, str, str, str], int]:
    counts = reader.edge_counts()
    for key, count in reader.peer_service_edge_counts().items():
        counts[key] = counts.get(key, 0) + count
    return counts


def _structural_edge_keys(
    reader: RunpackReader,
) -> set[tuple[str, str, str, str, str, str, str, str, str]]:
    entities = {entity.id: (entity.kind, entity.name) for entity in reader.entities()}
    events = {event.id: event for event in reader.events()}
    keys: set[tuple[str, str, str, str, str, str, str, str, str]] = set()
    for edge in reader.causal_edges():
        source = events[edge.source_event_id]
        target = events[edge.target_event_id]
        if source.kind in {
            "log.record",
            "python.call.aggregate",
            "python.stack.sample",
            "python.callsite",
        } or target.kind in {
            "log.record",
            "python.call.aggregate",
            "python.stack.sample",
            "python.callsite",
        }:
            continue
        source_entity = (
            entities[source.entity_id] if source.entity_id is not None else ("unowned", "unowned")
        )
        target_entity = (
            entities[target.entity_id] if target.entity_id is not None else ("unowned", "unowned")
        )
        keys.add(
            (
                *source_entity,
                source.kind,
                source.name,
                *target_entity,
                target.kind,
                target.name,
                edge.kind,
            )
        )
    return keys


def _structural_entity_parent_keys(
    reader: RunpackReader,
) -> set[tuple[str, str, str, str]]:
    entities = {entity.id: entity for entity in reader.entities()}
    return {
        (
            entity.kind,
            entity.name,
            entities[entity.parent_entity_id].kind,
            entities[entity.parent_entity_id].name,
        )
        for entity in entities.values()
        if entity.parent_entity_id is not None
    }


def _selected_environment(metadata: dict[str, JsonValue]) -> dict[str, str] | None:
    environment = metadata.get("environment")
    if environment is None:
        return None
    if not isinstance(environment, dict):
        raise RunpackError("execution environment metadata must be an object")
    selected = environment.get("selected_value_sha256")
    if selected is None:
        return None
    if not isinstance(selected, dict):
        raise RunpackError("selected environment identities must be an object")
    result: dict[str, str] = {}
    for variable, identity in selected.items():
        if not variable or not isinstance(identity, str) or len(identity) != 64:
            raise RunpackError(f"invalid selected environment identity: {variable}")
        try:
            decoded = bytes.fromhex(identity)
        except ValueError as exc:
            raise RunpackError(f"invalid selected environment identity: {variable}") from exc
        if len(decoded) != 32:
            raise RunpackError(f"invalid selected environment identity: {variable}")
        result[variable] = identity.lower()
    return result


def _instrumentation_mode(metadata: dict[str, JsonValue]) -> str:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return "passive"
    instrumentation = capture.get("instrumentation")
    mode_value = instrumentation.get("mode") if isinstance(instrumentation, dict) else None
    mode = mode_value if isinstance(mode_value, str) and mode_value else "passive"
    process_observer = capture.get("process_observer")
    observes_processes = (
        isinstance(process_observer, dict) and process_observer.get("requested") is True
    )
    if not observes_processes:
        return mode
    return "process" if mode == "passive" else f"{mode}+process"


def _environment_changes(
    baseline: dict[str, str] | None, candidate: dict[str, str] | None
) -> tuple[EnvironmentChange, ...]:
    if baseline is None or candidate is None:
        return ()
    return tuple(
        EnvironmentChange(variable, variable in baseline, variable in candidate)
        for variable in sorted(baseline.keys() | candidate.keys())
        if baseline.get(variable) != candidate.get(variable)
    )


def compare_readers(
    baseline_reader: RunpackReader, candidate_reader: RunpackReader
) -> ExecutionDiff:
    """Compare two runs using one stable snapshot of each artifact."""
    baseline_summary = inspect_reader(baseline_reader)
    candidate_summary = inspect_reader(candidate_reader)
    baseline_analysis = analyze_reader(baseline_reader, baseline_summary)
    candidate_analysis = analyze_reader(candidate_reader, candidate_summary)
    baseline_metadata = baseline_reader.execution().metadata
    baseline_environment = _selected_environment(baseline_metadata)
    baseline_instrumentation_mode = _instrumentation_mode(baseline_metadata)
    baseline_semantic_status, baseline_dropped_subprocess_count = _semantic_capture_observation(
        baseline_analysis.semantic_capture
    )
    baseline_http_status, baseline_dropped_http_request_count = _http_capture_observation(
        baseline_analysis.http_capture
    )
    baseline_network_status, baseline_dropped_network_connection_count = (
        _network_capture_observation(baseline_analysis.network_capture)
    )
    baseline_network_setup_status, baseline_dropped_network_setup_phase_count = (
        _network_setup_capture_observation(baseline_analysis.network_setup_capture)
    )
    baseline_logical_operation_status, baseline_dropped_logical_operation_count = (
        _logical_operation_capture_observation(baseline_analysis.logical_operation_capture)
    )
    baseline_entities = baseline_reader.entity_counts()
    baseline_operations = baseline_reader.operation_counts()
    baseline_errors = baseline_reader.operation_error_counts()
    baseline_concurrency = baseline_reader.operation_max_concurrency()
    baseline_durations = baseline_reader.operation_duration_totals()
    baseline_edges = _all_edge_counts(baseline_reader)
    baseline_structural_edges = _structural_edge_keys(baseline_reader)
    baseline_structural_parents = _structural_entity_parent_keys(baseline_reader)
    candidate_metadata = candidate_reader.execution().metadata
    candidate_environment = _selected_environment(candidate_metadata)
    candidate_instrumentation_mode = _instrumentation_mode(candidate_metadata)
    candidate_semantic_status, candidate_dropped_subprocess_count = _semantic_capture_observation(
        candidate_analysis.semantic_capture
    )
    candidate_http_status, candidate_dropped_http_request_count = _http_capture_observation(
        candidate_analysis.http_capture
    )
    candidate_network_status, candidate_dropped_network_connection_count = (
        _network_capture_observation(candidate_analysis.network_capture)
    )
    candidate_network_setup_status, candidate_dropped_network_setup_phase_count = (
        _network_setup_capture_observation(candidate_analysis.network_setup_capture)
    )
    candidate_logical_operation_status, candidate_dropped_logical_operation_count = (
        _logical_operation_capture_observation(candidate_analysis.logical_operation_capture)
    )
    candidate_entities = candidate_reader.entity_counts()
    candidate_operations = candidate_reader.operation_counts()
    candidate_errors = candidate_reader.operation_error_counts()
    candidate_concurrency = candidate_reader.operation_max_concurrency()
    candidate_durations = candidate_reader.operation_duration_totals()
    candidate_edges = _all_edge_counts(candidate_reader)
    candidate_structural_edges = _structural_edge_keys(candidate_reader)
    candidate_structural_parents = _structural_entity_parent_keys(candidate_reader)

    exit_equivalent = _known_equivalence(baseline_summary.exit_code, candidate_summary.exit_code)
    output_equivalent = _output_equivalence(
        baseline_summary.stdout_bytes,
        baseline_summary.stdout_sha256,
        candidate_summary.stdout_bytes,
        candidate_summary.stdout_sha256,
        baseline_summary.stdout_complete,
        candidate_summary.stdout_complete,
    )
    stderr_equivalent = _output_equivalence(
        baseline_summary.stderr_bytes,
        baseline_summary.stderr_sha256,
        candidate_summary.stderr_bytes,
        candidate_summary.stderr_sha256,
        baseline_summary.stderr_complete,
        candidate_summary.stderr_complete,
    )
    error_equivalent = (
        None
        if baseline_summary.annotation_error
        or candidate_summary.annotation_error
        or baseline_summary.dropped_attribute_count is None
        or candidate_summary.dropped_attribute_count is None
        or baseline_summary.dropped_attribute_count > 0
        or candidate_summary.dropped_attribute_count > 0
        or not _semantic_observation_complete(
            baseline_semantic_status,
            candidate_semantic_status,
        )
        or not _semantic_observation_complete(
            baseline_http_status,
            candidate_http_status,
        )
        or not _semantic_observation_complete(
            baseline_network_status,
            candidate_network_status,
        )
        or not _semantic_observation_complete(
            baseline_network_setup_status,
            candidate_network_setup_status,
        )
        or not _optional_semantic_observation_complete(
            baseline_logical_operation_status,
            candidate_logical_operation_status,
        )
        else baseline_errors == candidate_errors
    )
    if baseline_summary.id == candidate_summary.id:
        match_level: Literal["exact", "structural", "aggregate"] = "exact"
    elif (
        baseline_summary.missing_causal_references == 0
        and candidate_summary.missing_causal_references == 0
        and baseline_entities.keys() == candidate_entities.keys()
        and baseline_operations.keys() == candidate_operations.keys()
        and baseline_edges.keys() == candidate_edges.keys()
        and baseline_structural_edges == candidate_structural_edges
        and baseline_structural_parents == candidate_structural_parents
    ):
        match_level = "structural"
    else:
        match_level = "aggregate"
    return ExecutionDiff(
        baseline=ExecutionReference(
            baseline_summary.id,
            baseline_summary.name,
            baseline_summary.exit_code,
        ),
        candidate=ExecutionReference(
            candidate_summary.id,
            candidate_summary.name,
            candidate_summary.exit_code,
        ),
        instrumentation=InstrumentationComparison(
            baseline_instrumentation_mode,
            candidate_instrumentation_mode,
            baseline_instrumentation_mode == candidate_instrumentation_mode,
        ),
        match_level=match_level,
        baseline_annotation_error=baseline_summary.annotation_error,
        candidate_annotation_error=candidate_summary.annotation_error,
        baseline_missing_causal_references=baseline_summary.missing_causal_references,
        candidate_missing_causal_references=candidate_summary.missing_causal_references,
        baseline_dropped_attribute_count=baseline_summary.dropped_attribute_count,
        candidate_dropped_attribute_count=candidate_summary.dropped_attribute_count,
        baseline_semantic_capture_status=baseline_semantic_status,
        candidate_semantic_capture_status=candidate_semantic_status,
        baseline_dropped_subprocess_count=baseline_dropped_subprocess_count,
        candidate_dropped_subprocess_count=candidate_dropped_subprocess_count,
        baseline_http_capture_status=baseline_http_status,
        candidate_http_capture_status=candidate_http_status,
        baseline_dropped_http_request_count=baseline_dropped_http_request_count,
        candidate_dropped_http_request_count=candidate_dropped_http_request_count,
        baseline_network_capture_status=baseline_network_status,
        candidate_network_capture_status=candidate_network_status,
        baseline_dropped_network_connection_count=(baseline_dropped_network_connection_count),
        candidate_dropped_network_connection_count=(candidate_dropped_network_connection_count),
        baseline_network_setup_capture_status=baseline_network_setup_status,
        candidate_network_setup_capture_status=candidate_network_setup_status,
        baseline_dropped_network_setup_phase_count=(baseline_dropped_network_setup_phase_count),
        candidate_dropped_network_setup_phase_count=(candidate_dropped_network_setup_phase_count),
        baseline_logical_operation_capture_status=baseline_logical_operation_status,
        candidate_logical_operation_capture_status=candidate_logical_operation_status,
        baseline_dropped_logical_operation_count=(baseline_dropped_logical_operation_count),
        candidate_dropped_logical_operation_count=(candidate_dropped_logical_operation_count),
        baseline_stdout_relay_error=baseline_summary.stdout_relay_error,
        baseline_stderr_relay_error=baseline_summary.stderr_relay_error,
        candidate_stdout_relay_error=candidate_summary.stdout_relay_error,
        candidate_stderr_relay_error=candidate_summary.stderr_relay_error,
        baseline_incomplete_streams=_incomplete_streams(baseline_summary),
        candidate_incomplete_streams=_incomplete_streams(candidate_summary),
        outcome=_outcome(
            exit_equivalent,
            output_equivalent,
            stderr_equivalent,
            error_equivalent,
        ),
        exit_code_equivalent=exit_equivalent,
        output_equivalent=output_equivalent,
        stderr_equivalent=stderr_equivalent,
        operation_errors_equivalent=error_equivalent,
        wall_time=_value_change(
            baseline_summary.wall_time_seconds, candidate_summary.wall_time_seconds
        ),
        cpu_time=_value_change(_cpu_time(baseline_summary), _cpu_time(candidate_summary)),
        critical_path=_value_change(
            (
                baseline_analysis.critical_path.duration_seconds
                if baseline_analysis.critical_path
                else None
            ),
            (
                candidate_analysis.critical_path.duration_seconds
                if candidate_analysis.critical_path
                else None
            ),
        ),
        baseline_critical_path_certainty=(
            baseline_analysis.critical_path.certainty if baseline_analysis.critical_path else None
        ),
        candidate_critical_path_certainty=(
            candidate_analysis.critical_path.certainty if candidate_analysis.critical_path else None
        ),
        peak_memory=_value_change(
            baseline_summary.peak_memory_bytes, candidate_summary.peak_memory_bytes
        ),
        entity_count_changes=_entity_changes(baseline_entities, candidate_entities),
        operation_count_changes=_operation_changes(baseline_operations, candidate_operations),
        operation_error_count_changes=_operation_changes(baseline_errors, candidate_errors),
        operation_concurrency_changes=_concurrency_changes(
            baseline_concurrency,
            candidate_concurrency,
            baseline_operations,
            candidate_operations,
        ),
        operation_duration_changes=_duration_changes(
            baseline_durations,
            candidate_durations,
            baseline_operations,
            candidate_operations,
        ),
        edge_count_changes=_edge_changes(baseline_edges, candidate_edges),
        environment_changes=_environment_changes(baseline_environment, candidate_environment),
    )


def compare_runpacks(baseline_path: Path, candidate_path: Path) -> ExecutionDiff:
    baseline_path = resolve_runpack_path(baseline_path)
    candidate_path = resolve_runpack_path(candidate_path)
    with (
        RunpackReader(baseline_path) as baseline_reader,
        RunpackReader(candidate_path) as candidate_reader,
    ):
        return compare_readers(baseline_reader, candidate_reader)
