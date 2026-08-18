"""Structured RunDiff facts, independent of presentation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.inspect import ExecutionSummary, inspect_runpack
from runtime_tools.model import JsonValue
from runtime_tools.storage import RunpackError, RunpackReader

type Outcome = Literal["equivalent", "different", "unknown"]


@dataclass(frozen=True, slots=True)
class ValueChange:
    baseline: float | None
    candidate: float | None
    percent: float | None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "baseline": self.baseline,
            "candidate": self.candidate,
            "percent": self.percent,
        }


@dataclass(frozen=True, slots=True)
class EntityCountChange:
    entity_kind: str
    entity_name: str
    baseline: int
    candidate: int

    @property
    def change_kind(self) -> Literal["added", "removed", "changed"]:
        if self.baseline == 0:
            return "added"
        if self.candidate == 0:
            return "removed"
        return "changed"

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "entity_kind": self.entity_kind,
            "entity_name": self.entity_name,
            "baseline": self.baseline,
            "candidate": self.candidate,
            "change_kind": self.change_kind,
        }


@dataclass(frozen=True, slots=True)
class OperationCountChange:
    entity_kind: str
    entity_name: str
    operation_kind: str
    operation_name: str
    baseline: int
    candidate: int
    percent: float | None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "entity_kind": self.entity_kind,
            "entity_name": self.entity_name,
            "operation_kind": self.operation_kind,
            "operation_name": self.operation_name,
            "baseline": self.baseline,
            "candidate": self.candidate,
            "percent": self.percent,
        }


@dataclass(frozen=True, slots=True)
class OperationConcurrencyChange:
    entity_kind: str
    entity_name: str
    operation_kind: str
    operation_name: str
    baseline: int
    candidate: int
    percent: float | None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "entity_kind": self.entity_kind,
            "entity_name": self.entity_name,
            "operation_kind": self.operation_kind,
            "operation_name": self.operation_name,
            "baseline": self.baseline,
            "candidate": self.candidate,
            "percent": self.percent,
        }


@dataclass(frozen=True, slots=True)
class OperationDurationChange:
    entity_kind: str
    entity_name: str
    operation_kind: str
    operation_name: str
    baseline_seconds: float
    candidate_seconds: float
    percent: float | None

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "entity_kind": self.entity_kind,
            "entity_name": self.entity_name,
            "operation_kind": self.operation_kind,
            "operation_name": self.operation_name,
            "baseline_seconds": self.baseline_seconds,
            "candidate_seconds": self.candidate_seconds,
            "percent": self.percent,
        }


@dataclass(frozen=True, slots=True)
class EdgeCountChange:
    source_kind: str
    source_name: str
    target_kind: str
    target_name: str
    relation: str
    baseline: int
    candidate: int

    @property
    def change_kind(self) -> Literal["new", "removed", "changed"]:
        if self.baseline == 0:
            return "new"
        if self.candidate == 0:
            return "removed"
        return "changed"

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "source_kind": self.source_kind,
            "source_name": self.source_name,
            "target_kind": self.target_kind,
            "target_name": self.target_name,
            "relation": self.relation,
            "baseline": self.baseline,
            "candidate": self.candidate,
            "change_kind": self.change_kind,
        }


@dataclass(frozen=True, slots=True)
class EnvironmentChange:
    variable: str
    baseline_present: bool
    candidate_present: bool

    @property
    def change_kind(self) -> Literal["added", "removed", "changed"]:
        if not self.baseline_present:
            return "added"
        if not self.candidate_present:
            return "removed"
        return "changed"

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "variable": self.variable,
            "baseline_present": self.baseline_present,
            "candidate_present": self.candidate_present,
            "change_kind": self.change_kind,
        }


@dataclass(frozen=True, slots=True)
class ExecutionDiff:
    baseline_id: str
    baseline_name: str
    candidate_id: str
    candidate_name: str
    match_level: Literal["exact", "aggregate"]
    baseline_annotation_error: str | None
    candidate_annotation_error: str | None
    baseline_missing_causal_references: int | None
    candidate_missing_causal_references: int | None
    baseline_dropped_attribute_count: int | None
    candidate_dropped_attribute_count: int | None
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "baseline": {"id": self.baseline_id, "name": self.baseline_name},
            "candidate": {"id": self.candidate_id, "name": self.candidate_name},
            "match_level": self.match_level,
            "baseline_annotation_error": self.baseline_annotation_error,
            "candidate_annotation_error": self.candidate_annotation_error,
            "baseline_missing_causal_references": self.baseline_missing_causal_references,
            "candidate_missing_causal_references": self.candidate_missing_causal_references,
            "baseline_dropped_attribute_count": self.baseline_dropped_attribute_count,
            "candidate_dropped_attribute_count": self.candidate_dropped_attribute_count,
            "baseline_incomplete_streams": list(self.baseline_incomplete_streams),
            "candidate_incomplete_streams": list(self.candidate_incomplete_streams),
            "outcome": self.outcome,
            "exit_code_equivalent": self.exit_code_equivalent,
            "output_equivalent": self.output_equivalent,
            "stderr_equivalent": self.stderr_equivalent,
            "operation_errors_equivalent": self.operation_errors_equivalent,
            "wall_time": self.wall_time.as_json_value(),
            "cpu_time": self.cpu_time.as_json_value(),
            "critical_path": self.critical_path.as_json_value(),
            "baseline_critical_path_certainty": self.baseline_critical_path_certainty,
            "candidate_critical_path_certainty": self.candidate_critical_path_certainty,
            "peak_memory": self.peak_memory.as_json_value(),
            "entity_count_changes": [
                change.as_json_value() for change in self.entity_count_changes
            ],
            "operation_count_changes": [
                change.as_json_value() for change in self.operation_count_changes
            ],
            "operation_error_count_changes": [
                change.as_json_value() for change in self.operation_error_count_changes
            ],
            "operation_concurrency_changes": [
                change.as_json_value() for change in self.operation_concurrency_changes
            ],
            "operation_duration_changes": [
                change.as_json_value() for change in self.operation_duration_changes
            ],
            "edge_count_changes": [change.as_json_value() for change in self.edge_count_changes],
            "environment_changes": [change.as_json_value() for change in self.environment_changes],
        }


def _percent(baseline: float | int | None, candidate: float | int | None) -> float | None:
    if baseline is None or candidate is None or baseline == 0:
        return None
    return (candidate - baseline) / baseline * 100


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
    return summary.cpu_user_seconds + summary.cpu_system_seconds


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


def _selected_environment(metadata: dict[str, JsonValue]) -> dict[str, str]:
    environment = metadata.get("environment")
    if environment is None:
        return {}
    if not isinstance(environment, dict):
        raise RunpackError("execution environment metadata must be an object")
    selected = environment.get("selected_value_sha256")
    if selected is None:
        return {}
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


def _environment_changes(
    baseline: dict[str, str], candidate: dict[str, str]
) -> tuple[EnvironmentChange, ...]:
    return tuple(
        EnvironmentChange(variable, variable in baseline, variable in candidate)
        for variable in sorted(baseline.keys() | candidate.keys())
        if baseline.get(variable) != candidate.get(variable)
    )


def compare_runpacks(baseline_path: Path, candidate_path: Path) -> ExecutionDiff:
    baseline_summary = inspect_runpack(baseline_path)
    candidate_summary = inspect_runpack(candidate_path)
    baseline_analysis = analyze_runpack(baseline_path)
    candidate_analysis = analyze_runpack(candidate_path)
    with RunpackReader(baseline_path) as baseline_reader:
        baseline_environment = _selected_environment(baseline_reader.execution().metadata)
        baseline_entities = baseline_reader.entity_counts()
        baseline_operations = baseline_reader.operation_counts()
        baseline_errors = baseline_reader.operation_error_counts()
        baseline_concurrency = baseline_reader.operation_max_concurrency()
        baseline_durations = baseline_reader.operation_duration_totals()
        baseline_edges = _all_edge_counts(baseline_reader)
    with RunpackReader(candidate_path) as candidate_reader:
        candidate_environment = _selected_environment(candidate_reader.execution().metadata)
        candidate_entities = candidate_reader.entity_counts()
        candidate_operations = candidate_reader.operation_counts()
        candidate_errors = candidate_reader.operation_error_counts()
        candidate_concurrency = candidate_reader.operation_max_concurrency()
        candidate_durations = candidate_reader.operation_duration_totals()
        candidate_edges = _all_edge_counts(candidate_reader)

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
        else baseline_errors == candidate_errors
    )
    return ExecutionDiff(
        baseline_id=baseline_summary.id,
        baseline_name=baseline_summary.name,
        candidate_id=candidate_summary.id,
        candidate_name=candidate_summary.name,
        match_level=("exact" if baseline_summary.id == candidate_summary.id else "aggregate"),
        baseline_annotation_error=baseline_summary.annotation_error,
        candidate_annotation_error=candidate_summary.annotation_error,
        baseline_missing_causal_references=baseline_summary.missing_causal_references,
        candidate_missing_causal_references=candidate_summary.missing_causal_references,
        baseline_dropped_attribute_count=baseline_summary.dropped_attribute_count,
        candidate_dropped_attribute_count=candidate_summary.dropped_attribute_count,
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
