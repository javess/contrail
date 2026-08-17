"""Structured RunDiff facts, independent of presentation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.inspect import ExecutionSummary, inspect_runpack
from runtime_tools.model import JsonValue
from runtime_tools.storage import RunpackReader

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
class ExecutionDiff:
    baseline_id: str
    baseline_name: str
    candidate_id: str
    candidate_name: str
    match_level: Literal["aggregate"]
    outcome: Outcome
    exit_code_equivalent: bool | None
    output_equivalent: bool | None
    wall_time: ValueChange
    critical_path: ValueChange
    peak_memory: ValueChange
    operation_count_changes: tuple[OperationCountChange, ...]
    edge_count_changes: tuple[EdgeCountChange, ...]

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "baseline": {"id": self.baseline_id, "name": self.baseline_name},
            "candidate": {"id": self.candidate_id, "name": self.candidate_name},
            "match_level": self.match_level,
            "outcome": self.outcome,
            "exit_code_equivalent": self.exit_code_equivalent,
            "output_equivalent": self.output_equivalent,
            "wall_time": self.wall_time.as_json_value(),
            "critical_path": self.critical_path.as_json_value(),
            "peak_memory": self.peak_memory.as_json_value(),
            "operation_count_changes": [
                change.as_json_value() for change in self.operation_count_changes
            ],
            "edge_count_changes": [change.as_json_value() for change in self.edge_count_changes],
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


def _outcome(exit_equivalent: bool | None, output_equivalent: bool | None) -> Outcome:
    known = tuple(value for value in (exit_equivalent, output_equivalent) if value is not None)
    if not known:
        return "unknown"
    return "equivalent" if all(known) else "different"


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


def _output_hash(summary: ExecutionSummary) -> str | None:
    return summary.stdout_sha256


def _all_edge_counts(reader: RunpackReader) -> dict[tuple[str, str, str, str, str], int]:
    counts = {key: count for key, count in reader.edge_counts().items() if key[:2] != key[2:4]}
    entities = {entity.id: entity for entity in reader.entities()}
    for event in reader.events():
        peer = event.attributes.get("peer.service")
        entity = entities.get(event.entity_id or "")
        if event.kind != "client.request" or not isinstance(peer, str) or entity is None:
            continue
        key = (entity.kind, entity.name, "service", peer, "calls")
        counts[key] = counts.get(key, 0) + 1
    return counts


def compare_runpacks(baseline_path: Path, candidate_path: Path) -> ExecutionDiff:
    baseline_summary = inspect_runpack(baseline_path)
    candidate_summary = inspect_runpack(candidate_path)
    baseline_analysis = analyze_runpack(baseline_path)
    candidate_analysis = analyze_runpack(candidate_path)
    with RunpackReader(baseline_path) as baseline_reader:
        baseline_operations = baseline_reader.operation_counts()
        baseline_edges = _all_edge_counts(baseline_reader)
    with RunpackReader(candidate_path) as candidate_reader:
        candidate_operations = candidate_reader.operation_counts()
        candidate_edges = _all_edge_counts(candidate_reader)

    exit_equivalent = _known_equivalence(baseline_summary.exit_code, candidate_summary.exit_code)
    output_equivalent = _known_equivalence(
        _output_hash(baseline_summary), _output_hash(candidate_summary)
    )
    return ExecutionDiff(
        baseline_id=baseline_summary.id,
        baseline_name=baseline_summary.name,
        candidate_id=candidate_summary.id,
        candidate_name=candidate_summary.name,
        match_level="aggregate",
        outcome=_outcome(exit_equivalent, output_equivalent),
        exit_code_equivalent=exit_equivalent,
        output_equivalent=output_equivalent,
        wall_time=_value_change(
            baseline_summary.wall_time_seconds, candidate_summary.wall_time_seconds
        ),
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
        peak_memory=_value_change(
            baseline_summary.peak_memory_bytes, candidate_summary.peak_memory_bytes
        ),
        operation_count_changes=_operation_changes(baseline_operations, candidate_operations),
        edge_count_changes=_edge_changes(baseline_edges, candidate_edges),
    )
