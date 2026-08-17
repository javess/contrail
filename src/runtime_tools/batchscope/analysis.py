"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from runtime_tools.inspect import inspect_runpack
from runtime_tools.model import Event, JsonValue
from runtime_tools.storage import RunpackReader


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
    parallel_slack_seconds: float
    event_names: tuple[str, ...]
    certainty: Literal["observed", "inferred"]
    cycle_detected: bool

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "duration_seconds": self.duration_seconds,
            "parallel_slack_seconds": self.parallel_slack_seconds,
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

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "completed": self.completed,
            "total": self.total,
            "rate_per_second": self.rate_per_second,
            "remaining": self.remaining,
            "estimated_drain_seconds": self.estimated_drain_seconds,
        }


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
class BatchAnalysis:
    execution_id: str
    name: str
    total_seconds: float | None
    lifecycle: tuple[LifecyclePhase, ...]
    critical_path: CriticalPath | None
    throughput: Throughput | None
    bottlenecks: tuple[Bottleneck, ...]

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "execution_id": self.execution_id,
            "name": self.name,
            "total_seconds": self.total_seconds,
            "lifecycle": [phase.as_json_value() for phase in self.lifecycle],
            "critical_path": self.critical_path.as_json_value() if self.critical_path else None,
            "throughput": self.throughput.as_json_value() if self.throughput else None,
            "bottlenecks": [item.as_json_value() for item in self.bottlenecks],
        }


def _duration_ns(event: Event) -> int:
    if event.started_at_ns is None or event.finished_at_ns is None:
        return 0
    return max(0, event.finished_at_ns - event.started_at_ns)


def _covered_by_children(event: Event, children: tuple[Event, ...]) -> int:
    if event.started_at_ns is None or event.finished_at_ns is None:
        return 0
    intervals = []
    for child in children:
        if child.started_at_ns is None or child.finished_at_ns is None:
            continue
        start = max(event.started_at_ns, child.started_at_ns)
        end = min(event.finished_at_ns, child.finished_at_ns)
        if end > start:
            intervals.append((start, end))
    intervals.sort()
    covered = 0
    cursor_start: int | None = None
    cursor_end: int | None = None
    for start, end in intervals:
        if cursor_start is None:
            cursor_start, cursor_end = start, end
        elif cursor_end is not None and start <= cursor_end:
            cursor_end = max(cursor_end, end)
        else:
            if cursor_end is not None:
                covered += cursor_end - cursor_start
            cursor_start, cursor_end = start, end
    if cursor_start is not None and cursor_end is not None:
        covered += cursor_end - cursor_start
    return covered


def _critical_path(
    events: tuple[Event, ...], edge_pairs: tuple[tuple[str, str], ...], total: float | None
) -> CriticalPath | None:
    timed = {event.id: event for event in events if _duration_ns(event) > 0}
    if not timed:
        return None
    children: dict[str, list[str]] = {event_id: [] for event_id in timed}
    incoming: set[str] = set()
    used_edge_count = 0
    for source, target in edge_pairs:
        if source in timed and target in timed:
            children[source].append(target)
            incoming.add(target)
            used_edge_count += 1
    state: dict[str, int] = {}
    memo: dict[str, tuple[int, tuple[str, ...]]] = {}
    cycle_detected = False

    def visit(event_id: str) -> tuple[int, tuple[str, ...]]:
        nonlocal cycle_detected
        if state.get(event_id) == 1:
            cycle_detected = True
            return 0, ()
        if event_id in memo:
            return memo[event_id]
        state[event_id] = 1
        child_events = tuple(timed[child] for child in children[event_id])
        exclusive = _duration_ns(timed[event_id]) - _covered_by_children(
            timed[event_id], child_events
        )
        best_child = max((visit(child) for child in children[event_id]), default=(0, ()))
        result = (exclusive + best_child[0], (event_id, *best_child[1]))
        state[event_id] = 2
        memo[event_id] = result
        return result

    roots = [event_id for event_id in timed if event_id not in incoming]
    candidates = [visit(root) for root in roots]
    for event_id in timed:
        if event_id not in memo:
            candidates.append(visit(event_id))
    duration_ns, path_ids = max(candidates, default=(0, ()))
    duration_seconds = duration_ns / 1_000_000_000
    return CriticalPath(
        duration_seconds,
        max(0.0, round((total or duration_seconds) - duration_seconds, 12)),
        tuple(timed[event_id].name for event_id in path_ids),
        "observed" if used_edge_count else "inferred",
        cycle_detected,
    )


def _throughput(events: tuple[Event, ...]) -> Throughput | None:
    samples: list[tuple[int, float, float]] = []
    for event in events:
        if event.kind != "progress" or event.started_at_ns is None:
            continue
        completed = event.attributes.get("completed")
        total = event.attributes.get("total")
        if isinstance(completed, (int, float)) and isinstance(total, (int, float)):
            samples.append((event.started_at_ns, float(completed), float(total)))
    if not samples:
        return None
    samples.sort()
    latest = samples[-1]
    rate: float | None = None
    if len(samples) >= 2:
        elapsed = (latest[0] - samples[0][0]) / 1_000_000_000
        delta = latest[1] - samples[0][1]
        if elapsed > 0 and delta > 0:
            rate = delta / elapsed
    remaining = max(0.0, latest[2] - latest[1])
    return Throughput(latest[1], latest[2], rate, remaining, remaining / rate if rate else None)


def _lifecycle(events: tuple[Event, ...], total: float | None) -> tuple[LifecyclePhase, ...]:
    explicit = tuple(
        LifecyclePhase(event.name, _duration_ns(event) / 1_000_000_000, "explicit")
        for event in events
        if event.kind == "stage" and _duration_ns(event) > 0
    )
    if explicit:
        return explicit
    return (LifecyclePhase("executing", total, "derived"),) if total is not None else ()


def _bottlenecks(
    events: tuple[Event, ...], critical: CriticalPath | None, total: float | None
) -> tuple[Bottleneck, ...]:
    if total is None or total <= 0:
        return ()
    findings: list[Bottleneck] = []
    for event in events:
        duration = _duration_ns(event) / 1_000_000_000
        concurrency = event.attributes.get("concurrency")
        if event.kind == "stage" and concurrency == 1 and duration / total >= 0.25:
            findings.append(
                Bottleneck(
                    "serialized_stage",
                    f"{event.name} ran at concurrency 1 for {duration:.3f}s",
                    0.9,
                )
            )
    client_seconds = (
        sum(_duration_ns(event) for event in events if event.kind == "client.request")
        / 1_000_000_000
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
                    f"client operations account for {client_seconds:.3f}s against a "
                    f"{critical.duration_seconds:.3f}s critical path"
                ),
                0.75,
            )
        )
    return tuple(findings)


def analyze_runpack(path: Path) -> BatchAnalysis:
    summary = inspect_runpack(path)
    with RunpackReader(path) as reader:
        events = reader.events()
        edges = reader.causal_edges()
    edge_pairs = tuple((edge.source_event_id, edge.target_event_id) for edge in edges)
    critical = _critical_path(events, edge_pairs, summary.wall_time_seconds)
    return BatchAnalysis(
        summary.id,
        summary.name,
        summary.wall_time_seconds,
        _lifecycle(events, summary.wall_time_seconds),
        critical,
        _throughput(events),
        _bottlenecks(events, critical, summary.wall_time_seconds),
    )
