"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Literal

from runtime_tools.inspect import inspect_runpack
from runtime_tools.model import CausalEdge, Event, JsonValue
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


def _path_key(path: _Path) -> tuple[bool, int, int]:
    return bool(path.intervals), path.duration_ns, path.length


def _critical_path(
    events: tuple[Event, ...],
    edge_values: tuple[tuple[str, str, float], ...],
    total: float | None,
    *,
    clock_inconsistent: bool,
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
            and not clock_inconsistent
            and not cycle_detected
            else "inferred"
        ),
        cycle_detected=cycle_detected,
    )


def _throughput(
    events: tuple[Event, ...], edges: tuple[CausalEdge, ...], execution_finished_at_ns: int | None
) -> Throughput | None:
    parents_by_target: dict[str, set[str]] = {}
    for edge in edges:
        if edge.kind == "parent":
            parents_by_target.setdefault(edge.target_event_id, set()).add(edge.source_event_id)
    samples_by_series: dict[tuple[str, str], list[tuple[int, float, float]]] = {}
    entities_by_series: dict[tuple[str, str], set[str | None]] = {}
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
            samples_by_series.setdefault(series, []).append((event.started_at_ns, *values))
            entities_by_series.setdefault(series, set()).add(event.entity_id)
    if len(samples_by_series) != 1:
        return None
    series, samples = next(iter(samples_by_series.items()))
    series_entities = entities_by_series[series]
    samples_by_timestamp: dict[int, tuple[float, float]] = {}
    for timestamp_ns, completed, total in samples:
        previous = samples_by_timestamp.get(timestamp_ns)
        if previous is not None and previous != (completed, total):
            return None
        samples_by_timestamp[timestamp_ns] = (completed, total)
    samples = [
        (timestamp_ns, *values) for timestamp_ns, values in sorted(samples_by_timestamp.items())
    ]
    latest = samples[-1]
    rate = _sample_rate(samples)
    remaining = max(0.0, latest[2] - latest[1])
    series_event_ids: set[str] | None = None
    series_finished_at_ns = execution_finished_at_ns
    if series[0] == "parent":
        children_by_parent: dict[str, list[str]] = {}
        for edge in edges:
            if edge.kind == "parent":
                children_by_parent.setdefault(edge.source_event_id, []).append(edge.target_event_id)
        series_event_ids = {series[1]}
        series_parent = next((event for event in events if event.id == series[1]), None)
        if series_parent is not None and series_parent.finished_at_ns is not None:
            series_finished_at_ns = series_parent.finished_at_ns
        pending = [series[1]]
        while pending:
            parent_id = pending.pop()
            for child_id in children_by_parent.get(parent_id, ()):
                if child_id not in series_event_ids:
                    series_event_ids.add(child_id)
                    pending.append(child_id)
    compute_finishes = [
        event.finished_at_ns
        for event in events
        if event.kind == "stage"
        and event.finished_at_ns is not None
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
        before_compute = [sample for sample in samples if sample[0] <= compute_finished_at_ns]
        if before_compute:
            sample = before_compute[-1]
            remaining_at_compute = max(0.0, sample[2] - sample[1])
        if series_finished_at_ns is not None:
            post_compute_seconds = max(
                0.0, (series_finished_at_ns - compute_finished_at_ns) / 1_000_000_000
            )
        after_compute = [sample for sample in samples if sample[0] >= compute_finished_at_ns]
        post_compute_rate = _sample_rate(after_compute)
    estimated_drain = 0.0 if remaining == 0 else _finite_ratio(remaining, rate)
    return Throughput(
        latest[1],
        latest[2],
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


def _sample_rate(samples: list[tuple[int, float, float]]) -> float | None:
    if len(samples) < 2:
        return None
    first_total = samples[0][2]
    if any(sample[2] != first_total for sample in samples[1:]):
        return None
    if any(
        current[1] < previous[1] for previous, current in zip(samples, samples[1:], strict=False)
    ):
        return None
    elapsed = (samples[-1][0] - samples[0][0]) / 1_000_000_000
    delta = samples[-1][1] - samples[0][1]
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
    if pod_ids:
        pods = tuple(event for event in pods if event.id in pod_ids)
    container_ids = {
        edge.target_event_id
        for edge in edges
        if edge.kind == "contains" and edge.source_event_id in {pod.id for pod in pods}
    }
    if pod_ids:
        containers = tuple(event for event in containers if event.id in container_ids)
    return jobs, pods, containers


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
        job_start = min(
            (event.started_at_ns for event in jobs if event.started_at_ns is not None),
            default=None,
        )
        job_finish = max(
            (event.finished_at_ns for event in jobs if event.finished_at_ns is not None),
            default=None,
        )
        pod_start = min(
            (event.started_at_ns for event in pods if event.started_at_ns is not None),
            default=None,
        )
        container_start = min(
            (event.started_at_ns for event in containers if event.started_at_ns is not None),
            default=None,
        )
        container_finish = max(
            (event.finished_at_ns for event in containers if event.finished_at_ns is not None),
            default=None,
        )
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
        if phases:
            return phases
    return (LifecyclePhase("executing", total, "derived"),) if total is not None else ()


def _bottlenecks(
    events: tuple[Event, ...], critical: CriticalPath | None, total: float | None
) -> tuple[Bottleneck, ...]:
    if total is None or total <= 0:
        return ()
    findings: list[Bottleneck] = []
    failed_scheduling_count = sum(
        event.kind == "kubernetes.event" and event.name == "FailedScheduling" for event in events
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
                    f"client operations occupy {client_seconds:.3f}s of a "
                    f"{critical.duration_seconds:.3f}s critical path"
                ),
                0.75,
            )
        )
    straggler = _straggler_tail(events, total)
    if straggler is not None:
        findings.append(straggler)
    return tuple(findings)


def _straggler_tail(events: tuple[Event, ...], total: float) -> Bottleneck | None:
    cohorts: dict[tuple[str, str, str], list[Event]] = {}
    for event in events:
        if event.kind not in {"operation", "message.consume"} or not _has_complete_interval(event):
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


def analyze_runpack(path: Path) -> BatchAnalysis:
    summary = inspect_runpack(path)
    with RunpackReader(path) as reader:
        events = reader.events()
        edges = reader.causal_edges()
        clock_inconsistent = reader.clock_inconsistency_count() > 0
    events = tuple(event for event in events if event.kind != "log.record")
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
    )
    return BatchAnalysis(
        summary.id,
        summary.name,
        summary.wall_time_seconds,
        _lifecycle(events, edges, summary.wall_time_seconds),
        critical,
        _throughput(events, edges, summary.finished_at_ns),
        _bottlenecks(events, critical, summary.wall_time_seconds),
    )
