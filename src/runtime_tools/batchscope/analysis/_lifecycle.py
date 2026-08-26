"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median

from runtime_tools.batchscope.analysis._profile_models import (
    Bottleneck,
    CriticalPath,
    LifecyclePhase,
    Throughput,
    _ProgressSample,
)
from runtime_tools.model import CausalEdge, Event


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
