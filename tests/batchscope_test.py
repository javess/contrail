from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from runtime_tools import record_process
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.analysis import (
    BatchAnalysis,
    Bottleneck,
    CriticalPath,
    LifecyclePhase,
)
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.model import CausalEdge, Entity, Event, Execution, JsonValue
from runtime_tools.storage import RunpackWriter


def _event(
    event_id: str,
    kind: str,
    name: str,
    start: int,
    end: int,
    attributes: dict[str, JsonValue] | None = None,
) -> Event:
    return Event(event_id, kind, name, "worker", start, end, "test", None, None, attributes or {})


def _write_batch(path: Path) -> None:
    events = (
        _event("process", "process.run", "python", 0, 100_000_000),
        _event("run", "run", "pipeline", 5_000_000, 95_000_000),
        _event("compute", "stage", "compute", 10_000_000, 50_000_000),
        _event("task-a", "operation", "task-a", 10_000_000, 40_000_000),
        _event("task-b", "operation", "task-b", 20_000_000, 45_000_000),
        _event("drain", "stage", "drain", 50_000_000, 90_000_000, {"concurrency": 1}),
        _event("db", "client.request", "database.flush", 55_000_000, 85_000_000),
        _event(
            "progress-1",
            "progress",
            "progress",
            20_000_000,
            20_000_000,
            {"completed": 20, "total": 100},
        ),
        _event(
            "progress-2",
            "progress",
            "progress",
            60_000_000,
            60_000_000,
            {"completed": 60, "total": 100},
        ),
    )
    edges = (
        CausalEdge("process", "run", "parent", 1.0, {}),
        CausalEdge("run", "compute", "parent", 1.0, {}),
        CausalEdge("run", "drain", "parent", 1.0, {}),
        CausalEdge("compute", "task-a", "parent", 1.0, {}),
        CausalEdge("compute", "task-b", "parent", 1.0, {}),
        CausalEdge("drain", "db", "parent", 1.0, {}),
    )
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution("batch", "batch", 0, 100_000_000, (), str(path.parent), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        for event in events:
            writer.add_event(event)
        for edge in edges:
            writer.add_causal_edge(edge)


def test_batchscope_derives_overlap_aware_critical_path_and_throughput(tmp_path: Path) -> None:
    runpack = tmp_path / "batch.runpack"
    _write_batch(runpack)

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("compute", 0.04),
        ("drain", 0.04),
    ]
    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.1
    assert analysis.critical_path.parallel_slack_seconds == 0.0
    assert analysis.critical_path.event_ids[:2] == ("process", "run")
    assert analysis.critical_path.event_names[:2] == ("python", "pipeline")
    assert analysis.throughput is not None
    assert analysis.throughput.rate_per_second == 1000.0
    assert analysis.throughput.remaining == 40.0
    assert analysis.throughput.estimated_drain_seconds == 0.04
    assert analysis.throughput.compute_finished_at_ns == 50_000_000
    assert analysis.throughput.remaining_at_compute_completion == 80.0
    assert analysis.throughput.post_compute_seconds == 0.05
    assert analysis.throughput.post_compute_rate_per_second is None
    assert {item.classification for item in analysis.bottlenecks} == {"serialized_stage"}


def test_batchscope_scopes_compute_boundaries_to_the_progress_parent(tmp_path: Path) -> None:
    runpack = tmp_path / "scoped-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("scoped", "scoped", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("run-a", "run", "run-a", 0, 60_000_000),
                _event("compute-a", "stage", "compute", 10_000_000, 50_000_000),
                _event(
                    "progress-a",
                    "progress",
                    "progress",
                    40_000_000,
                    40_000_000,
                    {"completed": 50, "total": 100},
                ),
                _event(
                    "progress-b",
                    "progress",
                    "progress",
                    60_000_000,
                    60_000_000,
                    {"completed": 75, "total": 100},
                ),
                _event("run-b", "run", "run-b", 60_000_000, 100_000_000),
                _event("compute-b", "stage", "compute", 70_000_000, 90_000_000),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("run-a", "compute-a", "parent", 1.0, {}),
                CausalEdge("run-a", "progress-a", "parent", 1.0, {}),
                CausalEdge("run-a", "progress-b", "parent", 1.0, {}),
                CausalEdge("run-b", "compute-b", "parent", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.compute_finished_at_ns == 50_000_000
    assert analysis.throughput.post_compute_seconds == 0.01


def test_batchscope_lifecycle_does_not_double_count_nested_explicit_stages(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "nested-stages.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("nested", "nested", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("outer", "stage", "outer", 0, 100_000_000),
                _event("inner", "stage", "inner", 20_000_000, 80_000_000),
            )
        )
        writer.add_causal_edge(CausalEdge("outer", "inner", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("outer", 0.1)
    ]


def test_kubernetes_lifecycle_uses_the_uniquely_correlated_job(tmp_path: Path) -> None:
    runpack = tmp_path / "multiple-jobs.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job-a", "workload.job", "job-a", 10, 90),
                _event("pod-a", "workload.pod", "pod-a", 20, 80),
                _event("container-a", "workload.container", "container-a", 30, 70),
                _event("job-b", "workload.job", "job-b", 0, 100),
                _event("pod-b", "workload.pod", "pod-b", 1, 99),
                _event("container-b", "workload.container", "container-b", 2, 98),
                _event("trace-root", "operation", "trace-root", 30, 70),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("job-a", "pod-a", "owns", 1.0, {}),
                CausalEdge("pod-a", "container-a", "contains", 1.0, {}),
                CausalEdge("pod-a", "trace-root", "correlates", 1.0, {}),
                CausalEdge("job-b", "pod-b", "owns", 1.0, {}),
                CausalEdge("pod-b", "container-b", "contains", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("provisioning", 10 / 1_000_000_000),
        ("starting", 10 / 1_000_000_000),
        ("executing", 40 / 1_000_000_000),
        ("cleanup", 20 / 1_000_000_000),
    ]


def test_kubernetes_lifecycle_does_not_borrow_unrelated_container_intervals(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unrelated-container.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job-a", "workload.job", "job-a", 10, 90),
                _event("pod-a", "workload.pod", "pod-a", 20, 80),
                _event("job-b", "workload.job", "job-b", 0, 100),
                _event("pod-b", "workload.pod", "pod-b", 1, 99),
                _event("container-b", "workload.container", "container-b", 2, 98),
                _event("trace-root", "operation", "trace-root", 30, 70),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("job-a", "pod-a", "owns", 1.0, {}),
                CausalEdge("pod-a", "trace-root", "correlates", 1.0, {}),
                CausalEdge("job-b", "pod-b", "owns", 1.0, {}),
                CausalEdge("pod-b", "container-b", "contains", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("provisioning", 10 / 1_000_000_000)
    ]


def test_kubernetes_lifecycle_requires_job_to_pod_ownership(tmp_path: Path) -> None:
    runpack = tmp_path / "unowned-pod.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job", "workload.job", "job", 10, 90),
                _event("pod", "workload.pod", "unrelated-pod", 20, 80),
                _event("container", "workload.container", "container", 30, 70),
            )
        )
        writer.add_causal_edge(CausalEdge("pod", "container", "contains", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("executing", 100 / 1_000_000_000)
    ]


def test_batchscope_cli_emits_structured_json(tmp_path: Path) -> None:
    runpack = tmp_path / "batch.runpack"
    _write_batch(runpack)

    result = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.batchscope.cli",
            "inspect",
            str(runpack),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["name"] == "batch"
    assert payload["critical_path"]["certainty"] == "observed"
    assert payload["critical_path"]["event_ids"][:2] == ["process", "run"]
    assert payload["bottlenecks"][0]["classification"] == "serialized_stage"


def test_batchscope_labels_a_single_process_path_as_observed(tmp_path: Path) -> None:
    runpack = tmp_path / "local.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="local")

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "observed"


def test_critical_path_does_not_subtract_sequential_sibling_intervals(tmp_path: Path) -> None:
    runpack = tmp_path / "siblings.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("siblings", "siblings", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("root", "run", "root", 0, 100_000_000))
        writer.add_event(_event("first", "stage", "first", 0, 40_000_000))
        writer.add_event(_event("second", "stage", "second", 40_000_000, 90_000_000))
        writer.add_causal_edge(CausalEdge("root", "first", "parent", 1.0, {}))
        writer.add_causal_edge(CausalEdge("root", "second", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.1
    assert analysis.critical_path.parallel_slack_seconds == 0.0


def test_critical_path_separates_active_time_waiting_and_parallel_slack(tmp_path: Path) -> None:
    runpack = tmp_path / "waiting.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("waiting", "waiting", 0, 50_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("produce", "operation", "produce", 0, 10_000_000))
        writer.add_event(_event("consume", "operation", "consume", 30_000_000, 40_000_000))
        writer.add_causal_edge(CausalEdge("produce", "consume", "follows", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.04
    assert analysis.critical_path.active_seconds == 0.02
    assert analysis.critical_path.waiting_seconds == 0.02
    assert analysis.critical_path.parallel_slack_seconds == pytest.approx(0.01)
    report = render_analysis(analysis, "text")
    assert "active execution: 20.0ms" in report
    assert "causal waiting: 20.0ms" in report


def test_batchscope_text_report_bounds_repeated_sections() -> None:
    names = tuple(f"event-{index}" for index in range(101))
    analysis = BatchAnalysis(
        execution_id="execution",
        name="bounded",
        total_seconds=1.0,
        lifecycle=tuple(LifecyclePhase(name, 0.01, "explicit") for name in names),
        critical_path=CriticalPath(1.0, 1.0, 0.0, 0.0, names, names, "observed", False),
        throughput=None,
        bottlenecks=tuple(Bottleneck("serialized_stage", name, 0.9) for name in names),
    )

    report = render_analysis(analysis, "text")

    assert "event-99" in report
    assert "event-100" not in report
    assert report.count("1 additional items omitted from text output") == 3


def test_batchscope_json_report_rejects_non_finite_facts() -> None:
    analysis = BatchAnalysis("run", "run", float("nan"), (), None, None, ())

    with pytest.raises(ValueError, match="Out of range float values are not JSON compliant"):
        render_analysis(analysis, "json")


def test_batchscope_classifies_dominant_external_dependency(tmp_path: Path) -> None:
    runpack = tmp_path / "external.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("external", "external", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("root", "run", "root", 0, 100_000_000))
        writer.add_event(_event("database", "client.request", "database", 20_000_000, 80_000_000))
        writer.add_causal_edge(CausalEdge("root", "database", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert {item.classification for item in analysis.bottlenecks} == {"external_dependency"}


def test_batchscope_classifies_failed_scheduling_as_capacity_starvation(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "capacity.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("capacity", "capacity", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "pod", "worker", None, {}))
        writer.add_event(
            _event(
                "failed-scheduling",
                "kubernetes.event",
                "FailedScheduling",
                10_000_000,
                10_000_000,
                {"message": "0/4 nodes are available: insufficient cpu"},
            )
        )

    analysis = analyze_runpack(runpack)

    finding = next(
        item for item in analysis.bottlenecks if item.classification == "capacity_starvation"
    )
    assert finding.evidence == "1 Kubernetes FailedScheduling event indicates placement failure"
    assert finding.confidence == 0.85


def test_batchscope_classifies_a_dominant_operation_straggler(tmp_path: Path) -> None:
    runpack = tmp_path / "straggler.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("straggler", "straggler", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            _event(
                f"task-{index}",
                "operation",
                "task",
                index * 1_000_000,
                90_000_000 if index == 4 else 20_000_000,
            )
            for index in range(5)
        )

    analysis = analyze_runpack(runpack)

    finding = next(item for item in analysis.bottlenecks if item.classification == "straggler_tail")
    assert finding.evidence == ("task max duration 0.086s versus 0.019s median across 5 operations")
    assert finding.confidence == 0.8


def test_serialized_stage_uses_enclosing_logical_run_instead_of_process_startup(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "logical-window.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("logical", "logical", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("process", "process.run", "python", 0, 1_000_000_000))
        writer.add_event(_event("run", "run", "workload", 800_000_000, 900_000_000))
        writer.add_event(
            _event(
                "persist",
                "stage",
                "persist",
                850_000_000,
                890_000_000,
                {"concurrency": 1},
            )
        )

    analysis = analyze_runpack(runpack)

    assert {item.classification for item in analysis.bottlenecks} == {"serialized_stage"}


@pytest.mark.parametrize("concurrency", (True, 10**1000))
def test_serialized_stage_ignores_invalid_concurrency_values(
    tmp_path: Path, concurrency: int | bool
) -> None:
    runpack = tmp_path / "invalid-concurrency.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("invalid", "invalid", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "persist",
                "stage",
                "persist",
                0,
                100_000_000,
                {"concurrency": concurrency},
            )
        )

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "serialized_stage" for item in analysis.bottlenecks)


def test_batchscope_does_not_sum_parallel_side_branch_clients_as_a_bottleneck(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "parallel-clients.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("parallel", "parallel", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("root", "run", "root", 0, 100_000_000))
        writer.add_event(_event("compute", "operation", "compute", 0, 90_000_000))
        writer.add_causal_edge(CausalEdge("root", "compute", "parent", 1.0, {}))
        for index in range(60):
            event_id = f"client-{index:02}"
            writer.add_event(_event(event_id, "client.request", "parallel-client", 0, 1_000_000))
            writer.add_causal_edge(CausalEdge("root", event_id, "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.1
    assert not any(item.classification == "external_dependency" for item in analysis.bottlenecks)


def test_batchscope_calculates_observed_post_compute_drain_rate(tmp_path: Path) -> None:
    runpack = tmp_path / "drain.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("drain", "drain", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("compute", "stage", "compute", 0, 50_000_000))
        writer.add_event(
            _event(
                "progress-1",
                "progress",
                "progress",
                50_000_000,
                50_000_000,
                {"completed": 40, "total": 100},
            )
        )
        writer.add_event(
            _event(
                "progress-2",
                "progress",
                "progress",
                70_000_000,
                70_000_000,
                {"completed": 70, "total": 100},
            )
        )
        writer.add_event(
            _event(
                "progress-3",
                "progress",
                "progress",
                90_000_000,
                90_000_000,
                {"completed": 100, "total": 100},
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.remaining_at_compute_completion == 60.0
    assert analysis.throughput.post_compute_seconds == 0.05
    assert analysis.throughput.post_compute_rate_per_second == 1500.0


def test_completed_progress_needs_no_rate_to_estimate_zero_drain(tmp_path: Path) -> None:
    runpack = tmp_path / "completed.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("completed", "completed", 0, 10_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "progress",
                "progress",
                "progress",
                10_000_000,
                10_000_000,
                {"completed": 100, "total": 100},
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.remaining == 0.0
    assert analysis.throughput.rate_per_second is None
    assert analysis.throughput.estimated_drain_seconds == 0.0


def test_throughput_ignores_progress_integers_outside_float_range(tmp_path: Path) -> None:
    runpack = tmp_path / "oversized-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "progress",
                "progress",
                "progress",
                1,
                1,
                {"completed": 10**1000, "total": 10**1000},
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_does_not_round_large_integer_progress_to_complete(tmp_path: Path) -> None:
    runpack = tmp_path / "imprecise-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "progress",
                "progress",
                "progress",
                1,
                1,
                {"completed": 2**53, "total": 2**53 + 1},
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_omits_overflowed_rates_from_json(tmp_path: Path) -> None:
    runpack = tmp_path / "overflowed-rate.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event(
                    "first",
                    "progress",
                    "progress",
                    0,
                    0,
                    {"completed": 0.0, "total": sys.float_info.max},
                ),
                _event(
                    "last",
                    "progress",
                    "progress",
                    1,
                    1,
                    {"completed": sys.float_info.max, "total": sys.float_info.max},
                ),
            )
        )

    analysis = analyze_runpack(runpack)
    rendered = render_analysis(analysis, "json")

    assert analysis.throughput is not None
    assert analysis.throughput.rate_per_second is None
    assert "Infinity" not in rendered


def test_throughput_omits_overflowed_drain_estimates(tmp_path: Path) -> None:
    runpack = tmp_path / "overflowed-drain.runpack"
    finished_at_ns = (1 << 63) - 1
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "progress",
                "progress",
                0,
                finished_at_ns,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event(
                    "first",
                    "progress",
                    "progress",
                    0,
                    0,
                    {"completed": 0.0, "total": sys.float_info.max},
                ),
                _event(
                    "last",
                    "progress",
                    "progress",
                    finished_at_ns,
                    finished_at_ns,
                    {"completed": 1.0, "total": sys.float_info.max},
                ),
            )
        )

    throughput = analyze_runpack(runpack).throughput

    assert throughput is not None
    assert throughput.rate_per_second is not None
    assert throughput.estimated_drain_seconds is None


def test_throughput_rejects_invalid_progress_and_does_not_infer_across_resets(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "ambiguous-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("boolean", "progress", "progress", 0, 0, {"completed": True, "total": 10}),
                _event("first", "progress", "progress", 10, 10, {"completed": 8, "total": 10}),
                _event("reset", "progress", "progress", 20, 20, {"completed": 2, "total": 10}),
                _event("invalid", "progress", "progress", 30, 30, {"completed": 11, "total": 10}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.completed == 2
    assert analysis.throughput.total == 10
    assert analysis.throughput.remaining == 8
    assert analysis.throughput.rate_per_second is None
    assert analysis.throughput.estimated_drain_seconds is None


def test_throughput_rejects_conflicting_samples_at_the_same_timestamp(tmp_path: Path) -> None:
    runpack = tmp_path / "conflicting-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("first", "progress", "progress", 10, 10, {"completed": 2, "total": 10}),
                _event(
                    "conflict",
                    "progress",
                    "progress",
                    10,
                    10,
                    {"completed": 9, "total": 10},
                ),
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_collapses_identical_samples_at_the_same_timestamp(tmp_path: Path) -> None:
    runpack = tmp_path / "duplicate-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "progress",
                "progress",
                0,
                20_000_000,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event(
                    "first",
                    "progress",
                    "progress",
                    10_000_000,
                    10_000_000,
                    {"completed": 2, "total": 10},
                ),
                _event(
                    "second",
                    "progress",
                    "progress",
                    20_000_000,
                    20_000_000,
                    {"completed": 4, "total": 10},
                ),
                _event(
                    "duplicate",
                    "progress",
                    "progress",
                    20_000_000,
                    20_000_000,
                    {"completed": 4, "total": 10},
                ),
            )
        )

    throughput = analyze_runpack(runpack).throughput

    assert throughput is not None
    assert throughput.completed == 4
    assert throughput.rate_per_second == 200


def test_throughput_does_not_merge_progress_from_multiple_entities(tmp_path: Path) -> None:
    runpack = tmp_path / "multiple-progress-series.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entities(
            (
                Entity("worker-a", "worker", "worker-a", None, {}),
                Entity("worker-b", "worker", "worker-b", None, {}),
            )
        )
        writer.add_events(
            (
                Event(
                    "progress-a",
                    "progress",
                    "progress",
                    "worker-a",
                    10,
                    10,
                    "test",
                    None,
                    None,
                    {"completed": 5, "total": 10},
                ),
                Event(
                    "progress-b",
                    "progress",
                    "progress",
                    "worker-b",
                    20,
                    20,
                    "test",
                    None,
                    None,
                    {"completed": 6, "total": 10},
                ),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is None


def test_throughput_does_not_guess_between_multiple_parent_scopes(tmp_path: Path) -> None:
    runpack = tmp_path / "ambiguous-progress-parents.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("scope-a", "run", "scope-a", 0, 30),
                _event("scope-b", "run", "scope-b", 0, 30),
                _event("progress-a", "progress", "progress", 10, 10, {"completed": 5, "total": 10}),
            )
        )
        writer.add_causal_edges(
            CausalEdge(parent, "progress-a", "parent", 1.0, {}) for parent in ("scope-a", "scope-b")
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is None


def test_throughput_uses_the_progress_entity_compute_boundary(tmp_path: Path) -> None:
    runpack = tmp_path / "scoped-compute.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 100, (), str(tmp_path), 0, None, {})
        )
        writer.add_entities(
            (
                Entity("worker", "worker", "worker", None, {}),
                Entity("other", "service", "other", None, {}),
            )
        )
        writer.add_events(
            (
                Event("compute", "stage", "compute", "worker", 0, 40, "test", None, None, {}),
                Event(
                    "other-compute",
                    "stage",
                    "compute",
                    "other",
                    0,
                    80,
                    "test",
                    None,
                    None,
                    {},
                ),
                Event(
                    "progress-a",
                    "progress",
                    "progress",
                    "worker",
                    20,
                    20,
                    "test",
                    None,
                    None,
                    {"completed": 20, "total": 100},
                ),
                Event(
                    "progress-b",
                    "progress",
                    "progress",
                    "worker",
                    60,
                    60,
                    "test",
                    None,
                    None,
                    {"completed": 60, "total": 100},
                ),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.compute_finished_at_ns == 40
    assert analysis.throughput.remaining_at_compute_completion == 80


def test_clock_inconsistency_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "clock-skew.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("skew", "skew", 0, 120_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("parent", "run", "parent", 10_000_000, 100_000_000))
        writer.add_event(_event("child", "operation", "child", 0, 120_000_000))
        writer.add_causal_edge(CausalEdge("parent", "child", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.12
    assert analysis.critical_path.certainty == "inferred"


def test_reversed_non_parent_causality_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "reversed-cause.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("reversed", "reversed", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("cause", "operation", "cause", 50_000_000, 60_000_000))
        writer.add_event(_event("effect", "operation", "effect", 10_000_000, 20_000_000))
        writer.add_causal_edge(CausalEdge("cause", "effect", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"


def test_clock_uncertainty_can_cover_apparent_parent_skew(tmp_path: Path) -> None:
    runpack = tmp_path / "uncertain-clock.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("uncertain", "uncertain", 0, 100, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(Event("parent", "run", "parent", "worker", 10, 90, "test", 5, None, {}))
        writer.add_event(Event("child", "operation", "child", "worker", 5, 95, "test", 5, None, {}))
        writer.add_causal_edge(CausalEdge("parent", "child", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "observed"


def test_causal_cycle_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "cycle.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("cycle", "cycle", 0, 20_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("first", "operation", "first", 0, 10_000_000))
        writer.add_event(_event("second", "operation", "second", 10_000_000, 20_000_000))
        writer.add_causal_edge(CausalEdge("first", "second", "follows", 1.0, {}))
        writer.add_causal_edge(CausalEdge("second", "first", "follows", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.cycle_detected is True
    assert analysis.critical_path.certainty == "inferred"


def test_low_confidence_causal_edge_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "uncertain.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("uncertain", "uncertain", 0, 20_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("first", "operation", "first", 0, 10_000_000))
        writer.add_event(_event("second", "operation", "second", 10_000_000, 20_000_000))
        writer.add_causal_edge(CausalEdge("first", "second", "inferred", 0.4, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"


def test_cross_clock_domain_critical_path_is_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "cross-clock.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("cross-clock", "cross-clock", 0, 20_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            Event(
                "first",
                "operation",
                "first",
                "worker",
                0,
                10_000_000,
                "producer-clock",
                None,
                None,
                {},
            )
        )
        writer.add_event(
            Event(
                "second",
                "operation",
                "second",
                "worker",
                10_000_000,
                20_000_000,
                "consumer-clock",
                None,
                None,
                {},
            )
        )
        writer.add_causal_edge(CausalEdge("first", "second", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.02
    assert analysis.critical_path.certainty == "inferred"


def test_instant_events_preserve_causal_waiting_on_the_critical_path(tmp_path: Path) -> None:
    runpack = tmp_path / "instants.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("instants", "instants", 0, 50_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("publish", "event", "publish", 10_000_000, 10_000_000))
        writer.add_event(_event("consume", "event", "consume", 40_000_000, 40_000_000))
        writer.add_causal_edge(CausalEdge("publish", "consume", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.event_ids == ("publish", "consume")
    assert analysis.critical_path.duration_seconds == 0.03
    assert analysis.critical_path.active_seconds == 0.0
    assert analysis.critical_path.waiting_seconds == 0.03
    assert analysis.critical_path.certainty == "observed"


def test_untimed_events_preserve_causality_but_make_the_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "partial-timing.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "partial-timing",
                "partial-timing",
                0,
                50_000_000,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("produce", "operation", "produce", 0, 10_000_000))
        writer.add_event(
            Event(
                "handoff",
                "event",
                "handoff",
                "worker",
                None,
                None,
                "unknown",
                None,
                None,
                {},
            )
        )
        writer.add_event(_event("consume", "operation", "consume", 30_000_000, 40_000_000))
        writer.add_causal_edge(CausalEdge("produce", "handoff", "causes", 1.0, {}))
        writer.add_causal_edge(CausalEdge("handoff", "consume", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.event_ids == ("produce", "handoff", "consume")
    assert analysis.critical_path.duration_seconds == 0.04
    assert analysis.critical_path.active_seconds == 0.02
    assert analysis.critical_path.waiting_seconds == 0.02
    assert analysis.critical_path.certainty == "inferred"


def test_batchscope_handles_causal_chains_beyond_python_recursion_limit(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "long-chain.runpack"
    chain_length = 10_000
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "long-chain",
                "long-chain",
                0,
                chain_length,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
    with sqlite3.connect(runpack) as connection:
        connection.executemany(
            """
            INSERT INTO events(
                id, kind, name, entity_id, started_at_ns, finished_at_ns,
                clock_domain, uncertainty_ns, sequence, attributes_json
            ) VALUES (?, 'operation', ?, 'worker', ?, ?, 'test', NULL, ?, '{}')
            """,
            (
                (f"event-{index}", f"event-{index}", index, index + 1, index)
                for index in range(chain_length)
            ),
        )
        connection.executemany(
            """
            INSERT INTO causal_edges(
                source_event_id, target_event_id, kind, confidence, attributes_json
            ) VALUES (?, ?, 'parent', 1.0, '{}')
            """,
            ((f"event-{index}", f"event-{index + 1}") for index in range(chain_length - 1)),
        )

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == pytest.approx(chain_length / 1e9)
    assert len(analysis.critical_path.event_ids) == chain_length
