from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

import runtime_tools.rundiff.report as report_module
from runtime_tools import record_process
from runtime_tools.inspect import inspect_runpack, render_summary
from runtime_tools.model import CausalEdge, Entity, Event, Execution, JsonValue, Measurement
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter
from tests.runpack_support import write_runpack

_STDOUT_IDENTITY = "a" * 64
_STDERR_IDENTITY = "b" * 64


def _write_runpack(path: Path, *, candidate: bool) -> None:
    started_at_ns = 1_000_000
    finished_at_ns = 21_000_000 if candidate else 11_000_000
    entities = [
        Entity("gateway", "service", "gateway", None, {}),
        Entity("database", "service", "database", None, {}),
    ]
    if candidate:
        entities.append(Entity("metadata", "service", "metadata", None, {}))
    events = [
        Event(
            "root",
            "server.request",
            "GET /items",
            "gateway",
            started_at_ns,
            finished_at_ns,
            "test",
            None,
            None,
            {},
        )
    ]
    database_count = 3 if candidate else 1
    for index in range(database_count):
        events.append(
            Event(
                f"db-{index}",
                "client.request",
                "SELECT items",
                "database",
                2_000_000 + index,
                3_000_000 + index,
                "test",
                None,
                index,
                {"error": candidate and index == 0},
            )
        )
    if candidate:
        events.append(
            Event(
                "metadata-0",
                "client.request",
                "metadata.lookup",
                "metadata",
                4_000_000,
                5_000_000,
                "test",
                None,
                4,
                {},
            )
        )

    write_runpack(
        path,
        Execution(
            "candidate" if candidate else "baseline",
            "candidate" if candidate else "baseline",
            started_at_ns,
            finished_at_ns,
            (),
            str(path.parent),
            0,
            None,
            {
                "output": {
                    "stdout": {"bytes": 7, "sha256": _STDOUT_IDENTITY},
                    "stderr": {"bytes": 0, "sha256": _STDERR_IDENTITY},
                }
            },
        ),
        entities=entities,
        events=events,
        causal_edges=(CausalEdge("root", event.id, "parent", 1.0, {}) for event in events[1:]),
        measurements=(
            Measurement(
                "process.memory.peak",
                150 * 1024**2 if candidate else 100 * 1024**2,
                "By",
                finished_at_ns,
                "gateway",
                {},
            ),
            Measurement(
                "process.cpu.user",
                2.0 if candidate else 1.0,
                "s",
                finished_at_ns,
                "gateway",
                {},
            ),
            Measurement(
                "process.cpu.system",
                1.0 if candidate else 0.5,
                "s",
                finished_at_ns,
                "gateway",
                {},
            ),
        ),
    )


def _write_cpu_runpack(path: Path, user: float, system: float) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(path.stem, path.stem, 0, 1, (), str(path.parent), 0, None, {})
        )
        writer.add_measurements(
            (
                Measurement("process.cpu.user", user, "s", 1, None, {}),
                Measurement("process.cpu.system", system, "s", 1, None, {}),
            )
        )


def test_rundiff_uses_one_resolved_baseline_across_all_analysis_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validated = tmp_path / "validated.runpack"
    replacement = tmp_path / "replacement.runpack"
    candidate = tmp_path / "candidate.runpack"
    requested = tmp_path / "requested.runpack"
    _write_runpack(validated, candidate=False)
    _write_runpack(replacement, candidate=True)
    shutil.copyfile(validated, candidate)
    real_resolve = Path.resolve
    resolution_count = 0

    def retarget_after_validation(path: Path, strict: bool = False) -> Path:
        nonlocal resolution_count
        if path == requested:
            resolution_count += 1
            return validated if resolution_count == 1 else replacement
        return real_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", retarget_after_validation)

    diff = compare_runpacks(requested, candidate)

    assert diff.match_level == "exact"
    assert diff.operation_count_changes == ()
    assert resolution_count == 1


def test_rundiff_uses_one_snapshot_per_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    shutil.copyfile(baseline, candidate)
    real_close = RunpackReader.close
    mutated = False

    def mutate_candidate_after_snapshot(reader: RunpackReader) -> None:
        nonlocal mutated
        real_close(reader)
        if reader.path == candidate.resolve() and not mutated:
            mutated = True
            with RunpackWriter.open_existing(candidate) as writer:
                writer.add_event(
                    Event(
                        "late-error",
                        "client.request",
                        "late error",
                        "gateway",
                        4_000_000,
                        5_000_000,
                        "test",
                        None,
                        99,
                        {"error": True},
                    )
                )

    monkeypatch.setattr(RunpackReader, "close", mutate_candidate_after_snapshot)

    diff = compare_runpacks(baseline, candidate)

    assert diff.outcome == "equivalent"
    assert diff.operation_errors_equivalent is True
    assert diff.operation_count_changes == ()
    with sqlite3.connect(candidate) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 3


def test_compare_runpacks_finds_timing_cardinality_and_dependency_changes(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)

    diff = compare_runpacks(baseline, candidate)

    assert diff.match_level == "aggregate"
    assert diff.outcome == "different"
    assert diff.exit_code_equivalent is True
    assert diff.output_equivalent is True
    assert diff.stderr_equivalent is True
    assert diff.operation_errors_equivalent is False
    assert diff.wall_time.baseline == 0.01
    assert diff.wall_time.candidate == 0.02
    assert diff.wall_time.percent == 100.0
    assert diff.cpu_time.baseline == 1.5
    assert diff.cpu_time.candidate == 3.0
    assert diff.cpu_time.percent == 100.0
    assert diff.critical_path.baseline == 0.01
    assert diff.critical_path.candidate == 0.02
    assert diff.critical_path.percent == 100.0
    assert diff.baseline_critical_path_certainty == "observed"
    assert diff.candidate_critical_path_certainty == "observed"
    assert diff.peak_memory.percent == 50.0
    assert [
        (change.entity_kind, change.entity_name, change.baseline, change.candidate)
        for change in diff.entity_count_changes
    ] == [("service", "metadata", 0, 1)]
    assert [
        (change.entity_name, change.operation_name, change.baseline, change.candidate)
        for change in diff.operation_count_changes
    ] == [
        ("database", "SELECT items", 1, 3),
        ("metadata", "metadata.lookup", 0, 1),
    ]
    assert [
        (change.entity_name, change.operation_name, change.baseline, change.candidate)
        for change in diff.operation_error_count_changes
    ] == [("database", "SELECT items", 0, 1)]
    assert [
        (
            change.entity_name,
            change.operation_name,
            change.baseline_seconds,
            change.candidate_seconds,
        )
        for change in diff.operation_duration_changes
    ] == [
        ("gateway", "GET /items", 0.01, 0.02),
        ("database", "SELECT items", 0.001, 0.003),
        ("metadata", "metadata.lookup", 0.0, 0.001),
    ]
    assert [
        (change.entity_name, change.operation_name, change.baseline, change.candidate)
        for change in diff.operation_concurrency_changes
    ] == [
        ("database", "SELECT items", 1, 3),
        ("metadata", "metadata.lookup", 0, 1),
    ]
    assert [
        (change.source_name, change.target_name, change.change_kind)
        for change in diff.edge_count_changes
    ] == [
        ("gateway", "metadata", "new"),
        ("gateway", "database", "changed"),
    ]


def test_compare_runpacks_marks_repeated_semantic_shapes_as_structural(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process((sys.executable, "-c", "print('same')"), baseline, name="baseline")
    record_process((sys.executable, "-c", "print('same')"), candidate, name="candidate")

    diff = compare_runpacks(baseline, candidate)

    assert diff.baseline.id != diff.candidate.id
    assert diff.match_level == "structural"


def test_compare_runpacks_does_not_call_changed_internal_causality_structural(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, target in ((baseline, "fetch"), (candidate, "write")):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 3, (), str(tmp_path), 0, None, {})
            )
            writer.add_entity(Entity("worker", "service", "worker", None, {}))
            for index, name in enumerate(("parse", "fetch", "write")):
                writer.add_event(
                    Event(
                        name,
                        "operation",
                        name,
                        "worker",
                        index,
                        index + 1,
                        "test",
                        None,
                        index,
                        {},
                    )
                )
            writer.add_causal_edge(CausalEdge("parse", target, "parent", 1.0, {}))

    diff = compare_runpacks(baseline, candidate)

    assert diff.match_level == "aggregate"


def test_compare_runpacks_does_not_call_changed_entity_parentage_structural(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, parent in ((baseline, "pool-a"), (candidate, "pool-b")):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 1, (), str(tmp_path), 0, None, {})
            )
            writer.add_entity(Entity("pool-a", "pool", "a", None, {}))
            writer.add_entity(Entity("pool-b", "pool", "b", None, {}))
            writer.add_entity(Entity("worker", "worker", "worker", parent, {}))
            writer.add_event(
                Event("work", "operation", "work", "worker", 0, 1, "test", None, 0, {})
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.match_level == "aggregate"


def test_compare_runpacks_omits_unrepresentable_cpu_percentages(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_cpu_runpack(baseline, 5e-324, 0.0)
    _write_cpu_runpack(candidate, 1.0, 0.0)

    diff = compare_runpacks(baseline, candidate)
    rendered = render_diff(diff, "json")

    assert diff.cpu_time.percent is None
    assert "Infinity" not in rendered
    assert json.loads(rendered)["cpu_time"]["percent"] is None


def test_compare_runpacks_omits_overflowed_cpu_totals(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_cpu_runpack(baseline, 1.0, 0.0)
    _write_cpu_runpack(candidate, sys.float_info.max, sys.float_info.max)

    diff = compare_runpacks(baseline, candidate)

    assert diff.cpu_time.baseline == 1.0
    assert diff.cpu_time.candidate is None
    assert diff.cpu_time.percent is None


def test_reader_aggregates_peer_dependencies_from_client_rows(tmp_path: Path) -> None:
    runpack = tmp_path / "peer-counts.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("peer-counts", "peer-counts", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "service", "worker", None, {}))
        writer.add_events(
            Event(
                f"call-{index}",
                "client.request",
                "call",
                "worker",
                0,
                1,
                "test",
                None,
                index,
                {"peer.service": "database"},
            )
            for index in range(3)
        )
        invalid_peers: tuple[str | int, ...] = ("", 7)
        writer.add_events(
            Event(
                f"missing-peer-{index}",
                "client.request",
                "call",
                "worker",
                0,
                1,
                "test",
                None,
                index + 4,
                {"peer.service": peer},
            )
            for index, peer in enumerate(invalid_peers)
        )
    with RunpackReader(runpack) as reader:
        assert reader.peer_service_edge_counts() == {
            ("service", "worker", "service", "database", "calls"): 3
        }


def test_compare_runpacks_deduplicates_peer_hint_for_an_explicit_call(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    cases: tuple[tuple[Path, dict[str, JsonValue]], ...] = (
        (baseline, {}),
        (candidate, {"peer.service": "database"}),
    )
    for path, attributes in cases:
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 10, (), str(tmp_path), 0, None, {})
            )
            writer.add_entities(
                (
                    Entity("api", "service", "api", None, {}),
                    Entity("database", "service", "database", None, {}),
                )
            )
            writer.add_events(
                (
                    Event(
                        "request",
                        "client.request",
                        "query",
                        "api",
                        1,
                        9,
                        "test",
                        None,
                        0,
                        attributes,
                    ),
                    Event(
                        "handler",
                        "server.request",
                        "query",
                        "database",
                        2,
                        8,
                        "test",
                        None,
                        0,
                        {},
                    ),
                )
            )
            writer.add_causal_edge(CausalEdge("request", "handler", "calls", 1.0, {}))

    diff = compare_runpacks(baseline, candidate)

    assert diff.match_level == "structural"
    assert diff.edge_count_changes == ()
    with RunpackReader(candidate) as reader:
        assert reader.peer_service_edge_counts() == {}


def test_compare_runpacks_keeps_edges_between_same_named_entity_instances(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, add_edge in ((baseline, False), (candidate, True)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 10, (), str(tmp_path), 0, None, {})
            )
            writer.add_entities(
                (
                    Entity("api-a", "service", "api", None, {"service.instance.id": "a"}),
                    Entity("api-b", "service", "api", None, {"service.instance.id": "b"}),
                )
            )
            writer.add_events(
                (
                    Event(
                        "request", "client.request", "call", "api-a", 1, 9, "test", None, None, {}
                    ),
                    Event(
                        "handler", "server.request", "handle", "api-b", 2, 8, "test", None, None, {}
                    ),
                )
            )
            if add_edge:
                writer.add_causal_edge(CausalEdge("request", "handler", "parent", 1.0, {}))

    diff = compare_runpacks(baseline, candidate)

    assert [
        (change.source_name, change.target_name, change.baseline, change.candidate)
        for change in diff.edge_count_changes
    ] == [("api", "api", 0, 1)]


def test_reader_keeps_observed_concurrency_within_clock_domains(tmp_path: Path) -> None:
    runpack = tmp_path / "concurrency.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("concurrency", "concurrency", 0, 10, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "service", "worker", None, {}))
        writer.add_events(
            (
                Event("a", "stage", "work", "worker", 1, 9, "clock-a", None, None, {}),
                Event("b", "stage", "work", "worker", 2, 8, "clock-b", None, None, {}),
            )
        )

    with RunpackReader(runpack) as reader:
        concurrency = reader.operation_max_concurrency()

    assert concurrency == {("service", "worker", "stage", "work"): 1}


def test_reader_keeps_concurrency_unknown_without_a_clock_domain(tmp_path: Path) -> None:
    runpack = tmp_path / "unknown-concurrency.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("concurrency", "concurrency", 0, 10, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "service", "worker", None, {}))
        writer.add_events(
            (
                Event("a", "stage", "work", "worker", 1, 9, None, None, None, {}),
                Event("b", "stage", "work", "worker", 2, 8, None, None, None, {}),
            )
        )

    with RunpackReader(runpack) as reader:
        concurrency = reader.operation_max_concurrency()

    assert concurrency == {}


def test_rundiff_text_report_and_cli_json_share_outcome(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)

    text_report = render_diff(compare_runpacks(baseline, candidate), "text")
    command = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "compare",
            str(baseline),
            str(candidate),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert "10.0ms → 20.0ms (+100.0%)" in text_report
    assert "CPU time\n  1.500s → 3.000s (+100.0%)" in text_report
    assert "Critical path (observed → observed)\n  10.0ms → 20.0ms (+100.0%)" in text_report
    assert "database :: SELECT items [client.request]" in text_report
    assert "Entity changes\n  metadata [service]: 0 → 1 (added)" in text_report
    assert "Aggregate operation duration changes" in text_report
    assert "Failed operation changes" in text_report
    assert "Observed max concurrency changes" in text_report
    assert "database :: SELECT items [client.request]\n     1 → 3 (+200.0%)" in text_report
    assert "gateway :: GET /items [server.request]" in text_report
    assert "gateway → metadata [parent]: 0 → 1" in text_report
    assert command.returncode == 0
    payload = json.loads(command.stdout)
    assert payload["document_type"] == "rundiff.compare"
    assert payload["format_version"] == "2"
    assert payload["outcome"] == "different"


def test_rundiff_cli_normalizes_overlong_runpack_paths() -> None:
    compared = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "compare",
            "a" * 5000,
            "candidate",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert compared.returncode == 2
    assert compared.stdout == ""
    assert compared.stderr.startswith("rundiff: could not resolve runpack path: ")
    assert "Traceback" not in compared.stderr


@pytest.mark.parametrize(
    ("outcome", "expected_report"),
    (
        ("different", '"outcome": "different"'),
        ("unknown", '"outcome": "unknown"'),
    ),
)
def test_rundiff_cli_can_require_an_equivalent_behavioral_outcome(
    tmp_path: Path, outcome: str, expected_report: str
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    if outcome == "different":
        _write_runpack(baseline, candidate=False)
        _write_runpack(candidate, candidate=True)
    else:
        for path in (baseline, candidate):
            with RunpackWriter(path) as writer:
                writer.add_execution(
                    Execution(path.stem, path.stem, 0, 1, (), str(tmp_path), 0, None, {})
                )

    compared = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "compare",
            str(baseline),
            str(candidate),
            "--format",
            "json",
            "--require-equivalent-outcome",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert compared.returncode == 1
    assert expected_report in compared.stdout
    assert compared.stderr == ""


def test_rundiff_cli_outcome_gate_ignores_resource_and_structural_changes(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    with sqlite3.connect(candidate) as connection:
        connection.execute("UPDATE events SET attributes_json = '{}' WHERE attributes_json != '{}'")

    compared = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "compare",
            str(baseline),
            str(candidate),
            "--require-equivalent-outcome",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert compared.returncode == 0
    assert "Outcome\n  equivalent" in compared.stdout
    assert "Entity changes" in compared.stdout
    assert "Runtime\n  10.0ms → 20.0ms (+100.0%)" in compared.stdout


def test_rundiff_text_hides_submillisecond_duration_noise_but_json_retains_it(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, duration_ns in ((baseline, 100_000), (candidate, 101_000)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 1_000_000, (), str(tmp_path), 0, None, {})
            )
            writer.add_entity(Entity("worker", "service", "worker", None, {}))
            writer.add_event(
                Event("work", "operation", "work", "worker", 0, duration_ns, "test", None, 0, {})
            )

    diff = compare_runpacks(baseline, candidate)
    text_report = render_diff(diff, "text")
    json_report = json.loads(render_diff(diff, "json"))

    assert "Aggregate operation duration changes" in text_report
    assert "1 change with delta below 1.0ms omitted from text output" in text_report
    assert "No entity, structural, error, concurrency, duration" not in text_report
    assert "100.0µs → 101.0µs" in text_report
    assert len(json_report["operation_duration_changes"]) == 1


def test_rundiff_text_counts_hidden_duration_changes_alongside_visible_rows(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, durations in (
        (baseline, (100_000, 1_000_000)),
        (candidate, (101_000, 3_000_000)),
    ):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 10_000_000, (), str(tmp_path), 0, None, {})
            )
            writer.add_entity(Entity("worker", "service", "worker", None, {}))
            writer.add_events(
                Event(
                    f"work-{index}",
                    "operation",
                    f"work-{index}",
                    "worker",
                    0,
                    duration,
                    "test",
                    None,
                    index,
                    {},
                )
                for index, duration in enumerate(durations)
            )

    diff = compare_runpacks(baseline, candidate)
    text_report = render_diff(diff, "text")
    json_report = json.loads(render_diff(diff, "json"))

    assert "1 change with delta below 1.0ms omitted from text output" in text_report
    assert "1.0ms → 3.0ms (+200.0%)" in text_report
    assert len(json_report["operation_duration_changes"]) == 2


def test_rundiff_text_report_bounds_each_change_section(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    diff = compare_runpacks(baseline, candidate)
    monkeypatch.setattr(report_module, "MAX_TEXT_SECTION_ITEMS", 1)

    report = render_diff(diff, "text")

    assert "… 1 additional items omitted from text output" in report
    assert len(json.loads(render_diff(diff, "json"))["operation_count_changes"]) == 2


def test_rundiff_cli_records_named_alias_and_resolves_it_for_comparison(tmp_path: Path) -> None:
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "record",
            "baseline",
            "--output",
            "custom-baseline.runpack",
            "--",
            sys.executable,
            "-c",
            "print('result')",
        ),
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    compared = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "compare",
            "custom-baseline.runpack",
            "custom-baseline.runpack",
        ),
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert recorded.stdout == "result\n"
    assert (tmp_path / "custom-baseline.runpack").is_file()
    with RunpackReader(tmp_path / "custom-baseline.runpack") as reader:
        capture = reader.execution().metadata["capture"]
    assert isinstance(capture, dict)
    assert capture["worker"] == {
        "format_version": 1,
        "mode": "separate-process",
        "client_disconnected": False,
    }
    jobs = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "job",
            "list",
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert jobs.returncode == 0
    job = json.loads(jobs.stdout)["jobs"][0]
    assert job["operation"] == "rundiff record"
    assert job["state"] == "complete"
    assert job["artifacts"] == [str(tmp_path / "custom-baseline.runpack")]
    assert compared.returncode == 0
    assert "matching:  exact" in compared.stdout
    assert "Outcome\n  equivalent" in compared.stdout
    assert (
        "No entity, structural, error, concurrency, duration, or operation-count changes."
        in compared.stdout
    )


def test_rundiff_record_can_detach_and_publish_its_named_artifact(tmp_path: Path) -> None:
    output = tmp_path / "detached-baseline.runpack"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    launched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "record",
            "baseline",
            "--detach",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "print('detached rundiff')",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    job_id = next(
        line.removeprefix("id:     ")
        for line in launched.stdout.splitlines()
        if line.startswith("id:     ")
    )
    waited = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "job",
            "wait",
            job_id,
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=10,
    )

    assert launched.returncode == 0
    assert f"status: runtime job status {job_id}" in launched.stdout
    assert f"wait:   runtime job wait {job_id}" in launched.stdout
    assert f"output: runtime job output {job_id}" in launched.stdout
    assert f"follow: runtime job output {job_id} --follow" in launched.stdout
    assert waited.returncode == 0
    job = json.loads(waited.stdout)["job"]
    assert job["operation"] == "rundiff record"
    assert job["detached"] is True
    assert job["artifacts"] == [str(output)]
    assert output.is_file()


def test_rundiff_record_supports_the_shared_process_capture_level(tmp_path: Path) -> None:
    output = tmp_path / "process.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "record",
            "process",
            "--capture-level",
            "process",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(0.22)",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    with RunpackReader(output) as reader:
        capture = reader.execution().metadata["capture"]
    assert isinstance(capture, dict)
    assert capture["level"] == "process"
    assert isinstance(capture["process_observer"], dict)


def test_rundiff_record_rejects_an_output_limit_without_output_capture(tmp_path: Path) -> None:
    output = tmp_path / "ignored-limit.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "record",
            "ignored-limit",
            "--output",
            str(output),
            "--output-limit-bytes",
            "4",
            "--",
            sys.executable,
            "-c",
            "pass",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 2
    assert recorded.stdout == ""
    assert recorded.stderr == "rundiff: --output-limit-bytes requires --include-output\n"
    assert "Traceback" not in recorded.stderr
    assert not output.exists()


def test_rundiff_record_uses_an_explicit_working_directory(tmp_path: Path) -> None:
    working_directory = tmp_path / "work"
    working_directory.mkdir()
    output = tmp_path / "cwd.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "record",
            "cwd",
            "--cwd",
            str(working_directory),
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "from pathlib import Path; print(Path.cwd().name)",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert recorded.stdout == "work\n"
    with RunpackReader(output) as reader:
        assert reader.execution().working_directory == str(working_directory)


def test_rundiff_record_identifies_custom_environment_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "environment.runpack"
    monkeypatch.setenv("CONTRAIL_TEST_FEATURE_MODE", "experimental")

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "record",
            "environment",
            "--identify-env",
            "CONTRAIL_TEST_FEATURE_MODE",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "pass",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    with RunpackReader(output) as reader:
        environment = reader.execution().metadata["environment"]
    assert isinstance(environment, dict)
    identities = environment["selected_value_sha256"]
    assert isinstance(identities, dict)
    assert identities["CONTRAIL_TEST_FEATURE_MODE"] == hashlib.sha256(b"experimental").hexdigest()


def test_rundiff_record_normalizes_child_signal_exit_status(tmp_path: Path) -> None:
    output = tmp_path / "signaled.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "record",
            "signaled",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import os, signal; os.kill(os.getpid(), signal.SIGTERM)",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 128 + signal.SIGTERM
    with RunpackReader(output) as reader:
        assert reader.execution().exit_code == -signal.SIGTERM


def test_rundiff_alias_resolution_ignores_same_named_directories(tmp_path: Path) -> None:
    runpack = tmp_path / "baseline.runpack"
    _write_runpack(runpack, candidate=False)
    (tmp_path / "baseline").mkdir()

    compared = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.rundiff.cli",
            "compare",
            "baseline",
            "baseline",
        ),
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert compared.returncode == 0
    assert "Outcome\n  equivalent" in compared.stdout


def test_reader_aggregates_only_explicit_operation_error_evidence(tmp_path: Path) -> None:
    runpack = tmp_path / "errors.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("errors", "errors", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "service", "worker", None, {}))
        writer.add_events(
            (
                Event(
                    "flag", "operation", "work", "worker", 0, 1, "test", None, None, {"error": True}
                ),
                Event(
                    "status",
                    "operation",
                    "work",
                    "worker",
                    0,
                    1,
                    "test",
                    None,
                    None,
                    {"otel.status.code": "STATUS_CODE_ERROR"},
                ),
                Event(
                    "type",
                    "operation",
                    "work",
                    "worker",
                    0,
                    1,
                    "test",
                    None,
                    None,
                    {"error.type": "TimeoutError"},
                ),
                Event(
                    "ok",
                    "operation",
                    "work",
                    "worker",
                    0,
                    1,
                    "test",
                    None,
                    None,
                    {"otel.status.code": "STATUS_CODE_OK"},
                ),
            )
        )

    with RunpackReader(runpack) as reader:
        counts = reader.operation_error_counts()

    assert counts == {("service", "worker", "operation", "work"): 3}


def test_compare_runpacks_treats_changed_stderr_as_different_behavior(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process(
        (sys.executable, "-c", "import sys; print('same'); print('before', file=sys.stderr)"),
        baseline,
        name="baseline",
    )
    record_process(
        (sys.executable, "-c", "import sys; print('same'); print('after', file=sys.stderr)"),
        candidate,
        name="candidate",
    )

    diff = compare_runpacks(baseline, candidate)

    assert diff.exit_code_equivalent is True
    assert diff.output_equivalent is True
    assert diff.stderr_equivalent is False
    assert diff.outcome == "different"
    assert "stderr:      different" in render_diff(diff, "text")


def test_compare_runpacks_reports_signaled_exit_statuses(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, exit_code in ((baseline, 0), (candidate, -signal.SIGTERM)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 1, (), str(tmp_path), exit_code, None, {})
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.baseline.exit_code == 0
    assert diff.candidate.exit_code == -signal.SIGTERM
    assert diff.exit_code_equivalent is False
    assert "exit status: exit 0 → signal 15 (different)" in render_diff(diff, "text")
    payload = json.loads(render_diff(diff, "json"))
    assert payload["baseline"]["exit_code"] == 0
    assert payload["candidate"]["exit_code"] == -signal.SIGTERM


def test_compare_runpacks_reports_selected_environment_drift_without_hashes(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    environments = (
        (
            baseline,
            {
                "CI": "a" * 64,
                "LANG": "b" * 64,
                "TZ": "c" * 64,
            },
        ),
        (
            candidate,
            {
                "LANG": "b" * 64,
                "PYTHONHASHSEED": "d" * 64,
                "TZ": "e" * 64,
            },
        ),
    )
    for path, selected in environments:
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    path.stem,
                    path.stem,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    cast(Any, {"environment": {"selected_value_sha256": selected}}),
                )
            )

    diff = compare_runpacks(baseline, candidate)
    report = render_diff(diff, "text")

    assert [(change.variable, change.change_kind) for change in diff.environment_changes] == [
        ("CI", "removed"),
        ("PYTHONHASHSEED", "added"),
        ("TZ", "changed"),
    ]
    assert "Environment changes" in report
    assert "  PYTHONHASHSEED: added" in report
    assert "c" * 64 not in report
    assert json.loads(render_diff(diff, "json"))["environment_changes"][2] == {
        "baseline_present": True,
        "candidate_present": True,
        "change_kind": "changed",
        "variable": "TZ",
    }


def test_compare_runpacks_does_not_claim_environment_drift_without_evidence(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, metadata in (
        (baseline, {}),
        (
            candidate,
            {"environment": {"selected_value_sha256": {"CI": "a" * 64}}},
        ),
    ):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    path.stem,
                    path.stem,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    cast(Any, metadata),
                )
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.environment_changes == ()
    assert "Environment changes" not in render_diff(diff, "text")


def test_compare_runpacks_normalizes_environment_identity_case(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, identity in ((baseline, "A" * 64), (candidate, "a" * 64)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    path.stem,
                    path.stem,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {"environment": {"selected_value_sha256": {"CI": identity}}},
                )
            )

    assert compare_runpacks(baseline, candidate).environment_changes == ()


def test_compare_runpacks_rejects_malformed_environment_identities(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, identity in ((baseline, "not-a-sha256"), (candidate, "a" * 64)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    path.stem,
                    path.stem,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {"environment": {"selected_value_sha256": {"CI": identity}}},
                )
            )

    with pytest.raises(RunpackError, match="invalid selected environment identity: CI"):
        compare_runpacks(baseline, candidate)


def test_compare_runpacks_keeps_incomplete_output_identity_unknown(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, pipe_open in ((baseline, True), (candidate, False)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    path.stem,
                    path.stem,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {
                        "output": {
                            "stdout": {
                                "bytes": 4,
                                "sha256": _STDOUT_IDENTITY,
                                "pipe_open_after_exit": pipe_open,
                            },
                            "stderr": {"bytes": 0, "sha256": _STDERR_IDENTITY},
                        }
                    },
                )
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.output_equivalent is None
    assert diff.stderr_equivalent is True
    assert diff.outcome == "unknown"
    assert diff.baseline_incomplete_streams == ("stdout",)
    assert diff.candidate_incomplete_streams == ()
    assert "baseline: incomplete stdout identity" in render_diff(diff, "text")


def test_compare_runpacks_warns_about_relay_failures_without_changing_equivalence(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, stdout_error, stderr_error in (
        (baseline, "BrokenPipeError: consumer\x1b[31m closed", None),
        (candidate, None, "OSError: terminal closed"),
    ):
        stdout: dict[str, Any] = {"bytes": 4, "sha256": _STDOUT_IDENTITY}
        stderr: dict[str, Any] = {"bytes": 0, "sha256": _STDERR_IDENTITY}
        if stdout_error is not None:
            stdout["relay_error"] = stdout_error
        if stderr_error is not None:
            stderr["relay_error"] = stderr_error
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    path.stem,
                    path.stem,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {"output": {"stdout": stdout, "stderr": stderr}},
                )
            )

    diff = compare_runpacks(baseline, candidate)
    report = render_diff(diff, "text")
    payload = json.loads(render_diff(diff, "json"))

    assert diff.baseline_stdout_relay_error == "BrokenPipeError: consumer\x1b[31m closed"
    assert diff.baseline_stderr_relay_error is None
    assert diff.candidate_stdout_relay_error is None
    assert diff.candidate_stderr_relay_error == "OSError: terminal closed"
    assert diff.output_equivalent is True
    assert diff.stderr_equivalent is True
    assert diff.outcome == "equivalent"
    assert "baseline: stdout relay failed (BrokenPipeError: consumer\\x1b[31m closed)" in report
    assert "candidate: stderr relay failed (OSError: terminal closed)" in report
    assert "\x1b" not in report
    assert payload["baseline_stdout_relay_error"] == ("BrokenPipeError: consumer\x1b[31m closed")
    assert payload["candidate_stderr_relay_error"] == "OSError: terminal closed"


@pytest.mark.parametrize("invalid_completeness", ("false", 0, None))
def test_compare_runpacks_keeps_invalid_output_completeness_unknown(
    tmp_path: Path, invalid_completeness: object
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process((sys.executable, "-c", "print('same')"), baseline, name="baseline")
    record_process((sys.executable, "-c", "print('same')"), candidate, name="candidate")
    with RunpackReader(baseline) as reader:
        execution = reader.execution()
    metadata = execution.metadata.copy()
    output = metadata["output"]
    assert isinstance(output, dict)
    stdout = output["stdout"]
    assert isinstance(stdout, dict)
    stdout["pipe_open_after_exit"] = cast(Any, invalid_completeness)
    with RunpackWriter.open_existing(baseline) as writer:
        writer.set_execution_metadata(execution.id, metadata)

    diff = compare_runpacks(baseline, candidate)
    baseline_summary = json.loads(render_summary(inspect_runpack(baseline), "json"))
    candidate_summary = json.loads(render_summary(inspect_runpack(candidate), "json"))

    assert diff.output_equivalent is None
    assert diff.stderr_equivalent is True
    assert diff.outcome == "unknown"
    assert "stdout:      unknown" in render_diff(diff, "text")
    assert baseline_summary["stdout_complete"] is None
    assert candidate_summary["stdout_complete"] is True


def test_compare_runpacks_keeps_invalid_output_byte_counts_unknown(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process((sys.executable, "-c", "print('same')"), baseline, name="baseline")
    record_process((sys.executable, "-c", "print('same')"), candidate, name="candidate")
    with RunpackReader(baseline) as reader:
        execution = reader.execution()
    metadata = execution.metadata.copy()
    output = metadata["output"]
    assert isinstance(output, dict)
    stdout = output["stdout"]
    assert isinstance(stdout, dict)
    stdout["bytes"] = True
    with RunpackWriter.open_existing(baseline) as writer:
        writer.set_execution_metadata(execution.id, metadata)

    diff = compare_runpacks(baseline, candidate)

    assert diff.output_equivalent is None
    assert diff.stderr_equivalent is True
    assert diff.outcome == "unknown"


def test_compare_runpacks_keeps_outcome_unknown_when_stderr_evidence_is_missing(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, identity in ((baseline, "baseline"), (candidate, "candidate")):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    identity,
                    identity,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {"output": {"stdout": {"bytes": 4, "sha256": _STDOUT_IDENTITY}}},
                )
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.exit_code_equivalent is True
    assert diff.output_equivalent is True
    assert diff.stderr_equivalent is None
    assert diff.outcome == "unknown"


def test_compare_runpacks_does_not_accept_malformed_output_digests(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path in (baseline, candidate):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    path.stem,
                    path.stem,
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {
                        "output": {
                            "stdout": {"bytes": 4, "sha256": "same-but-not-a-sha256"},
                            "stderr": {"bytes": 0, "sha256": _STDERR_IDENTITY},
                        }
                    },
                )
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.output_equivalent is None
    assert diff.stderr_equivalent is True
    assert diff.outcome == "unknown"


def test_compare_runpacks_does_not_treat_untimed_operations_as_zero_duration(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    for path, finish in ((baseline, 10), (candidate, None)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 10, (), str(tmp_path), 0, None, {})
            )
            writer.add_entity(Entity("worker", "service", "worker", None, {}))
            writer.add_event(
                Event(
                    "operation",
                    "stage",
                    "transform",
                    "worker",
                    0 if finish is not None else None,
                    finish,
                    "test",
                    None,
                    None,
                    {},
                )
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.operation_count_changes == ()
    assert diff.operation_concurrency_changes == ()
    assert diff.operation_duration_changes == ()


def test_compare_runpacks_surfaces_incomplete_annotation_evidence(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process((sys.executable, "-c", "pass"), baseline, name="baseline")
    record_process((sys.executable, "-c", "pass"), candidate, name="candidate")
    with sqlite3.connect(candidate) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["capture"]["annotation_error"] = "invalid annotation JSON on line 1"
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))

    diff = compare_runpacks(baseline, candidate)

    assert diff.baseline_annotation_error is None
    assert diff.candidate_annotation_error == "invalid annotation JSON on line 1"
    assert diff.operation_errors_equivalent is None
    assert diff.outcome == "unknown"
    assert "candidate: annotations ignored" in render_diff(diff, "text")


@pytest.mark.parametrize(
    ("otel_metadata", "expected_count", "expected_warning"),
    (
        (
            {"missing_parent_count": 2, "missing_link_count": 1},
            3,
            "candidate: 3 unresolved causal references",
        ),
        (
            {"missing_parent_count": "invalid"},
            None,
            "candidate: causal completeness metadata invalid",
        ),
    ),
)
def test_compare_runpacks_downgrades_structural_match_with_incomplete_causal_evidence(
    tmp_path: Path,
    otel_metadata: dict[str, Any],
    expected_count: int | None,
    expected_warning: str,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    with sqlite3.connect(candidate) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["otel"] = otel_metadata
        connection.execute(
            "UPDATE executions SET id = ?, metadata_json = ?",
            ("candidate", json.dumps(metadata)),
        )

    diff = compare_runpacks(baseline, candidate)

    assert diff.baseline_missing_causal_references == 0
    assert diff.candidate_missing_causal_references == expected_count
    assert diff.match_level == "aggregate"
    assert expected_warning in render_diff(diff, "text")


def test_compare_runpacks_keeps_failure_equivalence_unknown_with_dropped_attributes(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    with sqlite3.connect(candidate) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["otel"] = {"dropped_attribute_count": 2}
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))

    diff = compare_runpacks(baseline, candidate)

    assert diff.candidate_dropped_attribute_count == 2
    assert diff.operation_errors_equivalent is None
    assert diff.outcome == "unknown"
    assert "candidate: 2 exporter-dropped OTLP attributes" in render_diff(diff, "text")
