from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

from runtime_tools import record_process
from runtime_tools.model import CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackReader, RunpackWriter


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

    with RunpackWriter(path) as writer:
        writer.add_execution(
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
                        "stdout": {"bytes": 7, "sha256": "same-output"},
                        "stderr": {"bytes": 0, "sha256": "empty"},
                    }
                },
            )
        )
        for entity in entities:
            writer.add_entity(entity)
        for event in events:
            writer.add_event(event)
        for event in events[1:]:
            writer.add_causal_edge(CausalEdge("root", event.id, "parent", 1.0, {}))
        writer.add_measurement(
            Measurement(
                "process.memory.peak",
                150 * 1024**2 if candidate else 100 * 1024**2,
                "By",
                finished_at_ns,
                "gateway",
                {},
            )
        )


def test_compare_runpacks_finds_timing_cardinality_and_dependency_changes(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)

    diff = compare_runpacks(baseline, candidate)

    assert diff.outcome == "equivalent"
    assert diff.exit_code_equivalent is True
    assert diff.output_equivalent is True
    assert diff.stderr_equivalent is True
    assert diff.wall_time.baseline == 0.01
    assert diff.wall_time.candidate == 0.02
    assert diff.wall_time.percent == 100.0
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
    with RunpackReader(runpack) as reader:
        assert reader.peer_service_edge_counts() == {
            ("service", "worker", "service", "database", "calls"): 3
        }


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


def test_rundiff_cli_emits_matching_text_and_json_reports(tmp_path: Path) -> None:
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
    assert payload["outcome"] == "equivalent"
    assert payload["stderr_equivalent"] is True
    assert payload["wall_time"]["percent"] == 100.0
    assert payload["critical_path"]["percent"] == 100.0
    assert payload["entity_count_changes"] == [
        {
            "baseline": 0,
            "candidate": 1,
            "change_kind": "added",
            "entity_kind": "service",
            "entity_name": "metadata",
        }
    ]
    assert payload["operation_count_changes"][0]["candidate"] == 3
    assert payload["operation_error_count_changes"][0]["candidate"] == 1
    assert payload["operation_duration_changes"][0]["candidate_seconds"] == 0.02
    assert payload["operation_concurrency_changes"][0]["candidate"] == 3


def test_rundiff_cli_records_named_alias_and_resolves_it_for_comparison(tmp_path: Path) -> None:
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
    assert compared.returncode == 0
    assert "Outcome\n  equivalent" in compared.stdout
    assert (
        "No entity, structural, error, concurrency, duration, or operation-count changes."
        in compared.stdout
    )


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


def test_compare_runpacks_reports_selected_environment_drift_without_hashes(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    environments = (
        (
            baseline,
            {
                "CI": "baseline-ci-hash",
                "LANG": "same-hash",
                "TZ": "baseline-tz-hash",
            },
        ),
        (
            candidate,
            {
                "LANG": "same-hash",
                "PYTHONHASHSEED": "candidate-seed-hash",
                "TZ": "candidate-tz-hash",
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
    assert "baseline-tz-hash" not in report
    assert json.loads(render_diff(diff, "json"))["environment_changes"][2] == {
        "baseline_present": True,
        "candidate_present": True,
        "change_kind": "changed",
        "variable": "TZ",
    }


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
                                "sha256": "same",
                                "pipe_open_after_exit": pipe_open,
                            },
                            "stderr": {"bytes": 0, "sha256": "empty"},
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
                    {"output": {"stdout": {"bytes": 4, "sha256": "same"}}},
                )
            )

    diff = compare_runpacks(baseline, candidate)

    assert diff.exit_code_equivalent is True
    assert diff.output_equivalent is True
    assert diff.stderr_equivalent is None
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
    assert "candidate: annotations ignored" in render_diff(diff, "text")


def test_compare_runpacks_surfaces_unresolved_causal_references(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    with sqlite3.connect(candidate) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["otel"] = {"missing_parent_count": 2, "missing_link_count": 1}
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))

    diff = compare_runpacks(baseline, candidate)

    assert diff.baseline_missing_causal_references == 0
    assert diff.candidate_missing_causal_references == 3
    assert "candidate: 3 unresolved causal references" in render_diff(diff, "text")
