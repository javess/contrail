from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

import runtime_tools.rundiff.report as report_module
from runtime_tools import record_process
from runtime_tools.model import CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter

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
                        "stdout": {"bytes": 7, "sha256": _STDOUT_IDENTITY},
                        "stderr": {"bytes": 0, "sha256": _STDERR_IDENTITY},
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
        writer.add_measurements(
            (
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
    assert payload["outcome"] == "different"
    assert payload["stderr_equivalent"] is True
    assert payload["operation_errors_equivalent"] is False
    assert payload["wall_time"]["percent"] == 100.0
    assert payload["cpu_time"] == {"baseline": 1.5, "candidate": 3.0, "percent": 100.0}
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
    assert "matching:  exact" in compared.stdout
    assert "Outcome\n  equivalent" in compared.stdout
    assert (
        "No entity, structural, error, concurrency, duration, or operation-count changes."
        in compared.stdout
    )


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


def test_compare_runpacks_keeps_invalid_output_completeness_unknown(tmp_path: Path) -> None:
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
    stdout["pipe_open_after_exit"] = "false"
    with RunpackWriter.open_existing(baseline) as writer:
        writer.set_execution_metadata(execution.id, metadata)

    diff = compare_runpacks(baseline, candidate)

    assert diff.output_equivalent is None
    assert diff.stderr_equivalent is True
    assert diff.outcome == "unknown"
    assert "stdout:      unknown" in render_diff(diff, "text")


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
