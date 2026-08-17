from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from runtime_tools.model import CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackWriter


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
                {},
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
    assert diff.wall_time.baseline == 0.01
    assert diff.wall_time.candidate == 0.02
    assert diff.wall_time.percent == 100.0
    assert diff.critical_path.baseline == 0.01
    assert diff.critical_path.candidate == 0.02
    assert diff.critical_path.percent == 100.0
    assert diff.peak_memory.percent == 50.0
    assert [
        (change.entity_name, change.operation_name, change.baseline, change.candidate)
        for change in diff.operation_count_changes
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
    assert "Critical path\n  10.0ms → 20.0ms (+100.0%)" in text_report
    assert "database :: SELECT items [client.request]" in text_report
    assert "gateway → metadata [parent]: 0 → 1" in text_report
    assert command.returncode == 0
    payload = json.loads(command.stdout)
    assert payload["outcome"] == "equivalent"
    assert payload["wall_time"]["percent"] == 100.0
    assert payload["critical_path"]["percent"] == 100.0
    assert payload["operation_count_changes"][0]["candidate"] == 3


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
    assert "No structural or operation-count changes." in compared.stdout
