from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from runtime_tools import record_process
from runtime_tools.batchscope import analyze_runpack
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
    assert analysis.critical_path.duration_seconds == 0.06
    assert analysis.critical_path.parallel_slack_seconds == 0.04
    assert analysis.critical_path.event_names == ("python", "pipeline", "drain", "database.flush")
    assert analysis.throughput is not None
    assert analysis.throughput.rate_per_second == 1000.0
    assert analysis.throughput.remaining == 40.0
    assert analysis.throughput.estimated_drain_seconds == 0.04
    assert {item.classification for item in analysis.bottlenecks} == {
        "serialized_stage",
        "external_dependency",
    }


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
    assert payload["bottlenecks"][0]["classification"] == "serialized_stage"


def test_batchscope_labels_an_unlinked_process_path_as_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "local.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="local")

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"
