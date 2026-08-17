from __future__ import annotations

import sys
from pathlib import Path

from runtime_tools import record_process
from runtime_tools.storage import RunpackReader


def test_record_process_normalizes_nested_domain_annotations(tmp_path: Path) -> None:
    workload = tmp_path / "workload.py"
    workload.write_text(
        """
from runtime_tools import runtime

with runtime.run("pipeline", total_work=10):
    with runtime.stage("transform"):
        first = runtime.event("db.write", kind="client.request", table="results")
        second = runtime.event("db.flush", kind="client.request")
        runtime.link(first, second, relation="flushes")
    runtime.progress(completed=10, total=10)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "annotated.runpack"

    record_process((sys.executable, str(workload)), output, name="annotated")

    with RunpackReader(output) as reader:
        events = {event.name: event for event in reader.events()}
        edges = reader.causal_edges()
    assert set(events) == {
        Path(sys.executable).name,
        "pipeline",
        "transform",
        "db.write",
        "db.flush",
        "progress",
    }
    assert events["pipeline"].kind == "run"
    assert events["transform"].kind == "stage"
    assert events["db.write"].attributes["table"] == "results"
    assert events["progress"].attributes == {"completed": 10, "total": 10}
    relationships = {
        (events_by_id.source_event_id, events_by_id.target_event_id, events_by_id.kind)
        for events_by_id in edges
    }
    assert (events["pipeline"].id, events["transform"].id, "parent") in relationships
    assert (events[Path(sys.executable).name].id, events["pipeline"].id, "parent") in relationships
    assert (events["transform"].id, events["db.write"].id, "parent") in relationships
    assert (events["db.write"].id, events["db.flush"].id, "flushes") in relationships


def test_record_process_preserves_an_incomplete_stage_after_abrupt_exit(tmp_path: Path) -> None:
    workload = tmp_path / "crash.py"
    workload.write_text(
        """
import os
from runtime_tools import runtime

with runtime.stage("before-crash"):
    os._exit(3)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "crash.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="crash")

    with RunpackReader(output) as reader:
        event = next(event for event in reader.events() if event.name == "before-crash")
    assert exit_code == 3
    assert event.started_at_ns is not None
    assert event.finished_at_ns is None
    assert not tuple(tmp_path.glob("*.annotations-*"))
