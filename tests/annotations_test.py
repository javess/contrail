from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from runtime_tools import record_process, runtime
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


def test_annotation_writer_completes_short_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    write = os.write

    def short_write(descriptor: int, data: bytes | bytearray | memoryview) -> int:
        return write(descriptor, data[:7])

    monkeypatch.setattr(os, "write", short_write)

    runtime.event("short-write", detail="complete")

    record = json.loads(annotations.read_text(encoding="utf-8"))
    assert record["name"] == "short-write"
    assert record["attributes"] == {"detail": "complete"}


def test_scope_restores_parent_context_when_end_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[dict[str, object]] = []
    failed = False

    def fail_first_end(record: dict[str, object]) -> None:
        nonlocal failed
        records.append(record)
        if record["record"] == "event_end" and not failed:
            failed = True
            raise OSError("annotation sink failed")

    monkeypatch.setattr(runtime, "_write", fail_first_end)

    with runtime.run("outer") as outer:
        with pytest.raises(OSError, match="annotation sink failed"):
            with runtime.stage("inner"):
                pass
        after_failure = runtime.event("after-failure")

    event_record = next(record for record in records if record.get("id") == after_failure.id)
    assert event_record["parent_id"] == outer.id
    assert runtime._current_event_id.get() is None
