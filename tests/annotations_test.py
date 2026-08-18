from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import runtime_tools.annotations as annotations_module
from runtime_tools import record_process, runtime
from runtime_tools.annotations import AnnotationError, load_annotations
from runtime_tools.inspect import inspect_runpack, render_summary
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


def test_record_process_preserves_core_capture_when_annotations_are_malformed(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "malformed.py"
    workload.write_text(
        """
import os
from pathlib import Path

Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_text("{")
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "malformed.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="malformed")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.exit_code == 0
    assert summary.annotation_error == "invalid annotation JSON on line 1"
    assert "annotations: ignored (invalid annotation JSON on line 1)" in render_summary(
        summary, "text"
    )


def test_record_process_preserves_core_capture_for_non_finite_annotation_json(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "non-finite.py"
    workload.write_text(
        """
import os
from pathlib import Path

Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_text(
    '{"record":"event_instant","id":"bad","kind":"event","name":"bad",'
    '"timestamp_ns":1,"attributes":{"value":NaN}}'
)
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "non-finite.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="non-finite")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error == "invalid annotation JSON on line 1: non-finite constant NaN"
    assert summary.record_counts["events"] == 1


def test_record_process_preserves_core_capture_for_non_utf8_annotations(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "non-utf8.py"
    workload.write_text(
        """
import os
from pathlib import Path

Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_bytes(b"\\xff")
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "non-utf8.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="non-utf8")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error == "captured annotations must be UTF-8"
    assert summary.record_counts["events"] == 1


def test_record_process_preserves_core_capture_for_oversized_annotation_fields(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "oversized-field.py"
    workload.write_text(
        """
from runtime_tools import runtime

runtime.event("oversized", value="x" * (4 * 1024 * 1024))
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "oversized-field.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="oversized-field")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error is not None
    assert summary.annotation_error.startswith("runpack JSON exceeds the 4194304-byte field limit")
    assert summary.record_counts["events"] == 1


def test_record_process_preserves_core_capture_for_reversed_annotation_intervals(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "reversed.py"
    workload.write_text(
        """
import json
import os
from pathlib import Path

records = [
    {"record": "event_start", "id": "bad", "kind": "stage", "name": "bad", "timestamp_ns": 10},
    {"record": "event_end", "id": "bad", "timestamp_ns": 9},
]
Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_text(
    "".join(json.dumps(record) + "\\n" for record in records)
)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "reversed.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="reversed")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error == "annotation event bad ends before it starts"
    assert summary.record_counts["events"] == 1


def test_record_process_preserves_core_capture_for_duplicate_annotation_links(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "duplicate-link.py"
    workload.write_text(
        """
from runtime_tools import runtime

source = runtime.event("source")
target = runtime.event("target")
runtime.link(source, target)
runtime.link(source, target)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "duplicate-link.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="duplicate-link")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error == "duplicate annotation causal edge"
    assert summary.record_counts["events"] == 1
    assert summary.record_counts["causal_edges"] == 0


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


def test_annotation_writer_keeps_concurrent_short_writes_as_complete_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    write = os.write

    def short_write(descriptor: int, data: bytes | bytearray | memoryview) -> int:
        return write(descriptor, data[:17])

    monkeypatch.setattr(os, "write", short_write)

    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(
            executor.map(
                lambda index: runtime.event(f"event-{index}", payload="x" * 1_000),
                range(100),
            )
        )

    records = [json.loads(line) for line in annotations.read_text().splitlines()]
    assert len(records) == 100
    assert {record["name"] for record in records} == {f"event-{index}" for index in range(100)}


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


def test_annotation_scopes_reject_reuse_without_emitting_duplicate_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[dict[str, object]] = []
    monkeypatch.setattr(runtime, "_write", records.append)
    scope = runtime.stage("one-shot")

    with scope:
        pass
    with pytest.raises(RuntimeError, match="annotation scopes cannot be reused"):
        with scope:
            pass

    assert [record["record"] for record in records] == ["event_start", "event_end"]
    assert runtime._current_event_id.get() is None


@pytest.mark.parametrize(
    ("records", "message"),
    (
        (
            (
                {"record": "event_instant", "id": "same"},
                {"record": "event_instant", "id": "same"},
            ),
            "duplicate annotation event id on line 2",
        ),
        (
            (
                {"record": "event_start", "id": "scope"},
                {"record": "event_end", "id": "scope"},
                {"record": "event_end", "id": "scope"},
            ),
            "duplicate annotation event end on line 3",
        ),
        (
            ({"record": "event_end", "id": "missing"},),
            "orphan annotation event end on line 1",
        ),
    ),
)
def test_annotation_loader_rejects_conflicting_lifecycle_records(
    tmp_path: Path,
    records: tuple[dict[str, object], ...],
    message: str,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    annotations.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records), encoding="utf-8"
    )

    with pytest.raises(AnnotationError, match=message):
        load_annotations(annotations, entity_id="process")


@pytest.mark.parametrize(
    ("records", "message"),
    (
        (
            (
                {
                    "record": "event_instant",
                    "id": "child",
                    "kind": "event",
                    "name": "child",
                    "timestamp_ns": 1,
                    "parent_id": "missing",
                },
            ),
            "parent event is unresolved: missing",
        ),
        (
            (
                {
                    "record": "event_instant",
                    "id": "event",
                    "kind": "event",
                    "name": "event",
                    "timestamp_ns": 1,
                },
                {
                    "record": "link",
                    "source_id": "event",
                    "target_id": "missing",
                    "relation": "causes",
                },
            ),
            "link event -> missing is unresolved",
        ),
        (
            (
                {
                    "record": "event_instant",
                    "id": "first",
                    "kind": "event",
                    "name": "first",
                    "timestamp_ns": 1,
                    "parent_id": "second",
                },
                {
                    "record": "event_instant",
                    "id": "second",
                    "kind": "event",
                    "name": "second",
                    "timestamp_ns": 2,
                    "parent_id": "first",
                },
            ),
            "parent relationships contain a cycle",
        ),
        (
            (
                {
                    "record": "event_instant",
                    "id": "first",
                    "kind": "event",
                    "name": "first",
                    "timestamp_ns": 1,
                },
                {
                    "record": "event_instant",
                    "id": "second",
                    "kind": "event",
                    "name": "second",
                    "timestamp_ns": 1,
                },
                {
                    "record": "event_instant",
                    "id": "child",
                    "kind": "event",
                    "name": "child",
                    "timestamp_ns": 2,
                    "parent_id": "first",
                },
                {
                    "record": "link",
                    "source_id": "second",
                    "target_id": "child",
                    "relation": "parent",
                },
            ),
            "event cannot have multiple parents",
        ),
    ),
)
def test_annotation_loader_rejects_incomplete_or_cyclic_causality(
    tmp_path: Path,
    records: tuple[dict[str, object], ...],
    message: str,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    annotations.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records), encoding="utf-8"
    )

    with pytest.raises(AnnotationError, match=message):
        load_annotations(annotations, entity_id="process")


@pytest.mark.parametrize(
    ("records", "message"),
    (
        (
            (
                {
                    "record": "event_instant",
                    "id": "event",
                    "kind": "event",
                    "name": "event",
                    "timestamp_ns": 1,
                    "atributes": {},
                },
            ),
            "contains unsupported fields: atributes",
        ),
        (
            (
                {
                    "record": "event_start",
                    "id": "event",
                    "kind": "event",
                    "name": "event",
                    "timestamp_ns": 1,
                },
                {
                    "record": "event_end",
                    "id": "event",
                    "timestamp_ns": 2,
                    "error": "yes",
                },
            ),
            "event_end error must be a boolean",
        ),
    ),
)
def test_annotation_loader_rejects_ignored_record_fields(
    tmp_path: Path,
    records: tuple[dict[str, object], ...],
    message: str,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    annotations.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records), encoding="utf-8"
    )

    with pytest.raises(AnnotationError, match=message):
        load_annotations(annotations, entity_id="process")


def test_annotation_loader_rejects_oversized_streams_before_decoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    annotation_path = tmp_path / "oversized.jsonl"
    annotation_path.write_bytes(b"{" + b" " * 32 + b"}")
    monkeypatch.setattr(annotations_module, "MAX_ANNOTATION_STREAM_BYTES", 32)

    with pytest.raises(AnnotationError, match="annotations exceed the 32-byte input limit"):
        load_annotations(annotation_path, entity_id="process")


def test_annotation_loader_rejects_too_many_records_before_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    annotation_path = tmp_path / "too-many.jsonl"
    annotation_path.write_bytes(b"{}\n{}\n{}\n")
    monkeypatch.setattr(annotations_module, "MAX_ANNOTATION_RECORDS", 2)

    with pytest.raises(AnnotationError, match="annotations exceed the 2-record input limit"):
        load_annotations(annotation_path, entity_id="process")
