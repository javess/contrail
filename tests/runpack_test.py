from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest

import runtime_tools.model as model
import runtime_tools.runpack as runpack_api
import runtime_tools.storage as storage
import runtime_tools.storage._reader as storage_reader
import runtime_tools.storage._snapshot as storage_snapshot
import runtime_tools.storage._validation as storage_validation
import runtime_tools.storage._writer as storage_writer
from runtime_tools import Runpack, RunpackError, open_runpack
from runtime_tools.runpack import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    Measurement,
)
from runtime_tools.storage import RunpackWriter, open_runpack_snapshot


def _complete_runpack(path: Path) -> None:
    execution = Execution(
        id="run-1",
        name="checkout",
        started_at_ns=100,
        finished_at_ns=400,
        command=("python", "checkout.py"),
        working_directory="/work",
        exit_code=0,
        revision="abc123",
        metadata={"environment": {"region": "test"}},
    )
    entity = Entity("service-1", "service", "checkout", None, {"version": 2})
    events = (
        Event(
            "event-1",
            "server.request",
            "POST /checkout",
            entity.id,
            110,
            390,
            "monotonic",
            1,
            1,
            {"http.status_code": 200},
        ),
        Event(
            "event-2",
            "db.query",
            "INSERT order",
            entity.id,
            150,
            250,
            "monotonic",
            1,
            2,
            {"db.system": "sqlite"},
        ),
    )
    edge = CausalEdge(events[0].id, events[1].id, "parent", 1.0, {"source": "trace"})
    measurement = Measurement("process.cpu.user", 0.25, "s", 400, entity.id, {"core": 0})
    attachment = Attachment(
        "stdout",
        "process.output",
        "stdout",
        "text/plain",
        b"created order\n",
        {"complete": True},
    )

    with RunpackWriter(path) as writer:
        writer.add_execution(execution)
        writer.add_entity(entity)
        writer.add_event_graph(events, (edge,))
        writer.add_measurement(measurement)
        writer.add_attachments((attachment,))


def test_public_module_exports_supported_reader_and_normalized_types() -> None:
    assert runpack_api.__all__ == [
        "Attachment",
        "CausalEdge",
        "Entity",
        "Event",
        "Execution",
        "JsonScalar",
        "JsonValue",
        "Measurement",
        "Runpack",
        "RunpackError",
        "open_runpack",
    ]
    assert runpack_api.Runpack is Runpack
    assert runpack_api.RunpackError is RunpackError
    assert runpack_api.open_runpack is open_runpack
    assert Attachment is model.Attachment
    assert CausalEdge is model.CausalEdge
    assert Entity is model.Entity
    assert Event is model.Event
    assert Execution is model.Execution
    assert Measurement is model.Measurement
    assert runpack_api.JsonScalar is model.JsonScalar
    assert runpack_api.JsonValue is model.JsonValue


def test_open_runpack_reads_every_normalized_collection(tmp_path: Path) -> None:
    path = tmp_path / "complete.runpack"
    _complete_runpack(path)

    with open_runpack(path) as runpack:
        assert runpack.execution() == Execution(
            "run-1",
            "checkout",
            100,
            400,
            ("python", "checkout.py"),
            "/work",
            0,
            "abc123",
            {"environment": {"region": "test"}},
        )
        assert runpack.manifest()["schema_version"] == "1.1"
        assert runpack.entities() == (
            Entity("service-1", "service", "checkout", None, {"version": 2}),
        )
        assert [event.id for event in runpack.events()] == ["event-1", "event-2"]
        assert runpack.causal_edges() == (
            CausalEdge("event-1", "event-2", "parent", 1.0, {"source": "trace"}),
        )
        assert runpack.measurements() == (
            Measurement("process.cpu.user", 0.25, "s", 400, "service-1", {"core": 0}),
        )
        assert runpack.attachments() == (
            Attachment(
                "stdout",
                "process.output",
                "stdout",
                "text/plain",
                b"created order\n",
                {"complete": True},
            ),
        )


@pytest.mark.parametrize(
    "path_value",
    (lambda path: str(path), lambda path: path),
    ids=("str", "path-like"),
)
def test_open_runpack_accepts_string_and_path_like_paths(
    tmp_path: Path, path_value: Callable[[Path], str | Path]
) -> None:
    path = tmp_path / "input.runpack"
    _complete_runpack(path)

    with open_runpack(path_value(path)) as runpack:
        assert runpack.execution().id == "run-1"

    with Runpack(path_value(path)) as runpack:
        assert runpack.execution().id == "run-1"


def test_close_is_idempotent_and_all_reads_fail_with_stable_public_error(tmp_path: Path) -> None:
    path = tmp_path / "input.runpack"
    _complete_runpack(path)
    runpack = open_runpack(path)

    with runpack as entered:
        assert entered is runpack
    runpack.close()

    reads = (
        runpack.execution,
        runpack.manifest,
        runpack.entities,
        runpack.events,
        runpack.causal_edges,
        runpack.measurements,
        runpack.attachments,
    )
    for read in reads:
        with pytest.raises(RunpackError, match="^runpack is closed$"):
            read()
    with pytest.raises(RunpackError, match="^runpack is closed$"):
        runpack.__enter__()


@pytest.mark.parametrize(
    ("name", "message"),
    (
        ("missing.runpack", "runpack does not exist"),
        ("not-a-runpack.runpack", "invalid runpack"),
    ),
)
def test_open_runpack_reports_invalid_inputs_as_public_errors(
    tmp_path: Path, name: str, message: str
) -> None:
    path = tmp_path / name
    if name == "not-a-runpack.runpack":
        path.write_text("not sqlite", encoding="utf-8")

    with pytest.raises(RunpackError, match=message):
        open_runpack(path)


def test_open_runpack_normalizes_invalid_path_errors() -> None:
    with pytest.raises(RunpackError, match="^invalid runpack path$"):
        open_runpack("invalid\0.runpack")


def test_returned_json_values_do_not_write_through_to_the_runpack(tmp_path: Path) -> None:
    path = tmp_path / "input.runpack"
    _complete_runpack(path)

    with open_runpack(path) as runpack:
        execution = runpack.execution()
        execution.metadata["environment"] = {"region": "changed"}
        event = runpack.events()[0]
        event.attributes["http.status_code"] = 500

        assert runpack.execution().metadata == {"environment": {"region": "test"}}
        assert runpack.events()[0].attributes == {"http.status_code": 200}


@pytest.mark.parametrize(
    ("limit_name", "limit", "message"),
    (
        ("MAX_RUNPACK_JSON_BYTES", 512, "runpack JSON exceeds"),
        ("MAX_RUNPACK_SQLITE_LENGTH_BYTES", 128, "^invalid runpack:"),
    ),
    ids=("json-field", "sqlite-length"),
)
def test_open_runpack_bounds_fields_before_materializing_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    limit: int,
    message: str,
) -> None:
    path = tmp_path / "oversized.runpack"
    _complete_runpack(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE executions SET metadata_json = ?",
            (json.dumps({"oversized": "x" * 1024}),),
        )
    monkeypatch.setattr(storage_validation, limit_name, limit)

    with pytest.raises(RunpackError, match=message):
        open_runpack(path)


def test_open_runpack_preflights_record_and_aggregate_byte_caps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "bounded.runpack"
    _complete_runpack(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO measurements(name, value, unit, attributes_json) VALUES (?, ?, ?, ?)",
            ("second", 1.0, "1", "{}"),
        )

    statements: list[str] = []

    def trace_connection(connection: sqlite3.Connection) -> None:
        connection.set_trace_callback(statements.append)

    monkeypatch.setattr(storage_validation, "MAX_RUNPACK_MEASUREMENT_RECORDS", 1)
    with pytest.raises(RunpackError, match="measurements exceeds the record limit of 1"):
        storage.RunpackReader(path, prepare_connection=trace_connection)
    assert any("SELECT 1 FROM measurements LIMIT 1 OFFSET 1" in sql for sql in statements)
    assert not any(
        "length(CAST(name AS BLOB))" in sql and "SELECT * FROM measurements" in sql
        for sql in statements
    )

    monkeypatch.setattr(storage_validation, "MAX_RUNPACK_MEASUREMENT_RECORDS", 2)
    monkeypatch.setattr(storage_validation, "MAX_RUNPACK_NORMALIZED_JSON_BYTES", 1)
    with pytest.raises(RunpackError, match="normalized JSON bytes; aggregate limit is 1"):
        open_runpack(path)


def test_path_readers_and_existing_writers_reject_runpacks_over_the_file_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "oversized-file.runpack"
    _complete_runpack(path)
    file_limit = path.stat().st_size
    with path.open("r+b") as artifact:
        artifact.truncate(file_limit + 4096)
    monkeypatch.setattr(storage_validation, "MAX_RUNPACK_FILE_BYTES", file_limit)

    with pytest.raises(RunpackError, match=f"file limit is {file_limit} bytes"):
        open_runpack(path)
    with pytest.raises(RunpackError, match=f"file limit is {file_limit} bytes"):
        RunpackWriter.open_existing(path)
    descriptor = os.open(path, os.O_RDONLY)
    try:
        with pytest.raises(RunpackError, match=f"file limit is {file_limit} bytes"):
            with storage.validated_runpack_snapshot(descriptor):
                pass
    finally:
        os.close(descriptor)


def test_descriptor_size_check_rejects_growth_before_hashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "growing.runpack"
    _complete_runpack(path)
    file_limit = path.stat().st_size
    real_open = os.open
    hash_called = False

    def grow_before_open(path_value: str | bytes | Path, flags: int, *args: int) -> int:
        os.truncate(path, file_limit + 4096)
        return real_open(path_value, flags, *args)

    def reject_hash(*args: object, **kwargs: object) -> object:
        nonlocal hash_called
        hash_called = True
        raise AssertionError("oversized descriptor reached hashing")

    monkeypatch.setattr(storage_validation, "MAX_RUNPACK_FILE_BYTES", file_limit)
    monkeypatch.setattr(os, "open", grow_before_open)
    monkeypatch.setattr(storage_snapshot, "_hash_descriptor", reject_hash)

    with pytest.raises(RunpackError, match=f"file limit is {file_limit} bytes"):
        with open_runpack_snapshot(path):
            pass

    assert hash_called is False


def test_attachment_at_the_configured_field_and_aggregate_limit_is_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "attachment-boundary.runpack"
    monkeypatch.setattr(storage_writer, "MAX_RUNPACK_ATTACHMENT_BYTES", 8)
    monkeypatch.setattr(storage_writer, "MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES", 8)
    monkeypatch.setattr(storage_reader, "MAX_RUNPACK_ATTACHMENT_BYTES", 8)
    monkeypatch.setattr(storage_reader, "MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES", 8)
    with RunpackWriter(path) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), "/work", 0, None, {}))
        writer.add_attachments(
            (Attachment("evidence", "raw", "evidence", "application/octet-stream", b"x" * 8, {}),)
        )

    with open_runpack(path) as runpack:
        assert runpack.attachments()[0].content == b"x" * 8


def test_runpack_does_not_expose_internal_copy_mutation_or_query_helpers(tmp_path: Path) -> None:
    path = tmp_path / "input.runpack"
    _complete_runpack(path)

    with open_runpack(path) as runpack:
        for name in (
            "add_execution",
            "copy_snapshot_to",
            "counts",
            "first_measurement_values",
            "normalized_json_bytes",
            "operation_counts",
            "path",
            "set_execution_metadata",
        ):
            assert not hasattr(runpack, name)
