from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import sys
import threading
import time
from collections.abc import Buffer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from runtime_tools import CaptureError, capture, inspect_runpack, record_process, storage
from runtime_tools.inspect import render_summary
from runtime_tools.model import Attachment, CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.storage import (
    MAX_RUNPACK_JSON_BYTES,
    MAX_RUNPACK_TEXT_BYTES,
    RunpackError,
    RunpackReader,
    RunpackWriter,
    UnsupportedSchemaError,
)


def test_record_process_captures_outcome_resources_and_output_identity(tmp_path: Path) -> None:
    output = tmp_path / "hello.runpack"
    stdout = io.BytesIO()
    stderr = io.BytesIO()

    exit_code = record_process(
        (sys.executable, "-c", "import sys; print('hello'); print('warning', file=sys.stderr)"),
        output,
        name="hello",
        stdout=stdout,
        stderr=stderr,
    )

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert stdout.getvalue() == b"hello\n"
    assert stderr.getvalue() == b"warning\n"
    assert summary.exit_code == 0
    assert summary.schema_version == "1.1"
    assert summary.producer_version == "0.1.0"
    assert summary.wall_time_seconds is not None and summary.wall_time_seconds >= 0
    assert summary.cpu_user_seconds is not None and summary.cpu_user_seconds >= 0
    assert summary.peak_memory_bytes is not None and summary.peak_memory_bytes > 0
    assert summary.stdout_bytes == 6
    assert summary.stdout_sha256 == hashlib.sha256(b"hello\n").hexdigest()
    assert summary.stderr_bytes == 8
    assert summary.stderr_sha256 == hashlib.sha256(b"warning\n").hexdigest()
    assert summary.record_counts == {
        "entities": 1,
        "events": 1,
        "causal_edges": 0,
        "measurements": 6,
        "attachments": 0,
    }


def test_record_process_preserves_nonzero_exit_as_execution_outcome(tmp_path: Path) -> None:
    output = tmp_path / "failed.runpack"

    exit_code = record_process((sys.executable, "-c", "raise SystemExit(7)"), output, name="failed")

    assert exit_code == 7
    assert inspect_runpack(output).exit_code == 7


def test_record_process_anchors_execution_finish_to_monotonic_elapsed_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "monotonic.runpack"
    wall_times = iter((100, 50))
    monotonic_times = iter((10, 20))
    monkeypatch.setattr(
        capture,
        "time",
        SimpleNamespace(
            time_ns=lambda: next(wall_times),
            perf_counter_ns=lambda: next(monotonic_times),
        ),
    )

    record_process((sys.executable, "-c", "pass"), output, name="monotonic")

    summary = inspect_runpack(output)
    assert summary.started_at_ns == 100
    assert summary.finished_at_ns == 110
    assert summary.wall_time_seconds == 10 / 1_000_000_000


def test_record_process_can_store_bounded_output_explicitly(tmp_path: Path) -> None:
    output = tmp_path / "output.runpack"

    record_process(
        (
            sys.executable,
            "-c",
            "import sys; print('hello'); print('error', file=sys.stderr)",
        ),
        output,
        name="output",
        capture_output_limit=3,
    )

    with RunpackReader(output) as reader:
        attachments = {attachment.name: attachment for attachment in reader.attachments()}
    assert attachments["stdout"].content == b"hel"
    assert attachments["stdout"].attributes == {
        "captured_bytes": 3,
        "total_bytes": 6,
        "truncated": True,
    }
    assert attachments["stderr"].content == b"err"
    assert attachments["stderr"].attributes["total_bytes"] == 6


def test_record_process_rejects_unbounded_output_capture(tmp_path: Path) -> None:
    with pytest.raises(CaptureError, match="cannot exceed"):
        record_process(
            (sys.executable, "-c", "pass"),
            tmp_path / "too-large.runpack",
            name="too-large",
            capture_output_limit=64 * 1024 * 1024 + 1,
        )


def test_record_process_hashes_selected_environment_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "environment.runpack"
    monkeypatch.setenv("PYTHONHASHSEED", "environment-value")

    record_process((sys.executable, "-c", "pass"), output, name="environment")

    with RunpackReader(output) as reader:
        metadata = reader.execution().metadata
    environment = metadata["environment"]
    assert isinstance(environment, dict)
    identities = environment["selected_value_sha256"]
    assert isinstance(identities, dict)
    assert identities["PYTHONHASHSEED"] == hashlib.sha256(b"environment-value").hexdigest()
    assert "environment-value" not in json.dumps(metadata)
    capture_runtime = metadata["capture_runtime"]
    assert isinstance(capture_runtime, dict)
    assert capture_runtime["python_implementation"]
    assert "runtime" not in metadata


def test_capture_hashes_surrogate_escaped_environment_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = "value-\udcff"
    monkeypatch.setenv("PYTHONHASHSEED", value)

    metadata = capture._initial_metadata()

    environment = metadata["environment"]
    assert isinstance(environment, dict)
    identities = environment["selected_value_sha256"]
    assert isinstance(identities, dict)
    assert identities["PYTHONHASHSEED"] == hashlib.sha256(os.fsencode(value)).hexdigest()


def test_record_process_drains_output_after_relay_failure(tmp_path: Path) -> None:
    class BrokenSink(io.BytesIO):
        def write(self, data: Buffer, /) -> int:
            raise BrokenPipeError("consumer closed")

    output = tmp_path / "broken-relay.runpack"
    content_size = 2 * 1024 * 1024

    exit_code = record_process(
        (sys.executable, "-c", f"import sys; sys.stdout.write('x' * {content_size})"),
        output,
        name="broken-relay",
        stdout=BrokenSink(),
    )

    with RunpackReader(output) as reader:
        execution = reader.execution()
    output_metadata = execution.metadata["output"]
    assert isinstance(output_metadata, dict)
    stdout_metadata = output_metadata["stdout"]
    assert isinstance(stdout_metadata, dict)
    assert exit_code == 0
    assert stdout_metadata["bytes"] == content_size
    assert stdout_metadata["sha256"] == hashlib.sha256(b"x" * content_size).hexdigest()
    assert stdout_metadata["relay_error"] == "BrokenPipeError: consumer closed"


def test_record_process_completes_short_output_relay_writes(tmp_path: Path) -> None:
    class ShortSink(io.BytesIO):
        def write(self, data: Buffer, /) -> int:
            return super().write(memoryview(data)[:3])

    output = tmp_path / "short-relay.runpack"
    sink = ShortSink()
    content = b"complete relay"

    exit_code = record_process(
        (sys.executable, "-c", f"import sys; sys.stdout.buffer.write({content!r})"),
        output,
        name="short-relay",
        stdout=sink,
    )

    assert exit_code == 0
    assert sink.getvalue() == content
    with RunpackReader(output) as reader:
        execution = reader.execution()
    output_metadata = execution.metadata["output"]
    assert isinstance(output_metadata, dict)
    stdout_metadata = output_metadata["stdout"]
    assert isinstance(stdout_metadata, dict)
    assert "relay_error" not in stdout_metadata


def test_record_process_does_not_wait_for_descendants_holding_output_pipes(
    tmp_path: Path,
) -> None:
    output = tmp_path / "inherited-pipe.runpack"
    started = time.monotonic()

    exit_code = record_process(
        (
            sys.executable,
            "-c",
            (
                "import subprocess, sys; "
                "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(1.5)']); "
                "print('parent complete')"
            ),
        ),
        output,
        name="inherited-pipe",
    )

    elapsed = time.monotonic() - started
    with RunpackReader(output) as reader:
        execution = reader.execution()
    output_metadata = execution.metadata["output"]
    assert isinstance(output_metadata, dict)
    stdout_metadata = output_metadata["stdout"]
    stderr_metadata = output_metadata["stderr"]
    assert isinstance(stdout_metadata, dict)
    assert isinstance(stderr_metadata, dict)
    assert exit_code == 0
    assert elapsed < 1.0
    assert stdout_metadata["bytes"] == len(b"parent complete\n")
    assert stdout_metadata["pipe_open_after_exit"] is True
    assert stderr_metadata["pipe_open_after_exit"] is True
    summary = inspect_runpack(output)
    assert summary.stdout_complete is False
    assert summary.stderr_complete is False
    assert "stdout:   16 B, sha256:" in render_summary(summary, "text")
    assert "incomplete: pipe remained open after exit" in render_summary(summary, "text")


def test_output_drain_recognizes_eof_at_the_post_exit_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = tmp_path / "stream"
    stream.write_bytes(b"complete")
    process_done = threading.Event()
    process_done.set()
    monkeypatch.setattr(capture, "MAX_POST_EXIT_DRAIN_BYTES", 8)

    with stream.open("rb") as source:
        digest = capture._pump(source, None, None, process_done)

    assert digest.byte_count == 8
    assert digest.sha256 == hashlib.sha256(b"complete").hexdigest()
    assert digest.pipe_open_after_exit is False


def test_record_process_refuses_to_overwrite_an_artifact(tmp_path: Path) -> None:
    output = tmp_path / "existing.runpack"
    output.write_bytes(b"keep me")

    with pytest.raises(CaptureError, match="refusing to overwrite"):
        record_process((sys.executable, "-c", "pass"), output, name="existing")

    assert output.read_bytes() == b"keep me"


def test_record_process_normalizes_publication_failures_and_cleans_temporary_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "unpublished.runpack"

    def fail_publication(temporary: Path, destination: Path) -> None:
        raise OSError("hard links unavailable")

    monkeypatch.setattr(capture, "publish_without_overwrite", fail_publication)

    with pytest.raises(CaptureError, match="could not publish runpack.*hard links unavailable"):
        record_process((sys.executable, "-c", "pass"), output, name="unpublished")

    assert not output.exists()
    assert not tuple(tmp_path.glob(".unpublished.runpack.tmp-*"))


def test_reader_rejects_unknown_schema_major(tmp_path: Path) -> None:
    output = tmp_path / "future.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="future")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE manifest SET value = '2' WHERE key = 'schema_version'")

    with pytest.raises(UnsupportedSchemaError, match="unsupported runpack schema"):
        RunpackReader(output)


def test_reader_accepts_additive_schema_minor_versions(tmp_path: Path) -> None:
    output = tmp_path / "future-minor.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="future-minor")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE manifest SET value = '1.7' WHERE key = 'schema_version'")

    assert inspect_runpack(output).name == "future-minor"


def test_reader_accepts_schema_one_without_optional_attachments(tmp_path: Path) -> None:
    output = tmp_path / "schema-one.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="schema-one")
    with sqlite3.connect(output) as connection:
        connection.execute("DROP TABLE attachments")
        connection.execute("UPDATE manifest SET value = '1' WHERE key = 'schema_version'")

    summary = inspect_runpack(output)

    assert summary.name == "schema-one"
    assert summary.record_counts["attachments"] == 0


def test_reader_rejects_runpacks_missing_required_columns(tmp_path: Path) -> None:
    output = tmp_path / "missing-column.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="missing-column")
    with sqlite3.connect(output) as connection:
        connection.execute("ALTER TABLE events DROP COLUMN sequence")

    with pytest.raises(
        RunpackError,
        match="runpack table events is missing required columns: sequence",
    ):
        RunpackReader(output)


def test_reader_rejects_sqlite_files_without_runpack_identity(tmp_path: Path) -> None:
    output = tmp_path / "not-a-runpack.runpack"
    with sqlite3.connect(output) as connection:
        connection.execute("CREATE TABLE manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO manifest VALUES ('schema_version', '1')")

    with pytest.raises(RunpackError, match="not a Contrail runpack"):
        RunpackReader(output)


def test_reader_rejects_dangling_runpack_relationships(tmp_path: Path) -> None:
    output = tmp_path / "dangling-relationship.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(Event("work", "work", "work", "worker", 0, 1, None, None, None, {}))
    with sqlite3.connect(output) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DELETE FROM entities WHERE id = 'worker'")

    with pytest.raises(
        RunpackError, match="runpack contains an invalid relationship in events row"
    ):
        RunpackReader(output)


def test_reader_reports_malformed_embedded_json_as_runpack_error(tmp_path: Path) -> None:
    output = tmp_path / "malformed.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="malformed")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET metadata_json = '{'")

    with pytest.raises(RunpackError, match="invalid JSON object"):
        inspect_runpack(output)


def test_reader_rejects_non_finite_embedded_json_numbers(tmp_path: Path) -> None:
    output = tmp_path / "non-finite.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="non-finite")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET metadata_json = ?", ('{"value": 1e999}',))

    with pytest.raises(RunpackError, match="non-finite number"):
        inspect_runpack(output)


def test_reader_normalizes_excessively_nested_embedded_json(tmp_path: Path) -> None:
    output = tmp_path / "nested-json.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="nested-json")
    nested = '{"value":' + "[" * 2_000 + "0" + "]" * 2_000 + "}"
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET metadata_json = ?", (nested,))

    with pytest.raises(RunpackError, match="invalid JSON object in runpack"):
        inspect_runpack(output)


def test_writer_rejects_oversized_normalized_json(tmp_path: Path) -> None:
    output = tmp_path / "oversized-json-write.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="runpack JSON exceeds"):
            writer.add_execution(
                Execution(
                    "run",
                    "run",
                    0,
                    1,
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {"value": "x" * MAX_RUNPACK_JSON_BYTES},
                )
            )

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            reader.execution()


def test_reader_rejects_oversized_normalized_json(tmp_path: Path) -> None:
    output = tmp_path / "oversized-json-read.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="oversized-json")
    oversized = json.dumps({"value": "x" * MAX_RUNPACK_JSON_BYTES})
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET metadata_json = ?", (oversized,))

    with pytest.raises(RunpackError, match="runpack JSON exceeds"):
        inspect_runpack(output)


def test_writer_rejects_oversized_normalized_text(tmp_path: Path) -> None:
    output = tmp_path / "oversized-text-write.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="entity name exceeds"):
            writer.add_entity(
                Entity("entity", "service", "x" * (MAX_RUNPACK_TEXT_BYTES + 1), None, {})
            )

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_reader_rejects_oversized_normalized_text(tmp_path: Path) -> None:
    output = tmp_path / "oversized-text-read.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="oversized-text")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE events SET name = ?", ("x" * (MAX_RUNPACK_TEXT_BYTES + 1),))

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="event name exceeds"):
            reader.events()


def test_reader_rejects_oversized_execution_command_json(tmp_path: Path) -> None:
    output = tmp_path / "oversized-command.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="oversized-command")
    oversized = json.dumps(["x" * MAX_RUNPACK_JSON_BYTES])
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET command_json = ?", (oversized,))

    with pytest.raises(RunpackError, match="runpack JSON exceeds"):
        inspect_runpack(output)


def test_reader_normalizes_deeply_nested_execution_command_json(tmp_path: Path) -> None:
    output = tmp_path / "nested-command.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="nested-command")
    nested = "[" * 10_000 + '"command"' + "]" * 10_000
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET command_json = ?", (nested,))

    with pytest.raises(RunpackError, match="execution command is invalid JSON"):
        inspect_runpack(output)


def test_reader_rejects_reversed_execution_intervals(tmp_path: Path) -> None:
    output = tmp_path / "reversed.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="reversed")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET finished_at_ns = started_at_ns - 1")

    with pytest.raises(RunpackError, match="execution cannot finish before it starts"):
        inspect_runpack(output)


def test_reader_rejects_invalid_execution_exit_codes(tmp_path: Path) -> None:
    output = tmp_path / "invalid-exit-code.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="invalid-exit-code")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET exit_code = 'invalid'")

    with pytest.raises(RunpackError, match="execution exit code must be an integer or null"):
        inspect_runpack(output)


def test_reader_rejects_non_finite_measurements(tmp_path: Path) -> None:
    output = tmp_path / "non-finite-measurement.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="non-finite-measurement")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE measurements SET value = 1e999 WHERE id = 1")

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="measurement value must be finite"):
            reader.measurements()


@pytest.mark.parametrize(
    ("column", "value", "message"),
    (
        ("started_at_ns", "invalid", "event start timestamp must be an integer or null"),
        ("uncertainty_ns", "invalid", "event uncertainty must be an integer or null"),
        ("sequence", "invalid", "event sequence must be an integer or null"),
    ),
)
def test_reader_rejects_invalid_event_integer_fields(
    tmp_path: Path, column: str, value: str, message: str
) -> None:
    output = tmp_path / f"invalid-event-{column}.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="invalid-event")
    with sqlite3.connect(output) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(f"UPDATE events SET {column} = ?", (value,))

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match=message):
            reader.events()


def test_reader_rejects_empty_event_identity_fields(tmp_path: Path) -> None:
    output = tmp_path / "empty-event-name.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="empty-event-name")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE events SET name = ''")

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="event name must be a non-empty string"):
            reader.events()


def test_reader_rejects_invalid_causal_confidence(tmp_path: Path) -> None:
    output = tmp_path / "invalid-confidence.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_events(
            (
                Event("source", "event", "source", None, 0, 1, None, None, None, {}),
                Event("target", "event", "target", None, 0, 1, None, None, None, {}),
            )
        )
        writer.add_causal_edge(CausalEdge("source", "target", "causes", 1.0, {}))
    with sqlite3.connect(output) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("UPDATE causal_edges SET confidence = 2")

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="causal edge confidence must be between 0 and 1"):
            reader.causal_edges()


def test_reader_normalizes_invalid_attachment_content(tmp_path: Path) -> None:
    output = tmp_path / "invalid-attachment.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="invalid-attachment")
    with sqlite3.connect(output) as connection:
        connection.execute(
            "INSERT INTO attachments VALUES (?, ?, ?, ?, ?, ?)",
            ("bad", "output", "stdout", "text/plain", "not-a-blob", "{}"),
        )

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="invalid binary attachment content"):
            reader.attachments()


def test_writer_rejects_oversized_attachment_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "oversized-attachment-write.runpack"
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_BYTES", 8)
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="attachment content exceeds"):
            writer.add_attachments(
                (Attachment("large", "raw", "large", "application/octet-stream", b"x" * 9, {}),)
            )

    with RunpackReader(output) as reader:
        assert reader.attachments() == ()


def test_reader_rejects_oversized_attachment_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "oversized-attachment-read.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="oversized-attachment")
    with sqlite3.connect(output) as connection:
        connection.execute(
            "INSERT INTO attachments VALUES (?, ?, ?, ?, ?, ?)",
            ("large", "raw", "large", "application/octet-stream", b"x" * 9, "{}"),
        )
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_BYTES", 8)

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="attachment content exceeds"):
            reader.attachments()


def test_writer_rejects_oversized_aggregate_attachment_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "oversized-attachments-write.runpack"
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES", 5)
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="aggregate runpack limit"):
            writer.add_attachments(
                (
                    Attachment("first", "raw", "first", "text/plain", b"abc", {}),
                    Attachment("second", "raw", "second", "text/plain", b"def", {}),
                )
            )

    with RunpackReader(output) as reader:
        assert reader.attachments() == ()


def test_reader_rejects_oversized_aggregate_attachment_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "oversized-attachments-read.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="oversized-attachments")
    with sqlite3.connect(output) as connection:
        connection.executemany(
            "INSERT INTO attachments VALUES (?, ?, ?, ?, ?, ?)",
            (
                ("first", "raw", "first", "text/plain", b"abc", "{}"),
                ("second", "raw", "second", "text/plain", b"def", "{}"),
            ),
        )
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES", 5)

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="aggregate runpack limit"):
            reader.attachments()


def test_writer_reports_identity_collisions_as_runpack_errors(tmp_path: Path) -> None:
    output = tmp_path / "collision.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="UNIQUE constraint failed"):
            writer.add_entities(
                (
                    Entity("same", "worker", "first", None, {}),
                    Entity("same", "worker", "second", None, {}),
                )
            )

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_writer_rejects_self_parented_entities(tmp_path: Path) -> None:
    output = tmp_path / "self-parent.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        with pytest.raises(RunpackError, match="entity cannot be its own parent"):
            writer.add_entity(Entity("worker", "worker", "worker", "worker", {}))

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_bulk_entity_write_accepts_children_before_parents(tmp_path: Path) -> None:
    output = tmp_path / "unordered-entities.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entities(
            (
                Entity("container", "container", "container", "pod", {}),
                Entity("pod", "pod", "pod", "job", {}),
                Entity("job", "job", "job", None, {}),
            )
        )

    with RunpackReader(output) as reader:
        entities = {entity.id: entity for entity in reader.entities()}
    assert entities["container"].parent_entity_id == "pod"
    assert entities["pod"].parent_entity_id == "job"


def test_bulk_entity_write_rejects_parent_cycles_before_inserting(tmp_path: Path) -> None:
    output = tmp_path / "cyclic-entities.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        with pytest.raises(RunpackError, match="entity parent relationships contain a cycle"):
            writer.add_entities(
                (
                    Entity("first", "worker", "first", "second", {}),
                    Entity("second", "worker", "second", "first", {}),
                )
            )

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_reader_rejects_cyclic_entity_parent_relationships(tmp_path: Path) -> None:
    output = tmp_path / "entity-cycle.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("first", "worker", "first", None, {}))
        writer.add_entity(Entity("second", "worker", "second", "first", {}))
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE entities SET parent_entity_id = 'second' WHERE id = 'first'")

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="entity parent relationships contain a cycle"):
            reader.entities()


def test_writer_refuses_to_modify_an_existing_file(tmp_path: Path) -> None:
    output = tmp_path / "existing.runpack"
    output.write_bytes(b"preserve this")

    with pytest.raises(RunpackError, match="refusing to overwrite existing runpack"):
        RunpackWriter(output)

    assert output.read_bytes() == b"preserve this"


def test_writer_creates_private_artifacts(tmp_path: Path) -> None:
    output = tmp_path / "private.runpack"

    with RunpackWriter(output):
        pass

    assert output.stat().st_mode & 0o777 == 0o600


def test_writer_normalizes_invalid_json_values(tmp_path: Path) -> None:
    output = tmp_path / "invalid-json.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="invalid JSON value for runpack"):
            writer.add_entity(Entity("entity", "service", "service", None, {"bad": float("nan")}))

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_writer_rejects_json_objects_with_non_string_keys(tmp_path: Path) -> None:
    output = tmp_path / "invalid-json-key.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="invalid JSON value for runpack"):
            writer.add_entity(Entity("entity", "service", "service", None, cast(Any, {1: "value"})))

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_writer_rejects_invalid_execution_commands_before_writing(tmp_path: Path) -> None:
    output = tmp_path / "invalid-command.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="execution command must be a tuple of strings"):
            writer.add_execution(
                Execution(
                    "run",
                    "run",
                    0,
                    1,
                    cast(Any, ("python", 1)),
                    str(tmp_path),
                    0,
                    None,
                    {},
                )
            )

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            reader.execution()


def test_writer_rejects_empty_execution_identity_before_writing(tmp_path: Path) -> None:
    output = tmp_path / "empty-execution-id.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="execution id must be a non-empty string"):
            writer.add_execution(Execution("", "run", 0, 1, (), str(tmp_path), 0, None, {}))

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            reader.execution()


def test_writer_rejects_surrogate_text_before_writing(tmp_path: Path) -> None:
    output = tmp_path / "surrogate-text.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="entity name must be valid UTF-8"):
            writer.add_entity(Entity("entity", "service", "bad-\udcff", None, {}))

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_bulk_entity_write_rolls_back_empty_semantic_fields(tmp_path: Path) -> None:
    output = tmp_path / "empty-entity-kind.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="entity kind must be a non-empty string"):
            writer.add_entities(
                (
                    Entity("valid", "service", "valid", None, {}),
                    Entity("invalid", "", "invalid", None, {}),
                )
            )

    with RunpackReader(output) as reader:
        assert reader.entities() == ()


def test_bulk_measurement_write_rolls_back_non_finite_values(tmp_path: Path) -> None:
    output = tmp_path / "invalid-measurement.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="measurement value must be finite"):
            writer.add_measurements(
                (
                    Measurement("valid", 1.0, "1", 0, None, {}),
                    Measurement("invalid", float("inf"), "1", 1, None, {}),
                )
            )

    with RunpackReader(output) as reader:
        assert reader.measurements() == ()


def test_writer_rejects_reversed_execution_intervals(tmp_path: Path) -> None:
    output = tmp_path / "reversed-execution.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="execution cannot finish before it starts"):
            writer.add_execution(Execution("run", "run", 10, 9, (), str(tmp_path), 0, None, {}))

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            reader.execution()


def test_writer_rejects_boolean_execution_exit_codes(tmp_path: Path) -> None:
    output = tmp_path / "boolean-exit-code.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="execution exit code must be an integer or null"):
            writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), True, None, {}))

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            reader.execution()


def test_writer_rejects_a_second_execution(tmp_path: Path) -> None:
    output = tmp_path / "multiple-executions.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("first", "first", 0, 1, (), str(tmp_path), 0, None, {}))
        with pytest.raises(RunpackError, match="runpack already contains an execution"):
            writer.add_execution(
                Execution("second", "second", 0, 1, (), str(tmp_path), 0, None, {})
            )

    with RunpackReader(output) as reader:
        assert reader.execution().id == "first"


@pytest.mark.parametrize(
    ("started_at_ns", "finished_at_ns", "message"),
    (
        (True, 2, "execution start timestamp must be an integer"),
        (2, False, "execution finish timestamp must be an integer or null"),
        (2, 1, "execution cannot finish before it starts"),
    ),
)
def test_writer_rejects_invalid_execution_bound_expansions(
    tmp_path: Path,
    started_at_ns: int,
    finished_at_ns: int | None,
    message: str,
) -> None:
    output = tmp_path / "invalid-expansion.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        with pytest.raises(RunpackError, match=message):
            writer.expand_execution_bounds(started_at_ns, finished_at_ns)

    with RunpackReader(output) as reader:
        execution = reader.execution()
    assert (execution.started_at_ns, execution.finished_at_ns) == (0, 1)


def test_execution_bound_expansion_requires_an_execution(tmp_path: Path) -> None:
    output = tmp_path / "empty-expansion.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="exactly one execution to expand"):
            writer.expand_execution_bounds(0, 1)


def test_execution_bound_expansion_does_not_close_an_open_execution(tmp_path: Path) -> None:
    output = tmp_path / "open-expansion.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 10, None, (), str(tmp_path), None, None, {}))
        writer.expand_execution_bounds(5, 20)

    with RunpackReader(output) as reader:
        execution = reader.execution()
    assert execution.started_at_ns == 5
    assert execution.finished_at_ns is None


def test_writer_rejects_reversed_interval_when_finishing_execution(tmp_path: Path) -> None:
    output = tmp_path / "reversed-finish.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 10, None, (), str(tmp_path), None, None, {}))
        with pytest.raises(RunpackError, match="execution cannot finish before it starts"):
            writer.finish_execution(
                "run",
                finished_at_ns=9,
                exit_code=0,
                metadata={},
                event=Event(
                    "process",
                    "process.run",
                    "process",
                    None,
                    10,
                    9,
                    "host.wall",
                    None,
                    None,
                    {},
                ),
                measurements=(),
            )

    with RunpackReader(output) as reader:
        assert reader.execution().finished_at_ns is None
        assert reader.events() == ()


def test_writer_rejects_invalid_exit_code_when_finishing_execution(tmp_path: Path) -> None:
    output = tmp_path / "invalid-finish-exit-code.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, None, (), str(tmp_path), None, None, {}))
        with pytest.raises(RunpackError, match="execution exit code must be an integer"):
            writer.finish_execution(
                "run",
                finished_at_ns=1,
                exit_code=True,
                metadata={},
                event=Event(
                    "process",
                    "process.run",
                    "process",
                    None,
                    0,
                    1,
                    "host.wall",
                    None,
                    None,
                    {},
                ),
                measurements=(),
            )

    with RunpackReader(output) as reader:
        assert reader.execution().finished_at_ns is None
        assert reader.events() == ()


def test_bulk_event_write_rejects_boolean_timestamps_and_rolls_back(tmp_path: Path) -> None:
    output = tmp_path / "invalid-event.runpack"
    valid = Event("valid", "event", "valid", None, 0, 1, "test", None, None, {})
    invalid = Event("invalid", "event", "invalid", None, True, 1, "test", None, None, {})
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="event start timestamp must be an integer"):
            writer.add_events((valid, invalid))

    with RunpackReader(output) as reader:
        assert reader.events() == ()


def test_writer_rejects_boolean_causal_confidence(tmp_path: Path) -> None:
    output = tmp_path / "invalid-edge.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="causal edge confidence must be between 0 and 1"):
            writer.add_causal_edge(CausalEdge("source", "target", "causes", True, {}))


def test_writer_rejects_self_referencing_causal_edges(tmp_path: Path) -> None:
    output = tmp_path / "self-edge.runpack"
    with RunpackWriter(output) as writer:
        writer.add_event(Event("event", "event", "event", None, 0, 1, None, None, None, {}))
        with pytest.raises(RunpackError, match="cannot reference the same event twice"):
            writer.add_causal_edge(CausalEdge("event", "event", "causes", 1.0, {}))

    with RunpackReader(output) as reader:
        assert reader.causal_edges() == ()


def test_reader_rejects_self_referencing_causal_edges(tmp_path: Path) -> None:
    output = tmp_path / "corrupt-self-edge.runpack"
    with RunpackWriter(output) as writer:
        writer.add_event(Event("event", "event", "event", None, 0, 1, None, None, None, {}))
    with sqlite3.connect(output) as connection:
        connection.execute(
            "INSERT INTO causal_edges VALUES (?, ?, ?, ?, ?)",
            ("event", "event", "causes", 1.0, "{}"),
        )

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="cannot reference the same event twice"):
            reader.causal_edges()


def test_writer_rejects_boolean_measurement_timestamps(tmp_path: Path) -> None:
    output = tmp_path / "invalid-measurement-time.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="measurement timestamp must be an integer"):
            writer.add_measurement(Measurement("value", 1.0, "1", True, None, {}))


def test_bulk_attachment_write_rolls_back_non_binary_content(tmp_path: Path) -> None:
    output = tmp_path / "invalid-attachment-write.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="invalid binary attachment content"):
            writer.add_attachments(
                (
                    Attachment("valid", "raw", "valid", "text/plain", b"valid", {}),
                    Attachment(
                        "invalid",
                        "raw",
                        "invalid",
                        "text/plain",
                        cast(Any, "not bytes"),
                        {},
                    ),
                )
            )

    with RunpackReader(output) as reader:
        assert reader.attachments() == ()


def test_event_graph_write_rolls_back_events_when_an_edge_is_invalid(tmp_path: Path) -> None:
    output = tmp_path / "invalid-event-graph.runpack"
    event = Event("event", "event", "event", None, 0, 1, "test", None, None, {})
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        with pytest.raises(RunpackError, match="FOREIGN KEY constraint failed"):
            writer.add_event_graph(
                (event,),
                (CausalEdge("event", "missing", "causes", 1.0, {}),),
            )

    with RunpackReader(output) as reader:
        assert reader.events() == ()
        assert reader.causal_edges() == ()


def test_each_capture_reports_its_own_child_peak_memory(tmp_path: Path) -> None:
    large = tmp_path / "large.runpack"
    small = tmp_path / "small.runpack"
    record_process(
        (sys.executable, "-c", "value = bytearray(50_000_000); print(len(value))"),
        large,
        name="large",
    )
    record_process((sys.executable, "-c", "print('small')"), small, name="small")

    large_peak = inspect_runpack(large).peak_memory_bytes
    small_peak = inspect_runpack(small).peak_memory_bytes
    assert large_peak is not None and small_peak is not None
    assert large_peak > small_peak + 20_000_000
