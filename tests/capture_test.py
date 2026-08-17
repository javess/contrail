from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import sys
from collections.abc import Buffer
from pathlib import Path
from typing import Any, cast

import pytest

from runtime_tools import CaptureError, inspect_runpack, record_process
from runtime_tools.model import Attachment, CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter, UnsupportedSchemaError


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


def test_record_process_refuses_to_overwrite_an_artifact(tmp_path: Path) -> None:
    output = tmp_path / "existing.runpack"
    output.write_bytes(b"keep me")

    with pytest.raises(CaptureError, match="refusing to overwrite"):
        record_process((sys.executable, "-c", "pass"), output, name="existing")

    assert output.read_bytes() == b"keep me"


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


def test_reader_rejects_sqlite_files_without_runpack_identity(tmp_path: Path) -> None:
    output = tmp_path / "not-a-runpack.runpack"
    with sqlite3.connect(output) as connection:
        connection.execute("CREATE TABLE manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO manifest VALUES ('schema_version', '1')")

    with pytest.raises(RunpackError, match="not a Contrail runpack"):
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


def test_reader_rejects_reversed_execution_intervals(tmp_path: Path) -> None:
    output = tmp_path / "reversed.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="reversed")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET finished_at_ns = started_at_ns - 1")

    with pytest.raises(RunpackError, match="execution cannot finish before it starts"):
        inspect_runpack(output)


def test_reader_rejects_non_finite_measurements(tmp_path: Path) -> None:
    output = tmp_path / "non-finite-measurement.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="non-finite-measurement")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE measurements SET value = 1e999 WHERE id = 1")

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="measurement value must be finite"):
            reader.measurements()


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


def test_writer_normalizes_invalid_json_values(tmp_path: Path) -> None:
    output = tmp_path / "invalid-json.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="invalid JSON value for runpack"):
            writer.add_entity(Entity("entity", "service", "service", None, {"bad": float("nan")}))

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
