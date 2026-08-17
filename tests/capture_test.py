from __future__ import annotations

import hashlib
import io
import sqlite3
import sys
from pathlib import Path

import pytest

from runtime_tools import CaptureError, inspect_runpack, record_process
from runtime_tools.storage import RunpackReader, UnsupportedSchemaError


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
    }


def test_record_process_preserves_nonzero_exit_as_execution_outcome(tmp_path: Path) -> None:
    output = tmp_path / "failed.runpack"

    exit_code = record_process((sys.executable, "-c", "raise SystemExit(7)"), output, name="failed")

    assert exit_code == 7
    assert inspect_runpack(output).exit_code == 7


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
