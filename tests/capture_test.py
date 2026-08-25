from __future__ import annotations

import hashlib
import io
import json
import os
import select
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Buffer, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from runtime_tools import (
    CaptureError,
    __version__,
    capture,
    inspect_runpack,
    record_process,
    storage,
)
from runtime_tools.annotations import AnnotationError
from runtime_tools.inspect import render_summary
from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    JsonValue,
    Measurement,
)
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
    assert summary.producer_version == __version__
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


def test_record_process_succeeds_when_annotation_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "captured.runpack"
    unlink = Path.unlink

    def fail_annotation_cleanup(path: Path, missing_ok: bool = False) -> None:
        if ".annotations-" in path.name:
            raise OSError("simulated cleanup failure")
        unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_annotation_cleanup)

    exit_code = record_process((sys.executable, "-c", "pass"), output, name="captured")

    assert exit_code == 0
    assert inspect_runpack(output).exit_code == 0


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


def test_record_process_excludes_prelaunch_setup_from_execution_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "setup-boundary.runpack"
    clock = {"now": 100}
    revision = "a" * 40
    monkeypatch.setattr(
        capture,
        "time",
        SimpleNamespace(
            time_ns=lambda: clock["now"],
            perf_counter_ns=lambda: clock["now"],
        ),
    )

    def delayed_revision(_cwd: Path) -> str:
        clock["now"] += 2_000_000_000
        return revision

    monkeypatch.setattr(capture, "_git_revision", delayed_revision)

    record_process((sys.executable, "-c", "pass"), output, name="setup-boundary")

    summary = inspect_runpack(output)
    assert summary.started_at_ns == 2_000_000_100
    assert summary.finished_at_ns == 2_000_000_100
    assert summary.wall_time_seconds == 0
    assert summary.revision == revision


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


def test_record_process_rejects_boolean_output_limits(tmp_path: Path) -> None:
    with pytest.raises(CaptureError, match="capture output limit must be an integer"):
        record_process(
            (sys.executable, "-c", "pass"),
            tmp_path / "boolean-limit.runpack",
            name="boolean-limit",
            capture_output_limit=True,
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


def test_record_process_hashes_custom_environment_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "custom-environment.runpack"
    monkeypatch.setenv("CONTRAIL_TEST_FEATURE_MODE", "experimental")

    record_process(
        (sys.executable, "-c", "pass"),
        output,
        name="environment",
        identify_environment=("CONTRAIL_TEST_FEATURE_MODE",),
    )

    with RunpackReader(output) as reader:
        environment = reader.execution().metadata["environment"]
    assert isinstance(environment, dict)
    identities = environment["selected_value_sha256"]
    assert isinstance(identities, dict)
    assert identities["CONTRAIL_TEST_FEATURE_MODE"] == hashlib.sha256(b"experimental").hexdigest()
    assert "experimental" not in json.dumps(environment)


def test_record_process_cwd_sets_and_identifies_the_child_pwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    working_directory = tmp_path / "work"
    working_directory.mkdir()
    stale_pwd = tmp_path / "stale"
    monkeypatch.setenv("PWD", str(stale_pwd))
    output = tmp_path / "pwd.runpack"

    record_process(
        (
            sys.executable,
            "-c",
            "import os; from pathlib import Path; print(os.environ['PWD']); print(Path.cwd())",
        ),
        output,
        name="pwd",
        cwd=working_directory,
        capture_output_limit=4_096,
        identify_environment=("PWD",),
    )

    with RunpackReader(output) as reader:
        execution = reader.execution()
        stdout = next(
            attachment.content for attachment in reader.attachments() if attachment.name == "stdout"
        )
    resolved_working_directory = str(working_directory.resolve())
    assert stdout.decode() == f"{resolved_working_directory}\n{resolved_working_directory}\n"
    environment = execution.metadata["environment"]
    assert isinstance(environment, dict)
    identities = environment["selected_value_sha256"]
    assert isinstance(identities, dict)
    assert identities["PWD"] == hashlib.sha256(os.fsencode(resolved_working_directory)).hexdigest()


@pytest.mark.parametrize(
    ("names", "message"),
    (
        (cast(Any, ["FEATURE_MODE"]), "must be a tuple of strings"),
        (("",), "must be non-empty"),
        (("BAD=NAME",), "cannot contain '=' or NUL"),
        (("BAD\0NAME",), "cannot contain '=' or NUL"),
        (("bad-\udcff",), "must be valid UTF-8"),
        ((("x" * 1025),), "cannot exceed 1024 UTF-8 bytes"),
        (("MISSING",) * 257, "cannot identify more than 256"),
    ),
)
def test_record_process_rejects_invalid_custom_environment_names(
    tmp_path: Path,
    names: tuple[str, ...],
    message: str,
) -> None:
    output = tmp_path / "invalid-environment.runpack"

    with pytest.raises(CaptureError, match=message):
        record_process(
            (sys.executable, "-c", "pass"),
            output,
            name="environment",
            identify_environment=names,
        )

    assert not output.exists()


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
    summary = inspect_runpack(output)
    assert summary.stdout_relay_error == "BrokenPipeError: consumer closed"
    assert summary.stderr_relay_error is None
    assert "stdout relay: failed (BrokenPipeError: consumer closed)" in render_summary(
        summary, "text"
    )
    assert json.loads(render_summary(summary, "json"))["stdout_relay_error"] == (
        "BrokenPipeError: consumer closed"
    )


def test_record_process_normalizes_invalid_unicode_in_relay_errors(tmp_path: Path) -> None:
    class InvalidUnicodeSink(io.BytesIO):
        def write(self, data: Buffer, /) -> int:
            raise OSError("bad-\ud800")

    output = tmp_path / "invalid-relay-error.runpack"

    exit_code = record_process(
        (sys.executable, "-c", "print('result')"),
        output,
        name="invalid-relay-error",
        stdout=InvalidUnicodeSink(),
    )

    with RunpackReader(output) as reader:
        metadata = reader.execution().metadata
    output_metadata = metadata["output"]
    assert isinstance(output_metadata, dict)
    stdout_metadata = output_metadata["stdout"]
    assert isinstance(stdout_metadata, dict)
    assert exit_code == 0
    assert stdout_metadata["relay_error"] == r"OSError: bad-\ud800"


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


def test_output_drain_retries_interrupted_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = tmp_path / "stream"
    stream.write_bytes(b"complete")
    process_done = threading.Event()
    process_done.set()
    real_read = os.read
    calls = 0

    def interrupted_once(descriptor: int, size: int) -> bytes:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InterruptedError
        return real_read(descriptor, size)

    monkeypatch.setattr("runtime_tools.capture.os.read", interrupted_once)

    with stream.open("rb") as source:
        digest = capture._pump(source, None, None, process_done)

    assert digest.byte_count == 8
    assert digest.sha256 == hashlib.sha256(b"complete").hexdigest()


def test_output_drain_retries_interrupted_select(monkeypatch: pytest.MonkeyPatch) -> None:
    read_descriptor, write_descriptor = os.pipe()
    os.write(write_descriptor, b"complete")
    os.close(write_descriptor)
    process_done = threading.Event()
    real_select = select.select
    calls = 0

    def interrupted_once(*args: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InterruptedError
        return real_select(*args)

    monkeypatch.setattr("runtime_tools.capture.select.select", interrupted_once)

    with os.fdopen(read_descriptor, "rb") as source:
        digest = capture._pump(source, None, None, process_done)

    assert digest.byte_count == 8
    assert digest.sha256 == hashlib.sha256(b"complete").hexdigest()


@pytest.mark.parametrize("wait_error", (False, True))
def test_process_cleanup_reaps_a_leader_after_its_group_vanishes(
    wait_error: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    waits: list[float | None] = []

    class VanishedProcess:
        pid = 123

        def wait(self, timeout: float | None = None) -> int:
            waits.append(timeout)
            if wait_error:
                raise ChildProcessError
            return 0

    def vanished_group(process_group: int, signal_number: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(os, "killpg", vanished_group)

    capture._terminate_and_reap(cast(subprocess.Popen[bytes], VanishedProcess()))

    assert waits == [capture.PROCESS_TERMINATION_TIMEOUT_SECONDS]


def test_process_cleanup_escalates_a_surviving_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signals: list[int] = []

    class SurvivingProcess:
        pid = 123

        def wait(self, timeout: float | None = None) -> int:
            return 0

    def surviving_group(process_group: int, signal_number: int) -> None:
        signals.append(signal_number)

    monkeypatch.setattr(os, "killpg", surviving_group)
    monkeypatch.setattr(capture, "PROCESS_TERMINATION_TIMEOUT_SECONDS", 0.0)

    capture._terminate_and_reap(cast(subprocess.Popen[bytes], SurvivingProcess()))

    assert signals == [signal.SIGTERM, 0, 0, signal.SIGKILL]


def test_process_cleanup_remains_bounded_when_group_signals_are_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waits: list[float | None] = []

    class UnsignalableProcess:
        pid = 123

        def wait(self, timeout: float | None = None) -> int:
            waits.append(timeout)
            raise subprocess.TimeoutExpired("child", timeout if timeout is not None else 0.0)

    def deny_signal(process_group: int, signal_number: int) -> None:
        raise PermissionError

    monkeypatch.setattr(os, "killpg", deny_signal)
    monkeypatch.setattr(capture, "PROCESS_TERMINATION_TIMEOUT_SECONDS", 0.0)

    capture._terminate_and_reap(cast(subprocess.Popen[bytes], UnsignalableProcess()))

    assert waits == [0.0, 0.0]


def test_capture_normalizes_child_status_collection_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = SimpleNamespace(pid=123, returncode=None)

    def fail_wait(pid: int, options: int) -> None:
        raise ChildProcessError("status was collected elsewhere")

    monkeypatch.setattr(os, "wait4", fail_wait)

    with pytest.raises(CaptureError, match="could not collect captured process status"):
        capture._wait_with_usage(cast(subprocess.Popen[bytes], process))


def test_capture_ignores_optional_git_revision_os_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_git(*args: Any, **kwargs: Any) -> None:
        raise PermissionError("git cannot execute")

    monkeypatch.setattr(subprocess, "run", fail_git)

    assert capture._git_revision(tmp_path) is None


@pytest.mark.parametrize("revision", ("not-a-revision", "a" * 39, "g" * 40, "a" * 65))
def test_capture_ignores_malformed_optional_git_revisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    revision: str,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=f"{revision}\n"),
    )

    assert capture._git_revision(tmp_path) is None


def test_capture_ignores_undecodable_optional_git_revisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_git(*args: Any, **kwargs: Any) -> None:
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    monkeypatch.setattr(subprocess, "run", fail_git)

    assert capture._git_revision(tmp_path) is None


@pytest.mark.parametrize("length", (40, 64))
def test_capture_normalizes_valid_optional_git_revisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    length: int,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=f"{'A' * length}\n"),
    )

    assert capture._git_revision(tmp_path) == "a" * length


def test_record_process_refuses_to_overwrite_an_artifact(tmp_path: Path) -> None:
    output = tmp_path / "existing.runpack"
    output.write_bytes(b"keep me")

    with pytest.raises(CaptureError, match="refusing to overwrite"):
        record_process((sys.executable, "-c", "pass"), output, name="existing")

    assert output.read_bytes() == b"keep me"


def test_record_process_refuses_to_overwrite_a_dangling_symlink(tmp_path: Path) -> None:
    output = tmp_path / "existing.runpack"
    output.symlink_to(tmp_path / "missing.runpack")

    with pytest.raises(CaptureError, match="refusing to overwrite"):
        record_process(("missing-command",), output, name="existing")

    assert output.is_symlink()


def test_record_process_preserves_a_colliding_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "collision.runpack"

    class FixedUuid:
        hex = "fixed"

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    temporary = tmp_path / ".collision.runpack.tmp-fixed"
    temporary.write_bytes(b"preserve me")

    with pytest.raises(RunpackError, match="refusing to overwrite existing runpack"):
        record_process((sys.executable, "-c", "pass"), output, name="collision")

    assert temporary.read_bytes() == b"preserve me"
    assert not (tmp_path / ".collision.runpack.annotations-fixed").exists()
    assert not output.exists()


def test_record_process_preserves_a_colliding_annotation_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "collision.runpack"

    class FixedUuid:
        hex = "fixed"

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    annotations = tmp_path / ".collision.runpack.annotations-fixed"
    annotations.write_bytes(b"preserve me")

    with pytest.raises(CaptureError, match="temporary annotation file already exists"):
        record_process((sys.executable, "-c", "pass"), output, name="collision")

    assert annotations.read_bytes() == b"preserve me"
    assert not output.exists()
    assert not (tmp_path / ".collision.runpack.tmp-fixed").exists()


@pytest.mark.parametrize("descriptor", (cast(Any, "3"), True, -1, 0, 2))
def test_record_process_rejects_invalid_annotation_transport_fds(
    tmp_path: Path, descriptor: int
) -> None:
    output = tmp_path / "invalid-fd.runpack"

    with pytest.raises(CaptureError, match="open POSIX descriptor above 2"):
        record_process(
            (sys.executable, "-c", "pass"),
            output,
            name="invalid-fd",
            _annotation_fd=descriptor,
        )

    assert not output.exists()


def test_record_process_restores_reserved_annotation_fd_after_launch_failure(
    tmp_path: Path,
) -> None:
    output = tmp_path / "launch-failure.runpack"
    descriptor = os.open(os.devnull, os.O_RDONLY)
    try:
        with pytest.raises(CaptureError, match="could not start command"):
            record_process(
                ("missing-contrail-command",),
                output,
                name="launch-failure",
                _annotation_fd=descriptor,
            )

        os.fstat(descriptor)
    finally:
        os.close(descriptor)

    assert not output.exists()
    assert not tuple(tmp_path.glob(".launch-failure.runpack.*"))


def test_record_process_does_not_replace_an_unreserved_annotation_fd(tmp_path: Path) -> None:
    output = tmp_path / "unreserved.runpack"
    unreserved = tmp_path / "unreserved.txt"
    descriptor = os.open(unreserved, os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        with pytest.raises(CaptureError, match="reserved non-inheritable devnull fd"):
            record_process(
                (sys.executable, "-c", "pass"),
                output,
                name="unreserved",
                _annotation_fd=descriptor,
            )
        os.write(descriptor, b"preserved")
    finally:
        os.close(descriptor)

    assert unreserved.read_bytes() == b"preserved"
    assert not output.exists()


@pytest.mark.parametrize(
    ("command", "message"),
    (
        (cast(Any, [sys.executable]), "command must be a tuple of strings"),
        (("",), "command executable must be non-empty"),
        (("bad\0command",), "command arguments cannot contain NUL bytes"),
        (("bad-\udcff",), "command arguments must be valid UTF-8"),
    ),
)
def test_record_process_rejects_invalid_command_shapes_before_creating_artifacts(
    tmp_path: Path,
    command: tuple[str, ...],
    message: str,
) -> None:
    output = tmp_path / "invalid-command.runpack"

    with pytest.raises(CaptureError, match=message):
        record_process(command, output, name="invalid-command")

    assert not output.exists()
    assert not tuple(tmp_path.glob(".invalid-command.*"))


@pytest.mark.parametrize(
    ("name", "message"),
    (
        (cast(Any, True), "capture name must be a non-empty string"),
        ("", "capture name must be a non-empty string"),
        ("bad-\udcff", "capture name must be valid UTF-8"),
    ),
)
def test_record_process_rejects_invalid_names_before_creating_artifacts(
    tmp_path: Path,
    name: str,
    message: str,
) -> None:
    output = tmp_path / "invalid-name.runpack"

    with pytest.raises(CaptureError, match=message):
        record_process((sys.executable, "-c", "pass"), output, name=name)

    assert not output.exists()
    assert not tuple(tmp_path.glob(".invalid-name.*"))


def test_record_process_normalizes_working_directory_symlink_loops(tmp_path: Path) -> None:
    working_directory = tmp_path / "loop"
    working_directory.symlink_to(working_directory.name)
    output = tmp_path / "unresolved.runpack"

    with pytest.raises(CaptureError, match="could not resolve the capture working directory"):
        record_process(
            (sys.executable, "-c", "pass"),
            output,
            name="unresolved",
            cwd=working_directory,
        )

    assert not output.exists()
    assert not tuple(tmp_path.glob(".unresolved.*"))


def test_record_process_retains_a_recoverable_checkpoint_after_publication_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "unpublished.runpack"

    def fail_publication(temporary: Path, destination: Path) -> None:
        raise OSError("hard links unavailable")

    monkeypatch.setattr(capture, "publish_without_overwrite", fail_publication)

    with pytest.raises(CaptureError, match="could not publish runpack.*hard links unavailable"):
        record_process((sys.executable, "-c", "pass"), output, name="unpublished")

    assert not output.exists()
    checkpoints = tuple(tmp_path.glob(".unpublished.runpack.tmp-*"))
    assert len(checkpoints) == 1

    monkeypatch.undo()
    recovered_exit_code = capture.recover_process_capture(checkpoints[0], output)

    assert recovered_exit_code == 0
    assert output.is_file()
    assert not checkpoints[0].exists()
    with RunpackReader(output) as reader:
        capture_metadata = cast(
            dict[str, JsonValue],
            reader.execution().metadata["capture"],
        )
        recovery = capture_metadata["recovery"]
    assert recovery == {
        "format_version": 1,
        "status": "complete",
        "checkpoint": "post-exit",
        "controller_restart_recovered": True,
    }


def test_record_process_bounds_annotation_diagnostics_without_losing_core_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "annotation-diagnostic.runpack"

    def fail_annotations(path: Path, *, entity_id: str) -> None:
        raise AnnotationError("x" * 10_000)

    monkeypatch.setattr(capture, "load_annotations", fail_annotations)

    exit_code = record_process(
        (sys.executable, "-c", "pass"),
        output,
        name="annotation-diagnostic",
    )

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error is not None
    assert len(summary.annotation_error) == 4_096
    assert summary.annotation_error.endswith("…")
    assert summary.record_counts["events"] == 1


def test_record_process_terminates_child_when_capture_is_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "interrupted.runpack"
    children: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen

    def tracked_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        child = cast("subprocess.Popen[bytes]", real_popen(*args, **kwargs))
        children.append(child)
        return child

    def interrupt_wait(
        process: subprocess.Popen[bytes], output_pumps: tuple[object, ...] = ()
    ) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(subprocess, "Popen", tracked_popen)
    monkeypatch.setattr(capture, "_wait_with_usage", interrupt_wait)

    with pytest.raises(KeyboardInterrupt):
        record_process(
            (sys.executable, "-c", "import time; time.sleep(30)"),
            output,
            name="interrupted",
        )

    assert children[-1].poll() is not None
    assert not output.exists()
    assert not tuple(tmp_path.glob(".interrupted.runpack.tmp-*"))


def test_sample_capture_recovers_after_post_exit_controller_kill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "controller-loss.runpack"
    checkpoint_ready = tmp_path / "checkpoint-ready"

    def block_after_checkpoint(*args: Any, **kwargs: Any) -> None:
        checkpoint_ready.write_text("ready", encoding="utf-8")
        time.sleep(30)

    monkeypatch.setattr(capture, "_assemble_profile_checkpoint", block_after_checkpoint)
    controller_pid = os.fork()
    if controller_pid == 0:
        try:
            record_process(
                (sys.executable, "-c", "import time; time.sleep(0.12)"),
                output,
                name="controller-loss",
                capture_level="sample",
            )
        finally:
            os._exit(0)

    try:
        deadline = time.monotonic() + 10
        while not checkpoint_ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert checkpoint_ready.exists()
        os.kill(controller_pid, signal.SIGKILL)
        waited_pid, status = os.waitpid(controller_pid, 0)
        assert waited_pid == controller_pid
        assert os.waitstatus_to_exitcode(status) == -signal.SIGKILL

        checkpoints = tuple(tmp_path.glob(".controller-loss.runpack.tmp-*"))
        profile_directories = tuple(tmp_path.glob(".contrail-sample-profile-*"))
        assert len(checkpoints) == 1
        assert len(profile_directories) == 1

        monkeypatch.undo()
        recovered_exit_code = capture.recover_process_capture(checkpoints[0], output)

        assert recovered_exit_code == 0
        assert output.is_file()
        assert not checkpoints[0].exists()
        assert not profile_directories[0].exists()
        with RunpackReader(output) as reader:
            execution = reader.execution()
        capture_metadata = cast(dict[str, JsonValue], execution.metadata["capture"])
        recovery = cast(dict[str, JsonValue], capture_metadata["recovery"])
        instrumentation = cast(dict[str, JsonValue], capture_metadata["instrumentation"])
        assert recovery == {
            "format_version": 1,
            "status": "complete",
            "checkpoint": "post-exit",
            "controller_restart_recovered": True,
        }
        assert instrumentation["mode"] == "sample"
        assert instrumentation["status"] == "complete"
        assert instrumentation["transport"] == "controller-unix-socket"
        snapshot_metrics = cast(dict[str, JsonValue], instrumentation["snapshot_metrics"])
        assert snapshot_metrics["status"] == "available"
        assert cast(int, snapshot_metrics["message_count"]) >= 2
    finally:
        try:
            waited_pid, _ = os.waitpid(controller_pid, os.WNOHANG)
        except ChildProcessError:
            waited_pid = controller_pid
        if waited_pid == 0:
            os.kill(controller_pid, signal.SIGKILL)
            os.waitpid(controller_pid, 0)


def test_record_process_terminates_descendants_when_capture_is_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "interrupted-tree.runpack"
    ready = tmp_path / "grandchild.pid"
    children: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen
    grandchild_pid: int | None = None

    def tracked_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        child = cast("subprocess.Popen[bytes]", real_popen(*args, **kwargs))
        children.append(child)
        return child

    def interrupt_after_grandchild_starts(
        process: subprocess.Popen[bytes], output_pumps: tuple[object, ...] = ()
    ) -> None:
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not ready.exists():
            raise RuntimeError("grandchild did not start")
        raise KeyboardInterrupt

    def process_exists(process_id: int) -> bool:
        try:
            os.kill(process_id, 0)
        except ProcessLookupError:
            return False
        return True

    grandchild_script = (
        "import os, pathlib, signal, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"pathlib.Path({str(ready)!r}).write_text(str(os.getpid()), encoding='utf-8'); "
        "time.sleep(30)"
    )
    workload = (
        "import subprocess, sys, time; "
        f"subprocess.Popen((sys.executable, '-c', {grandchild_script!r})); "
        "time.sleep(30)"
    )
    monkeypatch.setattr(subprocess, "Popen", tracked_popen)
    monkeypatch.setattr(capture, "_wait_with_usage", interrupt_after_grandchild_starts)
    monkeypatch.setattr(capture, "PROCESS_TERMINATION_TIMEOUT_SECONDS", 0.05)

    try:
        with pytest.raises(KeyboardInterrupt):
            record_process((sys.executable, "-c", workload), output, name="interrupted-tree")

        grandchild_pid = int(ready.read_text(encoding="utf-8"))
        deadline = time.monotonic() + 2
        while process_exists(grandchild_pid) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert children[-1].poll() is not None
        assert not process_exists(grandchild_pid)
        assert not output.exists()
        assert not tuple(tmp_path.glob(".interrupted-tree.runpack.*"))
    finally:
        if grandchild_pid is not None and process_exists(grandchild_pid):
            os.kill(grandchild_pid, signal.SIGKILL)


def test_record_process_terminates_child_when_output_pump_submission_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "pump-failure.runpack"
    children: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen
    real_submit = ThreadPoolExecutor.submit
    real_terminate = capture._terminate_and_reap
    submissions = 0
    cleanup_calls = 0

    def tracked_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        child = cast("subprocess.Popen[bytes]", real_popen(*args, **kwargs))
        children.append(child)
        return child

    def fail_second_submit(executor: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal submissions
        submissions += 1
        if submissions == 2:
            raise RuntimeError("simulated thread submission failure")
        return real_submit(executor, *args, **kwargs)

    def tracked_cleanup(process: subprocess.Popen[bytes]) -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        real_terminate(process)

    monkeypatch.setattr(subprocess, "Popen", tracked_popen)
    monkeypatch.setattr(ThreadPoolExecutor, "submit", fail_second_submit)
    monkeypatch.setattr(capture, "_terminate_and_reap", tracked_cleanup)

    with pytest.raises(RuntimeError, match="simulated thread submission failure"):
        record_process(
            (sys.executable, "-c", "import time; time.sleep(30)"),
            output,
            name="pump-failure",
        )

    assert children[-1].poll() is not None
    assert cleanup_calls == 1
    assert not output.exists()
    assert not tuple(tmp_path.glob(".pump-failure.runpack.tmp-*"))


def test_record_process_terminates_child_when_an_output_pump_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "failed-pump.runpack"
    children: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen

    def tracked_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        child = cast("subprocess.Popen[bytes]", real_popen(*args, **kwargs))
        children.append(child)
        return child

    def fail_pump(*args: object, **kwargs: object) -> None:
        raise OSError("simulated output read failure")

    monkeypatch.setattr(subprocess, "Popen", tracked_popen)
    monkeypatch.setattr(capture, "_pump", fail_pump)
    started = time.monotonic()

    with pytest.raises(OSError, match="simulated output read failure"):
        record_process(
            (
                sys.executable,
                "-c",
                "import sys, time; sys.stdout.write('x' * 10000000); time.sleep(30)",
            ),
            output,
            name="failed-pump",
        )

    assert time.monotonic() - started < 5
    assert children[-1].poll() is not None
    assert not output.exists()
    assert not tuple(tmp_path.glob(".failed-pump.runpack.tmp-*"))


def test_record_process_terminates_child_when_output_pipe_setup_is_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "pipe-failure.runpack"
    children: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen

    def incomplete_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        child = cast("subprocess.Popen[bytes]", real_popen(*args, **kwargs))
        children.append(child)
        assert child.stdout is not None
        child.stdout.close()
        child.stdout = None
        return child

    monkeypatch.setattr(subprocess, "Popen", incomplete_popen)

    with pytest.raises(CaptureError, match="failed to capture process output"):
        record_process(
            (sys.executable, "-c", "import time; time.sleep(30)"),
            output,
            name="pipe-failure",
        )

    assert children[-1].poll() is not None
    assert not output.exists()
    assert not tuple(tmp_path.glob(".pipe-failure.runpack.tmp-*"))


def test_reader_closes_connection_when_validation_is_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "interrupted-reader.runpack"
    output.touch()
    connections: list[sqlite3.Connection] = []
    connect = sqlite3.connect

    def tracked_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection: sqlite3.Connection = connect(*args, **kwargs)
        connections.append(connection)
        return connection

    def interrupt_validation(connection: sqlite3.Connection) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    monkeypatch.setattr(storage, "_validate_connection", interrupt_validation)

    with pytest.raises(KeyboardInterrupt):
        RunpackReader(output)

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connections[0].execute("SELECT 1")


def test_reader_holds_one_snapshot_until_it_closes(tmp_path: Path) -> None:
    output = tmp_path / "consistent-reader.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "before", 0, 1, (), str(tmp_path), 0, None, {}))

    with RunpackReader(output) as reader:
        assert reader.execution().name == "before"
        mutator = sqlite3.connect(output, timeout=0)
        try:
            mutator.execute("UPDATE executions SET name = 'after'")
            with pytest.raises(sqlite3.OperationalError, match="database is locked"):
                mutator.commit()
            mutator.rollback()
        finally:
            mutator.close()

        assert reader.execution().name == "before"


def test_derived_graph_and_execution_metadata_roll_back_together(tmp_path: Path) -> None:
    output = tmp_path / "atomic-derived-graph.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("process", "process", "python", None, {}))
        with pytest.raises(RunpackError, match="execution does not exist"):
            writer.add_event_graph_and_set_execution_metadata(
                "missing",
                (
                    Event(
                        "derived",
                        "python.stack.sample",
                        "work",
                        "process",
                        None,
                        None,
                        None,
                        None,
                        0,
                        {},
                    ),
                ),
                (),
                {"capture": {"instrumentation": {"status": "complete"}}},
            )

    with RunpackReader(output) as reader:
        assert reader.events() == ()
        assert reader.execution().metadata == {}


def test_reader_normalizes_runpack_path_resolution_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "run.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    resolve = Path.resolve

    def fail_runpack_resolution(path: Path, strict: bool = False) -> Path:
        if path == output:
            raise RuntimeError("simulated resolution failure")
        return resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", fail_runpack_resolution)

    with pytest.raises(RunpackError, match="could not resolve runpack path"):
        RunpackReader(output)


def test_existing_writer_closes_connection_when_validation_is_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "interrupted-writer.runpack"
    output.touch()
    connections: list[sqlite3.Connection] = []
    connect = sqlite3.connect

    def tracked_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection: sqlite3.Connection = connect(*args, **kwargs)
        connections.append(connection)
        return connection

    def interrupt_validation(connection: sqlite3.Connection) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    monkeypatch.setattr(storage, "_validate_connection", interrupt_validation)

    with pytest.raises(KeyboardInterrupt):
        RunpackWriter.open_existing(output)

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connections[0].execute("SELECT 1")


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


def test_writer_adds_optional_attachments_to_legacy_runpacks(tmp_path: Path) -> None:
    output = tmp_path / "legacy-attachment.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    with sqlite3.connect(output) as connection:
        connection.execute("DROP TABLE attachments")
        connection.execute("UPDATE manifest SET value = '1.0' WHERE key = 'schema_version'")

    with RunpackWriter.open_existing(output) as writer:
        writer.add_attachments(
            (Attachment("raw", "raw", "evidence", "text/plain", b"evidence", {}),)
        )

    with RunpackReader(output) as reader:
        assert reader.attachments()[0].content == b"evidence"


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


def test_reader_rejects_sqlite_triggers_before_they_can_mutate_enrichment(
    tmp_path: Path,
) -> None:
    output = tmp_path / "trigger.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="trigger")
    with sqlite3.connect(output) as connection:
        connection.execute(
            """
            CREATE TRIGGER delete_events_after_measurement
            AFTER INSERT ON measurements
            BEGIN
                DELETE FROM events;
            END
            """
        )

    with pytest.raises(RunpackError, match="unsupported SQLite triggers"):
        RunpackWriter.open_existing(output)

    with sqlite3.connect(output) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_reader_rejects_wal_mode_runpacks_that_depend_on_sidecar_files(tmp_path: Path) -> None:
    output = tmp_path / "wal.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="wal")
    with sqlite3.connect(output) as connection:
        assert connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"

    with pytest.raises(RunpackError, match="DELETE journal mode for single-file portability"):
        RunpackReader(output)


def test_reader_rejects_runpacks_without_required_identity_constraints(
    tmp_path: Path,
) -> None:
    output = tmp_path / "missing-primary-key.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="missing-primary-key")
    with sqlite3.connect(output) as connection:
        connection.execute("ALTER TABLE measurements RENAME TO original_measurements")
        connection.execute(
            """
            CREATE TABLE measurements (
                id INTEGER,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                timestamp_ns INTEGER,
                entity_id TEXT,
                attributes_json TEXT NOT NULL
            )
            """
        )
        connection.execute("INSERT INTO measurements SELECT * FROM original_measurements")
        connection.execute("DROP TABLE original_measurements")

    with pytest.raises(
        RunpackError,
        match="measurements does not enforce required primary key: id",
    ):
        RunpackReader(output)


def test_reader_requires_the_integer_measurement_primary_key_shape(tmp_path: Path) -> None:
    output = tmp_path / "text-measurement-primary-key.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="measurement-primary-key")
    with RunpackReader(output) as reader:
        expected_measurement_count = len(reader.measurements())
    with sqlite3.connect(output) as connection:
        connection.execute("ALTER TABLE measurements RENAME TO original_measurements")
        connection.execute(
            """
            CREATE TABLE measurements (
                id TEXT PRIMARY KEY COLLATE NOCASE,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                timestamp_ns INTEGER,
                entity_id TEXT REFERENCES entities(id),
                attributes_json TEXT NOT NULL
            )
            """
        )
        connection.execute("INSERT INTO measurements SELECT * FROM original_measurements")
        connection.execute("DROP TABLE original_measurements")

    with pytest.raises(
        RunpackError,
        match="runpack table measurements must use id INTEGER PRIMARY KEY",
    ):
        RunpackReader(output)

    with sqlite3.connect(output) as connection:
        assert connection.execute("SELECT count(*) FROM measurements").fetchone()[0] == (
            expected_measurement_count
        )


def test_reader_rejects_case_insensitive_identity_primary_keys(tmp_path: Path) -> None:
    output = tmp_path / "nocase-identity.runpack"
    schema = storage._SCHEMA.replace(
        "CREATE TABLE entities (\n    id TEXT PRIMARY KEY,",
        "CREATE TABLE entities (\n    id TEXT PRIMARY KEY COLLATE NOCASE,",
    )
    assert schema != storage._SCHEMA
    with sqlite3.connect(output) as connection:
        connection.executescript(schema)
        connection.execute(f"PRAGMA application_id = {storage.APPLICATION_ID}")
        connection.executemany(
            "INSERT INTO manifest(key, value) VALUES (?, ?)",
            (
                ("schema_version", storage.SCHEMA_VERSION),
                ("producer_version", "test"),
            ),
        )
        connection.execute(
            "INSERT INTO executions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("run", "run", 0, 1, "[]", str(tmp_path), 0, None, "{}"),
        )
        connection.execute(
            "INSERT INTO entities VALUES (?, ?, ?, ?, ?)",
            ("app", "service", "app", None, "{}"),
        )
        connection.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("work", "operation", "work", "APP", 0, 1, "test", None, None, "{}"),
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    with pytest.raises(
        RunpackError,
        match="runpack table entities primary key must use BINARY collation",
    ):
        RunpackReader(output)


def test_reader_rejects_runpacks_without_required_relationship_constraints(
    tmp_path: Path,
) -> None:
    output = tmp_path / "missing-foreign-key.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="missing-foreign-key")
    with sqlite3.connect(output) as connection:
        connection.execute("ALTER TABLE measurements RENAME TO original_measurements")
        connection.execute(
            """
            CREATE TABLE measurements (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                timestamp_ns INTEGER,
                entity_id TEXT,
                attributes_json TEXT NOT NULL
            )
            """
        )
        connection.execute("INSERT INTO measurements SELECT * FROM original_measurements")
        connection.execute("DROP TABLE original_measurements")

    with pytest.raises(
        RunpackError,
        match=r"measurements does not enforce required relationship: entity_id -> entities.id",
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


def test_reader_rejects_duplicate_embedded_json_keys(tmp_path: Path) -> None:
    output = tmp_path / "duplicate-json-keys.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="duplicate-json-keys")
    with sqlite3.connect(output) as connection:
        connection.execute(
            "UPDATE executions SET metadata_json = ?",
            ('{"output":{},"output":{"forged":true}}',),
        )

    with pytest.raises(RunpackError, match="invalid JSON object in runpack"):
        inspect_runpack(output)


def test_reader_rejects_non_finite_embedded_json_numbers(tmp_path: Path) -> None:
    output = tmp_path / "non-finite.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="non-finite")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET metadata_json = ?", ('{"value": 1e999}',))

    with pytest.raises(RunpackError, match="non-finite number"):
        inspect_runpack(output)


@pytest.mark.parametrize(
    ("column", "payload"),
    (
        ("metadata_json", r'{"value":"\udcff"}'),
        ("command_json", r'["\udcff"]'),
    ),
)
def test_reader_rejects_non_utf8_embedded_json_strings(
    tmp_path: Path,
    column: str,
    payload: str,
) -> None:
    output = tmp_path / "non-utf8-json.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="non-utf8-json")
    with sqlite3.connect(output) as connection:
        connection.execute(f"UPDATE executions SET {column} = ?", (payload,))

    with pytest.raises(RunpackError, match="runpack JSON strings must be valid UTF-8"):
        inspect_runpack(output)


def test_reader_normalizes_excessively_nested_embedded_json(tmp_path: Path) -> None:
    output = tmp_path / "nested-json.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="nested-json")
    nested = '{"value":' + "[" * 2_000 + "0" + "]" * 2_000 + "}"
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET metadata_json = ?", (nested,))

    with pytest.raises(RunpackError, match="invalid JSON object in runpack"):
        inspect_runpack(output)


@pytest.mark.parametrize(
    ("column", "payload", "message"),
    (
        ("metadata_json", '{"value":' + "1" * 5_000 + "}", "invalid JSON object in runpack"),
        ("command_json", "[" + "1" * 5_000 + "]", "execution command is invalid JSON"),
    ),
)
def test_reader_normalizes_oversized_json_integers(
    tmp_path: Path,
    column: str,
    payload: str,
    message: str,
) -> None:
    output = tmp_path / "oversized-integer.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="oversized-integer")
    with sqlite3.connect(output) as connection:
        connection.execute(f"UPDATE executions SET {column} = ?", (payload,))

    with pytest.raises(RunpackError, match=message):
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

    with pytest.raises(RunpackError, match="event name exceeds"):
        RunpackReader(output)


@pytest.mark.parametrize(
    "accessor",
    (
        "operation_counts",
        "operation_error_counts",
        "operation_duration_totals",
        "operation_max_concurrency",
    ),
)
def test_reader_aggregate_operations_validate_dynamic_sqlite_text_types(
    tmp_path: Path, accessor: str
) -> None:
    output = tmp_path / "invalid-operation-label.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="invalid-operation-label")
    with sqlite3.connect(output) as connection:
        connection.execute(
            "UPDATE events SET name = ?, attributes_json = ?",
            (sqlite3.Binary(b"not-text"), '{"error":true}'),
        )

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="operation name must be a non-empty string"):
            getattr(reader, accessor)()


def test_reader_aggregate_entities_validate_dynamic_sqlite_text_types(tmp_path: Path) -> None:
    output = tmp_path / "invalid-entity-label.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="invalid-entity-label")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE entities SET name = ?", (sqlite3.Binary(b"not-text"),))

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="entity name must be a non-empty string"):
            reader.entity_counts()


@pytest.mark.parametrize("accessor", ("edge_counts", "peer_service_edge_counts"))
def test_reader_aggregate_edges_validate_dynamic_sqlite_text_types(
    tmp_path: Path, accessor: str
) -> None:
    output = tmp_path / "invalid-edge-label.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entities(
            (
                Entity("source", "service", "source", None, {}),
                Entity("target", "service", "target", None, {}),
            )
        )
        writer.add_event_graph(
            (
                Event(
                    "request",
                    "client.request",
                    "request",
                    "source",
                    0,
                    1,
                    "test",
                    None,
                    None,
                    {"peer.service": "target"},
                ),
                Event(
                    "target-event", "operation", "target", "target", 0, 1, "test", None, None, {}
                ),
            ),
            (CausalEdge("request", "target-event", "calls", 1.0, {}),),
        )
    with sqlite3.connect(output) as connection:
        connection.execute(
            "UPDATE entities SET name = ? WHERE id = 'source'",
            (sqlite3.Binary(b"not-text"),),
        )

    with RunpackReader(output) as reader:
        with pytest.raises(
            RunpackError, match="edge source entity name must be a non-empty string"
        ):
            getattr(reader, accessor)()


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

    with pytest.raises(RunpackError, match="attachment content exceeds"):
        RunpackReader(output)


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


def test_writer_stops_consuming_attachments_at_the_aggregate_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "bounded-attachments-write.runpack"
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES", 5)

    def attachments() -> Iterator[Attachment]:
        yield Attachment("first", "raw", "first", "text/plain", b"abc", {})
        yield Attachment("second", "raw", "second", "text/plain", b"def", {})
        raise AssertionError("attachment producer was consumed past the aggregate limit")

    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="aggregate runpack limit"):
            writer.add_attachments(attachments())

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

    with pytest.raises(RunpackError, match="aggregate runpack limit"):
        RunpackReader(output)


def test_reader_counts_malformed_text_attachment_sizes_as_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "text-attachment-size.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="text-attachment")
    with sqlite3.connect(output) as connection:
        connection.execute(
            "INSERT INTO attachments VALUES (?, ?, ?, ?, ?, ?)",
            ("text", "raw", "text", "text/plain", "€€", "{}"),
        )
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_BYTES", 10)
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES", 5)

    with pytest.raises(RunpackError, match="aggregate runpack limit"):
        RunpackReader(output)


def test_writer_counts_existing_text_attachment_sizes_as_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "text-attachment-write.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="text-attachment")
    with sqlite3.connect(output) as connection:
        connection.execute(
            "INSERT INTO attachments VALUES (?, ?, ?, ?, ?, ?)",
            ("text", "raw", "text", "text/plain", "€€", "{}"),
        )
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_BYTES", 10)
    monkeypatch.setattr(storage, "MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES", 5)

    with pytest.raises(RunpackError, match="aggregate runpack limit"):
        RunpackWriter.open_existing(output)

    with sqlite3.connect(output) as connection:
        assert connection.execute("SELECT count(*) FROM attachments").fetchone()[0] == 1


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


def test_bulk_entity_write_normalizes_unhashable_ids(tmp_path: Path) -> None:
    output = tmp_path / "invalid-entity-id.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="entity id must be a non-empty string"):
            writer.add_entities((Entity(cast(str, []), "worker", "worker", None, {}),))

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


@pytest.mark.parametrize(
    "metadata",
    (
        {"bad": "value-\udcff"},
        {"bad-\udcff": "value"},
    ),
)
def test_writer_rejects_non_utf8_json_strings(
    tmp_path: Path,
    metadata: dict[str, JsonValue],
) -> None:
    output = tmp_path / "invalid-json-text.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match="invalid JSON value for runpack"):
            writer.add_execution(
                Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, metadata)
            )

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            reader.execution()


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


@pytest.mark.parametrize(
    ("command", "message"),
    (
        (("",), "execution command executable must be non-empty"),
        (("python", "bad\0argument"), "execution command arguments cannot contain NUL bytes"),
    ),
)
def test_writer_rejects_impossible_execution_commands(
    tmp_path: Path,
    command: tuple[str, ...],
    message: str,
) -> None:
    output = tmp_path / "impossible-command.runpack"
    with RunpackWriter(output) as writer:
        with pytest.raises(RunpackError, match=message):
            writer.add_execution(Execution("run", "run", 0, 1, command, str(tmp_path), 0, None, {}))

    with RunpackReader(output) as reader:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            reader.execution()


@pytest.mark.parametrize(
    ("command", "message"),
    (
        ([""], "execution command executable must be non-empty"),
        (["python", "bad\0argument"], "execution command arguments cannot contain NUL bytes"),
    ),
)
def test_reader_rejects_impossible_execution_commands(
    tmp_path: Path,
    command: list[str],
    message: str,
) -> None:
    output = tmp_path / "impossible-command.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="impossible-command")
    with sqlite3.connect(output) as connection:
        connection.execute("UPDATE executions SET command_json = ?", (json.dumps(command),))

    with pytest.raises(RunpackError, match=message):
        inspect_runpack(output)


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


def test_writer_rejects_multiple_executions_before_mutation(tmp_path: Path) -> None:
    output = tmp_path / "multiple-executions.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("first", "first", 10, 20, (), str(tmp_path), 0, None, {}))
    with sqlite3.connect(output) as connection:
        connection.execute(
            """
            INSERT INTO executions(
                id, name, started_at_ns, finished_at_ns, command_json,
                working_directory, exit_code, revision, metadata_json
            )
            SELECT
                'second', 'second', started_at_ns, finished_at_ns, command_json,
                working_directory, exit_code, revision, metadata_json
            FROM executions
            WHERE id = 'first'
            """
        )

    with pytest.raises(RunpackError, match="executions exceeds the record limit of 1"):
        RunpackWriter.open_existing(output)

    with sqlite3.connect(output) as connection:
        bounds = connection.execute(
            "SELECT started_at_ns, finished_at_ns FROM executions ORDER BY id"
        ).fetchall()
        entity_count = connection.execute("SELECT count(*) FROM entities").fetchone()[0]
    assert bounds == [(10, 20), (10, 20)]
    assert entity_count == 0


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


def test_writer_rejects_finishing_execution_twice_without_changing_first_evidence(
    tmp_path: Path,
) -> None:
    output = tmp_path / "repeated-finish.runpack"
    first_event = Event(
        "first-process",
        "process.run",
        "process",
        None,
        0,
        1,
        "host.wall",
        None,
        0,
        {"exit_code": 0},
    )
    first_measurement = Measurement("process.cpu", 1.0, "seconds", 1, None, {})
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, None, (), str(tmp_path), None, None, {}))
        writer.finish_execution(
            "run",
            finished_at_ns=1,
            exit_code=0,
            metadata={"finish": "first"},
            event=first_event,
            measurements=(first_measurement,),
        )

        with pytest.raises(RunpackError, match="execution is already finished: run"):
            writer.finish_execution(
                "run",
                finished_at_ns=2,
                exit_code=1,
                metadata={"finish": "second"},
                event=Event(
                    "second-process",
                    "process.run",
                    "process",
                    None,
                    0,
                    2,
                    "host.wall",
                    None,
                    1,
                    {"exit_code": 1},
                ),
                measurements=(Measurement("process.cpu", 2.0, "seconds", 2, None, {}),),
            )

    with RunpackReader(output) as reader:
        execution = reader.execution()
        events = reader.events()
        measurements = reader.measurements()
    assert (execution.finished_at_ns, execution.exit_code, execution.metadata) == (
        1,
        0,
        {"finish": "first"},
    )
    assert events == (first_event,)
    assert measurements == (first_measurement,)


def test_writer_rejects_finishing_an_execution_created_already_finished(tmp_path: Path) -> None:
    output = tmp_path / "already-finished.runpack"
    original = Execution(
        "run",
        "run",
        0,
        1,
        (),
        str(tmp_path),
        0,
        None,
        {"finish": "original"},
    )
    with RunpackWriter(output) as writer:
        writer.add_execution(original)

        with pytest.raises(RunpackError, match="execution is already finished: run"):
            writer.finish_execution(
                "run",
                finished_at_ns=2,
                exit_code=1,
                metadata={"finish": "replacement"},
                event=Event(
                    "process",
                    "process.run",
                    "process",
                    None,
                    0,
                    2,
                    "host.wall",
                    None,
                    0,
                    {"exit_code": 1},
                ),
                measurements=(Measurement("process.cpu", 2.0, "seconds", 2, None, {}),),
            )

    with RunpackReader(output) as reader:
        assert reader.execution() == original
        assert reader.events() == ()
        assert reader.measurements() == ()


def test_concurrent_writers_cannot_both_finish_the_same_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "concurrent-finish.runpack"
    with RunpackWriter(output) as writer:
        writer.add_execution(Execution("run", "run", 0, None, (), str(tmp_path), None, None, {}))

    finish_barrier = threading.Barrier(2)
    execution_interval = storage._execution_interval

    def synchronize_after_open_check(
        started_at_ns: object,
        finished_at_ns: object,
    ) -> tuple[int, int | None]:
        interval = execution_interval(started_at_ns, finished_at_ns)
        finish_barrier.wait(timeout=5)
        return interval

    def finish(label: str, finished_at_ns: int, exit_code: int) -> tuple[str, str]:
        try:
            with RunpackWriter.open_existing(output) as writer:
                writer.finish_execution(
                    "run",
                    finished_at_ns=finished_at_ns,
                    exit_code=exit_code,
                    metadata={"winner": label},
                    event=Event(
                        f"process-{label}",
                        "process.run",
                        "process",
                        None,
                        0,
                        finished_at_ns,
                        "host.wall",
                        None,
                        0,
                        {"exit_code": exit_code},
                    ),
                    measurements=(
                        Measurement(
                            "process.cpu",
                            float(finished_at_ns),
                            "seconds",
                            finished_at_ns,
                            None,
                            {},
                        ),
                    ),
                )
        except RunpackError as exc:
            return "error", str(exc)
        return "success", label

    with monkeypatch.context() as context:
        context.setattr(storage, "_execution_interval", synchronize_after_open_check)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = (
                executor.submit(finish, "first", 1, 0),
                executor.submit(finish, "second", 2, 1),
            )
            results = tuple(future.result(timeout=10) for future in futures)

    assert sorted(result[0] for result in results) == ["error", "success"]
    assert next(result[1] for result in results if result[0] == "error") == (
        "execution is already finished: run"
    )
    winner = next(result[1] for result in results if result[0] == "success")
    expected_finish, expected_exit = (1, 0) if winner == "first" else (2, 1)
    with RunpackReader(output) as reader:
        execution = reader.execution()
        events = reader.events()
        measurements = reader.measurements()
    assert (execution.finished_at_ns, execution.exit_code, execution.metadata) == (
        expected_finish,
        expected_exit,
        {"winner": winner},
    )
    assert [event.id for event in events] == [f"process-{winner}"]
    assert [(measurement.name, measurement.value) for measurement in measurements] == [
        ("process.cpu", float(expected_finish))
    ]


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
