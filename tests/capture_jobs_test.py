from __future__ import annotations

import os
import signal
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

import runtime_tools.capture_jobs as capture_jobs
import runtime_tools.capture_worker as capture_worker
from runtime_tools.capture_jobs import (
    CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
    checkpoint_current_capture_job_output_tail,
    create_capture_job,
    finish_current_capture_job,
    load_capture_job,
    mark_current_capture_job_disconnected,
    open_current_capture_job_output_sink,
    read_capture_job_output,
    record_current_capture_job_artifacts,
    start_current_capture_job,
)


def test_capture_job_tracks_worker_lifecycle_and_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "jobs"
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(root))
    job, actual_root = create_capture_job("runtime record")
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)

    lock_descriptor = start_current_capture_job()
    try:
        running = load_capture_job(job.job_id)
        assert running.state == "running"
        assert running.worker_pid == os.getpid()
        record_current_capture_job_artifacts((tmp_path / "result.runpack",))
        mark_current_capture_job_disconnected()
        finish_current_capture_job(7)
    finally:
        os.close(lock_descriptor)

    finished = load_capture_job(job.job_id)
    assert actual_root == root
    assert finished.state == "complete"
    assert finished.exit_status == 7
    assert finished.client_disconnected is True
    assert finished.artifacts == (str(tmp_path / "result.runpack"),)
    assert not hasattr(finished, "command")


def test_capture_job_uses_its_lock_instead_of_a_stale_pid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    monkeypatch.setattr(capture_jobs, "CAPTURE_JOB_STARTUP_GRACE_SECONDS", 0.0)
    job, _ = create_capture_job("proofline search")

    lost = load_capture_job(job.job_id)

    assert lost.state == "lost"
    assert lost.worker_pid is None
    assert lost.exit_status is None


def test_attached_capture_job_does_not_silently_retain_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("runtime record")

    with pytest.raises(
        capture_jobs.CaptureJobError,
        match="capture job does not retain output",
    ):
        read_capture_job_output(job.job_id)


def test_capture_job_output_reads_each_stream_from_an_independent_offset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("runtime record", detached=True)
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)
    stdout_descriptor = open_current_capture_job_output_sink("stdout")
    stderr_descriptor = open_current_capture_job_output_sink("stderr")
    try:
        assert os.write(stdout_descriptor, b"abcdef") == 6
        assert os.write(stderr_descriptor, b"12345") == 5
    finally:
        os.close(stdout_descriptor)
        os.close(stderr_descriptor)

    output = read_capture_job_output(
        job.job_id,
        stdout_offset=2,
        stderr_offset=3,
    )

    assert output.stdout == b"cdef"
    assert output.stderr == b"45"


def test_capture_job_output_replays_stable_head_and_tail_with_explicit_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("runtime record", detached=True)
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)
    lock_descriptor = start_current_capture_job()
    stdout_descriptor = open_current_capture_job_output_sink("stdout")
    try:
        assert os.write(stdout_descriptor, b"HEAD") == 4
        checkpoint_current_capture_job_output_tail("stdout", b"TAIL")
        capture_jobs.mark_current_capture_job_output(
            "stdout",
            4,
            4,
            9,
            omitted_bytes_truncated=False,
            truncated=True,
        )
        running_output = read_capture_job_output(job.job_id)
        finish_current_capture_job(0)
    finally:
        os.close(stdout_descriptor)
        os.close(lock_descriptor)

    output = read_capture_job_output(job.job_id, stdout_offset=1)

    assert running_output.stdout == b"HEAD"
    assert running_output.stdout_tail == b""
    assert running_output.terminal is False
    assert output.stdout == b"EAD"
    assert output.stdout_tail == b"TAIL"
    assert output.stdout_omitted_bytes == 9
    assert output.stdout_omitted_bytes_truncated is False
    assert output.stdout_truncated is True
    assert output.terminal is True


def test_legacy_head_only_capture_job_state_remains_readable() -> None:
    job_id = "1" * 32
    job = capture_jobs._capture_job_from_value(
        {
            "format_version": 1,
            "job_id": job_id,
            "operation": "runtime record",
            "state": "complete",
            "worker_pid": 123,
            "started_at_ns": 1,
            "updated_at_ns": 2,
            "client_disconnected": False,
            "exit_status": 0,
            "artifacts": [],
            "detached": True,
            "output": {
                "retained": True,
                "limit_bytes_per_stream": CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
                "stdout_size_bytes": 128,
                "stdout_truncated": False,
                "stderr_size_bytes": 64,
                "stderr_truncated": True,
            },
        },
        expected_job_id=job_id,
    )

    assert job.output_retention == "head"
    assert job.stdout_head_size_bytes == 128
    assert job.stdout_tail_size_bytes == 0
    assert job.stdout_omitted_bytes == 0
    assert job.stdout_omitted_bytes_truncated is False
    assert job.stderr_head_size_bytes == 64
    assert job.stderr_tail_size_bytes == 0
    assert job.stderr_omitted_bytes == 0
    assert job.stderr_omitted_bytes_truncated is True


def test_legacy_head_only_job_does_not_require_tail_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, root = create_capture_job("runtime record", detached=True)
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)
    descriptor = open_current_capture_job_output_sink("stdout")
    try:
        assert os.write(descriptor, b"legacy") == 6
    finally:
        os.close(descriptor)
    legacy = replace(
        job,
        output_retention="head",
        stdout_size_bytes=6,
        stdout_head_size_bytes=6,
    )
    capture_jobs._write_capture_job(root, legacy)
    for tail_file in (root / job.job_id).glob("*.tail.log"):
        tail_file.unlink()

    loaded = load_capture_job(job.job_id)
    output = read_capture_job_output(job.job_id)

    assert loaded.output_retention == "head"
    assert loaded.stdout_size_bytes == 6
    assert output.stdout == b"legacy"
    assert output.stdout_tail == b""


@pytest.mark.parametrize(
    ("stdout_offset", "stderr_offset", "message"),
    [
        (-1, 0, "stdout offset is invalid"),
        (0, True, "stderr offset is invalid"),
        (CAPTURE_JOB_OUTPUT_LIMIT_BYTES + 1, 0, "stdout offset is invalid"),
        (1, 0, "stdout offset exceeds retained output"),
    ],
)
def test_capture_job_output_rejects_invalid_or_unavailable_offsets(
    stdout_offset: int,
    stderr_offset: int,
    message: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("runtime record", detached=True)

    with pytest.raises(capture_jobs.CaptureJobError, match=message):
        read_capture_job_output(
            job.job_id,
            stdout_offset=stdout_offset,
            stderr_offset=stderr_offset,
        )


@pytest.mark.parametrize(
    ("module", "command"),
    [
        ("runtime_tools.cli", "runtime"),
        ("runtime_tools.contrail_cli", "contrail"),
        ("runtime_tools.proofline.cli", "runtime"),
        ("runtime_tools.rundiff.cli", "runtime"),
    ],
)
def test_detached_capture_acknowledgement_uses_a_supported_job_control_surface(
    module: str,
    command: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("test operation", detached=True)

    capture_worker._print_detached_capture_job(job, module=module, output_format="text")

    assert capsys.readouterr().out == (
        "CAPTURE JOB DETACHED\n\n"
        f"id:     {job.job_id}\n"
        f"status: {command} job status {job.job_id}\n"
        f"wait:   {command} job wait {job.job_id}\n"
        f"output: {command} job output {job.job_id}\n"
        f"follow: {command} job output {job.job_id} --follow\n"
    )


def test_lost_detached_job_marks_both_retained_streams_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    monkeypatch.setattr(capture_jobs, "CAPTURE_JOB_STARTUP_GRACE_SECONDS", 0.0)
    job, _ = create_capture_job("runtime record", detached=True)

    lost = load_capture_job(job.job_id)
    output = read_capture_job_output(job.job_id)

    assert lost.state == "lost"
    assert lost.stdout_truncated is True
    assert lost.stderr_truncated is True
    assert output.stdout == b""
    assert output.stderr == b""
    assert output.stdout_tail == b""
    assert output.stderr_tail == b""
    assert output.stdout_truncated is True
    assert output.stderr_truncated is True
    assert output.stdout_omitted_bytes == 0
    assert output.stdout_omitted_bytes_truncated is True
    assert output.stderr_omitted_bytes == 0
    assert output.stderr_omitted_bytes_truncated is True


def test_detached_output_setup_failure_marks_both_streams_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("runtime record", detached=True)
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)

    def fail_output_setup() -> None:
        raise capture_jobs.CaptureJobError("simulated output setup failure")

    monkeypatch.setattr(capture_worker, "_start_detached_output_spool", fail_output_setup)

    with pytest.raises(capture_jobs.CaptureJobError, match="simulated output setup failure"):
        capture_worker._run_worker_module("runtime_tools.cli", ("record",))

    failed = load_capture_job(job.job_id)
    assert failed.state == "failed"
    assert failed.exit_status == 2
    assert failed.stdout_truncated is True
    assert failed.stdout_omitted_bytes_truncated is True
    assert failed.stderr_truncated is True
    assert failed.stderr_omitted_bytes_truncated is True


def test_unstoppable_output_drain_becomes_lost_instead_of_claiming_terminal_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    monkeypatch.setattr(capture_jobs, "CAPTURE_JOB_STARTUP_GRACE_SECONDS", 0.0)
    job, _ = create_capture_job("runtime record", detached=True)
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)
    monkeypatch.setattr(capture_worker, "_start_detached_output_spool", lambda: None)
    monkeypatch.setattr(
        capture_worker.runpy,
        "run_module",
        lambda _module, *, run_name: {"run_name": run_name},
    )

    def fail_finalization(_spool: object) -> None:
        raise capture_worker._DetachedOutputUnstableError("simulated stuck drain")

    monkeypatch.setattr(capture_worker, "_finish_detached_output_spool", fail_finalization)

    with pytest.raises(capture_jobs.CaptureJobError, match="simulated stuck drain"):
        capture_worker._run_worker_module("runtime_tools.cli", ("record",))

    lost = load_capture_job(job.job_id)
    assert lost.state == "lost"
    assert lost.exit_status is None
    assert lost.stdout_truncated is True
    assert lost.stdout_omitted_bytes_truncated is True
    assert lost.stderr_truncated is True
    assert lost.stderr_omitted_bytes_truncated is True


def test_capture_job_rechecks_completion_after_acquiring_transition_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    monkeypatch.setattr(capture_jobs, "CAPTURE_JOB_STARTUP_GRACE_SECONDS", 0.0)
    job, root = create_capture_job("runtime record")
    acquire = capture_jobs._try_acquire_capture_job_lock

    def complete_before_acquire(actual_root: Path, job_id: str) -> int | None:
        completed = replace(
            job,
            state="complete",
            updated_at_ns=time.time_ns(),
            exit_status=0,
        )
        capture_jobs._write_capture_job(actual_root, completed)
        return acquire(actual_root, job_id)

    monkeypatch.setattr(capture_jobs, "_try_acquire_capture_job_lock", complete_before_acquire)

    completed = load_capture_job(job.job_id)

    assert completed.state == "complete"
    assert capture_jobs._read_capture_job(root, job.job_id).state == "complete"


def test_capture_job_cancel_uses_the_worker_owned_channel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("runtime record")
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)
    lock_descriptor = start_current_capture_job()
    cancel_descriptor = capture_jobs.open_current_capture_job_cancel_channel()
    cancel_received = threading.Event()

    def reject_pid_signal(_process_group: int, _signal_number: int) -> None:
        raise AssertionError("job cancellation must not signal a recorded PID")

    monkeypatch.setattr(os, "killpg", reject_pid_signal)

    def finish_after_cancel() -> None:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                content = os.read(cancel_descriptor, 1)
            except BlockingIOError:
                time.sleep(0.01)
                continue
            if content:
                cancel_received.set()
                finish_current_capture_job(128 + signal.SIGINT)
                return

    worker = threading.Thread(target=finish_after_cancel)
    worker.start()
    try:
        cancelled = capture_jobs.cancel_capture_job(job.job_id)
    finally:
        worker.join()
        os.close(cancel_descriptor)
        os.close(lock_descriptor)

    assert cancel_received.is_set()
    assert cancelled.state == "complete"
    assert cancelled.exit_status == 128 + signal.SIGINT


def test_capture_job_quiesces_late_cancellation_before_terminal_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(tmp_path / "jobs"))
    job, _ = create_capture_job("runtime record")
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ID", job.job_id)
    finalizing = threading.Event()
    cancel_requested = threading.Event()
    cancelled: list[capture_jobs.CaptureJob] = []
    cancel_errors: list[BaseException] = []
    finish = capture_worker.finish_current_capture_job
    request_cancel = capture_jobs._request_capture_job_cancel

    def complete_module(_module: str, *, run_name: str) -> dict[str, object]:
        assert run_name == "__main__"
        return {}

    def observe_cancel_request(root: Path, job_id: str) -> bool:
        requested = request_cancel(root, job_id)
        if requested:
            cancel_requested.set()
        return requested

    def finish_after_late_cancel(exit_status: int, *, failed: bool = False) -> None:
        finalizing.set()
        assert cancel_requested.wait(timeout=2)
        finish(exit_status, failed=failed)

    def cancel_during_finalization() -> None:
        assert finalizing.wait(timeout=2)
        try:
            cancelled.append(capture_jobs.cancel_capture_job(job.job_id))
        except BaseException as exc:
            cancel_errors.append(exc)

    monkeypatch.setattr(capture_worker.runpy, "run_module", complete_module)
    monkeypatch.setattr(capture_worker, "finish_current_capture_job", finish_after_late_cancel)
    monkeypatch.setattr(capture_jobs, "_request_capture_job_cancel", observe_cancel_request)
    client = threading.Thread(target=cancel_during_finalization)
    client.start()

    exit_status = capture_worker._run_worker_module("runtime_tools.cli", ("record",))
    client.join()

    assert exit_status == 0
    assert cancel_errors == []
    assert len(cancelled) == 1
    assert cancelled[0].state == "complete"
    assert cancelled[0].exit_status == 0
    assert load_capture_job(job.job_id).state == "complete"


def test_capture_job_root_must_be_private(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "public-jobs"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    monkeypatch.setenv("_CONTRAIL_CAPTURE_JOB_ROOT", str(root))

    with pytest.raises(
        capture_jobs.CaptureJobError,
        match="capture job root must be a private same-user directory",
    ):
        capture_jobs.list_capture_jobs()
