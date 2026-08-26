"""Private, daemon-free lifecycle registry for local CLI capture workers."""

from __future__ import annotations

import math
import os
import shutil
import threading
import time
import uuid
from dataclasses import replace
from pathlib import Path

from runtime_tools.capture_jobs._common import (
    _JOB_CANCEL_FIFO,
    _JOB_LOCK_FILE,
    _JOB_STDERR_FILE,
    _JOB_STDERR_TAIL_FILE,
    _JOB_STDOUT_FILE,
    _JOB_STDOUT_TAIL_FILE,
    CAPTURE_JOB_CANCEL_TIMEOUT_SECONDS,
    CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
    CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
    MAX_CAPTURE_JOB_ARTIFACT_BYTES,
    MAX_CAPTURE_JOB_ARTIFACTS,
    MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES,
    MAX_RETAINED_CAPTURE_JOBS,
    CaptureJobError,
    CaptureJobOutputStream,
    _validate_job_id,
    _validate_operation,
)
from runtime_tools.capture_jobs._models import CaptureJob, CaptureJobOutput
from runtime_tools.capture_jobs._output import (
    _capture_job_output_offset,
    _capture_job_output_path,
    _capture_job_output_tail_path,
    _checkpoint_capture_job_output_file,
    _create_capture_job_output_file,
    _read_capture_job_output_file,
    _read_capture_job_output_tail_file,
    _refresh_capture_job_output_sizes,
    _validate_capture_job_output_descriptor,
)
from runtime_tools.capture_jobs._repository import (
    _capture_job_is_within_startup_grace,
    _capture_job_root,
    _current_capture_job_id,
    _job_directories,
    _job_directory,
    _prune_capture_jobs,
    _read_capture_job,
    _release_capture_job_lock,
    _request_capture_job_cancel,
    _try_acquire_capture_job_lock,
    _update_capture_job,
    _validate_capture_job_cancel_channel,
    _write_capture_job,
)


def create_capture_job(operation: str, *, detached: bool = False) -> tuple[CaptureJob, Path]:
    operation = _validate_operation(operation)
    if not isinstance(detached, bool):
        raise CaptureJobError("capture job detached state must be a boolean")
    root = _capture_job_root()
    _prune_capture_jobs(root)
    now = time.time_ns()
    for _ in range(32):
        job_id = uuid.uuid4().hex
        directory = root / job_id
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            continue
        except OSError as exc:
            raise CaptureJobError(f"could not create capture job: {exc}") from exc
        try:
            lock_descriptor = os.open(
                directory / _JOB_LOCK_FILE,
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            os.close(lock_descriptor)
            os.mkfifo(directory / _JOB_CANCEL_FIFO, mode=0o600)
            if detached:
                _create_capture_job_output_file(directory / _JOB_STDOUT_FILE)
                _create_capture_job_output_file(directory / _JOB_STDERR_FILE)
                _create_capture_job_output_file(directory / _JOB_STDOUT_TAIL_FILE)
                _create_capture_job_output_file(directory / _JOB_STDERR_TAIL_FILE)
            job = CaptureJob(
                job_id,
                operation,
                "starting",
                None,
                now,
                now,
                False,
                None,
                (),
                detached=detached,
                output_retention="head-tail" if detached else "none",
            )
            _write_capture_job(root, job)
        except BaseException:
            shutil.rmtree(directory, ignore_errors=True)
            raise
        return job, root
    raise CaptureJobError("could not allocate a unique capture job identity")


def fail_capture_job_start(job_id: str) -> None:
    root = _capture_job_root()
    lock_descriptor: int | None = None
    try:
        lock_descriptor = _try_acquire_capture_job_lock(root, job_id)
        if lock_descriptor is None:
            return

        def fail(job: CaptureJob) -> CaptureJob:
            if job.terminal:
                return job
            return replace(
                job,
                state="failed",
                updated_at_ns=time.time_ns(),
                exit_status=2,
            )

        _update_capture_job(
            job_id,
            fail,
            root=root,
        )
    except CaptureJobError:
        pass
    finally:
        if lock_descriptor is not None:
            _release_capture_job_lock(lock_descriptor)


def start_current_capture_job() -> int:
    job_id = _current_capture_job_id(required=True)
    root = _capture_job_root()
    descriptor = _try_acquire_capture_job_lock(root, job_id)
    if descriptor is None:
        raise CaptureJobError("capture job liveness lock is already held")
    try:

        def start(job: CaptureJob) -> CaptureJob:
            if job.state != "starting":
                raise CaptureJobError("capture job is no longer starting")
            return replace(
                job,
                state="running",
                worker_pid=os.getpid(),
                updated_at_ns=time.time_ns(),
            )

        _update_capture_job(
            job_id,
            start,
            root=root,
        )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def open_current_capture_job_cancel_channel() -> int:
    job_id = _current_capture_job_id(required=True)
    root = _capture_job_root()
    channel_path = _job_directory(root, job_id) / _JOB_CANCEL_FIFO
    descriptor: int | None = None
    try:
        descriptor = os.open(
            channel_path,
            os.O_RDWR | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0),
        )
        _validate_capture_job_cancel_channel(descriptor)
    except CaptureJobError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise CaptureJobError(f"could not open capture job cancellation channel: {exc}") from exc
    assert descriptor is not None
    return descriptor


def open_current_capture_job_output_sink(stream: CaptureJobOutputStream) -> int:
    job_id = _current_capture_job_id(required=True)
    root = _capture_job_root()
    job = _read_capture_job(root, job_id)
    if not job.detached:
        raise CaptureJobError("capture job does not retain detached output")
    path = _capture_job_output_path(root, job_id, stream)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
        )
        _validate_capture_job_output_descriptor(
            descriptor,
            limit_bytes=job.output_limits[0],
        )
    except CaptureJobError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise CaptureJobError(f"could not open capture job output: {exc}") from exc
    assert descriptor is not None
    return descriptor


def checkpoint_current_capture_job_output_tail(
    stream: CaptureJobOutputStream,
    content: bytes,
) -> None:
    job_id = _current_capture_job_id(required=True)
    if stream not in {"stdout", "stderr"}:
        raise CaptureJobError("capture job output stream is invalid")
    if not isinstance(content, bytes) or len(content) > CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES:
        raise CaptureJobError("capture job output tail is invalid")
    root = _capture_job_root()
    job = _read_capture_job(root, job_id)
    if not job.detached or job.output_retention != "head-tail":
        raise CaptureJobError("capture job does not retain output tails")
    _checkpoint_capture_job_output_file(
        _capture_job_output_tail_path(root, job_id, stream),
        content,
        limit_bytes=CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
    )


def mark_current_capture_job_output(
    stream: CaptureJobOutputStream,
    head_size_bytes: int,
    tail_size_bytes: int,
    omitted_bytes: int,
    *,
    omitted_bytes_truncated: bool,
    truncated: bool,
) -> None:
    job_id = _current_capture_job_id(required=True)
    if stream not in {"stdout", "stderr"}:
        raise CaptureJobError("capture job output stream is invalid")
    if (
        not isinstance(head_size_bytes, int)
        or isinstance(head_size_bytes, bool)
        or not 0 <= head_size_bytes <= CAPTURE_JOB_OUTPUT_LIMIT_BYTES
        or not isinstance(tail_size_bytes, int)
        or isinstance(tail_size_bytes, bool)
        or not 0 <= tail_size_bytes <= CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES
        or head_size_bytes + tail_size_bytes > CAPTURE_JOB_OUTPUT_LIMIT_BYTES
    ):
        raise CaptureJobError("capture job output size is invalid")
    if (
        not isinstance(omitted_bytes, int)
        or isinstance(omitted_bytes, bool)
        or not 0 <= omitted_bytes <= MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES
    ):
        raise CaptureJobError("capture job omitted output size is invalid")
    if not isinstance(omitted_bytes_truncated, bool) or not isinstance(truncated, bool):
        raise CaptureJobError("capture job output truncation state is invalid")
    if (omitted_bytes or omitted_bytes_truncated) and not truncated:
        raise CaptureJobError("capture job omitted output must be truncated")
    size_bytes = head_size_bytes + tail_size_bytes

    def update(job: CaptureJob) -> CaptureJob:
        if not job.detached:
            raise CaptureJobError("capture job does not retain detached output")
        if head_size_bytes > job.output_limits[0]:
            raise CaptureJobError("capture job output head exceeds its byte limit")
        if tail_size_bytes > job.output_limits[1]:
            raise CaptureJobError("capture job output tail exceeds its byte limit")
        previous_size = job.stdout_size_bytes if stream == "stdout" else job.stderr_size_bytes
        previous_truncated = job.stdout_truncated if stream == "stdout" else job.stderr_truncated
        previous_head_size = (
            job.stdout_head_size_bytes if stream == "stdout" else job.stderr_head_size_bytes
        )
        previous_tail_size = (
            job.stdout_tail_size_bytes if stream == "stdout" else job.stderr_tail_size_bytes
        )
        previous_omitted = (
            job.stdout_omitted_bytes if stream == "stdout" else job.stderr_omitted_bytes
        )
        previous_omitted_truncated = (
            job.stdout_omitted_bytes_truncated
            if stream == "stdout"
            else job.stderr_omitted_bytes_truncated
        )
        if size_bytes < previous_size:
            raise CaptureJobError("capture job output size cannot decrease")
        if head_size_bytes < previous_head_size or tail_size_bytes < previous_tail_size:
            raise CaptureJobError("capture job output segment size cannot decrease")
        if omitted_bytes < previous_omitted:
            raise CaptureJobError("capture job omitted output size cannot decrease")
        if stream == "stdout":
            return replace(
                job,
                updated_at_ns=time.time_ns(),
                stdout_size_bytes=size_bytes,
                stdout_head_size_bytes=head_size_bytes,
                stdout_tail_size_bytes=tail_size_bytes,
                stdout_omitted_bytes=omitted_bytes,
                stdout_omitted_bytes_truncated=(
                    previous_omitted_truncated or omitted_bytes_truncated
                ),
                stdout_truncated=previous_truncated or truncated,
            )
        return replace(
            job,
            updated_at_ns=time.time_ns(),
            stderr_size_bytes=size_bytes,
            stderr_head_size_bytes=head_size_bytes,
            stderr_tail_size_bytes=tail_size_bytes,
            stderr_omitted_bytes=omitted_bytes,
            stderr_omitted_bytes_truncated=(previous_omitted_truncated or omitted_bytes_truncated),
            stderr_truncated=previous_truncated or truncated,
        )

    _update_capture_job(job_id, update)


def read_capture_job_output(
    job_id: str,
    *,
    stdout_offset: int = 0,
    stderr_offset: int = 0,
) -> CaptureJobOutput:
    job_id = _validate_job_id(job_id)
    stdout_offset = _capture_job_output_offset(stdout_offset, "stdout")
    stderr_offset = _capture_job_output_offset(stderr_offset, "stderr")
    root = _capture_job_root()
    job = load_capture_job(job_id, root=root)
    if not job.detached:
        raise CaptureJobError(f"capture job does not retain output: {job_id}")
    stdout = _read_capture_job_output_file(
        root,
        job_id,
        "stdout",
        offset=stdout_offset,
        limit_bytes=job.output_limits[0],
    )
    stderr = _read_capture_job_output_file(
        root,
        job_id,
        "stderr",
        offset=stderr_offset,
        limit_bytes=job.output_limits[0],
    )
    stdout_tail = b""
    stderr_tail = b""
    if job.terminal and job.output_retention == "head-tail":
        stdout_tail = _read_capture_job_output_tail_file(root, job_id, "stdout")
        stderr_tail = _read_capture_job_output_tail_file(root, job_id, "stderr")
    return CaptureJobOutput(
        stdout,
        stderr,
        job.stdout_truncated,
        job.stderr_truncated,
        job.state,
        stdout_tail,
        stderr_tail,
        job.stdout_omitted_bytes,
        job.stdout_omitted_bytes_truncated,
        job.stderr_omitted_bytes,
        job.stderr_omitted_bytes_truncated,
    )


def finish_current_capture_job(exit_status: int, *, failed: bool = False) -> None:
    job_id = _current_capture_job_id(required=True)
    if (
        not isinstance(exit_status, int)
        or isinstance(exit_status, bool)
        or not 0 <= exit_status <= 255
    ):
        raise CaptureJobError("capture job exit status is invalid")
    root = _capture_job_root()
    _update_capture_job(
        job_id,
        lambda job: replace(
            job,
            state="failed" if failed else "complete",
            updated_at_ns=time.time_ns(),
            exit_status=exit_status,
        ),
        root=root,
    )
    _prune_capture_jobs(root)


def mark_current_capture_job_disconnected() -> None:
    job_id = _current_capture_job_id(required=False)
    if job_id is None:
        return
    try:
        _update_capture_job(
            job_id,
            lambda job: replace(
                job,
                updated_at_ns=time.time_ns(),
                client_disconnected=True,
            ),
        )
    except CaptureJobError:
        pass


def mark_current_capture_job_output_incomplete() -> None:
    job_id = _current_capture_job_id(required=False)
    if job_id is None:
        return

    def mark(job: CaptureJob) -> CaptureJob:
        if not job.detached:
            return job
        return replace(
            job,
            updated_at_ns=time.time_ns(),
            stdout_truncated=True,
            stdout_omitted_bytes_truncated=True,
            stderr_truncated=True,
            stderr_omitted_bytes_truncated=True,
        )

    try:
        _update_capture_job(job_id, mark)
    except CaptureJobError:
        pass


def record_current_capture_job_artifacts(paths: tuple[Path, ...]) -> None:
    job_id = _current_capture_job_id(required=False)
    if job_id is None:
        return
    try:
        _record_current_capture_job_artifacts(job_id, paths)
    except CaptureJobError:
        pass


def _record_current_capture_job_artifacts(job_id: str, paths: tuple[Path, ...]) -> None:
    if not isinstance(paths, tuple) or not all(isinstance(path, Path) for path in paths):
        raise CaptureJobError("capture job artifacts must be a tuple of paths")
    if len(paths) > MAX_CAPTURE_JOB_ARTIFACTS:
        raise CaptureJobError(
            f"capture job cannot retain more than {MAX_CAPTURE_JOB_ARTIFACTS} artifact paths"
        )
    artifacts: list[str] = []
    for path in paths:
        try:
            value = os.path.abspath(path)
            encoded = os.fsencode(value)
        except (OSError, RuntimeError, UnicodeError) as exc:
            raise CaptureJobError(f"could not identify capture job artifact: {path}") from exc
        if len(encoded) > MAX_CAPTURE_JOB_ARTIFACT_BYTES:
            raise CaptureJobError(
                f"capture job artifact paths cannot exceed {MAX_CAPTURE_JOB_ARTIFACT_BYTES} bytes"
            )
        artifacts.append(value)
    _update_capture_job(
        job_id,
        lambda job: replace(
            job,
            updated_at_ns=time.time_ns(),
            artifacts=tuple(artifacts),
        ),
    )


def list_capture_jobs() -> tuple[CaptureJob, ...]:
    root = _capture_job_root()
    jobs: list[CaptureJob] = []
    for directory in _job_directories(root):
        try:
            jobs.append(load_capture_job(directory.name, root=root))
        except CaptureJobError:
            continue
    jobs.sort(key=lambda job: (job.started_at_ns, job.job_id), reverse=True)
    return tuple(jobs[:MAX_RETAINED_CAPTURE_JOBS])


def load_capture_job(job_id: str, *, root: Path | None = None) -> CaptureJob:
    job_id = _validate_job_id(job_id)
    actual_root = _capture_job_root() if root is None else root
    job = _refresh_capture_job_output_sizes(
        actual_root,
        _read_capture_job(actual_root, job_id),
    )
    if job.terminal or _capture_job_is_within_startup_grace(job):
        return job
    lock_descriptor = _try_acquire_capture_job_lock(actual_root, job_id)
    if lock_descriptor is None:
        return job
    try:
        current = _refresh_capture_job_output_sizes(
            actual_root,
            _read_capture_job(actual_root, job_id),
        )
        if current.terminal or _capture_job_is_within_startup_grace(current):
            return current
        lost = replace(
            current,
            state="lost",
            updated_at_ns=time.time_ns(),
            exit_status=None,
            stdout_truncated=current.stdout_truncated or current.detached,
            stderr_truncated=current.stderr_truncated or current.detached,
            stdout_omitted_bytes_truncated=(
                current.stdout_omitted_bytes_truncated or current.detached
            ),
            stderr_omitted_bytes_truncated=(
                current.stderr_omitted_bytes_truncated or current.detached
            ),
        )
        _write_capture_job(actual_root, lost)
        return lost
    finally:
        _release_capture_job_lock(lock_descriptor)


def wait_capture_job(job_id: str, *, timeout_seconds: float | None = None) -> CaptureJob:
    if timeout_seconds is not None and (
        not isinstance(timeout_seconds, (int, float))
        or isinstance(timeout_seconds, bool)
        or not math.isfinite(timeout_seconds)
        or timeout_seconds < 0
    ):
        raise CaptureJobError("capture job wait timeout must be a finite non-negative number")
    deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
    while True:
        job = load_capture_job(job_id)
        if job.terminal:
            return job
        if deadline is not None and time.monotonic() >= deadline:
            raise CaptureJobError(f"capture job wait timed out: {job_id}")
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        threading.Event().wait(0.1 if remaining is None else min(0.1, remaining))


def cancel_capture_job(job_id: str) -> CaptureJob:
    job = load_capture_job(job_id)
    if job.terminal:
        raise CaptureJobError(f"capture job is not running: {job_id}")
    deadline = time.monotonic() + CAPTURE_JOB_CANCEL_TIMEOUT_SECONDS
    root = _capture_job_root()
    while True:
        if _request_capture_job_cancel(root, job_id):
            return wait_capture_job(
                job_id,
                timeout_seconds=max(0.0, deadline - time.monotonic()),
            )
        job = load_capture_job(job_id)
        if job.terminal:
            return job
        if time.monotonic() >= deadline:
            raise CaptureJobError(f"capture job cancellation timed out: {job_id}")
        threading.Event().wait(0.05)


def render_capture_job(job: CaptureJob) -> str:
    artifacts = "\n".join(f"  - {artifact}" for artifact in job.artifacts) or "  none"
    worker_pid = job.worker_pid if job.worker_pid is not None else "unavailable"
    exit_status = job.exit_status if job.exit_status is not None else "unavailable"
    return "\n".join(
        (
            "CAPTURE JOB",
            "",
            f"id:                  {job.job_id}",
            f"operation:           {job.operation}",
            f"state:               {job.state}",
            f"worker pid:          {worker_pid}",
            f"detached:            {str(job.detached).lower()}",
            f"output retention:    {job.output_retention}",
            f"client disconnected: {str(job.client_disconnected).lower()}",
            f"exit status:         {exit_status}",
            f"stdout retained:     {job.stdout_size_bytes} bytes",
            f"stdout head/tail:    {job.stdout_head_size_bytes}/{job.stdout_tail_size_bytes} bytes",
            f"stdout omitted:      {_render_capture_job_omitted_output(job, 'stdout')}",
            f"stdout truncated:    {str(job.stdout_truncated).lower()}",
            f"stderr retained:     {job.stderr_size_bytes} bytes",
            f"stderr head/tail:    {job.stderr_head_size_bytes}/{job.stderr_tail_size_bytes} bytes",
            f"stderr omitted:      {_render_capture_job_omitted_output(job, 'stderr')}",
            f"stderr truncated:    {str(job.stderr_truncated).lower()}",
            "artifacts:",
            artifacts,
        )
    )


def render_capture_jobs(jobs: tuple[CaptureJob, ...]) -> str:
    if not jobs:
        return "CAPTURE JOBS\n\nNo retained capture jobs."
    lines = ["CAPTURE JOBS", "", "ID                                STATE     MODE      OPERATION"]
    lines.extend(
        f"{job.job_id}  {job.state:<9} {'detached' if job.detached else 'attached':<9} "
        f"{job.operation}"
        for job in jobs
    )
    return "\n".join(lines)


def _render_capture_job_omitted_output(
    job: CaptureJob,
    stream: CaptureJobOutputStream,
) -> str:
    if stream == "stdout":
        omitted = job.stdout_omitted_bytes
        lower_bound = job.stdout_omitted_bytes_truncated
    else:
        omitted = job.stderr_omitted_bytes
        lower_bound = job.stderr_omitted_bytes_truncated
    prefix = "at least " if lower_bound else ""
    return f"{prefix}{omitted} bytes"
