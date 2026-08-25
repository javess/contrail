"""Private, daemon-free lifecycle registry for local CLI capture workers."""

from __future__ import annotations

import errno
import fcntl
import json
import math
import os
import shutil
import stat
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, overload

from runtime_tools.capture import _CAPTURE_JOB_ID_ENV, _CAPTURE_JOB_ROOT_ENV
from runtime_tools.json_support import output_document
from runtime_tools.model import JsonValue

CaptureJobState = Literal["starting", "running", "complete", "failed", "lost"]
CaptureJobOutputStream = Literal["stdout", "stderr"]
CaptureJobOutputRetention = Literal["none", "head", "head-tail"]

CAPTURE_JOB_FORMAT_VERSION = 1
CAPTURE_JOB_RETENTION_SECONDS = 7 * 24 * 60 * 60
CAPTURE_JOB_STARTUP_GRACE_SECONDS = 5.0
CAPTURE_JOB_CANCEL_TIMEOUT_SECONDS = 5.0
MAX_CAPTURE_JOB_STATE_BYTES = 64 * 1024
MAX_CAPTURE_JOB_OPERATION_BYTES = 128
MAX_CAPTURE_JOB_ARTIFACTS = 4
MAX_CAPTURE_JOB_ARTIFACT_BYTES = 4096
CAPTURE_JOB_OUTPUT_LIMIT_BYTES = 1024 * 1024
CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES = CAPTURE_JOB_OUTPUT_LIMIT_BYTES // 2
CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES = (
    CAPTURE_JOB_OUTPUT_LIMIT_BYTES - CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES
)
MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES = (1 << 63) - 1
MAX_RETAINED_CAPTURE_JOBS = 100
MAX_CAPTURE_JOB_DIRECTORY_ENTRIES = 10_000
_JOB_STATE_FILE = "state.json"
_JOB_LOCK_FILE = "live.lock"
_JOB_CANCEL_FIFO = "cancel.fifo"
_JOB_STDOUT_FILE = "stdout.log"
_JOB_STDERR_FILE = "stderr.log"
_JOB_STDOUT_TAIL_FILE = "stdout.tail.log"
_JOB_STDERR_TAIL_FILE = "stderr.tail.log"
_JOB_ID_LENGTH = 32
_TERMINAL_STATES = frozenset({"complete", "failed", "lost"})
_STATE_UPDATE_LOCK = threading.Lock()


class CaptureJobError(ValueError):
    """Raised when local capture-job lifecycle state is unsafe or invalid."""


@dataclass(frozen=True, slots=True)
class CaptureJob:
    job_id: str
    operation: str
    state: CaptureJobState
    worker_pid: int | None
    started_at_ns: int
    updated_at_ns: int
    client_disconnected: bool
    exit_status: int | None
    artifacts: tuple[str, ...]
    detached: bool = False
    stdout_size_bytes: int = 0
    stdout_truncated: bool = False
    stderr_size_bytes: int = 0
    stderr_truncated: bool = False
    output_retention: CaptureJobOutputRetention = "none"
    stdout_head_size_bytes: int = 0
    stdout_tail_size_bytes: int = 0
    stdout_omitted_bytes: int = 0
    stdout_omitted_bytes_truncated: bool = False
    stderr_head_size_bytes: int = 0
    stderr_tail_size_bytes: int = 0
    stderr_omitted_bytes: int = 0
    stderr_omitted_bytes_truncated: bool = False

    @property
    def terminal(self) -> bool:
        return self.state in _TERMINAL_STATES

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "job_id": self.job_id,
            "operation": self.operation,
            "state": self.state,
            "worker_pid": self.worker_pid,
            "started_at_ns": self.started_at_ns,
            "updated_at_ns": self.updated_at_ns,
            "client_disconnected": self.client_disconnected,
            "exit_status": self.exit_status,
            "artifacts": list(self.artifacts),
            "detached": self.detached,
            "output": {
                "retained": self.detached,
                "limit_bytes_per_stream": CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
                "retention_strategy": self.output_retention,
                "head_limit_bytes_per_stream": _capture_job_output_head_limit(self),
                "tail_limit_bytes_per_stream": _capture_job_output_tail_limit(self),
                "stdout_size_bytes": self.stdout_size_bytes,
                "stdout_head_size_bytes": self.stdout_head_size_bytes,
                "stdout_tail_size_bytes": self.stdout_tail_size_bytes,
                "stdout_omitted_bytes": self.stdout_omitted_bytes,
                "stdout_omitted_bytes_truncated": self.stdout_omitted_bytes_truncated,
                "stdout_truncated": self.stdout_truncated,
                "stderr_size_bytes": self.stderr_size_bytes,
                "stderr_head_size_bytes": self.stderr_head_size_bytes,
                "stderr_tail_size_bytes": self.stderr_tail_size_bytes,
                "stderr_omitted_bytes": self.stderr_omitted_bytes,
                "stderr_omitted_bytes_truncated": self.stderr_omitted_bytes_truncated,
                "stderr_truncated": self.stderr_truncated,
            },
        }


@dataclass(frozen=True, slots=True)
class CaptureJobOutput:
    stdout: bytes
    stderr: bytes
    stdout_truncated: bool
    stderr_truncated: bool
    job_state: CaptureJobState
    stdout_tail: bytes
    stderr_tail: bytes
    stdout_omitted_bytes: int
    stdout_omitted_bytes_truncated: bool
    stderr_omitted_bytes: int
    stderr_omitted_bytes_truncated: bool

    @property
    def terminal(self) -> bool:
        return self.job_state in _TERMINAL_STATES


def capture_job_document(job: CaptureJob) -> dict[str, JsonValue]:
    return output_document("runtime.capture_job", {"job": job.as_json_value()})


def capture_jobs_document(jobs: tuple[CaptureJob, ...]) -> dict[str, JsonValue]:
    return output_document(
        "runtime.capture_jobs",
        {"jobs": [job.as_json_value() for job in jobs]},
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
            limit_bytes=_capture_job_output_head_limit(job),
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
        if head_size_bytes > _capture_job_output_head_limit(job):
            raise CaptureJobError("capture job output head exceeds its byte limit")
        if tail_size_bytes > _capture_job_output_tail_limit(job):
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
        limit_bytes=_capture_job_output_head_limit(job),
    )
    stderr = _read_capture_job_output_file(
        root,
        job_id,
        "stderr",
        offset=stderr_offset,
        limit_bytes=_capture_job_output_head_limit(job),
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


def _capture_job_root() -> Path:
    configured = os.environ.get(_CAPTURE_JOB_ROOT_ENV)
    if configured is not None:
        if not configured or "\0" in configured:
            raise CaptureJobError("capture job root is invalid")
        root = Path(configured)
    else:
        runtime_root = os.environ.get("XDG_RUNTIME_DIR")
        base = Path(runtime_root) if runtime_root else Path(tempfile.gettempdir())
        root = base / f"contrail-capture-jobs-{os.getuid()}"
    try:
        if not root.is_absolute():
            root = Path(os.path.abspath(root))
        root.mkdir(mode=0o700, parents=False, exist_ok=True)
        metadata = root.lstat()
    except (OSError, RuntimeError) as exc:
        raise CaptureJobError(f"could not prepare capture job root: {exc}") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise CaptureJobError("capture job root must be a private same-user directory")
    return root


def _job_directories(root: Path) -> tuple[Path, ...]:
    try:
        entries = []
        for index, entry in enumerate(os.scandir(root), start=1):
            if index > MAX_CAPTURE_JOB_DIRECTORY_ENTRIES:
                raise CaptureJobError("capture job directory contains too many entries")
            if len(entry.name) == _JOB_ID_LENGTH and entry.is_dir(follow_symlinks=False):
                entries.append(Path(entry.path))
        return tuple(entries)
    except OSError as exc:
        raise CaptureJobError(f"could not list capture jobs: {exc}") from exc


def _job_directory(root: Path, job_id: str) -> Path:
    return root / _validate_job_id(job_id)


def _validate_job_id(job_id: str) -> str:
    if (
        not isinstance(job_id, str)
        or len(job_id) != _JOB_ID_LENGTH
        or any(character not in "0123456789abcdef" for character in job_id)
    ):
        raise CaptureJobError("capture job ID must be 32 lowercase hexadecimal characters")
    return job_id


def _validate_operation(operation: object) -> str:
    if not isinstance(operation, str) or not operation or "\0" in operation:
        raise CaptureJobError("capture job operation is invalid")
    try:
        encoded = operation.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CaptureJobError("capture job operation must be valid UTF-8") from exc
    if len(encoded) > MAX_CAPTURE_JOB_OPERATION_BYTES:
        raise CaptureJobError(
            f"capture job operation cannot exceed {MAX_CAPTURE_JOB_OPERATION_BYTES} bytes"
        )
    return operation


@overload
def _current_capture_job_id(*, required: Literal[True]) -> str: ...


@overload
def _current_capture_job_id(*, required: Literal[False]) -> str | None: ...


def _current_capture_job_id(*, required: bool) -> str | None:
    raw = os.environ.get(_CAPTURE_JOB_ID_ENV)
    if raw is None and not required:
        return None
    if raw is None:
        raise CaptureJobError("capture worker has no job identity")
    return _validate_job_id(raw)


def _read_capture_job(root: Path, job_id: str) -> CaptureJob:
    state_path = _job_directory(root, job_id) / _JOB_STATE_FILE
    try:
        metadata = state_path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise CaptureJobError("capture job state must be a private regular file")
        if metadata.st_size > MAX_CAPTURE_JOB_STATE_BYTES:
            raise CaptureJobError("capture job state exceeds its byte limit")
        raw = state_path.read_bytes()
    except FileNotFoundError as exc:
        raise CaptureJobError(f"capture job does not exist: {job_id}") from exc
    except OSError as exc:
        raise CaptureJobError(f"could not read capture job: {exc}") from exc
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_nonfinite,
        )
    except (UnicodeError, json.JSONDecodeError, CaptureJobError) as exc:
        raise CaptureJobError("capture job state is invalid JSON") from exc
    return _capture_job_from_value(value, expected_job_id=job_id)


def _capture_job_from_value(value: object, *, expected_job_id: str) -> CaptureJob:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise CaptureJobError("capture job state must be an object")
    if value.get("format_version") != CAPTURE_JOB_FORMAT_VERSION:
        raise CaptureJobError("capture job state version is unsupported")
    job_id = value.get("job_id")
    if job_id != expected_job_id:
        raise CaptureJobError("capture job state identity does not match its directory")
    operation = _validate_operation(value.get("operation"))
    state_value = value.get("state")
    if state_value not in {"starting", "running", "complete", "failed", "lost"}:
        raise CaptureJobError("capture job state is invalid")
    state: CaptureJobState = state_value
    worker_pid = _optional_integer(value.get("worker_pid"), "worker PID", minimum=1)
    started_at_ns = _integer(value.get("started_at_ns"), "start timestamp", minimum=0)
    updated_at_ns = _integer(value.get("updated_at_ns"), "update timestamp", minimum=0)
    if updated_at_ns < started_at_ns:
        raise CaptureJobError("capture job update precedes its start")
    client_disconnected = value.get("client_disconnected")
    if not isinstance(client_disconnected, bool):
        raise CaptureJobError("capture job client-disconnection state is invalid")
    exit_status = _optional_integer(value.get("exit_status"), "exit status", minimum=0, maximum=255)
    artifacts_value = value.get("artifacts")
    if not isinstance(artifacts_value, list) or len(artifacts_value) > MAX_CAPTURE_JOB_ARTIFACTS:
        raise CaptureJobError("capture job artifacts are invalid")
    artifacts: list[str] = []
    for artifact in artifacts_value:
        if not isinstance(artifact, str) or "\0" in artifact:
            raise CaptureJobError("capture job artifact path is invalid")
        try:
            encoded = os.fsencode(artifact)
        except UnicodeError as exc:
            raise CaptureJobError("capture job artifact path is invalid") from exc
        if len(encoded) > MAX_CAPTURE_JOB_ARTIFACT_BYTES:
            raise CaptureJobError("capture job artifact path exceeds its byte limit")
        artifacts.append(artifact)
    detached = value.get("detached", False)
    if not isinstance(detached, bool):
        raise CaptureJobError("capture job detached state is invalid")
    output_value = value.get("output")
    output_retention: CaptureJobOutputRetention = "head" if detached else "none"
    if output_value is None:
        stdout_size_bytes = 0
        stdout_truncated = False
        stderr_size_bytes = 0
        stderr_truncated = False
        stdout_head_size_bytes = 0
        stdout_tail_size_bytes = 0
        stdout_omitted_bytes = 0
        stdout_omitted_bytes_truncated = False
        stderr_head_size_bytes = 0
        stderr_tail_size_bytes = 0
        stderr_omitted_bytes = 0
        stderr_omitted_bytes_truncated = False
    else:
        if not isinstance(output_value, dict) or not all(
            isinstance(key, str) for key in output_value
        ):
            raise CaptureJobError("capture job output state is invalid")
        if output_value.get("retained") is not detached:
            raise CaptureJobError("capture job output retention state is invalid")
        if output_value.get("limit_bytes_per_stream") != CAPTURE_JOB_OUTPUT_LIMIT_BYTES:
            raise CaptureJobError("capture job output limit is unsupported")
        stdout_size_bytes = _integer(
            output_value.get("stdout_size_bytes"),
            "stdout size",
            minimum=0,
            maximum=CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
        )
        stderr_size_bytes = _integer(
            output_value.get("stderr_size_bytes"),
            "stderr size",
            minimum=0,
            maximum=CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
        )
        stdout_truncated = output_value.get("stdout_truncated")
        stderr_truncated = output_value.get("stderr_truncated")
        if not isinstance(stdout_truncated, bool) or not isinstance(stderr_truncated, bool):
            raise CaptureJobError("capture job output truncation state is invalid")
        retention_value = output_value.get("retention_strategy")
        if retention_value is None:
            stdout_head_size_bytes = stdout_size_bytes
            stdout_tail_size_bytes = 0
            stdout_omitted_bytes = 0
            stdout_omitted_bytes_truncated = stdout_truncated
            stderr_head_size_bytes = stderr_size_bytes
            stderr_tail_size_bytes = 0
            stderr_omitted_bytes = 0
            stderr_omitted_bytes_truncated = stderr_truncated
        else:
            if retention_value not in {"none", "head", "head-tail"}:
                raise CaptureJobError("capture job output retention strategy is invalid")
            output_retention = retention_value
            expected_head_limit, expected_tail_limit = _capture_job_output_limits(output_retention)
            if (
                output_value.get("head_limit_bytes_per_stream") != expected_head_limit
                or output_value.get("tail_limit_bytes_per_stream") != expected_tail_limit
            ):
                raise CaptureJobError("capture job output segment limits are unsupported")
            stdout_head_size_bytes = _integer(
                output_value.get("stdout_head_size_bytes"),
                "stdout head size",
                minimum=0,
                maximum=expected_head_limit,
            )
            stdout_tail_size_bytes = _integer(
                output_value.get("stdout_tail_size_bytes"),
                "stdout tail size",
                minimum=0,
                maximum=expected_tail_limit,
            )
            stderr_head_size_bytes = _integer(
                output_value.get("stderr_head_size_bytes"),
                "stderr head size",
                minimum=0,
                maximum=expected_head_limit,
            )
            stderr_tail_size_bytes = _integer(
                output_value.get("stderr_tail_size_bytes"),
                "stderr tail size",
                minimum=0,
                maximum=expected_tail_limit,
            )
            stdout_omitted_bytes = _integer(
                output_value.get("stdout_omitted_bytes"),
                "stdout omitted size",
                minimum=0,
                maximum=MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES,
            )
            stderr_omitted_bytes = _integer(
                output_value.get("stderr_omitted_bytes"),
                "stderr omitted size",
                minimum=0,
                maximum=MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES,
            )
            stdout_omitted_bytes_truncated = output_value.get("stdout_omitted_bytes_truncated")
            stderr_omitted_bytes_truncated = output_value.get("stderr_omitted_bytes_truncated")
            if not isinstance(stdout_omitted_bytes_truncated, bool) or not isinstance(
                stderr_omitted_bytes_truncated, bool
            ):
                raise CaptureJobError("capture job omitted output state is invalid")
        if stdout_size_bytes != stdout_head_size_bytes + stdout_tail_size_bytes:
            raise CaptureJobError("capture job stdout segment sizes are inconsistent")
        if stderr_size_bytes != stderr_head_size_bytes + stderr_tail_size_bytes:
            raise CaptureJobError("capture job stderr segment sizes are inconsistent")
        if (stdout_omitted_bytes or stdout_omitted_bytes_truncated) and not stdout_truncated:
            raise CaptureJobError("capture job omitted stdout is not marked truncated")
        if (stderr_omitted_bytes or stderr_omitted_bytes_truncated) and not stderr_truncated:
            raise CaptureJobError("capture job omitted stderr is not marked truncated")
    if detached and output_retention == "none":
        raise CaptureJobError("detached capture job must retain output")
    if not detached and output_retention != "none":
        raise CaptureJobError("attached capture job cannot retain output")
    if not detached and (
        stdout_size_bytes != 0
        or stdout_truncated
        or stderr_size_bytes != 0
        or stderr_truncated
        or stdout_omitted_bytes
        or stdout_omitted_bytes_truncated
        or stderr_omitted_bytes
        or stderr_omitted_bytes_truncated
    ):
        raise CaptureJobError("attached capture job cannot retain output")
    if state == "running" and worker_pid is None:
        raise CaptureJobError("running capture job must have a worker PID")
    if state in {"starting", "running"} and exit_status is not None:
        raise CaptureJobError("active capture job cannot have an exit status")
    if state in {"complete", "failed"} and exit_status is None:
        raise CaptureJobError("finished capture job must have an exit status")
    return CaptureJob(
        expected_job_id,
        operation,
        state,
        worker_pid,
        started_at_ns,
        updated_at_ns,
        client_disconnected,
        exit_status,
        tuple(artifacts),
        detached=detached,
        stdout_size_bytes=stdout_size_bytes,
        stdout_truncated=stdout_truncated,
        stderr_size_bytes=stderr_size_bytes,
        stderr_truncated=stderr_truncated,
        output_retention=output_retention,
        stdout_head_size_bytes=stdout_head_size_bytes,
        stdout_tail_size_bytes=stdout_tail_size_bytes,
        stdout_omitted_bytes=stdout_omitted_bytes,
        stdout_omitted_bytes_truncated=stdout_omitted_bytes_truncated,
        stderr_head_size_bytes=stderr_head_size_bytes,
        stderr_tail_size_bytes=stderr_tail_size_bytes,
        stderr_omitted_bytes=stderr_omitted_bytes,
        stderr_omitted_bytes_truncated=stderr_omitted_bytes_truncated,
    )


def _write_capture_job(root: Path, job: CaptureJob) -> None:
    directory = _job_directory(root, job.job_id)
    state_path = directory / _JOB_STATE_FILE
    temporary = directory / f".{_JOB_STATE_FILE}.{uuid.uuid4().hex}.tmp"
    document = {"format_version": CAPTURE_JOB_FORMAT_VERSION, **job.as_json_value()}
    payload = json.dumps(document, allow_nan=False, sort_keys=True).encode("utf-8")
    if len(payload) > MAX_CAPTURE_JOB_STATE_BYTES:
        raise CaptureJobError("capture job state exceeds its byte limit")
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, state_path)
    except OSError as exc:
        raise CaptureJobError(f"could not update capture job: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


def _update_capture_job(
    job_id: str,
    update: Callable[[CaptureJob], CaptureJob],
    *,
    root: Path | None = None,
) -> CaptureJob:
    actual_root = _capture_job_root() if root is None else root
    with _STATE_UPDATE_LOCK:
        job = _read_capture_job(actual_root, job_id)
        updated = update(job)
        _write_capture_job(actual_root, updated)
    return updated


def _capture_job_is_within_startup_grace(job: CaptureJob) -> bool:
    return job.state == "starting" and time.time_ns() - job.started_at_ns < int(
        CAPTURE_JOB_STARTUP_GRACE_SECONDS * 1_000_000_000
    )


def _try_acquire_capture_job_lock(root: Path, job_id: str) -> int | None:
    lock_path = _job_directory(root, job_id) / _JOB_LOCK_FILE
    descriptor: int | None = None
    try:
        descriptor = os.open(lock_path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        lock_metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(lock_metadata.st_mode)
            or lock_metadata.st_uid != os.getuid()
            or stat.S_IMODE(lock_metadata.st_mode) & 0o077
        ):
            raise CaptureJobError("capture job liveness lock is invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            return None
        return descriptor
    except CaptureJobError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise CaptureJobError(f"could not inspect capture job liveness: {exc}") from exc


def _release_capture_job_lock(descriptor: int) -> None:
    try:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _validate_capture_job_cancel_channel(descriptor: int) -> None:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISFIFO(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise CaptureJobError("capture job cancellation channel is invalid")


def _request_capture_job_cancel(root: Path, job_id: str) -> bool:
    channel_path = _job_directory(root, job_id) / _JOB_CANCEL_FIFO
    descriptor: int | None = None
    try:
        descriptor = os.open(
            channel_path,
            os.O_WRONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0),
        )
        _validate_capture_job_cancel_channel(descriptor)
        os.write(descriptor, b"\x03")
        return True
    except OSError as exc:
        if exc.errno in {errno.ENOENT, errno.ENXIO, errno.EPIPE}:
            return False
        raise CaptureJobError(f"could not request capture job cancellation: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _capture_job_output_path(
    root: Path,
    job_id: str,
    stream: CaptureJobOutputStream,
) -> Path:
    if stream == "stdout":
        name = _JOB_STDOUT_FILE
    elif stream == "stderr":
        name = _JOB_STDERR_FILE
    else:
        raise CaptureJobError("capture job output stream is invalid")
    return _job_directory(root, job_id) / name


def _capture_job_output_tail_path(
    root: Path,
    job_id: str,
    stream: CaptureJobOutputStream,
) -> Path:
    if stream == "stdout":
        name = _JOB_STDOUT_TAIL_FILE
    elif stream == "stderr":
        name = _JOB_STDERR_TAIL_FILE
    else:
        raise CaptureJobError("capture job output stream is invalid")
    return _job_directory(root, job_id) / name


def _capture_job_output_limits(
    retention: CaptureJobOutputRetention,
) -> tuple[int, int]:
    if retention == "none":
        return 0, 0
    if retention == "head":
        return CAPTURE_JOB_OUTPUT_LIMIT_BYTES, 0
    if retention == "head-tail":
        return CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES, CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES
    raise CaptureJobError("capture job output retention strategy is invalid")


def _capture_job_output_head_limit(job: CaptureJob) -> int:
    return _capture_job_output_limits(job.output_retention)[0]


def _capture_job_output_tail_limit(job: CaptureJob) -> int:
    return _capture_job_output_limits(job.output_retention)[1]


def _create_capture_job_output_file(path: Path) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)


def _checkpoint_capture_job_output_file(
    path: Path,
    content: bytes,
    *,
    limit_bytes: int,
) -> None:
    if len(content) > limit_bytes:
        raise CaptureJobError("capture job output exceeds its byte limit")
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0))
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        _validate_capture_job_output_descriptor(descriptor, limit_bytes=limit_bytes)
        os.ftruncate(descriptor, 0)
        written = 0
        while written < len(content):
            try:
                count = os.write(descriptor, content[written:])
            except InterruptedError:
                continue
            if count <= 0:
                raise OSError("capture job output write made no progress")
            written += count
        os.fsync(descriptor)
    except OSError as exc:
        raise CaptureJobError(f"could not checkpoint capture job output: {exc}") from exc
    finally:
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


def _validate_capture_job_output_descriptor(
    descriptor: int,
    *,
    limit_bytes: int,
) -> os.stat_result:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or metadata.st_size > limit_bytes
    ):
        raise CaptureJobError("capture job output file is invalid")
    return metadata


def _capture_job_output_file_size(
    path: Path,
    *,
    limit_bytes: int,
    lock: bool = False,
) -> int:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        if lock:
            fcntl.flock(descriptor, fcntl.LOCK_SH)
        return _validate_capture_job_output_descriptor(
            descriptor,
            limit_bytes=limit_bytes,
        ).st_size
    except CaptureJobError:
        raise
    except OSError as exc:
        raise CaptureJobError(f"could not inspect capture job output: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _refresh_capture_job_output_sizes(root: Path, job: CaptureJob) -> CaptureJob:
    if not job.detached:
        return job
    head_limit = _capture_job_output_head_limit(job)
    tail_limit = _capture_job_output_tail_limit(job)
    stdout_head_size = _capture_job_output_file_size(
        _capture_job_output_path(root, job.job_id, "stdout"),
        limit_bytes=head_limit,
    )
    stderr_head_size = _capture_job_output_file_size(
        _capture_job_output_path(root, job.job_id, "stderr"),
        limit_bytes=head_limit,
    )
    stdout_tail_size = 0
    stderr_tail_size = 0
    if job.output_retention == "head-tail":
        stdout_tail_size = _capture_job_output_file_size(
            _capture_job_output_tail_path(root, job.job_id, "stdout"),
            limit_bytes=tail_limit,
            lock=True,
        )
        stderr_tail_size = _capture_job_output_file_size(
            _capture_job_output_tail_path(root, job.job_id, "stderr"),
            limit_bytes=tail_limit,
            lock=True,
        )
    return replace(
        job,
        stdout_size_bytes=stdout_head_size + stdout_tail_size,
        stdout_head_size_bytes=stdout_head_size,
        stdout_tail_size_bytes=stdout_tail_size,
        stderr_size_bytes=stderr_head_size + stderr_tail_size,
        stderr_head_size_bytes=stderr_head_size,
        stderr_tail_size_bytes=stderr_tail_size,
    )


def _read_capture_job_output_file(
    root: Path,
    job_id: str,
    stream: CaptureJobOutputStream,
    *,
    offset: int,
    limit_bytes: int,
) -> bytes:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            _capture_job_output_path(root, job_id, stream),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = _validate_capture_job_output_descriptor(
            descriptor,
            limit_bytes=limit_bytes,
        )
        if offset > metadata.st_size:
            raise CaptureJobError(f"capture job {stream} offset exceeds retained output")
        os.lseek(descriptor, offset, os.SEEK_SET)
        remaining = metadata.st_size - offset
        chunks: list[bytes] = []
        while remaining:
            try:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
            except InterruptedError:
                continue
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)
    except CaptureJobError:
        raise
    except OSError as exc:
        raise CaptureJobError(f"could not read capture job output: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_capture_job_output_tail_file(
    root: Path,
    job_id: str,
    stream: CaptureJobOutputStream,
) -> bytes:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            _capture_job_output_tail_path(root, job_id, stream),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        metadata = _validate_capture_job_output_descriptor(
            descriptor,
            limit_bytes=CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
        )
        remaining = metadata.st_size
        chunks: list[bytes] = []
        while remaining:
            try:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
            except InterruptedError:
                continue
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)
    except CaptureJobError:
        raise
    except OSError as exc:
        raise CaptureJobError(f"could not read capture job output tail: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _capture_job_output_offset(value: object, stream: CaptureJobOutputStream) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= CAPTURE_JOB_OUTPUT_LIMIT_BYTES
    ):
        raise CaptureJobError(f"capture job {stream} offset is invalid")
    return value


def _prune_capture_jobs(root: Path) -> None:
    terminal: list[tuple[int, Path]] = []
    cutoff = time.time_ns() - CAPTURE_JOB_RETENTION_SECONDS * 1_000_000_000
    for directory in _job_directories(root):
        try:
            job = load_capture_job(directory.name, root=root)
        except CaptureJobError:
            continue
        if job.terminal:
            terminal.append((job.updated_at_ns, directory))
    terminal.sort(reverse=True)
    for index, (updated_at_ns, directory) in enumerate(terminal):
        if index >= MAX_RETAINED_CAPTURE_JOBS or updated_at_ns < cutoff:
            shutil.rmtree(directory, ignore_errors=True)


def _integer(
    value: object,
    label: str,
    *,
    minimum: int,
    maximum: int = (1 << 63) - 1,
) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise CaptureJobError(f"capture job {label} is invalid")
    return value


def _optional_integer(
    value: object,
    label: str,
    *,
    minimum: int,
    maximum: int = (1 << 63) - 1,
) -> int | None:
    if value is None:
        return None
    return _integer(value, label, minimum=minimum, maximum=maximum)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CaptureJobError(f"duplicate capture job field: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> None:
    raise CaptureJobError(f"non-finite capture job value: {value}")
