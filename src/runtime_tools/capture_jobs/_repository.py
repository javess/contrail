"""Private, daemon-free lifecycle registry for local CLI capture workers."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import shutil
import stat
import tempfile
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Literal, overload

from runtime_tools.capture._common import _CAPTURE_JOB_ID_ENV, _CAPTURE_JOB_ROOT_ENV
from runtime_tools.capture_jobs._common import (
    _JOB_CANCEL_FIFO,
    _JOB_ID_LENGTH,
    _JOB_LOCK_FILE,
    _JOB_STATE_FILE,
    _STATE_UPDATE_LOCK,
    CAPTURE_JOB_FORMAT_VERSION,
    CAPTURE_JOB_RETENTION_SECONDS,
    CAPTURE_JOB_STARTUP_GRACE_SECONDS,
    MAX_CAPTURE_JOB_DIRECTORY_ENTRIES,
    MAX_CAPTURE_JOB_STATE_BYTES,
    MAX_RETAINED_CAPTURE_JOBS,
    CaptureJobError,
    _validate_job_id,
)
from runtime_tools.capture_jobs._models import CaptureJob, _capture_job_from_value
from runtime_tools.json_support import (
    reject_duplicate_object,
    reject_nonfinite_constant,
)


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
            object_pairs_hook=reject_duplicate_object,
            parse_constant=reject_nonfinite_constant,
        )
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise CaptureJobError("capture job state is invalid JSON") from exc
    return _capture_job_from_value(value, expected_job_id=job_id)


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


def _prune_capture_jobs(root: Path) -> None:
    terminal: list[tuple[int, Path]] = []
    cutoff = time.time_ns() - CAPTURE_JOB_RETENTION_SECONDS * 1_000_000_000
    for directory in _job_directories(root):
        try:
            job = _read_capture_job(root, directory.name)
        except CaptureJobError:
            continue
        if job.terminal:
            terminal.append((job.updated_at_ns, directory))
    terminal.sort(reverse=True)
    for index, (updated_at_ns, directory) in enumerate(terminal):
        if index >= MAX_RETAINED_CAPTURE_JOBS or updated_at_ns < cutoff:
            shutil.rmtree(directory, ignore_errors=True)
