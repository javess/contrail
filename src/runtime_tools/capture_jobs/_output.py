"""Private, daemon-free lifecycle registry for local CLI capture workers."""

from __future__ import annotations

import fcntl
import os
import stat
from dataclasses import replace
from pathlib import Path

from runtime_tools.capture_jobs._common import (
    _JOB_STDERR_FILE,
    _JOB_STDERR_TAIL_FILE,
    _JOB_STDOUT_FILE,
    _JOB_STDOUT_TAIL_FILE,
    CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
    CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
    CaptureJobError,
    CaptureJobOutputStream,
)
from runtime_tools.capture_jobs._models import CaptureJob
from runtime_tools.capture_jobs._repository import _job_directory


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
    head_limit, tail_limit = job.output_limits
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


def _read_capture_job_output_segment(
    root: Path,
    job_id: str,
    stream: CaptureJobOutputStream,
    *,
    offset: int,
    limit_bytes: int,
    tail: bool,
) -> bytes:
    descriptor: int | None = None
    label = " output tail" if tail else " output"
    try:
        path = (
            _capture_job_output_tail_path(root, job_id, stream)
            if tail
            else _capture_job_output_path(root, job_id, stream)
        )
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        if tail:
            fcntl.flock(descriptor, fcntl.LOCK_SH)
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
        raise CaptureJobError(f"could not read capture job{label}: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_capture_job_output_file(
    root: Path,
    job_id: str,
    stream: CaptureJobOutputStream,
    *,
    offset: int,
    limit_bytes: int,
) -> bytes:
    return _read_capture_job_output_segment(
        root,
        job_id,
        stream,
        offset=offset,
        limit_bytes=limit_bytes,
        tail=False,
    )


def _read_capture_job_output_tail_file(
    root: Path,
    job_id: str,
    stream: CaptureJobOutputStream,
) -> bytes:
    return _read_capture_job_output_segment(
        root,
        job_id,
        stream,
        offset=0,
        limit_bytes=CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
        tail=True,
    )


def _capture_job_output_offset(value: object, stream: CaptureJobOutputStream) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= CAPTURE_JOB_OUTPUT_LIMIT_BYTES
    ):
        raise CaptureJobError(f"capture job {stream} offset is invalid")
    return value
