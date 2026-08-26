"""Private, daemon-free lifecycle registry for local CLI capture workers."""

from __future__ import annotations

import threading
from typing import Literal

CaptureJobState = Literal["starting", "running", "complete", "failed", "lost"]
CaptureJobOutputStream = Literal["stdout", "stderr"]
CaptureJobOutputRetention = Literal["none", "head", "head-tail"]


CAPTURE_JOB_FORMAT_VERSION = 2


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


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise CaptureJobError(f"capture job {label} is invalid")
    return value
