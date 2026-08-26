"""Capture a local process into the normalized execution model."""

from __future__ import annotations

import threading
from dataclasses import dataclass


class CaptureError(ValueError):
    """Raised when a process cannot be captured."""


MAX_CAPTURE_OUTPUT_BYTES = 64 * 1024 * 1024
MAX_POST_EXIT_DRAIN_BYTES = 1024 * 1024
MAX_CUSTOM_ENVIRONMENT_IDENTITIES = 256
MAX_ENVIRONMENT_NAME_BYTES = 1024
PROCESS_TERMINATION_TIMEOUT_SECONDS = 1.0
CAPTURE_LEVELS = ("passive", "process", "sample", "deep")
_STATUS_POLL_EVENT = threading.Event()
_ANNOTATIONS_FD_ENV = "_CONTRAIL_ANNOTATIONS_FD"
_ANNOTATIONS_FALLBACK_ENV = "_CONTRAIL_ANNOTATIONS_FALLBACK"
_ANNOTATIONS_IDENTITY_ENV = "_CONTRAIL_ANNOTATIONS_IDENTITY"
_CAPTURE_WORKER_CLIENT_FD_ENV = "_CONTRAIL_CAPTURE_WORKER_CLIENT_FD"
_CAPTURE_WORKER_DETACHED_ENV = "_CONTRAIL_CAPTURE_WORKER_DETACHED"
_CAPTURE_WORKER_STDOUT_FD_ENV = "_CONTRAIL_CAPTURE_WORKER_STDOUT_FD"
_CAPTURE_WORKER_STDERR_FD_ENV = "_CONTRAIL_CAPTURE_WORKER_STDERR_FD"
_CAPTURE_JOB_ID_ENV = "_CONTRAIL_CAPTURE_JOB_ID"
_CAPTURE_JOB_ROOT_ENV = "_CONTRAIL_CAPTURE_JOB_ROOT"
_CAPTURE_RECOVERY_VERSION = 1
_CAPTURE_WORKER_METADATA_VERSION = 1
_IDENTIFIED_ENVIRONMENT_VARIABLES = (
    "CI",
    "CUDA_VISIBLE_DEVICES",
    "LANG",
    "LC_ALL",
    "MKL_NUM_THREADS",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "PYTHONHASHSEED",
    "TZ",
)


@dataclass(frozen=True, slots=True)
class OutputDigest:
    byte_count: int
    sha256: str
    captured: bytes | None
    truncated: bool
    relay_error: str | None
    pipe_open_after_exit: bool


@dataclass(frozen=True, slots=True)
class CaptureConfiguration:
    requested_level: str | None
    instrument: str | None
    observe_process_tree: bool


def resolve_capture_configuration(
    *,
    capture_level: str | None,
    instrument: str | None,
    observe_process_tree: bool,
) -> CaptureConfiguration:
    """Resolve one capture preset while retaining the lower-level expert options."""
    if capture_level is not None and capture_level not in CAPTURE_LEVELS:
        expected = ", ".join(repr(level) for level in CAPTURE_LEVELS)
        raise CaptureError(f"capture level must be one of {expected}, or None")
    if instrument not in (None, "sample", "deep"):
        raise CaptureError("instrument must be 'sample', 'deep', or None")
    if not isinstance(observe_process_tree, bool):
        raise CaptureError("observe process tree must be a boolean")
    if capture_level is not None and (instrument is not None or observe_process_tree):
        raise CaptureError(
            "capture level cannot be combined with instrumentation or process-tree observation"
        )
    if capture_level == "process":
        return CaptureConfiguration(capture_level, None, True)
    if capture_level in {"sample", "deep"}:
        return CaptureConfiguration(capture_level, capture_level, True)
    return CaptureConfiguration(capture_level, instrument, observe_process_tree)
