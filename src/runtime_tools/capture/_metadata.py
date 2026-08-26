"""Capture metadata assembly and provenance."""

from __future__ import annotations

import hashlib
import os
import platform
import threading
from collections.abc import Mapping

from runtime_tools.capture._common import (
    _CAPTURE_RECOVERY_VERSION,
    _CAPTURE_WORKER_METADATA_VERSION,
    _IDENTIFIED_ENVIRONMENT_VARIABLES,
    CaptureError,
)
from runtime_tools.deep_profile import (
    DeepProfileSession,
)
from runtime_tools.model import (
    JsonValue,
)
from runtime_tools.terminal import terminal_text


def _validate_annotation_fd(descriptor: int | None) -> None:
    if descriptor is None:
        return
    if (
        os.name != "posix"
        or not isinstance(descriptor, int)
        or isinstance(descriptor, bool)
        or descriptor < 3
    ):
        raise CaptureError("annotation transport fd must be an open POSIX descriptor above 2")
    try:
        descriptor_status = os.fstat(descriptor)
        devnull_status = os.stat(os.devnull)
        inheritable = os.get_inheritable(descriptor)
    except OSError as exc:
        raise CaptureError("annotation transport fd must be open") from exc
    if inheritable or not os.path.samestat(descriptor_status, devnull_status):
        raise CaptureError("annotation transport fd must be a reserved non-inheritable devnull fd")


def _restore_annotation_fd(descriptor: int) -> None:
    placeholder = os.open(os.devnull, os.O_RDONLY)
    try:
        os.dup2(placeholder, descriptor, inheritable=False)
    finally:
        os.close(placeholder)


def _initial_metadata(
    environment_names: tuple[str, ...] = _IDENTIFIED_ENVIRONMENT_VARIABLES,
    *,
    environment: Mapping[str, str] | None = None,
) -> dict[str, JsonValue]:
    actual_environment = os.environ if environment is None else environment
    environment_identities: dict[str, JsonValue] = {
        name: hashlib.sha256(os.fsencode(actual_environment[name])).hexdigest()
        for name in environment_names
        if name in actual_environment
    }
    return {
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "capture_runtime": {
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
        },
        "environment": {"selected_value_sha256": environment_identities},
        "capture": {"adapter": "local-process"},
    }


def _metadata_with_recovery(
    metadata: dict[str, JsonValue],
    *,
    status: str,
    recovered: bool,
    profile_checkpoint: dict[str, JsonValue] | None = None,
    root_process_id: int | None = None,
    entity_id: str | None = None,
) -> dict[str, JsonValue]:
    capture_metadata = metadata.get("capture")
    if not isinstance(capture_metadata, dict):
        raise CaptureError("capture metadata is unavailable for recovery")
    recovery: dict[str, JsonValue] = {
        "format_version": _CAPTURE_RECOVERY_VERSION,
        "status": status,
        "checkpoint": "post-exit",
        "controller_restart_recovered": recovered,
    }
    if profile_checkpoint is not None:
        recovery["profile_session"] = profile_checkpoint
    if root_process_id is not None:
        recovery["root_process_id"] = root_process_id
    if entity_id is not None:
        recovery["entity_id"] = entity_id
    return {
        **metadata,
        "capture": {
            **capture_metadata,
            "recovery": recovery,
        },
    }


def _invalid_instrumentation_metadata(
    session: DeepProfileSession,
    exc: BaseException,
) -> dict[str, JsonValue]:
    return {
        "mode": session.mode,
        "observer": (
            "python-sys-setprofile-and-settrace"
            if session.mode == "deep"
            else "python-stack-sampler"
        ),
        "intrusive": True,
        "estimated": session.mode == "sample",
        "per_call": session.mode == "deep",
        "status": "invalid",
        "error": terminal_text(exc),
    }


def _metadata_with_instrumentation(
    metadata: dict[str, JsonValue],
    instrumentation: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    capture_metadata = metadata.get("capture")
    if not isinstance(capture_metadata, dict):
        raise CaptureError("capture metadata is unavailable for instrumentation")
    return {
        **metadata,
        "capture": {
            **capture_metadata,
            "instrumentation": instrumentation,
        },
    }


def _metadata_with_capture_worker(
    metadata: dict[str, JsonValue],
    client_disconnected: threading.Event,
) -> dict[str, JsonValue]:
    capture_metadata = metadata.get("capture")
    if not isinstance(capture_metadata, dict):
        raise CaptureError("capture metadata is unavailable for worker provenance")
    return {
        **metadata,
        "capture": {
            **capture_metadata,
            "worker": {
                "format_version": _CAPTURE_WORKER_METADATA_VERSION,
                "mode": "separate-process",
                "client_disconnected": client_disconnected.is_set(),
            },
        },
    }
