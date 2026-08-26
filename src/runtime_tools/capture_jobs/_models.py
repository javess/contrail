"""Private, daemon-free lifecycle registry for local CLI capture workers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Annotated

from pydantic import Field, computed_field

from runtime_tools.capture_jobs._common import (
    _TERMINAL_STATES,
    CAPTURE_JOB_FORMAT_VERSION,
    CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
    MAX_CAPTURE_JOB_ARTIFACT_BYTES,
    MAX_CAPTURE_JOB_ARTIFACTS,
    MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES,
    CaptureJobError,
    CaptureJobOutputRetention,
    CaptureJobOutputStream,
    CaptureJobState,
    _boolean,
    _capture_job_output_limits,
    _integer,
    _optional_integer,
    _validate_operation,
)
from runtime_tools.json_support import JsonValueModel, output_document
from runtime_tools.model import JsonValue


@dataclass(frozen=True, slots=True)
class CaptureJob(JsonValueModel):
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
    stdout_size_bytes: Annotated[int, Field(exclude=True)] = 0
    stdout_truncated: Annotated[bool, Field(exclude=True)] = False
    stderr_size_bytes: Annotated[int, Field(exclude=True)] = 0
    stderr_truncated: Annotated[bool, Field(exclude=True)] = False
    output_retention: Annotated[CaptureJobOutputRetention, Field(exclude=True)] = "none"
    stdout_head_size_bytes: Annotated[int, Field(exclude=True)] = 0
    stdout_tail_size_bytes: Annotated[int, Field(exclude=True)] = 0
    stdout_omitted_bytes: Annotated[int, Field(exclude=True)] = 0
    stdout_omitted_bytes_truncated: Annotated[bool, Field(exclude=True)] = False
    stderr_head_size_bytes: Annotated[int, Field(exclude=True)] = 0
    stderr_tail_size_bytes: Annotated[int, Field(exclude=True)] = 0
    stderr_omitted_bytes: Annotated[int, Field(exclude=True)] = 0
    stderr_omitted_bytes_truncated: Annotated[bool, Field(exclude=True)] = False

    @property
    def terminal(self) -> bool:
        return self.state in _TERMINAL_STATES

    @property
    def output_limits(self) -> tuple[int, int]:
        return _capture_job_output_limits(self.output_retention)

    @computed_field
    @property
    def output(self) -> dict[str, JsonValue]:
        return {
            "retained": self.detached,
            "limit_bytes_per_stream": CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
            "retention_strategy": self.output_retention,
            "head_limit_bytes_per_stream": self.output_limits[0],
            "tail_limit_bytes_per_stream": self.output_limits[1],
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


@dataclass(frozen=True, slots=True)
class _CaptureJobStreamState:
    size_bytes: int = 0
    truncated: bool = False
    head_size_bytes: int = 0
    tail_size_bytes: int = 0
    omitted_bytes: int = 0
    omitted_bytes_truncated: bool = False


def capture_job_document(job: CaptureJob) -> dict[str, JsonValue]:
    return output_document("runtime.capture_job", {"job": job.as_json_value()})


def capture_jobs_document(jobs: tuple[CaptureJob, ...]) -> dict[str, JsonValue]:
    return output_document(
        "runtime.capture_jobs",
        {"jobs": [job.as_json_value() for job in jobs]},
    )


def _capture_job_stream_state(
    output: dict[str, object],
    stream: CaptureJobOutputStream,
    *,
    head_limit: int,
    tail_limit: int,
) -> _CaptureJobStreamState:
    state = _CaptureJobStreamState(
        size_bytes=_integer(
            output.get(f"{stream}_size_bytes"),
            f"{stream} size",
            minimum=0,
            maximum=CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
        ),
        truncated=_boolean(
            output.get(f"{stream}_truncated"),
            f"{stream} truncation state",
        ),
        head_size_bytes=_integer(
            output.get(f"{stream}_head_size_bytes"),
            f"{stream} head size",
            minimum=0,
            maximum=head_limit,
        ),
        tail_size_bytes=_integer(
            output.get(f"{stream}_tail_size_bytes"),
            f"{stream} tail size",
            minimum=0,
            maximum=tail_limit,
        ),
        omitted_bytes=_integer(
            output.get(f"{stream}_omitted_bytes"),
            f"{stream} omitted size",
            minimum=0,
            maximum=MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES,
        ),
        omitted_bytes_truncated=_boolean(
            output.get(f"{stream}_omitted_bytes_truncated"),
            f"{stream} omitted output state",
        ),
    )
    if state.size_bytes != state.head_size_bytes + state.tail_size_bytes:
        raise CaptureJobError(f"capture job {stream} segment sizes are inconsistent")
    if (state.omitted_bytes or state.omitted_bytes_truncated) and not state.truncated:
        raise CaptureJobError(f"capture job omitted {stream} is not marked truncated")
    return state


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
    detached = value.get("detached")
    if not isinstance(detached, bool):
        raise CaptureJobError("capture job detached state is invalid")
    output_value = value.get("output")
    output_retention: CaptureJobOutputRetention = "none"
    stdout = _CaptureJobStreamState()
    stderr = _CaptureJobStreamState()
    if not isinstance(output_value, dict) or not all(isinstance(key, str) for key in output_value):
        raise CaptureJobError("capture job output state is invalid")
    if output_value.get("retained") is not detached:
        raise CaptureJobError("capture job output retention state is invalid")
    if output_value.get("limit_bytes_per_stream") != CAPTURE_JOB_OUTPUT_LIMIT_BYTES:
        raise CaptureJobError("capture job output limit is unsupported")
    retention_value = output_value.get("retention_strategy")
    if retention_value not in {"none", "head", "head-tail"}:
        raise CaptureJobError("capture job output retention strategy is invalid")
    output_retention = retention_value
    head_limit, tail_limit = _capture_job_output_limits(output_retention)
    if (
        output_value.get("head_limit_bytes_per_stream") != head_limit
        or output_value.get("tail_limit_bytes_per_stream") != tail_limit
    ):
        raise CaptureJobError("capture job output segment limits are unsupported")
    stdout = _capture_job_stream_state(
        output_value,
        "stdout",
        head_limit=head_limit,
        tail_limit=tail_limit,
    )
    stderr = _capture_job_stream_state(
        output_value,
        "stderr",
        head_limit=head_limit,
        tail_limit=tail_limit,
    )
    if detached and output_retention == "none":
        raise CaptureJobError("detached capture job must retain output")
    if not detached and output_retention != "none":
        raise CaptureJobError("attached capture job cannot retain output")
    if not detached and (stdout != _CaptureJobStreamState() or stderr != _CaptureJobStreamState()):
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
        stdout_size_bytes=stdout.size_bytes,
        stdout_truncated=stdout.truncated,
        stderr_size_bytes=stderr.size_bytes,
        stderr_truncated=stderr.truncated,
        output_retention=output_retention,
        stdout_head_size_bytes=stdout.head_size_bytes,
        stdout_tail_size_bytes=stdout.tail_size_bytes,
        stdout_omitted_bytes=stdout.omitted_bytes,
        stdout_omitted_bytes_truncated=stdout.omitted_bytes_truncated,
        stderr_head_size_bytes=stderr.head_size_bytes,
        stderr_tail_size_bytes=stderr.tail_size_bytes,
        stderr_omitted_bytes=stderr.omitted_bytes,
        stderr_omitted_bytes_truncated=stderr.omitted_bytes_truncated,
    )
