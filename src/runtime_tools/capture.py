"""Capture a local process into the normalized execution model."""

from __future__ import annotations

import hashlib
import os
import platform
import resource
import select
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

from runtime_tools.annotations import AnnotationError, load_annotations
from runtime_tools.artifacts import publish_without_overwrite
from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    JsonValue,
    Measurement,
)
from runtime_tools.storage import RunpackError, RunpackWriter


class CaptureError(ValueError):
    """Raised when a process cannot be captured."""


MAX_CAPTURE_OUTPUT_BYTES = 64 * 1024 * 1024
MAX_POST_EXIT_DRAIN_BYTES = 1024 * 1024
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


def _git_revision(cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() or None


def _pump(
    source: BinaryIO,
    sink: BinaryIO | None,
    capture_limit: int | None,
    process_done: threading.Event,
) -> OutputDigest:
    digest = hashlib.sha256()
    byte_count = 0
    captured = bytearray() if capture_limit is not None else None
    relay_error: str | None = None
    pipe_open_after_exit = False
    post_exit_bytes = 0
    descriptor = source.fileno()
    os.set_blocking(descriptor, False)

    def consume(chunk: bytes) -> None:
        nonlocal byte_count, relay_error, sink
        digest.update(chunk)
        byte_count += len(chunk)
        if captured is not None:
            assert capture_limit is not None
            if len(captured) < capture_limit:
                captured.extend(chunk[: capture_limit - len(captured)])
        if sink is not None:
            try:
                remaining = memoryview(chunk)
                while remaining:
                    written = sink.write(remaining)
                    if written is None or written <= 0 or written > len(remaining):
                        raise OSError("output relay did not accept the complete chunk")
                    remaining = remaining[written:]
                sink.flush()
            except Exception as exc:
                relay_error = f"{type(exc).__name__}: {exc}"
                sink = None

    while True:
        if process_done.is_set():
            try:
                chunk = os.read(descriptor, 64 * 1024)
            except BlockingIOError:
                pipe_open_after_exit = True
                break
            if not chunk:
                break
            consume(chunk)
            post_exit_bytes += len(chunk)
            if post_exit_bytes >= MAX_POST_EXIT_DRAIN_BYTES:
                try:
                    continuation = os.read(descriptor, 1)
                except BlockingIOError:
                    pipe_open_after_exit = True
                else:
                    if continuation:
                        consume(continuation)
                        pipe_open_after_exit = True
                break
            continue
        readable, _, _ = select.select((descriptor,), (), (), 0.05)
        if not readable:
            continue
        try:
            chunk = os.read(descriptor, 64 * 1024)
        except BlockingIOError:
            continue
        if not chunk:
            break
        consume(chunk)

    return OutputDigest(
        byte_count=byte_count,
        sha256=digest.hexdigest(),
        captured=bytes(captured) if captured is not None else None,
        truncated=captured is not None and byte_count > len(captured),
        relay_error=relay_error,
        pipe_open_after_exit=pipe_open_after_exit,
    )


def _output_metadata(digest: OutputDigest) -> dict[str, JsonValue]:
    metadata: dict[str, JsonValue] = {
        "bytes": digest.byte_count,
        "sha256": digest.sha256,
    }
    if digest.relay_error is not None:
        metadata["relay_error"] = digest.relay_error
    if digest.pipe_open_after_exit:
        metadata["pipe_open_after_exit"] = True
    return metadata


def _peak_memory_bytes(value: float) -> float:
    # Linux reports KiB; macOS and the other supported BSDs report bytes.
    return value * 1024 if sys.platform.startswith("linux") else value


def _wait_with_usage(
    process: subprocess.Popen[bytes],
) -> tuple[int, resource.struct_rusage]:
    while True:
        try:
            _, status, usage = os.wait4(process.pid, 0)
            break
        except InterruptedError:
            continue
    exit_code = os.waitstatus_to_exitcode(status)
    process.returncode = exit_code
    return exit_code, usage


def _initial_metadata() -> dict[str, JsonValue]:
    environment_identities: dict[str, JsonValue] = {
        name: hashlib.sha256(os.fsencode(os.environ[name])).hexdigest()
        for name in _IDENTIFIED_ENVIRONMENT_VARIABLES
        if name in os.environ
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


def record_process(
    command: tuple[str, ...],
    output: Path,
    *,
    name: str,
    cwd: Path | None = None,
    stdout: BinaryIO | None = None,
    stderr: BinaryIO | None = None,
    capture_output_limit: int | None = None,
) -> int:
    """Run ``command``, write ``output``, and return the process exit code."""
    if not isinstance(command, tuple) or not all(isinstance(item, str) for item in command):
        raise CaptureError("command must be a tuple of strings")
    if not command:
        raise CaptureError("a command is required")
    if not command[0]:
        raise CaptureError("command executable must be non-empty")
    if any("\0" in item for item in command):
        raise CaptureError("command arguments cannot contain NUL bytes")
    if capture_output_limit is not None and (
        not isinstance(capture_output_limit, int) or isinstance(capture_output_limit, bool)
    ):
        raise CaptureError("capture output limit must be an integer")
    if capture_output_limit is not None and capture_output_limit <= 0:
        raise CaptureError("capture output limit must be positive")
    if capture_output_limit is not None and capture_output_limit > MAX_CAPTURE_OUTPUT_BYTES:
        raise CaptureError(
            f"capture output limit cannot exceed {MAX_CAPTURE_OUTPUT_BYTES} bytes per stream"
        )
    if output.exists():
        raise CaptureError(f"refusing to overwrite existing runpack: {output}")
    working_directory = (cwd or Path.cwd()).resolve()
    if not working_directory.is_dir():
        raise CaptureError(f"working directory does not exist: {working_directory}")
    if not output.parent.is_dir():
        raise CaptureError(f"output directory does not exist: {output.parent}")

    execution_id = uuid.uuid4().hex
    entity_id = uuid.uuid4().hex
    event_id = uuid.uuid4().hex
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")
    annotation_path = output.with_name(f".{output.name}.annotations-{uuid.uuid4().hex}")
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.perf_counter_ns()
    metadata = _initial_metadata()

    try:
        with RunpackWriter(temporary) as writer:
            writer.add_execution(
                Execution(
                    id=execution_id,
                    name=name,
                    started_at_ns=started_at_ns,
                    finished_at_ns=None,
                    command=command,
                    working_directory=str(working_directory),
                    exit_code=None,
                    revision=_git_revision(working_directory),
                    metadata=metadata,
                )
            )
            writer.add_entity(
                Entity(
                    id=entity_id,
                    kind="process",
                    name=Path(command[0]).name,
                    parent_entity_id=None,
                    attributes={},
                )
            )
            try:
                process = subprocess.Popen(
                    command,
                    cwd=working_directory,
                    env={**os.environ, "CONTRAIL_ANNOTATIONS_FILE": str(annotation_path.resolve())},
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            except (OSError, ValueError) as exc:
                raise CaptureError(f"could not start command {command[0]!r}: {exc}") from exc

            if process.stdout is None or process.stderr is None:
                raise CaptureError("failed to capture process output")
            stdout_pipe = cast(BinaryIO, process.stdout)
            stderr_pipe = cast(BinaryIO, process.stderr)
            process_done = threading.Event()
            with ThreadPoolExecutor(max_workers=2) as executor:
                stdout_result = executor.submit(
                    _pump, stdout_pipe, stdout, capture_output_limit, process_done
                )
                stderr_result = executor.submit(
                    _pump, stderr_pipe, stderr, capture_output_limit, process_done
                )
                try:
                    exit_code, process_usage = _wait_with_usage(process)
                finally:
                    process_done.set()
                stdout_digest = stdout_result.result()
                stderr_digest = stderr_result.result()

            elapsed_ns = time.perf_counter_ns() - started_monotonic_ns
            finished_at_ns = started_at_ns + elapsed_ns
            wall_seconds = elapsed_ns / 1_000_000_000
            metadata = {
                **metadata,
                "output": {
                    "stdout": _output_metadata(stdout_digest),
                    "stderr": _output_metadata(stderr_digest),
                },
            }
            try:
                annotation_events, annotation_edges = load_annotations(
                    annotation_path, entity_id=entity_id
                )
                writer.add_event_graph(annotation_events, annotation_edges)
            except (AnnotationError, RunpackError) as exc:
                annotation_events, annotation_edges = (), ()
                capture_metadata = metadata.get("capture")
                assert isinstance(capture_metadata, dict)
                metadata = {
                    **metadata,
                    "capture": {**capture_metadata, "annotation_error": str(exc)},
                }
            measurements = (
                Measurement("process.wall_time", wall_seconds, "s", finished_at_ns, entity_id, {}),
                Measurement(
                    "process.cpu.user",
                    process_usage.ru_utime,
                    "s",
                    finished_at_ns,
                    entity_id,
                    {},
                ),
                Measurement(
                    "process.cpu.system",
                    process_usage.ru_stime,
                    "s",
                    finished_at_ns,
                    entity_id,
                    {},
                ),
                Measurement(
                    "process.memory.peak",
                    _peak_memory_bytes(process_usage.ru_maxrss),
                    "By",
                    finished_at_ns,
                    entity_id,
                    {},
                ),
                Measurement(
                    "process.stdout.bytes",
                    float(stdout_digest.byte_count),
                    "By",
                    finished_at_ns,
                    entity_id,
                    {},
                ),
                Measurement(
                    "process.stderr.bytes",
                    float(stderr_digest.byte_count),
                    "By",
                    finished_at_ns,
                    entity_id,
                    {},
                ),
            )
            writer.finish_execution(
                execution_id,
                finished_at_ns=finished_at_ns,
                exit_code=exit_code,
                metadata=metadata,
                event=Event(
                    id=event_id,
                    kind="process.run",
                    name=Path(command[0]).name,
                    entity_id=entity_id,
                    started_at_ns=started_at_ns,
                    finished_at_ns=finished_at_ns,
                    clock_domain="host.wall",
                    uncertainty_ns=None,
                    sequence=0,
                    attributes={"exit_code": exit_code},
                ),
                measurements=measurements,
            )
            output_digests = (("stdout", stdout_digest), ("stderr", stderr_digest))
            writer.add_attachments(
                Attachment(
                    id=f"capture:{execution_id}:{stream_name}",
                    kind="log",
                    name=stream_name,
                    media_type="application/octet-stream",
                    content=digest.captured,
                    attributes={
                        "captured_bytes": len(digest.captured),
                        "total_bytes": digest.byte_count,
                        "truncated": digest.truncated,
                    },
                )
                for stream_name, digest in output_digests
                if digest.captured is not None
            )
            annotation_targets = {edge.target_event_id for edge in annotation_edges}
            writer.add_causal_edges(
                CausalEdge(
                    event_id,
                    annotation_event.id,
                    "parent",
                    1.0,
                    {"source": "capture"},
                )
                for annotation_event in annotation_events
                if annotation_event.id not in annotation_targets
            )
        try:
            publish_without_overwrite(temporary, output)
        except FileExistsError as exc:
            raise CaptureError(f"refusing to overwrite existing runpack: {output}") from exc
        except OSError as exc:
            raise CaptureError(f"could not publish runpack {output}: {exc}") from exc
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        annotation_path.unlink(missing_ok=True)
    return exit_code
