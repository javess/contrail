"""Capture a local process into the normalized execution model."""

from __future__ import annotations

import hashlib
import os
import platform
import resource
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

from runtime_tools.annotations import AnnotationError, load_annotations
from runtime_tools.artifacts import publish_without_overwrite
from runtime_tools.model import CausalEdge, Entity, Event, Execution, JsonValue, Measurement
from runtime_tools.storage import RunpackWriter


class CaptureError(ValueError):
    """Raised when a process cannot be captured."""


@dataclass(frozen=True, slots=True)
class OutputDigest:
    byte_count: int
    sha256: str


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


def _pump(source: BinaryIO, sink: BinaryIO | None) -> OutputDigest:
    digest = hashlib.sha256()
    byte_count = 0
    while chunk := source.read(64 * 1024):
        digest.update(chunk)
        byte_count += len(chunk)
        if sink is not None:
            sink.write(chunk)
            sink.flush()
    return OutputDigest(byte_count=byte_count, sha256=digest.hexdigest())


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
    return {
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
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
) -> int:
    """Run ``command``, write ``output``, and return the process exit code."""
    if not command:
        raise CaptureError("a command is required")
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
            except OSError as exc:
                raise CaptureError(f"could not start command {command[0]!r}: {exc}") from exc

            if process.stdout is None or process.stderr is None:
                raise CaptureError("failed to capture process output")
            stdout_pipe = cast(BinaryIO, process.stdout)
            stderr_pipe = cast(BinaryIO, process.stderr)
            with ThreadPoolExecutor(max_workers=2) as executor:
                stdout_result = executor.submit(_pump, stdout_pipe, stdout)
                stderr_result = executor.submit(_pump, stderr_pipe, stderr)
                exit_code, process_usage = _wait_with_usage(process)
                stdout_digest = stdout_result.result()
                stderr_digest = stderr_result.result()

            finished_at_ns = time.time_ns()
            wall_seconds = (time.perf_counter_ns() - started_monotonic_ns) / 1_000_000_000
            metadata = {
                **metadata,
                "output": {
                    "stdout": {
                        "bytes": stdout_digest.byte_count,
                        "sha256": stdout_digest.sha256,
                    },
                    "stderr": {
                        "bytes": stderr_digest.byte_count,
                        "sha256": stderr_digest.sha256,
                    },
                },
            }
            try:
                annotation_events, annotation_edges = load_annotations(
                    annotation_path, entity_id=entity_id
                )
            except AnnotationError as exc:
                raise CaptureError(str(exc)) from exc
            writer.add_events(annotation_events)
            writer.add_causal_edges(annotation_edges)
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
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        annotation_path.unlink(missing_ok=True)
    return exit_code
