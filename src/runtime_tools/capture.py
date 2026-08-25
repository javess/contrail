"""Capture a local process into the normalized execution model."""

from __future__ import annotations

import hashlib
import os
import platform
import resource
import select
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

from runtime_tools.annotations import AnnotationError, load_annotations
from runtime_tools.artifacts import artifact_exists, publish_without_overwrite, remove_best_effort
from runtime_tools.deep_profile import (
    DEEP_PROFILE_DIRECTORY_ENV,
    PROFILE_SNAPSHOT_SOCKET_ENV,
    SAMPLE_PROFILE_DIRECTORY_ENV,
    DeepProfileError,
    DeepProfileSession,
    load_python_profile,
    prepare_deep_profile_session,
    prepare_sample_profile_session,
    profile_session_checkpoint,
    recover_profile_session,
)
from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    JsonValue,
    Measurement,
)
from runtime_tools.process_observer import ProcessObservationResult, ProcessTreeObserver
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter
from runtime_tools.terminal import terminal_text


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
    except (OSError, UnicodeError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    revision = result.stdout.strip()
    if len(revision) not in (40, 64) or any(
        character not in "0123456789abcdefABCDEF" for character in revision
    ):
        return None
    return revision.lower()


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

    def read(max_bytes: int) -> bytes:
        while True:
            try:
                return os.read(descriptor, max_bytes)
            except InterruptedError:
                continue

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
                relay_error = terminal_text(f"{type(exc).__name__}: {exc}")
                sink = None

    while True:
        if process_done.is_set():
            try:
                chunk = read(64 * 1024)
            except BlockingIOError:
                pipe_open_after_exit = True
                break
            if not chunk:
                break
            consume(chunk)
            post_exit_bytes += len(chunk)
            if post_exit_bytes >= MAX_POST_EXIT_DRAIN_BYTES:
                try:
                    continuation = read(1)
                except BlockingIOError:
                    pipe_open_after_exit = True
                else:
                    if continuation:
                        consume(continuation)
                        pipe_open_after_exit = True
                break
            continue
        try:
            readable, _, _ = select.select((descriptor,), (), (), 0.05)
        except InterruptedError:
            continue
        if not readable:
            continue
        try:
            chunk = read(64 * 1024)
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
    output_pumps: tuple[Future[OutputDigest], ...] = (),
) -> tuple[int, resource.struct_rusage]:
    while True:
        for future in output_pumps:
            if future.done():
                future.result()
        try:
            child_pid, status, usage = os.wait4(process.pid, os.WNOHANG)
        except InterruptedError:
            continue
        except OSError as exc:
            raise CaptureError(f"could not collect captured process status: {exc}") from exc
        if child_pid == process.pid:
            break
        _STATUS_POLL_EVENT.wait(0.01)
    exit_code = os.waitstatus_to_exitcode(status)
    process.returncode = exit_code
    return exit_code, usage


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _signal_process_group(process_group: int, signal_number: int) -> None:
    try:
        os.killpg(process_group, signal_number)
    except ProcessLookupError:
        pass
    except OSError:
        pass


def _terminate_and_reap(process: subprocess.Popen[bytes]) -> None:
    """Best-effort cleanup for a captured process group after capture failure."""
    process_group = process.pid
    _signal_process_group(process_group, signal.SIGTERM)
    deadline = time.monotonic() + PROCESS_TERMINATION_TIMEOUT_SECONDS
    while _process_group_exists(process_group) and time.monotonic() < deadline:
        try:
            process.poll()
        except OSError:
            pass
        remaining = deadline - time.monotonic()
        if remaining > 0:
            _STATUS_POLL_EVENT.wait(min(0.01, remaining))
    if _process_group_exists(process_group):
        _signal_process_group(process_group, signal.SIGKILL)
    try:
        process.wait(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        _signal_process_group(process_group, signal.SIGKILL)
        try:
            process.wait(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
        except (subprocess.TimeoutExpired, ProcessLookupError, OSError):
            pass
    except ProcessLookupError:
        try:
            process.wait()
        except OSError:
            pass
    except OSError:
        pass


def _identified_environment_names(custom: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(custom, tuple) or not all(isinstance(name, str) for name in custom):
        raise CaptureError("identified environment names must be a tuple of strings")
    if len(custom) > MAX_CUSTOM_ENVIRONMENT_IDENTITIES:
        raise CaptureError(
            "cannot identify more than "
            f"{MAX_CUSTOM_ENVIRONMENT_IDENTITIES} custom environment variables"
        )
    for name in custom:
        if not name or "\0" in name or "=" in name:
            raise CaptureError(
                "identified environment names must be non-empty and cannot contain '=' or NUL"
            )
        try:
            encoded = name.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise CaptureError("identified environment names must be valid UTF-8") from exc
        if len(encoded) > MAX_ENVIRONMENT_NAME_BYTES:
            raise CaptureError(
                "identified environment names cannot exceed "
                f"{MAX_ENVIRONMENT_NAME_BYTES} UTF-8 bytes"
            )
    return tuple(dict.fromkeys((*_IDENTIFIED_ENVIRONMENT_VARIABLES, *custom)))


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


def _assemble_profile_checkpoint(
    writer: RunpackWriter,
    *,
    execution_id: str,
    entity_id: str,
    root_process_id: int,
    metadata: dict[str, JsonValue],
    session: DeepProfileSession,
    profile_checkpoint: dict[str, JsonValue],
    recovered: bool,
) -> dict[str, JsonValue]:
    try:
        result = load_python_profile(
            session,
            entity_id=entity_id,
            root_process_id=root_process_id,
        )
        assembled = _metadata_with_instrumentation(metadata, result.as_metadata())
        assembled = _metadata_with_recovery(
            assembled,
            status="assembled",
            recovered=recovered,
            profile_checkpoint=profile_checkpoint,
        )
        writer.add_event_graph_and_set_execution_metadata(
            execution_id,
            result.events,
            result.edges,
            assembled,
        )
    except (DeepProfileError, RunpackError) as exc:
        assembled = _metadata_with_instrumentation(
            metadata,
            _invalid_instrumentation_metadata(session, exc),
        )
        assembled = _metadata_with_recovery(
            assembled,
            status="assembled",
            recovered=recovered,
            profile_checkpoint=profile_checkpoint,
        )
        writer.set_execution_metadata(execution_id, assembled)
    session.close()
    complete = _metadata_with_recovery(
        assembled,
        status="complete",
        recovered=recovered,
    )
    writer.set_execution_metadata(execution_id, complete)
    return complete


def _recovery_object(metadata: dict[str, JsonValue]) -> dict[str, JsonValue]:
    capture_metadata = metadata.get("capture")
    if not isinstance(capture_metadata, dict):
        raise CaptureError("recovery checkpoint has no capture metadata")
    recovery = capture_metadata.get("recovery")
    if not isinstance(recovery, dict):
        raise CaptureError("runpack has no post-exit recovery checkpoint")
    if recovery.get("format_version") != _CAPTURE_RECOVERY_VERSION:
        raise CaptureError("capture recovery checkpoint version is unsupported")
    if recovery.get("checkpoint") != "post-exit":
        raise CaptureError("capture recovery checkpoint kind is unsupported")
    status = recovery.get("status")
    if status not in {"pending", "assembled", "complete"}:
        raise CaptureError("capture recovery checkpoint status is unsupported")
    if not isinstance(recovery.get("controller_restart_recovered"), bool):
        raise CaptureError("capture recovery checkpoint provenance is invalid")
    return recovery


def _pending_profile_session(
    recovery: dict[str, JsonValue],
    *,
    entity_ids: set[str],
) -> tuple[DeepProfileSession, dict[str, JsonValue], str, int] | None:
    raw_checkpoint = recovery.get("profile_session")
    raw_entity_id = recovery.get("entity_id")
    raw_root_process_id = recovery.get("root_process_id")
    if raw_checkpoint is None:
        if raw_entity_id is not None or raw_root_process_id is not None:
            raise CaptureError("capture recovery checkpoint has incomplete profile identity")
        return None
    if not isinstance(raw_checkpoint, dict):
        raise CaptureError("capture recovery profile session is invalid")
    if not isinstance(raw_entity_id, str) or not raw_entity_id or raw_entity_id not in entity_ids:
        raise CaptureError("capture recovery profile entity is invalid")
    if (
        not isinstance(raw_root_process_id, int)
        or isinstance(raw_root_process_id, bool)
        or raw_root_process_id <= 0
        or raw_root_process_id > (1 << 63) - 1
    ):
        raise CaptureError("capture recovery root process id is invalid")
    try:
        session = recover_profile_session(raw_checkpoint)
    except DeepProfileError as exc:
        raise CaptureError(str(exc)) from exc
    return session, raw_checkpoint, raw_entity_id, raw_root_process_id


def recover_process_capture(checkpoint: Path, output: Path) -> int:
    """Finish and publish a post-exit capture checkpoint after controller loss."""
    if not isinstance(checkpoint, Path) or not isinstance(output, Path):
        raise CaptureError("capture recovery paths must be paths")
    if artifact_exists(output):
        raise CaptureError(f"refusing to overwrite existing runpack: {output}")
    if not output.parent.is_dir():
        raise CaptureError(f"output directory does not exist: {output.parent}")
    try:
        with RunpackReader(checkpoint) as reader:
            source = reader.path
            execution = reader.execution()
            entities = reader.entities()
    except RunpackError as exc:
        raise CaptureError(f"invalid capture recovery checkpoint: {exc}") from exc
    if execution.finished_at_ns is None or execution.exit_code is None:
        raise CaptureError("capture recovery checkpoint was not created after workload exit")
    recovery = _recovery_object(execution.metadata)
    status = recovery["status"]
    pending_profile = (
        _pending_profile_session(
            recovery,
            entity_ids={entity.id for entity in entities},
        )
        if status == "pending"
        else None
    )
    writer: RunpackWriter | None = None
    try:
        writer = RunpackWriter.open_existing(source)
        if status == "pending" and pending_profile is not None:
            session, profile_checkpoint, entity_id, root_process_id = pending_profile
            _assemble_profile_checkpoint(
                writer,
                execution_id=execution.id,
                entity_id=entity_id,
                root_process_id=root_process_id,
                metadata=execution.metadata,
                session=session,
                profile_checkpoint=profile_checkpoint,
                recovered=True,
            )
        else:
            if status == "assembled":
                raw_profile = recovery.get("profile_session")
                if raw_profile is not None:
                    try:
                        recover_profile_session(raw_profile).close()
                    except DeepProfileError:
                        pass
            complete = _metadata_with_recovery(
                execution.metadata,
                status="complete",
                recovered=True,
            )
            writer.set_execution_metadata(execution.id, complete)
    finally:
        if writer is not None:
            writer.close()
    try:
        publish_without_overwrite(source, output)
    except FileExistsError as exc:
        raise CaptureError(f"refusing to overwrite existing runpack: {output}") from exc
    except OSError as exc:
        raise CaptureError(f"could not publish recovered runpack {output}: {exc}") from exc
    return execution.exit_code


def record_process(
    command: tuple[str, ...],
    output: Path,
    *,
    name: str,
    cwd: Path | None = None,
    stdout: BinaryIO | None = None,
    stderr: BinaryIO | None = None,
    capture_output_limit: int | None = None,
    identify_environment: tuple[str, ...] = (),
    capture_level: str | None = None,
    instrument: str | None = None,
    observe_process_tree: bool = False,
    _annotation_fd: int | None = None,
    _annotation_directory: Path | None = None,
    _capture_client_disconnected: threading.Event | None = None,
) -> int:
    """Run ``command``, write ``output``, and return the process exit code.

    ``capture_level`` selects ``passive``, ``process``, ``sample``, or ``deep``.
    The sampling and deep presets also enable controller-side process capture.
    ``instrument="sample"`` injects bounded statistical Python stack sampling.
    ``instrument="deep"`` injects intrusive, bounded Python and native C call profiling.
    Both modes require interpreters that honor inherited site initialization.
    ``observe_process_tree=True`` samples process-group RSS and CPU from the
    controller without modifying the workload.
    """
    if not isinstance(command, tuple) or not all(isinstance(item, str) for item in command):
        raise CaptureError("command must be a tuple of strings")
    if not command:
        raise CaptureError("a command is required")
    if not command[0]:
        raise CaptureError("command executable must be non-empty")
    if any("\0" in item for item in command):
        raise CaptureError("command arguments cannot contain NUL bytes")
    try:
        for item in command:
            item.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CaptureError("command arguments must be valid UTF-8") from exc
    if not isinstance(name, str) or not name:
        raise CaptureError("capture name must be a non-empty string")
    try:
        name.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CaptureError("capture name must be valid UTF-8") from exc
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
    configuration = resolve_capture_configuration(
        capture_level=capture_level,
        instrument=instrument,
        observe_process_tree=observe_process_tree,
    )
    instrument = configuration.instrument
    observe_process_tree = configuration.observe_process_tree
    environment_names = _identified_environment_names(identify_environment)
    _validate_annotation_fd(_annotation_fd)
    if _capture_client_disconnected is not None and not isinstance(
        _capture_client_disconnected, threading.Event
    ):
        raise CaptureError("capture client-disconnected state must be a threading event")
    if artifact_exists(output):
        raise CaptureError(f"refusing to overwrite existing runpack: {output}")
    try:
        working_directory = (cwd or Path.cwd()).resolve()
    except (OSError, RuntimeError) as exc:
        raise CaptureError("could not resolve the capture working directory") from exc
    if not working_directory.is_dir():
        raise CaptureError(f"working directory does not exist: {working_directory}")
    if not output.parent.is_dir():
        raise CaptureError(f"output directory does not exist: {output.parent}")
    if _annotation_directory is not None and _annotation_fd is None:
        raise CaptureError("a private annotation directory requires an annotation transport fd")
    if _annotation_directory is not None and (
        not isinstance(_annotation_directory, Path) or not _annotation_directory.is_absolute()
    ):
        raise CaptureError("private annotation directory must be an absolute path")
    try:
        annotation_directory = (
            output.parent if _annotation_directory is None else _annotation_directory.resolve()
        )
    except (OSError, RuntimeError) as exc:
        raise CaptureError("could not resolve the private annotation directory") from exc
    if not annotation_directory.is_dir():
        raise CaptureError(f"private annotation directory does not exist: {annotation_directory}")

    execution_id = uuid.uuid4().hex
    entity_id = uuid.uuid4().hex
    event_id = uuid.uuid4().hex
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")
    annotation_path = annotation_directory / f".{output.name}.annotations-{uuid.uuid4().hex}"

    temporary_created = False
    annotation_created = False
    annotation_fd_installed = False
    recovery_ready = False
    published = False
    deep_profile_session: DeepProfileSession | None = None
    process_observer: ProcessTreeObserver | None = None
    try:
        if instrument == "deep":
            try:
                deep_profile_session = prepare_deep_profile_session(annotation_directory)
            except DeepProfileError as exc:
                raise CaptureError(str(exc)) from exc
        elif instrument == "sample":
            try:
                deep_profile_session = prepare_sample_profile_session(annotation_directory)
            except DeepProfileError as exc:
                raise CaptureError(str(exc)) from exc
        try:
            annotation_descriptor = os.open(
                annotation_path,
                os.O_APPEND | os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError as exc:
            raise CaptureError(
                f"temporary annotation file already exists: {annotation_path}"
            ) from exc
        except OSError as exc:
            raise CaptureError(f"could not create temporary annotation file: {exc}") from exc
        annotation_created = True
        try:
            annotation_status = os.fstat(annotation_descriptor)
            if _annotation_fd is not None:
                os.dup2(annotation_descriptor, _annotation_fd, inheritable=False)
                annotation_fd_installed = True
        except OSError as exc:
            raise CaptureError(f"could not prepare temporary annotation file: {exc}") from exc
        finally:
            active_error = sys.exception()
            try:
                os.close(annotation_descriptor)
            except OSError as exc:
                if active_error is None:
                    raise CaptureError(
                        f"could not prepare temporary annotation file: {exc}"
                    ) from exc
                active_error.add_note(f"temporary annotation fd cleanup also failed: {exc}")
        try:
            resolved_annotation_path = annotation_path.resolve()
        except (OSError, RuntimeError) as exc:
            raise CaptureError("could not resolve temporary annotation file") from exc
        child_environment: dict[str, str] = {
            **os.environ,
            "PWD": str(working_directory),
            "CONTRAIL_ANNOTATIONS_FILE": (
                str(resolved_annotation_path)
                if _annotation_fd is None
                else f"/dev/fd/{_annotation_fd}"
            ),
        }
        child_environment.pop(_ANNOTATIONS_FD_ENV, None)
        child_environment.pop(_ANNOTATIONS_FALLBACK_ENV, None)
        child_environment.pop(_ANNOTATIONS_IDENTITY_ENV, None)
        child_environment.pop(_CAPTURE_WORKER_CLIENT_FD_ENV, None)
        child_environment.pop(_CAPTURE_WORKER_DETACHED_ENV, None)
        child_environment.pop(_CAPTURE_WORKER_STDOUT_FD_ENV, None)
        child_environment.pop(_CAPTURE_WORKER_STDERR_FD_ENV, None)
        child_environment.pop(_CAPTURE_JOB_ID_ENV, None)
        child_environment.pop(_CAPTURE_JOB_ROOT_ENV, None)
        child_environment.pop(DEEP_PROFILE_DIRECTORY_ENV, None)
        child_environment.pop(SAMPLE_PROFILE_DIRECTORY_ENV, None)
        child_environment.pop(PROFILE_SNAPSHOT_SOCKET_ENV, None)
        if _annotation_fd is not None:
            child_environment[_ANNOTATIONS_FD_ENV] = str(_annotation_fd)
            child_environment[_ANNOTATIONS_FALLBACK_ENV] = str(resolved_annotation_path)
            child_environment[_ANNOTATIONS_IDENTITY_ENV] = (
                f"{annotation_status.st_dev}:{annotation_status.st_ino}"
            )
        if deep_profile_session is not None:
            deep_profile_session.configure_environment(child_environment)
        metadata: dict[str, JsonValue] = _initial_metadata(
            environment_names, environment=child_environment
        )
        if configuration.requested_level is not None:
            capture_metadata = metadata.get("capture")
            assert isinstance(capture_metadata, dict)
            metadata = {
                **metadata,
                "capture": {
                    **capture_metadata,
                    "level": configuration.requested_level,
                },
            }
        runpack_writer = RunpackWriter(temporary)
        temporary_created = True
        with runpack_writer as writer:
            revision = _git_revision(working_directory)
            started_at_ns = time.time_ns()
            started_monotonic_ns = time.perf_counter_ns()
            writer.add_execution(
                Execution(
                    id=execution_id,
                    name=name,
                    started_at_ns=started_at_ns,
                    finished_at_ns=None,
                    command=command,
                    working_directory=str(working_directory),
                    exit_code=None,
                    revision=revision,
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
                process: subprocess.Popen[bytes] = subprocess.Popen(
                    command,
                    cwd=working_directory,
                    env=child_environment,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=True,
                    pass_fds=() if _annotation_fd is None else (_annotation_fd,),
                    text=False,
                )
            except (OSError, ValueError) as exc:
                raise CaptureError(f"could not start command {command[0]!r}: {exc}") from exc

            if _annotation_fd is not None:
                try:
                    _restore_annotation_fd(_annotation_fd)
                    annotation_fd_installed = False
                except OSError as exc:
                    _terminate_and_reap(process)
                    raise CaptureError(
                        f"could not restore reserved annotation transport fd: {exc}"
                    ) from exc

            if observe_process_tree:
                process_observer = ProcessTreeObserver(
                    process_group_id=process.pid,
                    execution_id=execution_id,
                    root_entity_id=entity_id,
                    started_at_ns=started_at_ns,
                    started_monotonic_ns=started_monotonic_ns,
                )
                process_observer.start()

            if process.stdout is None or process.stderr is None:
                _terminate_and_reap(process)
                raise CaptureError("failed to capture process output")
            stdout_pipe = cast(BinaryIO, process.stdout)
            stderr_pipe = cast(BinaryIO, process.stderr)
            process_done = threading.Event()
            cleanup_attempted = False
            try:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    try:
                        stdout_result = executor.submit(
                            _pump, stdout_pipe, stdout, capture_output_limit, process_done
                        )
                        stderr_result = executor.submit(
                            _pump, stderr_pipe, stderr, capture_output_limit, process_done
                        )
                        exit_code, process_usage = _wait_with_usage(
                            process, (stdout_result, stderr_result)
                        )
                        process_done.set()
                        stdout_digest = stdout_result.result()
                        stderr_digest = stderr_result.result()
                    except BaseException:
                        process_done.set()
                        cleanup_attempted = True
                        _terminate_and_reap(process)
                        raise
                    finally:
                        process_done.set()
            except BaseException:
                process_done.set()
                if not cleanup_attempted:
                    _terminate_and_reap(process)
                raise

            elapsed_ns = time.perf_counter_ns() - started_monotonic_ns
            finished_at_ns = started_at_ns + elapsed_ns
            wall_seconds = elapsed_ns / 1_000_000_000
            process_observation: ProcessObservationResult | None = None
            if process_observer is not None:
                process_observation = process_observer.stop()
            metadata = {
                **metadata,
                "output": {
                    "stdout": _output_metadata(stdout_digest),
                    "stderr": _output_metadata(stderr_digest),
                },
            }
            if process_observation is not None:
                process_observation_metadata: dict[str, JsonValue]
                try:
                    writer.add_entities(process_observation.entities)
                    writer.add_measurements(process_observation.measurements)
                    process_observation_metadata = process_observation.as_metadata()
                except RunpackError as exc:
                    process_observation_metadata = {
                        "requested": True,
                        "observer": "posix-process-table",
                        "status": "invalid",
                        "error": terminal_text(exc),
                    }
                capture_metadata = metadata.get("capture")
                assert isinstance(capture_metadata, dict)
                metadata = {
                    **metadata,
                    "capture": {
                        **capture_metadata,
                        "process_observer": process_observation_metadata,
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
                    "capture": {
                        **capture_metadata,
                        "annotation_error": terminal_text(exc),
                    },
                }
            profile_checkpoint: dict[str, JsonValue] | None = None
            if deep_profile_session is not None:
                try:
                    profile_checkpoint = profile_session_checkpoint(deep_profile_session)
                except DeepProfileError as exc:
                    metadata = _metadata_with_instrumentation(
                        metadata,
                        _invalid_instrumentation_metadata(deep_profile_session, exc),
                    )
                    deep_profile_session.close()
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
            if annotation_created and _annotation_directory is None:
                remove_best_effort(annotation_path)
                annotation_created = artifact_exists(annotation_path)
            metadata = _metadata_with_recovery(
                metadata,
                status="pending",
                recovered=False,
                profile_checkpoint=profile_checkpoint,
                root_process_id=process.pid if profile_checkpoint is not None else None,
                entity_id=entity_id if profile_checkpoint is not None else None,
            )
            writer.set_execution_metadata(execution_id, metadata)
            recovery_ready = True
            if deep_profile_session is not None and profile_checkpoint is not None:
                metadata = _assemble_profile_checkpoint(
                    writer,
                    execution_id=execution_id,
                    entity_id=entity_id,
                    root_process_id=process.pid,
                    metadata=metadata,
                    session=deep_profile_session,
                    profile_checkpoint=profile_checkpoint,
                    recovered=False,
                )
            else:
                metadata = _metadata_with_recovery(
                    metadata,
                    status="complete",
                    recovered=False,
                )
                writer.set_execution_metadata(execution_id, metadata)
            if _capture_client_disconnected is not None:
                metadata = _metadata_with_capture_worker(
                    metadata,
                    _capture_client_disconnected,
                )
                writer.set_execution_metadata(execution_id, metadata)
        try:
            publish_without_overwrite(temporary, output)
            published = True
        except FileExistsError as exc:
            raise CaptureError(f"refusing to overwrite existing runpack: {output}") from exc
        except OSError as exc:
            raise CaptureError(f"could not publish runpack {output}: {exc}") from exc
    except BaseException:
        if temporary_created and not recovery_ready:
            remove_best_effort(temporary)
        raise
    finally:
        if annotation_fd_installed:
            assert _annotation_fd is not None
            try:
                _restore_annotation_fd(_annotation_fd)
            except OSError as exc:
                active_error = sys.exception()
                if active_error is None:
                    raise CaptureError(
                        f"could not restore reserved annotation transport fd: {exc}"
                    ) from exc
                active_error.add_note(
                    f"reserved annotation transport fd cleanup also failed: {exc}"
                )
        if annotation_created and _annotation_directory is None:
            remove_best_effort(annotation_path)
        if deep_profile_session is not None and (not recovery_ready or published):
            deep_profile_session.close()
        if process_observer is not None:
            process_observer.stop()
    return exit_code
