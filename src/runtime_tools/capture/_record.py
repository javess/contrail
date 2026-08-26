"""Orchestrate one local process capture."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import BinaryIO, cast

from runtime_tools.annotations import AnnotationError, load_annotations
from runtime_tools.artifacts import artifact_exists, publish_without_overwrite, remove_best_effort
from runtime_tools.capture._common import (
    _ANNOTATIONS_FALLBACK_ENV,
    _ANNOTATIONS_FD_ENV,
    _ANNOTATIONS_IDENTITY_ENV,
    _CAPTURE_JOB_ID_ENV,
    _CAPTURE_JOB_ROOT_ENV,
    _CAPTURE_WORKER_CLIENT_FD_ENV,
    _CAPTURE_WORKER_DETACHED_ENV,
    _CAPTURE_WORKER_STDERR_FD_ENV,
    _CAPTURE_WORKER_STDOUT_FD_ENV,
    MAX_CAPTURE_OUTPUT_BYTES,
    CaptureError,
    resolve_capture_configuration,
)
from runtime_tools.capture._metadata import (
    _initial_metadata,
    _invalid_instrumentation_metadata,
    _metadata_with_capture_worker,
    _metadata_with_instrumentation,
    _metadata_with_recovery,
    _restore_annotation_fd,
    _validate_annotation_fd,
)
from runtime_tools.capture._process import (
    _git_revision,
    _identified_environment_names,
    _output_metadata,
    _peak_memory_bytes,
    _pump,
    _terminate_and_reap,
    _wait_with_usage,
)
from runtime_tools.capture._recovery import _assemble_profile_checkpoint
from runtime_tools.deep_profile import (
    DEEP_PROFILE_DIRECTORY_ENV,
    PROFILE_SNAPSHOT_SOCKET_ENV,
    SAMPLE_PROFILE_DIRECTORY_ENV,
    DeepProfileError,
    DeepProfileSession,
    prepare_deep_profile_session,
    prepare_sample_profile_session,
    profile_session_checkpoint,
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
from runtime_tools.storage import RunpackError, RunpackWriter
from runtime_tools.terminal import terminal_text


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
