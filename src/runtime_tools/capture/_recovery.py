"""Recovery of post-exit capture checkpoints."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.artifacts import artifact_exists, publish_without_overwrite
from runtime_tools.capture._common import _CAPTURE_RECOVERY_VERSION, CaptureError
from runtime_tools.capture._metadata import (
    _invalid_instrumentation_metadata,
    _metadata_with_instrumentation,
    _metadata_with_recovery,
)
from runtime_tools.deep_profile import (
    DeepProfileError,
    DeepProfileSession,
    load_python_profile,
    recover_profile_session,
)
from runtime_tools.model import (
    JsonValue,
)
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter


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
