"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from runtime_tools.deep_profile._common import (
    _MAX_INTEGER,
    FILTERED_CONTROL_FLOW_EXCEPTION_TYPES,
    MAX_DEEP_PROFILE_DIRECTORY_ENTRIES,
    MAX_DEEP_PROFILE_FILE_BYTES,
    MAX_DEEP_PROFILE_FILES,
    MAX_DEEP_PROFILE_TOTAL_BYTES,
    MAX_SEMANTIC_EXECUTABLE_CHARACTERS,
    OBSERVER_INTEGRITY_VERSION,
    PROFILE_CHECKPOINT_INTERVAL_NS,
    PROFILE_FIRST_CHECKPOINT_DELAY_NS,
    PYTHON_EXCEPTION_FILTER_VERSION,
    CallerObservation,
    DeepProfileError,
    SemanticCaptureStatus,
    _bounded_sum,
    _process_role,
)
from runtime_tools.deep_profile._evidence import (
    _SUBPROCESS_CALLER_CONTRACT,
    _CallerEventContract,
    _CallerRecord,
    _FunctionIdentity,
    _SubprocessCaller,
    _SubprocessRecord,
)
from runtime_tools.deep_profile._session import (
    DeepProfileSession,
    _boolean,
    _integer,
    _list,
    _object,
    _optional_boolean,
    _reject_json_constant,
    _text,
)
from runtime_tools.json_support import reject_duplicate_object
from runtime_tools.model import CausalEdge, Event, JsonValue


def _snapshot_kind(document: dict[str, object], label: str) -> Literal["checkpoint", "final"]:
    value = document.get("snapshot_kind", "final")
    if "snapshot_kind" in document:
        interval_ns = _integer(
            document.get("checkpoint_interval_ns"),
            f"{label} checkpoint interval",
        )
        if interval_ns != PROFILE_CHECKPOINT_INTERVAL_NS:
            raise DeepProfileError(f"{label} checkpoint interval is unsupported")
        first_delay_ns = document.get("first_checkpoint_delay_ns")
        if first_delay_ns is not None and (
            _integer(first_delay_ns, f"{label} first checkpoint delay")
            != PROFILE_FIRST_CHECKPOINT_DELAY_NS
        ):
            raise DeepProfileError(f"{label} first checkpoint delay is unsupported")
    if value == "checkpoint":
        return "checkpoint"
    if value == "final":
        return "final"
    raise DeepProfileError(f"{label} snapshot kind is unsupported")


def _registration_only(
    document: dict[str, object],
    label: str,
    snapshot_kind: Literal["checkpoint", "final"],
) -> bool:
    value = _boolean(document.get("registration_only", False), f"{label} registration marker")
    if value and snapshot_kind != "checkpoint":
        raise DeepProfileError(f"{label} final report cannot be registration-only")
    return value


def _profile_files(
    session: DeepProfileSession,
    root_process_id: int | None,
) -> tuple[tuple[Path, ...], int]:
    candidates: list[tuple[Path, int]] = []
    try:
        entries = session.directory.iterdir()
        for entry_count, path in enumerate(entries, 1):
            if entry_count > MAX_DEEP_PROFILE_DIRECTORY_ENTRIES:
                raise DeepProfileError(
                    "generated deep-profile directory exceeds its entry-count limit"
                )
            if not path.name.startswith("profile-") or not path.name.endswith(".json"):
                continue
            status = path.stat(follow_symlinks=False)
            if not stat.S_ISREG(status.st_mode):
                raise DeepProfileError("generated deep-profile evidence must be regular files")
            if status.st_size > MAX_DEEP_PROFILE_FILE_BYTES:
                raise DeepProfileError("generated deep-profile evidence exceeds its per-file limit")
            candidates.append((path, status.st_size))
    except DeepProfileError:
        raise
    except OSError as exc:
        raise DeepProfileError("could not inspect generated deep-profile evidence") from exc
    root_name = f"profile-{root_process_id}.json" if root_process_id is not None else None
    candidates.sort(key=lambda item: (item[0].name != root_name, item[0].name))
    selected = candidates[:MAX_DEEP_PROFILE_FILES]
    if sum(size for _, size in selected) > MAX_DEEP_PROFILE_TOTAL_BYTES:
        raise DeepProfileError("generated deep-profile evidence exceeds its aggregate limit")
    return tuple(path for path, _ in selected), max(0, len(candidates) - len(selected))


def _read_profile_file(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode):
            raise DeepProfileError("generated deep-profile evidence must be regular files")
        if status.st_size > MAX_DEEP_PROFILE_FILE_BYTES:
            raise DeepProfileError("generated deep-profile evidence exceeds its per-file limit")
        chunks: list[bytes] = []
        byte_count = 0
        while True:
            chunk = os.read(
                descriptor, min(64 * 1024, MAX_DEEP_PROFILE_FILE_BYTES + 1 - byte_count)
            )
            if not chunk:
                break
            chunks.append(chunk)
            byte_count += len(chunk)
            if byte_count > MAX_DEEP_PROFILE_FILE_BYTES:
                raise DeepProfileError("generated deep-profile evidence exceeds its per-file limit")
        return b"".join(chunks)
    except DeepProfileError:
        raise
    except OSError as exc:
        raise DeepProfileError("could not read generated deep-profile evidence") from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _parse_document(raw: bytes) -> dict[str, object]:
    try:
        value = json.loads(
            raw,
            parse_constant=_reject_json_constant,
            object_pairs_hook=reject_duplicate_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise DeepProfileError("generated deep-profile evidence is invalid JSON") from exc
    return _object(value, "deep-profile document")


def _observer_integrity(document: dict[str, object]) -> tuple[int, int] | None:
    raw_integrity = document.get("observer_integrity")
    if raw_integrity is None:
        return None
    integrity = _object(raw_integrity, "deep-profile observer integrity")
    if (
        _integer(
            integrity.get("format_version"),
            "deep-profile observer integrity format version",
        )
        != OBSERVER_INTEGRITY_VERSION
    ):
        raise DeepProfileError("deep-profile observer integrity format version is unsupported")
    return (
        _integer(
            integrity.get("profile_hook_setter_call_count"),
            "deep-profile profile-hook setter call count",
        ),
        _integer(
            integrity.get("trace_hook_setter_call_count"),
            "deep-profile trace-hook setter call count",
        ),
    )


def _python_exception_filter(document: dict[str, object]) -> bool:
    raw_filter = document.get("python_exception_filter")
    if raw_filter is None:
        return False
    exception_filter = _object(raw_filter, "deep-profile Python exception filter")
    if (
        _integer(
            exception_filter.get("format_version"),
            "deep-profile Python exception filter format version",
        )
        != PYTHON_EXCEPTION_FILTER_VERSION
    ):
        raise DeepProfileError("deep-profile Python exception filter format version is unsupported")
    filtered_types = _list(
        exception_filter.get("filtered_exception_types"),
        "deep-profile filtered exception types",
    )
    if (
        exception_filter.get("event_semantics") != "exact_type_identity"
        or exception_filter.get("exception_type_identity_inspected") is not True
        or exception_filter.get("exception_types_captured") is not False
        or tuple(filtered_types) != FILTERED_CONTROL_FLOW_EXCEPTION_TYPES
    ):
        raise DeepProfileError("deep-profile Python exception filter metadata is invalid")
    return True


def _profile_payloads(
    session: DeepProfileSession,
    root_process_id: int | None,
) -> tuple[tuple[bytes, ...], int]:
    files, dropped_file_count = _profile_files(session, root_process_id)
    payloads: list[bytes] = []
    total_bytes = 0
    for path in files:
        payload = _read_profile_file(path)
        total_bytes += len(payload)
        if total_bytes > MAX_DEEP_PROFILE_TOTAL_BYTES:
            raise DeepProfileError("generated deep-profile evidence exceeds its aggregate limit")
        payloads.append(payload)
    return tuple(payloads), dropped_file_count


def _optional_signed_integer(value: object, label: str) -> int | None:
    if value is None:
        return None
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not -_MAX_INTEGER <= value <= _MAX_INTEGER
    ):
        raise DeepProfileError(f"{label} must be a bounded integer or null")
    return value


def _optional_nonnegative_integer(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _integer(value, label)


def _subprocess_caller(value: object) -> _SubprocessCaller:
    item = _object(value, "semantic subprocess caller")
    scope = _text(item.get("scope"), "semantic subprocess caller scope")
    if scope not in {"application", "library", "runtime"}:
        raise DeepProfileError("semantic subprocess caller scope is unsupported")
    raw_observation = item.get("observation")
    observation: CallerObservation
    if raw_observation == "exact":
        observation = "exact"
    elif raw_observation == "sampled":
        observation = "sampled"
    else:
        raise DeepProfileError("semantic subprocess caller observation is unsupported")
    return _SubprocessCaller(
        _FunctionIdentity(
            _text(item.get("module"), "semantic subprocess caller module"),
            _text(item.get("qualname"), "semantic subprocess caller qualified name"),
            _text(item.get("filename"), "semantic subprocess caller filename"),
            _integer(item.get("firstlineno"), "semantic subprocess caller first line"),
            scope,
        ),
        observation,
    )


def _subprocess_record(value: object, *, document_pid: int) -> _SubprocessRecord:
    item = _object(value, "semantic subprocess record")
    identifier = _integer(item.get("id"), "semantic subprocess id")
    name = _text(item.get("name"), "semantic subprocess executable")
    if len(name) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic subprocess executable exceeds its character limit")
    parent_pid = _integer(item.get("parent_pid"), "semantic subprocess parent process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic subprocess parent process id is inconsistent")
    child_pid = _optional_nonnegative_integer(
        item.get("child_pid"),
        "semantic subprocess child process id",
    )
    if child_pid is not None and child_pid <= 0:
        raise DeepProfileError("semantic subprocess child process id must be positive")
    shell = _optional_boolean(item.get("shell"), "semantic subprocess shell marker")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic subprocess start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic subprocess start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic subprocess duration",
    )
    exit_code = _optional_signed_integer(
        item.get("exit_code"),
        "semantic subprocess exit code",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["exited", "launch_error", "unknown"]
    if raw_outcome == "exited":
        outcome = "exited"
    elif raw_outcome == "launch_error":
        outcome = "launch_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic subprocess outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic subprocess launch error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic subprocess launch error type exceeds its character limit")
    if outcome == "exited" and (
        child_pid is None or duration_ns is None or exit_code is None or error_type is not None
    ):
        raise DeepProfileError("exited semantic subprocess evidence is incomplete")
    if outcome == "launch_error" and (
        child_pid is not None or duration_ns is None or exit_code is not None or error_type is None
    ):
        raise DeepProfileError("failed semantic subprocess launch evidence is inconsistent")
    if outcome == "unknown" and (
        child_pid is None
        or duration_ns is not None
        or exit_code is not None
        or error_type is not None
    ):
        raise DeepProfileError("unfinished semantic subprocess evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic subprocess finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _SubprocessRecord(
        identifier,
        name,
        parent_pid,
        child_pid,
        shell,
        started_at_ns,
        duration_ns,
        exit_code,
        outcome,
        error_type,
        caller,
        caller_status,
    )


def _semantic_event(
    record: _SubprocessRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-subprocess-wrapper",
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "shell": record.shell,
        "outcome": record.outcome,
        "arguments_captured": False,
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _caller_event_id(
            _SUBPROCESS_CALLER_CONTRACT, record.caller.identity
        )
    if record.child_pid is not None:
        attributes["child_pid"] = record.child_pid
    if record.exit_code is not None:
        attributes["exit_code"] = record.exit_code
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    elif record.exit_code is not None and record.exit_code != 0:
        attributes["error"] = True
        attributes["error.type"] = "subprocess_exit"
    return Event(
        id=f"semantic:subprocess:{record.parent_pid}:{record.identifier}",
        kind="subprocess.run",
        name=record.name,
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _caller_event_id(contract: _CallerEventContract, identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"{contract.namespace}:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _caller_event(
    contract: _CallerEventContract,
    identity: _FunctionIdentity,
    *,
    count: int,
    entity_id: str,
) -> Event:
    return Event(
        id=_caller_event_id(contract, identity),
        kind="python.callsite",
        name=identity.name,
        entity_id=entity_id,
        started_at_ns=None,
        finished_at_ns=None,
        clock_domain=None,
        uncertainty_ns=None,
        sequence=None,
        attributes={
            "source": contract.source,
            "scope": identity.scope,
            "module": identity.module,
            "qualname": identity.qualname,
            "filename": identity.filename,
            "firstlineno": identity.firstlineno,
            contract.count_attribute: count,
            "arguments_captured": False,
            "locals_captured": False,
        },
    )


def _caller_evidence[Record: _CallerRecord](
    records: tuple[Record, ...],
    *,
    contract: _CallerEventContract,
    entity_id: str,
    target_event_id: Callable[[Record], str],
    relation_for: Callable[[Record], str] | None = None,
) -> tuple[tuple[Event, ...], tuple[CausalEdge, ...], int, int]:
    caller_counts: dict[_FunctionIdentity, int] = {}
    edges: list[CausalEdge] = []
    for record in records:
        caller = record.caller
        if caller is None:
            continue
        identity = caller.identity
        caller_counts[identity] = caller_counts.get(identity, 0) + 1
        edges.append(
            CausalEdge(
                _caller_event_id(contract, identity),
                target_event_id(record),
                contract.relation if relation_for is None else relation_for(record),
                1.0 if caller.observation == "exact" else 0.9,
                {"source": contract.source, "observation": caller.observation},
            )
        )
    caller_events = tuple(
        _caller_event(contract, identity, count=count, entity_id=entity_id)
        for identity, count in sorted(
            caller_counts.items(),
            key=lambda item: (
                item[0].name,
                item[0].filename,
                item[0].firstlineno,
                item[0].scope,
            ),
        )
    )
    attributed_count = len(edges)
    return caller_events, tuple(edges), attributed_count, len(records) - attributed_count


def _retained_capture_status(
    *, partial: bool, dropped_count: int, callback_error_count: int
) -> SemanticCaptureStatus:
    if partial:
        return "partial"
    if dropped_count or callback_error_count:
        return "truncated"
    return "complete"


def _caller_capture_status(
    *,
    invalid_count: int,
    partial: bool,
    attributed_count: int,
    unattributed_count: int,
    callback_error_count: int,
    dropped_count: int,
) -> SemanticCaptureStatus:
    if invalid_count:
        return "invalid"
    if partial or unattributed_count or callback_error_count:
        return "partial"
    if dropped_count:
        return "truncated"
    return "complete"
