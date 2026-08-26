"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import heapq

from runtime_tools.deep_profile._common import (
    MAX_SEMANTIC_SUBPROCESS_EVENTS,
    MAX_SEMANTIC_SUBPROCESS_PER_PROCESS,
    DeepProfileError,
    _bounded_sum,
)
from runtime_tools.deep_profile._evidence import (
    _SUBPROCESS_CALLER_CONTRACT,
    _SemanticCaptureEvidence,
    _SubprocessRecord,
)
from runtime_tools.deep_profile._parsing import (
    _caller_capture_status,
    _caller_evidence,
    _parse_document,
    _retained_capture_status,
    _semantic_event,
    _subprocess_record,
)
from runtime_tools.deep_profile._session import _boolean, _integer, _list, _object


def _semantic_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _SemanticCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _SubprocessRecord]] = []
    semantic_process_count = 0
    dropped_subprocess_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_subprocess_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_unknown = False
    saw_unknown_identity = False
    invalid = False
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if semantic.get("format_version") != 2:
                raise DeepProfileError("semantic capture format version is unsupported")
            if semantic.get("observer") != "python-runtime-boundary-wrapper":
                raise DeepProfileError("semantic capture observer is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_subprocesses"),
                    "semantic capture subprocess limit",
                )
                != MAX_SEMANTIC_SUBPROCESS_PER_PROCESS
            ):
                raise DeepProfileError("semantic capture subprocess limit is unsupported")
            pid = _integer(document.get("pid"), "semantic capture process id")
            raw_records = _list(semantic.get("subprocesses"), "semantic subprocesses")
            subprocess_count = _integer(
                semantic.get("subprocess_count"),
                "semantic capture subprocess count",
            )
            if (
                subprocess_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_SUBPROCESS_PER_PROCESS
            ):
                raise DeepProfileError("semantic capture subprocess count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_subprocess_count"),
                "semantic capture dropped subprocess count",
            )
            document_callback_errors = _integer(
                semantic.get("callback_error_count"),
                "semantic capture callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("caller_callback_error_count"),
                "semantic caller callback error count",
            )
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic capture registration contains evidence")
            records = tuple(
                _subprocess_record(raw_record, document_pid=pid) for raw_record in raw_records
            )
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic subprocess ids must be unique per process")
                identifiers.add(record.identifier)
            semantic_process_count += 1
            dropped_subprocess_count = _bounded_sum(
                dropped_subprocess_count,
                document_dropped_count,
                "semantic capture dropped subprocess count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic capture callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_subprocess_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_unknown = saw_unknown or record.outcome == "unknown"
                saw_unknown_identity = (
                    saw_unknown_identity
                    or record.shell is None
                    or record.name
                    in {
                        "<command>",
                        "<unknown>",
                    }
                )
                important = int(
                    record.outcome != "exited"
                    or (record.exit_code is not None and record.exit_code != 0)
                )
                rank = (
                    important,
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_SUBPROCESS_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _SemanticCaptureEvidence(
            (),
            (),
            (),
            "invalid",
            semantic_process_count,
            0,
            0,
            "invalid",
            0,
            0,
            invalid_caller_count,
            caller_callback_error_count,
        )
    if semantic_process_count == 0:
        return _SemanticCaptureEvidence(
            (),
            (),
            (),
            "unavailable",
            0,
            0,
            0,
            "unavailable",
            0,
            0,
            0,
            0,
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    selection_dropped = valid_subprocess_count - len(selected_records)
    dropped_subprocess_count = _bounded_sum(
        dropped_subprocess_count,
        selection_dropped,
        "semantic capture dropped subprocess count",
    )
    partial = saw_missing or saw_checkpoint or saw_unknown or saw_unknown_identity
    status = _retained_capture_status(
        partial=partial,
        dropped_count=dropped_subprocess_count,
        callback_error_count=callback_error_count,
    )
    (
        caller_events,
        caller_edges,
        attributed_subprocess_count,
        unattributed_subprocess_count,
    ) = _caller_evidence(
        selected_records,
        contract=_SUBPROCESS_CALLER_CONTRACT,
        entity_id=entity_id,
        target_event_id=lambda record: (
            f"semantic:subprocess:{record.parent_pid}:{record.identifier}"
        ),
    )
    caller_attribution_status = _caller_capture_status(
        invalid_count=invalid_caller_count,
        partial=saw_missing or saw_checkpoint,
        attributed_count=attributed_subprocess_count,
        unattributed_count=unattributed_subprocess_count,
        callback_error_count=caller_callback_error_count,
        dropped_count=dropped_subprocess_count,
    )
    return _SemanticCaptureEvidence(
        tuple(
            _semantic_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        semantic_process_count,
        dropped_subprocess_count,
        callback_error_count,
        caller_attribution_status,
        attributed_subprocess_count,
        unattributed_subprocess_count,
        invalid_caller_count,
        caller_callback_error_count,
    )
