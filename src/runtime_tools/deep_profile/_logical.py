"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import heapq
from typing import Literal

from runtime_tools.deep_profile._common import (
    MAX_SEMANTIC_EXECUTABLE_CHARACTERS,
    MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS,
    MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS,
    SEMANTIC_LOGICAL_OPERATION_ADAPTER_CATEGORIES,
    SEMANTIC_LOGICAL_OPERATION_ADAPTERS,
    DeepProfileError,
    LogicalOperationCategory,
    LogicalOperationName,
    _bounded_sum,
    _process_role,
)
from runtime_tools.deep_profile._evidence import (
    _LOGICAL_OPERATION_CALLER_CONTRACT,
    _BoundaryEvidence,
    _LogicalOperationCaptureEvidence,
    _LogicalOperationRecord,
    _SubprocessCaller,
)
from runtime_tools.deep_profile._http import _http_capture_evidence
from runtime_tools.deep_profile._network import (
    _network_capture_evidence,
    _network_setup_capture_evidence,
)
from runtime_tools.deep_profile._parsing import (
    _caller_capture_status,
    _caller_event_id,
    _caller_evidence,
    _optional_nonnegative_integer,
    _parse_document,
    _retained_capture_status,
    _subprocess_caller,
)
from runtime_tools.deep_profile._session import _boolean, _integer, _list, _object, _text
from runtime_tools.deep_profile._subprocess import _semantic_capture_evidence
from runtime_tools.model import Event, JsonValue


def _logical_operation_record(
    value: object,
    *,
    document_pid: int,
) -> _LogicalOperationRecord:
    item = _object(value, "semantic logical operation record")
    identifier = _integer(item.get("id"), "semantic logical operation id")
    raw_category = item.get("category")
    categories: dict[str, LogicalOperationCategory] = {
        "broker": "broker",
        "cache": "cache",
        "database": "database",
        "executor": "executor",
        "queue": "queue",
        "scheduler": "scheduler",
        "server": "server",
    }
    if type(raw_category) is not str or raw_category not in categories:
        raise DeepProfileError("semantic logical operation category is unsupported")
    category = categories[raw_category]
    raw_operation = item.get("operation")
    operations: dict[str, LogicalOperationName] = {
        "batch": "batch",
        "command": "command",
        "commit": "commit",
        "consume": "consume",
        "execute": "execute",
        "executemany": "executemany",
        "executescript": "executescript",
        "get": "get",
        "publish": "publish",
        "put": "put",
        "rollback": "rollback",
        "request": "request",
        "task": "task",
    }
    if type(raw_operation) is not str or raw_operation not in operations:
        raise DeepProfileError("semantic logical operation is unsupported")
    operation = operations[raw_operation]
    operations_by_category = {
        "broker": {"consume", "publish"},
        "cache": {"batch", "command"},
        "database": {"commit", "execute", "executemany", "executescript", "rollback"},
        "executor": {"task"},
        "queue": {"get", "put"},
        "scheduler": {"task"},
        "server": {"request"},
    }
    if operation not in operations_by_category[category]:
        raise DeepProfileError("semantic logical operation and category are inconsistent")
    adapter = _text(item.get("adapter"), "semantic logical operation adapter")
    if adapter not in SEMANTIC_LOGICAL_OPERATION_ADAPTERS:
        raise DeepProfileError("semantic logical operation adapter is unsupported")
    if SEMANTIC_LOGICAL_OPERATION_ADAPTER_CATEGORIES.get(adapter) != category:
        raise DeepProfileError("semantic logical operation adapter and category are inconsistent")
    parent_pid = _integer(item.get("parent_pid"), "semantic logical operation process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic logical operation process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic logical operation start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic logical operation start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic logical operation duration",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["completed", "operation_error", "unknown"]
    if raw_outcome == "completed":
        outcome = "completed"
    elif raw_outcome == "operation_error":
        outcome = "operation_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic logical operation outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic logical operation error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic logical operation error type exceeds its character limit")
    if outcome == "completed" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("successful semantic logical operation evidence is inconsistent")
    if outcome == "operation_error" and (duration_ns is None or error_type is None):
        raise DeepProfileError("failed semantic logical operation evidence is inconsistent")
    if outcome == "unknown" and (duration_ns is not None or error_type is not None):
        raise DeepProfileError("unfinished semantic logical operation evidence is inconsistent")
    raw_status_code = item.get("status_code")
    status_code = (
        None
        if raw_status_code is None
        else _integer(raw_status_code, "semantic logical operation HTTP status")
    )
    if status_code is not None and not 100 <= status_code <= 999:
        raise DeepProfileError("semantic logical operation HTTP status is invalid")
    if category != "server" and status_code is not None:
        raise DeepProfileError("non-server logical operation has an HTTP status")
    if category == "server" and status_code is not None:
        if status_code >= 500 and (outcome != "operation_error" or error_type != "HTTPStatusError"):
            raise DeepProfileError("failed server logical operation evidence is inconsistent")
        if status_code < 500 and outcome != "completed":
            raise DeepProfileError("successful server logical operation evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic logical operation finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _LogicalOperationRecord(
        identifier,
        category,
        operation,
        adapter,
        parent_pid,
        started_at_ns,
        duration_ns,
        outcome,
        error_type,
        status_code,
        caller,
        caller_status,
    )


def _logical_operation_event_id(record: _LogicalOperationRecord) -> str:
    return f"semantic:logical-operation:{record.parent_pid}:{record.identifier}"


def _logical_operation_name(record: _LogicalOperationRecord) -> str:
    if record.category == "scheduler":
        return "Async task"
    if record.category == "server":
        return "Inbound HTTP request"
    names = {
        "batch": "Cache batch",
        "command": "Cache command",
        "commit": "Database commit",
        "consume": "Broker consume",
        "execute": "Database execute",
        "executemany": "Database executemany",
        "executescript": "Database script",
        "get": "Queue get",
        "publish": "Broker publish",
        "put": "Queue put",
        "rollback": "Database rollback",
        "task": "Executor task",
    }
    return names[record.operation]


def _logical_operation_event(
    record: _LogicalOperationRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-logical-operation-wrapper",
        "category": record.category,
        "operation": record.operation,
        "adapter": record.adapter,
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "statement_captured": False,
        "parameters_captured": False,
        "payload_captured": False,
        "queue_item_captured": False,
        "queue_identity_captured": False,
        "callable_captured": False,
        "awaitable_captured": False,
        "task_name_captured": False,
        "context_captured": False,
        "arguments_captured": False,
        "return_value_captured": False,
        "http_method_captured": False,
        "route_captured": False,
        "url_captured": False,
        "headers_captured": False,
        "body_captured": False,
        "response_body_captured": False,
        "client_address_captured": False,
        "duration_boundary": (
            "submission_to_completion"
            if record.category == "executor"
            else "creation_to_completion"
            if record.category == "scheduler"
            else "request_to_response_completion"
            if record.category == "server"
            else "logical_operation"
        ),
    }
    if record.status_code is not None:
        attributes["status_code"] = record.status_code
    if record.caller is not None:
        attributes["caller_event_id"] = _caller_event_id(
            _LOGICAL_OPERATION_CALLER_CONTRACT, record.caller.identity
        )
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_logical_operation_event_id(record),
        kind=f"{record.category}.{record.operation}",
        name=_logical_operation_name(record),
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


def _logical_operation_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _LogicalOperationCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _LogicalOperationRecord]] = []
    process_count = 0
    dropped_operation_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_operation_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if "logical_operation_capture_enabled" not in semantic:
                saw_missing = True
                continue
            enabled = _boolean(
                semantic.get("logical_operation_capture_enabled"),
                "semantic logical operation enabled marker",
            )
            raw_records = _list(
                semantic.get("logical_operations"),
                "semantic logical operations",
            )
            declared_count = _integer(
                semantic.get("logical_operation_count"),
                "semantic logical operation count",
            )
            document_dropped_count = _integer(
                semantic.get("dropped_logical_operation_count"),
                "semantic dropped logical operation count",
            )
            document_callback_errors = _integer(
                semantic.get("logical_operation_callback_error_count"),
                "semantic logical operation callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("logical_operation_caller_callback_error_count"),
                "semantic logical operation caller callback error count",
            )
            raw_adapters = _list(
                semantic.get("logical_operation_adapters"),
                "semantic logical operation adapters",
            )
            if not enabled:
                if (
                    raw_records
                    or declared_count
                    or document_dropped_count
                    or document_callback_errors
                    or document_caller_callback_errors
                    or raw_adapters
                ):
                    raise DeepProfileError(
                        "disabled semantic logical operation capture contains evidence"
                    )
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic logical operation capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_logical_operations"),
                    "semantic logical operation limit",
                )
                != MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic logical operation limit is unsupported")
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic logical operation adapter")
                if (
                    adapter not in SEMANTIC_LOGICAL_OPERATION_ADAPTERS
                    or adapter in document_adapters
                ):
                    raise DeepProfileError("semantic logical operation adapters are unsupported")
                document_adapters.add(adapter)
            pid = _integer(document.get("pid"), "semantic logical operation process id")
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic logical operation count is inconsistent")
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
                raise DeepProfileError("semantic logical operation registration contains evidence")
            records = tuple(
                _logical_operation_record(raw_record, document_pid=pid)
                for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic logical operation adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError(
                        "semantic logical operation ids must be unique per process"
                    )
                identifiers.add(record.identifier)
            process_count += 1
            dropped_operation_count = _bounded_sum(
                dropped_operation_count,
                document_dropped_count,
                "semantic dropped logical operation count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic logical operation callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic logical operation caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_operation_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = (
                    saw_incomplete
                    or record.outcome == "unknown"
                    or (record.category == "server" and record.status_code is None)
                )
                rank = (
                    int(record.outcome != "completed"),
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _LogicalOperationCaptureEvidence(
            (), (), (), "invalid", process_count, 0, 0, "invalid", 0, 0, 0, 0, ()
        )
    if process_count == 0:
        return _LogicalOperationCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_operation_count = _bounded_sum(
        dropped_operation_count,
        valid_operation_count - len(selected_records),
        "semantic dropped logical operation count",
    )
    status = _retained_capture_status(
        partial=saw_missing or saw_checkpoint or saw_incomplete,
        dropped_count=dropped_operation_count,
        callback_error_count=callback_error_count,
    )
    (
        caller_events,
        caller_edges,
        attributed_operation_count,
        unattributed_operation_count,
    ) = _caller_evidence(
        selected_records,
        contract=_LOGICAL_OPERATION_CALLER_CONTRACT,
        entity_id=entity_id,
        target_event_id=_logical_operation_event_id,
    )
    caller_status = _caller_capture_status(
        invalid_count=invalid_caller_count,
        partial=saw_missing or saw_checkpoint,
        attributed_count=attributed_operation_count,
        unattributed_count=unattributed_operation_count,
        callback_error_count=caller_callback_error_count,
        dropped_count=dropped_operation_count,
    )
    return _LogicalOperationCaptureEvidence(
        tuple(
            _logical_operation_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_operation_count,
        callback_error_count,
        caller_status,
        attributed_operation_count,
        unattributed_operation_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )


def _boundary_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _BoundaryEvidence:
    return _BoundaryEvidence(
        _semantic_capture_evidence(payloads, entity_id=entity_id, root_process_id=root_process_id),
        _http_capture_evidence(payloads, entity_id=entity_id, root_process_id=root_process_id),
        _network_capture_evidence(payloads, entity_id=entity_id, root_process_id=root_process_id),
        _network_setup_capture_evidence(
            payloads, entity_id=entity_id, root_process_id=root_process_id
        ),
        _logical_operation_capture_evidence(
            payloads, entity_id=entity_id, root_process_id=root_process_id
        ),
    )
