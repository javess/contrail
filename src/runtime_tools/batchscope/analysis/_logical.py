"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from typing import Literal

from runtime_tools.batchscope.analysis._boundary_models import (
    LogicalOperation,
    LogicalOperationCaptureSummary,
    LogicalOperationHotspot,
    _LogicalOperationHotspotAggregate,
)
from runtime_tools.batchscope.analysis._common import (
    LOGICAL_OPERATION_CAPTURE_ADAPTER_CATEGORIES,
    LOGICAL_OPERATION_CAPTURE_ADAPTERS,
    LOGICAL_OPERATION_MIN_RUN_RATIO,
    LOGICAL_OPERATION_MIN_SECONDS,
    LogicalOperationCategory,
    LogicalOperationName,
)
from runtime_tools.batchscope.analysis._network import _LOGICAL_OPERATION_IDENTITIES
from runtime_tools.batchscope.analysis._profile_models import Bottleneck
from runtime_tools.batchscope.analysis._profile_summary import (
    _caller_attribution_counts,
    _capture_status,
    _semantic_count,
)
from runtime_tools.batchscope.analysis._subprocess import (
    _LOGICAL_OPERATION_CALLER_CONTRACT,
    _operation_callers,
)
from runtime_tools.model import CausalEdge, Event, JsonValue


def _logical_operations(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[LogicalOperation, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_operation, invalid_caller_count = _operation_callers(
        events, edges, _LOGICAL_OPERATION_CALLER_CONTRACT
    )
    operations: list[LogicalOperation] = []
    invalid_event_count = 0
    for event in events:
        if event.attributes.get("source") != "python-logical-operation-wrapper" or event.kind == (
            "python.callsite"
        ):
            continue
        raw_category = event.attributes.get("category")
        raw_operation = event.attributes.get("operation")
        adapter = event.attributes.get("adapter")
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        raw_outcome = event.attributes.get("outcome")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        status_code = event.attributes.get("status_code")
        expected_kind = (
            f"{raw_category}.{raw_operation}"
            if isinstance(raw_category, str) and isinstance(raw_operation, str)
            else None
        )
        expected_name = (
            _LOGICAL_OPERATION_IDENTITIES.get((raw_category, raw_operation))
            if isinstance(raw_category, str) and isinstance(raw_operation, str)
            else None
        )
        if (
            event.started_at_ns is None
            or expected_kind != event.kind
            or expected_name != event.name
            or not isinstance(adapter, str)
            or adapter not in LOGICAL_OPERATION_CAPTURE_ADAPTERS
            or LOGICAL_OPERATION_CAPTURE_ADAPTER_CATEGORIES.get(adapter) != raw_category
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or raw_outcome not in {"completed", "operation_error", "unknown"}
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
            or event.attributes.get("statement_captured") is not False
            or event.attributes.get("parameters_captured") is not False
            or event.attributes.get("payload_captured") is not False
            or event.attributes.get("queue_item_captured") is not False
            or event.attributes.get("queue_identity_captured") is not False
            or event.attributes.get("return_value_captured") is not False
            or event.attributes.get("duration_boundary")
            != (
                "submission_to_completion"
                if raw_category == "executor"
                else "creation_to_completion"
                if raw_category == "scheduler"
                else "request_to_response_completion"
                if raw_category == "server"
                else "logical_operation"
            )
            or (
                raw_category == "executor"
                and (
                    event.attributes.get("callable_captured") is not False
                    or event.attributes.get("arguments_captured") is not False
                )
            )
            or (
                raw_category == "scheduler"
                and (
                    event.attributes.get("callable_captured") is not False
                    or event.attributes.get("awaitable_captured") is not False
                    or event.attributes.get("task_name_captured") is not False
                    or event.attributes.get("context_captured") is not False
                    or event.attributes.get("arguments_captured") is not False
                )
            )
            or (
                raw_category == "server"
                and (
                    event.attributes.get("http_method_captured") is not False
                    or event.attributes.get("route_captured") is not False
                    or event.attributes.get("url_captured") is not False
                    or event.attributes.get("headers_captured") is not False
                    or event.attributes.get("body_captured") is not False
                    or event.attributes.get("response_body_captured") is not False
                    or event.attributes.get("client_address_captured") is not False
                    or (
                        status_code is not None
                        and (
                            not isinstance(status_code, int)
                            or isinstance(status_code, bool)
                            or not 100 <= status_code <= 999
                        )
                    )
                    or (status_code is None and raw_outcome != "completed")
                    or (
                        isinstance(status_code, int)
                        and not isinstance(status_code, bool)
                        and status_code >= 500
                        and (raw_outcome != "operation_error" or error_type != "HTTPStatusError")
                    )
                    or (
                        isinstance(status_code, int)
                        and not isinstance(status_code, bool)
                        and status_code < 500
                        and raw_outcome != "completed"
                    )
                )
            )
            or (raw_category != "server" and status_code is not None)
        ):
            invalid_event_count += 1
            continue
        category_values: dict[str, LogicalOperationCategory] = {
            "broker": "broker",
            "cache": "cache",
            "database": "database",
            "executor": "executor",
            "queue": "queue",
            "scheduler": "scheduler",
            "server": "server",
        }
        if not isinstance(raw_category, str) or raw_category not in category_values:
            invalid_event_count += 1
            continue
        category = category_values[raw_category]
        operation_values: dict[str, LogicalOperationName] = {
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
        if not isinstance(raw_operation, str) or raw_operation not in operation_values:
            invalid_event_count += 1
            continue
        operation = operation_values[raw_operation]
        role: Literal["root", "descendant", "unknown"]
        if raw_role == "root":
            role = "root"
        elif raw_role == "descendant":
            role = "descendant"
        else:
            role = "unknown"
        outcome: Literal["completed", "operation_error", "unknown"]
        if raw_outcome == "completed":
            outcome = "completed"
        elif raw_outcome == "operation_error":
            outcome = "operation_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "completed"
                and (event.finished_at_ns is None or error_type is not None or error is True)
            )
            or (
                outcome == "operation_error"
                and (event.finished_at_ns is None or error_type is None or error is not True)
            )
            or (
                outcome == "unknown"
                and (event.finished_at_ns is not None or error_type is not None or error is True)
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        operations.append(
            LogicalOperation(
                event.id,
                category,
                operation,
                adapter,
                parent_pid,
                role,
                outcome,
                error_type,
                status_code if isinstance(status_code, int) else None,
                event.started_at_ns,
                duration_seconds,
                caller_by_operation.get(event.id),
            )
        )
    operations.sort(
        key=lambda operation: (
            operation.started_at_ns,
            operation.parent_pid,
            operation.event_id,
        )
    )
    return (
        tuple(operations),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_operation.values()}),
        len(caller_by_operation),
        tuple(sorted({operation.adapter for operation in operations})),
    )


def _logical_operation_hotspots(
    operations: tuple[LogicalOperation, ...],
) -> tuple[LogicalOperationHotspot, ...]:
    aggregates: dict[tuple[str | None, str, str, str], _LogicalOperationHotspotAggregate] = {}
    for operation in operations:
        caller_event_id = operation.caller.event_id if operation.caller is not None else None
        key = (caller_event_id, operation.category, operation.operation, operation.adapter)
        aggregate = aggregates.get(key)
        if aggregate is None:
            aggregate = _LogicalOperationHotspotAggregate(
                operation.category,
                operation.operation,
                operation.adapter,
                operation.caller,
            )
            aggregates[key] = aggregate
        aggregate.operation_count += 1
        if operation.outcome == "completed":
            aggregate.completed_operation_count += 1
        elif operation.outcome == "operation_error":
            aggregate.failed_operation_count += 1
        else:
            aggregate.unfinished_operation_count += 1
        if operation.duration_seconds is not None:
            aggregate.total_duration_seconds += operation.duration_seconds
            aggregate.max_duration_seconds = max(
                aggregate.max_duration_seconds,
                operation.duration_seconds,
            )
    hotspots = tuple(
        LogicalOperationHotspot(
            aggregate.category,
            aggregate.operation,
            aggregate.adapter,
            aggregate.operation_count,
            aggregate.completed_operation_count,
            aggregate.failed_operation_count,
            aggregate.unfinished_operation_count,
            aggregate.total_duration_seconds,
            aggregate.max_duration_seconds,
            aggregate.caller,
        )
        for aggregate in aggregates.values()
    )
    return tuple(
        sorted(
            hotspots,
            key=lambda hotspot: (
                -hotspot.failed_operation_count,
                -hotspot.operation_count,
                -hotspot.total_duration_seconds,
                hotspot.category,
                hotspot.operation,
                hotspot.caller.name if hotspot.caller is not None else "",
                hotspot.adapter,
            ),
        )
    )


def _logical_operation_bottlenecks(
    operations: tuple[LogicalOperation, ...],
    capture: LogicalOperationCaptureSummary | None,
    total_seconds: float | None,
) -> tuple[Bottleneck, ...]:
    if (
        capture is None
        or capture.status in {"unavailable", "invalid"}
        or total_seconds is None
        or total_seconds <= 0
    ):
        return ()
    findings: list[Bottleneck] = []
    for category, label in (
        ("database", "Database"),
        ("cache", "Cache"),
        ("queue", "Queue"),
        ("broker", "Broker"),
        ("executor", "Executor"),
        ("scheduler", "Scheduler"),
        ("server", "Server"),
    ):
        matching = tuple(operation for operation in operations if operation.category == category)
        failed_count = sum(operation.outcome == "operation_error" for operation in matching)
        if failed_count:
            findings.append(
                Bottleneck(
                    f"{category}_operation_failures",
                    (
                        f"{failed_count:,} of {len(matching):,} retained "
                        f"{label.lower()} operations failed"
                    ),
                    0.9 if capture.status == "complete" else 0.7,
                )
            )
        completed = tuple(
            operation
            for operation in matching
            if operation.outcome == "completed" and operation.duration_seconds is not None
        )
        if not completed:
            continue
        slowest = max(completed, key=lambda operation: operation.duration_seconds or 0.0)
        duration_seconds = slowest.duration_seconds or 0.0
        ratio = duration_seconds / total_seconds
        if (
            duration_seconds >= LOGICAL_OPERATION_MIN_SECONDS
            and ratio >= LOGICAL_OPERATION_MIN_RUN_RATIO
        ):
            findings.append(
                Bottleneck(
                    f"{category}_operation_latency",
                    (
                        f"{label} {slowest.operation} took {duration_seconds:.3f}s "
                        f"({ratio:.0%} of the run)"
                    ),
                    0.8,
                )
            )
    return tuple(findings)


def _logical_operation_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    operation_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_operation_count: int,
    invalid_caller_count: int,
    observed_adapters: tuple[str, ...],
    hotspot_count: int,
) -> LogicalOperationCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    raw_operation = instrumentation.get("logical_operation_capture")
    if not isinstance(raw_operation, dict):
        return None
    status = _capture_status(raw_operation.get("status"))

    raw_counts = {
        key: _semantic_count(raw_operation.get(key))
        for key in (
            "process_count",
            "operation_count",
            "dropped_operation_count",
            "callback_error_count",
        )
    }
    raw_adapters = raw_operation.get("adapters")
    adapters: tuple[str, ...] = ()
    adapters_invalid = not isinstance(raw_adapters, list)
    if isinstance(raw_adapters, list):
        adapter_values = [adapter for adapter in raw_adapters if isinstance(adapter, str)]
        if len(raw_adapters) > len(LOGICAL_OPERATION_CAPTURE_ADAPTERS) or len(
            adapter_values
        ) != len(raw_adapters):
            adapters_invalid = True
        else:
            adapters = tuple(sorted(adapter_values))
            adapters_invalid = (
                len(adapters) != len(set(adapters))
                or any(adapter not in LOGICAL_OPERATION_CAPTURE_ADAPTERS for adapter in adapters)
                or any(adapter not in adapters for adapter in observed_adapters)
            )
    declared_operation_count = raw_counts["operation_count"]
    if (
        raw_operation.get("observer") != "python-logical-operation-wrapper"
        or raw_operation.get("zero_code") is not True
        or raw_operation.get("deep_only") is not True
        or raw_operation.get("statement_captured") is not False
        or raw_operation.get("parameters_captured") is not False
        or raw_operation.get("payload_captured") is not False
        or raw_operation.get("queue_item_captured") is not False
        or raw_operation.get("queue_identity_captured") is not False
        or raw_operation.get("callable_captured", False) is not False
        or raw_operation.get("awaitable_captured", False) is not False
        or raw_operation.get("task_name_captured", False) is not False
        or raw_operation.get("context_captured", False) is not False
        or raw_operation.get("arguments_captured", False) is not False
        or raw_operation.get("return_value_captured") is not False
        or raw_operation.get("exception_messages_captured", False) is not False
        or raw_operation.get("http_method_captured", False) is not False
        or raw_operation.get("route_captured", False) is not False
        or raw_operation.get("url_captured", False) is not False
        or raw_operation.get("headers_captured", False) is not False
        or raw_operation.get("body_captured", False) is not False
        or raw_operation.get("response_body_captured", False) is not False
        or raw_operation.get("client_address_captured", False) is not False
        or any(value is None for value in raw_counts.values())
        or adapters_invalid
        or declared_operation_count != operation_event_count
        or invalid_event_count
        or (
            status == "unavailable"
            and (
                any((value or 0) != 0 for value in raw_counts.values())
                or bool(adapters)
                or operation_event_count
            )
        )
    ):
        status = "invalid"
    caller = _caller_attribution_counts(
        raw_operation.get("caller_attribution"),
        target_name="operation",
        target_count=operation_event_count,
        caller_count=caller_count,
        attributed_count=attributed_operation_count,
        invalid_count=invalid_caller_count,
    )

    return LogicalOperationCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_operation_count or 0,
        raw_counts["dropped_operation_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller.status,
        caller.caller_count,
        caller.attributed_count,
        caller.unattributed_count,
        caller.invalid_count,
        caller.callback_error_count,
        adapters,
        hotspot_count,
    )
