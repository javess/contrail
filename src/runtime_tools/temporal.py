"""Normalize bounded Temporal protojson history into an existing execution."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Never, cast

from runtime_tools.enrichment import EnrichmentError, enrich_copy, validate_enrichment_destination
from runtime_tools.json_support import reject_duplicate_object
from runtime_tools.model import CausalEdge, Event, JsonValue
from runtime_tools.storage import RunpackReader, RunpackWriter


class TemporalHistoryImportError(EnrichmentError):
    """Raised when Temporal history evidence is malformed or ambiguous."""


@dataclass(frozen=True, slots=True)
class TemporalHistoryImportResult:
    activity_count: int
    queue_wait_count: int
    fallback_execution_count: int
    event_count: int
    edge_count: int
    correlation_count: int


@dataclass(frozen=True, slots=True)
class _HistoryEvent:
    id: int
    timestamp_ns: int
    type: str
    attributes: dict[str, object]


@dataclass(frozen=True, slots=True)
class _Activity:
    activity_id: str
    activity_type: str
    scheduled: _HistoryEvent
    started: _HistoryEvent | None
    terminal: _HistoryEvent | None
    attempt: int | None
    outcome: str | None


@dataclass(frozen=True, slots=True)
class _History:
    workflow_id: str | None
    workflow_type: str
    started: _HistoryEvent
    terminal: _HistoryEvent | None
    outcome: str | None
    activities: tuple[_Activity, ...]


_RFC3339_TIMESTAMP = re.compile(
    r"^(?P<whole>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?(?P<zone>Z|[+-]\d{2}:\d{2})$"
)
_RFC3339_WITHOUT_ZONE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MAX_RUNPACK_INTEGER = (1 << 63) - 1
_MIN_RUNPACK_INTEGER = -(1 << 63)
_WORKFLOW_STARTED = "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED"
_ACTIVITY_SCHEDULED = "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
_ACTIVITY_STARTED = "EVENT_TYPE_ACTIVITY_TASK_STARTED"
_ACTIVITY_TERMINALS = {
    "EVENT_TYPE_ACTIVITY_TASK_COMPLETED": (
        "activityTaskCompletedEventAttributes",
        "completed",
    ),
    "EVENT_TYPE_ACTIVITY_TASK_FAILED": ("activityTaskFailedEventAttributes", "failed"),
    "EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT": (
        "activityTaskTimedOutEventAttributes",
        "timed_out",
    ),
    "EVENT_TYPE_ACTIVITY_TASK_CANCELED": (
        "activityTaskCanceledEventAttributes",
        "canceled",
    ),
}
_WORKFLOW_TERMINALS = {
    "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED": "completed",
    "EVENT_TYPE_WORKFLOW_EXECUTION_FAILED": "failed",
    "EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT": "timed_out",
    "EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED": "canceled",
    "EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED": "terminated",
    "EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW": "continued_as_new",
}
MAX_TEMPORAL_HISTORY_BYTES = 64 * 1024 * 1024
MAX_TEMPORAL_HISTORY_EVENTS = 200_000


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TemporalHistoryImportError(f"{label} must be an object")
    return cast(dict[str, object], value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise TemporalHistoryImportError(f"{label} must be a list")
    return cast(list[object], value)


def _string(value: object, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        suffix = "a string" if optional else "a non-empty string"
        raise TemporalHistoryImportError(f"{label} must be {suffix}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise TemporalHistoryImportError(f"{label} must be valid UTF-8") from exc
    return value


def _positive_integer(value: object, label: str) -> int:
    if isinstance(value, str) and value.isascii() and value.isdigit():
        normalized = int(value)
    elif isinstance(value, int) and not isinstance(value, bool):
        normalized = value
    else:
        raise TemporalHistoryImportError(f"{label} must be a positive integer")
    if normalized <= 0:
        raise TemporalHistoryImportError(f"{label} must be a positive integer")
    if normalized > _MAX_RUNPACK_INTEGER:
        raise TemporalHistoryImportError(f"{label} exceeds the runpack integer range")
    return normalized


def _optional_event_reference(value: object, label: str) -> int | None:
    if value is None or value == 0 or value == "0":
        return None
    return _positive_integer(value, label)


def _timestamp(value: object) -> int:
    if not isinstance(value, str):
        raise TemporalHistoryImportError("Temporal eventTime must be an RFC 3339 string")
    match = _RFC3339_TIMESTAMP.fullmatch(value)
    if match is None:
        if _RFC3339_WITHOUT_ZONE.fullmatch(value):
            raise TemporalHistoryImportError(f"Temporal eventTime requires a timezone: {value}")
        raise TemporalHistoryImportError(f"invalid Temporal eventTime: {value}")
    zone = "+00:00" if match.group("zone") == "Z" else match.group("zone")
    try:
        parsed = datetime.fromisoformat(f"{match.group('whole')}{zone}")
    except ValueError as exc:
        raise TemporalHistoryImportError(f"invalid Temporal eventTime: {value}") from exc
    delta = parsed.astimezone(UTC) - _EPOCH
    seconds = delta.days * 86_400 + delta.seconds
    fraction = match.group("fraction") or ""
    timestamp_ns = seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")
    if not _MIN_RUNPACK_INTEGER <= timestamp_ns <= _MAX_RUNPACK_INTEGER:
        raise TemporalHistoryImportError(f"Temporal eventTime exceeds runpack range: {value}")
    return timestamp_ns


def _reject_json_constant(value: str) -> Never:
    raise ValueError(f"non-finite JSON constant: {value}")


def _load(source: Path) -> list[dict[str, object]]:
    try:
        with source.open("rb") as stream:
            raw = stream.read(MAX_TEMPORAL_HISTORY_BYTES + 1)
    except OSError as exc:
        raise TemporalHistoryImportError(f"could not read Temporal history: {source}") from exc
    if len(raw) > MAX_TEMPORAL_HISTORY_BYTES:
        raise TemporalHistoryImportError(
            f"Temporal history exceeds the {MAX_TEMPORAL_HISTORY_BYTES}-byte input limit"
        )
    try:
        document = json.loads(
            raw.decode("utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=reject_duplicate_object,
        )
    except UnicodeDecodeError as exc:
        raise TemporalHistoryImportError("Temporal history must be UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise TemporalHistoryImportError(
            f"invalid Temporal history JSON at line {exc.lineno}, column {exc.colno}"
        ) from exc
    except RecursionError as exc:
        raise TemporalHistoryImportError("Temporal history JSON nesting is too deep") from exc
    except ValueError as exc:
        raise TemporalHistoryImportError(f"invalid Temporal history JSON: {exc}") from exc
    events = _list(_object(document, "Temporal history").get("events"), "Temporal events")
    if len(events) > MAX_TEMPORAL_HISTORY_EVENTS:
        raise TemporalHistoryImportError(
            f"Temporal history exceeds the {MAX_TEMPORAL_HISTORY_EVENTS}-event input limit"
        )
    return [_object(event, "Temporal event") for event in events]


def _event_attributes(event: dict[str, object], key: str, event_id: int) -> dict[str, object]:
    return _object(event.get(key, {}), f"Temporal event {event_id} {key}")


def _normalize_events(raw_events: list[dict[str, object]]) -> tuple[_HistoryEvent, ...]:
    if not raw_events:
        raise TemporalHistoryImportError("Temporal history must contain at least one event")
    events: list[_HistoryEvent] = []
    previous_id = 0
    previous_timestamp: int | None = None
    for raw_event in raw_events:
        event_id = _positive_integer(raw_event.get("eventId"), "Temporal eventId")
        if event_id <= previous_id:
            raise TemporalHistoryImportError("Temporal eventId values must be strictly increasing")
        timestamp = _timestamp(raw_event.get("eventTime"))
        if previous_timestamp is not None and timestamp < previous_timestamp:
            raise TemporalHistoryImportError("Temporal eventTime values must be non-decreasing")
        event_type = _string(raw_event.get("eventType"), f"Temporal event {event_id} eventType")
        assert event_type is not None
        attribute_key: str | None = None
        if event_type == _WORKFLOW_STARTED:
            attribute_key = "workflowExecutionStartedEventAttributes"
        elif event_type == _ACTIVITY_SCHEDULED:
            attribute_key = "activityTaskScheduledEventAttributes"
        elif event_type == _ACTIVITY_STARTED:
            attribute_key = "activityTaskStartedEventAttributes"
        elif event_type in _ACTIVITY_TERMINALS:
            attribute_key = _ACTIVITY_TERMINALS[event_type][0]
        attributes = _event_attributes(raw_event, attribute_key, event_id) if attribute_key else {}
        events.append(_HistoryEvent(event_id, timestamp, event_type, attributes))
        previous_id = event_id
        previous_timestamp = timestamp
    return tuple(events)


def _history(raw_events: list[dict[str, object]]) -> _History:
    events = _normalize_events(raw_events)
    if events[0].type != _WORKFLOW_STARTED:
        raise TemporalHistoryImportError(
            "Temporal history must start with WorkflowExecutionStarted"
        )
    if sum(event.type == _WORKFLOW_STARTED for event in events) != 1:
        raise TemporalHistoryImportError("Temporal history must contain one workflow start event")
    workflow_attributes = events[0].attributes
    workflow_type = _string(
        _object(workflow_attributes.get("workflowType"), "Temporal workflowType").get("name"),
        "Temporal workflowType.name",
    )
    workflow_id = _string(
        workflow_attributes.get("workflowId"), "Temporal workflowId", optional=True
    )
    assert workflow_type is not None
    workflow_terminals = [event for event in events if event.type in _WORKFLOW_TERMINALS]
    if len(workflow_terminals) > 1:
        raise TemporalHistoryImportError("Temporal history contains multiple workflow terminals")
    workflow_terminal = workflow_terminals[0] if workflow_terminals else None
    if workflow_terminal is not None and workflow_terminal != events[-1]:
        raise TemporalHistoryImportError(
            "Temporal workflow terminal must be the last history event"
        )

    scheduled: dict[int, tuple[str, str, _HistoryEvent]] = {}
    started: dict[int, tuple[_HistoryEvent, int]] = {}
    terminals: dict[int, tuple[_HistoryEvent, str]] = {}
    event_by_id = {event.id: event for event in events}
    for event in events:
        if event.type == _ACTIVITY_SCHEDULED:
            activity_id = _string(
                event.attributes.get("activityId"),
                f"Temporal activity scheduled event {event.id} activityId",
            )
            activity_type = _string(
                _object(
                    event.attributes.get("activityType"),
                    f"Temporal activity scheduled event {event.id} activityType",
                ).get("name"),
                f"Temporal activity scheduled event {event.id} activityType.name",
            )
            assert activity_id is not None and activity_type is not None
            scheduled[event.id] = (activity_id, activity_type, event)
            continue
        if event.type == _ACTIVITY_STARTED:
            scheduled_id = _positive_integer(
                event.attributes.get("scheduledEventId"),
                f"Temporal activity started event {event.id} scheduledEventId",
            )
            if scheduled_id not in scheduled:
                raise TemporalHistoryImportError(
                    f"activity started event {event.id} references unknown scheduled event "
                    f"{scheduled_id}"
                )
            if scheduled_id in started:
                raise TemporalHistoryImportError(
                    f"activity scheduled event {scheduled_id} has multiple started events"
                )
            attempt = _positive_integer(
                event.attributes.get("attempt"),
                f"Temporal activity started event {event.id} attempt",
            )
            started[scheduled_id] = (event, attempt)
            continue
        if event.type not in _ACTIVITY_TERMINALS:
            continue
        scheduled_id = _positive_integer(
            event.attributes.get("scheduledEventId"),
            f"Temporal activity terminal event {event.id} scheduledEventId",
        )
        if scheduled_id not in scheduled:
            raise TemporalHistoryImportError(
                f"activity terminal event {event.id} references unknown scheduled event "
                f"{scheduled_id}"
            )
        if scheduled_id in terminals:
            raise TemporalHistoryImportError(
                f"activity scheduled event {scheduled_id} has multiple terminal events"
            )
        started_id = _optional_event_reference(
            event.attributes.get("startedEventId"),
            f"Temporal activity terminal event {event.id} startedEventId",
        )
        known_started = started.get(scheduled_id)
        if started_id is not None:
            referenced = event_by_id.get(started_id)
            if referenced is None or referenced.type != _ACTIVITY_STARTED:
                raise TemporalHistoryImportError(
                    f"activity terminal event {event.id} references unknown started event "
                    f"{started_id}"
                )
            if known_started is None or known_started[0].id != started_id:
                raise TemporalHistoryImportError(
                    f"activity terminal event {event.id} startedEventId does not match "
                    f"scheduled event {scheduled_id}"
                )
        elif known_started is not None and _ACTIVITY_TERMINALS[event.type][1] != "timed_out":
            raise TemporalHistoryImportError(
                f"activity terminal event {event.id} is missing startedEventId"
            )
        terminals[scheduled_id] = (event, _ACTIVITY_TERMINALS[event.type][1])

    activities = []
    for scheduled_id, (activity_id, activity_type, scheduled_event) in sorted(scheduled.items()):
        started_value = started.get(scheduled_id)
        terminal_value = terminals.get(scheduled_id)
        activities.append(
            _Activity(
                activity_id,
                activity_type,
                scheduled_event,
                started_value[0] if started_value else None,
                terminal_value[0] if terminal_value else None,
                started_value[1] if started_value else None,
                terminal_value[1] if terminal_value else None,
            )
        )
    return _History(
        workflow_id,
        workflow_type,
        events[0],
        workflow_terminal,
        _WORKFLOW_TERMINALS[workflow_terminal.type] if workflow_terminal else None,
        tuple(activities),
    )


def _activity_attributes(activity: _Activity) -> dict[str, JsonValue]:
    attributes: dict[str, JsonValue] = {
        "adapter": "temporal-history",
        "temporal.activity_id": activity.activity_id,
        "temporal.activity_type": activity.activity_type,
        "temporal.scheduled_event_id": activity.scheduled.id,
        "temporal.state": activity.outcome
        or ("started" if activity.started is not None else "scheduled"),
    }
    if activity.started is not None:
        attributes["temporal.started_event_id"] = activity.started.id
    if activity.attempt is not None:
        attributes["temporal.attempt"] = activity.attempt
    if activity.terminal is not None:
        attributes["temporal.terminal_event_id"] = activity.terminal.id
    if activity.outcome is not None:
        attributes["temporal.outcome"] = activity.outcome
    return attributes


def _otel_activity_match(
    activity: _Activity,
    workflow_id: str | None,
    existing_events: tuple[Event, ...],
) -> Event | None:
    expected_name = f"RunActivity:{activity.activity_type}"
    candidates = [
        event
        for event in existing_events
        if event.kind == "server.request"
        and event.name == expected_name
        and event.attributes.get("temporalActivityID") == activity.activity_id
        and (workflow_id is None or event.attributes.get("temporalWorkflowID") == workflow_id)
    ]
    return candidates[0] if len(candidates) == 1 else None


def _normalize(
    history: _History,
    existing_events: tuple[Event, ...],
) -> tuple[tuple[Event, ...], tuple[CausalEdge, ...], int, int]:
    workflow_event_id = f"temporal:workflow:{history.started.id}"
    workflow_attributes: dict[str, JsonValue] = {
        "adapter": "temporal-history",
        "temporal.workflow_type": history.workflow_type,
        "temporal.started_event_id": history.started.id,
        "temporal.state": history.outcome or "running",
    }
    if history.workflow_id is not None:
        workflow_attributes["temporal.workflow_id"] = history.workflow_id
    if history.terminal is not None:
        workflow_attributes["temporal.terminal_event_id"] = history.terminal.id
    if history.outcome is not None:
        workflow_attributes["temporal.outcome"] = history.outcome
    normalized_events = [
        Event(
            workflow_event_id,
            "run",
            history.workflow_type,
            None,
            history.started.timestamp_ns,
            history.terminal.timestamp_ns if history.terminal else None,
            "temporal.history",
            None,
            history.started.id,
            workflow_attributes,
        )
    ]
    edges: list[CausalEdge] = []
    correlation_count = 0
    fallback_count = 0
    existing_ids = {event.id for event in existing_events}
    activity_key_counts: dict[tuple[str, str], int] = {}
    for activity in history.activities:
        key = activity.activity_id, activity.activity_type
        activity_key_counts[key] = activity_key_counts.get(key, 0) + 1
    for activity in history.activities:
        activity_event_id = f"temporal:activity:{activity.scheduled.id}"
        queue_event_id = f"{activity_event_id}:queue"
        activity_attributes = _activity_attributes(activity)
        normalized_events.append(
            Event(
                activity_event_id,
                "temporal.activity",
                activity.activity_type,
                None,
                activity.scheduled.timestamp_ns,
                activity.terminal.timestamp_ns if activity.terminal else None,
                "temporal.history",
                None,
                activity.scheduled.id,
                activity_attributes,
            )
        )
        queue_finish = (
            activity.started.timestamp_ns
            if activity.started is not None
            else activity.terminal.timestamp_ns
            if activity.terminal is not None
            else None
        )
        queue_attributes = dict(activity_attributes)
        queue_attributes["temporal.queue_outcome"] = (
            "started" if activity.started is not None else activity.outcome or "waiting"
        )
        normalized_events.append(
            Event(
                queue_event_id,
                "queue.wait",
                f"{activity.activity_type} queue",
                None,
                activity.scheduled.timestamp_ns,
                queue_finish,
                "temporal.history",
                None,
                activity.scheduled.id,
                queue_attributes,
            )
        )
        edges.extend(
            (
                CausalEdge(
                    workflow_event_id,
                    activity_event_id,
                    "parent",
                    1.0,
                    {"source": "temporal-history"},
                ),
                CausalEdge(
                    activity_event_id,
                    queue_event_id,
                    "parent",
                    1.0,
                    {"source": "temporal-history"},
                ),
            )
        )
        execution_event = (
            _otel_activity_match(activity, history.workflow_id, existing_events)
            if activity_key_counts[(activity.activity_id, activity.activity_type)] == 1
            else None
        )
        if execution_event is not None:
            correlation_count += 1
        elif activity.started is not None and activity.terminal is not None:
            fallback_count += 1
            execution_event = Event(
                f"{activity_event_id}:execution",
                "operation",
                f"RunActivity:{activity.activity_type}",
                None,
                activity.started.timestamp_ns,
                activity.terminal.timestamp_ns,
                "temporal.history",
                None,
                activity.started.id,
                activity_attributes,
            )
            normalized_events.append(execution_event)
        if execution_event is not None:
            edges.append(
                CausalEdge(
                    queue_event_id,
                    execution_event.id,
                    "dispatches",
                    1.0,
                    {"source": "temporal-history"},
                )
            )
    duplicate = next((event.id for event in normalized_events if event.id in existing_ids), None)
    if duplicate is not None:
        raise TemporalHistoryImportError(f"Temporal history event already exists: {duplicate}")
    return tuple(normalized_events), tuple(edges), correlation_count, fallback_count


def import_temporal_history(
    runpack: Path,
    source: Path,
    output: Path,
) -> TemporalHistoryImportResult:
    """Add workflow and activity lifecycle facts from a Temporal History protojson object."""
    validate_enrichment_destination(output)
    history = _history(_load(source))

    def append(writer: RunpackWriter) -> TemporalHistoryImportResult:
        with RunpackReader(writer.path) as reader:
            events, edges, correlations, fallbacks = _normalize(history, reader.events())
        timestamps = [
            timestamp
            for event in events
            for timestamp in (event.started_at_ns, event.finished_at_ns)
            if timestamp is not None
        ]
        if timestamps:
            writer.expand_execution_bounds(min(timestamps), max(timestamps))
        writer.add_event_graph(events, edges)
        return TemporalHistoryImportResult(
            activity_count=len(history.activities),
            queue_wait_count=len(history.activities),
            fallback_execution_count=fallbacks,
            event_count=len(events),
            edge_count=len(edges),
            correlation_count=correlations,
        )

    return enrich_copy(runpack, output, append)
