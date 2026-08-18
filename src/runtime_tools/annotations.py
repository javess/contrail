"""Normalize capture-local annotation records into core events and edges."""

from __future__ import annotations

import fcntl
import json
import math
import time
from pathlib import Path
from typing import Never

from runtime_tools.json_support import reject_duplicate_object
from runtime_tools.model import CausalEdge, Event, JsonValue


class AnnotationError(ValueError):
    """Raised when a captured annotation stream is malformed."""


_MAX_TIMESTAMP_NS = (1 << 63) - 1
MAX_ANNOTATION_STREAM_BYTES = 64 * 1024 * 1024
MAX_ANNOTATION_RECORDS = 200_000
ANNOTATION_LOCK_TIMEOUT_SECONDS = 1.0
_RECORD_FIELDS = {
    "event_start": {"record", "id", "kind", "name", "timestamp_ns", "parent_id", "attributes"},
    "event_end": {"record", "id", "timestamp_ns", "error"},
    "event_instant": {
        "record",
        "id",
        "kind",
        "name",
        "timestamp_ns",
        "parent_id",
        "attributes",
    },
    "link": {"record", "source_id", "target_id", "relation"},
}


def _utf8_string(value: str, label: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise AnnotationError(f"{label} contains a string that is not valid UTF-8") from exc
    return value


def _json_value(value: object, label: str) -> JsonValue:
    if isinstance(value, str):
        return _utf8_string(value, label)
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AnnotationError(f"{label} contains a non-finite number")
        return value
    if isinstance(value, list):
        return [_json_value(item, label) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {_utf8_string(key, label): _json_value(item, label) for key, item in value.items()}
    raise AnnotationError(f"{label} contains an invalid JSON value")


def _object(value: object, label: str) -> dict[str, JsonValue]:
    normalized = _json_value(value, label)
    if not isinstance(normalized, dict):
        raise AnnotationError(f"{label} must be an object")
    return normalized


def _reject_json_constant(value: str) -> Never:
    raise ValueError(f"non-finite constant {value}")


def _string(record: dict[str, JsonValue], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise AnnotationError(f"annotation {key} must be a non-empty string")
    return value


def _timestamp(record: dict[str, JsonValue]) -> int:
    value = record.get("timestamp_ns")
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise AnnotationError("annotation timestamp_ns must be a non-negative integer")
    if value > _MAX_TIMESTAMP_NS:
        raise AnnotationError("annotation timestamp_ns exceeds the runpack integer range")
    return value


def _flock(descriptor: int, operation: int) -> None:
    while True:
        try:
            fcntl.flock(descriptor, operation)
            return
        except InterruptedError:
            continue


def _lock_for_read(descriptor: int) -> None:
    deadline = time.monotonic() + ANNOTATION_LOCK_TIMEOUT_SECONDS
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
            return
        except InterruptedError:
            continue
        except BlockingIOError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AnnotationError("timed out waiting for captured annotations") from None
            time.sleep(min(0.01, remaining))


def load_annotations(
    path: Path, *, entity_id: str
) -> tuple[tuple[Event, ...], tuple[CausalEdge, ...]]:
    if not path.exists():
        return (), ()
    starts: dict[str, dict[str, JsonValue]] = {}
    ends: dict[str, dict[str, JsonValue]] = {}
    instants: list[dict[str, JsonValue]] = []
    links: list[dict[str, JsonValue]] = []
    event_ids: set[str] = set()
    end_lines: dict[str, int] = {}
    try:
        with path.open("rb") as stream:
            _lock_for_read(stream.fileno())
            try:
                raw = stream.read(MAX_ANNOTATION_STREAM_BYTES + 1)
            finally:
                _flock(stream.fileno(), fcntl.LOCK_UN)
    except OSError as exc:
        raise AnnotationError("could not read captured annotations") from exc
    if len(raw) > MAX_ANNOTATION_STREAM_BYTES:
        raise AnnotationError(
            f"captured annotations exceed the {MAX_ANNOTATION_STREAM_BYTES}-byte input limit"
        )
    record_count = raw.count(b"\n") + int(bool(raw) and not raw.endswith(b"\n"))
    if record_count > MAX_ANNOTATION_RECORDS:
        raise AnnotationError(
            f"captured annotations exceed the {MAX_ANNOTATION_RECORDS}-record input limit"
        )
    try:
        lines = raw.decode("utf-8").split("\n")
    except UnicodeDecodeError as exc:
        raise AnnotationError("captured annotations must be UTF-8") from exc
    if lines and not lines[-1]:
        lines.pop()
    for line_number, line in enumerate(lines, 1):
        try:
            record = _object(
                json.loads(
                    line,
                    parse_constant=_reject_json_constant,
                    object_pairs_hook=reject_duplicate_object,
                ),
                "annotation",
            )
        except json.JSONDecodeError as exc:
            raise AnnotationError(f"invalid annotation JSON on line {line_number}") from exc
        except (AnnotationError, RecursionError, ValueError) as exc:
            raise AnnotationError(f"invalid annotation JSON on line {line_number}: {exc}") from exc
        record_kind = record.get("record")
        if not isinstance(record_kind, str) or record_kind not in _RECORD_FIELDS:
            raise AnnotationError(f"unknown annotation record on line {line_number}")
        unknown_fields = sorted(record.keys() - _RECORD_FIELDS[record_kind])
        if unknown_fields:
            raise AnnotationError(
                f"annotation {record_kind} on line {line_number} contains unsupported fields: "
                f"{', '.join(unknown_fields)}"
            )
        if record_kind == "event_start":
            event_id = _string(record, "id")
            if event_id in event_ids:
                raise AnnotationError(f"duplicate annotation event id on line {line_number}")
            event_ids.add(event_id)
            starts[event_id] = record
        elif record_kind == "event_end":
            event_id = _string(record, "id")
            error = record.get("error")
            if error is not None and not isinstance(error, bool):
                raise AnnotationError(
                    f"annotation event_end error must be a boolean on line {line_number}"
                )
            if event_id in ends:
                raise AnnotationError(f"duplicate annotation event end on line {line_number}")
            ends[event_id] = record
            end_lines[event_id] = line_number
        elif record_kind == "event_instant":
            event_id = _string(record, "id")
            if event_id in event_ids:
                raise AnnotationError(f"duplicate annotation event id on line {line_number}")
            event_ids.add(event_id)
            instants.append(record)
        elif record_kind == "link":
            links.append(record)

    orphaned_ends = ends.keys() - starts.keys()
    if orphaned_ends:
        event_id = min(orphaned_ends, key=end_lines.__getitem__)
        raise AnnotationError(f"orphan annotation event end on line {end_lines[event_id]}")

    events: list[Event] = []
    edges: list[CausalEdge] = []
    known_ids = set(starts)
    for record in instants:
        known_ids.add(_string(record, "id"))
    for event_id, start in starts.items():
        end = ends.get(event_id)
        attributes = _object(start.get("attributes", {}), "annotation attributes")
        if end is not None and end.get("error") is True:
            attributes = {**attributes, "error": True}
        started_at_ns = _timestamp(start)
        finished_at_ns = _timestamp(end) if end is not None else None
        if finished_at_ns is not None and finished_at_ns < started_at_ns:
            raise AnnotationError(f"annotation event {event_id} ends before it starts")
        events.append(
            Event(
                event_id,
                _string(start, "kind"),
                _string(start, "name"),
                entity_id,
                started_at_ns,
                finished_at_ns,
                "host.wall",
                None,
                None,
                attributes,
            )
        )
        _append_parent_edge(edges, start, event_id, known_ids)
    for instant in instants:
        event_id = _string(instant, "id")
        timestamp_ns = _timestamp(instant)
        events.append(
            Event(
                event_id,
                _string(instant, "kind"),
                _string(instant, "name"),
                entity_id,
                timestamp_ns,
                timestamp_ns,
                "host.wall",
                None,
                None,
                _object(instant.get("attributes", {}), "annotation attributes"),
            )
        )
        _append_parent_edge(edges, instant, event_id, known_ids)
    for record in links:
        source_id = _string(record, "source_id")
        target_id = _string(record, "target_id")
        relation = _string(record, "relation")
        if source_id not in known_ids or target_id not in known_ids:
            raise AnnotationError(f"annotation link {source_id} -> {target_id} is unresolved")
        if source_id == target_id:
            raise AnnotationError(f"annotation event cannot link to itself: {source_id}")
        edges.append(CausalEdge(source_id, target_id, relation, 1.0, {}))
    edge_identities: set[tuple[str, str, str]] = set()
    unique_edges: list[CausalEdge] = []
    for edge in edges:
        identity = (edge.source_event_id, edge.target_event_id, edge.kind)
        if identity in edge_identities:
            continue
        edge_identities.add(identity)
        unique_edges.append(edge)
    _validate_parent_hierarchy(unique_edges)
    events.sort(
        key=lambda item: (
            -1 if item.started_at_ns is None else item.started_at_ns,
            item.id,
        )
    )
    return tuple(events), tuple(unique_edges)


def _append_parent_edge(
    edges: list[CausalEdge],
    record: dict[str, JsonValue],
    event_id: str,
    known_ids: set[str],
) -> None:
    parent_id = record.get("parent_id")
    if parent_id is None:
        return
    if not isinstance(parent_id, str) or not parent_id:
        raise AnnotationError("annotation parent_id must be a non-empty string or null")
    if parent_id not in known_ids:
        raise AnnotationError(f"annotation parent event is unresolved: {parent_id}")
    edges.append(CausalEdge(parent_id, event_id, "parent", 1.0, {"source": "annotation"}))


def _validate_parent_hierarchy(edges: list[CausalEdge]) -> None:
    parent_by_child: dict[str, str] = {}
    for edge in edges:
        if edge.kind != "parent":
            continue
        parent = parent_by_child.get(edge.target_event_id)
        if parent is not None and parent != edge.source_event_id:
            raise AnnotationError("annotation event cannot have multiple parents")
        parent_by_child[edge.target_event_id] = edge.source_event_id
    complete: set[str] = set()
    for child in parent_by_child:
        trail: set[str] = set()
        current = child
        while current in parent_by_child and current not in complete:
            if current in trail:
                raise AnnotationError("annotation parent relationships contain a cycle")
            trail.add(current)
            current = parent_by_child[current]
        complete.update(trail)
