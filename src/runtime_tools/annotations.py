"""Normalize capture-local annotation records into core events and edges."""

from __future__ import annotations

import json
from pathlib import Path

from runtime_tools.model import CausalEdge, Event, JsonValue


class AnnotationError(ValueError):
    """Raised when a captured annotation stream is malformed."""


def _object(value: object, label: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise AnnotationError(f"{label} must be an object")
    return value


def _string(record: dict[str, JsonValue], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise AnnotationError(f"annotation {key} must be a non-empty string")
    return value


def _timestamp(record: dict[str, JsonValue]) -> int:
    value = record.get("timestamp_ns")
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise AnnotationError("annotation timestamp_ns must be a non-negative integer")
    return value


def load_annotations(
    path: Path, *, entity_id: str
) -> tuple[tuple[Event, ...], tuple[CausalEdge, ...]]:
    if not path.exists():
        return (), ()
    starts: dict[str, dict[str, JsonValue]] = {}
    ends: dict[str, dict[str, JsonValue]] = {}
    instants: list[dict[str, JsonValue]] = []
    links: list[dict[str, JsonValue]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise AnnotationError("could not read captured annotations") from exc
    for line_number, line in enumerate(lines, 1):
        try:
            record = _object(json.loads(line), "annotation")
        except json.JSONDecodeError as exc:
            raise AnnotationError(f"invalid annotation JSON on line {line_number}") from exc
        record_kind = record.get("record")
        if record_kind == "event_start":
            starts[_string(record, "id")] = record
        elif record_kind == "event_end":
            ends[_string(record, "id")] = record
        elif record_kind == "event_instant":
            instants.append(record)
        elif record_kind == "link":
            links.append(record)
        else:
            raise AnnotationError(f"unknown annotation record on line {line_number}")

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
        events.append(
            Event(
                event_id,
                _string(start, "kind"),
                _string(start, "name"),
                entity_id,
                _timestamp(start),
                _timestamp(end) if end is not None else None,
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
        if source_id in known_ids and target_id in known_ids:
            edges.append(CausalEdge(source_id, target_id, _string(record, "relation"), 1.0, {}))
    events.sort(key=lambda item: (item.started_at_ns or -1, item.id))
    return tuple(events), tuple(edges)


def _append_parent_edge(
    edges: list[CausalEdge],
    record: dict[str, JsonValue],
    event_id: str,
    known_ids: set[str],
) -> None:
    parent_id = record.get("parent_id")
    if isinstance(parent_id, str) and parent_id in known_ids:
        edges.append(CausalEdge(parent_id, event_id, "parent", 1.0, {"source": "annotation"}))
