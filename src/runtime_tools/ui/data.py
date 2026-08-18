"""Build browser-ready facts from one or two runpacks."""

from __future__ import annotations

import json
from pathlib import Path

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.inspect import inspect_runpack
from runtime_tools.model import Event, JsonValue, Measurement
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.storage import RunpackReader

MAX_TIMELINE_EVENTS = 50_000
MAX_TIMELINE_ENTITIES = 50_000
MAX_TIMELINE_EDGES = 200_000
MAX_TIMELINE_MEASUREMENTS = 200_000
MAX_TIMELINE_JSON_BYTES = 64 * 1024 * 1024
MAX_TIMELINE_TEXT_BYTES = 64 * 1024 * 1024
MAX_TIMELINE_PAYLOAD_BYTES = 128 * 1024 * 1024


class TimelineError(ValueError):
    """Raised when a run cannot be represented safely in the local UI."""


def _event_value(event: Event, execution_start_ns: int) -> dict[str, JsonValue]:
    started_at_ns = event.started_at_ns
    finished_at_ns = event.finished_at_ns
    return {
        "id": event.id,
        "entity_id": event.entity_id,
        "kind": event.kind,
        "name": event.name,
        "clock_domain": event.clock_domain,
        "uncertainty_ns": event.uncertainty_ns,
        "start_offset_ns": (
            started_at_ns - execution_start_ns if started_at_ns is not None else None
        ),
        "duration_ns": (
            finished_at_ns - started_at_ns
            if started_at_ns is not None and finished_at_ns is not None
            else None
        ),
        "attributes": event.attributes,
    }


def _measurement_value(measurement: Measurement, execution_start_ns: int) -> dict[str, JsonValue]:
    timestamp_ns = measurement.timestamp_ns
    return {
        "name": measurement.name,
        "value": measurement.value,
        "unit": measurement.unit,
        "entity_id": measurement.entity_id,
        "timestamp_offset_ns": (
            timestamp_ns - execution_start_ns if timestamp_ns is not None else None
        ),
        "attributes": measurement.attributes,
    }


def _timeline_duration_ns(
    events: tuple[Event, ...], execution_start_ns: int, execution_finish_ns: int | None
) -> int:
    known_ends = []
    for event in events:
        timestamp = (
            event.finished_at_ns if event.finished_at_ns is not None else event.started_at_ns
        )
        if timestamp is not None:
            known_ends.append(timestamp - execution_start_ns)
    if execution_finish_ns is not None:
        known_ends.append(execution_finish_ns - execution_start_ns)
    return max(1, *known_ends)


def _run_value(
    path: Path,
    event_limit: int,
    entity_limit: int,
    edge_limit: int,
    measurement_limit: int,
    json_byte_limit: int,
    text_byte_limit: int,
) -> dict[str, JsonValue]:
    with RunpackReader(path) as reader:
        counts = reader.counts()
        event_count = counts["events"]
        if event_count > event_limit:
            raise TimelineError(
                f"timeline has {event_count:,} events; local UI limit is {event_limit:,}"
            )
        entity_count = counts["entities"]
        if entity_count > entity_limit:
            raise TimelineError(
                f"timeline has {entity_count:,} entities; local UI limit is {entity_limit:,}"
            )
        edge_count = counts["causal_edges"]
        if edge_count > edge_limit:
            raise TimelineError(
                f"timeline has {edge_count:,} edges; local UI limit is {edge_limit:,}"
            )
        measurement_count = counts["measurements"]
        if measurement_count > measurement_limit:
            raise TimelineError(
                f"timeline has {measurement_count:,} measurements; "
                f"local UI limit is {measurement_limit:,}"
            )
        normalized_json_bytes = reader.normalized_json_bytes()
        if normalized_json_bytes > json_byte_limit:
            raise TimelineError(
                f"timeline has {normalized_json_bytes:,} normalized JSON bytes; "
                f"local UI limit is {json_byte_limit:,}"
            )
        normalized_text_bytes = reader.normalized_text_bytes()
        if normalized_text_bytes > text_byte_limit:
            raise TimelineError(
                f"timeline has {normalized_text_bytes:,} normalized text bytes; "
                f"local UI limit is {text_byte_limit:,}"
            )
    summary = inspect_runpack(path)
    with RunpackReader(path) as reader:
        entities = reader.entities()
        events = reader.events()
        edges = reader.causal_edges()
        measurements = reader.measurements()
        clock_inconsistencies = reader.clock_inconsistency_count()
    analysis = analyze_runpack(path)
    return {
        "summary": summary.as_json_value(),
        "timeline_duration_ns": _timeline_duration_ns(
            events, summary.started_at_ns, summary.finished_at_ns
        ),
        "entities": [
            {
                "id": entity.id,
                "kind": entity.kind,
                "name": entity.name,
                "parent_entity_id": entity.parent_entity_id,
                "attributes": entity.attributes,
            }
            for entity in entities
        ],
        "events": [_event_value(event, summary.started_at_ns) for event in events],
        "measurements": [
            _measurement_value(measurement, summary.started_at_ns) for measurement in measurements
        ],
        "edges": [
            {
                "source_event_id": edge.source_event_id,
                "target_event_id": edge.target_event_id,
                "kind": edge.kind,
                "confidence": edge.confidence,
            }
            for edge in edges
        ],
        "clock_inconsistencies": clock_inconsistencies,
        "analysis": analysis.as_json_value(),
    }


def build_timeline_payload(
    baseline: Path,
    candidate: Path | None = None,
    *,
    event_limit: int = MAX_TIMELINE_EVENTS,
    entity_limit: int = MAX_TIMELINE_ENTITIES,
    edge_limit: int = MAX_TIMELINE_EDGES,
    measurement_limit: int = MAX_TIMELINE_MEASUREMENTS,
    json_byte_limit: int = MAX_TIMELINE_JSON_BYTES,
    text_byte_limit: int = MAX_TIMELINE_TEXT_BYTES,
    payload_byte_limit: int = MAX_TIMELINE_PAYLOAD_BYTES,
) -> dict[str, JsonValue]:
    if (
        event_limit <= 0
        or entity_limit <= 0
        or edge_limit <= 0
        or measurement_limit <= 0
        or json_byte_limit <= 0
        or text_byte_limit <= 0
        or payload_byte_limit <= 0
    ):
        raise TimelineError("timeline limits must be positive")
    runs: list[JsonValue] = [
        _run_value(
            baseline,
            event_limit,
            entity_limit,
            edge_limit,
            measurement_limit,
            json_byte_limit,
            text_byte_limit,
        )
    ]
    comparison: JsonValue = None
    if candidate is not None:
        runs.append(
            _run_value(
                candidate,
                event_limit,
                entity_limit,
                edge_limit,
                measurement_limit,
                json_byte_limit,
                text_byte_limit,
            )
        )
        comparison = compare_runpacks(baseline, candidate).as_json_value()
    payload: dict[str, JsonValue] = {"runs": runs, "comparison": comparison}
    encoded_size = len(
        json.dumps(
            payload,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    if encoded_size > payload_byte_limit:
        raise TimelineError(
            f"timeline payload is {encoded_size:,} bytes; local UI limit is {payload_byte_limit:,}"
        )
    return payload
