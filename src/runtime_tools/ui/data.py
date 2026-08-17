"""Build browser-ready facts from one or two runpacks."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.inspect import inspect_runpack
from runtime_tools.model import Event, JsonValue
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.storage import RunpackReader

MAX_TIMELINE_EVENTS = 50_000


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


def _run_value(path: Path) -> dict[str, JsonValue]:
    summary = inspect_runpack(path)
    analysis = analyze_runpack(path)
    with RunpackReader(path) as reader:
        entities = reader.entities()
        events = reader.events()
        edges = reader.causal_edges()
        clock_inconsistencies = reader.clock_inconsistency_count()
    if len(events) > MAX_TIMELINE_EVENTS:
        raise TimelineError(
            f"timeline has {len(events):,} events; local UI limit is {MAX_TIMELINE_EVENTS:,}"
        )
    return {
        "summary": summary.as_json_value(),
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


def build_timeline_payload(baseline: Path, candidate: Path | None = None) -> dict[str, JsonValue]:
    runs: list[JsonValue] = [_run_value(baseline)]
    comparison: JsonValue = None
    if candidate is not None:
        runs.append(_run_value(candidate))
        comparison = compare_runpacks(baseline, candidate).as_json_value()
    return {"runs": runs, "comparison": comparison}
