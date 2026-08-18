"""Build browser-ready facts from one or two runpacks."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path

from runtime_tools.batchscope.analysis import analyze_reader
from runtime_tools.inspect import inspect_reader
from runtime_tools.model import Event, JsonValue, Measurement
from runtime_tools.proofline.contracts import ContractError
from runtime_tools.rundiff.compare import compare_readers
from runtime_tools.storage import RunpackReader, open_runpack_snapshot, resolve_runpack_path
from runtime_tools.ui.proofline import ProoflineDataError, build_proofline_payload

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
        "finish_offset_ns": (
            finished_at_ns - execution_start_ns if finished_at_ns is not None else None
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
    return max(0, *known_ends)


def _run_value(
    reader: RunpackReader,
    event_limit: int,
    entity_limit: int,
    edge_limit: int,
    measurement_limit: int,
    json_byte_limit: int,
    text_byte_limit: int,
) -> dict[str, JsonValue]:
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
        raise TimelineError(f"timeline has {edge_count:,} edges; local UI limit is {edge_limit:,}")
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
    summary = inspect_reader(reader)
    entities = reader.entities()
    events = reader.events()
    edges = reader.causal_edges()
    measurements = reader.measurements()
    clock_inconsistencies = reader.clock_inconsistency_count()
    analysis = analyze_reader(reader, summary)
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
    contract: Path | None = None,
    proofline_report: Path | None = None,
    event_limit: int = MAX_TIMELINE_EVENTS,
    entity_limit: int = MAX_TIMELINE_ENTITIES,
    edge_limit: int = MAX_TIMELINE_EDGES,
    measurement_limit: int = MAX_TIMELINE_MEASUREMENTS,
    json_byte_limit: int = MAX_TIMELINE_JSON_BYTES,
    text_byte_limit: int = MAX_TIMELINE_TEXT_BYTES,
    payload_byte_limit: int = MAX_TIMELINE_PAYLOAD_BYTES,
) -> dict[str, JsonValue]:
    limits = (
        event_limit,
        entity_limit,
        edge_limit,
        measurement_limit,
        json_byte_limit,
        text_byte_limit,
        payload_byte_limit,
    )
    if any(not isinstance(limit, int) or isinstance(limit, bool) for limit in limits):
        raise TimelineError("timeline limits must be integers")
    if any(limit <= 0 for limit in limits):
        raise TimelineError("timeline limits must be positive")
    if contract is not None and proofline_report is not None:
        raise TimelineError("Proofline contract and report inputs are mutually exclusive")
    if candidate is None and (contract is not None or proofline_report is not None):
        raise TimelineError("Proofline timeline evidence requires a candidate runpack")
    with ExitStack() as stack:
        if proofline_report is not None:
            baseline_reader, baseline_identity = stack.enter_context(
                open_runpack_snapshot(baseline)
            )
            candidate_snapshot = (
                stack.enter_context(open_runpack_snapshot(candidate))
                if candidate is not None
                else None
            )
            candidate_reader = candidate_snapshot[0] if candidate_snapshot is not None else None
            candidate_identity = candidate_snapshot[1] if candidate_snapshot is not None else None
        else:
            baseline_reader = stack.enter_context(RunpackReader(resolve_runpack_path(baseline)))
            candidate_reader = (
                stack.enter_context(RunpackReader(resolve_runpack_path(candidate)))
                if candidate is not None
                else None
            )
            baseline_identity = None
            candidate_identity = None
        runs: list[JsonValue] = [
            _run_value(
                baseline_reader,
                event_limit,
                entity_limit,
                edge_limit,
                measurement_limit,
                json_byte_limit,
                text_byte_limit,
            )
        ]
        comparison: JsonValue = None
        proofline: JsonValue = None
        if candidate_reader is not None:
            runs.append(
                _run_value(
                    candidate_reader,
                    event_limit,
                    entity_limit,
                    edge_limit,
                    measurement_limit,
                    json_byte_limit,
                    text_byte_limit,
                )
            )
            diff = compare_readers(baseline_reader, candidate_reader)
            comparison = diff.as_json_value()
            source_path = contract if contract is not None else proofline_report
            if source_path is not None:
                try:
                    proofline = build_proofline_payload(
                        "contract" if contract is not None else "report",
                        source_path,
                        baseline_reader,
                        candidate_reader,
                        diff,
                        baseline_identity,
                        candidate_identity,
                    )
                except (ContractError, ProoflineDataError) as exc:
                    raise TimelineError(str(exc)) from exc
        payload: dict[str, JsonValue] = {
            "runs": runs,
            "comparison": comparison,
            "proofline": proofline,
        }
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
                f"timeline payload is {encoded_size:,} bytes; "
                f"local UI limit is {payload_byte_limit:,}"
            )
        return payload
