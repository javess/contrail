"""Import bounded OTLP/JSON trace exports into a runpack."""

from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.artifacts import publish_without_overwrite
from runtime_tools.model import Attachment, CausalEdge, Entity, Event, Execution, JsonValue
from runtime_tools.storage import RunpackWriter


class OtelImportError(ValueError):
    """Raised when OTLP JSON cannot be normalized safely."""


_MAX_RUNPACK_TIMESTAMP_NS = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class OtelImportResult:
    entity_count: int
    event_count: int
    edge_count: int
    missing_parent_count: int
    missing_link_count: int


def _typed_value(value: object) -> JsonValue:
    if not isinstance(value, dict):
        raise OtelImportError("OTLP attribute value must be an object")
    if "stringValue" in value:
        return str(value["stringValue"])
    if "boolValue" in value:
        boolean = value["boolValue"]
        if not isinstance(boolean, bool):
            raise OtelImportError("OTLP boolValue is invalid")
        return boolean
    if "intValue" in value:
        try:
            return int(str(value["intValue"]))
        except ValueError as exc:
            raise OtelImportError("OTLP intValue is invalid") from exc
    if "doubleValue" in value:
        try:
            number = float(str(value["doubleValue"]))
        except ValueError as exc:
            raise OtelImportError("OTLP doubleValue is invalid") from exc
        if not math.isfinite(number):
            raise OtelImportError("OTLP doubleValue must be finite")
        return number
    if "bytesValue" in value:
        return str(value["bytesValue"])
    if "arrayValue" in value:
        array = value["arrayValue"]
        if not isinstance(array, dict) or not isinstance(array.get("values", []), list):
            raise OtelImportError("OTLP arrayValue is invalid")
        return [_typed_value(item) for item in array.get("values", [])]
    if "kvlistValue" in value:
        key_values = value["kvlistValue"]
        if not isinstance(key_values, dict):
            raise OtelImportError("OTLP kvlistValue is invalid")
        return _attributes(key_values.get("values", []))
    raise OtelImportError("unsupported OTLP attribute value")


def _attributes(raw: object) -> dict[str, JsonValue]:
    if raw is None:
        return {}
    if not isinstance(raw, list):
        raise OtelImportError("OTLP attributes must be a list")
    result: dict[str, JsonValue] = {}
    for item in raw:
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            raise OtelImportError("OTLP attribute must contain a string key")
        result[item["key"]] = _typed_value(item.get("value"))
    return result


def _as_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise OtelImportError(f"{label} must be an object")
    return value


def _as_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise OtelImportError(f"{label} must be a list")
    return value


def _timestamp(value: object, label: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        timestamp = int(str(value))
    except ValueError as exc:
        raise OtelImportError(f"{label} must be Unix nanoseconds") from exc
    if timestamp < 0:
        raise OtelImportError(f"{label} cannot be negative")
    if timestamp > _MAX_RUNPACK_TIMESTAMP_NS:
        raise OtelImportError(f"{label} exceeds the runpack timestamp range")
    return timestamp


def _span_kind(value: object) -> str:
    kinds = {
        1: "operation",
        2: "server.request",
        3: "client.request",
        4: "message.publish",
        5: "message.consume",
        "SPAN_KIND_INTERNAL": "operation",
        "SPAN_KIND_SERVER": "server.request",
        "SPAN_KIND_CLIENT": "client.request",
        "SPAN_KIND_PRODUCER": "message.publish",
        "SPAN_KIND_CONSUMER": "message.consume",
    }
    return kinds.get(value, "operation")


def _event_id(trace_id: str, span_id: str) -> str:
    return f"otel:{trace_id}:{span_id}"


def _load_document(source: Path) -> tuple[dict[str, object], bytes]:
    try:
        raw = source.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except OSError as exc:
        raise OtelImportError(f"could not read OTLP JSON: {source}") from exc
    except UnicodeDecodeError as exc:
        raise OtelImportError("OTLP JSON must be UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise OtelImportError(
            f"invalid OTLP JSON at line {exc.lineno}, column {exc.colno}"
        ) from exc
    return _as_object(value, "OTLP document"), raw


def import_otlp_json(
    source: Path,
    output: Path,
    *,
    name: str,
    include_raw: bool = False,
) -> OtelImportResult:
    """Normalize one OTLP/JSON trace export into a new runpack."""
    if output.exists():
        raise OtelImportError(f"refusing to overwrite existing runpack: {output}")
    if not output.parent.is_dir():
        raise OtelImportError(f"output directory does not exist: {output.parent}")
    document, raw_document = _load_document(source)
    resource_spans = _as_list(document.get("resourceSpans"), "resourceSpans")
    execution_id = uuid.uuid4().hex
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")

    entities: list[Entity] = []
    events: list[Event] = []
    parent_references: list[tuple[str, str, str]] = []
    link_references: list[tuple[str, str, str, dict[str, JsonValue]]] = []
    known_events: set[str] = set()
    trace_ids: set[str] = set()

    for resource_index, raw_resource_spans in enumerate(resource_spans):
        resource_group = _as_object(raw_resource_spans, "resourceSpans entry")
        resource = _as_object(resource_group.get("resource", {}), "resource")
        resource_attributes = _attributes(resource.get("attributes", []))
        service_name_value = resource_attributes.get("service.name", "unknown-service")
        service_name = str(service_name_value)
        entity_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{execution_id}:otel-resource:{resource_index}:{service_name}",
        ).hex
        entities.append(Entity(entity_id, "service", service_name, None, resource_attributes))

        scope_spans = _as_list(resource_group.get("scopeSpans", []), "scopeSpans")
        for raw_scope_spans in scope_spans:
            scope_group = _as_object(raw_scope_spans, "scopeSpans entry")
            scope = _as_object(scope_group.get("scope", {}), "scope")
            scope_name = str(scope.get("name", ""))
            spans = _as_list(scope_group.get("spans", []), "spans")
            for raw_span in spans:
                span = _as_object(raw_span, "span")
                trace_id = str(span.get("traceId", ""))
                span_id = str(span.get("spanId", ""))
                if not trace_id or not span_id:
                    raise OtelImportError("every span requires traceId and spanId")
                event_id = _event_id(trace_id, span_id)
                if event_id in known_events:
                    raise OtelImportError(f"duplicate span identity: {trace_id}/{span_id}")
                known_events.add(event_id)
                trace_ids.add(trace_id)
                parent_span_id = str(span.get("parentSpanId", ""))
                attributes = _attributes(span.get("attributes", []))
                attributes.update(
                    {
                        "otel.trace_id": trace_id,
                        "otel.span_id": span_id,
                        "otel.scope.name": scope_name,
                    }
                )
                if parent_span_id:
                    attributes["otel.parent_span_id"] = parent_span_id
                    parent_references.append((trace_id, parent_span_id, event_id))
                raw_links = _as_list(span.get("links", []), "span links")
                for raw_link in raw_links:
                    link = _as_object(raw_link, "span link")
                    linked_trace_id = str(link.get("traceId", ""))
                    linked_span_id = str(link.get("spanId", ""))
                    if not linked_trace_id or not linked_span_id:
                        raise OtelImportError("every span link requires traceId and spanId")
                    link_references.append(
                        (
                            linked_trace_id,
                            linked_span_id,
                            event_id,
                            _attributes(link.get("attributes", [])),
                        )
                    )
                status = span.get("status")
                if isinstance(status, dict) and "code" in status:
                    attributes["otel.status.code"] = str(status["code"])
                started_at_ns = _timestamp(span.get("startTimeUnixNano"), "span start")
                finished_at_ns = _timestamp(span.get("endTimeUnixNano"), "span end")
                if (
                    started_at_ns is not None
                    and finished_at_ns is not None
                    and finished_at_ns < started_at_ns
                ):
                    raise OtelImportError(f"span {trace_id}/{span_id} ends before it starts")
                events.append(
                    Event(
                        id=event_id,
                        kind=_span_kind(span.get("kind")),
                        name=str(span.get("name", "unnamed")),
                        entity_id=entity_id,
                        started_at_ns=started_at_ns,
                        finished_at_ns=finished_at_ns,
                        clock_domain=f"otel-resource-{resource_index}",
                        uncertainty_ns=None,
                        sequence=None,
                        attributes=attributes,
                    )
                )

    starts = [event.started_at_ns for event in events if event.started_at_ns is not None]
    finishes = [event.finished_at_ns for event in events if event.finished_at_ns is not None]
    if not events:
        raise OtelImportError("OTLP document contains no spans")
    if not starts:
        raise OtelImportError("OTLP document contains no span start timestamps")
    started_at_ns = min(starts)
    finished_at_ns = max(finishes) if finishes else None
    edges: list[CausalEdge] = []
    missing_parent_count = 0
    for trace_id, parent_span_id, child_id in parent_references:
        parent_id = _event_id(trace_id, parent_span_id)
        if parent_id not in known_events:
            missing_parent_count += 1
            continue
        edges.append(CausalEdge(parent_id, child_id, "parent", 1.0, {"source": "otel"}))
    missing_link_count = 0
    known_links: set[tuple[str, str]] = set()
    for trace_id, span_id, target_id, attributes in link_references:
        source_id = _event_id(trace_id, span_id)
        if source_id not in known_events:
            missing_link_count += 1
            continue
        identity = (source_id, target_id)
        if identity in known_links:
            raise OtelImportError(f"duplicate span link: {trace_id}/{span_id} -> {target_id}")
        known_links.add(identity)
        edges.append(
            CausalEdge(
                source_id,
                target_id,
                "link",
                1.0,
                {"source": "otel", "otel.link.attributes": attributes},
            )
        )

    try:
        with RunpackWriter(temporary) as writer:
            writer.add_execution(
                Execution(
                    id=execution_id,
                    name=name,
                    started_at_ns=started_at_ns,
                    finished_at_ns=finished_at_ns,
                    command=(),
                    working_directory=str(source.parent.resolve()),
                    exit_code=None,
                    revision=None,
                    metadata={
                        "capture": {"adapter": "otlp-json", "source": source.name},
                        "otel": {
                            "trace_count": len(trace_ids),
                            "missing_parent_count": missing_parent_count,
                            "missing_link_count": missing_link_count,
                        },
                    },
                )
            )
            writer.add_entities(entities)
            writer.add_events(events)
            writer.add_causal_edges(edges)
            if include_raw:
                writer.add_attachments(
                    (
                        Attachment(
                            id=f"raw:otlp-json:{execution_id}",
                            kind="raw",
                            name=source.name,
                            media_type="application/json",
                            content=raw_document,
                            attributes={"adapter": "otlp-json"},
                        ),
                    )
                )
        try:
            publish_without_overwrite(temporary, output)
        except FileExistsError as exc:
            raise OtelImportError(f"refusing to overwrite existing runpack: {output}") from exc
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return OtelImportResult(
        len(entities),
        len(events),
        len(edges),
        missing_parent_count,
        missing_link_count,
    )
