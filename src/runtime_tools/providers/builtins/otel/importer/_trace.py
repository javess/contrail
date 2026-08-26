"""Normalize bounded OTLP trace documents."""

from __future__ import annotations

import uuid
from pathlib import Path

from runtime_tools.artifacts import artifact_exists, publish_without_overwrite, remove_best_effort
from runtime_tools.model import Attachment, CausalEdge, Entity, Event, Execution, JsonValue
from runtime_tools.providers.builtins.otel.importer._common import (
    _MAX_RUNPACK_TIMESTAMP_NS,
    MAX_OTLP_LINKS,
    MAX_OTLP_SPANS,
    OtelImportError,
    OtelImportResult,
    _as_list,
    _as_object,
    _attributes,
    _bounded_count_total,
    _event_id,
    _load_document,
    _nonnegative_count,
    _semantic_name,
    _source_directory,
    _source_name,
    _span_id,
    _span_kind,
    _status_code,
    _timestamp,
    _trace_id,
    _validate_parent_hierarchy,
)
from runtime_tools.storage import RunpackWriter


def import_otlp_json(
    source: Path,
    output: Path,
    *,
    name: str,
    include_raw: bool = False,
) -> OtelImportResult:
    """Normalize one OTLP/JSON trace export into a new runpack."""
    if not isinstance(name, str) or not name:
        raise OtelImportError("OTLP execution name must be a non-empty string")
    try:
        name.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise OtelImportError("OTLP execution name must be valid UTF-8") from exc
    if artifact_exists(output):
        raise OtelImportError(f"refusing to overwrite existing runpack: {output}")
    if not output.parent.is_dir():
        raise OtelImportError(f"output directory does not exist: {output.parent}")
    source_name = _source_name(source)
    document, raw_document = _load_document(source)
    working_directory = _source_directory(source)
    resource_spans = _as_list(document.get("resourceSpans"), "resourceSpans")
    execution_id = uuid.uuid4().hex
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")

    entities: list[Entity] = []
    events: list[Event] = []
    parent_references: list[tuple[str, str, str]] = []
    link_references: list[tuple[str, str, str, dict[str, JsonValue]]] = []
    known_events: set[str] = set()
    trace_ids: set[str] = set()
    dropped_link_count = 0
    dropped_attribute_count = 0

    for resource_index, raw_resource_spans in enumerate(resource_spans):
        resource_group = _as_object(raw_resource_spans, "resourceSpans entry")
        resource = _as_object(resource_group.get("resource", {}), "resource")
        resource_dropped_attributes = _nonnegative_count(
            resource.get("droppedAttributesCount"),
            "resource droppedAttributesCount",
        )
        resource_attributes = _attributes(resource.get("attributes", []))
        service_name = _semantic_name(
            resource_attributes.get("service.name"),
            "service.name",
            default="unknown-service",
        )
        entity_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{execution_id}:otel-resource:{resource_index}:{service_name}",
        ).hex
        resource_has_spans = False

        scope_spans = _as_list(resource_group.get("scopeSpans", []), "scopeSpans")
        for raw_scope_spans in scope_spans:
            scope_group = _as_object(raw_scope_spans, "scopeSpans entry")
            scope = _as_object(scope_group.get("scope", {}), "scope")
            scope_name = _semantic_name(scope.get("name"), "span scope name", default="")
            scope_version = _semantic_name(scope.get("version"), "span scope version", default="")
            scope_attributes = _attributes(scope.get("attributes", []))
            spans = _as_list(scope_group.get("spans", []), "spans")
            if spans:
                dropped_attribute_count = _bounded_count_total(
                    dropped_attribute_count,
                    scope.get("droppedAttributesCount"),
                    "dropped OTLP attributes",
                )
            for raw_span in spans:
                if len(events) >= MAX_OTLP_SPANS:
                    raise OtelImportError(
                        f"OTLP JSON exceeds the {MAX_OTLP_SPANS}-span input limit"
                    )
                span = _as_object(raw_span, "span")
                resource_has_spans = True
                dropped_attribute_count = _bounded_count_total(
                    dropped_attribute_count,
                    span.get("droppedAttributesCount"),
                    "dropped OTLP attributes",
                )
                trace_id = _trace_id(span.get("traceId"), "span traceId")
                span_id = _span_id(span.get("spanId"), "span spanId")
                event_id = _event_id(trace_id, span_id)
                if event_id in known_events:
                    raise OtelImportError(f"duplicate span identity: {trace_id}/{span_id}")
                known_events.add(event_id)
                trace_ids.add(trace_id)
                parent_span_id = _span_id(
                    span.get("parentSpanId"), "span parentSpanId", optional=True
                )
                attributes = _attributes(span.get("attributes", []))
                attributes.update(
                    {
                        "otel.trace_id": trace_id,
                        "otel.span_id": span_id,
                        "otel.scope.name": scope_name,
                    }
                )
                if scope_version:
                    attributes["otel.scope.version"] = scope_version
                if scope_attributes:
                    attributes["otel.scope.attributes"] = scope_attributes
                if parent_span_id:
                    attributes["otel.parent_span_id"] = parent_span_id
                    parent_references.append((trace_id, parent_span_id, event_id))
                raw_links = _as_list(span.get("links", []), "span links")
                dropped_link_count += _nonnegative_count(
                    span.get("droppedLinksCount"), "span droppedLinksCount"
                )
                if dropped_link_count > _MAX_RUNPACK_TIMESTAMP_NS:
                    raise OtelImportError("total dropped OTLP links exceeds the runpack range")
                for raw_link in raw_links:
                    if len(link_references) >= MAX_OTLP_LINKS:
                        raise OtelImportError(
                            f"OTLP JSON exceeds the {MAX_OTLP_LINKS}-span-link input limit"
                        )
                    link = _as_object(raw_link, "span link")
                    dropped_attribute_count = _bounded_count_total(
                        dropped_attribute_count,
                        link.get("droppedAttributesCount"),
                        "dropped OTLP attributes",
                    )
                    linked_trace_id = _trace_id(link.get("traceId"), "span link traceId")
                    linked_span_id = _span_id(link.get("spanId"), "span link spanId")
                    link_references.append(
                        (
                            linked_trace_id,
                            linked_span_id,
                            event_id,
                            _attributes(link.get("attributes", [])),
                        )
                    )
                status_code = _status_code(span.get("status"))
                if status_code is not None:
                    attributes["otel.status.code"] = status_code
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
                        name=_semantic_name(span.get("name"), "span name", default="unnamed"),
                        entity_id=entity_id,
                        started_at_ns=started_at_ns,
                        finished_at_ns=finished_at_ns,
                        clock_domain=f"otel-resource-{resource_index}",
                        uncertainty_ns=None,
                        sequence=None,
                        attributes=attributes,
                    )
                )
        if resource_has_spans:
            entities.append(Entity(entity_id, "service", service_name, None, resource_attributes))
            dropped_attribute_count = _bounded_count_total(
                dropped_attribute_count,
                resource_dropped_attributes,
                "dropped OTLP attributes",
            )

    starts = [event.started_at_ns for event in events if event.started_at_ns is not None]
    finishes = [event.finished_at_ns for event in events if event.finished_at_ns is not None]
    if not events:
        raise OtelImportError("OTLP document contains no spans")
    if not starts:
        raise OtelImportError("OTLP document contains no span start timestamps")
    started_at_ns = min(starts)
    timing_complete = len(starts) == len(events) and len(finishes) == len(events)
    finished_at_ns = max(finishes) if timing_complete else None
    edges: list[CausalEdge] = []
    parent_edges: list[tuple[str, str]] = []
    missing_parent_count = 0
    for trace_id, parent_span_id, child_id in parent_references:
        parent_id = _event_id(trace_id, parent_span_id)
        if parent_id not in known_events:
            missing_parent_count += 1
            continue
        parent_edges.append((parent_id, child_id))
        edges.append(CausalEdge(parent_id, child_id, "parent", 1.0, {"source": "otel"}))
    _validate_parent_hierarchy(parent_edges)
    missing_link_count = dropped_link_count
    known_links: set[tuple[str, str]] = set()
    for trace_id, span_id, target_id, attributes in link_references:
        source_id = _event_id(trace_id, span_id)
        if source_id not in known_events:
            missing_link_count = _bounded_count_total(
                missing_link_count,
                1,
                "missing OTLP links",
            )
            continue
        if source_id == target_id:
            raise OtelImportError(f"span cannot link to itself: {trace_id}/{span_id}")
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

    temporary_created = False
    try:
        runpack_writer = RunpackWriter(temporary)
        temporary_created = True
        with runpack_writer as writer:
            writer.add_execution(
                Execution(
                    id=execution_id,
                    name=name,
                    started_at_ns=started_at_ns,
                    finished_at_ns=finished_at_ns,
                    command=(),
                    working_directory=working_directory,
                    exit_code=None,
                    revision=None,
                    metadata={
                        "capture": {"adapter": "otlp-json", "source": source_name},
                        "otel": {
                            "trace_count": len(trace_ids),
                            "missing_parent_count": missing_parent_count,
                            "missing_link_count": missing_link_count,
                            "dropped_attribute_count": dropped_attribute_count,
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
                            name=source_name,
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
        except OSError as exc:
            raise OtelImportError(f"could not publish runpack {output}: {exc}") from exc
    except BaseException:
        if temporary_created:
            remove_best_effort(temporary)
        raise
    return OtelImportResult(
        len(entities),
        len(events),
        len(edges),
        missing_parent_count,
        missing_link_count,
    )
