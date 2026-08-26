"""Enrich runpacks with bounded OTLP log documents."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

from runtime_tools.model import Attachment, CausalEdge, Entity, Event, Execution, JsonValue
from runtime_tools.providers.builtins.otel.importer._common import (
    MAX_OTLP_LOG_RECORDS,
    OtelImportError,
    OtelLogImportResult,
    _as_list,
    _as_object,
    _attributes,
    _bounded_count_total,
    _load_document,
    _nonnegative_count,
    _OtelLogEnrichmentPlan,
    _semantic_name,
    _severity_number,
    _source_name,
    _span_id,
    _timestamp,
    _trace_id,
    _typed_value,
    _validate_utf8,
)
from runtime_tools.providers.enrichment import enrich_copy, validate_enrichment_destination
from runtime_tools.storage import RunpackReader, RunpackWriter


def _log_name(body: JsonValue) -> str:
    if not isinstance(body, str) or not body:
        return "log"
    if len(body) <= 120:
        return body
    return f"{body[:117]}..."


def _matching_service(
    service_name: str,
    resource_attributes: dict[str, JsonValue],
    entities: tuple[Entity, ...],
) -> tuple[str | None, bool]:
    candidates = tuple(
        entity for entity in entities if entity.kind == "service" and entity.name == service_name
    )
    namespace = resource_attributes.get("service.namespace")
    if isinstance(namespace, str) and namespace:
        candidates = tuple(
            entity
            for entity in candidates
            if entity.attributes.get("service.namespace") == namespace
        )
    instance_id = resource_attributes.get("service.instance.id")
    if isinstance(instance_id, str) and instance_id:
        exact = tuple(
            entity.id
            for entity in candidates
            if entity.attributes.get("service.instance.id") == instance_id
        )
        return (exact[0], False) if len(exact) == 1 else (None, len(candidates) > 1)
    return (candidates[0].id if len(candidates) == 1 else None), len(candidates) > 1


def _plan_otlp_logs(
    execution: Execution,
    existing_entities: tuple[Entity, ...],
    existing_events: tuple[Event, ...],
    resource_logs: list[object],
    *,
    source_identity: str,
    source_name: str,
    raw_document: bytes,
    include_raw: bool,
) -> _OtelLogEnrichmentPlan:
    spans: dict[tuple[str, str], list[Event]] = {}
    for event in existing_events:
        if event.kind == "log.record":
            continue
        trace_id = event.attributes.get("otel.trace_id")
        span_id = event.attributes.get("otel.span_id")
        if isinstance(trace_id, str) and isinstance(span_id, str):
            spans.setdefault((trace_id, span_id), []).append(event)

    entities: list[Entity] = []
    events: list[Event] = []
    edges: list[CausalEdge] = []
    dropped = 0
    missing_spans = 0
    ambiguous_services = 0
    dropped_attribute_count = 0
    log_record_count = 0

    for resource_index, raw_resource_logs in enumerate(resource_logs):
        resource_group = _as_object(raw_resource_logs, "resourceLogs entry")
        resource = _as_object(resource_group.get("resource", {}), "resource")
        resource_dropped_attributes = _nonnegative_count(
            resource.get("droppedAttributesCount"),
            "resource droppedAttributesCount",
        )
        resource_attributes = _attributes(resource.get("attributes", []))
        raw_service_name = resource_attributes.get("service.name")
        service_name = _semantic_name(
            raw_service_name,
            "service.name",
            default="unknown-service",
        )
        fallback_entity = Entity(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (f"{execution.id}:otel-log-source:{source_identity}:resource:{resource_index}"),
            ).hex,
            "service",
            service_name,
            None,
            resource_attributes,
        )
        if raw_service_name in (None, ""):
            matched_service = fallback_entity.id if fallback_entity in existing_entities else None
            ambiguous_service = False
        else:
            matched_service, ambiguous_service = _matching_service(
                service_name, resource_attributes, existing_entities
            )
        new_entity: Entity | None = None
        if matched_service is not None:
            entity_id = matched_service
        else:
            entity_id = fallback_entity.id
            new_entity = fallback_entity

        resource_event_count = 0
        resource_entity_used = False
        scope_logs = _as_list(resource_group.get("scopeLogs", []), "scopeLogs")
        for scope_index, raw_scope_logs in enumerate(scope_logs):
            scope_group = _as_object(raw_scope_logs, "scopeLogs entry")
            scope = _as_object(scope_group.get("scope", {}), "scope")
            scope_name = _semantic_name(scope.get("name"), "log scope name", default="")
            scope_version = _semantic_name(scope.get("version"), "log scope version", default="")
            scope_attributes = _attributes(scope.get("attributes", []))
            scope_dropped_attributes = _nonnegative_count(
                scope.get("droppedAttributesCount"),
                "scope droppedAttributesCount",
            )
            log_records = _as_list(scope_group.get("logRecords", []), "logRecords")
            scope_event_count = 0
            for record_index, raw_log_record in enumerate(log_records):
                log_record_count += 1
                if log_record_count > MAX_OTLP_LOG_RECORDS:
                    raise OtelImportError(
                        f"OTLP JSON exceeds the {MAX_OTLP_LOG_RECORDS}-log-record input limit"
                    )
                record = _as_object(raw_log_record, "log record")
                raw_timestamp = record.get("timeUnixNano")
                if raw_timestamp in (None, ""):
                    raw_timestamp = record.get("observedTimeUnixNano")
                timestamp_ns = _timestamp(raw_timestamp, "log timestamp")
                record_dropped_attributes = _nonnegative_count(
                    record.get("droppedAttributesCount"),
                    "log droppedAttributesCount",
                )
                trace_id = _trace_id(record.get("traceId"), "log traceId", optional=True)
                span_id = _span_id(record.get("spanId"), "log spanId", optional=True)
                if bool(trace_id) != bool(span_id):
                    raise OtelImportError("log records must contain traceId and spanId together")

                raw_body = record.get("body")
                body = _typed_value(raw_body) if raw_body is not None else None
                attributes = _attributes(record.get("attributes", []))
                attributes["log.body"] = body
                if scope_name:
                    attributes["otel.scope.name"] = scope_name
                if scope_version:
                    attributes["otel.scope.version"] = scope_version
                if scope_attributes:
                    attributes["otel.scope.attributes"] = scope_attributes
                severity_text = record.get("severityText")
                if severity_text is not None:
                    if not isinstance(severity_text, str):
                        raise OtelImportError("log severityText must be a string")
                    attributes["log.severity_text"] = _validate_utf8(
                        severity_text, "log severityText"
                    )
                if "severityNumber" in record:
                    attributes["log.severity_number"] = _severity_number(record["severityNumber"])
                if trace_id:
                    attributes["otel.trace_id"] = trace_id
                    attributes["otel.span_id"] = span_id

                if timestamp_ns is not None and (
                    timestamp_ns < execution.started_at_ns
                    or (
                        execution.finished_at_ns is not None
                        and timestamp_ns > execution.finished_at_ns
                    )
                ):
                    dropped += 1
                    continue

                dropped_attribute_count = _bounded_count_total(
                    dropped_attribute_count,
                    record_dropped_attributes,
                    "log droppedAttributesCount",
                )

                event_id = (
                    "otel:log:"
                    + uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        (
                            f"{execution.id}:{source_identity}:"
                            f"{resource_index}:{scope_index}:{record_index}"
                        ),
                    ).hex
                )
                event_entity_id = entity_id
                if trace_id:
                    span_matches = spans.get((trace_id, span_id), [])
                    if len(span_matches) == 1:
                        span = span_matches[0]
                        if span.entity_id is not None:
                            event_entity_id = span.entity_id
                        edges.append(
                            CausalEdge(
                                span.id,
                                event_id,
                                "emits",
                                1.0,
                                {"source": "otel-log"},
                            )
                        )
                    else:
                        missing_spans += 1
                if event_entity_id == entity_id:
                    resource_entity_used = True
                events.append(
                    Event(
                        event_id,
                        "log.record",
                        _log_name(body),
                        event_entity_id,
                        timestamp_ns,
                        timestamp_ns,
                        f"otel-log-resource-{resource_index}-scope-{scope_index}",
                        None,
                        record_index,
                        attributes,
                    )
                )
                resource_event_count += 1
                scope_event_count += 1
            if scope_event_count:
                dropped_attribute_count = _bounded_count_total(
                    dropped_attribute_count,
                    scope_dropped_attributes,
                    "scope droppedAttributesCount",
                )

        if resource_entity_used and new_entity is not None:
            entities.append(new_entity)
        if resource_entity_used and ambiguous_service:
            ambiguous_services += 1
        if resource_event_count:
            dropped_attribute_count = _bounded_count_total(
                dropped_attribute_count,
                resource_dropped_attributes,
                "resource droppedAttributesCount",
            )

    execution_metadata: dict[str, JsonValue] | None = None
    if dropped_attribute_count or missing_spans:
        execution_metadata = execution.metadata.copy()
        raw_otel_metadata = execution_metadata.get("otel")
        if raw_otel_metadata is None:
            otel_metadata: dict[str, JsonValue] = {}
        elif isinstance(raw_otel_metadata, dict):
            otel_metadata = raw_otel_metadata.copy()
        else:
            raise OtelImportError("existing execution otel metadata must be an object")
        if dropped_attribute_count:
            otel_metadata["dropped_attribute_count"] = _bounded_count_total(
                _nonnegative_count(
                    otel_metadata.get("dropped_attribute_count"),
                    "existing dropped OTLP attributes",
                ),
                dropped_attribute_count,
                "dropped OTLP attributes",
            )
        if missing_spans:
            otel_metadata["missing_log_span_count"] = _bounded_count_total(
                _nonnegative_count(
                    otel_metadata.get("missing_log_span_count"),
                    "existing missing OTLP log span references",
                ),
                missing_spans,
                "missing OTLP log span references",
            )
        execution_metadata["otel"] = otel_metadata
    attachments = (
        (
            Attachment(
                id="raw:otlp-logs:" + source_identity,
                kind="raw",
                name=source_name,
                media_type="application/json",
                content=raw_document,
                attributes={"adapter": "otlp-logs"},
            ),
        )
        if include_raw
        else ()
    )
    result = OtelLogImportResult(
        len(events),
        len(edges),
        len(entities),
        dropped,
        missing_spans,
        ambiguous_services,
        dropped_attribute_count,
    )
    return _OtelLogEnrichmentPlan(
        execution.id,
        execution_metadata,
        tuple(entities),
        tuple(events),
        tuple(edges),
        attachments,
        result,
    )


def import_otlp_logs(
    runpack: Path,
    source: Path,
    output: Path,
    *,
    include_raw: bool = False,
) -> OtelLogImportResult:
    """Add bounded OTLP/JSON log records to an existing execution."""
    validate_enrichment_destination(output)
    source_name = _source_name(source)
    document, raw_document = _load_document(source)
    source_identity = hashlib.sha256(raw_document).hexdigest()
    resource_logs = _as_list(document.get("resourceLogs"), "resourceLogs")

    def append(writer: RunpackWriter) -> OtelLogImportResult:
        with RunpackReader(writer.path) as reader:
            plan = _plan_otlp_logs(
                reader.execution(),
                reader.entities(),
                reader.events(),
                resource_logs,
                source_identity=source_identity,
                source_name=source_name,
                raw_document=raw_document,
                include_raw=include_raw,
            )
        if plan.execution_metadata is not None:
            writer.set_execution_metadata(plan.execution_id, plan.execution_metadata)
        writer.add_entities(plan.entities)
        writer.add_events(plan.events)
        writer.add_causal_edges(plan.edges)
        if plan.attachments:
            writer.add_attachments(plan.attachments)
        return plan.result

    return enrich_copy(runpack, output, append)
