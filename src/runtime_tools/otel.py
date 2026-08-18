"""Import bounded OTLP/JSON trace exports into a runpack."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Never

from runtime_tools.artifacts import publish_without_overwrite
from runtime_tools.enrichment import enrich_copy
from runtime_tools.model import Attachment, CausalEdge, Entity, Event, Execution, JsonValue
from runtime_tools.storage import RunpackReader, RunpackWriter


class OtelImportError(ValueError):
    """Raised when OTLP JSON cannot be normalized safely."""


_MAX_RUNPACK_TIMESTAMP_NS = (1 << 63) - 1
_MIN_OTLP_INT = -(1 << 63)
_MAX_OTLP_INT = (1 << 63) - 1
MAX_OTLP_DOCUMENT_BYTES = 64 * 1024 * 1024
MAX_OTLP_ATTRIBUTE_DEPTH = 64
MAX_OTLP_SPANS = 1_000_000
MAX_OTLP_LINKS = 1_000_000
MAX_OTLP_LOG_RECORDS = 1_000_000
_OTLP_VALUE_FIELDS = (
    "stringValue",
    "boolValue",
    "intValue",
    "doubleValue",
    "bytesValue",
    "arrayValue",
    "kvlistValue",
)


@dataclass(frozen=True, slots=True)
class OtelImportResult:
    entity_count: int
    event_count: int
    edge_count: int
    missing_parent_count: int
    missing_link_count: int


@dataclass(frozen=True, slots=True)
class OtelLogImportResult:
    event_count: int
    edge_count: int
    new_entity_count: int
    dropped_outside_window: int
    missing_span_count: int
    ambiguous_service_count: int
    dropped_attribute_count: int


def _typed_value(value: object, *, depth: int = 0) -> JsonValue:
    if depth > MAX_OTLP_ATTRIBUTE_DEPTH:
        raise OtelImportError(f"OTLP attribute nesting exceeds {MAX_OTLP_ATTRIBUTE_DEPTH} levels")
    if not isinstance(value, dict):
        raise OtelImportError("OTLP attribute value must be an object")
    variants = tuple(field for field in _OTLP_VALUE_FIELDS if field in value)
    if len(variants) > 1:
        raise OtelImportError("OTLP attribute value must contain exactly one value variant")
    if "stringValue" in value:
        string = value["stringValue"]
        if not isinstance(string, str):
            raise OtelImportError("OTLP stringValue is invalid")
        return string
    if "boolValue" in value:
        boolean = value["boolValue"]
        if not isinstance(boolean, bool):
            raise OtelImportError("OTLP boolValue is invalid")
        return boolean
    if "intValue" in value:
        try:
            integer_value = _integral_decimal(value["intValue"])
        except (InvalidOperation, ValueError) as exc:
            raise OtelImportError("OTLP intValue is invalid") from exc
        if not _MIN_OTLP_INT <= integer_value <= _MAX_OTLP_INT:
            raise OtelImportError("OTLP intValue exceeds the signed 64-bit range")
        return int(integer_value)
    if "doubleValue" in value:
        try:
            number = float(str(value["doubleValue"]))
        except ValueError as exc:
            raise OtelImportError("OTLP doubleValue is invalid") from exc
        if not math.isfinite(number):
            raise OtelImportError("OTLP doubleValue must be finite")
        return number
    if "bytesValue" in value:
        encoded = value["bytesValue"]
        if not isinstance(encoded, str):
            raise OtelImportError("OTLP bytesValue is invalid")
        try:
            raw = encoded.encode("ascii")
            decoded = base64.b64decode(
                raw + b"=" * (-len(raw) % 4),
                altchars=b"-_",
                validate=True,
            )
        except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
            raise OtelImportError("OTLP bytesValue is invalid base64") from exc
        return base64.b64encode(decoded).decode("ascii")
    if "arrayValue" in value:
        array = value["arrayValue"]
        if not isinstance(array, dict) or not isinstance(array.get("values", []), list):
            raise OtelImportError("OTLP arrayValue is invalid")
        return [_typed_value(item, depth=depth + 1) for item in array.get("values", [])]
    if "kvlistValue" in value:
        key_values = value["kvlistValue"]
        if not isinstance(key_values, dict):
            raise OtelImportError("OTLP kvlistValue is invalid")
        return _attributes(key_values.get("values", []), depth=depth + 1)
    raise OtelImportError("unsupported OTLP attribute value")


def _attributes(raw: object, *, depth: int = 0) -> dict[str, JsonValue]:
    if raw is None:
        return {}
    if not isinstance(raw, list):
        raise OtelImportError("OTLP attributes must be a list")
    result: dict[str, JsonValue] = {}
    for item in raw:
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            raise OtelImportError("OTLP attribute must contain a string key")
        key = item["key"]
        if key in result:
            raise OtelImportError(f"duplicate OTLP attribute key: {key}")
        result[key] = _typed_value(item.get("value"), depth=depth)
    return result


def _as_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise OtelImportError(f"{label} must be an object")
    return value


def _as_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise OtelImportError(f"{label} must be a list")
    return value


def _identifier(value: object, label: str, *, optional: bool = False) -> str:
    if value in (None, "") and optional:
        return ""
    if not isinstance(value, str) or not value:
        raise OtelImportError(f"{label} must be a non-empty string")
    return value


def _semantic_name(value: object, label: str, *, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise OtelImportError(f"{label} must be a string")
    return value or default


def _integral_decimal(value: object) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer")
    number = Decimal(str(value))
    if not number.is_finite() or number != number.to_integral_value():
        raise ValueError("value is not a finite integer")
    return number


def _timestamp(value: object, label: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        timestamp_value = _integral_decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise OtelImportError(f"{label} must be Unix nanoseconds") from exc
    if timestamp_value < 0:
        raise OtelImportError(f"{label} cannot be negative")
    if timestamp_value > _MAX_RUNPACK_TIMESTAMP_NS:
        raise OtelImportError(f"{label} exceeds the runpack timestamp range")
    return int(timestamp_value)


def _nonnegative_count(value: object, label: str) -> int:
    if value is None:
        return 0
    try:
        count_value = _integral_decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise OtelImportError(f"{label} must be a non-negative integer") from exc
    if count_value < 0 or count_value > _MAX_RUNPACK_TIMESTAMP_NS:
        raise OtelImportError(f"{label} must be a non-negative runpack integer")
    return int(count_value)


def _bounded_count_total(current: int, value: object, label: str) -> int:
    total = current + _nonnegative_count(value, label)
    if total > _MAX_RUNPACK_TIMESTAMP_NS:
        raise OtelImportError(f"total {label} exceeds the runpack range")
    return total


def _span_kind(value: object) -> str:
    kinds = {
        0: "operation",
        1: "operation",
        2: "server.request",
        3: "client.request",
        4: "message.publish",
        5: "message.consume",
        "0": "operation",
        "1": "operation",
        "2": "server.request",
        "3": "client.request",
        "4": "message.publish",
        "5": "message.consume",
        "SPAN_KIND_UNSPECIFIED": "operation",
        "SPAN_KIND_INTERNAL": "operation",
        "SPAN_KIND_SERVER": "server.request",
        "SPAN_KIND_CLIENT": "client.request",
        "SPAN_KIND_PRODUCER": "message.publish",
        "SPAN_KIND_CONSUMER": "message.consume",
    }
    if value is None:
        return "operation"
    if isinstance(value, bool) or not isinstance(value, (int, str)) or value not in kinds:
        raise OtelImportError(f"unsupported OTLP span kind: {value}")
    return kinds[value]


def _status_code(value: object) -> str | None:
    if value is None:
        return None
    status = _as_object(value, "span status")
    if "code" not in status:
        return None
    code = status["code"]
    codes = {
        0: "STATUS_CODE_UNSET",
        1: "STATUS_CODE_OK",
        2: "STATUS_CODE_ERROR",
        "0": "STATUS_CODE_UNSET",
        "1": "STATUS_CODE_OK",
        "2": "STATUS_CODE_ERROR",
        "STATUS_CODE_UNSET": "STATUS_CODE_UNSET",
        "STATUS_CODE_OK": "STATUS_CODE_OK",
        "STATUS_CODE_ERROR": "STATUS_CODE_ERROR",
    }
    if isinstance(code, bool) or not isinstance(code, (int, str)) or code not in codes:
        raise OtelImportError(f"unsupported OTLP span status code: {code}")
    return codes[code]


def _severity_number(value: object) -> int:
    names = {"SEVERITY_NUMBER_UNSPECIFIED": 0}
    for base, start in (
        ("TRACE", 1),
        ("DEBUG", 5),
        ("INFO", 9),
        ("WARN", 13),
        ("ERROR", 17),
        ("FATAL", 21),
    ):
        names[f"SEVERITY_NUMBER_{base}"] = start
        names.update(
            {f"SEVERITY_NUMBER_{base}{offset}": start + offset - 1 for offset in range(2, 5)}
        )
    if isinstance(value, str) and value in names:
        return names[value]
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise OtelImportError("log severityNumber must be an OTLP severity enum")
    try:
        number = int(value)
    except ValueError as exc:
        raise OtelImportError("log severityNumber must be an OTLP severity enum") from exc
    if not 0 <= number <= 24:
        raise OtelImportError("log severityNumber must be between 0 and 24")
    return number


def _event_id(trace_id: str, span_id: str) -> str:
    return f"otel:{trace_id}:{span_id}"


def _validate_parent_hierarchy(parent_edges: list[tuple[str, str]]) -> None:
    parent_by_child = {child: parent for parent, child in parent_edges}
    complete: set[str] = set()
    for child in parent_by_child:
        trail: set[str] = set()
        current = child
        while current in parent_by_child and current not in complete:
            if current in trail:
                raise OtelImportError("OTLP parent relationships contain a cycle")
            trail.add(current)
            current = parent_by_child[current]
        complete.update(trail)


def _reject_json_constant(value: str) -> Never:
    raise ValueError(f"non-finite JSON constant: {value}")


def _load_document(source: Path) -> tuple[dict[str, object], bytes]:
    try:
        with source.open("rb") as stream:
            raw = stream.read(MAX_OTLP_DOCUMENT_BYTES + 1)
    except OSError as exc:
        raise OtelImportError(f"could not read OTLP JSON: {source}") from exc
    if len(raw) > MAX_OTLP_DOCUMENT_BYTES:
        raise OtelImportError(f"OTLP JSON exceeds the {MAX_OTLP_DOCUMENT_BYTES}-byte input limit")
    try:
        value = json.loads(raw.decode("utf-8"), parse_constant=_reject_json_constant)
    except UnicodeDecodeError as exc:
        raise OtelImportError("OTLP JSON must be UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise OtelImportError(
            f"invalid OTLP JSON at line {exc.lineno}, column {exc.colno}"
        ) from exc
    except RecursionError as exc:
        raise OtelImportError("OTLP JSON nesting is too deep") from exc
    except ValueError as exc:
        raise OtelImportError(f"invalid OTLP JSON: {exc}") from exc
    return _as_object(value, "OTLP document"), raw


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
    dropped_link_count = 0
    dropped_attribute_count = 0

    for resource_index, raw_resource_spans in enumerate(resource_spans):
        resource_group = _as_object(raw_resource_spans, "resourceSpans entry")
        resource = _as_object(resource_group.get("resource", {}), "resource")
        dropped_attribute_count = _bounded_count_total(
            dropped_attribute_count,
            resource.get("droppedAttributesCount"),
            "dropped OTLP attributes",
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
        entities.append(Entity(entity_id, "service", service_name, None, resource_attributes))

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
                dropped_attribute_count = _bounded_count_total(
                    dropped_attribute_count,
                    span.get("droppedAttributesCount"),
                    "dropped OTLP attributes",
                )
                trace_id = _identifier(span.get("traceId"), "span traceId")
                span_id = _identifier(span.get("spanId"), "span spanId")
                event_id = _event_id(trace_id, span_id)
                if event_id in known_events:
                    raise OtelImportError(f"duplicate span identity: {trace_id}/{span_id}")
                known_events.add(event_id)
                trace_ids.add(trace_id)
                parent_span_id = _identifier(
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
                    linked_trace_id = _identifier(link.get("traceId"), "span link traceId")
                    linked_span_id = _identifier(link.get("spanId"), "span link spanId")
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
        except OSError as exc:
            raise OtelImportError(f"could not publish runpack {output}: {exc}") from exc
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
    instance_id = resource_attributes.get("service.instance.id")
    if isinstance(instance_id, str) and instance_id:
        exact = tuple(
            entity.id
            for entity in candidates
            if entity.attributes.get("service.instance.id") == instance_id
        )
        return (exact[0], False) if len(exact) == 1 else (None, len(candidates) > 1)
    return (candidates[0].id if len(candidates) == 1 else None), len(candidates) > 1


def import_otlp_logs(
    runpack: Path,
    source: Path,
    output: Path,
    *,
    include_raw: bool = False,
) -> OtelLogImportResult:
    """Add bounded OTLP/JSON log records to an existing execution."""
    document, raw_document = _load_document(source)
    source_identity = hashlib.sha256(raw_document).hexdigest()
    resource_logs = _as_list(document.get("resourceLogs"), "resourceLogs")
    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        existing_entities = reader.entities()
        existing_events = reader.events()

    spans: dict[tuple[str, str], list[Event]] = {}
    for event in existing_events:
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
        service_name = _semantic_name(
            resource_attributes.get("service.name"),
            "service.name",
            default="unknown-service",
        )
        matched_service, ambiguous_service = _matching_service(
            service_name, resource_attributes, existing_entities
        )
        new_entity: Entity | None = None
        if matched_service is not None:
            entity_id = matched_service
        else:
            entity_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{execution.id}:otel-log-resource:{resource_index}:{service_name}",
            ).hex
            new_entity = Entity(entity_id, "service", service_name, None, resource_attributes)

        resource_event_count = 0
        scope_logs = _as_list(resource_group.get("scopeLogs", []), "scopeLogs")
        for scope_index, raw_scope_logs in enumerate(scope_logs):
            scope_group = _as_object(raw_scope_logs, "scopeLogs entry")
            scope = _as_object(scope_group.get("scope", {}), "scope")
            scope_name = _semantic_name(scope.get("name"), "log scope name", default="")
            scope_version = _semantic_name(scope.get("version"), "log scope version", default="")
            scope_attributes = _attributes(scope.get("attributes", []))
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
                    record.get("droppedAttributesCount"),
                    "log droppedAttributesCount",
                )

                trace_id_value = record.get("traceId", "")
                span_id_value = record.get("spanId", "")
                if not isinstance(trace_id_value, str) or not isinstance(span_id_value, str):
                    raise OtelImportError("log traceId and spanId must be strings")
                trace_id = trace_id_value
                span_id = span_id_value
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
                    attributes["log.severity_text"] = severity_text
                if "severityNumber" in record:
                    attributes["log.severity_number"] = _severity_number(record["severityNumber"])
                if trace_id:
                    attributes["otel.trace_id"] = trace_id
                    attributes["otel.span_id"] = span_id

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
                events.append(
                    Event(
                        event_id,
                        "log.record",
                        _log_name(body),
                        event_entity_id,
                        timestamp_ns,
                        None,
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
                    scope.get("droppedAttributesCount"),
                    "scope droppedAttributesCount",
                )

        if resource_event_count and new_entity is not None:
            entities.append(new_entity)
        if resource_event_count and ambiguous_service:
            ambiguous_services += 1
        if resource_event_count:
            dropped_attribute_count = _bounded_count_total(
                dropped_attribute_count,
                resource_dropped_attributes,
                "resource droppedAttributesCount",
            )

    def append(writer: RunpackWriter) -> OtelLogImportResult:
        if dropped_attribute_count:
            metadata = execution.metadata.copy()
            raw_otel_metadata = metadata.get("otel")
            if raw_otel_metadata is None:
                otel_metadata: dict[str, JsonValue] = {}
            elif isinstance(raw_otel_metadata, dict):
                otel_metadata = raw_otel_metadata.copy()
            else:
                raise OtelImportError("existing execution otel metadata must be an object")
            otel_metadata["dropped_attribute_count"] = _bounded_count_total(
                _nonnegative_count(
                    otel_metadata.get("dropped_attribute_count"),
                    "existing dropped OTLP attributes",
                ),
                dropped_attribute_count,
                "dropped OTLP attributes",
            )
            metadata["otel"] = otel_metadata
            writer.set_execution_metadata(execution.id, metadata)
        writer.add_entities(entities)
        writer.add_events(events)
        writer.add_causal_edges(edges)
        if include_raw:
            writer.add_attachments(
                (
                    Attachment(
                        id="raw:otlp-logs:" + source_identity,
                        kind="raw",
                        name=source.name,
                        media_type="application/json",
                        content=raw_document,
                        attributes={"adapter": "otlp-logs"},
                    ),
                )
            )
        return OtelLogImportResult(
            len(events),
            len(edges),
            len(entities),
            dropped,
            missing_spans,
            ambiguous_services,
            dropped_attribute_count,
        )

    return enrich_copy(runpack, output, append)
