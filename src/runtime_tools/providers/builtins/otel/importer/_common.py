"""Normalize bounded OTLP/JSON trace and log evidence."""

from __future__ import annotations

import base64
import binascii
import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast
from urllib.parse import quote

from runtime_tools.json_support import JsonInputError, load_bounded_json
from runtime_tools.model import Attachment, CausalEdge, Entity, Event, JsonValue


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


@dataclass(frozen=True, slots=True)
class _OtelLogEnrichmentPlan:
    execution_id: str
    execution_metadata: dict[str, JsonValue] | None
    entities: tuple[Entity, ...]
    events: tuple[Event, ...]
    edges: tuple[CausalEdge, ...]
    attachments: tuple[Attachment, ...]
    result: OtelLogImportResult


def _validate_utf8(value: str, label: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise OtelImportError(f"{label} must be valid UTF-8") from exc
    return value


def _source_name(source: Path) -> str:
    return _validate_utf8(source.name, "OTLP source filename")


def _source_directory(source: Path) -> str:
    try:
        return str(source.parent.resolve())
    except (OSError, RuntimeError) as exc:
        raise OtelImportError("could not resolve OTLP source directory") from exc


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
        return _validate_utf8(string, "OTLP stringValue")
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
            if b"=" in raw:
                padding = len(raw) - len(raw.rstrip(b"="))
                if len(raw) % 4 or padding not in (1, 2) or b"=" in raw[:-padding]:
                    raise ValueError("invalid base64 padding")
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
        _validate_utf8(key, "OTLP attribute key")
        if key in result:
            raise OtelImportError(f"duplicate OTLP attribute key: {key}")
        result[key] = _typed_value(item.get("value"), depth=depth)
    return result


def _as_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise OtelImportError(f"{label} must be an object")
    return cast(dict[str, object], value)


def _as_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise OtelImportError(f"{label} must be a list")
    return cast(list[object], value)


def _identifier(value: object, label: str, *, optional: bool = False) -> str:
    if value in (None, "") and optional:
        return ""
    if not isinstance(value, str) or not value:
        raise OtelImportError(f"{label} must be a non-empty string")
    return _validate_utf8(value, label)


def _hex_identifier(
    value: object,
    label: str,
    *,
    length: int,
    optional: bool = False,
) -> str:
    if value in (None, "") and optional:
        return ""
    identifier = _identifier(value, label)
    normalized = identifier.lower()
    if len(identifier) != length or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise OtelImportError(f"{label} must be a {length}-character hexadecimal string")
    if normalized == "0" * length:
        raise OtelImportError(f"{label} must not be all zero")
    return normalized


def _trace_id(value: object, label: str, *, optional: bool = False) -> str:
    return _hex_identifier(value, label, length=32, optional=optional)


def _span_id(value: object, label: str, *, optional: bool = False) -> str:
    return _hex_identifier(value, label, length=16, optional=optional)


def _semantic_name(value: object, label: str, *, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise OtelImportError(f"{label} must be a string")
    return _validate_utf8(value, label) or default


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


def _enum_integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        if (
            not value.is_finite()
            or value != value.to_integral_value()
            or not _MIN_OTLP_INT <= value <= _MAX_OTLP_INT
        ):
            return None
        return int(value)
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value.is_integer() else None
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _span_kind(value: object) -> str:
    kinds = {
        0: "operation",
        1: "operation",
        2: "server.request",
        3: "client.request",
        4: "message.publish",
        5: "message.consume",
        "SPAN_KIND_UNSPECIFIED": "operation",
        "SPAN_KIND_INTERNAL": "operation",
        "SPAN_KIND_SERVER": "server.request",
        "SPAN_KIND_CLIENT": "client.request",
        "SPAN_KIND_PRODUCER": "message.publish",
        "SPAN_KIND_CONSUMER": "message.consume",
    }
    if value is None:
        return "operation"
    if isinstance(value, str) and value in kinds:
        return kinds[value]
    number = _enum_integer(value)
    if number is None or number not in kinds:
        raise OtelImportError(f"unsupported OTLP span kind: {value}")
    return kinds[number]


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
        "STATUS_CODE_UNSET": "STATUS_CODE_UNSET",
        "STATUS_CODE_OK": "STATUS_CODE_OK",
        "STATUS_CODE_ERROR": "STATUS_CODE_ERROR",
    }
    if isinstance(code, str) and code in codes:
        return codes[code]
    number = _enum_integer(code)
    if number is None or number not in codes:
        raise OtelImportError(f"unsupported OTLP span status code: {code}")
    return codes[number]


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
    number = _enum_integer(value)
    if number is None:
        raise OtelImportError("log severityNumber must be an OTLP severity enum")
    if not 0 <= number <= 24:
        raise OtelImportError("log severityNumber must be between 0 and 24")
    return number


def _event_id(trace_id: str, span_id: str) -> str:
    return f"otel:{quote(trace_id, safe='')}:{quote(span_id, safe='')}"


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


def _load_document(source: Path) -> tuple[dict[str, object], bytes]:
    try:
        value, raw = load_bounded_json(
            source,
            label="OTLP JSON",
            syntax_label="OTLP",
            max_bytes=MAX_OTLP_DOCUMENT_BYTES,
            parse_float=Decimal,
        )
    except JsonInputError as exc:
        raise OtelImportError(str(exc)) from exc
    return _as_object(value, "OTLP document"), raw
