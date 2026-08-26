"""Strict helpers shared by evidence JSON protocols."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import ClassVar, cast

from pydantic import TypeAdapter

from runtime_tools.model import JsonValue

OUTPUT_FORMAT_VERSION = "2"
_RFC3339_TIMESTAMP = re.compile(
    r"^(?P<whole>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?(?P<zone>Z|[+-]\d{2}:\d{2})$"
)
_RFC3339_WITHOUT_ZONE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MIN_SIGNED_64 = -(1 << 63)
_MAX_SIGNED_64 = (1 << 63) - 1


class JsonInputError(ValueError):
    """Raised when a bounded JSON input cannot be decoded safely."""


@cache
def _json_model_adapter(model_type: type[object]) -> TypeAdapter[object]:
    return TypeAdapter(model_type)


class JsonValueModel:
    """Serialize dataclass fields through one cached Pydantic adapter."""

    __slots__ = ()

    @classmethod
    def json_schema(cls) -> dict[str, object]:
        """Generate this model's JSON Schema from its type annotations."""
        return cast(dict[str, object], _json_model_adapter(cls).json_schema())

    def as_json_value(self) -> dict[str, JsonValue]:
        value: object = _json_model_adapter(type(self)).dump_python(self, mode="json")
        if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
            raise TypeError("JSON model serialization must produce an object")
        return cast(dict[str, JsonValue], value)


class JsonDocumentModel(JsonValueModel):
    """Pydantic-serialized model with a public document discriminator."""

    __slots__ = ()
    document_type: ClassVar[str]

    def as_json_value(self) -> dict[str, JsonValue]:
        return output_document(
            self.document_type,
            JsonValueModel.as_json_value(self),
        )


def output_document(
    document_type: str,
    body: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    """Add the stable protocol discriminator to one public JSON document."""
    if "document_type" in body or "format_version" in body:
        raise ValueError("output document body contains reserved protocol fields")
    return {
        "document_type": document_type,
        "format_version": OUTPUT_FORMAT_VERSION,
        **body,
    }


def reject_duplicate_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object while rejecting ambiguous duplicate member names."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def reject_nonfinite_constant(value: str) -> None:
    """Reject the non-standard NaN and infinity tokens accepted by ``json``."""
    raise ValueError(f"non-finite JSON constant: {value}")


def load_bounded_json(
    source: Path,
    *,
    label: str,
    max_bytes: int,
    syntax_label: str | None = None,
    include_column: bool = True,
    parse_float: Callable[[str], object] = float,
) -> tuple[object, bytes]:
    """Read and strictly decode one size-bounded UTF-8 JSON document."""
    try:
        with source.open("rb") as stream:
            raw = stream.read(max_bytes + 1)
    except OSError as exc:
        raise JsonInputError(f"could not read {label}: {source}") from exc
    if len(raw) > max_bytes:
        raise JsonInputError(f"{label} exceeds the {max_bytes}-byte input limit")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise JsonInputError(f"{label} must be UTF-8") from exc

    json_label = syntax_label or label
    try:
        value = json.loads(
            text,
            parse_float=parse_float,
            parse_constant=reject_nonfinite_constant,
            object_pairs_hook=reject_duplicate_object,
        )
    except json.JSONDecodeError as exc:
        location = f"line {exc.lineno}, column {exc.colno}"
        if not include_column:
            location = f"line {exc.lineno}"
        raise JsonInputError(f"invalid {json_label} JSON at {location}") from exc
    except RecursionError as exc:
        raise JsonInputError(f"{json_label} JSON nesting is too deep") from exc
    except ValueError as exc:
        raise JsonInputError(f"invalid {json_label} JSON: {exc}") from exc
    return value, raw


def parse_rfc3339_nanoseconds(
    value: object,
    *,
    label: str,
    optional: bool = False,
) -> int | None:
    """Parse an RFC 3339 timestamp into a signed 64-bit nanosecond value."""
    if optional and (value is None or value == ""):
        return None
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an RFC 3339 string")
    match = _RFC3339_TIMESTAMP.fullmatch(value)
    if match is None:
        if _RFC3339_WITHOUT_ZONE.fullmatch(value):
            raise ValueError(f"{label} requires a timezone: {value}")
        raise ValueError(f"invalid {label}: {value}")
    zone = "+00:00" if match.group("zone") == "Z" else match.group("zone")
    try:
        parsed = datetime.fromisoformat(f"{match.group('whole')}{zone}")
    except ValueError as exc:
        raise ValueError(f"invalid {label}: {value}") from exc
    delta = parsed.astimezone(UTC) - _EPOCH
    whole_seconds = delta.days * 86_400 + delta.seconds
    fraction = match.group("fraction") or ""
    timestamp_ns = whole_seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")
    if not _MIN_SIGNED_64 <= timestamp_ns <= _MAX_SIGNED_64:
        raise ValueError(f"{label} exceeds runpack range: {value}")
    return timestamp_ns
