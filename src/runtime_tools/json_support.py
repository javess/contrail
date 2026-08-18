"""Strict helpers shared by evidence JSON protocols."""

from __future__ import annotations

from runtime_tools.model import JsonValue

OUTPUT_FORMAT_VERSION = "1"


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
