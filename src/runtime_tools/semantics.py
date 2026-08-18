"""Shared semantic predicates over normalized runtime evidence."""

from __future__ import annotations

from collections.abc import Mapping

from runtime_tools.model import JsonValue


def is_operation_error(attributes: Mapping[str, JsonValue]) -> bool:
    """Return whether normalized event attributes explicitly mark an error."""
    status = attributes.get("otel.status.code")
    error_type = attributes.get("error.type")
    return bool(
        attributes.get("error") is True
        or status in (2, "2", "STATUS_CODE_ERROR")
        or isinstance(error_type, str)
        and error_type
    )
