"""Small value coercions shared by BatchScope analyses."""

from __future__ import annotations

import math

from runtime_tools.model import Event, JsonValue


def nonnegative_number(value: JsonValue | None) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    result = float(value)
    return result if math.isfinite(result) and result >= 0 else None


def event_number(event: Event, key: str) -> float | None:
    return nonnegative_number(event.attributes.get(key))
