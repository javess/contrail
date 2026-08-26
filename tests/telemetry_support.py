from __future__ import annotations

import uuid


def trace_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-trace:{label}").hex


def span_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-span:{label}").hex[:16]
