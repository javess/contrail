"""Small optional annotation API for domain concepts capture cannot infer."""

from __future__ import annotations

import contextvars
import fcntl
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from runtime_tools.model import JsonValue

_ANNOTATIONS_ENV = "CONTRAIL_ANNOTATIONS_FILE"
_current_event_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "contrail_current_event_id", default=None
)


@dataclass(frozen=True, slots=True)
class EventRef:
    id: str


def _write(record: dict[str, JsonValue]) -> None:
    target = os.environ.get(_ANNOTATIONS_ENV)
    if target is None:
        return
    payload = (json.dumps(record, allow_nan=False, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(Path(target), os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            remaining = memoryview(payload)
            while remaining:
                try:
                    written = os.write(descriptor, remaining)
                except InterruptedError:
                    continue
                if written <= 0:
                    raise OSError("could not append annotation record")
                remaining = remaining[written:]
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


class _Scope:
    def __init__(self, kind: str, name: str, attributes: dict[str, JsonValue]) -> None:
        self.ref = EventRef(uuid.uuid4().hex)
        self.kind = kind
        self.name = name
        self.attributes = attributes
        self._token: contextvars.Token[str | None] | None = None
        self._used = False

    def __enter__(self) -> EventRef:
        if self._used:
            raise RuntimeError("annotation scopes cannot be reused")
        self._used = True
        parent_id = _current_event_id.get()
        _write(
            {
                "record": "event_start",
                "id": self.ref.id,
                "kind": self.kind,
                "name": self.name,
                "timestamp_ns": time.time_ns(),
                "parent_id": parent_id,
                "attributes": self.attributes,
            }
        )
        self._token = _current_event_id.set(self.ref.id)
        return self.ref

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            _write(
                {
                    "record": "event_end",
                    "id": self.ref.id,
                    "timestamp_ns": time.time_ns(),
                    "error": exc_type is not None,
                }
            )
        finally:
            if self._token is not None:
                _current_event_id.reset(self._token)
                self._token = None


def run(name: str, **attributes: JsonValue) -> _Scope:
    return _Scope("run", name, attributes)


def stage(name: str, **attributes: JsonValue) -> _Scope:
    return _Scope("stage", name, attributes)


def event(name: str, *, kind: str = "event", **attributes: JsonValue) -> EventRef:
    ref = EventRef(uuid.uuid4().hex)
    _write(
        {
            "record": "event_instant",
            "id": ref.id,
            "kind": kind,
            "name": name,
            "timestamp_ns": time.time_ns(),
            "parent_id": _current_event_id.get(),
            "attributes": attributes,
        }
    )
    return ref


def progress(*, completed: int | float, total: int | float, **attributes: JsonValue) -> EventRef:
    return event("progress", kind="progress", completed=completed, total=total, **attributes)


def link(source: EventRef, target: EventRef, *, relation: str = "causes") -> None:
    _write(
        {
            "record": "link",
            "source_id": source.id,
            "target_id": target.id,
            "relation": relation,
        }
    )
