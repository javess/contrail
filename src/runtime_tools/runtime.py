"""Small optional annotation API for domain concepts capture cannot infer."""

from __future__ import annotations

import contextvars
import errno
import fcntl
import json
import math
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from runtime_tools.model import JsonValue

_ANNOTATIONS_ENV = "CONTRAIL_ANNOTATIONS_FILE"
_ANNOTATIONS_FD_ENV = "_CONTRAIL_ANNOTATIONS_FD"
_ANNOTATIONS_FALLBACK_ENV = "_CONTRAIL_ANNOTATIONS_FALLBACK"
_ANNOTATIONS_IDENTITY_ENV = "_CONTRAIL_ANNOTATIONS_IDENTITY"
_MAX_ANNOTATION_FD = 2_147_483_647
_current_event_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "contrail_current_event_id", default=None
)


@dataclass(frozen=True, slots=True)
class EventRef:
    id: str


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError(f"{label} must be a non-empty string without NUL bytes")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{label} must be valid UTF-8") from exc
    return value


def _annotation_identity(value: str | None) -> tuple[int, int]:
    if value is None:
        raise ValueError("inherited annotation fd requires a private file identity")
    device, separator, inode = value.partition(":")
    if (
        separator != ":"
        or ":" in inode
        or not device.isascii()
        or not device.isdigit()
        or not inode.isascii()
        or not inode.isdigit()
        or len(device) > 20
        or len(inode) > 20
    ):
        raise ValueError("inherited annotation identity must be canonical device:inode integers")
    device_number = int(device)
    inode_number = int(inode)
    if device != str(device_number) or inode != str(inode_number):
        raise ValueError("inherited annotation identity must be canonical device:inode integers")
    return device_number, inode_number


def _open_annotation_fallback(path: Path, identity: tuple[int, int]) -> int:
    descriptor = os.open(
        path,
        os.O_APPEND | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        status = os.fstat(descriptor)
    except BaseException:
        os.close(descriptor)
        raise
    if (status.st_dev, status.st_ino) != identity:
        os.close(descriptor)
        raise OSError(errno.ESTALE, "inherited annotation fallback identity changed")
    return descriptor


def _write(record: dict[str, JsonValue]) -> None:
    target = os.environ.get(_ANNOTATIONS_ENV)
    if target is None:
        return
    try:
        payload = (
            json.dumps(
                record,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("annotation strings must be valid UTF-8") from exc
    inherited_fd = os.environ.get(_ANNOTATIONS_FD_ENV)
    if inherited_fd is None:
        descriptor = os.open(Path(target), os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    else:
        if not inherited_fd.isascii() or not inherited_fd.isdigit() or len(inherited_fd) > 10:
            raise ValueError("inherited annotation fd must be a canonical decimal integer")
        descriptor_number = int(inherited_fd)
        if (
            descriptor_number < 3
            or descriptor_number > _MAX_ANNOTATION_FD
            or inherited_fd != str(descriptor_number)
        ):
            raise ValueError("inherited annotation fd must be a canonical descriptor above 2")
        if target != f"/dev/fd/{descriptor_number}":
            raise ValueError("inherited annotation fd does not match the annotation target")
        expected_identity = _annotation_identity(os.environ.get(_ANNOTATIONS_IDENTITY_ENV))
        fallback = os.environ.get(_ANNOTATIONS_FALLBACK_ENV)
        if fallback is None:
            raise ValueError("inherited annotation fd requires a private fallback path")
        fallback_path = Path(fallback)
        prefix, separator, suffix = fallback_path.name.rpartition(".annotations-")
        if (
            not fallback_path.is_absolute()
            or os.path.normpath(fallback) != fallback
            or fallback == target
            or separator != ".annotations-"
            or not prefix.startswith(".")
            or len(suffix) != 32
            or any(character not in "0123456789abcdef" for character in suffix)
        ):
            raise ValueError("inherited annotation fallback must be a canonical capture path")
        try:
            descriptor = os.dup(descriptor_number)
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise
            descriptor = _open_annotation_fallback(fallback_path, expected_identity)
        else:
            try:
                descriptor_status = os.fstat(descriptor)
            except BaseException:
                os.close(descriptor)
                raise
            if (descriptor_status.st_dev, descriptor_status.st_ino) != expected_identity:
                os.close(descriptor)
                descriptor = _open_annotation_fallback(fallback_path, expected_identity)
    try:
        _flock(descriptor, fcntl.LOCK_EX)
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
            _flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _flock(descriptor: int, operation: int) -> None:
    while True:
        try:
            fcntl.flock(descriptor, operation)
            return
        except InterruptedError:
            continue


class _Scope:
    def __init__(self, kind: str, name: str, attributes: dict[str, JsonValue]) -> None:
        self.ref = EventRef(uuid.uuid4().hex)
        self.kind = _required_text(kind, "annotation kind")
        self.name = _required_text(name, "annotation name")
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
    name = _required_text(name, "annotation name")
    kind = _required_text(kind, "annotation kind")
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


def _progress_values(completed: object, total: object) -> tuple[float, float]:
    message = (
        "annotation progress completed and total must be finite numbers with "
        "0 <= completed <= total"
    )
    if (
        not isinstance(completed, (int, float))
        or isinstance(completed, bool)
        or not isinstance(total, (int, float))
        or isinstance(total, bool)
    ):
        raise ValueError(message)
    try:
        completed_value = float(completed)
        total_value = float(total)
    except OverflowError as exc:
        raise ValueError(message) from exc
    completed_is_inexact = isinstance(completed, int) and int(completed_value) != completed
    total_is_inexact = isinstance(total, int) and int(total_value) != total
    if (
        completed_is_inexact
        or total_is_inexact
        or not math.isfinite(completed_value)
        or not math.isfinite(total_value)
        or not 0 <= completed_value <= total_value
    ):
        raise ValueError(message)
    return completed_value, total_value


def progress(*, completed: int | float, total: int | float, **attributes: JsonValue) -> EventRef:
    _progress_values(completed, total)
    return event("progress", kind="progress", completed=completed, total=total, **attributes)


def link(source: EventRef, target: EventRef, *, relation: str = "causes") -> None:
    if not isinstance(source, EventRef) or not isinstance(target, EventRef):
        raise ValueError("annotation links require EventRef endpoints")
    source_id = _required_text(source.id, "annotation source id")
    target_id = _required_text(target.id, "annotation target id")
    relation = _required_text(relation, "annotation relation")
    if source_id == target_id:
        raise ValueError("annotation events cannot link to themselves")
    _write(
        {
            "record": "link",
            "source_id": source_id,
            "target_id": target_id,
            "relation": relation,
        }
    )
