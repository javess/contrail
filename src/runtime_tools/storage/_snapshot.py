"""Descriptor-bound runpack snapshots and artifact identities."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.storage._reader import RunpackReader, resolve_runpack_path
from runtime_tools.storage._validation import (
    RunpackArtifactIdentity,
    RunpackError,
    _require_runpack_file_size,
    _require_writable_schema,
    _set_runpack_connection_limits,
    _validate_connection,
)


@contextmanager
def validated_runpack_connection(
    path: Path,
    *,
    prepare_connection: Callable[[sqlite3.Connection], None] | None = None,
) -> Iterator[sqlite3.Connection]:
    """Yield one validated connection so later reads use the same artifact."""
    with RunpackReader(path, prepare_connection=prepare_connection) as reader:
        reader.execution()
        yield reader._connection


def _read_only_descriptor_connection(descriptor: int) -> sqlite3.Connection:
    last_error: sqlite3.OperationalError | None = None
    for root in (Path("/dev/fd"), Path("/proc/self/fd")):
        descriptor_path = root / str(descriptor)
        if not descriptor_path.exists():
            continue
        try:
            return sqlite3.connect(f"{descriptor_path.as_uri()}?mode=ro", uri=True)
        except sqlite3.OperationalError as exc:
            last_error = exc
    raise RunpackError("could not open runpack through its file descriptor") from last_error


@dataclass(frozen=True, slots=True)
class _RunpackDescriptorState:
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int
    link_count: int


def _descriptor_state(descriptor: int) -> _RunpackDescriptorState:
    try:
        status = os.fstat(descriptor)
    except OSError as exc:
        raise RunpackError("could not inspect the open runpack snapshot") from exc
    if not stat.S_ISREG(status.st_mode):
        raise RunpackError("runpack snapshot must be a regular file")
    _require_runpack_file_size(status.st_size)
    return _RunpackDescriptorState(
        status.st_dev,
        status.st_ino,
        status.st_size,
        status.st_mtime_ns,
        status.st_ctime_ns,
        status.st_nlink,
    )


def _require_unchanged_descriptor(descriptor: int, expected: _RunpackDescriptorState) -> None:
    current = _descriptor_state(descriptor)
    same_content_generation = (
        current.device == expected.device
        and current.inode == expected.inode
        and current.size_bytes == expected.size_bytes
        and current.modified_ns == expected.modified_ns
    )
    metadata_unchanged = (
        current.changed_ns == expected.changed_ns and current.link_count == expected.link_count
    )
    detached_by_path_replacement = current.link_count == 0
    if not same_content_generation or not (metadata_unchanged or detached_by_path_replacement):
        raise RunpackError("runpack changed while its read snapshot was open")


def _hash_descriptor(descriptor: int, expected: _RunpackDescriptorState) -> RunpackArtifactIdentity:
    digest = hashlib.sha256()
    offset = 0
    try:
        while offset < expected.size_bytes:
            chunk = os.pread(
                descriptor,
                min(1024 * 1024, expected.size_bytes - offset),
                offset,
            )
            if not chunk:
                break
            digest.update(chunk)
            offset += len(chunk)
    except OSError as exc:
        raise RunpackError("could not hash the open runpack snapshot") from exc
    _require_unchanged_descriptor(descriptor, expected)
    if offset != expected.size_bytes:
        raise RunpackError("runpack changed while its read snapshot was open")
    return RunpackArtifactIdentity(expected.size_bytes, digest.hexdigest())


@contextmanager
def _validated_descriptor_snapshot(
    descriptor: int,
    path: Path,
    *,
    require_writable: bool,
) -> Iterator[RunpackReader]:
    connection: sqlite3.Connection | None = None
    reader: RunpackReader | None = None
    try:
        _descriptor_state(descriptor)
        connection = _read_only_descriptor_connection(descriptor)
        connection.row_factory = sqlite3.Row
        _set_runpack_connection_limits(connection)
        connection.execute("BEGIN")
        schema_version = _validate_connection(connection)
        if require_writable:
            _require_writable_schema(schema_version)
        reader = RunpackReader.__new__(RunpackReader)
        reader.path = path
        reader._connection = connection
        reader._schema_version = schema_version
        reader.execution()
        yield reader
    except sqlite3.DatabaseError as exc:
        raise RunpackError("invalid runpack opened through its file descriptor") from exc
    finally:
        if reader is not None:
            reader.close()
        elif connection is not None:
            connection.close()


@contextmanager
def validated_runpack_snapshot(descriptor: int) -> Iterator[RunpackReader]:
    """Yield a validated, writable-schema snapshot bound to an open descriptor."""
    with _validated_descriptor_snapshot(
        descriptor,
        Path(f"/dev/fd/{descriptor}"),
        require_writable=True,
    ) as reader:
        yield reader


@contextmanager
def open_runpack_snapshot(
    path: Path,
) -> Iterator[tuple[RunpackReader, RunpackArtifactIdentity]]:
    """Open one stable readable runpack generation and its streamed byte identity."""
    resolved_path = resolve_runpack_path(path)
    descriptor: int | None = None
    stable_state: _RunpackDescriptorState | None = None
    try:
        descriptor = os.open(
            resolved_path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        opened_state = _descriptor_state(descriptor)
        try:
            path_status = resolved_path.stat(follow_symlinks=False)
        except OSError as exc:
            raise RunpackError("runpack path changed while opening its read snapshot") from exc
        if (path_status.st_dev, path_status.st_ino) != (
            opened_state.device,
            opened_state.inode,
        ):
            raise RunpackError("runpack path changed while opening its read snapshot")
        try:
            with _validated_descriptor_snapshot(
                descriptor,
                resolved_path,
                require_writable=False,
            ) as reader:
                stable_state = _descriptor_state(descriptor)
                identity = _hash_descriptor(descriptor, stable_state)
                try:
                    yield reader, identity
                finally:
                    _require_unchanged_descriptor(descriptor, stable_state)
        finally:
            if stable_state is not None:
                _require_unchanged_descriptor(descriptor, stable_state)
    except OSError as exc:
        raise RunpackError(f"could not open runpack snapshot: {path}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
