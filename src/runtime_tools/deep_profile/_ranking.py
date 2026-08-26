"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import heapq
import os
import socket
import sqlite3
import struct
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Literal

from runtime_tools.deep_profile._common import (
    _MAX_INTEGER,
    _SNAPSHOT_HEADER,
    _SNAPSHOT_KIND_CHECKPOINT,
    _SNAPSHOT_PROTOCOL_MAGIC,
    _SNAPSHOT_RECEIVE_TIMEOUT_SECONDS,
    MAX_DEEP_PROFILE_DIRECTORY_ENTRIES,
    MAX_DEEP_PROFILE_FILE_BYTES,
    MAX_DEEP_PROFILE_FILES,
    MAX_PROFILE_RANKING_BYTES,
    MAX_PROFILE_SNAPSHOT_MESSAGES,
    MAX_PROFILE_SNAPSHOT_SERIALIZATION_NS,
    DeepProfileError,
    _bounded_sum,
    _write_all,
)


class _AggregateRanker:
    _PAGE_BYTES = 4_096
    _UPSERT = """
        INSERT INTO candidates (
            candidate_key,
            collision_identity,
            primary_score,
            secondary_score
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT(candidate_key) DO UPDATE SET
            primary_score = candidates.primary_score + excluded.primary_score,
            secondary_score = candidates.secondary_score + excluded.secondary_score
        WHERE candidates.collision_identity = excluded.collision_identity
          AND candidates.primary_score <= ? - excluded.primary_score
          AND candidates.secondary_score <= ? - excluded.secondary_score
    """

    def __init__(self, directory: Path) -> None:
        connection: sqlite3.Connection | None = None
        descriptor: int | None = None
        path: Path | None = None
        try:
            descriptor, raw_path = tempfile.mkstemp(
                prefix=".contrail-profile-ranking-",
                suffix=".sqlite3",
                dir=directory,
            )
            path = Path(raw_path)
            os.fchmod(descriptor, 0o600)
            os.close(descriptor)
            descriptor = None
            connection = sqlite3.connect(path)
            connection.execute(f"PRAGMA page_size={self._PAGE_BYTES}")
            maximum_pages = MAX_PROFILE_RANKING_BYTES // self._PAGE_BYTES
            maximum_page_result = connection.execute(
                f"PRAGMA max_page_count={MAX_PROFILE_RANKING_BYTES // self._PAGE_BYTES}"
            ).fetchone()
            if (
                maximum_page_result is None
                or len(maximum_page_result) != 1
                or not isinstance(maximum_page_result[0], int)
                or isinstance(maximum_page_result[0], bool)
                or not 0 < maximum_page_result[0] <= maximum_pages
            ):
                raise sqlite3.OperationalError("profile-ranking page limit was not applied")
            connection.execute("PRAGMA cache_size=-8192")
            connection.execute("PRAGMA mmap_size=0")
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute(
                """
                CREATE TABLE candidates (
                    candidate_key BLOB PRIMARY KEY,
                    collision_identity BLOB NOT NULL,
                    primary_score INTEGER NOT NULL,
                    secondary_score INTEGER NOT NULL
                ) WITHOUT ROWID
                """
            )
        except (OSError, sqlite3.Error) as exc:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            if path is not None:
                try:
                    path.unlink()
                except OSError:
                    pass
            raise DeepProfileError(
                "could not prepare the bounded profile-ranking workspace"
            ) from exc
        if path is None or connection is None:
            raise DeepProfileError("could not prepare the bounded profile-ranking workspace")
        self._connection = connection
        self._path = path

    def __enter__(self) -> _AggregateRanker:
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> Literal[False]:
        cleanup_error: OSError | sqlite3.Error | None = None
        try:
            self._connection.close()
        except sqlite3.Error as exc:
            cleanup_error = exc
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            if cleanup_error is None:
                cleanup_error = exc
        if exception is None and cleanup_error is not None:
            raise DeepProfileError("could not clean up the profile-ranking workspace") from (
                cleanup_error
            )
        return False

    def add(
        self,
        key: bytes,
        collision_identity: bytes,
        primary_score: int,
        secondary_score: int,
        label: str,
    ) -> None:
        try:
            cursor = self._connection.execute(
                self._UPSERT,
                (
                    key,
                    collision_identity,
                    primary_score,
                    secondary_score,
                    _MAX_INTEGER,
                    _MAX_INTEGER,
                ),
            )
            if cursor.rowcount == 1:
                return
            row = self._connection.execute(
                "SELECT collision_identity FROM candidates WHERE candidate_key = ?",
                (key,),
            ).fetchone()
        except sqlite3.Error as exc:
            raise DeepProfileError(
                "generated profile evidence exceeds its bounded ranking workspace"
            ) from exc
        if row is None or len(row) != 1 or not isinstance(row[0], bytes):
            raise DeepProfileError("generated profile ranking state is invalid")
        if row[0] != collision_identity:
            raise DeepProfileError("generated profile function identities conflict")
        raise DeepProfileError(f"{label} exceeds the supported integer range")

    def selected_keys(self, limit: int) -> frozenset[bytes]:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise DeepProfileError("profile ranking limit must be a positive integer")
        try:
            rows = self._connection.execute(
                """
                SELECT candidate_key, primary_score, secondary_score
                FROM candidates
                """
            )
            selected: list[tuple[int, int, bytes]] = []
            for row in rows:
                if (
                    len(row) != 3
                    or not isinstance(row[0], bytes)
                    or not isinstance(row[1], int)
                    or isinstance(row[1], bool)
                    or not isinstance(row[2], int)
                    or isinstance(row[2], bool)
                ):
                    raise DeepProfileError("generated profile ranking state is invalid")
                candidate = (row[1], row[2], row[0])
                if len(selected) < limit:
                    heapq.heappush(selected, candidate)
                elif candidate > selected[0]:
                    heapq.heapreplace(selected, candidate)
            return frozenset(candidate[2] for candidate in selected)
        except DeepProfileError:
            raise
        except sqlite3.Error as exc:
            raise DeepProfileError("could not select bounded profile evidence") from exc

    def database_bytes(self) -> int:
        try:
            page_count = self._connection.execute("PRAGMA page_count").fetchone()
            page_size = self._connection.execute("PRAGMA page_size").fetchone()
        except sqlite3.Error as exc:
            raise DeepProfileError("could not measure the profile-ranking workspace") from exc
        if (
            page_count is None
            or len(page_count) != 1
            or not isinstance(page_count[0], int)
            or isinstance(page_count[0], bool)
            or page_count[0] < 0
            or page_size is None
            or len(page_size) != 1
            or not isinstance(page_size[0], int)
            or isinstance(page_size[0], bool)
            or page_size[0] <= 0
        ):
            raise DeepProfileError("generated profile ranking state is invalid")
        size = page_count[0] * page_size[0]
        if size > MAX_PROFILE_RANKING_BYTES:
            raise DeepProfileError("generated profile ranking exceeded its workspace limit")
        return size


@dataclass(slots=True)
class _FunctionProcessAggregate:
    call_count: int = 0
    total_ns: int = 0
    self_ns: int = 0
    max_ns: int = 0
    exception_count: int = 0
    non_control_flow_exception_count: int = 0

    def add(
        self,
        call_count: int,
        total_ns: int,
        self_ns: int,
        max_ns: int,
        exception_count: int,
        non_control_flow_exception_count: int,
    ) -> None:
        self.call_count = _bounded_sum(
            self.call_count, call_count, "deep-profile process call count"
        )
        self.total_ns = _bounded_sum(self.total_ns, total_ns, "deep-profile process total time")
        self.self_ns = _bounded_sum(self.self_ns, self_ns, "deep-profile process self time")
        self.max_ns = max(self.max_ns, max_ns)
        self.exception_count = _bounded_sum(
            self.exception_count,
            exception_count,
            "deep-profile process exception count",
        )
        self.non_control_flow_exception_count = _bounded_sum(
            self.non_control_flow_exception_count,
            non_control_flow_exception_count,
            "deep-profile process non-control-flow exception count",
        )


@dataclass(slots=True)
class _FunctionAggregate:
    call_count: int = 0
    total_ns: int = 0
    self_ns: int = 0
    max_ns: int = 0
    exception_count: int = 0
    non_control_flow_exception_count: int = 0
    processes: dict[int, _FunctionProcessAggregate] | None = None

    def add(
        self,
        call_count: int,
        total_ns: int,
        self_ns: int,
        max_ns: int,
        exception_count: int,
        non_control_flow_exception_count: int,
        pid: int,
    ) -> None:
        self.call_count = _bounded_sum(
            self.call_count, call_count, "deep-profile function call count"
        )
        self.total_ns = _bounded_sum(self.total_ns, total_ns, "deep-profile function total time")
        self.self_ns = _bounded_sum(self.self_ns, self_ns, "deep-profile function self time")
        self.max_ns = max(self.max_ns, max_ns)
        self.exception_count = _bounded_sum(
            self.exception_count,
            exception_count,
            "deep-profile function exception count",
        )
        self.non_control_flow_exception_count = _bounded_sum(
            self.non_control_flow_exception_count,
            non_control_flow_exception_count,
            "deep-profile function non-control-flow exception count",
        )
        if self.processes is None:
            self.processes = {}
        process = self.processes.get(pid)
        if process is None:
            process = _FunctionProcessAggregate()
            self.processes[pid] = process
        process.add(
            call_count,
            total_ns,
            self_ns,
            max_ns,
            exception_count,
            non_control_flow_exception_count,
        )


@dataclass(slots=True)
class _EdgeAggregate:
    call_count: int = 0
    total_ns: int = 0


@dataclass(slots=True)
class _SampleFunctionProcessAggregate:
    sample_count: int = 0
    leaf_sample_count: int = 0

    def add(self, sample_count: int, leaf_sample_count: int) -> None:
        self.sample_count = _bounded_sum(
            self.sample_count, sample_count, "sample-profile process sample count"
        )
        self.leaf_sample_count = _bounded_sum(
            self.leaf_sample_count,
            leaf_sample_count,
            "sample-profile process leaf sample count",
        )


@dataclass(slots=True)
class _SampleFunctionAggregate:
    sample_count: int = 0
    leaf_sample_count: int = 0
    processes: dict[int, _SampleFunctionProcessAggregate] | None = None

    def add(self, sample_count: int, leaf_sample_count: int, pid: int) -> None:
        self.sample_count = _bounded_sum(
            self.sample_count, sample_count, "sample-profile function sample count"
        )
        self.leaf_sample_count = _bounded_sum(
            self.leaf_sample_count,
            leaf_sample_count,
            "sample-profile function leaf sample count",
        )
        if self.processes is None:
            self.processes = {}
        process = self.processes.get(pid)
        if process is None:
            process = _SampleFunctionProcessAggregate()
            self.processes[pid] = process
        process.add(sample_count, leaf_sample_count)


@dataclass(slots=True)
class _SampleEdgeAggregate:
    sample_count: int = 0


def _receive_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = connection.recv(min(64 * 1024, remaining))
        if not chunk:
            raise OSError("profile snapshot connection closed before its message completed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


@dataclass(slots=True)
class _ProfileSnapshotCollector:
    directory: Path
    socket_path: Path
    listener: socket.socket
    stop_event: threading.Event
    thread: threading.Thread | None = None
    message_count: int = 0
    error_count: int = 0
    total_payload_bytes: int = 0
    max_payload_bytes: int = 0
    total_serialization_ns: int = 0
    max_serialization_ns: int = 0
    checkpoint_message_count: int = 0
    checkpoint_payload_bytes: int = 0
    max_checkpoint_payload_bytes: int = 0
    checkpoint_serialization_ns: int = 0
    max_checkpoint_serialization_ns: int = 0
    accepted_process_ids: set[int] = field(default_factory=set)
    dropped_process_ids: set[int] = field(default_factory=set)
    dropped_process_count_truncated: bool = False
    _stopped: bool = False

    def start(self) -> None:
        self.thread = threading.Thread(
            target=self._run,
            name="contrail-profile-snapshot-collector",
            daemon=True,
        )
        self.thread.start()

    def _write_snapshot(self, pid: int, payload: bytes) -> None:
        temporary = self.directory / f".profile-{pid}-{uuid.uuid4().hex}.tmp"
        destination = self.directory / f"profile-{pid}.json"
        descriptor: int | None = None
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            _write_all(descriptor, payload)
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, destination)
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            try:
                temporary.unlink()
            except OSError:
                pass

    def _handle(self, connection: socket.socket) -> None:
        if self.message_count >= MAX_PROFILE_SNAPSHOT_MESSAGES:
            raise OSError("profile snapshot message limit exceeded")
        connection.settimeout(_SNAPSHOT_RECEIVE_TIMEOUT_SECONDS)
        header = _receive_exact(connection, _SNAPSHOT_HEADER.size)
        magic, pid, payload_size, serialization_ns, snapshot_kind = _SNAPSHOT_HEADER.unpack(header)
        if magic != _SNAPSHOT_PROTOCOL_MAGIC:
            raise OSError("profile snapshot protocol magic is invalid")
        if pid <= 0 or pid > _MAX_INTEGER:
            raise OSError("profile snapshot process id is invalid")
        if payload_size <= 0 or payload_size > MAX_DEEP_PROFILE_FILE_BYTES:
            raise OSError("profile snapshot payload size is invalid")
        if serialization_ns > MAX_PROFILE_SNAPSHOT_SERIALIZATION_NS:
            raise OSError("profile snapshot serialization duration is invalid")
        if snapshot_kind not in {1, _SNAPSHOT_KIND_CHECKPOINT, 3}:
            raise OSError("profile snapshot kind is invalid")
        payload = _receive_exact(connection, payload_size)
        self.message_count += 1
        self.total_payload_bytes += payload_size
        self.max_payload_bytes = max(self.max_payload_bytes, payload_size)
        self.total_serialization_ns += serialization_ns
        self.max_serialization_ns = max(self.max_serialization_ns, serialization_ns)
        if snapshot_kind == _SNAPSHOT_KIND_CHECKPOINT:
            self.checkpoint_message_count += 1
            self.checkpoint_payload_bytes += payload_size
            self.max_checkpoint_payload_bytes = max(
                self.max_checkpoint_payload_bytes,
                payload_size,
            )
            self.checkpoint_serialization_ns += serialization_ns
            self.max_checkpoint_serialization_ns = max(
                self.max_checkpoint_serialization_ns,
                serialization_ns,
            )
        if pid in self.dropped_process_ids:
            return
        if pid not in self.accepted_process_ids:
            if len(self.accepted_process_ids) >= MAX_DEEP_PROFILE_FILES:
                if len(self.dropped_process_ids) < MAX_DEEP_PROFILE_DIRECTORY_ENTRIES:
                    self.dropped_process_ids.add(pid)
                else:
                    self.dropped_process_count_truncated = True
                return
            self.accepted_process_ids.add(pid)
        self._write_snapshot(pid, payload)

    def _run(self) -> None:
        self.listener.settimeout(0.05)
        while True:
            if self.message_count >= MAX_PROFILE_SNAPSHOT_MESSAGES:
                self.error_count += 1
                try:
                    self.listener.close()
                except OSError:
                    pass
                return
            try:
                connection, _ = self.listener.accept()
            except TimeoutError:
                if self.stop_event.is_set():
                    return
                continue
            except OSError:
                return
            try:
                with connection:
                    self._handle(connection)
            except (OSError, OverflowError, struct.error):
                self.error_count += 1

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=2.0)
        try:
            self.listener.close()
        except OSError:
            pass
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=0.5)
            if self.thread.is_alive():
                self.error_count += 1
        try:
            self.socket_path.unlink()
        except OSError:
            pass
