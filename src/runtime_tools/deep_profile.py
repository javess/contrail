"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import hashlib
import heapq
import json
import math
import os
import socket
import sqlite3
import stat
import struct
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Literal, Never

from runtime_tools.json_support import reject_duplicate_object
from runtime_tools.model import CausalEdge, Event, JsonValue


class DeepProfileError(ValueError):
    """Raised when deep-profile setup or generated evidence is invalid."""


DEEP_PROFILE_DIRECTORY_ENV = "_CONTRAIL_DEEP_PROFILE_DIRECTORY"
SAMPLE_PROFILE_DIRECTORY_ENV = "_CONTRAIL_SAMPLE_PROFILE_DIRECTORY"
PROFILE_SNAPSHOT_SOCKET_ENV = "_CONTRAIL_PROFILE_SNAPSHOT_SOCKET"
MAX_DEEP_PROFILE_FILES = 128
MAX_DEEP_PROFILE_DIRECTORY_ENTRIES = 1_024
MAX_DEEP_PROFILE_FILE_BYTES = 16 * 1024 * 1024
MAX_DEEP_PROFILE_TOTAL_BYTES = 64 * 1024 * 1024
MAX_DEEP_PROFILE_FUNCTIONS = 20_000
MAX_DEEP_PROFILE_EDGES = 50_000
MAX_DEEP_PROFILE_PYTHON_FUNCTIONS_PER_PROCESS = 2_000
MAX_DEEP_PROFILE_NATIVE_FUNCTIONS_PER_PROCESS = 2_000
MAX_DEEP_PROFILE_NATIVE_EDGES_PER_PROCESS = 10_000
MAX_SEMANTIC_SUBPROCESS_PER_PROCESS = 256
MAX_SEMANTIC_SUBPROCESS_EVENTS = 2_000
MAX_SEMANTIC_EXECUTABLE_CHARACTERS = 256
MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS = 256
MAX_SEMANTIC_HTTP_REQUEST_EVENTS = 2_000
MAX_SEMANTIC_HTTP_METHOD_CHARACTERS = 32
SEMANTIC_HTTP_ADAPTERS = frozenset(
    {
        "stdlib.http.client",
        "httpcore.sync",
        "httpcore.async",
        "aiohttp.async",
    }
)
MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS = 256
MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS = 2_000
SEMANTIC_NETWORK_ADAPTERS = frozenset(
    {
        "stdlib.socket.connect",
        "asyncio.create_connection",
        "asyncio.create_unix_connection",
    }
)
MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS = 256
MAX_SEMANTIC_NETWORK_SETUP_EVENTS = 2_000
SEMANTIC_NETWORK_SETUP_ADAPTERS = frozenset(
    {
        "stdlib.socket.getaddrinfo",
        "stdlib.ssl.SSLObject.do_handshake",
        "stdlib.ssl.SSLSocket.do_handshake",
    }
)
MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS = 256
MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS = 2_000
SEMANTIC_LOGICAL_OPERATION_ADAPTER_CATEGORIES = {
    "stdlib.asyncio.TaskGroup": "scheduler",
    "stdlib.asyncio.create_task": "scheduler",
    "stdlib.asyncio.ensure_future": "scheduler",
    "stdlib.asyncio.gather": "scheduler",
    "stdlib.wsgiref": "server",
    "uvicorn.h11": "server",
    "uvicorn.httptools": "server",
    "stdlib.concurrent.futures.ProcessPoolExecutor": "executor",
    "stdlib.concurrent.futures.ThreadPoolExecutor": "executor",
    "stdlib.asyncio.Queue": "queue",
    "stdlib.queue.Queue": "queue",
    "stdlib.sqlite3.Connection": "database",
    "stdlib.sqlite3.Cursor": "database",
    "sqlalchemy.engine.Connection": "database",
    "sqlalchemy.orm.Session": "database",
    "sqlalchemy.ext.asyncio.AsyncConnection": "database",
    "sqlalchemy.ext.asyncio.AsyncSession": "database",
    "redis.Redis": "cache",
    "redis.Pipeline": "cache",
    "redis.asyncio.Redis": "cache",
    "redis.asyncio.Pipeline": "cache",
    "pika.BlockingChannel": "broker",
    "aiokafka.AIOKafkaProducer": "broker",
    "aiokafka.AIOKafkaConsumer": "broker",
}
SEMANTIC_LOGICAL_OPERATION_ADAPTERS = frozenset(SEMANTIC_LOGICAL_OPERATION_ADAPTER_CATEGORIES)
MAX_PROFILE_RANKING_BYTES = 256 * 1024 * 1024
PROFILE_CHECKPOINT_INTERVAL_NS = 500_000_000
PROFILE_FIRST_CHECKPOINT_DELAY_NS = 50_000_000
MAX_PROFILE_SNAPSHOT_MESSAGES = 1_000_000
MAX_PROFILE_SNAPSHOT_SERIALIZATION_NS = 3_600_000_000_000
PROFILE_PUBLICATION_METRICS_VERSION = 1
PROFILE_SESSION_CHECKPOINT_VERSION = 1
OBSERVER_INTEGRITY_VERSION = 1
PYTHON_EXCEPTION_FILTER_VERSION = 1
FILTERED_CONTROL_FLOW_EXCEPTION_TYPES = (
    "GeneratorExit",
    "StopAsyncIteration",
    "StopIteration",
)
_MAX_INTEGER = (1 << 63) - 1
_BOOTSTRAP_NAME = "sitecustomize.py"
_SEMANTIC_BOOTSTRAP_NAME = "_semantic_capture_bootstrap.py"
_SNAPSHOT_PROTOCOL_MAGIC = b"CTRP0002"
_SNAPSHOT_HEADER = struct.Struct("!8sQQQQ")
_SNAPSHOT_KIND_CHECKPOINT = 2
_SOCKET_PATH_LIMIT = 96
_SNAPSHOT_RECEIVE_TIMEOUT_SECONDS = 0.1

type PythonProfileMode = Literal["deep", "sample"]
type _DeepFunctionValues = tuple[int, int, int, int, int, int]
type SemanticCaptureStatus = Literal["complete", "partial", "truncated", "unavailable", "invalid"]
type CallerObservation = Literal["exact", "sampled"]
type LogicalOperationCategory = Literal[
    "broker", "cache", "database", "executor", "queue", "scheduler", "server"
]
type LogicalOperationName = Literal[
    "batch",
    "command",
    "commit",
    "consume",
    "execute",
    "executemany",
    "executescript",
    "get",
    "publish",
    "put",
    "rollback",
    "request",
    "task",
]


@dataclass(frozen=True, slots=True)
class _FunctionIdentity:
    module: str
    qualname: str
    filename: str
    firstlineno: int
    scope: str

    @property
    def name(self) -> str:
        return f"{self.module}.{self.qualname}"

    @property
    def native(self) -> bool:
        return self.filename == "<native>"


@dataclass(frozen=True, slots=True)
class _SubprocessRecord:
    identifier: int
    name: str
    parent_pid: int
    child_pid: int | None
    shell: bool | None
    started_at_ns: int
    duration_ns: int | None
    exit_code: int | None
    outcome: Literal["exited", "launch_error", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _SubprocessCaller:
    identity: _FunctionIdentity
    observation: CallerObservation


@dataclass(frozen=True, slots=True)
class _SemanticCaptureEvidence:
    subprocess_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_subprocess_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_subprocess_count: int
    unattributed_subprocess_count: int
    invalid_caller_count: int
    caller_callback_error_count: int


@dataclass(frozen=True, slots=True)
class _HttpRequestRecord:
    identifier: int
    method: str
    scheme: Literal["http", "https"]
    server_port: int | None
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    status_code: int | None
    outcome: Literal["response", "request_error", "closed", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]
    adapter: str


@dataclass(frozen=True, slots=True)
class _HttpCaptureEvidence:
    request_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_request_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_request_count: int
    unattributed_request_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _NetworkConnectionRecord:
    identifier: int
    adapter: str
    transport: Literal["tcp", "unix"]
    address_family: Literal["ipv4", "ipv6", "unix", "unknown"]
    server_port: int | None
    tls_requested: bool | None
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    outcome: Literal["connected", "connect_error", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _NetworkCaptureEvidence:
    connection_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_connection_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_connection_count: int
    unattributed_connection_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _NetworkSetupRecord:
    identifier: int
    phase: Literal["dns", "tls"]
    adapter: str
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    outcome: Literal["completed", "setup_error", "unknown"]
    error_type: str | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _NetworkSetupCaptureEvidence:
    phase_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_phase_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_phase_count: int
    unattributed_phase_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _LogicalOperationRecord:
    identifier: int
    category: LogicalOperationCategory
    operation: LogicalOperationName
    adapter: str
    parent_pid: int
    started_at_ns: int
    duration_ns: int | None
    outcome: Literal["completed", "operation_error", "unknown"]
    error_type: str | None
    status_code: int | None
    caller: _SubprocessCaller | None
    caller_status: Literal["complete", "unavailable", "invalid"]


@dataclass(frozen=True, slots=True)
class _LogicalOperationCaptureEvidence:
    operation_events: tuple[Event, ...]
    caller_events: tuple[Event, ...]
    caller_edges: tuple[CausalEdge, ...]
    status: SemanticCaptureStatus
    process_count: int
    dropped_operation_count: int
    callback_error_count: int
    caller_attribution_status: SemanticCaptureStatus
    attributed_operation_count: int
    unattributed_operation_count: int
    invalid_caller_count: int
    caller_callback_error_count: int
    adapters: tuple[str, ...]


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


@dataclass(frozen=True, slots=True)
class DeepProfileResult:
    events: tuple[Event, ...]
    edges: tuple[CausalEdge, ...]
    process_count: int
    process_ids: tuple[int, ...]
    truncated: bool
    dropped_call_count: int
    dropped_edge_count: int
    callback_error_count: int
    mode: PythonProfileMode = "deep"
    native_function_count: int = 0
    native_call_count: int = 0
    native_exception_count: int = 0
    python_exception_function_count: int = 0
    python_exception_event_count: int = 0
    dropped_python_exception_event_count: int = 0
    python_exception_filter_status: Literal["complete", "unavailable"] = "unavailable"
    python_non_control_flow_exception_function_count: int = 0
    python_non_control_flow_exception_event_count: int = 0
    dropped_python_non_control_flow_exception_event_count: int = 0
    observer_integrity_status: Literal["complete", "partial", "unavailable"] = "unavailable"
    observer_integrity_process_count: int = 0
    profile_hook_setter_call_count: int = 0
    profile_hook_setter_process_count: int = 0
    trace_hook_setter_call_count: int = 0
    trace_hook_setter_process_count: int = 0
    sample_count: int = 0
    thread_sample_count: int = 0
    interval_ns: int = 0
    open_call_count: int = 0
    checkpoint_process_count: int = 0
    registration_only_process_count: int = 0
    dropped_profile_process_count: int = 0
    dropped_profile_process_count_truncated: bool = False
    transport: str = "workload-file"
    collector_error_count: int = 0
    snapshot_metrics_status: Literal["available", "unavailable"] = "unavailable"
    snapshot_message_count: int = 0
    snapshot_payload_bytes: int = 0
    max_snapshot_payload_bytes: int = 0
    snapshot_serialization_ns: int = 0
    max_snapshot_serialization_ns: int = 0
    checkpoint_snapshot_message_count: int = 0
    checkpoint_snapshot_payload_bytes: int = 0
    max_checkpoint_snapshot_payload_bytes: int = 0
    checkpoint_snapshot_serialization_ns: int = 0
    max_checkpoint_snapshot_serialization_ns: int = 0
    publication_metrics_status: Literal["available", "unavailable", "invalid"] = "unavailable"
    publication_fallback_process_ids: tuple[int, ...] = ()
    publication_socket_attempted_process_count: int = 0
    publication_socket_failure_ns: int = 0
    max_publication_socket_failure_ns: int = 0
    normalization_metrics_status: Literal["available", "unavailable"] = "unavailable"
    normalization_duration_ns: int = 0
    ranking_database_peak_bytes: int = 0
    semantic_capture_status: SemanticCaptureStatus = "unavailable"
    semantic_capture_process_count: int = 0
    subprocess_event_count: int = 0
    dropped_subprocess_count: int = 0
    semantic_callback_error_count: int = 0
    semantic_caller_event_count: int = 0
    semantic_caller_edge_count: int = 0
    caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_subprocess_count: int = 0
    unattributed_subprocess_count: int = 0
    invalid_caller_count: int = 0
    caller_callback_error_count: int = 0
    http_capture_status: SemanticCaptureStatus = "unavailable"
    http_capture_process_count: int = 0
    http_request_event_count: int = 0
    dropped_http_request_count: int = 0
    http_callback_error_count: int = 0
    http_caller_event_count: int = 0
    http_caller_edge_count: int = 0
    http_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_http_request_count: int = 0
    unattributed_http_request_count: int = 0
    invalid_http_caller_count: int = 0
    http_caller_callback_error_count: int = 0
    http_adapters: tuple[str, ...] = ()
    network_capture_status: SemanticCaptureStatus = "unavailable"
    network_capture_process_count: int = 0
    network_connection_event_count: int = 0
    dropped_network_connection_count: int = 0
    network_callback_error_count: int = 0
    network_caller_event_count: int = 0
    network_caller_edge_count: int = 0
    network_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_network_connection_count: int = 0
    unattributed_network_connection_count: int = 0
    invalid_network_caller_count: int = 0
    network_caller_callback_error_count: int = 0
    network_adapters: tuple[str, ...] = ()
    network_setup_capture_status: SemanticCaptureStatus = "unavailable"
    network_setup_capture_process_count: int = 0
    network_setup_event_count: int = 0
    dropped_network_setup_count: int = 0
    network_setup_callback_error_count: int = 0
    network_setup_caller_event_count: int = 0
    network_setup_caller_edge_count: int = 0
    network_setup_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_network_setup_count: int = 0
    unattributed_network_setup_count: int = 0
    invalid_network_setup_caller_count: int = 0
    network_setup_caller_callback_error_count: int = 0
    network_setup_adapters: tuple[str, ...] = ()
    logical_operation_capture_status: SemanticCaptureStatus = "unavailable"
    logical_operation_capture_process_count: int = 0
    logical_operation_event_count: int = 0
    dropped_logical_operation_count: int = 0
    logical_operation_callback_error_count: int = 0
    logical_operation_caller_event_count: int = 0
    logical_operation_caller_edge_count: int = 0
    logical_operation_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_logical_operation_count: int = 0
    unattributed_logical_operation_count: int = 0
    invalid_logical_operation_caller_count: int = 0
    logical_operation_caller_callback_error_count: int = 0
    logical_operation_adapters: tuple[str, ...] = ()

    def as_metadata(self) -> dict[str, JsonValue]:
        if self.process_count == 0:
            status = "unavailable"
        elif self.checkpoint_process_count:
            status = "partial"
        elif self.truncated:
            status = "truncated"
        else:
            status = "complete"
        common: dict[str, JsonValue] = {
            "mode": self.mode,
            "status": status,
            "process_count": self.process_count,
            "process_ids": list(self.process_ids),
            "function_count": (
                len(self.events)
                - self.subprocess_event_count
                - self.semantic_caller_event_count
                - self.http_request_event_count
                - self.http_caller_event_count
                - self.network_connection_event_count
                - self.network_caller_event_count
                - self.network_setup_event_count
                - self.network_setup_caller_event_count
                - self.logical_operation_event_count
                - self.logical_operation_caller_event_count
            ),
            "edge_count": (
                len(self.edges)
                - self.semantic_caller_edge_count
                - self.http_caller_edge_count
                - self.network_caller_edge_count
                - self.network_setup_caller_edge_count
                - self.logical_operation_caller_edge_count
            ),
            "truncated": self.truncated,
            "callback_error_count": self.callback_error_count,
            "open_call_count": self.open_call_count,
            "checkpoint_process_count": self.checkpoint_process_count,
            "registration_only_process_count": self.registration_only_process_count,
            "dropped_profile_process_count": self.dropped_profile_process_count,
            "dropped_profile_process_count_truncated": (
                self.dropped_profile_process_count_truncated
            ),
            "checkpoint_interval_ns": PROFILE_CHECKPOINT_INTERVAL_NS,
            "first_checkpoint_delay_ns": PROFILE_FIRST_CHECKPOINT_DELAY_NS,
            "transport": self.transport,
            "collector_error_count": self.collector_error_count,
            "snapshot_metrics": {
                "status": self.snapshot_metrics_status,
                "message_count": self.snapshot_message_count,
                "payload_bytes": self.snapshot_payload_bytes,
                "max_payload_bytes": self.max_snapshot_payload_bytes,
                "serialization_ns": self.snapshot_serialization_ns,
                "max_serialization_ns": self.max_snapshot_serialization_ns,
                "checkpoint_message_count": self.checkpoint_snapshot_message_count,
                "checkpoint_payload_bytes": self.checkpoint_snapshot_payload_bytes,
                "max_checkpoint_payload_bytes": self.max_checkpoint_snapshot_payload_bytes,
                "checkpoint_serialization_ns": self.checkpoint_snapshot_serialization_ns,
                "max_checkpoint_serialization_ns": (self.max_checkpoint_snapshot_serialization_ns),
            },
            "publication_metrics": {
                "status": self.publication_metrics_status,
                "fallback_process_count": len(self.publication_fallback_process_ids),
                "fallback_process_ids": list(self.publication_fallback_process_ids),
                "socket_attempted_process_count": (self.publication_socket_attempted_process_count),
                "socket_failure_ns": self.publication_socket_failure_ns,
                "max_socket_failure_ns": self.max_publication_socket_failure_ns,
            },
            "normalization_metrics": {
                "status": self.normalization_metrics_status,
                "duration_ns": self.normalization_duration_ns,
                "ranking_database_peak_bytes": self.ranking_database_peak_bytes,
                "ranking_database_limit_bytes": MAX_PROFILE_RANKING_BYTES,
            },
            "semantic_capture": {
                "status": self.semantic_capture_status,
                "observer": "python-subprocess-wrapper",
                "zero_code": True,
                "process_count": self.semantic_capture_process_count,
                "subprocess_count": self.subprocess_event_count,
                "dropped_subprocess_count": self.dropped_subprocess_count,
                "callback_error_count": self.semantic_callback_error_count,
                "arguments_captured": False,
                "environment_captured": False,
                "working_directory_captured": False,
                "caller_attribution": {
                    "status": self.caller_attribution_status,
                    "caller_count": self.semantic_caller_event_count,
                    "attributed_subprocess_count": self.attributed_subprocess_count,
                    "unattributed_subprocess_count": self.unattributed_subprocess_count,
                    "invalid_caller_count": self.invalid_caller_count,
                    "callback_error_count": self.caller_callback_error_count,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_subprocesses_per_process": MAX_SEMANTIC_SUBPROCESS_PER_PROCESS,
                    "max_subprocess_events": MAX_SEMANTIC_SUBPROCESS_EVENTS,
                },
            },
            "http_capture": {
                "status": self.http_capture_status,
                "observer": "python-http-client-wrapper",
                "zero_code": True,
                "process_count": self.http_capture_process_count,
                "request_count": self.http_request_event_count,
                "dropped_request_count": self.dropped_http_request_count,
                "callback_error_count": self.http_callback_error_count,
                "adapters": list(self.http_adapters),
                "server_identity_policy": "redact",
                "method_captured": True,
                "scheme_captured": True,
                "server_address_captured": False,
                "path_captured": False,
                "query_captured": False,
                "headers_captured": False,
                "body_captured": False,
                "response_body_captured": False,
                "caller_attribution": {
                    "status": self.http_caller_attribution_status,
                    "caller_count": self.http_caller_event_count,
                    "attributed_request_count": self.attributed_http_request_count,
                    "unattributed_request_count": self.unattributed_http_request_count,
                    "invalid_caller_count": self.invalid_http_caller_count,
                    "callback_error_count": self.http_caller_callback_error_count,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_requests_per_process": MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS,
                    "max_request_events": MAX_SEMANTIC_HTTP_REQUEST_EVENTS,
                },
            },
            "network_capture": {
                "status": self.network_capture_status,
                "observer": "python-network-connection-wrapper",
                "zero_code": True,
                "process_count": self.network_capture_process_count,
                "connection_count": self.network_connection_event_count,
                "dropped_connection_count": self.dropped_network_connection_count,
                "callback_error_count": self.network_callback_error_count,
                "adapters": list(self.network_adapters),
                "server_identity_policy": "redact",
                "server_address_captured": False,
                "path_captured": False,
                "credentials_captured": False,
                "caller_attribution": {
                    "status": self.network_caller_attribution_status,
                    "caller_count": self.network_caller_event_count,
                    "attributed_connection_count": self.attributed_network_connection_count,
                    "unattributed_connection_count": (self.unattributed_network_connection_count),
                    "invalid_caller_count": self.invalid_network_caller_count,
                    "callback_error_count": self.network_caller_callback_error_count,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_connections_per_process": (MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS),
                    "max_connection_events": MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS,
                },
            },
            "network_setup_capture": {
                "status": self.network_setup_capture_status,
                "observer": "python-network-setup-wrapper",
                "zero_code": True,
                "process_count": self.network_setup_capture_process_count,
                "phase_count": self.network_setup_event_count,
                "dropped_phase_count": self.dropped_network_setup_count,
                "callback_error_count": self.network_setup_callback_error_count,
                "adapters": list(self.network_setup_adapters),
                "hostname_captured": False,
                "server_address_captured": False,
                "sni_captured": False,
                "certificate_captured": False,
                "credentials_captured": False,
                "caller_attribution": {
                    "status": self.network_setup_caller_attribution_status,
                    "caller_count": self.network_setup_caller_event_count,
                    "attributed_phase_count": self.attributed_network_setup_count,
                    "unattributed_phase_count": self.unattributed_network_setup_count,
                    "invalid_caller_count": self.invalid_network_setup_caller_count,
                    "callback_error_count": (self.network_setup_caller_callback_error_count),
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_phases_per_process": MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS,
                    "max_phase_events": MAX_SEMANTIC_NETWORK_SETUP_EVENTS,
                },
            },
            "logical_operation_capture": {
                "status": self.logical_operation_capture_status,
                "observer": "python-logical-operation-wrapper",
                "zero_code": True,
                "deep_only": True,
                "process_count": self.logical_operation_capture_process_count,
                "operation_count": self.logical_operation_event_count,
                "dropped_operation_count": self.dropped_logical_operation_count,
                "callback_error_count": self.logical_operation_callback_error_count,
                "adapters": list(self.logical_operation_adapters),
                "statement_captured": False,
                "parameters_captured": False,
                "payload_captured": False,
                "queue_item_captured": False,
                "queue_identity_captured": False,
                "callable_captured": False,
                "awaitable_captured": False,
                "task_name_captured": False,
                "context_captured": False,
                "arguments_captured": False,
                "return_value_captured": False,
                "exception_messages_captured": False,
                "http_method_captured": False,
                "route_captured": False,
                "url_captured": False,
                "headers_captured": False,
                "body_captured": False,
                "response_body_captured": False,
                "client_address_captured": False,
                "caller_attribution": {
                    "status": self.logical_operation_caller_attribution_status,
                    "caller_count": self.logical_operation_caller_event_count,
                    "attributed_operation_count": self.attributed_logical_operation_count,
                    "unattributed_operation_count": self.unattributed_logical_operation_count,
                    "invalid_caller_count": self.invalid_logical_operation_caller_count,
                    "callback_error_count": (self.logical_operation_caller_callback_error_count),
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_operations_per_process": (MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS),
                    "max_operation_events": MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS,
                },
            },
            "limits": {
                "max_profile_files": MAX_DEEP_PROFILE_FILES,
                "max_functions": MAX_DEEP_PROFILE_FUNCTIONS,
                "max_edges": MAX_DEEP_PROFILE_EDGES,
                "max_native_functions_per_process": (MAX_DEEP_PROFILE_NATIVE_FUNCTIONS_PER_PROCESS),
                "max_native_edges_per_process": MAX_DEEP_PROFILE_NATIVE_EDGES_PER_PROCESS,
                "max_total_bytes": MAX_DEEP_PROFILE_TOTAL_BYTES,
                "max_ranking_bytes": MAX_PROFILE_RANKING_BYTES,
                "max_snapshot_messages": MAX_PROFILE_SNAPSHOT_MESSAGES,
                "max_subprocess_events": MAX_SEMANTIC_SUBPROCESS_EVENTS,
                "max_http_request_events": MAX_SEMANTIC_HTTP_REQUEST_EVENTS,
                "max_network_connection_events": MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS,
                "max_network_setup_events": MAX_SEMANTIC_NETWORK_SETUP_EVENTS,
                "max_logical_operation_events": MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS,
            },
        }
        if self.mode == "sample":
            return {
                **common,
                "observer": "python-stack-sampler",
                "intrusive": True,
                "estimated": True,
                "per_call": False,
                "sample_count": self.sample_count,
                "thread_sample_count": self.thread_sample_count,
                "interval_ns": self.interval_ns,
                "dropped_frame_sample_count": self.dropped_call_count,
                "dropped_edge_sample_count": self.dropped_edge_count,
            }
        return {
            **common,
            "observer": "python-sys-setprofile-and-settrace",
            "intrusive": True,
            "per_call": True,
            "observer_integrity": {
                "format_version": OBSERVER_INTEGRITY_VERSION,
                "status": self.observer_integrity_status,
                "process_count": self.observer_integrity_process_count,
                "missing_process_count": max(
                    0,
                    self.process_count - self.observer_integrity_process_count,
                ),
                "profile_hook_setter_call_count": self.profile_hook_setter_call_count,
                "profile_hook_setter_process_count": self.profile_hook_setter_process_count,
                "trace_hook_setter_call_count": self.trace_hook_setter_call_count,
                "trace_hook_setter_process_count": self.trace_hook_setter_process_count,
                "arguments_captured": False,
                "locals_captured": False,
                "hook_values_captured": False,
            },
            "python_exception_capture": {
                "enabled": True,
                "deep_only": True,
                "event_semantics": "per_propagated_frame",
                "function_count": self.python_exception_function_count,
                "event_count": self.python_exception_event_count,
                "dropped_event_count": self.dropped_python_exception_event_count,
                "arguments_captured": False,
                "locals_captured": False,
                "exception_types_captured": False,
                "exception_values_captured": False,
                "exception_messages_captured": False,
                "tracebacks_captured": False,
                "line_events_enabled": False,
                "opcode_events_enabled": False,
                **(
                    {
                        "control_flow_filter": {
                            "format_version": PYTHON_EXCEPTION_FILTER_VERSION,
                            "status": status,
                            "event_semantics": "exact_type_identity",
                            "filtered_exception_types": list(FILTERED_CONTROL_FLOW_EXCEPTION_TYPES),
                            "exception_type_identity_inspected": True,
                            "exception_types_captured": False,
                            "non_control_flow_function_count": (
                                self.python_non_control_flow_exception_function_count
                            ),
                            "non_control_flow_event_count": (
                                self.python_non_control_flow_exception_event_count
                            ),
                            "filtered_event_count": (
                                self.python_exception_event_count
                                - self.python_non_control_flow_exception_event_count
                            ),
                            "dropped_non_control_flow_event_count": (
                                self.dropped_python_non_control_flow_exception_event_count
                            ),
                            "dropped_filtered_event_count": (
                                self.dropped_python_exception_event_count
                                - self.dropped_python_non_control_flow_exception_event_count
                            ),
                        }
                    }
                    if self.python_exception_filter_status == "complete"
                    else {}
                ),
                "limits": {
                    "max_functions_per_process": (MAX_DEEP_PROFILE_PYTHON_FUNCTIONS_PER_PROCESS),
                },
            },
            "native_call_capture": {
                "enabled": True,
                "deep_only": True,
                "function_count": self.native_function_count,
                "call_count": self.native_call_count,
                "exception_count": self.native_exception_count,
                "arguments_captured": False,
                "return_values_captured": False,
                "exception_messages_captured": False,
                "limits": {
                    "max_functions_per_process": (MAX_DEEP_PROFILE_NATIVE_FUNCTIONS_PER_PROCESS),
                    "max_edges_per_process": MAX_DEEP_PROFILE_NATIVE_EDGES_PER_PROCESS,
                },
            },
            "dropped_call_count": self.dropped_call_count,
            "dropped_edge_count": self.dropped_edge_count,
        }


@dataclass(frozen=True, slots=True)
class _SnapshotTransportStats:
    dropped_profile_process_count: int
    dropped_profile_process_count_truncated: bool
    metrics_status: Literal["available", "unavailable"]
    message_count: int
    payload_bytes: int
    max_payload_bytes: int
    serialization_ns: int
    max_serialization_ns: int
    checkpoint_message_count: int
    checkpoint_payload_bytes: int
    max_checkpoint_payload_bytes: int
    checkpoint_serialization_ns: int
    max_checkpoint_serialization_ns: int


@dataclass(frozen=True, slots=True)
class _PublicationMetrics:
    status: Literal["available", "unavailable", "invalid"]
    fallback_process_ids: tuple[int, ...]
    socket_attempted_process_count: int
    socket_failure_ns: int
    max_socket_failure_ns: int


@dataclass(slots=True)
class _PublicationMetricsAccumulator:
    saw_available: bool = False
    saw_unavailable: bool = False
    invalid: bool = False
    fallback_process_ids: set[int] = field(default_factory=set)
    socket_attempted_process_count: int = 0
    socket_failure_ns: int = 0
    max_socket_failure_ns: int = 0

    def add(
        self,
        document: dict[str, object],
        *,
        pid: int,
        snapshot_kind: Literal["checkpoint", "final"],
        registration_only: bool,
    ) -> None:
        version = document.get("publication_metrics_version")
        if version is None:
            self.saw_unavailable = True
            return
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version != PROFILE_PUBLICATION_METRICS_VERSION
        ):
            self.invalid = True
            return
        self.saw_available = True
        raw_fallback = document.get("publication_fallback")
        if raw_fallback is None:
            return
        if not isinstance(raw_fallback, dict):
            self.invalid = True
            return
        socket_attempted = raw_fallback.get("socket_attempted")
        socket_failure_ns = raw_fallback.get("socket_failure_ns")
        raw_fallback_kind = raw_fallback.get("snapshot_kind")
        expected_kind = "registration" if registration_only else snapshot_kind
        if (
            not isinstance(socket_attempted, bool)
            or not isinstance(socket_failure_ns, int)
            or isinstance(socket_failure_ns, bool)
            or not 0 <= socket_failure_ns <= _MAX_INTEGER
            or raw_fallback_kind != expected_kind
            or (not socket_attempted and socket_failure_ns != 0)
        ):
            self.invalid = True
            return
        self.fallback_process_ids.add(pid)
        if socket_attempted:
            self.socket_attempted_process_count += 1
            if self.socket_failure_ns > _MAX_INTEGER - socket_failure_ns:
                self.invalid = True
                return
            self.socket_failure_ns += socket_failure_ns
            self.max_socket_failure_ns = max(self.max_socket_failure_ns, socket_failure_ns)

    def result(self) -> _PublicationMetrics:
        if self.invalid or (self.saw_available and self.saw_unavailable):
            return _PublicationMetrics("invalid", (), 0, 0, 0)
        if not self.saw_available:
            return _PublicationMetrics("unavailable", (), 0, 0, 0)
        return _PublicationMetrics(
            "available",
            tuple(sorted(self.fallback_process_ids)),
            self.socket_attempted_process_count,
            self.socket_failure_ns,
            self.max_socket_failure_ns,
        )


@dataclass(slots=True)
class DeepProfileSession:
    directory: Path
    identity: tuple[int, int]
    mode: PythonProfileMode = "deep"
    collector: _ProfileSnapshotCollector | None = None
    recovered_transport_stats: _SnapshotTransportStats | None = None
    recovered_collector_error_count: int = 0

    def configure_environment(self, environment: dict[str, str]) -> None:
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            str(self.directory)
            if not existing_pythonpath
            else f"{self.directory}{os.pathsep}{existing_pythonpath}"
        )
        directory_environment = (
            DEEP_PROFILE_DIRECTORY_ENV if self.mode == "deep" else SAMPLE_PROFILE_DIRECTORY_ENV
        )
        environment[directory_environment] = str(self.directory)
        environment.pop(PROFILE_SNAPSHOT_SOCKET_ENV, None)
        if self.collector is not None:
            environment[PROFILE_SNAPSHOT_SOCKET_ENV] = str(self.collector.socket_path)

    @property
    def transport(self) -> str:
        return (
            "controller-unix-socket"
            if self.snapshot_metrics_status == "available"
            else "workload-file"
        )

    @property
    def collector_error_count(self) -> int:
        if self.collector is not None:
            return self.collector.error_count
        return self.recovered_collector_error_count

    @property
    def snapshot_metrics_status(self) -> Literal["available", "unavailable"]:
        if self.collector is not None:
            return "available"
        if self.recovered_transport_stats is not None:
            return self.recovered_transport_stats.metrics_status
        return "unavailable"

    def finish_collection(self) -> None:
        if self.collector is not None:
            self.collector.stop()

    def close(self) -> None:
        self.finish_collection()
        try:
            status = self.directory.stat(follow_symlinks=False)
        except OSError:
            return
        if not stat.S_ISDIR(status.st_mode) or (status.st_dev, status.st_ino) != self.identity:
            return
        try:
            entries = tuple(self.directory.iterdir())
        except OSError:
            return
        for entry in entries:
            try:
                entry_status = entry.stat(follow_symlinks=False)
                if stat.S_ISREG(entry_status.st_mode) or stat.S_ISLNK(entry_status.st_mode):
                    entry.unlink()
                elif entry.name == "__pycache__" and stat.S_ISDIR(entry_status.st_mode):
                    for cached in entry.iterdir():
                        cached_status = cached.stat(follow_symlinks=False)
                        if stat.S_ISREG(cached_status.st_mode) or stat.S_ISLNK(
                            cached_status.st_mode
                        ):
                            cached.unlink()
                    entry.rmdir()
            except OSError:
                pass
        try:
            self.directory.rmdir()
        except OSError:
            pass


def _profile_transport(session: DeepProfileSession, metrics: _PublicationMetrics) -> str:
    if session.snapshot_metrics_status == "unavailable":
        return "workload-file"
    if metrics.status == "available" and metrics.fallback_process_ids:
        return "mixed"
    return "controller-unix-socket"


def _snapshot_transport_stats(
    session: DeepProfileSession,
    dropped_file_count: int,
) -> _SnapshotTransportStats:
    collector = session.collector
    if collector is None:
        recovered = session.recovered_transport_stats
        if recovered is not None:
            return _SnapshotTransportStats(
                recovered.dropped_profile_process_count + dropped_file_count,
                recovered.dropped_profile_process_count_truncated,
                recovered.metrics_status,
                recovered.message_count,
                recovered.payload_bytes,
                recovered.max_payload_bytes,
                recovered.serialization_ns,
                recovered.max_serialization_ns,
                recovered.checkpoint_message_count,
                recovered.checkpoint_payload_bytes,
                recovered.max_checkpoint_payload_bytes,
                recovered.checkpoint_serialization_ns,
                recovered.max_checkpoint_serialization_ns,
            )
        return _SnapshotTransportStats(
            dropped_file_count,
            False,
            "unavailable",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        )
    return _SnapshotTransportStats(
        dropped_file_count + len(collector.dropped_process_ids),
        collector.dropped_process_count_truncated,
        "available",
        collector.message_count,
        collector.total_payload_bytes,
        collector.max_payload_bytes,
        collector.total_serialization_ns,
        collector.max_serialization_ns,
        collector.checkpoint_message_count,
        collector.checkpoint_payload_bytes,
        collector.max_checkpoint_payload_bytes,
        collector.checkpoint_serialization_ns,
        collector.max_checkpoint_serialization_ns,
    )


def _write_all(descriptor: int, content: bytes) -> None:
    view = memoryview(content)
    while view:
        try:
            written = os.write(descriptor, view)
        except InterruptedError:
            continue
        if written <= 0:
            raise OSError("deep-profile bootstrap write made no progress")
        view = view[written:]


def _prepare_snapshot_collector(directory: Path) -> _ProfileSnapshotCollector | None:
    socket_path = Path(tempfile.gettempdir()) / f"contrail-profile-{uuid.uuid4().hex[:16]}.sock"
    if len(os.fsencode(socket_path)) > _SOCKET_PATH_LIMIT:
        return None
    listener: socket.socket | None = None
    try:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        os.chmod(socket_path, 0o600)
        listener.listen(64)
        collector = _ProfileSnapshotCollector(
            directory,
            socket_path,
            listener,
            threading.Event(),
        )
        collector.start()
        return collector
    except (OSError, RuntimeError):
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        try:
            socket_path.unlink()
        except OSError:
            pass
        return None


def _prepare_profile_session(parent: Path, mode: PythonProfileMode) -> DeepProfileSession:
    source = Path(__file__).with_name(
        "_deep_profile_bootstrap.py" if mode == "deep" else "_sampling_profile_bootstrap.py"
    )
    semantic_source = Path(__file__).with_name("_semantic_capture_bootstrap.py")
    try:
        bootstrap = source.read_bytes()
        semantic_bootstrap = semantic_source.read_bytes()
    except OSError as exc:
        raise DeepProfileError(f"could not read the packaged {mode} capture bootstrap") from exc
    directory = parent / f".contrail-{mode}-profile-{uuid.uuid4().hex}"
    try:
        directory.mkdir(mode=0o700)
        status = directory.stat(follow_symlinks=False)
        for name, content in (
            (_BOOTSTRAP_NAME, bootstrap),
            (_SEMANTIC_BOOTSTRAP_NAME, semantic_bootstrap),
        ):
            descriptor = os.open(
                directory / name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                _write_all(descriptor, content)
            finally:
                os.close(descriptor)
    except OSError as exc:
        for name in (_BOOTSTRAP_NAME, _SEMANTIC_BOOTSTRAP_NAME):
            try:
                (directory / name).unlink(missing_ok=True)
            except OSError:
                pass
        try:
            directory.rmdir()
        except OSError:
            pass
        raise DeepProfileError(f"could not prepare the private {mode}-profile bootstrap") from exc
    collector = _prepare_snapshot_collector(directory)
    return DeepProfileSession(directory, (status.st_dev, status.st_ino), mode, collector)


def prepare_deep_profile_session(parent: Path) -> DeepProfileSession:
    return _prepare_profile_session(parent, "deep")


def prepare_sample_profile_session(parent: Path) -> DeepProfileSession:
    return _prepare_profile_session(parent, "sample")


def _reject_json_constant(value: str) -> Never:
    raise ValueError(f"non-finite constant {value}")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise DeepProfileError(f"{label} must be an object")
    return {str(key): item for key, item in value.items()}


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise DeepProfileError(f"{label} must be an array")
    return list(value)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DeepProfileError(f"{label} must be a non-empty string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise DeepProfileError(f"{label} must be valid UTF-8") from exc
    if len(encoded) > 4_096:
        raise DeepProfileError(f"{label} exceeds the 4096-byte limit")
    return value


def _integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _MAX_INTEGER:
        raise DeepProfileError(f"{label} must be a bounded non-negative integer")
    return value


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise DeepProfileError(f"{label} must be a boolean")
    return value


def _optional_boolean(value: object, label: str) -> bool | None:
    if value is None:
        return None
    return _boolean(value, label)


def profile_session_checkpoint(session: DeepProfileSession) -> dict[str, JsonValue]:
    """Freeze and describe one profile session for post-exit recovery."""
    session.finish_collection()
    try:
        directory = session.directory.resolve(strict=True)
        status = directory.stat(follow_symlinks=False)
    except (OSError, RuntimeError) as exc:
        raise DeepProfileError("could not checkpoint the private profile session") from exc
    if not stat.S_ISDIR(status.st_mode) or (status.st_dev, status.st_ino) != session.identity:
        raise DeepProfileError("private profile session changed before checkpointing")
    transport = _snapshot_transport_stats(session, 0)
    return {
        "format_version": PROFILE_SESSION_CHECKPOINT_VERSION,
        "directory": str(directory),
        "directory_device": status.st_dev,
        "directory_inode": status.st_ino,
        "mode": session.mode,
        "collector_error_count": session.collector_error_count,
        "transport": {
            "dropped_profile_process_count": transport.dropped_profile_process_count,
            "dropped_profile_process_count_truncated": (
                transport.dropped_profile_process_count_truncated
            ),
            "metrics_status": transport.metrics_status,
            "message_count": transport.message_count,
            "payload_bytes": transport.payload_bytes,
            "max_payload_bytes": transport.max_payload_bytes,
            "serialization_ns": transport.serialization_ns,
            "max_serialization_ns": transport.max_serialization_ns,
            "checkpoint_message_count": transport.checkpoint_message_count,
            "checkpoint_payload_bytes": transport.checkpoint_payload_bytes,
            "max_checkpoint_payload_bytes": transport.max_checkpoint_payload_bytes,
            "checkpoint_serialization_ns": transport.checkpoint_serialization_ns,
            "max_checkpoint_serialization_ns": transport.max_checkpoint_serialization_ns,
        },
    }


def recover_profile_session(value: object) -> DeepProfileSession:
    """Validate and reopen one frozen profile session without a live collector."""
    checkpoint = _object(value, "profile session checkpoint")
    if checkpoint.get("format_version") != PROFILE_SESSION_CHECKPOINT_VERSION:
        raise DeepProfileError("profile session checkpoint version is unsupported")
    raw_mode = checkpoint.get("mode")
    if raw_mode == "sample":
        mode: PythonProfileMode = "sample"
    elif raw_mode == "deep":
        mode = "deep"
    else:
        raise DeepProfileError("profile session checkpoint mode is unsupported")
    raw_directory = _text(checkpoint.get("directory"), "profile session checkpoint directory")
    directory = Path(raw_directory)
    if not directory.is_absolute():
        raise DeepProfileError("profile session checkpoint directory must be absolute")
    device = _integer(
        checkpoint.get("directory_device"),
        "profile session checkpoint directory device",
    )
    inode = _integer(
        checkpoint.get("directory_inode"),
        "profile session checkpoint directory inode",
    )
    try:
        status = directory.stat(follow_symlinks=False)
    except OSError as exc:
        raise DeepProfileError("profile session checkpoint directory is unavailable") from exc
    if not stat.S_ISDIR(status.st_mode) or (status.st_dev, status.st_ino) != (device, inode):
        raise DeepProfileError("profile session checkpoint directory identity changed")

    collector_error_count = _integer(
        checkpoint.get("collector_error_count"),
        "profile session checkpoint collector error count",
    )
    raw_transport = _object(
        checkpoint.get("transport"),
        "profile session checkpoint transport",
    )
    raw_metrics_status = raw_transport.get("metrics_status")
    if raw_metrics_status == "available":
        metrics_status: Literal["available", "unavailable"] = "available"
    elif raw_metrics_status == "unavailable":
        metrics_status = "unavailable"
    else:
        raise DeepProfileError("profile session checkpoint transport status is unsupported")
    dropped_count = _integer(
        raw_transport.get("dropped_profile_process_count"),
        "profile session checkpoint dropped process count",
    )
    dropped_truncated = _boolean(
        raw_transport.get("dropped_profile_process_count_truncated"),
        "profile session checkpoint dropped process truncation",
    )
    integer_fields = (
        "message_count",
        "payload_bytes",
        "max_payload_bytes",
        "serialization_ns",
        "max_serialization_ns",
        "checkpoint_message_count",
        "checkpoint_payload_bytes",
        "max_checkpoint_payload_bytes",
        "checkpoint_serialization_ns",
        "max_checkpoint_serialization_ns",
    )
    counts = {
        field: _integer(
            raw_transport.get(field),
            f"profile session checkpoint {field.replace('_', ' ')}",
        )
        for field in integer_fields
    }
    if (
        dropped_count > MAX_DEEP_PROFILE_DIRECTORY_ENTRIES
        or counts["message_count"] > MAX_PROFILE_SNAPSHOT_MESSAGES
        or counts["max_payload_bytes"] > MAX_DEEP_PROFILE_FILE_BYTES
        or counts["max_payload_bytes"] > counts["payload_bytes"]
        or counts["max_serialization_ns"] > MAX_PROFILE_SNAPSHOT_SERIALIZATION_NS
        or counts["max_serialization_ns"] > counts["serialization_ns"]
        or counts["checkpoint_message_count"] > counts["message_count"]
        or counts["checkpoint_payload_bytes"] > counts["payload_bytes"]
        or counts["max_checkpoint_payload_bytes"] > counts["checkpoint_payload_bytes"]
        or counts["max_checkpoint_payload_bytes"] > counts["max_payload_bytes"]
        or counts["checkpoint_serialization_ns"] > counts["serialization_ns"]
        or counts["max_checkpoint_serialization_ns"] > counts["checkpoint_serialization_ns"]
        or counts["max_checkpoint_serialization_ns"] > counts["max_serialization_ns"]
    ):
        raise DeepProfileError("profile session checkpoint transport metrics are inconsistent")
    if metrics_status == "unavailable" and (
        collector_error_count != 0
        or dropped_count != 0
        or dropped_truncated
        or any(counts.values())
    ):
        raise DeepProfileError("unavailable profile session checkpoint has transport metrics")
    transport = _SnapshotTransportStats(
        dropped_count,
        dropped_truncated,
        metrics_status,
        counts["message_count"],
        counts["payload_bytes"],
        counts["max_payload_bytes"],
        counts["serialization_ns"],
        counts["max_serialization_ns"],
        counts["checkpoint_message_count"],
        counts["checkpoint_payload_bytes"],
        counts["max_checkpoint_payload_bytes"],
        counts["checkpoint_serialization_ns"],
        counts["max_checkpoint_serialization_ns"],
    )
    return DeepProfileSession(
        directory,
        (device, inode),
        mode,
        recovered_transport_stats=transport,
        recovered_collector_error_count=collector_error_count,
    )


def _snapshot_kind(document: dict[str, object], label: str) -> Literal["checkpoint", "final"]:
    value = document.get("snapshot_kind", "final")
    if "snapshot_kind" in document:
        interval_ns = _integer(
            document.get("checkpoint_interval_ns"),
            f"{label} checkpoint interval",
        )
        if interval_ns != PROFILE_CHECKPOINT_INTERVAL_NS:
            raise DeepProfileError(f"{label} checkpoint interval is unsupported")
        first_delay_ns = document.get("first_checkpoint_delay_ns")
        if first_delay_ns is not None and (
            _integer(first_delay_ns, f"{label} first checkpoint delay")
            != PROFILE_FIRST_CHECKPOINT_DELAY_NS
        ):
            raise DeepProfileError(f"{label} first checkpoint delay is unsupported")
    if value == "checkpoint":
        return "checkpoint"
    if value == "final":
        return "final"
    raise DeepProfileError(f"{label} snapshot kind is unsupported")


def _registration_only(
    document: dict[str, object],
    label: str,
    snapshot_kind: Literal["checkpoint", "final"],
) -> bool:
    value = _boolean(document.get("registration_only", False), f"{label} registration marker")
    if value and snapshot_kind != "checkpoint":
        raise DeepProfileError(f"{label} final report cannot be registration-only")
    return value


def _bounded_sum(left: int, right: int, label: str) -> int:
    result = left + right
    if result > _MAX_INTEGER:
        raise DeepProfileError(f"{label} exceeds the supported integer range")
    return result


def _profile_files(
    session: DeepProfileSession,
    root_process_id: int | None,
) -> tuple[tuple[Path, ...], int]:
    candidates: list[tuple[Path, int]] = []
    try:
        entries = session.directory.iterdir()
        for entry_count, path in enumerate(entries, 1):
            if entry_count > MAX_DEEP_PROFILE_DIRECTORY_ENTRIES:
                raise DeepProfileError(
                    "generated deep-profile directory exceeds its entry-count limit"
                )
            if not path.name.startswith("profile-") or not path.name.endswith(".json"):
                continue
            status = path.stat(follow_symlinks=False)
            if not stat.S_ISREG(status.st_mode):
                raise DeepProfileError("generated deep-profile evidence must be regular files")
            if status.st_size > MAX_DEEP_PROFILE_FILE_BYTES:
                raise DeepProfileError("generated deep-profile evidence exceeds its per-file limit")
            candidates.append((path, status.st_size))
    except DeepProfileError:
        raise
    except OSError as exc:
        raise DeepProfileError("could not inspect generated deep-profile evidence") from exc
    root_name = f"profile-{root_process_id}.json" if root_process_id is not None else None
    candidates.sort(key=lambda item: (item[0].name != root_name, item[0].name))
    selected = candidates[:MAX_DEEP_PROFILE_FILES]
    if sum(size for _, size in selected) > MAX_DEEP_PROFILE_TOTAL_BYTES:
        raise DeepProfileError("generated deep-profile evidence exceeds its aggregate limit")
    return tuple(path for path, _ in selected), max(0, len(candidates) - len(selected))


def _read_profile_file(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode):
            raise DeepProfileError("generated deep-profile evidence must be regular files")
        if status.st_size > MAX_DEEP_PROFILE_FILE_BYTES:
            raise DeepProfileError("generated deep-profile evidence exceeds its per-file limit")
        chunks: list[bytes] = []
        byte_count = 0
        while True:
            chunk = os.read(
                descriptor, min(64 * 1024, MAX_DEEP_PROFILE_FILE_BYTES + 1 - byte_count)
            )
            if not chunk:
                break
            chunks.append(chunk)
            byte_count += len(chunk)
            if byte_count > MAX_DEEP_PROFILE_FILE_BYTES:
                raise DeepProfileError("generated deep-profile evidence exceeds its per-file limit")
        return b"".join(chunks)
    except DeepProfileError:
        raise
    except OSError as exc:
        raise DeepProfileError("could not read generated deep-profile evidence") from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _parse_document(raw: bytes) -> dict[str, object]:
    try:
        value = json.loads(
            raw,
            parse_constant=_reject_json_constant,
            object_pairs_hook=reject_duplicate_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise DeepProfileError("generated deep-profile evidence is invalid JSON") from exc
    return _object(value, "deep-profile document")


def _observer_integrity(document: dict[str, object]) -> tuple[int, int] | None:
    raw_integrity = document.get("observer_integrity")
    if raw_integrity is None:
        return None
    integrity = _object(raw_integrity, "deep-profile observer integrity")
    if (
        _integer(
            integrity.get("format_version"),
            "deep-profile observer integrity format version",
        )
        != OBSERVER_INTEGRITY_VERSION
    ):
        raise DeepProfileError("deep-profile observer integrity format version is unsupported")
    return (
        _integer(
            integrity.get("profile_hook_setter_call_count"),
            "deep-profile profile-hook setter call count",
        ),
        _integer(
            integrity.get("trace_hook_setter_call_count"),
            "deep-profile trace-hook setter call count",
        ),
    )


def _python_exception_filter(document: dict[str, object]) -> bool:
    raw_filter = document.get("python_exception_filter")
    if raw_filter is None:
        return False
    exception_filter = _object(raw_filter, "deep-profile Python exception filter")
    if (
        _integer(
            exception_filter.get("format_version"),
            "deep-profile Python exception filter format version",
        )
        != PYTHON_EXCEPTION_FILTER_VERSION
    ):
        raise DeepProfileError("deep-profile Python exception filter format version is unsupported")
    filtered_types = _list(
        exception_filter.get("filtered_exception_types"),
        "deep-profile filtered exception types",
    )
    if (
        exception_filter.get("event_semantics") != "exact_type_identity"
        or exception_filter.get("exception_type_identity_inspected") is not True
        or exception_filter.get("exception_types_captured") is not False
        or tuple(filtered_types) != FILTERED_CONTROL_FLOW_EXCEPTION_TYPES
    ):
        raise DeepProfileError("deep-profile Python exception filter metadata is invalid")
    return True


def _profile_payloads(
    session: DeepProfileSession,
    root_process_id: int | None,
) -> tuple[tuple[bytes, ...], int]:
    files, dropped_file_count = _profile_files(session, root_process_id)
    payloads: list[bytes] = []
    total_bytes = 0
    for path in files:
        payload = _read_profile_file(path)
        total_bytes += len(payload)
        if total_bytes > MAX_DEEP_PROFILE_TOTAL_BYTES:
            raise DeepProfileError("generated deep-profile evidence exceeds its aggregate limit")
        payloads.append(payload)
    return tuple(payloads), dropped_file_count


def _optional_signed_integer(value: object, label: str) -> int | None:
    if value is None:
        return None
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not -_MAX_INTEGER <= value <= _MAX_INTEGER
    ):
        raise DeepProfileError(f"{label} must be a bounded integer or null")
    return value


def _optional_nonnegative_integer(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _integer(value, label)


def _subprocess_caller(value: object) -> _SubprocessCaller:
    item = _object(value, "semantic subprocess caller")
    scope = _text(item.get("scope"), "semantic subprocess caller scope")
    if scope not in {"application", "library", "runtime"}:
        raise DeepProfileError("semantic subprocess caller scope is unsupported")
    raw_observation = item.get("observation")
    observation: CallerObservation
    if raw_observation == "exact":
        observation = "exact"
    elif raw_observation == "sampled":
        observation = "sampled"
    else:
        raise DeepProfileError("semantic subprocess caller observation is unsupported")
    return _SubprocessCaller(
        _FunctionIdentity(
            _text(item.get("module"), "semantic subprocess caller module"),
            _text(item.get("qualname"), "semantic subprocess caller qualified name"),
            _text(item.get("filename"), "semantic subprocess caller filename"),
            _integer(item.get("firstlineno"), "semantic subprocess caller first line"),
            scope,
        ),
        observation,
    )


def _subprocess_record(value: object, *, document_pid: int) -> _SubprocessRecord:
    item = _object(value, "semantic subprocess record")
    identifier = _integer(item.get("id"), "semantic subprocess id")
    name = _text(item.get("name"), "semantic subprocess executable")
    if len(name) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic subprocess executable exceeds its character limit")
    parent_pid = _integer(item.get("parent_pid"), "semantic subprocess parent process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic subprocess parent process id is inconsistent")
    child_pid = _optional_nonnegative_integer(
        item.get("child_pid"),
        "semantic subprocess child process id",
    )
    if child_pid is not None and child_pid <= 0:
        raise DeepProfileError("semantic subprocess child process id must be positive")
    shell = _optional_boolean(item.get("shell"), "semantic subprocess shell marker")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic subprocess start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic subprocess start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic subprocess duration",
    )
    exit_code = _optional_signed_integer(
        item.get("exit_code"),
        "semantic subprocess exit code",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["exited", "launch_error", "unknown"]
    if raw_outcome == "exited":
        outcome = "exited"
    elif raw_outcome == "launch_error":
        outcome = "launch_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic subprocess outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic subprocess launch error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic subprocess launch error type exceeds its character limit")
    if outcome == "exited" and (
        child_pid is None or duration_ns is None or exit_code is None or error_type is not None
    ):
        raise DeepProfileError("exited semantic subprocess evidence is incomplete")
    if outcome == "launch_error" and (
        child_pid is not None or duration_ns is None or exit_code is not None or error_type is None
    ):
        raise DeepProfileError("failed semantic subprocess launch evidence is inconsistent")
    if outcome == "unknown" and (
        child_pid is None
        or duration_ns is not None
        or exit_code is not None
        or error_type is not None
    ):
        raise DeepProfileError("unfinished semantic subprocess evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic subprocess finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _SubprocessRecord(
        identifier,
        name,
        parent_pid,
        child_pid,
        shell,
        started_at_ns,
        duration_ns,
        exit_code,
        outcome,
        error_type,
        caller,
        caller_status,
    )


def _semantic_event(
    record: _SubprocessRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-subprocess-wrapper",
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "shell": record.shell,
        "outcome": record.outcome,
        "arguments_captured": False,
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _semantic_caller_event_id(record.caller.identity)
    if record.child_pid is not None:
        attributes["child_pid"] = record.child_pid
    if record.exit_code is not None:
        attributes["exit_code"] = record.exit_code
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    elif record.exit_code is not None and record.exit_code != 0:
        attributes["error"] = True
        attributes["error.type"] = "subprocess_exit"
    return Event(
        id=f"semantic:subprocess:{record.parent_pid}:{record.identifier}",
        kind="subprocess.run",
        name=record.name,
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _semantic_caller_event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"semantic:caller:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _semantic_caller_event(
    identity: _FunctionIdentity,
    *,
    subprocess_count: int,
    entity_id: str,
) -> Event:
    return Event(
        id=_semantic_caller_event_id(identity),
        kind="python.callsite",
        name=identity.name,
        entity_id=entity_id,
        started_at_ns=None,
        finished_at_ns=None,
        clock_domain=None,
        uncertainty_ns=None,
        sequence=None,
        attributes={
            "source": "python-subprocess-wrapper",
            "scope": identity.scope,
            "module": identity.module,
            "qualname": identity.qualname,
            "filename": identity.filename,
            "firstlineno": identity.firstlineno,
            "subprocess_count": subprocess_count,
            "arguments_captured": False,
            "locals_captured": False,
        },
    )


def _semantic_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _SemanticCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _SubprocessRecord]] = []
    semantic_process_count = 0
    dropped_subprocess_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_subprocess_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_unknown = False
    saw_unknown_identity = False
    saw_legacy_caller_capture = False
    invalid = False
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            semantic_version = semantic.get("format_version")
            if semantic_version not in {1, 2}:
                raise DeepProfileError("semantic capture format version is unsupported")
            expected_observer = (
                "python-subprocess-wrapper"
                if semantic_version == 1
                else "python-runtime-boundary-wrapper"
            )
            if semantic.get("observer") != expected_observer:
                raise DeepProfileError("semantic capture observer is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_subprocesses"),
                    "semantic capture subprocess limit",
                )
                != MAX_SEMANTIC_SUBPROCESS_PER_PROCESS
            ):
                raise DeepProfileError("semantic capture subprocess limit is unsupported")
            pid = _integer(document.get("pid"), "semantic capture process id")
            raw_records = _list(semantic.get("subprocesses"), "semantic subprocesses")
            subprocess_count = _integer(
                semantic.get("subprocess_count"),
                "semantic capture subprocess count",
            )
            if (
                subprocess_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_SUBPROCESS_PER_PROCESS
            ):
                raise DeepProfileError("semantic capture subprocess count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_subprocess_count"),
                "semantic capture dropped subprocess count",
            )
            document_callback_errors = _integer(
                semantic.get("callback_error_count"),
                "semantic capture callback error count",
            )
            if "caller_callback_error_count" in semantic:
                document_caller_callback_errors = _integer(
                    semantic.get("caller_callback_error_count"),
                    "semantic caller callback error count",
                )
            else:
                document_caller_callback_errors = 0
                saw_legacy_caller_capture = True
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic capture registration contains evidence")
            records = tuple(
                _subprocess_record(raw_record, document_pid=pid) for raw_record in raw_records
            )
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic subprocess ids must be unique per process")
                identifiers.add(record.identifier)
            semantic_process_count += 1
            dropped_subprocess_count = _bounded_sum(
                dropped_subprocess_count,
                document_dropped_count,
                "semantic capture dropped subprocess count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic capture callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_subprocess_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_unknown = saw_unknown or record.outcome == "unknown"
                saw_unknown_identity = (
                    saw_unknown_identity
                    or record.shell is None
                    or record.name
                    in {
                        "<command>",
                        "<unknown>",
                    }
                )
                important = int(
                    record.outcome != "exited"
                    or (record.exit_code is not None and record.exit_code != 0)
                )
                rank = (
                    important,
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_SUBPROCESS_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _SemanticCaptureEvidence(
            (),
            (),
            (),
            "invalid",
            semantic_process_count,
            0,
            0,
            "invalid",
            0,
            0,
            invalid_caller_count,
            caller_callback_error_count,
        )
    if semantic_process_count == 0:
        return _SemanticCaptureEvidence(
            (),
            (),
            (),
            "unavailable",
            0,
            0,
            0,
            "unavailable",
            0,
            0,
            0,
            0,
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    selection_dropped = valid_subprocess_count - len(selected_records)
    dropped_subprocess_count = _bounded_sum(
        dropped_subprocess_count,
        selection_dropped,
        "semantic capture dropped subprocess count",
    )
    status: SemanticCaptureStatus
    if saw_missing or saw_checkpoint or saw_unknown or saw_unknown_identity:
        status = "partial"
    elif dropped_subprocess_count or callback_error_count:
        status = "truncated"
    else:
        status = "complete"
    attributed_subprocess_count = sum(record.caller is not None for record in selected_records)
    unattributed_subprocess_count = len(selected_records) - attributed_subprocess_count
    caller_counts: dict[_FunctionIdentity, int] = {}
    for record in selected_records:
        if record.caller is not None:
            identity = record.caller.identity
            caller_counts[identity] = caller_counts.get(identity, 0) + 1
    caller_events = tuple(
        _semantic_caller_event(
            identity,
            subprocess_count=count,
            entity_id=entity_id,
        )
        for identity, count in sorted(
            caller_counts.items(),
            key=lambda item: (
                item[0].name,
                item[0].filename,
                item[0].firstlineno,
                item[0].scope,
            ),
        )
    )
    caller_edges = tuple(
        CausalEdge(
            _semantic_caller_event_id(record.caller.identity),
            f"semantic:subprocess:{record.parent_pid}:{record.identifier}",
            "launches",
            1.0 if record.caller.observation == "exact" else 0.9,
            {
                "source": "python-subprocess-wrapper",
                "observation": record.caller.observation,
            },
        )
        for record in selected_records
        if record.caller is not None
    )
    caller_attribution_status: SemanticCaptureStatus
    if invalid_caller_count:
        caller_attribution_status = "invalid"
    elif saw_legacy_caller_capture:
        caller_attribution_status = "partial" if attributed_subprocess_count else "unavailable"
    elif (
        saw_missing
        or saw_checkpoint
        or unattributed_subprocess_count
        or caller_callback_error_count
    ):
        caller_attribution_status = "partial"
    elif dropped_subprocess_count:
        caller_attribution_status = "truncated"
    else:
        caller_attribution_status = "complete"
    return _SemanticCaptureEvidence(
        tuple(
            _semantic_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        semantic_process_count,
        dropped_subprocess_count,
        callback_error_count,
        caller_attribution_status,
        attributed_subprocess_count,
        unattributed_subprocess_count,
        invalid_caller_count,
        caller_callback_error_count,
    )


def _http_request_record(value: object, *, document_pid: int) -> _HttpRequestRecord:
    item = _object(value, "semantic HTTP request record")
    identifier = _integer(item.get("id"), "semantic HTTP request id")
    method = _text(item.get("method"), "semantic HTTP method")
    if len(method) > MAX_SEMANTIC_HTTP_METHOD_CHARACTERS or (
        method != "<method>"
        and (
            not method.isascii()
            or not method
            or not all(character in "ABCDEFGHIJKLMNOPQRSTUVWXYZ-" for character in method)
        )
    ):
        raise DeepProfileError("semantic HTTP method is unsupported")
    raw_scheme = item.get("scheme")
    scheme: Literal["http", "https"]
    if raw_scheme == "http":
        scheme = "http"
    elif raw_scheme == "https":
        scheme = "https"
    else:
        raise DeepProfileError("semantic HTTP scheme is unsupported")
    if item.get("server_address") is not None or item.get("server_identity_policy") != "redact":
        raise DeepProfileError("semantic HTTP server identity is not redacted")
    adapter = item.get("adapter", "stdlib.http.client")
    if not isinstance(adapter, str) or adapter not in SEMANTIC_HTTP_ADAPTERS:
        raise DeepProfileError("semantic HTTP adapter is unsupported")
    server_port = _optional_nonnegative_integer(
        item.get("server_port"),
        "semantic HTTP server port",
    )
    if server_port is not None and not 0 < server_port <= 65_535:
        raise DeepProfileError("semantic HTTP server port is invalid")
    parent_pid = _integer(item.get("parent_pid"), "semantic HTTP parent process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic HTTP parent process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic HTTP request start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic HTTP request start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic HTTP request duration",
    )
    status_code = _optional_nonnegative_integer(
        item.get("status_code"),
        "semantic HTTP response status",
    )
    if status_code is not None and not 100 <= status_code <= 999:
        raise DeepProfileError("semantic HTTP response status is invalid")
    raw_outcome = item.get("outcome")
    outcome: Literal["response", "request_error", "closed", "unknown"]
    if raw_outcome == "response":
        outcome = "response"
    elif raw_outcome == "request_error":
        outcome = "request_error"
    elif raw_outcome == "closed":
        outcome = "closed"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic HTTP request outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic HTTP request error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic HTTP request error type exceeds its character limit")
    if outcome == "response" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("semantic HTTP response evidence is inconsistent")
    if outcome == "request_error" and (
        duration_ns is None or status_code is not None or error_type is None
    ):
        raise DeepProfileError("failed semantic HTTP request evidence is inconsistent")
    if outcome == "closed" and (
        duration_ns is None or status_code is not None or error_type is not None
    ):
        raise DeepProfileError("closed semantic HTTP request evidence is inconsistent")
    if outcome == "unknown" and (
        duration_ns is not None or status_code is not None or error_type is not None
    ):
        raise DeepProfileError("unfinished semantic HTTP request evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic HTTP request finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _HttpRequestRecord(
        identifier,
        method,
        scheme,
        server_port,
        parent_pid,
        started_at_ns,
        duration_ns,
        status_code,
        outcome,
        error_type,
        caller,
        caller_status,
        adapter,
    )


def _http_request_event_id(record: _HttpRequestRecord) -> str:
    return f"semantic:http:{record.parent_pid}:{record.identifier}"


def _http_request_event(
    record: _HttpRequestRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-http-client-wrapper",
        "adapter": record.adapter,
        "method": record.method,
        "scheme": record.scheme,
        "server_address": None,
        "server_port": record.server_port,
        "server_identity_policy": "redact",
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "headers_captured": False,
        "body_captured": False,
        "path_captured": False,
        "query_captured": False,
        "response_body_captured": False,
        "duration_boundary": "response_headers",
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _http_caller_event_id(record.caller.identity)
    if record.status_code is not None:
        attributes["status_code"] = record.status_code
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_http_request_event_id(record),
        kind="http.client.request",
        name=f"HTTP {record.method}",
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _http_caller_event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"semantic:http-caller:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _http_caller_event(
    identity: _FunctionIdentity,
    *,
    request_count: int,
    entity_id: str,
) -> Event:
    return Event(
        id=_http_caller_event_id(identity),
        kind="python.callsite",
        name=identity.name,
        entity_id=entity_id,
        started_at_ns=None,
        finished_at_ns=None,
        clock_domain=None,
        uncertainty_ns=None,
        sequence=None,
        attributes={
            "source": "python-http-client-wrapper",
            "scope": identity.scope,
            "module": identity.module,
            "qualname": identity.qualname,
            "filename": identity.filename,
            "firstlineno": identity.firstlineno,
            "http_request_count": request_count,
            "arguments_captured": False,
            "locals_captured": False,
        },
    )


def _http_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _HttpCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _HttpRequestRecord]] = []
    process_count = 0
    dropped_request_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_request_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if semantic.get("format_version") == 1:
                saw_missing = True
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic HTTP capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_http_requests"),
                    "semantic HTTP request limit",
                )
                != MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS
            ):
                raise DeepProfileError("semantic HTTP request limit is unsupported")
            if semantic.get("server_identity_policy") != "redact":
                raise DeepProfileError("semantic HTTP server identity policy is unsupported")
            raw_adapters = _list(
                semantic.get("http_adapters", ["stdlib.http.client"]),
                "semantic HTTP adapters",
            )
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic HTTP adapter")
                if adapter not in SEMANTIC_HTTP_ADAPTERS or adapter in document_adapters:
                    raise DeepProfileError("semantic HTTP adapters are unsupported")
                document_adapters.add(adapter)
            if "stdlib.http.client" not in document_adapters:
                raise DeepProfileError("semantic HTTP standard-library adapter is missing")
            pid = _integer(document.get("pid"), "semantic HTTP capture process id")
            raw_records = _list(semantic.get("http_requests"), "semantic HTTP requests")
            declared_count = _integer(
                semantic.get("http_request_count"),
                "semantic HTTP request count",
            )
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS
            ):
                raise DeepProfileError("semantic HTTP request count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_http_request_count"),
                "semantic dropped HTTP request count",
            )
            document_callback_errors = _integer(
                semantic.get("http_callback_error_count"),
                "semantic HTTP callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("http_caller_callback_error_count"),
                "semantic HTTP caller callback error count",
            )
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic HTTP registration contains evidence")
            records = tuple(
                _http_request_record(raw_record, document_pid=pid) for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic HTTP request adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic HTTP ids must be unique per process")
                identifiers.add(record.identifier)
            process_count += 1
            dropped_request_count = _bounded_sum(
                dropped_request_count,
                document_dropped_count,
                "semantic dropped HTTP request count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic HTTP callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic HTTP caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_request_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = saw_incomplete or (
                    record.outcome in {"closed", "unknown"}
                    or record.method == "<method>"
                    or (record.outcome == "response" and record.status_code is None)
                )
                important = int(
                    record.outcome != "response"
                    or (record.status_code is not None and record.status_code >= 500)
                )
                rank = (
                    important,
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_HTTP_REQUEST_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _HttpCaptureEvidence(
            (),
            (),
            (),
            "invalid",
            process_count,
            0,
            0,
            "invalid",
            0,
            0,
            invalid_caller_count,
            0,
            (),
        )
    if process_count == 0:
        return _HttpCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_request_count = _bounded_sum(
        dropped_request_count,
        valid_request_count - len(selected_records),
        "semantic dropped HTTP request count",
    )
    status: SemanticCaptureStatus
    if saw_missing or saw_checkpoint or saw_incomplete:
        status = "partial"
    elif dropped_request_count or callback_error_count:
        status = "truncated"
    else:
        status = "complete"
    attributed_request_count = sum(record.caller is not None for record in selected_records)
    unattributed_request_count = len(selected_records) - attributed_request_count
    caller_counts: dict[_FunctionIdentity, int] = {}
    for record in selected_records:
        if record.caller is not None:
            identity = record.caller.identity
            caller_counts[identity] = caller_counts.get(identity, 0) + 1
    caller_events = tuple(
        _http_caller_event(identity, request_count=count, entity_id=entity_id)
        for identity, count in sorted(
            caller_counts.items(),
            key=lambda item: (
                item[0].name,
                item[0].filename,
                item[0].firstlineno,
                item[0].scope,
            ),
        )
    )
    caller_edges = tuple(
        CausalEdge(
            _http_caller_event_id(record.caller.identity),
            _http_request_event_id(record),
            "requests",
            1.0 if record.caller.observation == "exact" else 0.9,
            {
                "source": "python-http-client-wrapper",
                "observation": record.caller.observation,
            },
        )
        for record in selected_records
        if record.caller is not None
    )
    caller_status: SemanticCaptureStatus
    if invalid_caller_count:
        caller_status = "invalid"
    elif saw_missing or saw_checkpoint or unattributed_request_count or caller_callback_error_count:
        caller_status = "partial"
    elif dropped_request_count:
        caller_status = "truncated"
    else:
        caller_status = "complete"
    return _HttpCaptureEvidence(
        tuple(
            _http_request_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_request_count,
        callback_error_count,
        caller_status,
        attributed_request_count,
        unattributed_request_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )


def _network_connection_record(
    value: object,
    *,
    document_pid: int,
) -> _NetworkConnectionRecord:
    item = _object(value, "semantic network connection record")
    identifier = _integer(item.get("id"), "semantic network connection id")
    adapter = _text(item.get("adapter"), "semantic network adapter")
    if adapter not in SEMANTIC_NETWORK_ADAPTERS:
        raise DeepProfileError("semantic network adapter is unsupported")
    raw_transport = item.get("transport")
    transport: Literal["tcp", "unix"]
    if raw_transport == "tcp":
        transport = "tcp"
    elif raw_transport == "unix":
        transport = "unix"
    else:
        raise DeepProfileError("semantic network transport is unsupported")
    raw_family = item.get("address_family")
    address_family: Literal["ipv4", "ipv6", "unix", "unknown"]
    if raw_family == "ipv4":
        address_family = "ipv4"
    elif raw_family == "ipv6":
        address_family = "ipv6"
    elif raw_family == "unix":
        address_family = "unix"
    elif raw_family == "unknown":
        address_family = "unknown"
    else:
        raise DeepProfileError("semantic network address family is unsupported")
    if (transport == "unix") != (address_family == "unix"):
        raise DeepProfileError("semantic network transport and address family are inconsistent")
    if item.get("server_address") is not None or item.get("server_identity_policy") != "redact":
        raise DeepProfileError("semantic network server identity is not redacted")
    server_port = _optional_nonnegative_integer(
        item.get("server_port"),
        "semantic network server port",
    )
    if server_port is not None and not 0 < server_port <= 65_535:
        raise DeepProfileError("semantic network server port is invalid")
    if transport == "unix" and server_port is not None:
        raise DeepProfileError("semantic Unix connection cannot contain a server port")
    tls_requested = _optional_boolean(
        item.get("tls_requested"),
        "semantic network TLS marker",
    )
    parent_pid = _integer(item.get("parent_pid"), "semantic network parent process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic network parent process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic network connection start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic network connection start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic network connection duration",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["connected", "connect_error", "unknown"]
    if raw_outcome == "connected":
        outcome = "connected"
    elif raw_outcome == "connect_error":
        outcome = "connect_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic network connection outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic network connection error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic network error type exceeds its character limit")
    if outcome == "connected" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("successful semantic network evidence is inconsistent")
    if outcome == "connect_error" and (duration_ns is None or error_type is None):
        raise DeepProfileError("failed semantic network evidence is inconsistent")
    if outcome == "unknown" and (duration_ns is not None or error_type is not None):
        raise DeepProfileError("unfinished semantic network evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic network connection finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _NetworkConnectionRecord(
        identifier,
        adapter,
        transport,
        address_family,
        server_port,
        tls_requested,
        parent_pid,
        started_at_ns,
        duration_ns,
        outcome,
        error_type,
        caller,
        caller_status,
    )


def _network_connection_event_id(record: _NetworkConnectionRecord) -> str:
    return f"semantic:network:{record.parent_pid}:{record.identifier}"


def _network_caller_event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"semantic:network-caller:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _network_connection_event(
    record: _NetworkConnectionRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-network-connection-wrapper",
        "adapter": record.adapter,
        "transport": record.transport,
        "address_family": record.address_family,
        "server_address": None,
        "server_port": record.server_port,
        "server_identity_policy": "redact",
        "tls_requested": record.tls_requested,
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "path_captured": False,
        "credentials_captured": False,
        "duration_boundary": "connection_ready",
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _network_caller_event_id(record.caller.identity)
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_network_connection_event_id(record),
        kind="network.connect",
        name=f"{record.transport.upper()} connect",
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _network_caller_event(
    identity: _FunctionIdentity,
    *,
    connection_count: int,
    entity_id: str,
) -> Event:
    return Event(
        id=_network_caller_event_id(identity),
        kind="python.callsite",
        name=identity.name,
        entity_id=entity_id,
        started_at_ns=None,
        finished_at_ns=None,
        clock_domain=None,
        uncertainty_ns=None,
        sequence=None,
        attributes={
            "source": "python-network-connection-wrapper",
            "scope": identity.scope,
            "module": identity.module,
            "qualname": identity.qualname,
            "filename": identity.filename,
            "firstlineno": identity.firstlineno,
            "network_connection_count": connection_count,
            "arguments_captured": False,
            "locals_captured": False,
        },
    )


def _network_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _NetworkCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _NetworkConnectionRecord]] = []
    process_count = 0
    dropped_connection_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_connection_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if semantic.get("format_version") == 1 or "network_connections" not in semantic:
                saw_missing = True
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic network capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_network_connections"),
                    "semantic network connection limit",
                )
                != MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic network connection limit is unsupported")
            if semantic.get("server_identity_policy") != "redact":
                raise DeepProfileError("semantic network server identity policy is unsupported")
            raw_adapters = _list(
                semantic.get("network_adapters", ["stdlib.socket.connect"]),
                "semantic network adapters",
            )
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic network adapter")
                if adapter not in SEMANTIC_NETWORK_ADAPTERS or adapter in document_adapters:
                    raise DeepProfileError("semantic network adapters are unsupported")
                document_adapters.add(adapter)
            if "stdlib.socket.connect" not in document_adapters:
                raise DeepProfileError("semantic network standard-library adapter is missing")
            pid = _integer(document.get("pid"), "semantic network capture process id")
            raw_records = _list(
                semantic.get("network_connections"),
                "semantic network connections",
            )
            declared_count = _integer(
                semantic.get("network_connection_count"),
                "semantic network connection count",
            )
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic network connection count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_network_connection_count"),
                "semantic dropped network connection count",
            )
            document_callback_errors = _integer(
                semantic.get("network_callback_error_count"),
                "semantic network callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("network_caller_callback_error_count"),
                "semantic network caller callback error count",
            )
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic network registration contains evidence")
            records = tuple(
                _network_connection_record(raw_record, document_pid=pid)
                for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic network connection adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic network ids must be unique per process")
                identifiers.add(record.identifier)
            process_count += 1
            dropped_connection_count = _bounded_sum(
                dropped_connection_count,
                document_dropped_count,
                "semantic dropped network connection count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic network callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic network caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_connection_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = saw_incomplete or record.outcome == "unknown"
                rank = (
                    int(record.outcome != "connected"),
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _NetworkCaptureEvidence(
            (), (), (), "invalid", process_count, 0, 0, "invalid", 0, 0, 0, 0, ()
        )
    if process_count == 0:
        return _NetworkCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_connection_count = _bounded_sum(
        dropped_connection_count,
        valid_connection_count - len(selected_records),
        "semantic dropped network connection count",
    )
    status: SemanticCaptureStatus
    if saw_missing or saw_checkpoint or saw_incomplete:
        status = "partial"
    elif dropped_connection_count or callback_error_count:
        status = "truncated"
    else:
        status = "complete"
    attributed_connection_count = sum(record.caller is not None for record in selected_records)
    unattributed_connection_count = len(selected_records) - attributed_connection_count
    caller_counts: dict[_FunctionIdentity, int] = {}
    for record in selected_records:
        if record.caller is not None:
            identity = record.caller.identity
            caller_counts[identity] = caller_counts.get(identity, 0) + 1
    caller_events = tuple(
        _network_caller_event(identity, connection_count=count, entity_id=entity_id)
        for identity, count in sorted(
            caller_counts.items(),
            key=lambda item: (
                item[0].name,
                item[0].filename,
                item[0].firstlineno,
                item[0].scope,
            ),
        )
    )
    caller_edges = tuple(
        CausalEdge(
            _network_caller_event_id(record.caller.identity),
            _network_connection_event_id(record),
            "connects",
            1.0 if record.caller.observation == "exact" else 0.9,
            {
                "source": "python-network-connection-wrapper",
                "observation": record.caller.observation,
            },
        )
        for record in selected_records
        if record.caller is not None
    )
    caller_status: SemanticCaptureStatus
    if invalid_caller_count:
        caller_status = "invalid"
    elif (
        saw_missing
        or saw_checkpoint
        or unattributed_connection_count
        or caller_callback_error_count
    ):
        caller_status = "partial"
    elif dropped_connection_count:
        caller_status = "truncated"
    else:
        caller_status = "complete"
    return _NetworkCaptureEvidence(
        tuple(
            _network_connection_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_connection_count,
        callback_error_count,
        caller_status,
        attributed_connection_count,
        unattributed_connection_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )


def _network_setup_record(
    value: object,
    *,
    document_pid: int,
) -> _NetworkSetupRecord:
    item = _object(value, "semantic network setup record")
    identifier = _integer(item.get("id"), "semantic network setup id")
    raw_phase = item.get("phase")
    phase: Literal["dns", "tls"]
    if raw_phase == "dns":
        phase = "dns"
    elif raw_phase == "tls":
        phase = "tls"
    else:
        raise DeepProfileError("semantic network setup phase is unsupported")
    adapter = _text(item.get("adapter"), "semantic network setup adapter")
    if adapter not in SEMANTIC_NETWORK_SETUP_ADAPTERS:
        raise DeepProfileError("semantic network setup adapter is unsupported")
    if (phase == "dns") != (adapter == "stdlib.socket.getaddrinfo"):
        raise DeepProfileError("semantic network setup phase and adapter are inconsistent")
    parent_pid = _integer(item.get("parent_pid"), "semantic network setup process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic network setup process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic network setup start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic network setup start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic network setup duration",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["completed", "setup_error", "unknown"]
    if raw_outcome == "completed":
        outcome = "completed"
    elif raw_outcome == "setup_error":
        outcome = "setup_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic network setup outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic network setup error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic network setup error type exceeds its character limit")
    if outcome == "completed" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("successful semantic network setup evidence is inconsistent")
    if outcome == "setup_error" and (duration_ns is None or error_type is None):
        raise DeepProfileError("failed semantic network setup evidence is inconsistent")
    if outcome == "unknown" and (duration_ns is not None or error_type is not None):
        raise DeepProfileError("unfinished semantic network setup evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic network setup finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _NetworkSetupRecord(
        identifier,
        phase,
        adapter,
        parent_pid,
        started_at_ns,
        duration_ns,
        outcome,
        error_type,
        caller,
        caller_status,
    )


def _network_setup_event_id(record: _NetworkSetupRecord) -> str:
    return f"semantic:network-setup:{record.parent_pid}:{record.identifier}"


def _network_setup_caller_event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"semantic:network-setup-caller:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _network_setup_event(
    record: _NetworkSetupRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-network-setup-wrapper",
        "phase": record.phase,
        "adapter": record.adapter,
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "hostname_captured": False,
        "server_address_captured": False,
        "sni_captured": False,
        "certificate_captured": False,
        "credentials_captured": False,
        "duration_boundary": record.phase,
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _network_setup_caller_event_id(record.caller.identity)
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_network_setup_event_id(record),
        kind="network.resolve" if record.phase == "dns" else "network.tls_handshake",
        name="DNS resolution" if record.phase == "dns" else "TLS handshake",
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _network_setup_caller_event(
    identity: _FunctionIdentity,
    *,
    phase_count: int,
    entity_id: str,
) -> Event:
    return Event(
        id=_network_setup_caller_event_id(identity),
        kind="python.callsite",
        name=identity.name,
        entity_id=entity_id,
        started_at_ns=None,
        finished_at_ns=None,
        clock_domain=None,
        uncertainty_ns=None,
        sequence=None,
        attributes={
            "source": "python-network-setup-wrapper",
            "scope": identity.scope,
            "module": identity.module,
            "qualname": identity.qualname,
            "filename": identity.filename,
            "firstlineno": identity.firstlineno,
            "network_setup_phase_count": phase_count,
            "arguments_captured": False,
            "locals_captured": False,
        },
    )


def _network_setup_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _NetworkSetupCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _NetworkSetupRecord]] = []
    process_count = 0
    dropped_phase_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_phase_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if semantic.get("format_version") == 1 or "network_setup_phases" not in semantic:
                saw_missing = True
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic network setup capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_network_setup_phases"),
                    "semantic network setup limit",
                )
                != MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS
            ):
                raise DeepProfileError("semantic network setup limit is unsupported")
            raw_adapters = _list(
                semantic.get("network_setup_adapters"),
                "semantic network setup adapters",
            )
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic network setup adapter")
                if adapter not in SEMANTIC_NETWORK_SETUP_ADAPTERS or adapter in document_adapters:
                    raise DeepProfileError("semantic network setup adapters are unsupported")
                document_adapters.add(adapter)
            if "stdlib.socket.getaddrinfo" not in document_adapters:
                raise DeepProfileError("semantic DNS adapter is missing")
            pid = _integer(document.get("pid"), "semantic network setup process id")
            raw_records = _list(
                semantic.get("network_setup_phases"),
                "semantic network setup phases",
            )
            declared_count = _integer(
                semantic.get("network_setup_count"),
                "semantic network setup count",
            )
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS
            ):
                raise DeepProfileError("semantic network setup count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_network_setup_count"),
                "semantic dropped network setup count",
            )
            document_callback_errors = _integer(
                semantic.get("network_setup_callback_error_count"),
                "semantic network setup callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("network_setup_caller_callback_error_count"),
                "semantic network setup caller callback error count",
            )
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic network setup registration contains evidence")
            records = tuple(
                _network_setup_record(raw_record, document_pid=pid) for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic network setup adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic network setup ids must be unique per process")
                identifiers.add(record.identifier)
            process_count += 1
            dropped_phase_count = _bounded_sum(
                dropped_phase_count,
                document_dropped_count,
                "semantic dropped network setup count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic network setup callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic network setup caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_phase_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = saw_incomplete or record.outcome == "unknown"
                rank = (
                    int(record.outcome != "completed"),
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_NETWORK_SETUP_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _NetworkSetupCaptureEvidence(
            (), (), (), "invalid", process_count, 0, 0, "invalid", 0, 0, 0, 0, ()
        )
    if process_count == 0:
        return _NetworkSetupCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_phase_count = _bounded_sum(
        dropped_phase_count,
        valid_phase_count - len(selected_records),
        "semantic dropped network setup count",
    )
    status: SemanticCaptureStatus
    if saw_missing or saw_checkpoint or saw_incomplete:
        status = "partial"
    elif dropped_phase_count or callback_error_count:
        status = "truncated"
    else:
        status = "complete"
    attributed_phase_count = sum(record.caller is not None for record in selected_records)
    unattributed_phase_count = len(selected_records) - attributed_phase_count
    caller_counts: dict[_FunctionIdentity, int] = {}
    for record in selected_records:
        if record.caller is not None:
            identity = record.caller.identity
            caller_counts[identity] = caller_counts.get(identity, 0) + 1
    caller_events = tuple(
        _network_setup_caller_event(identity, phase_count=count, entity_id=entity_id)
        for identity, count in sorted(
            caller_counts.items(),
            key=lambda item: (
                item[0].name,
                item[0].filename,
                item[0].firstlineno,
                item[0].scope,
            ),
        )
    )
    caller_edges = tuple(
        CausalEdge(
            _network_setup_caller_event_id(record.caller.identity),
            _network_setup_event_id(record),
            "resolves" if record.phase == "dns" else "handshakes",
            1.0 if record.caller.observation == "exact" else 0.9,
            {
                "source": "python-network-setup-wrapper",
                "observation": record.caller.observation,
            },
        )
        for record in selected_records
        if record.caller is not None
    )
    caller_status: SemanticCaptureStatus
    if invalid_caller_count:
        caller_status = "invalid"
    elif saw_missing or saw_checkpoint or unattributed_phase_count or caller_callback_error_count:
        caller_status = "partial"
    elif dropped_phase_count:
        caller_status = "truncated"
    else:
        caller_status = "complete"
    return _NetworkSetupCaptureEvidence(
        tuple(
            _network_setup_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_phase_count,
        callback_error_count,
        caller_status,
        attributed_phase_count,
        unattributed_phase_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )


def _logical_operation_record(
    value: object,
    *,
    document_pid: int,
) -> _LogicalOperationRecord:
    item = _object(value, "semantic logical operation record")
    identifier = _integer(item.get("id"), "semantic logical operation id")
    raw_category = item.get("category")
    categories: dict[str, LogicalOperationCategory] = {
        "broker": "broker",
        "cache": "cache",
        "database": "database",
        "executor": "executor",
        "queue": "queue",
        "scheduler": "scheduler",
        "server": "server",
    }
    if type(raw_category) is not str or raw_category not in categories:
        raise DeepProfileError("semantic logical operation category is unsupported")
    category = categories[raw_category]
    raw_operation = item.get("operation")
    operations: dict[str, LogicalOperationName] = {
        "batch": "batch",
        "command": "command",
        "commit": "commit",
        "consume": "consume",
        "execute": "execute",
        "executemany": "executemany",
        "executescript": "executescript",
        "get": "get",
        "publish": "publish",
        "put": "put",
        "rollback": "rollback",
        "request": "request",
        "task": "task",
    }
    if type(raw_operation) is not str or raw_operation not in operations:
        raise DeepProfileError("semantic logical operation is unsupported")
    operation = operations[raw_operation]
    operations_by_category = {
        "broker": {"consume", "publish"},
        "cache": {"batch", "command"},
        "database": {"commit", "execute", "executemany", "executescript", "rollback"},
        "executor": {"task"},
        "queue": {"get", "put"},
        "scheduler": {"task"},
        "server": {"request"},
    }
    if operation not in operations_by_category[category]:
        raise DeepProfileError("semantic logical operation and category are inconsistent")
    adapter = _text(item.get("adapter"), "semantic logical operation adapter")
    if adapter not in SEMANTIC_LOGICAL_OPERATION_ADAPTERS:
        raise DeepProfileError("semantic logical operation adapter is unsupported")
    if SEMANTIC_LOGICAL_OPERATION_ADAPTER_CATEGORIES.get(adapter) != category:
        raise DeepProfileError("semantic logical operation adapter and category are inconsistent")
    parent_pid = _integer(item.get("parent_pid"), "semantic logical operation process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic logical operation process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic logical operation start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic logical operation start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic logical operation duration",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["completed", "operation_error", "unknown"]
    if raw_outcome == "completed":
        outcome = "completed"
    elif raw_outcome == "operation_error":
        outcome = "operation_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic logical operation outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic logical operation error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic logical operation error type exceeds its character limit")
    if outcome == "completed" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("successful semantic logical operation evidence is inconsistent")
    if outcome == "operation_error" and (duration_ns is None or error_type is None):
        raise DeepProfileError("failed semantic logical operation evidence is inconsistent")
    if outcome == "unknown" and (duration_ns is not None or error_type is not None):
        raise DeepProfileError("unfinished semantic logical operation evidence is inconsistent")
    raw_status_code = item.get("status_code")
    status_code = (
        None
        if raw_status_code is None
        else _integer(raw_status_code, "semantic logical operation HTTP status")
    )
    if status_code is not None and not 100 <= status_code <= 999:
        raise DeepProfileError("semantic logical operation HTTP status is invalid")
    if category != "server" and status_code is not None:
        raise DeepProfileError("non-server logical operation has an HTTP status")
    if category == "server" and status_code is not None:
        if status_code >= 500 and (outcome != "operation_error" or error_type != "HTTPStatusError"):
            raise DeepProfileError("failed server logical operation evidence is inconsistent")
        if status_code < 500 and outcome != "completed":
            raise DeepProfileError("successful server logical operation evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic logical operation finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _LogicalOperationRecord(
        identifier,
        category,
        operation,
        adapter,
        parent_pid,
        started_at_ns,
        duration_ns,
        outcome,
        error_type,
        status_code,
        caller,
        caller_status,
    )


def _logical_operation_event_id(record: _LogicalOperationRecord) -> str:
    return f"semantic:logical-operation:{record.parent_pid}:{record.identifier}"


def _logical_operation_caller_event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"semantic:logical-operation-caller:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _logical_operation_name(record: _LogicalOperationRecord) -> str:
    if record.category == "scheduler":
        return "Async task"
    if record.category == "server":
        return "Inbound HTTP request"
    names = {
        "batch": "Cache batch",
        "command": "Cache command",
        "commit": "Database commit",
        "consume": "Broker consume",
        "execute": "Database execute",
        "executemany": "Database executemany",
        "executescript": "Database script",
        "get": "Queue get",
        "publish": "Broker publish",
        "put": "Queue put",
        "rollback": "Database rollback",
        "task": "Executor task",
    }
    return names[record.operation]


def _logical_operation_event(
    record: _LogicalOperationRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-logical-operation-wrapper",
        "category": record.category,
        "operation": record.operation,
        "adapter": record.adapter,
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "statement_captured": False,
        "parameters_captured": False,
        "payload_captured": False,
        "queue_item_captured": False,
        "queue_identity_captured": False,
        "callable_captured": False,
        "awaitable_captured": False,
        "task_name_captured": False,
        "context_captured": False,
        "arguments_captured": False,
        "return_value_captured": False,
        "http_method_captured": False,
        "route_captured": False,
        "url_captured": False,
        "headers_captured": False,
        "body_captured": False,
        "response_body_captured": False,
        "client_address_captured": False,
        "duration_boundary": (
            "submission_to_completion"
            if record.category == "executor"
            else "creation_to_completion"
            if record.category == "scheduler"
            else "request_to_response_completion"
            if record.category == "server"
            else "logical_operation"
        ),
    }
    if record.status_code is not None:
        attributes["status_code"] = record.status_code
    if record.caller is not None:
        attributes["caller_event_id"] = _logical_operation_caller_event_id(record.caller.identity)
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_logical_operation_event_id(record),
        kind=f"{record.category}.{record.operation}",
        name=_logical_operation_name(record),
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _logical_operation_caller_event(
    identity: _FunctionIdentity,
    *,
    operation_count: int,
    entity_id: str,
) -> Event:
    return Event(
        id=_logical_operation_caller_event_id(identity),
        kind="python.callsite",
        name=identity.name,
        entity_id=entity_id,
        started_at_ns=None,
        finished_at_ns=None,
        clock_domain=None,
        uncertainty_ns=None,
        sequence=None,
        attributes={
            "source": "python-logical-operation-wrapper",
            "scope": identity.scope,
            "module": identity.module,
            "qualname": identity.qualname,
            "filename": identity.filename,
            "firstlineno": identity.firstlineno,
            "logical_operation_count": operation_count,
            "arguments_captured": False,
            "locals_captured": False,
        },
    )


def _logical_operation_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _LogicalOperationCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _LogicalOperationRecord]] = []
    process_count = 0
    dropped_operation_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_operation_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if "logical_operation_capture_enabled" not in semantic:
                saw_missing = True
                continue
            enabled = _boolean(
                semantic.get("logical_operation_capture_enabled"),
                "semantic logical operation enabled marker",
            )
            raw_records = _list(
                semantic.get("logical_operations"),
                "semantic logical operations",
            )
            declared_count = _integer(
                semantic.get("logical_operation_count"),
                "semantic logical operation count",
            )
            document_dropped_count = _integer(
                semantic.get("dropped_logical_operation_count"),
                "semantic dropped logical operation count",
            )
            document_callback_errors = _integer(
                semantic.get("logical_operation_callback_error_count"),
                "semantic logical operation callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("logical_operation_caller_callback_error_count"),
                "semantic logical operation caller callback error count",
            )
            raw_adapters = _list(
                semantic.get("logical_operation_adapters"),
                "semantic logical operation adapters",
            )
            if not enabled:
                if (
                    raw_records
                    or declared_count
                    or document_dropped_count
                    or document_callback_errors
                    or document_caller_callback_errors
                    or raw_adapters
                ):
                    raise DeepProfileError(
                        "disabled semantic logical operation capture contains evidence"
                    )
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic logical operation capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_logical_operations"),
                    "semantic logical operation limit",
                )
                != MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic logical operation limit is unsupported")
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic logical operation adapter")
                if (
                    adapter not in SEMANTIC_LOGICAL_OPERATION_ADAPTERS
                    or adapter in document_adapters
                ):
                    raise DeepProfileError("semantic logical operation adapters are unsupported")
                document_adapters.add(adapter)
            pid = _integer(document.get("pid"), "semantic logical operation process id")
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic logical operation count is inconsistent")
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic logical operation registration contains evidence")
            records = tuple(
                _logical_operation_record(raw_record, document_pid=pid)
                for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic logical operation adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError(
                        "semantic logical operation ids must be unique per process"
                    )
                identifiers.add(record.identifier)
            process_count += 1
            dropped_operation_count = _bounded_sum(
                dropped_operation_count,
                document_dropped_count,
                "semantic dropped logical operation count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic logical operation callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic logical operation caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_operation_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = (
                    saw_incomplete
                    or record.outcome == "unknown"
                    or (record.category == "server" and record.status_code is None)
                )
                rank = (
                    int(record.outcome != "completed"),
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _LogicalOperationCaptureEvidence(
            (), (), (), "invalid", process_count, 0, 0, "invalid", 0, 0, 0, 0, ()
        )
    if process_count == 0:
        return _LogicalOperationCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_operation_count = _bounded_sum(
        dropped_operation_count,
        valid_operation_count - len(selected_records),
        "semantic dropped logical operation count",
    )
    status: SemanticCaptureStatus
    if saw_missing or saw_checkpoint or saw_incomplete:
        status = "partial"
    elif dropped_operation_count or callback_error_count:
        status = "truncated"
    else:
        status = "complete"
    attributed_operation_count = sum(record.caller is not None for record in selected_records)
    unattributed_operation_count = len(selected_records) - attributed_operation_count
    caller_counts: dict[_FunctionIdentity, int] = {}
    for record in selected_records:
        if record.caller is not None:
            identity = record.caller.identity
            caller_counts[identity] = caller_counts.get(identity, 0) + 1
    caller_events = tuple(
        _logical_operation_caller_event(identity, operation_count=count, entity_id=entity_id)
        for identity, count in sorted(
            caller_counts.items(),
            key=lambda item: (
                item[0].name,
                item[0].filename,
                item[0].firstlineno,
                item[0].scope,
            ),
        )
    )
    caller_edges = tuple(
        CausalEdge(
            _logical_operation_caller_event_id(record.caller.identity),
            _logical_operation_event_id(record),
            "performs",
            1.0 if record.caller.observation == "exact" else 0.9,
            {
                "source": "python-logical-operation-wrapper",
                "observation": record.caller.observation,
            },
        )
        for record in selected_records
        if record.caller is not None
    )
    caller_status: SemanticCaptureStatus
    if invalid_caller_count:
        caller_status = "invalid"
    elif (
        saw_missing or saw_checkpoint or unattributed_operation_count or caller_callback_error_count
    ):
        caller_status = "partial"
    elif dropped_operation_count:
        caller_status = "truncated"
    else:
        caller_status = "complete"
    return _LogicalOperationCaptureEvidence(
        tuple(
            _logical_operation_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_operation_count,
        callback_error_count,
        caller_status,
        attributed_operation_count,
        unattributed_operation_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )


def _function_identity(
    value: object,
) -> tuple[int, _FunctionIdentity, _DeepFunctionValues]:
    item = _object(value, "deep-profile function")
    identifier = _integer(item.get("id"), "deep-profile function id")
    scope = _text(item.get("scope"), "deep-profile function scope")
    if scope not in {"application", "library", "runtime"}:
        raise DeepProfileError("deep-profile function scope is unsupported")
    identity = _FunctionIdentity(
        _text(item.get("module"), "deep-profile function module"),
        _text(item.get("qualname"), "deep-profile function qualname"),
        _text(item.get("filename"), "deep-profile function filename"),
        _integer(item.get("firstlineno"), "deep-profile function first line"),
        scope,
    )
    native = _boolean(item.get("native", False), "deep-profile native function marker")
    if native != identity.native:
        raise DeepProfileError("deep-profile native function identity is inconsistent")
    values = (
        _integer(item.get("call_count"), "deep-profile function call count"),
        _integer(item.get("total_ns"), "deep-profile function total time"),
        _integer(item.get("self_ns"), "deep-profile function self time"),
        _integer(item.get("max_ns"), "deep-profile function maximum time"),
        _integer(item.get("exception_count", 0), "deep-profile function exception count"),
        _integer(
            item.get("non_control_flow_exception_count", 0),
            "deep-profile function non-control-flow exception count",
        ),
    )
    if (
        values[2] > values[1]
        or values[3] > values[1]
        or values[5] > values[4]
        or (native and (values[4] > values[0] or values[5] != 0))
    ):
        raise DeepProfileError("deep-profile function timings are inconsistent")
    return identifier, identity, values


def _sample_function_identity(
    value: object,
) -> tuple[int, _FunctionIdentity, tuple[int, int]]:
    item = _object(value, "sample-profile function")
    identifier = _integer(item.get("id"), "sample-profile function id")
    scope = _text(item.get("scope"), "sample-profile function scope")
    if scope not in {"application", "library", "runtime"}:
        raise DeepProfileError("sample-profile function scope is unsupported")
    identity = _FunctionIdentity(
        _text(item.get("module"), "sample-profile function module"),
        _text(item.get("qualname"), "sample-profile function qualname"),
        _text(item.get("filename"), "sample-profile function filename"),
        _integer(item.get("firstlineno"), "sample-profile function first line"),
        scope,
    )
    values = (
        _integer(item.get("sample_count"), "sample-profile function sample count"),
        _integer(item.get("leaf_sample_count"), "sample-profile function leaf sample count"),
    )
    if values[1] > values[0]:
        raise DeepProfileError("sample-profile function sample counts are inconsistent")
    return identifier, identity, values


def _function_ranking_key(identity: _FunctionIdentity) -> tuple[bytes, bytes]:
    encoded = json.dumps(
        [
            identity.module,
            identity.qualname,
            identity.filename,
            identity.firstlineno,
            identity.scope,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).digest(), encoded


def _edge_ranking_key(
    edge: tuple[_FunctionIdentity, _FunctionIdentity],
) -> bytes:
    source, target = edge
    return _function_ranking_key(source)[0] + _function_ranking_key(target)[0]


def _deep_functions(
    raw_functions: list[object],
    *,
    exception_filter: bool | None = None,
) -> dict[int, tuple[_FunctionIdentity, _DeepFunctionValues]]:
    by_identifier: dict[int, tuple[_FunctionIdentity, _DeepFunctionValues]] = {}
    for raw_function in raw_functions:
        item = _object(raw_function, "deep-profile function")
        if exception_filter is not None and (
            ("non_control_flow_exception_count" in item) != exception_filter
        ):
            raise DeepProfileError("deep-profile Python exception filter evidence is inconsistent")
        identifier, identity, values = _function_identity(raw_function)
        if identifier in by_identifier:
            raise DeepProfileError("deep-profile function ids must be unique per process")
        by_identifier[identifier] = (identity, values)
    return by_identifier


def _sample_functions(
    raw_functions: list[object],
) -> dict[int, tuple[_FunctionIdentity, tuple[int, int]]]:
    by_identifier: dict[int, tuple[_FunctionIdentity, tuple[int, int]]] = {}
    for raw_function in raw_functions:
        identifier, identity, values = _sample_function_identity(raw_function)
        if identifier in by_identifier:
            raise DeepProfileError("sample-profile function ids must be unique per process")
        by_identifier[identifier] = (identity, values)
    return by_identifier


def _deep_edge(
    value: object,
    functions: dict[int, tuple[_FunctionIdentity, _DeepFunctionValues]],
) -> tuple[_FunctionIdentity, _FunctionIdentity, int, int]:
    edge = _object(value, "deep-profile edge")
    source_entry = functions.get(_integer(edge.get("source_id"), "deep-profile edge source"))
    target_entry = functions.get(_integer(edge.get("target_id"), "deep-profile edge target"))
    if source_entry is None or target_entry is None:
        raise DeepProfileError("deep-profile edge references an unknown function")
    return (
        source_entry[0],
        target_entry[0],
        _integer(edge.get("call_count"), "deep-profile edge call count"),
        _integer(edge.get("total_ns"), "deep-profile edge total time"),
    )


def _sample_edge(
    value: object,
    functions: dict[int, tuple[_FunctionIdentity, tuple[int, int]]],
) -> tuple[_FunctionIdentity, _FunctionIdentity, int]:
    edge = _object(value, "sample-profile edge")
    source_entry = functions.get(_integer(edge.get("source_id"), "sample-profile edge source"))
    target_entry = functions.get(_integer(edge.get("target_id"), "sample-profile edge target"))
    if source_entry is None or target_entry is None:
        raise DeepProfileError("sample-profile edge references an unknown function")
    return (
        source_entry[0],
        target_entry[0],
        _integer(edge.get("sample_count"), "sample-profile edge sample count"),
    )


def _select_deep_function_keys(
    payloads: tuple[bytes, ...],
    workspace_directory: Path,
) -> tuple[frozenset[bytes], int]:
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            functions = _deep_functions(_list(document.get("functions"), "deep-profile functions"))
            for identity, values in functions.values():
                key, collision_identity = _function_ranking_key(identity)
                ranker.add(
                    key,
                    collision_identity,
                    values[2],
                    values[1],
                    "deep-profile aggregate function rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_FUNCTIONS)
        return selected, ranker.database_bytes()


def _select_sample_function_keys(
    payloads: tuple[bytes, ...],
    workspace_directory: Path,
) -> tuple[frozenset[bytes], int]:
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            functions = _sample_functions(
                _list(document.get("functions"), "sample-profile functions")
            )
            for identity, values in functions.values():
                key, collision_identity = _function_ranking_key(identity)
                ranker.add(
                    key,
                    collision_identity,
                    values[1],
                    values[0],
                    "sample-profile aggregate function rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_FUNCTIONS)
        return selected, ranker.database_bytes()


def _aggregate_deep_functions_and_select_edges(
    payloads: tuple[bytes, ...],
    selected_function_keys: frozenset[bytes],
    workspace_directory: Path,
) -> tuple[
    dict[_FunctionIdentity, _FunctionAggregate],
    int,
    int,
    int,
    frozenset[bytes],
    int,
]:
    functions: dict[_FunctionIdentity, _FunctionAggregate] = {}
    dropped_call_count = 0
    dropped_exception_event_count = 0
    dropped_non_control_flow_exception_event_count = 0
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            pid = _integer(document.get("pid"), "deep-profile process id")
            by_identifier = _deep_functions(
                _list(document.get("functions"), "deep-profile functions")
            )
            for identity, values in by_identifier.values():
                function_key = _function_ranking_key(identity)[0]
                if function_key not in selected_function_keys:
                    dropped_call_count = _bounded_sum(
                        dropped_call_count,
                        values[0],
                        "deep-profile dropped call count",
                    )
                    if not identity.native:
                        dropped_exception_event_count = _bounded_sum(
                            dropped_exception_event_count,
                            values[4],
                            "deep-profile dropped Python exception event count",
                        )
                        dropped_non_control_flow_exception_event_count = _bounded_sum(
                            dropped_non_control_flow_exception_event_count,
                            values[5],
                            "deep-profile dropped Python non-control-flow exception event count",
                        )
                    continue
                aggregate = functions.get(identity)
                if aggregate is None:
                    aggregate = _FunctionAggregate()
                    functions[identity] = aggregate
                aggregate.add(*values, pid)
            for raw_edge in _list(document.get("edges"), "deep-profile edges"):
                source, target, call_count, total_ns = _deep_edge(raw_edge, by_identifier)
                source_key = _function_ranking_key(source)[0]
                target_key = _function_ranking_key(target)[0]
                if (
                    source == target
                    or source_key not in selected_function_keys
                    or target_key not in selected_function_keys
                ):
                    continue
                edge_key = source_key + target_key
                ranker.add(
                    edge_key,
                    edge_key,
                    total_ns,
                    call_count,
                    "deep-profile aggregate edge rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_EDGES)
        return (
            functions,
            dropped_call_count,
            dropped_exception_event_count,
            dropped_non_control_flow_exception_event_count,
            selected,
            ranker.database_bytes(),
        )


def _aggregate_sample_functions_and_select_edges(
    payloads: tuple[bytes, ...],
    selected_function_keys: frozenset[bytes],
    workspace_directory: Path,
) -> tuple[
    dict[_FunctionIdentity, _SampleFunctionAggregate],
    int,
    frozenset[bytes],
    int,
]:
    functions: dict[_FunctionIdentity, _SampleFunctionAggregate] = {}
    dropped_sample_count = 0
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            pid = _integer(document.get("pid"), "sample-profile process id")
            by_identifier = _sample_functions(
                _list(document.get("functions"), "sample-profile functions")
            )
            for identity, values in by_identifier.values():
                function_key = _function_ranking_key(identity)[0]
                if function_key not in selected_function_keys:
                    dropped_sample_count = _bounded_sum(
                        dropped_sample_count,
                        values[0],
                        "sample-profile dropped frame sample count",
                    )
                    continue
                aggregate = functions.get(identity)
                if aggregate is None:
                    aggregate = _SampleFunctionAggregate()
                    functions[identity] = aggregate
                aggregate.add(*values, pid)
            for raw_edge in _list(document.get("edges"), "sample-profile edges"):
                source, target, edge_sample_count = _sample_edge(raw_edge, by_identifier)
                source_key = _function_ranking_key(source)[0]
                target_key = _function_ranking_key(target)[0]
                if (
                    source == target
                    or source_key not in selected_function_keys
                    or target_key not in selected_function_keys
                ):
                    continue
                edge_key = source_key + target_key
                ranker.add(
                    edge_key,
                    edge_key,
                    edge_sample_count,
                    0,
                    "sample-profile aggregate edge rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_EDGES)
        return functions, dropped_sample_count, selected, ranker.database_bytes()


def _event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"deep:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _sample_event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"sample:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _validate_root_process_id(root_process_id: int | None) -> None:
    if root_process_id is None:
        return
    if (
        not isinstance(root_process_id, int)
        or isinstance(root_process_id, bool)
        or root_process_id <= 0
        or root_process_id > _MAX_INTEGER
    ):
        raise DeepProfileError("profile root process id must be a positive integer")


def _process_role(pid: int, root_process_id: int | None) -> str:
    if root_process_id is None:
        return "unknown"
    return "root" if pid == root_process_id else "descendant"


def _deep_process_contributions(
    aggregate: _FunctionAggregate,
    root_process_id: int | None,
    *,
    include_non_control_flow_exceptions: bool,
) -> list[JsonValue]:
    return [
        {
            "pid": pid,
            "role": _process_role(pid, root_process_id),
            "call_count": process.call_count,
            "total_seconds": process.total_ns / 1_000_000_000,
            "self_seconds": process.self_ns / 1_000_000_000,
            "max_seconds": process.max_ns / 1_000_000_000,
            "exception_count": process.exception_count,
            **(
                {"non_control_flow_exception_count": (process.non_control_flow_exception_count)}
                if include_non_control_flow_exceptions
                else {}
            ),
        }
        for pid, process in sorted((aggregate.processes or {}).items())
    ]


def _sample_process_contributions(
    aggregate: _SampleFunctionAggregate,
    root_process_id: int | None,
    interval_seconds: float,
) -> list[JsonValue]:
    return [
        {
            "pid": pid,
            "role": _process_role(pid, root_process_id),
            "sample_count": process.sample_count,
            "leaf_sample_count": process.leaf_sample_count,
            "estimated_total_seconds": process.sample_count * interval_seconds,
            "estimated_leaf_seconds": process.leaf_sample_count * interval_seconds,
        }
        for pid, process in sorted((aggregate.processes or {}).items())
    ]


def load_deep_profile(
    session: DeepProfileSession,
    *,
    entity_id: str,
    root_process_id: int | None = None,
) -> DeepProfileResult:
    if session.mode != "deep":
        raise DeepProfileError("deep-profile loader requires a deep-profile session")
    normalization_started_ns = time.perf_counter_ns()
    session.finish_collection()
    _validate_root_process_id(root_process_id)
    truncated = False
    dropped_call_count = 0
    dropped_edge_count = 0
    dropped_exception_event_count = 0
    dropped_non_control_flow_exception_event_count = 0
    callback_error_count = 0
    open_call_count = 0
    observer_integrity_process_count = 0
    profile_hook_setter_call_count = 0
    profile_hook_setter_process_count = 0
    trace_hook_setter_call_count = 0
    trace_hook_setter_process_count = 0
    python_exception_filter_process_count = 0
    payloads, dropped_file_count = _profile_payloads(session, root_process_id)
    transport_stats = _snapshot_transport_stats(session, dropped_file_count)
    truncated = transport_stats.dropped_profile_process_count > 0
    process_ids: set[int] = set()
    checkpoint_process_ids: set[int] = set()
    registration_only_process_ids: set[int] = set()
    publication_accumulator = _PublicationMetricsAccumulator()
    for payload in payloads:
        document = _parse_document(payload)
        if document.get("format_version") != 1:
            raise DeepProfileError("generated deep-profile format version is unsupported")
        if document.get("mode", "deep") != "deep":
            raise DeepProfileError("generated deep-profile mode is unsupported")
        pid = _integer(document.get("pid"), "deep-profile process id")
        if pid <= 0:
            raise DeepProfileError("deep-profile process id must be positive")
        if pid in process_ids:
            raise DeepProfileError("deep-profile process ids must be unique")
        process_ids.add(pid)
        snapshot_kind = _snapshot_kind(document, "deep-profile")
        registration_only = _registration_only(document, "deep-profile", snapshot_kind)
        publication_accumulator.add(
            document,
            pid=pid,
            snapshot_kind=snapshot_kind,
            registration_only=registration_only,
        )
        if snapshot_kind == "checkpoint":
            checkpoint_process_ids.add(pid)
        if registration_only:
            registration_only_process_ids.add(pid)
        document_truncated = _boolean(document.get("truncated"), "deep-profile truncation")
        document_dropped_call_count = _integer(
            document.get("dropped_call_count"), "deep-profile dropped call count"
        )
        document_dropped_edge_count = _integer(
            document.get("dropped_edge_count"), "deep-profile dropped edge count"
        )
        document_dropped_exception_event_count = _integer(
            document.get("dropped_exception_event_count", 0),
            "deep-profile dropped Python exception event count",
        )
        document_exception_filter = _python_exception_filter(document)
        if document_exception_filter:
            python_exception_filter_process_count += 1
            document_dropped_non_control_flow_exception_event_count = _integer(
                document.get("dropped_non_control_flow_exception_event_count"),
                "deep-profile dropped Python non-control-flow exception event count",
            )
            if (
                document_dropped_non_control_flow_exception_event_count
                > document_dropped_exception_event_count
            ):
                raise DeepProfileError(
                    "deep-profile dropped Python exception counts are inconsistent"
                )
        else:
            if "dropped_non_control_flow_exception_event_count" in document:
                raise DeepProfileError(
                    "deep-profile Python exception filter evidence is inconsistent"
                )
            document_dropped_non_control_flow_exception_event_count = 0
        document_callback_error_count = _integer(
            document.get("callback_error_count"), "deep-profile callback error count"
        )
        document_open_call_count = _integer(
            document.get("open_call_count", 0), "deep-profile open call count"
        )
        observer_integrity = _observer_integrity(document)
        document_profile_hook_setter_call_count = 0
        document_trace_hook_setter_call_count = 0
        if observer_integrity is not None:
            observer_integrity_process_count += 1
            (
                document_profile_hook_setter_call_count,
                document_trace_hook_setter_call_count,
            ) = observer_integrity
            profile_hook_setter_call_count = _bounded_sum(
                profile_hook_setter_call_count,
                document_profile_hook_setter_call_count,
                "deep-profile profile-hook setter call count",
            )
            trace_hook_setter_call_count = _bounded_sum(
                trace_hook_setter_call_count,
                document_trace_hook_setter_call_count,
                "deep-profile trace-hook setter call count",
            )
            profile_hook_setter_process_count += document_profile_hook_setter_call_count > 0
            trace_hook_setter_process_count += document_trace_hook_setter_call_count > 0
        raw_functions = _list(document.get("functions"), "deep-profile functions")
        raw_edges = _list(document.get("edges"), "deep-profile edges")
        if registration_only and (
            document_truncated
            or document_dropped_call_count
            or document_dropped_edge_count
            or document_dropped_exception_event_count
            or document_dropped_non_control_flow_exception_event_count
            or document_callback_error_count
            or document_open_call_count
            or document_profile_hook_setter_call_count
            or document_trace_hook_setter_call_count
            or raw_functions
            or raw_edges
        ):
            raise DeepProfileError("deep-profile registration must not contain profile evidence")
        truncated = document_truncated or truncated
        dropped_call_count = _bounded_sum(
            dropped_call_count,
            document_dropped_call_count,
            "deep-profile dropped call count",
        )
        dropped_edge_count = _bounded_sum(
            dropped_edge_count,
            document_dropped_edge_count,
            "deep-profile dropped edge count",
        )
        dropped_exception_event_count = _bounded_sum(
            dropped_exception_event_count,
            document_dropped_exception_event_count,
            "deep-profile dropped Python exception event count",
        )
        dropped_non_control_flow_exception_event_count = _bounded_sum(
            dropped_non_control_flow_exception_event_count,
            document_dropped_non_control_flow_exception_event_count,
            "deep-profile dropped Python non-control-flow exception event count",
        )
        callback_error_count = _bounded_sum(
            callback_error_count,
            document_callback_error_count,
            "deep-profile callback error count",
        )
        open_call_count = _bounded_sum(
            open_call_count,
            document_open_call_count,
            "deep-profile open call count",
        )
        by_identifier = _deep_functions(
            raw_functions,
            exception_filter=document_exception_filter,
        )
        for raw_edge in raw_edges:
            _deep_edge(raw_edge, by_identifier)

    semantic_evidence = _semantic_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    http_evidence = _http_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    network_evidence = _network_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    network_setup_evidence = _network_setup_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    logical_operation_evidence = _logical_operation_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    publication_metrics = publication_accumulator.result()
    selected_function_keys, function_ranking_bytes = _select_deep_function_keys(
        payloads,
        session.directory,
    )
    (
        functions,
        selection_dropped_call_count,
        selection_dropped_exception_event_count,
        selection_dropped_non_control_flow_exception_event_count,
        selected_edge_keys,
        edge_ranking_bytes,
    ) = _aggregate_deep_functions_and_select_edges(
        payloads,
        selected_function_keys,
        session.directory,
    )
    if selection_dropped_call_count:
        truncated = True
        dropped_call_count = _bounded_sum(
            dropped_call_count,
            selection_dropped_call_count,
            "deep-profile dropped call count",
        )
    if selection_dropped_exception_event_count:
        truncated = True
        dropped_exception_event_count = _bounded_sum(
            dropped_exception_event_count,
            selection_dropped_exception_event_count,
            "deep-profile dropped Python exception event count",
        )
    if selection_dropped_non_control_flow_exception_event_count:
        dropped_non_control_flow_exception_event_count = _bounded_sum(
            dropped_non_control_flow_exception_event_count,
            selection_dropped_non_control_flow_exception_event_count,
            "deep-profile dropped Python non-control-flow exception event count",
        )
    edges: dict[tuple[_FunctionIdentity, _FunctionIdentity], _EdgeAggregate] = {}
    for payload in payloads:
        document = _parse_document(payload)
        by_identifier = _deep_functions(_list(document.get("functions"), "deep-profile functions"))
        for raw_edge in _list(document.get("edges"), "deep-profile edges"):
            source, target, call_count, total_ns = _deep_edge(raw_edge, by_identifier)
            key = (source, target)
            source_key = _function_ranking_key(source)[0]
            target_key = _function_ranking_key(target)[0]
            if (
                source == target
                or source_key not in selected_function_keys
                or target_key not in selected_function_keys
            ):
                continue
            if source_key + target_key not in selected_edge_keys:
                truncated = True
                dropped_edge_count = _bounded_sum(
                    dropped_edge_count,
                    call_count,
                    "deep-profile dropped edge count",
                )
                continue
            aggregate_edge = edges.get(key)
            if aggregate_edge is None:
                aggregate_edge = _EdgeAggregate()
                edges[key] = aggregate_edge
            aggregate_edge.call_count = _bounded_sum(
                aggregate_edge.call_count,
                call_count,
                "deep-profile edge call count",
            )
            aggregate_edge.total_ns = _bounded_sum(
                aggregate_edge.total_ns,
                total_ns,
                "deep-profile edge total time",
            )
    profile_events = tuple(
        Event(
            id=_event_id(identity),
            kind="python.call.aggregate",
            name=identity.name,
            entity_id=entity_id,
            started_at_ns=None,
            finished_at_ns=None,
            clock_domain=None,
            uncertainty_ns=None,
            sequence=None,
            attributes={
                "source": "deep-profile",
                "scope": identity.scope,
                "module": identity.module,
                "qualname": identity.qualname,
                "filename": identity.filename,
                "firstlineno": identity.firstlineno,
                "implementation": "native" if identity.native else "python",
                "call_count": aggregate.call_count,
                "exception_count": aggregate.exception_count,
                **(
                    {
                        "non_control_flow_exception_count": (
                            aggregate.non_control_flow_exception_count
                        )
                    }
                    if python_exception_filter_process_count == len(process_ids)
                    and process_ids
                    and not identity.native
                    else {}
                ),
                "total_seconds": aggregate.total_ns / 1_000_000_000,
                "self_seconds": aggregate.self_ns / 1_000_000_000,
                "max_seconds": aggregate.max_ns / 1_000_000_000,
                "process_count": len(aggregate.processes or ()),
                "processes": _deep_process_contributions(
                    aggregate,
                    root_process_id,
                    include_non_control_flow_exceptions=(
                        python_exception_filter_process_count == len(process_ids)
                        and bool(process_ids)
                        and not identity.native
                    ),
                ),
            },
        )
        for identity, aggregate in sorted(functions.items(), key=lambda item: item[0].name)
    )
    normalized_events = (
        profile_events
        + semantic_evidence.caller_events
        + semantic_evidence.subprocess_events
        + http_evidence.caller_events
        + http_evidence.request_events
        + network_evidence.caller_events
        + network_evidence.connection_events
        + network_setup_evidence.caller_events
        + network_setup_evidence.phase_events
        + logical_operation_evidence.caller_events
        + logical_operation_evidence.operation_events
    )
    event_ids = {identity: _event_id(identity) for identity in functions}
    profile_edges = tuple(
        CausalEdge(
            event_ids[source],
            event_ids[target],
            "calls",
            1.0,
            {
                "source": "deep-profile",
                "call_count": aggregate.call_count,
                "total_seconds": aggregate.total_ns / 1_000_000_000,
            },
        )
        for (source, target), aggregate in sorted(
            edges.items(), key=lambda item: (item[0][0].name, item[0][1].name)
        )
    )
    normalized_edges = (
        profile_edges
        + semantic_evidence.caller_edges
        + http_evidence.caller_edges
        + network_evidence.caller_edges
        + network_setup_evidence.caller_edges
        + logical_operation_evidence.caller_edges
    )
    if any(
        not math.isfinite(value)
        for event in profile_events
        for value in (
            event.attributes["total_seconds"],
            event.attributes["self_seconds"],
            event.attributes["max_seconds"],
        )
        if isinstance(value, float)
    ):
        raise DeepProfileError("deep-profile normalized timings exceed the numeric range")
    normalization_duration_ns = max(0, time.perf_counter_ns() - normalization_started_ns)
    observer_integrity_status: Literal["complete", "partial", "unavailable"]
    if observer_integrity_process_count == 0:
        observer_integrity_status = "unavailable"
    elif (
        observer_integrity_process_count != len(process_ids)
        or profile_hook_setter_call_count
        or trace_hook_setter_call_count
    ):
        observer_integrity_status = "partial"
    else:
        observer_integrity_status = "complete"
    return DeepProfileResult(
        events=normalized_events,
        edges=normalized_edges,
        process_count=len(process_ids),
        process_ids=tuple(sorted(process_ids)),
        truncated=truncated,
        dropped_call_count=dropped_call_count,
        dropped_edge_count=dropped_edge_count,
        callback_error_count=callback_error_count,
        native_function_count=sum(identity.native for identity in functions),
        native_call_count=sum(
            aggregate.call_count for identity, aggregate in functions.items() if identity.native
        ),
        native_exception_count=sum(
            aggregate.exception_count
            for identity, aggregate in functions.items()
            if identity.native
        ),
        python_exception_function_count=sum(
            not identity.native and aggregate.exception_count > 0
            for identity, aggregate in functions.items()
        ),
        python_exception_event_count=sum(
            aggregate.exception_count
            for identity, aggregate in functions.items()
            if not identity.native
        ),
        dropped_python_exception_event_count=dropped_exception_event_count,
        python_exception_filter_status=(
            "complete"
            if python_exception_filter_process_count == len(process_ids) and process_ids
            else "unavailable"
        ),
        python_non_control_flow_exception_function_count=sum(
            not identity.native and aggregate.non_control_flow_exception_count > 0
            for identity, aggregate in functions.items()
        ),
        python_non_control_flow_exception_event_count=sum(
            aggregate.non_control_flow_exception_count
            for identity, aggregate in functions.items()
            if not identity.native
        ),
        dropped_python_non_control_flow_exception_event_count=(
            dropped_non_control_flow_exception_event_count
        ),
        observer_integrity_status=observer_integrity_status,
        observer_integrity_process_count=observer_integrity_process_count,
        profile_hook_setter_call_count=profile_hook_setter_call_count,
        profile_hook_setter_process_count=profile_hook_setter_process_count,
        trace_hook_setter_call_count=trace_hook_setter_call_count,
        trace_hook_setter_process_count=trace_hook_setter_process_count,
        open_call_count=open_call_count,
        checkpoint_process_count=len(checkpoint_process_ids),
        registration_only_process_count=len(registration_only_process_ids),
        dropped_profile_process_count=transport_stats.dropped_profile_process_count,
        dropped_profile_process_count_truncated=(
            transport_stats.dropped_profile_process_count_truncated
        ),
        transport=_profile_transport(session, publication_metrics),
        collector_error_count=session.collector_error_count,
        snapshot_metrics_status=transport_stats.metrics_status,
        snapshot_message_count=transport_stats.message_count,
        snapshot_payload_bytes=transport_stats.payload_bytes,
        max_snapshot_payload_bytes=transport_stats.max_payload_bytes,
        snapshot_serialization_ns=transport_stats.serialization_ns,
        max_snapshot_serialization_ns=transport_stats.max_serialization_ns,
        checkpoint_snapshot_message_count=transport_stats.checkpoint_message_count,
        checkpoint_snapshot_payload_bytes=transport_stats.checkpoint_payload_bytes,
        max_checkpoint_snapshot_payload_bytes=transport_stats.max_checkpoint_payload_bytes,
        checkpoint_snapshot_serialization_ns=transport_stats.checkpoint_serialization_ns,
        max_checkpoint_snapshot_serialization_ns=(transport_stats.max_checkpoint_serialization_ns),
        publication_metrics_status=publication_metrics.status,
        publication_fallback_process_ids=publication_metrics.fallback_process_ids,
        publication_socket_attempted_process_count=(
            publication_metrics.socket_attempted_process_count
        ),
        publication_socket_failure_ns=publication_metrics.socket_failure_ns,
        max_publication_socket_failure_ns=publication_metrics.max_socket_failure_ns,
        normalization_metrics_status="available",
        normalization_duration_ns=normalization_duration_ns,
        ranking_database_peak_bytes=max(function_ranking_bytes, edge_ranking_bytes),
        semantic_capture_status=semantic_evidence.status,
        semantic_capture_process_count=semantic_evidence.process_count,
        subprocess_event_count=len(semantic_evidence.subprocess_events),
        dropped_subprocess_count=semantic_evidence.dropped_subprocess_count,
        semantic_callback_error_count=semantic_evidence.callback_error_count,
        semantic_caller_event_count=len(semantic_evidence.caller_events),
        semantic_caller_edge_count=len(semantic_evidence.caller_edges),
        caller_attribution_status=semantic_evidence.caller_attribution_status,
        attributed_subprocess_count=semantic_evidence.attributed_subprocess_count,
        unattributed_subprocess_count=semantic_evidence.unattributed_subprocess_count,
        invalid_caller_count=semantic_evidence.invalid_caller_count,
        caller_callback_error_count=semantic_evidence.caller_callback_error_count,
        http_capture_status=http_evidence.status,
        http_capture_process_count=http_evidence.process_count,
        http_request_event_count=len(http_evidence.request_events),
        dropped_http_request_count=http_evidence.dropped_request_count,
        http_callback_error_count=http_evidence.callback_error_count,
        http_caller_event_count=len(http_evidence.caller_events),
        http_caller_edge_count=len(http_evidence.caller_edges),
        http_caller_attribution_status=http_evidence.caller_attribution_status,
        attributed_http_request_count=http_evidence.attributed_request_count,
        unattributed_http_request_count=http_evidence.unattributed_request_count,
        invalid_http_caller_count=http_evidence.invalid_caller_count,
        http_caller_callback_error_count=http_evidence.caller_callback_error_count,
        http_adapters=http_evidence.adapters,
        network_capture_status=network_evidence.status,
        network_capture_process_count=network_evidence.process_count,
        network_connection_event_count=len(network_evidence.connection_events),
        dropped_network_connection_count=network_evidence.dropped_connection_count,
        network_callback_error_count=network_evidence.callback_error_count,
        network_caller_event_count=len(network_evidence.caller_events),
        network_caller_edge_count=len(network_evidence.caller_edges),
        network_caller_attribution_status=network_evidence.caller_attribution_status,
        attributed_network_connection_count=network_evidence.attributed_connection_count,
        unattributed_network_connection_count=network_evidence.unattributed_connection_count,
        invalid_network_caller_count=network_evidence.invalid_caller_count,
        network_caller_callback_error_count=network_evidence.caller_callback_error_count,
        network_adapters=network_evidence.adapters,
        network_setup_capture_status=network_setup_evidence.status,
        network_setup_capture_process_count=network_setup_evidence.process_count,
        network_setup_event_count=len(network_setup_evidence.phase_events),
        dropped_network_setup_count=network_setup_evidence.dropped_phase_count,
        network_setup_callback_error_count=network_setup_evidence.callback_error_count,
        network_setup_caller_event_count=len(network_setup_evidence.caller_events),
        network_setup_caller_edge_count=len(network_setup_evidence.caller_edges),
        network_setup_caller_attribution_status=(network_setup_evidence.caller_attribution_status),
        attributed_network_setup_count=network_setup_evidence.attributed_phase_count,
        unattributed_network_setup_count=network_setup_evidence.unattributed_phase_count,
        invalid_network_setup_caller_count=network_setup_evidence.invalid_caller_count,
        network_setup_caller_callback_error_count=(
            network_setup_evidence.caller_callback_error_count
        ),
        network_setup_adapters=network_setup_evidence.adapters,
        logical_operation_capture_status=logical_operation_evidence.status,
        logical_operation_capture_process_count=logical_operation_evidence.process_count,
        logical_operation_event_count=len(logical_operation_evidence.operation_events),
        dropped_logical_operation_count=(logical_operation_evidence.dropped_operation_count),
        logical_operation_callback_error_count=(logical_operation_evidence.callback_error_count),
        logical_operation_caller_event_count=len(logical_operation_evidence.caller_events),
        logical_operation_caller_edge_count=len(logical_operation_evidence.caller_edges),
        logical_operation_caller_attribution_status=(
            logical_operation_evidence.caller_attribution_status
        ),
        attributed_logical_operation_count=(logical_operation_evidence.attributed_operation_count),
        unattributed_logical_operation_count=(
            logical_operation_evidence.unattributed_operation_count
        ),
        invalid_logical_operation_caller_count=(logical_operation_evidence.invalid_caller_count),
        logical_operation_caller_callback_error_count=(
            logical_operation_evidence.caller_callback_error_count
        ),
        logical_operation_adapters=logical_operation_evidence.adapters,
    )


def load_sample_profile(
    session: DeepProfileSession,
    *,
    entity_id: str,
    root_process_id: int | None = None,
) -> DeepProfileResult:
    if session.mode != "sample":
        raise DeepProfileError("sample-profile loader requires a sample-profile session")
    normalization_started_ns = time.perf_counter_ns()
    session.finish_collection()
    _validate_root_process_id(root_process_id)
    truncated = False
    dropped_frame_sample_count = 0
    dropped_edge_sample_count = 0
    callback_error_count = 0
    sample_count = 0
    thread_sample_count = 0
    interval_ns = 0
    payloads, dropped_file_count = _profile_payloads(session, root_process_id)
    transport_stats = _snapshot_transport_stats(session, dropped_file_count)
    truncated = transport_stats.dropped_profile_process_count > 0
    process_ids: set[int] = set()
    checkpoint_process_ids: set[int] = set()
    registration_only_process_ids: set[int] = set()
    publication_accumulator = _PublicationMetricsAccumulator()
    for payload in payloads:
        document = _parse_document(payload)
        if document.get("format_version") != 1:
            raise DeepProfileError("generated sample-profile format version is unsupported")
        if document.get("mode") != "sample":
            raise DeepProfileError("generated sample-profile mode is unsupported")
        pid = _integer(document.get("pid"), "sample-profile process id")
        if pid <= 0:
            raise DeepProfileError("sample-profile process id must be positive")
        if pid in process_ids:
            raise DeepProfileError("sample-profile process ids must be unique")
        process_ids.add(pid)
        snapshot_kind = _snapshot_kind(document, "sample-profile")
        registration_only = _registration_only(document, "sample-profile", snapshot_kind)
        publication_accumulator.add(
            document,
            pid=pid,
            snapshot_kind=snapshot_kind,
            registration_only=registration_only,
        )
        if snapshot_kind == "checkpoint":
            checkpoint_process_ids.add(pid)
        if registration_only:
            registration_only_process_ids.add(pid)
        document_interval_ns = _integer(document.get("interval_ns"), "sample-profile interval")
        if document_interval_ns <= 0 or document_interval_ns > 1_000_000_000:
            raise DeepProfileError("sample-profile interval is unsupported")
        if interval_ns and document_interval_ns != interval_ns:
            raise DeepProfileError("sample-profile intervals must be consistent")
        interval_ns = document_interval_ns
        document_truncated = _boolean(document.get("truncated"), "sample-profile truncation")
        document_sample_count = _integer(
            document.get("sample_count"), "sample-profile sample count"
        )
        document_thread_sample_count = _integer(
            document.get("thread_sample_count"),
            "sample-profile thread sample count",
        )
        document_dropped_frame_sample_count = _integer(
            document.get("dropped_frame_sample_count"),
            "sample-profile dropped frame sample count",
        )
        document_dropped_edge_sample_count = _integer(
            document.get("dropped_edge_sample_count"),
            "sample-profile dropped edge sample count",
        )
        document_callback_error_count = _integer(
            document.get("callback_error_count"),
            "sample-profile callback error count",
        )
        raw_functions = _list(document.get("functions"), "sample-profile functions")
        raw_edges = _list(document.get("edges"), "sample-profile edges")
        if registration_only and (
            document_truncated
            or document_sample_count
            or document_thread_sample_count
            or document_dropped_frame_sample_count
            or document_dropped_edge_sample_count
            or document_callback_error_count
            or raw_functions
            or raw_edges
        ):
            raise DeepProfileError("sample-profile registration must not contain sample evidence")
        truncated = document_truncated or truncated
        sample_count = _bounded_sum(
            sample_count,
            document_sample_count,
            "sample-profile sample count",
        )
        thread_sample_count = _bounded_sum(
            thread_sample_count,
            document_thread_sample_count,
            "sample-profile thread sample count",
        )
        dropped_frame_sample_count = _bounded_sum(
            dropped_frame_sample_count,
            document_dropped_frame_sample_count,
            "sample-profile dropped frame sample count",
        )
        dropped_edge_sample_count = _bounded_sum(
            dropped_edge_sample_count,
            document_dropped_edge_sample_count,
            "sample-profile dropped edge sample count",
        )
        callback_error_count = _bounded_sum(
            callback_error_count,
            document_callback_error_count,
            "sample-profile callback error count",
        )
        by_identifier = _sample_functions(raw_functions)
        for raw_edge in raw_edges:
            _sample_edge(raw_edge, by_identifier)

    semantic_evidence = _semantic_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    http_evidence = _http_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    network_evidence = _network_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    network_setup_evidence = _network_setup_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    logical_operation_evidence = _logical_operation_capture_evidence(
        payloads,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
    publication_metrics = publication_accumulator.result()
    selected_function_keys, function_ranking_bytes = _select_sample_function_keys(
        payloads,
        session.directory,
    )
    functions, selection_dropped_sample_count, selected_edge_keys, edge_ranking_bytes = (
        _aggregate_sample_functions_and_select_edges(
            payloads,
            selected_function_keys,
            session.directory,
        )
    )
    if selection_dropped_sample_count:
        truncated = True
        dropped_frame_sample_count = _bounded_sum(
            dropped_frame_sample_count,
            selection_dropped_sample_count,
            "sample-profile dropped frame sample count",
        )
    edges: dict[tuple[_FunctionIdentity, _FunctionIdentity], _SampleEdgeAggregate] = {}
    for payload in payloads:
        document = _parse_document(payload)
        by_identifier = _sample_functions(
            _list(document.get("functions"), "sample-profile functions")
        )
        for raw_edge in _list(document.get("edges"), "sample-profile edges"):
            source, target, edge_sample_count = _sample_edge(raw_edge, by_identifier)
            key = (source, target)
            source_key = _function_ranking_key(source)[0]
            target_key = _function_ranking_key(target)[0]
            if (
                source == target
                or source_key not in selected_function_keys
                or target_key not in selected_function_keys
            ):
                continue
            if source_key + target_key not in selected_edge_keys:
                truncated = True
                dropped_edge_sample_count = _bounded_sum(
                    dropped_edge_sample_count,
                    edge_sample_count,
                    "sample-profile dropped edge sample count",
                )
                continue
            aggregate_edge = edges.get(key)
            if aggregate_edge is None:
                aggregate_edge = _SampleEdgeAggregate()
                edges[key] = aggregate_edge
            aggregate_edge.sample_count = _bounded_sum(
                aggregate_edge.sample_count,
                edge_sample_count,
                "sample-profile edge sample count",
            )
    interval_seconds = interval_ns / 1_000_000_000
    profile_events = tuple(
        Event(
            id=_sample_event_id(identity),
            kind="python.stack.sample",
            name=identity.name,
            entity_id=entity_id,
            started_at_ns=None,
            finished_at_ns=None,
            clock_domain=None,
            uncertainty_ns=None,
            sequence=None,
            attributes={
                "source": "python-sampler",
                "scope": identity.scope,
                "module": identity.module,
                "qualname": identity.qualname,
                "filename": identity.filename,
                "firstlineno": identity.firstlineno,
                "sample_count": aggregate.sample_count,
                "leaf_sample_count": aggregate.leaf_sample_count,
                "estimated_total_seconds": aggregate.sample_count * interval_seconds,
                "estimated_leaf_seconds": aggregate.leaf_sample_count * interval_seconds,
                "process_count": len(aggregate.processes or ()),
                "interval_seconds": interval_seconds,
                "processes": _sample_process_contributions(
                    aggregate,
                    root_process_id,
                    interval_seconds,
                ),
            },
        )
        for identity, aggregate in sorted(functions.items(), key=lambda item: item[0].name)
    )
    normalized_events = (
        profile_events
        + semantic_evidence.caller_events
        + semantic_evidence.subprocess_events
        + http_evidence.caller_events
        + http_evidence.request_events
        + network_evidence.caller_events
        + network_evidence.connection_events
        + network_setup_evidence.caller_events
        + network_setup_evidence.phase_events
        + logical_operation_evidence.caller_events
        + logical_operation_evidence.operation_events
    )
    event_ids = {identity: _sample_event_id(identity) for identity in functions}
    profile_edges = tuple(
        CausalEdge(
            event_ids[source],
            event_ids[target],
            "stack_parent",
            1.0,
            {
                "source": "python-sampler",
                "sample_count": aggregate.sample_count,
                "estimated_seconds": aggregate.sample_count * interval_seconds,
            },
        )
        for (source, target), aggregate in sorted(
            edges.items(), key=lambda item: (item[0][0].name, item[0][1].name)
        )
    )
    normalized_edges = (
        profile_edges
        + semantic_evidence.caller_edges
        + http_evidence.caller_edges
        + network_evidence.caller_edges
        + network_setup_evidence.caller_edges
        + logical_operation_evidence.caller_edges
    )
    if any(
        not math.isfinite(value)
        for event in profile_events
        for value in (
            event.attributes["estimated_total_seconds"],
            event.attributes["estimated_leaf_seconds"],
        )
        if isinstance(value, float)
    ):
        raise DeepProfileError("sample-profile normalized estimates exceed the numeric range")
    normalization_duration_ns = max(0, time.perf_counter_ns() - normalization_started_ns)
    return DeepProfileResult(
        events=normalized_events,
        edges=normalized_edges,
        process_count=len(process_ids),
        process_ids=tuple(sorted(process_ids)),
        truncated=truncated,
        dropped_call_count=dropped_frame_sample_count,
        dropped_edge_count=dropped_edge_sample_count,
        callback_error_count=callback_error_count,
        mode="sample",
        sample_count=sample_count,
        thread_sample_count=thread_sample_count,
        interval_ns=interval_ns,
        checkpoint_process_count=len(checkpoint_process_ids),
        registration_only_process_count=len(registration_only_process_ids),
        dropped_profile_process_count=transport_stats.dropped_profile_process_count,
        dropped_profile_process_count_truncated=(
            transport_stats.dropped_profile_process_count_truncated
        ),
        transport=_profile_transport(session, publication_metrics),
        collector_error_count=session.collector_error_count,
        snapshot_metrics_status=transport_stats.metrics_status,
        snapshot_message_count=transport_stats.message_count,
        snapshot_payload_bytes=transport_stats.payload_bytes,
        max_snapshot_payload_bytes=transport_stats.max_payload_bytes,
        snapshot_serialization_ns=transport_stats.serialization_ns,
        max_snapshot_serialization_ns=transport_stats.max_serialization_ns,
        checkpoint_snapshot_message_count=transport_stats.checkpoint_message_count,
        checkpoint_snapshot_payload_bytes=transport_stats.checkpoint_payload_bytes,
        max_checkpoint_snapshot_payload_bytes=transport_stats.max_checkpoint_payload_bytes,
        checkpoint_snapshot_serialization_ns=transport_stats.checkpoint_serialization_ns,
        max_checkpoint_snapshot_serialization_ns=(transport_stats.max_checkpoint_serialization_ns),
        publication_metrics_status=publication_metrics.status,
        publication_fallback_process_ids=publication_metrics.fallback_process_ids,
        publication_socket_attempted_process_count=(
            publication_metrics.socket_attempted_process_count
        ),
        publication_socket_failure_ns=publication_metrics.socket_failure_ns,
        max_publication_socket_failure_ns=publication_metrics.max_socket_failure_ns,
        normalization_metrics_status="available",
        normalization_duration_ns=normalization_duration_ns,
        ranking_database_peak_bytes=max(function_ranking_bytes, edge_ranking_bytes),
        semantic_capture_status=semantic_evidence.status,
        semantic_capture_process_count=semantic_evidence.process_count,
        subprocess_event_count=len(semantic_evidence.subprocess_events),
        dropped_subprocess_count=semantic_evidence.dropped_subprocess_count,
        semantic_callback_error_count=semantic_evidence.callback_error_count,
        semantic_caller_event_count=len(semantic_evidence.caller_events),
        semantic_caller_edge_count=len(semantic_evidence.caller_edges),
        caller_attribution_status=semantic_evidence.caller_attribution_status,
        attributed_subprocess_count=semantic_evidence.attributed_subprocess_count,
        unattributed_subprocess_count=semantic_evidence.unattributed_subprocess_count,
        invalid_caller_count=semantic_evidence.invalid_caller_count,
        caller_callback_error_count=semantic_evidence.caller_callback_error_count,
        http_capture_status=http_evidence.status,
        http_capture_process_count=http_evidence.process_count,
        http_request_event_count=len(http_evidence.request_events),
        dropped_http_request_count=http_evidence.dropped_request_count,
        http_callback_error_count=http_evidence.callback_error_count,
        http_caller_event_count=len(http_evidence.caller_events),
        http_caller_edge_count=len(http_evidence.caller_edges),
        http_caller_attribution_status=http_evidence.caller_attribution_status,
        attributed_http_request_count=http_evidence.attributed_request_count,
        unattributed_http_request_count=http_evidence.unattributed_request_count,
        invalid_http_caller_count=http_evidence.invalid_caller_count,
        http_caller_callback_error_count=http_evidence.caller_callback_error_count,
        http_adapters=http_evidence.adapters,
        network_capture_status=network_evidence.status,
        network_capture_process_count=network_evidence.process_count,
        network_connection_event_count=len(network_evidence.connection_events),
        dropped_network_connection_count=network_evidence.dropped_connection_count,
        network_callback_error_count=network_evidence.callback_error_count,
        network_caller_event_count=len(network_evidence.caller_events),
        network_caller_edge_count=len(network_evidence.caller_edges),
        network_caller_attribution_status=network_evidence.caller_attribution_status,
        attributed_network_connection_count=network_evidence.attributed_connection_count,
        unattributed_network_connection_count=network_evidence.unattributed_connection_count,
        invalid_network_caller_count=network_evidence.invalid_caller_count,
        network_caller_callback_error_count=network_evidence.caller_callback_error_count,
        network_adapters=network_evidence.adapters,
        network_setup_capture_status=network_setup_evidence.status,
        network_setup_capture_process_count=network_setup_evidence.process_count,
        network_setup_event_count=len(network_setup_evidence.phase_events),
        dropped_network_setup_count=network_setup_evidence.dropped_phase_count,
        network_setup_callback_error_count=network_setup_evidence.callback_error_count,
        network_setup_caller_event_count=len(network_setup_evidence.caller_events),
        network_setup_caller_edge_count=len(network_setup_evidence.caller_edges),
        network_setup_caller_attribution_status=(network_setup_evidence.caller_attribution_status),
        attributed_network_setup_count=network_setup_evidence.attributed_phase_count,
        unattributed_network_setup_count=network_setup_evidence.unattributed_phase_count,
        invalid_network_setup_caller_count=network_setup_evidence.invalid_caller_count,
        network_setup_caller_callback_error_count=(
            network_setup_evidence.caller_callback_error_count
        ),
        network_setup_adapters=network_setup_evidence.adapters,
        logical_operation_capture_status=logical_operation_evidence.status,
        logical_operation_capture_process_count=logical_operation_evidence.process_count,
        logical_operation_event_count=len(logical_operation_evidence.operation_events),
        dropped_logical_operation_count=(logical_operation_evidence.dropped_operation_count),
        logical_operation_callback_error_count=(logical_operation_evidence.callback_error_count),
        logical_operation_caller_event_count=len(logical_operation_evidence.caller_events),
        logical_operation_caller_edge_count=len(logical_operation_evidence.caller_edges),
        logical_operation_caller_attribution_status=(
            logical_operation_evidence.caller_attribution_status
        ),
        attributed_logical_operation_count=(logical_operation_evidence.attributed_operation_count),
        unattributed_logical_operation_count=(
            logical_operation_evidence.unattributed_operation_count
        ),
        invalid_logical_operation_caller_count=(logical_operation_evidence.invalid_caller_count),
        logical_operation_caller_callback_error_count=(
            logical_operation_evidence.caller_callback_error_count
        ),
        logical_operation_adapters=logical_operation_evidence.adapters,
    )


def load_python_profile(
    session: DeepProfileSession,
    *,
    entity_id: str,
    root_process_id: int | None = None,
) -> DeepProfileResult:
    if session.mode == "sample":
        return load_sample_profile(
            session,
            entity_id=entity_id,
            root_process_id=root_process_id,
        )
    return load_deep_profile(
        session,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
