"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import os
from typing import Literal

import runtime_tools._profile_bootstrap_support as _profile_support


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


PROFILE_PUBLICATION_METRICS_VERSION = _profile_support.PUBLICATION_METRICS_VERSION


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


_SEMANTIC_BOOTSTRAP_NAME = "_semantic_capture_bootstrap"


_PROFILE_SUPPORT_BOOTSTRAP_NAME = "_profile_bootstrap_support.py"


_SNAPSHOT_PROTOCOL_MAGIC = _profile_support.SNAPSHOT_PROTOCOL_MAGIC


_SNAPSHOT_HEADER = _profile_support.SNAPSHOT_HEADER


_SNAPSHOT_KIND_CHECKPOINT = _profile_support.SNAPSHOT_KIND_CHECKPOINT


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


def _process_role(pid: int, root_process_id: int | None) -> str:
    if root_process_id is None:
        return "unknown"
    return "root" if pid == root_process_id else "descendant"


def _bounded_sum(left: int, right: int, label: str) -> int:
    result = left + right
    if result > _MAX_INTEGER:
        raise DeepProfileError(f"{label} exceeds the supported integer range")
    return result


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
