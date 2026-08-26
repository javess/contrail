"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from typing import Literal

MAX_PROFILE_PROCESS_CONTRIBUTIONS = 128


MAX_PROFILE_COVERAGE_GAPS = 100


MAX_SUBPROCESS_CALL_SUMMARIES = 100


MAX_HTTP_REQUEST_SUMMARIES = 100


MAX_NETWORK_CONNECTION_SUMMARIES = 100


MAX_NETWORK_SETUP_SUMMARIES = 100


MAX_LOGICAL_OPERATION_SUMMARIES = 100


MAX_NATIVE_FUNCTIONS_PER_PROCESS = 2_000


MAX_NATIVE_EDGES_PER_PROCESS = 10_000


MAX_PYTHON_FUNCTIONS_PER_PROCESS = 2_000


MAX_PYTHON_HOTSPOTS = 100


PYTHON_EXCEPTION_CHURN_MIN_EVENTS = 10


PYTHON_EXCEPTION_CHURN_MIN_EVENTS_PER_CALL = 0.5


NETWORK_CHURN_MIN_CONNECTIONS = 10


NETWORK_CHURN_MIN_RATE_PER_SECOND = 5.0


NETWORK_SETUP_MIN_SECONDS = 0.05


NETWORK_SETUP_MIN_RUN_RATIO = 0.25


LOGICAL_OPERATION_MIN_SECONDS = 0.05


LOGICAL_OPERATION_MIN_RUN_RATIO = 0.25


HTTP_CAPTURE_ADAPTERS = frozenset(
    {
        "stdlib.http.client",
        "httpcore.sync",
        "httpcore.async",
        "aiohttp.async",
    }
)


NETWORK_CAPTURE_ADAPTERS = frozenset(
    {
        "stdlib.socket.connect",
        "asyncio.create_connection",
        "asyncio.create_unix_connection",
    }
)


NETWORK_SETUP_CAPTURE_ADAPTERS = frozenset(
    {
        "stdlib.socket.getaddrinfo",
        "stdlib.ssl.SSLObject.do_handshake",
        "stdlib.ssl.SSLSocket.do_handshake",
    }
)


LOGICAL_OPERATION_CAPTURE_ADAPTER_CATEGORIES = {
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


LOGICAL_OPERATION_CAPTURE_ADAPTERS = frozenset(LOGICAL_OPERATION_CAPTURE_ADAPTER_CATEGORIES)


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


type CaptureStatus = Literal["complete", "partial", "truncated", "unavailable", "invalid"]
