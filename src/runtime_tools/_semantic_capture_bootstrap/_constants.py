"""Standalone semantic observer copied beside a capture ``sitecustomize``.

The module must remain standard-library-only. It deliberately records no
subprocess arguments, environment values, working directories, or output, and
no HTTP server names, URLs, headers, request or response bodies, or credentials.
Deep capture also observes selected database, cache, queue, broker, executor,
and scheduler operation boundaries, including executor task submission-to-completion
intervals, without retaining statements, parameters, keys, destinations,
payloads, queue items, callables, awaitables, task names, context values,
arguments, or return values.
"""

from __future__ import annotations

import contextvars
from collections.abc import Callable

FORMAT_VERSION = 2


MAX_SUBPROCESS_RECORDS = 256


MAX_HTTP_REQUEST_RECORDS = 256


MAX_NETWORK_CONNECTION_RECORDS = 256


MAX_NETWORK_SETUP_RECORDS = 256


MAX_LOGICAL_OPERATION_RECORDS = 256


MAX_EXECUTABLE_CHARACTERS = 256


MAX_HTTP_METHOD_CHARACTERS = 32


MAX_CALLER_TEXT_CHARACTERS = 1_024


MAX_COUNTER = (1 << 63) - 1


_LOGICAL_WRAPPER_MARKER = "__contrail_logical_operation_wrapper__"


_ASYNCIO_ENSURE_FUTURE_ENGINE_MARKER = "__contrail_ensure_future_engine_wrapper__"


_ASYNCIO_ENSURE_FUTURE_ENTRY_MARKER = "__contrail_ensure_future_entry_wrapper__"


HTTP_ADAPTERS = frozenset(
    {
        "stdlib.http.client",
        "httpcore.sync",
        "httpcore.async",
        "aiohttp.async",
    }
)


OPTIONAL_HTTP_MODULES = frozenset({"httpcore", "aiohttp.client"})


NETWORK_ADAPTERS = frozenset(
    {
        "stdlib.socket.connect",
        "asyncio.create_connection",
        "asyncio.create_unix_connection",
    }
)


NETWORK_SETUP_ADAPTERS = frozenset(
    {
        "stdlib.socket.getaddrinfo",
        "stdlib.ssl.SSLObject.do_handshake",
        "stdlib.ssl.SSLSocket.do_handshake",
    }
)


LOGICAL_OPERATION_ADAPTER_CATEGORIES = {
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


LOGICAL_OPERATION_ADAPTERS = frozenset(LOGICAL_OPERATION_ADAPTER_CATEGORIES)


OPTIONAL_LOGICAL_OPERATION_MODULES = frozenset(
    {
        "aiokafka.consumer.consumer",
        "aiokafka.producer.producer",
        "concurrent.futures.process",
        "concurrent.futures.thread",
        "pika.adapters.blocking_connection",
        "redis.asyncio.client",
        "redis.client",
        "sqlalchemy.engine.base",
        "sqlalchemy.ext.asyncio.engine",
        "sqlalchemy.ext.asyncio.session",
        "sqlalchemy.orm.session",
    }
)


STDLIB_LOGICAL_OPERATION_MODULES = frozenset(
    {
        "asyncio",
        "asyncio.queues",
        "asyncio.taskgroups",
        "asyncio.tasks",
        "queue",
        "sqlite3",
        "sqlite3.dbapi2",
    }
)


LOGICAL_OPERATION_MODULES = OPTIONAL_LOGICAL_OPERATION_MODULES | STDLIB_LOGICAL_OPERATION_MODULES


LAZY_ADAPTER_MODULES = (
    OPTIONAL_HTTP_MODULES
    | LOGICAL_OPERATION_MODULES
    | {
        "asyncio.base_events",
        "asyncio.unix_events",
    }
)


type CallerIdentity = tuple[str, str, str, int, str, str]


type CallerProvider = Callable[[], CallerIdentity | None]


type ActiveRecordKey = tuple[str, int]


type ActiveToken = tuple[int, ActiveRecordKey]


type NetworkSuppressionToken = contextvars.Token[bool]
