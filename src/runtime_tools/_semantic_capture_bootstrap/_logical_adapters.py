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

import functools
import sys
from collections.abc import Awaitable, Callable
from types import ModuleType
from typing import cast

from ._asyncio_adapters import _logical_async_wrapper, _logical_sync_wrapper, _patch_executor
from ._constants import _LOGICAL_WRAPPER_MARKER
from ._http_adapters import _module_values
from ._state import STATE


def _patch_sqlite3(module: ModuleType) -> bool:
    connection_adapter = "stdlib.sqlite3.Connection"
    cursor_adapter = "stdlib.sqlite3.Cursor"
    connection_type = getattr(module, "Connection", None)
    cursor_type = getattr(module, "Cursor", None)
    original_connect = getattr(module, "connect", None)
    dbapi2 = sys.modules.get("sqlite3.dbapi2")
    dbapi2_connection = getattr(dbapi2, "Connection", None)
    dbapi2_cursor = getattr(dbapi2, "Cursor", None)
    dbapi2_connect = getattr(dbapi2, "connect", None)
    if (
        not isinstance(connection_type, type)
        or not isinstance(cursor_type, type)
        or not callable(original_connect)
    ):
        return False
    if (
        getattr(connection_type, _LOGICAL_WRAPPER_MARKER, False) is True
        and getattr(cursor_type, _LOGICAL_WRAPPER_MARKER, False) is True
        and getattr(original_connect, _LOGICAL_WRAPPER_MARKER, False) is True
    ):
        return True
    connection_operations = ("execute", "executemany", "executescript", "commit", "rollback")
    cursor_operations = ("execute", "executemany", "executescript")
    connection_methods: dict[str, object] = {}
    cursor_methods: dict[str, object] = {}
    for operation in connection_operations:
        original = getattr(connection_type, operation, None)
        if not callable(original):
            return False
        connection_methods[operation] = _logical_sync_wrapper(
            cast(Callable[..., object], original),
            category="database",
            operation=operation,
            adapter=connection_adapter,
        )
    for operation in cursor_operations:
        original = getattr(cursor_type, operation, None)
        if not callable(original):
            return False
        cursor_methods[operation] = _logical_sync_wrapper(
            cast(Callable[..., object], original),
            category="database",
            operation=operation,
            adapter=cursor_adapter,
        )
    try:
        observed_cursor_type = type(
            "Cursor",
            (cursor_type,),
            {
                "__module__": "sqlite3",
                _LOGICAL_WRAPPER_MARKER: True,
                **cursor_methods,
            },
        )
        original_cursor = cast(Callable[..., object], connection_type.cursor)

        @functools.wraps(original_cursor)
        def observed_cursor(
            instance: object,
            *positional: object,
            **keywords: object,
        ) -> object:
            if positional or "factory" in keywords:
                return original_cursor(instance, *positional, **keywords)
            return original_cursor(instance, observed_cursor_type)

        observed_connection_type = type(
            "Connection",
            (connection_type,),
            {
                "__module__": "sqlite3",
                _LOGICAL_WRAPPER_MARKER: True,
                "cursor": observed_cursor,
                **connection_methods,
            },
        )
        original_connect_callable = cast(Callable[..., object], original_connect)

        @functools.wraps(original_connect_callable)
        def observed_connect(*positional: object, **keywords: object) -> object:
            if len(positional) >= 6 or "factory" in keywords:
                return original_connect_callable(*positional, **keywords)
            return original_connect_callable(
                *positional,
                factory=observed_connection_type,
                **keywords,
            )

        setattr(observed_connect, _LOGICAL_WRAPPER_MARKER, True)

        ModuleType.__setattr__(module, "Connection", observed_connection_type)
        ModuleType.__setattr__(module, "Cursor", observed_cursor_type)
        ModuleType.__setattr__(module, "connect", observed_connect)
        if isinstance(dbapi2, ModuleType):
            ModuleType.__setattr__(dbapi2, "Connection", observed_connection_type)
            ModuleType.__setattr__(dbapi2, "Cursor", observed_cursor_type)
            ModuleType.__setattr__(dbapi2, "connect", observed_connect)
    except BaseException:
        try:
            ModuleType.__setattr__(module, "Connection", connection_type)
            ModuleType.__setattr__(module, "Cursor", cursor_type)
            ModuleType.__setattr__(module, "connect", original_connect)
            if isinstance(dbapi2, ModuleType):
                ModuleType.__setattr__(dbapi2, "Connection", dbapi2_connection)
                ModuleType.__setattr__(dbapi2, "Cursor", dbapi2_cursor)
                ModuleType.__setattr__(dbapi2, "connect", dbapi2_connect)
        except BaseException:
            pass
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.update({connection_adapter, cursor_adapter})
    return True


def _patch_optional_logical_type(
    module: ModuleType,
    *,
    owner_name: str,
    adapter: str,
    methods: dict[str, tuple[str, str, bool]],
) -> bool:
    owner = _module_values(module).get(owner_name)
    if not isinstance(owner, type):
        return False
    originals: dict[str, object] = {}
    wrappers: dict[str, object] = {}
    for method_name, (category, operation, is_async) in methods.items():
        try:
            original = type.__getattribute__(owner, method_name)
        except BaseException:
            return False
        if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
            continue
        if not callable(original):
            return False
        originals[method_name] = original
        wrappers[method_name] = (
            _logical_async_wrapper(
                cast(Callable[..., Awaitable[object]], original),
                category=category,
                operation=operation,
                adapter=adapter,
            )
            if is_async
            else _logical_sync_wrapper(
                cast(Callable[..., object], original),
                category=category,
                operation=operation,
                adapter=adapter,
                sampled_caller=False,
            )
        )
    try:
        for method_name, wrapper in wrappers.items():
            type.__setattr__(owner, method_name, wrapper)
    except BaseException:
        for method_name, original in originals.items():
            try:
                type.__setattr__(owner, method_name, original)
            except BaseException:
                pass
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.add(adapter)
    return True


def _patch_optional_logical_operations(module_name: str, module: ModuleType) -> bool:
    if module_name == "concurrent.futures.thread":
        return _patch_executor(
            module,
            owner_name="ThreadPoolExecutor",
            adapter="stdlib.concurrent.futures.ThreadPoolExecutor",
        )
    if module_name == "concurrent.futures.process":
        return _patch_executor(
            module,
            owner_name="ProcessPoolExecutor",
            adapter="stdlib.concurrent.futures.ProcessPoolExecutor",
        )
    if module_name == "sqlalchemy.engine.base":
        return _patch_optional_logical_type(
            module,
            owner_name="Connection",
            adapter="sqlalchemy.engine.Connection",
            methods={
                "commit": ("database", "commit", False),
                "execute": ("database", "execute", False),
                "exec_driver_sql": ("database", "execute", False),
                "rollback": ("database", "rollback", False),
            },
        )
    if module_name == "sqlalchemy.orm.session":
        return _patch_optional_logical_type(
            module,
            owner_name="Session",
            adapter="sqlalchemy.orm.Session",
            methods={
                "commit": ("database", "commit", False),
                "execute": ("database", "execute", False),
                "rollback": ("database", "rollback", False),
            },
        )
    if module_name == "sqlalchemy.ext.asyncio.engine":
        return _patch_optional_logical_type(
            module,
            owner_name="AsyncConnection",
            adapter="sqlalchemy.ext.asyncio.AsyncConnection",
            methods={
                "commit": ("database", "commit", True),
                "execute": ("database", "execute", True),
                "exec_driver_sql": ("database", "execute", True),
                "rollback": ("database", "rollback", True),
            },
        )
    if module_name == "sqlalchemy.ext.asyncio.session":
        return _patch_optional_logical_type(
            module,
            owner_name="AsyncSession",
            adapter="sqlalchemy.ext.asyncio.AsyncSession",
            methods={
                "commit": ("database", "commit", True),
                "execute": ("database", "execute", True),
                "rollback": ("database", "rollback", True),
            },
        )
    if module_name == "redis.client":
        outcomes = (
            _patch_optional_logical_type(
                module,
                owner_name="Redis",
                adapter="redis.Redis",
                methods={"execute_command": ("cache", "command", False)},
            ),
            _patch_optional_logical_type(
                module,
                owner_name="Pipeline",
                adapter="redis.Pipeline",
                methods={"execute": ("cache", "batch", False)},
            ),
        )
        return all(outcomes)
    if module_name == "redis.asyncio.client":
        outcomes = (
            _patch_optional_logical_type(
                module,
                owner_name="Redis",
                adapter="redis.asyncio.Redis",
                methods={"execute_command": ("cache", "command", True)},
            ),
            _patch_optional_logical_type(
                module,
                owner_name="Pipeline",
                adapter="redis.asyncio.Pipeline",
                methods={"execute": ("cache", "batch", True)},
            ),
        )
        return all(outcomes)
    if module_name == "pika.adapters.blocking_connection":
        return _patch_optional_logical_type(
            module,
            owner_name="BlockingChannel",
            adapter="pika.BlockingChannel",
            methods={
                "basic_get": ("broker", "consume", False),
                "basic_publish": ("broker", "publish", False),
            },
        )
    if module_name == "aiokafka.producer.producer":
        return _patch_optional_logical_type(
            module,
            owner_name="AIOKafkaProducer",
            adapter="aiokafka.AIOKafkaProducer",
            methods={"send_and_wait": ("broker", "publish", True)},
        )
    if module_name == "aiokafka.consumer.consumer":
        return _patch_optional_logical_type(
            module,
            owner_name="AIOKafkaConsumer",
            adapter="aiokafka.AIOKafkaConsumer",
            methods={
                "getmany": ("broker", "consume", True),
                "getone": ("broker", "consume", True),
            },
        )
    return False
