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
from collections.abc import Awaitable, Callable
from types import ModuleType
from typing import cast

from ._boundaries import (
    _finish_network_record,
    _network_family,
    _reserve_network_boundary,
    _run_async_logical_operation,
    _run_asyncio_create_task,
    _run_executor_submit,
    _run_logical_operation,
)
from ._constants import (
    _ASYNCIO_ENSURE_FUTURE_ENGINE_MARKER,
    _ASYNCIO_ENSURE_FUTURE_ENTRY_MARKER,
    _LOGICAL_WRAPPER_MARKER,
    NetworkSuppressionToken,
)
from ._http_adapters import _module_values
from ._network_hooks import _run_asyncio_ensure_future, _run_asyncio_task_scheduling_entry_point
from ._records import (
    _begin_network_suppression,
    _end_network_suppression,
    _record_network_callback_error,
)
from ._state import STATE


def _patch_asyncio(module: ModuleType) -> bool:
    values = _module_values(module)
    loop_type = values.get("BaseEventLoop")
    if not isinstance(loop_type, type):
        return False

    try:
        raw_tcp_original = type.__getattribute__(loop_type, "create_connection")
    except BaseException:
        return False
    if not callable(raw_tcp_original):
        return False
    tcp_original = cast(Callable[..., Awaitable[object]], raw_tcp_original)

    @functools.wraps(tcp_original)
    async def observed_tcp(
        loop: object,
        protocol_factory: object,
        host: object = None,
        port: object = None,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        suppression_token: NetworkSuppressionToken | None = None
        should_observe = (
            not STATE._network_capture_suppressed.get()
            and host is not None
            and keywords.get("sock") is None
        )
        try:
            if should_observe:
                server_port = port if type(port) is int and 0 < port <= 65_535 else None
                identifier = _reserve_network_boundary(
                    transport="tcp",
                    family=_network_family(keywords.get("family")),
                    server_port=server_port,
                    adapter="asyncio.create_connection",
                    tls_requested=(
                        keywords.get("ssl") is not None and keywords.get("ssl") is not False
                    ),
                )
                suppression_token = _begin_network_suppression()
        except BaseException:
            _record_network_callback_error()
        try:
            result = await tcp_original(
                loop,
                protocol_factory,
                host,
                port,
                *positional,
                **keywords,
            )
        except BaseException as error:
            try:
                _finish_network_record(
                    identifier,
                    outcome="connect_error",
                    error_type=type(error).__name__,
                )
            except BaseException:
                _record_network_callback_error()
            raise
        finally:
            try:
                _end_network_suppression(suppression_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_network_record(identifier, outcome="connected")
        except BaseException:
            _record_network_callback_error()
        return result

    try:
        type.__setattr__(loop_type, "create_connection", observed_tcp)
    except BaseException:
        return False
    with STATE._lock:
        STATE._network_adapters.add("asyncio.create_connection")
    return True


def _patch_asyncio_unix(module: ModuleType) -> bool:
    values = _module_values(module)
    loop_type = values.get("_UnixSelectorEventLoop")
    if not isinstance(loop_type, type):
        return False
    try:
        raw_original = type.__getattribute__(loop_type, "create_unix_connection")
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., Awaitable[object]], raw_original)

    @functools.wraps(original)
    async def observed(
        loop: object,
        protocol_factory: object,
        path: object = None,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        suppression_token: NetworkSuppressionToken | None = None
        should_observe = (
            not STATE._network_capture_suppressed.get()
            and path is not None
            and keywords.get("sock") is None
        )
        try:
            if should_observe:
                identifier = _reserve_network_boundary(
                    transport="unix",
                    family="unix",
                    server_port=None,
                    adapter="asyncio.create_unix_connection",
                    tls_requested=(
                        keywords.get("ssl") is not None and keywords.get("ssl") is not False
                    ),
                )
                suppression_token = _begin_network_suppression()
        except BaseException:
            _record_network_callback_error()
        try:
            result = await original(
                loop,
                protocol_factory,
                path,
                *positional,
                **keywords,
            )
        except BaseException as error:
            try:
                _finish_network_record(
                    identifier,
                    outcome="connect_error",
                    error_type=type(error).__name__,
                )
            except BaseException:
                _record_network_callback_error()
            raise
        finally:
            try:
                _end_network_suppression(suppression_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_network_record(identifier, outcome="connected")
        except BaseException:
            _record_network_callback_error()
        return result

    try:
        type.__setattr__(loop_type, "create_unix_connection", observed)
    except BaseException:
        return False
    with STATE._lock:
        STATE._network_adapters.add("asyncio.create_unix_connection")
    return True


def _logical_sync_wrapper(
    original: Callable[..., object],
    *,
    category: str,
    operation: str,
    adapter: str,
    sampled_caller: bool = True,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(instance: object, *positional: object, **keywords: object) -> object:
        return _run_logical_operation(
            original,
            instance,
            positional,
            keywords,
            category=category,
            operation=operation,
            adapter=adapter,
            sampled_caller=sampled_caller,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _executor_submit_wrapper(
    original: Callable[..., object],
    *,
    adapter: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(instance: object, *positional: object, **keywords: object) -> object:
        return _run_executor_submit(
            original,
            instance,
            positional,
            keywords,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _asyncio_create_task_wrapper(
    original: Callable[..., object],
    *,
    adapter: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(*positional: object, **keywords: object) -> object:
        return _run_asyncio_create_task(
            original,
            positional,
            keywords,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _asyncio_task_scheduling_entry_point_wrapper(
    original: Callable[..., object],
    *,
    adapter: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(*positional: object, **keywords: object) -> object:
        return _run_asyncio_task_scheduling_entry_point(
            original,
            positional,
            keywords,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _asyncio_ensure_future_wrapper(
    original: Callable[..., object],
    *,
    adapter: str | None,
    marker: str,
) -> Callable[..., object]:
    @functools.wraps(original)
    def observed(*positional: object, **keywords: object) -> object:
        return _run_asyncio_ensure_future(
            original,
            positional,
            keywords,
            default_adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    setattr(observed, marker, True)
    return observed


def _patch_executor(
    module: ModuleType,
    *,
    owner_name: str,
    adapter: str,
) -> bool:
    executor_type = getattr(module, owner_name, None)
    if not isinstance(executor_type, type):
        return False
    original = getattr(executor_type, "submit", None)
    if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
        return True
    if not callable(original):
        return False
    wrapped = _executor_submit_wrapper(
        cast(Callable[..., object], original),
        adapter=adapter,
    )
    try:
        type.__setattr__(executor_type, "submit", wrapped)
    except BaseException:
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.add(adapter)
    return True


def _patch_asyncio_create_task(module: ModuleType) -> bool:
    original = getattr(module, "create_task", None)
    if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
        with STATE._lock:
            STATE._logical_operation_adapters.add("stdlib.asyncio.create_task")
        return True
    if not callable(original):
        return False
    adapter = "stdlib.asyncio.create_task"
    wrapped = _asyncio_create_task_wrapper(
        cast(Callable[..., object], original),
        adapter=adapter,
    )
    try:
        ModuleType.__setattr__(module, "create_task", wrapped)
    except BaseException:
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.add(adapter)
    return True


def _patch_asyncio_implicit_tasks(module: ModuleType) -> bool:
    module_name = getattr(module, "__name__", None)
    names_and_adapters = {"gather": "stdlib.asyncio.gather"}
    originals: dict[str, object] = {}
    wrappers: dict[str, object] = {}
    for name, adapter in names_and_adapters.items():
        original = getattr(module, name, None)
        if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
            continue
        if not callable(original):
            return False
        originals[name] = original
        wrappers[name] = _asyncio_task_scheduling_entry_point_wrapper(
            cast(Callable[..., object], original),
            adapter=adapter,
        )
    ensure_future = getattr(module, "ensure_future", None)
    ensure_future_marker = (
        _ASYNCIO_ENSURE_FUTURE_ENGINE_MARKER
        if module_name == "asyncio.tasks"
        else _ASYNCIO_ENSURE_FUTURE_ENTRY_MARKER
    )
    if getattr(ensure_future, ensure_future_marker, False) is not True:
        if not callable(ensure_future):
            return False
        originals["ensure_future"] = ensure_future
        wrappers["ensure_future"] = _asyncio_ensure_future_wrapper(
            cast(Callable[..., object], ensure_future),
            adapter=(None if module_name == "asyncio.tasks" else "stdlib.asyncio.ensure_future"),
            marker=ensure_future_marker,
        )
    try:
        for name, wrapper in wrappers.items():
            ModuleType.__setattr__(module, name, wrapper)
    except BaseException:
        for name, original in originals.items():
            try:
                ModuleType.__setattr__(module, name, original)
            except BaseException:
                pass
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.update(
            {"stdlib.asyncio.ensure_future", "stdlib.asyncio.gather"}
        )
    return True


def _patch_asyncio_task_scheduling(module: ModuleType) -> bool:
    return _patch_asyncio_create_task(module) and _patch_asyncio_implicit_tasks(module)


def _patch_asyncio_task_group(module: ModuleType) -> bool:
    task_group_type = getattr(module, "TaskGroup", None)
    if not isinstance(task_group_type, type):
        return False
    try:
        original = type.__getattribute__(task_group_type, "create_task")
    except BaseException:
        return False
    if getattr(original, _LOGICAL_WRAPPER_MARKER, False) is True:
        with STATE._lock:
            STATE._logical_operation_adapters.add("stdlib.asyncio.TaskGroup")
        return True
    if not callable(original):
        return False
    adapter = "stdlib.asyncio.TaskGroup"
    wrapped = _asyncio_create_task_wrapper(
        cast(Callable[..., object], original),
        adapter=adapter,
    )
    try:
        type.__setattr__(task_group_type, "create_task", wrapped)
    except BaseException:
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.add(adapter)
    return True


def _patch_sync_queue(module: ModuleType) -> bool:
    adapter = "stdlib.queue.Queue"
    queue_type = getattr(module, "Queue", None)
    if not isinstance(queue_type, type):
        return False
    if all(
        getattr(getattr(queue_type, operation, None), _LOGICAL_WRAPPER_MARKER, False) is True
        for operation in ("put", "get")
    ):
        return True
    originals: dict[str, Callable[..., object]] = {}
    for operation in ("put", "get"):
        try:
            original = type.__getattribute__(queue_type, operation)
        except BaseException:
            return False
        if not callable(original):
            return False
        originals[operation] = cast(Callable[..., object], original)
    try:
        for operation, original in originals.items():
            type.__setattr__(
                queue_type,
                operation,
                _logical_sync_wrapper(
                    original,
                    category="queue",
                    operation=operation,
                    adapter=adapter,
                ),
            )
    except BaseException:
        for operation, original in originals.items():
            try:
                type.__setattr__(queue_type, operation, original)
            except BaseException:
                pass
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.add(adapter)
    return True


def _logical_async_wrapper(
    original: Callable[..., Awaitable[object]],
    *,
    category: str,
    operation: str,
    adapter: str,
) -> Callable[..., Awaitable[object]]:
    @functools.wraps(original)
    async def observed(instance: object, *positional: object, **keywords: object) -> object:
        return await _run_async_logical_operation(
            original,
            instance,
            positional,
            keywords,
            category=category,
            operation=operation,
            adapter=adapter,
        )

    setattr(observed, _LOGICAL_WRAPPER_MARKER, True)
    return observed


def _patch_async_queue(module: ModuleType) -> bool:
    adapter = "stdlib.asyncio.Queue"
    queue_type = getattr(module, "Queue", None)
    if not isinstance(queue_type, type):
        return False
    if all(
        getattr(getattr(queue_type, operation, None), _LOGICAL_WRAPPER_MARKER, False) is True
        for operation in ("put", "get")
    ):
        return True
    originals: dict[str, Callable[..., Awaitable[object]]] = {}
    for operation in ("put", "get"):
        try:
            original = type.__getattribute__(queue_type, operation)
        except BaseException:
            return False
        if not callable(original):
            return False
        originals[operation] = cast(Callable[..., Awaitable[object]], original)
    try:
        for operation, original in originals.items():
            type.__setattr__(
                queue_type,
                operation,
                _logical_async_wrapper(
                    original,
                    category="queue",
                    operation=operation,
                    adapter=adapter,
                ),
            )
    except BaseException:
        for operation, original in originals.items():
            try:
                type.__setattr__(queue_type, operation, original)
            except BaseException:
                pass
        return False
    with STATE._lock:
        STATE._logical_operation_adapters.add(adapter)
    return True
