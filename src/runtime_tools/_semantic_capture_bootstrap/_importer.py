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

import importlib.abc
import importlib.machinery
import sys
from collections.abc import Sequence
from types import ModuleType

from ._asyncio_adapters import (
    _patch_async_queue,
    _patch_asyncio,
    _patch_asyncio_task_group,
    _patch_asyncio_task_scheduling,
    _patch_asyncio_unix,
    _patch_sync_queue,
)
from ._constants import (
    LAZY_ADAPTER_MODULES,
    LOGICAL_OPERATION_MODULES,
    OPTIONAL_LOGICAL_OPERATION_MODULES,
)
from ._http_adapters import _patch_aiohttp, _patch_httpcore
from ._logical_adapters import _patch_optional_logical_operations, _patch_sqlite3
from ._records import (
    _record_http_callback_error,
    _record_logical_operation_callback_error,
    _record_network_callback_error,
)
from ._state import STATE


def _activate_lazy_adapter(module_name: str, module: ModuleType) -> None:
    try:
        patched = (
            _patch_httpcore(module)
            if module_name == "httpcore"
            else _patch_aiohttp(module)
            if module_name == "aiohttp.client"
            else _patch_asyncio(module)
            if module_name == "asyncio.base_events"
            else _patch_asyncio_unix(module)
            if module_name == "asyncio.unix_events"
            else _patch_asyncio_task_scheduling(module)
            if module_name in {"asyncio", "asyncio.tasks"}
            and STATE._logical_operation_capture_enabled
            else _patch_asyncio_task_group(module)
            if module_name == "asyncio.taskgroups" and STATE._logical_operation_capture_enabled
            else _patch_sync_queue(module)
            if module_name == "queue" and STATE._logical_operation_capture_enabled
            else _patch_async_queue(module)
            if module_name == "asyncio.queues" and STATE._logical_operation_capture_enabled
            else _patch_sqlite3(module)
            if module_name in {"sqlite3", "sqlite3.dbapi2"}
            and STATE._logical_operation_capture_enabled
            else _patch_optional_logical_operations(module_name, module)
            if module_name in OPTIONAL_LOGICAL_OPERATION_MODULES
            and STATE._logical_operation_capture_enabled
            else True
        )
        if not patched:
            if module_name in LOGICAL_OPERATION_MODULES:
                _record_logical_operation_callback_error()
            elif module_name in {"asyncio.base_events", "asyncio.unix_events"}:
                _record_network_callback_error()
            else:
                _record_http_callback_error()
    except BaseException:
        if module_name in LOGICAL_OPERATION_MODULES:
            _record_logical_operation_callback_error()
        elif module_name in {"asyncio.base_events", "asyncio.unix_events"}:
            _record_network_callback_error()
        else:
            _record_http_callback_error()


class _OptionalAdapterLoader(importlib.abc.Loader):
    def __init__(
        self,
        loader: importlib.abc.Loader,
        module_name: str,
    ) -> None:
        self._loader = loader
        self._module_name = module_name

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType | None:
        return self._loader.create_module(spec)

    def exec_module(self, module: ModuleType) -> None:
        self._loader.exec_module(module)
        try:
            spec = module.__spec__
            if spec is not None:
                spec.loader = self._loader
            module.__loader__ = self._loader
        except BaseException:
            if self._module_name in LOGICAL_OPERATION_MODULES:
                _record_logical_operation_callback_error()
            elif self._module_name.startswith("asyncio."):
                _record_network_callback_error()
            else:
                _record_http_callback_error()
        _activate_lazy_adapter(self._module_name, module)

    def __getattr__(self, name: str) -> object:
        return getattr(self._loader, name)


class _OptionalAdapterFinder(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if fullname not in LAZY_ADAPTER_MODULES:
            return None
        if fullname in LOGICAL_OPERATION_MODULES and not STATE._logical_operation_capture_enabled:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        if spec is None or spec.loader is None:
            return spec
        spec.loader = _OptionalAdapterLoader(spec.loader, fullname)
        return spec


_optional_adapter_finder = _OptionalAdapterFinder()


def _install_lazy_adapters() -> None:
    for module_name in LAZY_ADAPTER_MODULES:
        loaded = sys.modules.get(module_name)
        if isinstance(loaded, ModuleType):
            _activate_lazy_adapter(module_name, loaded)
    if any(finder is _optional_adapter_finder for finder in sys.meta_path):
        return
    insert_at = next(
        (
            index
            for index, finder in enumerate(sys.meta_path)
            if finder is importlib.machinery.PathFinder
        ),
        len(sys.meta_path),
    )
    sys.meta_path.insert(insert_at, _optional_adapter_finder)
