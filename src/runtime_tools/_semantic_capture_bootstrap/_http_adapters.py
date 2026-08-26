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
import urllib.parse
from collections.abc import Awaitable, Callable
from types import ModuleType
from typing import cast

from ._boundaries import (
    _begin_active_identifier,
    _finish_http_record,
    _http_connection_values,
    _http_scheme,
    _reserve_http_boundary,
)
from ._constants import ActiveToken, NetworkSuppressionToken
from ._records import (
    _begin_network_suppression,
    _end_active,
    _end_network_suppression,
    _record_http_callback_error,
    _record_http_caller_callback_error,
    _record_network_callback_error,
)
from ._state import STATE


def _exact_attribute(value: object, name: str) -> object:
    values = _http_connection_values(value)
    if name in values:
        return values[name]
    try:
        return object.__getattribute__(value, name)
    except BaseException:
        return None


def _httpcore_request_values(
    request: object,
    *,
    request_type: type[object],
    url_type: type[object],
) -> tuple[object, str, object] | None:
    if type(request) is not request_type:
        return None
    method = _exact_attribute(request, "method")
    url = _exact_attribute(request, "url")
    if type(url) is not url_type:
        return None
    scheme = _http_scheme(_exact_attribute(url, "scheme"))
    if scheme is None:
        return None
    return method, scheme, _exact_attribute(url, "port")


def _typed_response_status(response: object, response_type: type[object]) -> int | None:
    if type(response) is not response_type:
        return None
    status = _exact_attribute(response, "status")
    return status if type(status) is int and 100 <= status <= 999 else None


def _finish_optional_http_error(identifier: int | None, error: BaseException) -> None:
    try:
        _finish_http_record(
            identifier,
            status_code=None,
            outcome="request_error",
            error_type=type(error).__name__,
        )
    except BaseException:
        _record_http_callback_error()


def _patch_httpcore_sync(
    owner: type[object],
    *,
    request_type: type[object],
    url_type: type[object],
    response_type: type[object],
) -> bool:
    method_name = "handle_request"
    try:
        raw_original = type.__getattribute__(owner, method_name)
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., object], raw_original)

    @functools.wraps(original)
    def observed(
        instance: object,
        request: object,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        token: ActiveToken | None = None
        network_token: NetworkSuppressionToken | None = None
        try:
            request_values = _httpcore_request_values(
                request,
                request_type=request_type,
                url_type=url_type,
            )
            if request_values is not None:
                method, scheme, server_port = request_values
                identifier = _reserve_http_boundary(
                    method,
                    scheme=scheme,
                    server_port=server_port,
                    adapter="httpcore.sync",
                )
                token = _begin_active_identifier("http", identifier)
                network_token = _begin_network_suppression()
        except BaseException:
            _record_http_callback_error()
        try:
            response = original(instance, request, *positional, **keywords)
        except BaseException as exc:
            _finish_optional_http_error(identifier, exc)
            raise
        finally:
            try:
                _end_network_suppression(network_token)
                _end_active(token)
            except BaseException:
                _record_http_caller_callback_error()
        try:
            _finish_http_record(
                identifier,
                status_code=_typed_response_status(response, response_type),
                outcome="response",
            )
        except BaseException:
            _record_http_callback_error()
        return response

    try:
        type.__setattr__(owner, method_name, observed)
    except BaseException:
        return False
    with STATE._lock:
        STATE._http_adapters.add("httpcore.sync")
    return True


def _patch_httpcore_async(
    owner: type[object],
    *,
    request_type: type[object],
    url_type: type[object],
    response_type: type[object],
) -> bool:
    method_name = "handle_async_request"
    try:
        raw_original = type.__getattribute__(owner, method_name)
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., Awaitable[object]], raw_original)

    @functools.wraps(original)
    async def observed(
        instance: object,
        request: object,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        network_token: NetworkSuppressionToken | None = None
        try:
            request_values = _httpcore_request_values(
                request,
                request_type=request_type,
                url_type=url_type,
            )
            if request_values is not None:
                method, scheme, server_port = request_values
                identifier = _reserve_http_boundary(
                    method,
                    scheme=scheme,
                    server_port=server_port,
                    adapter="httpcore.async",
                )
                network_token = _begin_network_suppression()
        except BaseException:
            _record_http_callback_error()
        try:
            response = await original(instance, request, *positional, **keywords)
        except BaseException as exc:
            _finish_optional_http_error(identifier, exc)
            raise
        finally:
            try:
                _end_network_suppression(network_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_http_record(
                identifier,
                status_code=_typed_response_status(response, response_type),
                outcome="response",
            )
        except BaseException:
            _record_http_callback_error()
        return response

    try:
        type.__setattr__(owner, method_name, observed)
    except BaseException:
        return False
    with STATE._lock:
        STATE._http_adapters.add("httpcore.async")
    return True


def _module_values(module: ModuleType) -> dict[str, object]:
    try:
        values = ModuleType.__getattribute__(module, "__dict__")
    except BaseException:
        return {}
    return cast(dict[str, object], values) if type(values) is dict else {}


def _patch_httpcore(module: ModuleType) -> bool:
    values = _module_values(module)
    request_type = values.get("Request")
    url_type = values.get("URL")
    response_type = values.get("Response")
    sync_pool = values.get("ConnectionPool")
    async_pool = values.get("AsyncConnectionPool")
    if not all(isinstance(value, type) for value in (request_type, url_type, response_type)):
        return False
    typed_request = cast(type[object], request_type)
    typed_url = cast(type[object], url_type)
    typed_response = cast(type[object], response_type)
    sync_patched = isinstance(sync_pool, type) and _patch_httpcore_sync(
        sync_pool,
        request_type=typed_request,
        url_type=typed_url,
        response_type=typed_response,
    )
    async_patched = isinstance(async_pool, type) and _patch_httpcore_async(
        async_pool,
        request_type=typed_request,
        url_type=typed_url,
        response_type=typed_response,
    )
    return sync_patched and async_patched


def _url_scheme_and_port(value: object) -> tuple[str, object] | None:
    if type(value) is str:
        try:
            parsed = urllib.parse.urlsplit(value)
            scheme = _http_scheme(parsed.scheme)
            return None if scheme is None else (scheme, parsed.port)
        except (UnicodeError, ValueError):
            return None
    value_type = type(value)
    try:
        module_name = type.__getattribute__(value_type, "__module__")
    except BaseException:
        return None
    if type(module_name) is not str or not module_name.startswith("yarl"):
        return None
    scheme = _http_scheme(_exact_attribute(value, "scheme"))
    return None if scheme is None else (scheme, _exact_attribute(value, "port"))


def _aiohttp_request_values(
    session: object,
    method: object,
    url: object,
) -> tuple[object, str, object] | None:
    target = _url_scheme_and_port(url)
    if target is None:
        target = _url_scheme_and_port(_exact_attribute(session, "_base_url"))
    if target is None:
        return None
    scheme, server_port = target
    return method, scheme, server_port


def _aiohttp_response_status(response: object) -> int | None:
    try:
        module_name = type.__getattribute__(type(response), "__module__")
    except BaseException:
        return None
    if type(module_name) is not str or not module_name.startswith("aiohttp"):
        return None
    status = _exact_attribute(response, "status")
    return status if type(status) is int and 100 <= status <= 999 else None


def _patch_aiohttp(module: ModuleType) -> bool:
    values = _module_values(module)
    session_type = values.get("ClientSession")
    if not isinstance(session_type, type):
        return False
    method_name = "_request"
    try:
        raw_original = type.__getattribute__(session_type, method_name)
    except BaseException:
        return False
    if not callable(raw_original):
        return False
    original = cast(Callable[..., Awaitable[object]], raw_original)

    @functools.wraps(original)
    async def observed(
        session: object,
        method: object,
        url: object,
        *positional: object,
        **keywords: object,
    ) -> object:
        identifier: int | None = None
        network_token: NetworkSuppressionToken | None = None
        try:
            request_values = _aiohttp_request_values(session, method, url)
            if request_values is not None:
                safe_method, scheme, server_port = request_values
                identifier = _reserve_http_boundary(
                    safe_method,
                    scheme=scheme,
                    server_port=server_port,
                    adapter="aiohttp.async",
                )
                network_token = _begin_network_suppression()
        except BaseException:
            _record_http_callback_error()
        try:
            response = await original(session, method, url, *positional, **keywords)
        except BaseException as exc:
            _finish_optional_http_error(identifier, exc)
            raise
        finally:
            try:
                _end_network_suppression(network_token)
            except BaseException:
                _record_network_callback_error()
        try:
            _finish_http_record(
                identifier,
                status_code=_aiohttp_response_status(response),
                outcome="response",
            )
        except BaseException:
            _record_http_callback_error()
        return response

    try:
        type.__setattr__(session_type, method_name, observed)
    except BaseException:
        return False
    with STATE._lock:
        STATE._http_adapters.add("aiohttp.async")
    return True
