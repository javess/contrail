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

from ._constants import ActiveToken
from ._records import (
    _associate_record,
    _begin_active,
    _end_active,
    _finish_record,
    _record_callback_error,
    _record_caller_callback_error,
    _record_child,
    _record_identifier,
    _release_record,
    _reserve_record,
)
from ._state import STATE


@functools.wraps(STATE._original_init)
def _observed_init(
    process: object,
    arguments: object,
    *positional: object,
    **keywords: object,
) -> None:
    identifier: int | None = None
    try:
        identifier = _reserve_record(arguments, positional, keywords)
        _associate_record(process, identifier)
    except BaseException:
        _record_callback_error()
    try:
        STATE._original_init(process, arguments, *positional, **keywords)
    except BaseException as exc:
        try:
            _finish_record(
                identifier,
                exit_code=None,
                outcome="launch_error",
                error_type=type(exc).__name__,
            )
            _release_record(process)
        except BaseException:
            _record_callback_error()
        raise
    try:
        _record_child(identifier, process)
    except BaseException:
        _record_callback_error()


@functools.wraps(STATE._original_wait)
def _observed_wait(process: object, *positional: object, **keywords: object) -> int:
    token: ActiveToken | None = None
    try:
        token = _begin_active(process)
    except BaseException:
        _record_caller_callback_error()
    try:
        result = STATE._original_wait(process, *positional, **keywords)
    finally:
        try:
            _end_active(token)
        except BaseException:
            _record_caller_callback_error()
    try:
        _finish_record(
            _record_identifier(process),
            exit_code=result,
            outcome="exited",
        )
        _release_record(process)
    except BaseException:
        _record_callback_error()
    return result


@functools.wraps(STATE._original_communicate)
def _observed_communicate(
    process: object,
    *positional: object,
    **keywords: object,
) -> object:
    token: ActiveToken | None = None
    try:
        token = _begin_active(process)
    except BaseException:
        _record_caller_callback_error()
    try:
        return STATE._original_communicate(process, *positional, **keywords)
    finally:
        try:
            _end_active(token)
        except BaseException:
            _record_caller_callback_error()


@functools.wraps(STATE._original_poll)
def _observed_poll(process: object, *positional: object, **keywords: object) -> int | None:
    result = STATE._original_poll(process, *positional, **keywords)
    if result is None:
        return None
    try:
        _finish_record(
            _record_identifier(process),
            exit_code=result,
            outcome="exited",
        )
        _release_record(process)
    except BaseException:
        _record_callback_error()
    return result
