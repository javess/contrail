"""Safe rendering of untrusted artifact text in terminal-oriented output."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from functools import wraps

MAX_TERMINAL_TEXT_CHARACTERS = 4_096


def _silence_standard_streams() -> None:
    try:
        null_descriptor = os.open(os.devnull, os.O_WRONLY)
    except OSError:
        return
    try:
        for stream in (sys.stdout, sys.stderr):
            try:
                descriptor = stream.fileno()
                os.dup2(null_descriptor, descriptor)
            except (AttributeError, OSError, ValueError):
                continue
    finally:
        os.close(null_descriptor)


def broken_pipe_safe[**P](function: Callable[P, int]) -> Callable[P, int]:
    """Keep an expected downstream pipe close from producing a traceback."""

    @wraps(function)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> int:
        try:
            try:
                return function(*args, **kwargs)
            finally:
                sys.stdout.flush()
                sys.stderr.flush()
        except BrokenPipeError:
            _silence_standard_streams()
            return 1

    return wrapped


def terminal_text(value: object) -> str:
    """Escape and bound untrusted text without changing machine-readable values."""
    parts: list[str] = []
    rendered_length = 0
    raw = str(value)
    for index, character in enumerate(raw):
        rendered = (
            character
            if character.isprintable()
            else character.encode("unicode_escape", errors="backslashreplace").decode("ascii")
        )
        available = MAX_TERMINAL_TEXT_CHARACTERS - (index < len(raw) - 1)
        if rendered_length + len(rendered) > available:
            parts.append("…")
            break
        parts.append(rendered)
        rendered_length += len(rendered)
    return "".join(parts)
