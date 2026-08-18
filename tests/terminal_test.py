from __future__ import annotations

import sys

import pytest

from runtime_tools.terminal import (
    MAX_TERMINAL_TEXT_CHARACTERS,
    broken_pipe_safe,
    terminal_text,
)


class _DeferredBrokenPipe:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        raise BrokenPipeError

    def fileno(self) -> int:
        raise OSError("no descriptor")


def test_terminal_text_bounds_long_printable_values() -> None:
    rendered = terminal_text("x" * (MAX_TERMINAL_TEXT_CHARACTERS + 1))

    assert len(rendered) == MAX_TERMINAL_TEXT_CHARACTERS
    assert rendered.endswith("…")


def test_terminal_text_bounds_expanded_control_escapes() -> None:
    rendered = terminal_text("\x1b" * MAX_TERMINAL_TEXT_CHARACTERS)

    assert len(rendered) <= MAX_TERMINAL_TEXT_CHARACTERS
    assert "\x1b" not in rendered
    assert rendered.endswith("…")


def test_broken_pipe_safe_handles_a_deferred_flush_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @broken_pipe_safe
    def command() -> int:
        print("report")
        return 0

    stream = _DeferredBrokenPipe()
    with monkeypatch.context() as context:
        context.setattr(sys, "stdout", stream)
        context.setattr(sys, "stderr", stream)
        status = command()

    assert status == 1
