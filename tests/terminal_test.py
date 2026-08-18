from __future__ import annotations

from runtime_tools.terminal import MAX_TERMINAL_TEXT_CHARACTERS, terminal_text


def test_terminal_text_bounds_long_printable_values() -> None:
    rendered = terminal_text("x" * (MAX_TERMINAL_TEXT_CHARACTERS + 1))

    assert len(rendered) == MAX_TERMINAL_TEXT_CHARACTERS
    assert rendered.endswith("…")


def test_terminal_text_bounds_expanded_control_escapes() -> None:
    rendered = terminal_text("\x1b" * MAX_TERMINAL_TEXT_CHARACTERS)

    assert len(rendered) <= MAX_TERMINAL_TEXT_CHARACTERS
    assert "\x1b" not in rendered
    assert rendered.endswith("…")
