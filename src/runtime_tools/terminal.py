"""Safe rendering of untrusted artifact text in terminal-oriented output."""

from __future__ import annotations

MAX_TERMINAL_TEXT_CHARACTERS = 4_096


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
