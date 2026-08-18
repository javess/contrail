"""Safe rendering of untrusted artifact text in terminal-oriented output."""

from __future__ import annotations


def terminal_text(value: object) -> str:
    """Escape non-printing Unicode without changing machine-readable values."""
    return "".join(
        character
        if character.isprintable()
        else character.encode("unicode_escape", errors="backslashreplace").decode("ascii")
        for character in str(value)
    )
