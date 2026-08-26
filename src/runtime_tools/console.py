"""Rich terminal output that keeps machine streams exact."""

from __future__ import annotations

import sys
from typing import Literal

from rich.console import Console, RenderableType
from rich.text import Text

_stdout = Console(highlight=False)
_stderr = Console(stderr=True, highlight=False)


def print_text(
    value: object = "",
    *,
    stderr: bool = False,
    style: str | None = None,
    end: str = "\n",
) -> None:
    """Print trusted presentation text without interpreting Rich markup."""
    console = _stderr if stderr else _stdout
    console.print(Text(str(value), style=style or ""), soft_wrap=True, end=end)


def print_renderable(value: RenderableType, *, stderr: bool = False) -> None:
    """Print a deliberately constructed Rich renderable."""
    (_stderr if stderr else _stdout).print(value)


def print_json(value: str) -> None:
    """Write an already serialized machine document without terminal processing."""
    sys.stdout.write(value)


def diagnostic(
    label: Literal["error", "note", "warning"],
    message: object,
    *,
    prefix: str = "contrail",
) -> None:
    styles = {"error": "bold red", "warning": "bold yellow", "note": "bold cyan"}
    print_text(f"{prefix}: {label}: {message}", stderr=True, style=styles[label])
