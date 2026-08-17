"""Command-line interface for runtime capture and inspection."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO

from runtime_tools.capture import CaptureError, record_process
from runtime_tools.inspect import inspect_runpack, render_summary
from runtime_tools.storage import RunpackError


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runtime")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    record = subparsers.add_parser("record", help="capture a local process")
    record.add_argument("--name", help="logical execution name")
    record.add_argument("--output", type=Path, help="output .runpack path")
    record.add_argument("command", nargs=argparse.REMAINDER, help="command after --")

    inspect = subparsers.add_parser("inspect", help="inspect a .runpack")
    inspect.add_argument("runpack", type=Path)
    inspect.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def _safe_name(command: tuple[str, ...]) -> str:
    raw = Path(command[0]).name if command else "run"
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw).strip("-.")
    return normalized or "run"


def _binary_stream(name: str) -> BinaryIO | None:
    stream = getattr(sys, name)
    return getattr(stream, "buffer", None)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.subcommand == "record":
            command = tuple(args.command)
            if command and command[0] == "--":
                command = command[1:]
            if not command:
                raise CaptureError("a command is required after --")
            name = args.name or _safe_name(command)
            output = args.output or Path(f"{_safe_name((name,))}.runpack")
            exit_code = record_process(
                command,
                output,
                name=name,
                stdout=_binary_stream("stdout"),
                stderr=_binary_stream("stderr"),
            )
            print(f"recorded {output}", file=sys.stderr)
            return exit_code
        summary = inspect_runpack(args.runpack)
        print(render_summary(summary, args.format))
        return 0
    except (CaptureError, RunpackError) as exc:
        print(f"runtime: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
