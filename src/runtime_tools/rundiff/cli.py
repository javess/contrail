"""RunDiff command-line interface."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO

from runtime_tools.capture import CaptureError, record_process
from runtime_tools.rundiff.compare import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rundiff")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    record = subparsers.add_parser("record", help="capture a named local execution")
    record.add_argument("name")
    record.add_argument("--output", type=Path)
    record.add_argument("command", nargs=argparse.REMAINDER, help="command after --")

    compare = subparsers.add_parser("compare", help="compare two .runpack artifacts")
    compare.add_argument("baseline", type=Path)
    compare.add_argument("candidate", type=Path)
    compare.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def _binary_stream(name: str) -> BinaryIO | None:
    stream = getattr(sys, name)
    return getattr(stream, "buffer", None)


def _safe_name(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-.")
    return normalized or "run"


def _resolve_runpack(path: Path) -> Path:
    if path.exists() or path.suffix == ".runpack":
        return path
    return path.with_name(f"{path.name}.runpack")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.subcommand == "record":
            command = tuple(args.command)
            if command and command[0] == "--":
                command = command[1:]
            if not command:
                raise CaptureError("a command is required after --")
            output = args.output or Path(f"{_safe_name(args.name)}.runpack")
            exit_code = record_process(
                command,
                output,
                name=args.name,
                stdout=_binary_stream("stdout"),
                stderr=_binary_stream("stderr"),
            )
            print(f"recorded {output}", file=sys.stderr)
            return exit_code
        diff = compare_runpacks(_resolve_runpack(args.baseline), _resolve_runpack(args.candidate))
    except (CaptureError, RunpackError) as exc:
        print(f"rundiff: {exc}", file=sys.stderr)
        return 2
    print(render_diff(diff, args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
