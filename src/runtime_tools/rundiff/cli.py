"""RunDiff command-line interface."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO

from runtime_tools import __version__
from runtime_tools.capture import CaptureError, record_process
from runtime_tools.rundiff.compare import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import terminal_text


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rundiff")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    record = subparsers.add_parser("record", help="capture a named local execution")
    record.add_argument("name")
    record.add_argument("--output", type=Path)
    record.add_argument("--include-output", action="store_true")
    record.add_argument("--output-limit-bytes", type=int, default=1_048_576)

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


def _record(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="rundiff record")
    parser.add_argument("name")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--include-output", action="store_true")
    parser.add_argument("--output-limit-bytes", type=int, default=1_048_576)
    if "--" not in argv:
        parser.parse_args(argv)
        print("rundiff: a command is required after --", file=sys.stderr)
        return 2
    separator = argv.index("--")
    args = parser.parse_args(argv[:separator])
    command = tuple(argv[separator + 1 :])
    if not command:
        print("rundiff: a command is required after --", file=sys.stderr)
        return 2
    output = args.output or Path(f"{_safe_name(args.name)}.runpack")
    try:
        exit_code = record_process(
            command,
            output,
            name=args.name,
            stdout=_binary_stream("stdout"),
            stderr=_binary_stream("stderr"),
            capture_output_limit=(args.output_limit_bytes if args.include_output else None),
        )
    except (CaptureError, RunpackError) as exc:
        print(f"rundiff: {terminal_text(exc)}", file=sys.stderr)
        return 2
    print(f"recorded {terminal_text(output)}", file=sys.stderr)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "record":
        return _record(arguments[1:])
    args = _parser().parse_args(arguments)
    try:
        diff = compare_runpacks(_resolve_runpack(args.baseline), _resolve_runpack(args.candidate))
    except (CaptureError, RunpackError) as exc:
        print(f"rundiff: {terminal_text(exc)}", file=sys.stderr)
        return 2
    print(render_diff(diff, args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
