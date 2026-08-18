"""RunDiff command-line interface."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO, cast

from runtime_tools._version import __version__
from runtime_tools.capture import CaptureError, record_process
from runtime_tools.rundiff.compare import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe, terminal_text


def _parser(*, prog: str = "rundiff") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    record = subparsers.add_parser("record", help="capture a named local execution")
    record.add_argument("name")
    record.add_argument("--output", type=Path)
    record.add_argument("--cwd", type=Path, help="working directory for the command")
    record.add_argument("--identify-env", action="append", default=[], metavar="NAME")
    record.add_argument("--include-output", action="store_true")
    record.add_argument("--output-limit-bytes", type=int)

    compare = subparsers.add_parser("compare", help="compare two .runpack artifacts")
    compare.add_argument("baseline", type=Path)
    compare.add_argument("candidate", type=Path)
    compare.add_argument("--format", choices=("text", "json"), default="text")
    compare.add_argument(
        "--require-equivalent-outcome",
        action="store_true",
        help="exit 1 unless the behavioral outcome is equivalent",
    )
    return parser


def _binary_stream(name: str) -> BinaryIO | None:
    stream = getattr(sys, name)
    return cast(BinaryIO | None, getattr(stream, "buffer", None))


def _safe_name(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-.")
    return normalized or "run"


def _resolve_runpack(path: Path) -> Path:
    try:
        is_file = path.is_file()
    except (OSError, RuntimeError) as exc:
        raise RunpackError(f"could not resolve runpack path: {path}") from exc
    if is_file or path.suffix == ".runpack":
        return path
    return path.with_name(f"{path.name}.runpack")


def _process_exit_status(return_code: int) -> int:
    return 128 - return_code if return_code < 0 else return_code


def _record(
    argv: list[str],
    *,
    prog: str = "rundiff record",
    error_label: str = "rundiff",
) -> int:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("name")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cwd", type=Path, help="working directory for the command")
    parser.add_argument("--identify-env", action="append", default=[], metavar="NAME")
    parser.add_argument("--include-output", action="store_true")
    parser.add_argument("--output-limit-bytes", type=int)
    if "--" not in argv:
        parser.parse_args(argv)
        print(f"{error_label}: a command is required after --", file=sys.stderr)
        return 2
    separator = argv.index("--")
    args = parser.parse_args(argv[:separator])
    command = tuple(argv[separator + 1 :])
    if not command:
        print(f"{error_label}: a command is required after --", file=sys.stderr)
        return 2
    output = args.output or Path(f"{_safe_name(args.name)}.runpack")
    try:
        if args.output_limit_bytes is not None and not args.include_output:
            raise CaptureError("--output-limit-bytes requires --include-output")
        capture_output_limit = (
            (args.output_limit_bytes if args.output_limit_bytes is not None else 1_048_576)
            if args.include_output
            else None
        )
        exit_code = record_process(
            command,
            output,
            name=args.name,
            cwd=args.cwd,
            stdout=_binary_stream("stdout"),
            stderr=_binary_stream("stderr"),
            capture_output_limit=capture_output_limit,
            identify_environment=tuple(args.identify_env),
        )
    except (CaptureError, RunpackError) as exc:
        print(f"{error_label}: {terminal_text(exc)}", file=sys.stderr)
        return 2
    print(f"recorded {terminal_text(output)}", file=sys.stderr)
    return _process_exit_status(exit_code)


@broken_pipe_safe
def main(
    argv: list[str] | None = None,
    *,
    prog: str = "rundiff",
    error_label: str = "rundiff",
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "record":
        return _record(
            arguments[1:],
            prog=f"{prog} record",
            error_label=error_label,
        )
    args = _parser(prog=prog).parse_args(arguments)
    try:
        diff = compare_runpacks(_resolve_runpack(args.baseline), _resolve_runpack(args.candidate))
    except (CaptureError, RunpackError) as exc:
        print(f"{error_label}: {terminal_text(exc)}", file=sys.stderr)
        return 2
    print(render_diff(diff, args.format))
    return 0 if not args.require_equivalent_outcome or diff.outcome == "equivalent" else 1


if __name__ == "__main__":
    raise SystemExit(main())
