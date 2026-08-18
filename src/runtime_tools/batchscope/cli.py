"""BatchScope command-line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from runtime_tools._version import __version__
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe, terminal_text


@broken_pipe_safe
def main(
    argv: list[str] | None = None,
    *,
    prog: str = "batchscope",
    error_label: str = "batchscope",
    command_name: str = "inspect",
) -> int:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    inspect = subparsers.add_parser(command_name, help="explain a finite execution")
    inspect.add_argument("runpack", type=Path)
    inspect.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)
    try:
        analysis = analyze_runpack(args.runpack)
    except RunpackError as exc:
        print(f"{error_label}: {terminal_text(exc)}", file=sys.stderr)
        return 2
    print(render_analysis(analysis, args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
