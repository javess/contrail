"""BatchScope command-line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from runtime_tools import __version__
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import terminal_text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="batchscope")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    inspect = subparsers.add_parser("inspect", help="explain a finite execution")
    inspect.add_argument("runpack", type=Path)
    inspect.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)
    try:
        analysis = analyze_runpack(args.runpack)
    except RunpackError as exc:
        print(f"batchscope: {terminal_text(exc)}", file=sys.stderr)
        return 2
    print(render_analysis(analysis, args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
