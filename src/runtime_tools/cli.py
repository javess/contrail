"""Command-line interface for runtime capture and inspection."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO

from runtime_tools.capture import CaptureError, record_process
from runtime_tools.inspect import inspect_runpack, render_causal_tree, render_summary
from runtime_tools.otel import OtelImportError, import_otlp_json
from runtime_tools.storage import RunpackError
from runtime_tools.ui import TimelineError, serve_runpacks


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
    inspect.add_argument("--tree", action="store_true", help="print parent-child causal structure")

    import_otel = subparsers.add_parser("import-otel", help="import an OTLP/JSON trace export")
    import_otel.add_argument("source", type=Path)
    import_otel.add_argument("--name", help="logical execution name")
    import_otel.add_argument("--output", type=Path, help="output .runpack path")

    serve = subparsers.add_parser("serve", help="open a local execution timeline")
    serve.add_argument("runpack", type=Path)
    serve.add_argument("--compare", type=Path, help="candidate runpack for compare mode")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--no-open", action="store_true", help="do not open a browser")
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
        if args.subcommand == "import-otel":
            name = args.name or args.source.stem
            output = args.output or args.source.with_suffix(".runpack")
            result = import_otlp_json(args.source, output, name=name)
            print(
                f"imported {result.event_count} spans and {result.edge_count} causal edges "
                f"into {output}",
                file=sys.stderr,
            )
            return 0
        if args.subcommand == "serve":
            serve_runpacks(
                args.runpack,
                args.compare,
                host=args.host,
                port=args.port,
                open_browser=not args.no_open,
            )
            return 0
        if args.tree and args.format != "text":
            raise RunpackError("--tree is only available with text output")
        summary = inspect_runpack(args.runpack)
        print(render_summary(summary, args.format))
        if args.tree:
            print()
            print(render_causal_tree(args.runpack))
        return 0
    except (CaptureError, OtelImportError, RunpackError, TimelineError) as exc:
        print(f"runtime: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
