"""Command-line interface for runtime capture and inspection."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO

from runtime_tools.capture import CaptureError, record_process
from runtime_tools.inspect import inspect_runpack, render_causal_tree, render_summary
from runtime_tools.kubernetes import KubernetesImportError, import_kubernetes_snapshot
from runtime_tools.otel import OtelImportError, import_otlp_json
from runtime_tools.prometheus import PrometheusImportError, import_prometheus_response
from runtime_tools.query import QueryError, query_runpack, render_query
from runtime_tools.storage import RunpackError
from runtime_tools.ui import TimelineError, serve_runpacks


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runtime")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    record = subparsers.add_parser("record", help="capture a local process")
    record.add_argument("--name", help="logical execution name")
    record.add_argument("--output", type=Path, help="output .runpack path")
    record.add_argument(
        "--include-output",
        action="store_true",
        help="store bounded stdout/stderr content (may contain secrets)",
    )
    record.add_argument("--output-limit-bytes", type=int, default=1_048_576)
    record.add_argument("command", nargs=argparse.REMAINDER, help="command after --")

    inspect = subparsers.add_parser("inspect", help="inspect a .runpack")
    inspect.add_argument("runpack", type=Path)
    inspect.add_argument("--format", choices=("text", "json"), default="text")
    inspect.add_argument("--tree", action="store_true", help="print parent-child causal structure")

    import_otel = subparsers.add_parser("import-otel", help="import an OTLP/JSON trace export")
    import_otel.add_argument("source", type=Path)
    import_otel.add_argument("--name", help="logical execution name")
    import_otel.add_argument("--output", type=Path, help="output .runpack path")
    import_otel.add_argument(
        "--include-raw",
        action="store_true",
        help="store the source OTLP JSON in the runpack",
    )

    serve = subparsers.add_parser("serve", help="open a local execution timeline")
    serve.add_argument("runpack", type=Path)
    serve.add_argument("--compare", type=Path, help="candidate runpack for compare mode")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--no-open", action="store_true", help="do not open a browser")

    kubernetes = subparsers.add_parser(
        "enrich-kubernetes", help="add a bounded Kubernetes API snapshot"
    )
    kubernetes.add_argument("runpack", type=Path)
    kubernetes.add_argument("snapshot", type=Path)
    kubernetes.add_argument("--output", type=Path, required=True)

    prometheus = subparsers.add_parser(
        "enrich-prometheus", help="add a bounded Prometheus HTTP API response"
    )
    prometheus.add_argument("runpack", type=Path)
    prometheus.add_argument("response", type=Path)
    prometheus.add_argument("--output", type=Path, required=True)

    query = subparsers.add_parser("query", help="run bounded read-only SQL over a runpack")
    query.add_argument("runpack", type=Path)
    query.add_argument("sql")
    query.add_argument("--limit", type=int, default=1000)
    query.add_argument("--format", choices=("table", "json", "jsonl"), default="table")
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
                capture_output_limit=(args.output_limit_bytes if args.include_output else None),
            )
            print(f"recorded {output}", file=sys.stderr)
            return exit_code
        if args.subcommand == "import-otel":
            name = args.name or args.source.stem
            output = args.output or args.source.with_suffix(".runpack")
            otel_result = import_otlp_json(
                args.source, output, name=name, include_raw=args.include_raw
            )
            print(
                f"imported {otel_result.event_count} spans and "
                f"{otel_result.edge_count} causal edges into {output}",
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
        if args.subcommand == "enrich-kubernetes":
            kubernetes_result = import_kubernetes_snapshot(args.runpack, args.snapshot, args.output)
            print(
                f"added {kubernetes_result.entity_count} Kubernetes entities, "
                f"{kubernetes_result.event_count} events, and "
                f"{kubernetes_result.correlation_count} telemetry correlations to {args.output}",
                file=sys.stderr,
            )
            return 0
        if args.subcommand == "enrich-prometheus":
            prometheus_result = import_prometheus_response(args.runpack, args.response, args.output)
            print(
                f"added {prometheus_result.sample_count} Prometheus samples "
                f"({prometheus_result.dropped_outside_window} outside the run window) "
                f"to {args.output}",
                file=sys.stderr,
            )
            return 0
        if args.subcommand == "query":
            print(
                render_query(query_runpack(args.runpack, args.sql, limit=args.limit), args.format)
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
    except (
        CaptureError,
        KubernetesImportError,
        OtelImportError,
        PrometheusImportError,
        QueryError,
        RunpackError,
        TimelineError,
    ) as exc:
        print(f"runtime: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
