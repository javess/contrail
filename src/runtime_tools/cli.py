"""Command-line interface for runtime capture and inspection."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO, cast

from runtime_tools._version import __version__
from runtime_tools.capture import CaptureError, record_process
from runtime_tools.enrichment import EnrichmentError
from runtime_tools.inspect import inspect_reader, render_causal_tree_reader, render_summary
from runtime_tools.kubernetes import KubernetesImportError, import_kubernetes_snapshot
from runtime_tools.otel import OtelImportError, import_otlp_json, import_otlp_logs
from runtime_tools.prometheus import PrometheusImportError, import_prometheus_response
from runtime_tools.query import QueryError, query_runpack, render_query
from runtime_tools.storage import RunpackError, RunpackReader, resolve_runpack_path
from runtime_tools.terminal import broken_pipe_safe, terminal_text
from runtime_tools.ui import TimelineError, serve_runpacks


def _parser(*, prog: str = "runtime") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    record = subparsers.add_parser("record", help="capture a local process")
    record.add_argument("--name", help="logical execution name")
    record.add_argument("--output", type=Path, help="output .runpack path")
    record.add_argument("--cwd", type=Path, help="working directory for the command")
    record.add_argument(
        "--identify-env",
        action="append",
        default=[],
        metavar="NAME",
        help="hash an environment value for drift detection; repeat as needed",
    )
    record.add_argument(
        "--include-output",
        action="store_true",
        help="store bounded stdout/stderr content (may contain secrets)",
    )
    record.add_argument("--output-limit-bytes", type=int)
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
    proofline_input = serve.add_mutually_exclusive_group()
    proofline_input.add_argument(
        "--contract",
        type=Path,
        help="evaluate and navigate a Proofline contract in compare mode",
    )
    proofline_input.add_argument(
        "--proofline-report",
        type=Path,
        help="navigate an explained Proofline report retained with the runpacks",
    )
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

    otel_logs = subparsers.add_parser("enrich-otel-logs", help="add bounded OTLP/JSON log records")
    otel_logs.add_argument("runpack", type=Path)
    otel_logs.add_argument("source", type=Path)
    otel_logs.add_argument("--output", type=Path, required=True)
    otel_logs.add_argument(
        "--include-raw",
        action="store_true",
        help="store the source OTLP logs JSON in the runpack",
    )

    query = subparsers.add_parser("query", help="run bounded read-only SQL over a runpack")
    query.add_argument("runpack", type=Path)
    query.add_argument("sql")
    query.add_argument(
        "--limit", type=int, default=1000, help="maximum rows to return (hard limit: 100000)"
    )
    query.add_argument("--format", choices=("table", "json", "jsonl"), default="table")
    return parser


def _safe_name(command: tuple[str, ...]) -> str:
    raw = Path(command[0]).name if command else "run"
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw).strip("-.")
    return normalized or "run"


def _binary_stream(name: str) -> BinaryIO | None:
    stream = getattr(sys, name)
    return cast(BinaryIO | None, getattr(stream, "buffer", None))


def _process_exit_status(return_code: int) -> int:
    return 128 - return_code if return_code < 0 else return_code


@broken_pipe_safe
def main(
    argv: list[str] | None = None,
    *,
    prog: str = "runtime",
    error_label: str = "runtime",
) -> int:
    args = _parser(prog=prog).parse_args(argv)
    try:
        if args.subcommand == "record":
            command = tuple(args.command)
            if command and command[0] == "--":
                command = command[1:]
            if not command:
                raise CaptureError("a command is required after --")
            if args.output_limit_bytes is not None and not args.include_output:
                raise CaptureError("--output-limit-bytes requires --include-output")
            name = args.name or _safe_name(command)
            output = args.output or Path(f"{_safe_name((name,))}.runpack")
            capture_output_limit = (
                (args.output_limit_bytes if args.output_limit_bytes is not None else 1_048_576)
                if args.include_output
                else None
            )
            exit_code = record_process(
                command,
                output,
                name=name,
                cwd=args.cwd,
                stdout=_binary_stream("stdout"),
                stderr=_binary_stream("stderr"),
                capture_output_limit=capture_output_limit,
                identify_environment=tuple(args.identify_env),
            )
            print(f"recorded {terminal_text(output)}", file=sys.stderr)
            return _process_exit_status(exit_code)
        if args.subcommand == "import-otel":
            name = args.name or args.source.stem
            output = args.output or args.source.with_suffix(".runpack")
            otel_result = import_otlp_json(
                args.source, output, name=name, include_raw=args.include_raw
            )
            print(
                f"imported {otel_result.event_count} spans and "
                f"{otel_result.edge_count} causal edges into {terminal_text(output)}",
                file=sys.stderr,
            )
            return 0
        if args.subcommand == "serve":
            if args.compare is None and args.contract is not None:
                raise TimelineError("--contract requires --compare")
            if args.compare is None and args.proofline_report is not None:
                raise TimelineError("--proofline-report requires --compare")
            serve_runpacks(
                args.runpack,
                args.compare,
                contract=args.contract,
                proofline_report=args.proofline_report,
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
                f"{kubernetes_result.correlation_count} telemetry correlations to "
                f"{terminal_text(args.output)}",
                file=sys.stderr,
            )
            return 0
        if args.subcommand == "enrich-prometheus":
            prometheus_result = import_prometheus_response(args.runpack, args.response, args.output)
            print(
                f"added {prometheus_result.sample_count} Prometheus samples "
                f"({prometheus_result.dropped_outside_window} outside the run window) "
                f"to {terminal_text(args.output)}",
                file=sys.stderr,
            )
            return 0
        if args.subcommand == "enrich-otel-logs":
            logs_result = import_otlp_logs(
                args.runpack,
                args.source,
                args.output,
                include_raw=args.include_raw,
            )
            print(
                f"added {logs_result.event_count} OTLP log records and "
                f"{logs_result.edge_count} span correlations "
                f"({logs_result.dropped_outside_window} outside the run window) "
                f"with {logs_result.dropped_attribute_count} exporter-dropped attributes "
                f"to {terminal_text(args.output)}",
                file=sys.stderr,
            )
            return 0
        if args.subcommand == "query":
            result = query_runpack(args.runpack, args.sql, limit=args.limit)
            rendered = render_query(result, args.format)
            if rendered:
                print(rendered)
            if args.format == "jsonl" and result.truncated:
                print(
                    f"{error_label}: query result truncated at the requested row limit",
                    file=sys.stderr,
                )
            return 0
        if args.tree and args.format != "text":
            raise RunpackError("--tree is only available with text output")
        runpack = resolve_runpack_path(args.runpack)
        with RunpackReader(runpack) as reader:
            summary = inspect_reader(reader)
            causal_tree = render_causal_tree_reader(reader) if args.tree else None
        print(render_summary(summary, args.format))
        if causal_tree is not None:
            print()
            print(causal_tree)
        return 0
    except (
        CaptureError,
        EnrichmentError,
        KubernetesImportError,
        OtelImportError,
        PrometheusImportError,
        QueryError,
        RunpackError,
        TimelineError,
    ) as exc:
        print(f"{error_label}: {terminal_text(exc)}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
