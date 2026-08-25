"""Command-line interface for runtime capture and inspection."""

from __future__ import annotations

import argparse
import json
import re
import signal
import sys
import threading
import time
from pathlib import Path
from typing import BinaryIO, cast

from runtime_tools._version import __version__
from runtime_tools.capture import (
    CAPTURE_LEVELS,
    CaptureError,
    record_process,
    recover_process_capture,
)
from runtime_tools.capture_jobs import (
    CaptureJob,
    CaptureJobError,
    CaptureJobOutput,
    cancel_capture_job,
    capture_job_document,
    capture_jobs_document,
    list_capture_jobs,
    load_capture_job,
    read_capture_job_output,
    record_current_capture_job_artifacts,
    render_capture_job,
    render_capture_jobs,
    wait_capture_job,
)
from runtime_tools.capture_worker import capture_worker_client_event, run_capture_worker
from runtime_tools.inspect import inspect_reader, render_causal_tree_reader, render_summary
from runtime_tools.providers import ProviderError, ProviderRegistry, resolve_provider_registry
from runtime_tools.query import QueryError, query_runpack, render_query
from runtime_tools.storage import RunpackError, RunpackReader, resolve_runpack_path
from runtime_tools.terminal import broken_pipe_safe, terminal_text
from runtime_tools.ui import TimelineError, serve_runpacks

CAPTURE_JOB_OUTPUT_POLL_SECONDS = 0.1
CORE_COMMANDS = frozenset(
    {
        "record",
        "recover",
        "job",
        "inspect",
        "serve",
        "query",
        "providers",
    }
)


def _parser(
    provider_registry: ProviderRegistry,
    *,
    prog: str = "runtime",
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    record = subparsers.add_parser("record", help="capture a local process")
    record.add_argument("--name", help="logical execution name")
    record.add_argument("--output", type=Path, help="output .runpack path")
    record.add_argument("--cwd", type=Path, help="working directory for the command")
    record.add_argument(
        "--detach",
        action="store_true",
        help=(
            "run in the background and privately retain bounded stdout/stderr (may contain secrets)"
        ),
    )
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
    record.add_argument(
        "--capture-level",
        choices=CAPTURE_LEVELS,
        help=(
            "capture preset: passive outcome, process resources, sampled Python, "
            "or expensive deep Python and native C calls"
        ),
    )
    record.add_argument(
        "--instrument",
        choices=("sample", "deep"),
        help="sample Python stacks, or observe every call with expensive deep capture",
    )
    record.add_argument(
        "--observe-process-tree",
        action="store_true",
        help="sample process-group RSS and CPU from the controller",
    )
    record.add_argument("command", nargs=argparse.REMAINDER, help="command after --")

    recover = subparsers.add_parser(
        "recover",
        help="finish a post-exit capture checkpoint after controller loss",
    )
    recover.add_argument("checkpoint", type=Path, help="retained .runpack.tmp-* checkpoint")
    recover.add_argument("--output", type=Path, required=True, help="output .runpack path")

    job = subparsers.add_parser("job", help="discover and control local capture workers")
    job_commands = job.add_subparsers(dest="job_command", required=True)
    job_list = job_commands.add_parser("list", help="list retained capture jobs")
    job_list.add_argument("--format", choices=("text", "json"), default="text")
    job_status = job_commands.add_parser("status", help="inspect one capture job")
    job_status.add_argument("job_id")
    job_status.add_argument("--format", choices=("text", "json"), default="text")
    job_wait = job_commands.add_parser("wait", help="wait for one capture job")
    job_wait.add_argument("job_id")
    job_wait.add_argument("--timeout", type=float)
    job_wait.add_argument("--format", choices=("text", "json"), default="text")
    job_cancel = job_commands.add_parser("cancel", help="cancel and reap one capture job")
    job_cancel.add_argument("job_id")
    job_cancel.add_argument("--format", choices=("text", "json"), default="text")
    job_output = job_commands.add_parser(
        "output", help="replay bounded output retained by --detach"
    )
    job_output.add_argument("job_id")
    job_output.add_argument(
        "--follow",
        action="store_true",
        help="replay retained output, then follow new retained bytes until the job finishes",
    )

    inspect = subparsers.add_parser("inspect", help="inspect a .runpack")
    inspect.add_argument("runpack", type=Path)
    inspect.add_argument("--format", choices=("text", "json"), default="text")
    inspect.add_argument("--tree", action="store_true", help="print parent-child causal structure")

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

    query = subparsers.add_parser("query", help="run bounded read-only SQL over a runpack")
    query.add_argument("runpack", type=Path)
    query.add_argument("sql")
    query.add_argument(
        "--limit", type=int, default=1000, help="maximum rows to return (hard limit: 100000)"
    )
    query.add_argument("--format", choices=("table", "json", "jsonl"), default="table")
    subparsers.add_parser("providers", help="list installed and enabled evidence providers")
    for command in provider_registry.commands:
        provider_parser = subparsers.add_parser(command.name, help=command.help)
        command.configure(provider_parser)
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


def _write_output(name: str, content: bytes) -> None:
    if not content:
        return
    binary = _binary_stream(name)
    if binary is not None:
        binary.write(content)
        binary.flush()
        return
    stream = getattr(sys, name)
    stream.write(content.decode("utf-8", errors="backslashreplace"))
    stream.flush()


def _report_capture_job_output_truncation(
    output: CaptureJobOutput,
    *,
    error_label: str,
) -> None:
    _report_capture_job_stream_truncation(
        "stdout",
        output.stdout_truncated,
        output.stdout_omitted_bytes,
        output.stdout_omitted_bytes_truncated,
        error_label=error_label,
    )
    _report_capture_job_stream_truncation(
        "stderr",
        output.stderr_truncated,
        output.stderr_omitted_bytes,
        output.stderr_omitted_bytes_truncated,
        error_label=error_label,
    )


def _report_capture_job_stream_truncation(
    stream: str,
    truncated: bool,
    omitted_bytes: int,
    omitted_bytes_truncated: bool,
    *,
    error_label: str,
) -> None:
    if not truncated:
        return
    detail = ""
    if omitted_bytes:
        qualifier = "at least " if omitted_bytes_truncated else ""
        detail = f"; omitted {qualifier}{omitted_bytes} bytes between retained head and tail"
    elif omitted_bytes_truncated:
        detail = "; the omitted byte count is not fully known"
    print(
        f"{error_label}: retained {stream} was truncated{detail}",
        file=sys.stderr,
        flush=True,
    )


def _follow_capture_job_output(job_id: str, *, error_label: str) -> None:
    stdout_offset = 0
    stderr_offset = 0
    while True:
        output = read_capture_job_output(
            job_id,
            stdout_offset=stdout_offset,
            stderr_offset=stderr_offset,
        )
        _write_output("stdout", output.stdout)
        _write_output("stderr", output.stderr)
        stdout_offset += len(output.stdout)
        stderr_offset += len(output.stderr)
        if output.terminal:
            _write_output("stdout", output.stdout_tail)
            _write_output("stderr", output.stderr_tail)
            _report_capture_job_output_truncation(output, error_label=error_label)
            return
        time.sleep(CAPTURE_JOB_OUTPUT_POLL_SECONDS)


def _render_capture_job_result(job: CaptureJob, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(capture_job_document(job), allow_nan=False, indent=2, sort_keys=True)
    return render_capture_job(job)


def _render_provider_inventory(registry: ProviderRegistry) -> str:
    lines = ["PROVIDER\tSTATE\tSOURCE\tCOMMANDS"]
    for provider in registry.inventory:
        commands = ", ".join(provider.commands) if provider.commands else "-"
        source = provider.source
        if provider.distribution is not None:
            source = f"{source} ({terminal_text(provider.distribution)})"
        lines.append(
            "\t".join(
                (
                    provider.key,
                    "enabled" if provider.enabled else "disabled",
                    source,
                    commands,
                )
            )
        )
    return "\n".join(lines)


@broken_pipe_safe
def main(
    argv: list[str] | None = None,
    *,
    prog: str = "runtime",
    error_label: str = "runtime",
    _provider_registry: ProviderRegistry | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    capture_worker_client: threading.Event | None = None
    try:
        provider_registry = _provider_registry or resolve_provider_registry(
            reserved_commands=CORE_COMMANDS
        )
        args = _parser(provider_registry, prog=prog).parse_args(arguments)
        if args.subcommand == "record":
            capture_worker_client = capture_worker_client_event()
            if capture_worker_client is None:
                module = (
                    "runtime_tools.contrail_cli"
                    if error_label == "contrail"
                    else "runtime_tools.cli"
                )
                return run_capture_worker(
                    tuple(arguments),
                    module=module,
                    detached=args.detach,
                )
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
            if args.capture_level is not None and (
                args.instrument is not None or args.observe_process_tree
            ):
                raise CaptureError(
                    "--capture-level cannot be combined with --instrument or --observe-process-tree"
                )
            effective_instrument = args.instrument
            if args.capture_level in {"sample", "deep"}:
                effective_instrument = args.capture_level
            if effective_instrument == "deep":
                print(
                    f"{error_label}: warning: deep instrumentation is intrusive and can "
                    "materially perturb timings; it observes every Python and native C call plus "
                    "Python exception propagation "
                    "and is the "
                    "most expensive capture level",
                    file=sys.stderr,
                )
            elif effective_instrument == "sample":
                print(
                    f"{error_label}: note: sampling estimates Python hotspots and may "
                    "perturb timings",
                    file=sys.stderr,
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
                capture_level=args.capture_level,
                instrument=args.instrument,
                observe_process_tree=args.observe_process_tree,
                _capture_client_disconnected=capture_worker_client,
            )
            record_current_capture_job_artifacts((output,))
            print(f"recorded {terminal_text(output)}", file=sys.stderr)
            return _process_exit_status(exit_code)
        if args.subcommand == "recover":
            recover_process_capture(args.checkpoint, args.output)
            print(f"recovered {terminal_text(args.output)}", file=sys.stderr)
            return 0
        if args.subcommand == "job":
            if args.job_command == "list":
                jobs = list_capture_jobs()
                if args.format == "json":
                    print(
                        json.dumps(
                            capture_jobs_document(jobs),
                            allow_nan=False,
                            indent=2,
                            sort_keys=True,
                        )
                    )
                else:
                    print(render_capture_jobs(jobs))
                return 0
            if args.job_command == "status":
                job = load_capture_job(args.job_id)
                print(_render_capture_job_result(job, args.format))
                return 0
            if args.job_command == "wait":
                job = wait_capture_job(args.job_id, timeout_seconds=args.timeout)
                print(_render_capture_job_result(job, args.format))
                return job.exit_status if job.exit_status is not None else 2
            if args.job_command == "output":
                if args.follow:
                    try:
                        _follow_capture_job_output(args.job_id, error_label=error_label)
                    except KeyboardInterrupt:
                        return 128 + signal.SIGINT
                    return 0
                output = read_capture_job_output(args.job_id)
                _write_output("stdout", output.stdout)
                _write_output("stdout", output.stdout_tail)
                _write_output("stderr", output.stderr)
                _write_output("stderr", output.stderr_tail)
                _report_capture_job_output_truncation(output, error_label=error_label)
                return 0
            job = cancel_capture_job(args.job_id)
            print(_render_capture_job_result(job, args.format))
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
        if args.subcommand == "providers":
            print(_render_provider_inventory(provider_registry))
            return 0
        provider_command = provider_registry.command(args.subcommand)
        if provider_command is not None:
            provider_result = provider_registry.execute(args.subcommand, args)
            if provider_result.summary is not None:
                print(terminal_text(provider_result.summary), file=sys.stderr)
            return provider_result.exit_status
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
    except KeyboardInterrupt:
        if capture_worker_client is not None:
            return 128 + signal.SIGINT
        raise
    except (
        CaptureError,
        CaptureJobError,
        ProviderError,
        QueryError,
        RunpackError,
        TimelineError,
    ) as exc:
        print(f"{error_label}: {terminal_text(exc)}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
