"""Typed runtime capture and inspection commands."""

from __future__ import annotations

import json
import signal
import sys
import time
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from runtime_tools._cli_support import (
    CaptureLevel,
    Instrumentation,
    OutputFormat,
    RecordOptions,
    binary_stream,
    command_context,
    fail,
    finish,
    run_cli,
    run_record_command,
    safe_runpack_name,
)
from runtime_tools.capture import CaptureError, recover_process_capture
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
    render_capture_job,
    render_capture_jobs,
    wait_capture_job,
)
from runtime_tools.capture_worker import capture_worker_client_event, run_capture_worker
from runtime_tools.console import print_json, print_renderable, print_text
from runtime_tools.inspect import inspect_reader, render_causal_tree_reader, render_summary
from runtime_tools.provider_cli import app as provider_app
from runtime_tools.providers import ProviderError, ProviderRegistry, resolve_provider_registry
from runtime_tools.query import QueryError, query_runpack, render_query
from runtime_tools.storage import RunpackError, RunpackReader, resolve_runpack_path
from runtime_tools.terminal import broken_pipe_safe, terminal_text

CAPTURE_JOB_OUTPUT_POLL_SECONDS = 0.1
CORE_COMMANDS = frozenset({"record", "recover", "job", "inspect", "query", "providers"})

app = typer.Typer(
    name="runtime",
    help="Capture and inspect normalized runtime evidence.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)
job_app = typer.Typer(
    help="Discover and control local capture workers.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)
app.add_typer(job_app, name="job", rich_help_panel="Capture")
app.add_typer(provider_app, name=None)


def _record_options(
    *,
    cwd: Path | None,
    detach: bool,
    identify_env: list[str] | None,
    include_output: bool,
    output_limit_bytes: int | None,
    capture_level: CaptureLevel | None,
    instrument: Instrumentation | None,
    observe_process_tree: bool,
) -> RecordOptions:
    return RecordOptions(
        cwd=cwd,
        detach=detach,
        identify_environment=tuple(identify_env or ()),
        include_output=include_output,
        output_limit_bytes=output_limit_bytes,
        capture_level=capture_level.value if capture_level is not None else None,
        instrument=instrument.value if instrument is not None else None,
        observe_process_tree=observe_process_tree,
    )


@app.command(
    "record",
    help="Capture one local process as a .runpack.",
    rich_help_panel="Capture",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def record_command(
    context: typer.Context,
    command: Annotated[list[str], typer.Argument(help="Command after --")],
    name: Annotated[str | None, typer.Option("--name", help="Logical execution name")] = None,
    output: Annotated[Path | None, typer.Option("--output", help="Output .runpack path")] = None,
    cwd: Annotated[
        Path | None, typer.Option("--cwd", help="Working directory for the command")
    ] = None,
    detach: Annotated[
        bool,
        typer.Option(
            "--detach",
            help="Run in the background and privately retain bounded output",
        ),
    ] = False,
    identify_env: Annotated[
        list[str] | None,
        typer.Option(
            "--identify-env",
            metavar="NAME",
            help="Hash an environment value for drift detection; repeat as needed",
        ),
    ] = None,
    include_output: Annotated[
        bool,
        typer.Option(
            "--include-output",
            help="Store bounded stdout/stderr content (may contain secrets)",
        ),
    ] = False,
    output_limit_bytes: Annotated[int | None, typer.Option("--output-limit-bytes")] = None,
    capture_level: Annotated[
        CaptureLevel | None,
        typer.Option(
            "--capture-level",
            help="Passive, process, sampled Python, or expensive deep capture",
        ),
    ] = None,
    instrument: Annotated[
        Instrumentation | None,
        typer.Option(
            "--instrument",
            help="Sample Python stacks or observe every call with deep capture",
        ),
    ] = None,
    observe_process_tree: Annotated[
        bool,
        typer.Option(
            "--observe-process-tree",
            help="Sample process-group RSS and CPU from the controller",
        ),
    ] = False,
) -> None:
    application = command_context(context)
    try:
        capture_worker_client = capture_worker_client_event()
        if capture_worker_client is None:
            finish(
                run_capture_worker(
                    application.arguments,
                    module=application.module,
                    detached=detach,
                )
            )
            return
        if not command:
            raise CaptureError("a command is required after --")
        execution_name = name or safe_runpack_name(Path(command[0]).name)
        destination = output or Path(f"{safe_runpack_name(execution_name)}.runpack")
        options = _record_options(
            cwd=cwd,
            detach=detach,
            identify_env=identify_env,
            include_output=include_output,
            output_limit_bytes=output_limit_bytes,
            capture_level=capture_level,
            instrument=instrument,
            observe_process_tree=observe_process_tree,
        )
        finish(
            run_record_command(
                options,
                tuple(command),
                name=execution_name,
                output=destination,
                error_label=application.error_label,
                capture_worker_client=capture_worker_client,
            )
        )
    except KeyboardInterrupt:
        finish(128 + signal.SIGINT)
    except (CaptureError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("recover", help="Finish a retained post-exit capture checkpoint.")
def recover_command(
    context: typer.Context,
    checkpoint: Annotated[Path, typer.Argument(help="Retained .runpack.tmp-* checkpoint")],
    output: Annotated[Path, typer.Option("--output", help="Output .runpack path")],
) -> None:
    application = command_context(context)
    try:
        recover_process_capture(checkpoint, output)
        print_text(f"recovered {terminal_text(output)}", stderr=True, style="green")
    except (CaptureError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


def _render_capture_job_result(job: CaptureJob, output_format: OutputFormat) -> str:
    if output_format is OutputFormat.json:
        return json.dumps(capture_job_document(job), allow_nan=False, indent=2, sort_keys=True)
    return render_capture_job(job)


@job_app.command("list", help="List retained capture jobs.")
def list_jobs_command(
    context: typer.Context,
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
) -> None:
    application = command_context(context)
    try:
        jobs = list_capture_jobs()
        if output_format is OutputFormat.json:
            print_json(
                json.dumps(capture_jobs_document(jobs), allow_nan=False, indent=2, sort_keys=True)
                + "\n"
            )
        else:
            print_text(render_capture_jobs(jobs))
    except CaptureJobError as exc:
        fail(exc, label=application.error_label)
        finish(2)


@job_app.command("status", help="Inspect one retained capture job.")
def job_status_command(
    context: typer.Context,
    job_id: Annotated[str, typer.Argument()],
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
) -> None:
    _print_job_action(context, "status", job_id, output_format=output_format)


@job_app.command("wait", help="Wait for one retained capture job.")
def job_wait_command(
    context: typer.Context,
    job_id: Annotated[str, typer.Argument()],
    timeout: Annotated[float | None, typer.Option("--timeout")] = None,
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
) -> None:
    _print_job_action(
        context,
        "wait",
        job_id,
        timeout=timeout,
        output_format=output_format,
    )


@job_app.command("cancel", help="Cancel and reap one capture job.")
def job_cancel_command(
    context: typer.Context,
    job_id: Annotated[str, typer.Argument()],
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
) -> None:
    _print_job_action(context, "cancel", job_id, output_format=output_format)


def _print_job_action(
    context: typer.Context,
    action: str,
    job_id: str,
    *,
    output_format: OutputFormat,
    timeout: float | None = None,
) -> None:
    application = command_context(context)
    try:
        if action == "status":
            job = load_capture_job(job_id)
        elif action == "wait":
            job = wait_capture_job(job_id, timeout_seconds=timeout)
        else:
            job = cancel_capture_job(job_id)
        rendered = _render_capture_job_result(job, output_format)
        if output_format is OutputFormat.json:
            print_json(rendered + "\n")
        else:
            print_text(rendered)
        if action == "wait":
            finish(job.exit_status if job.exit_status is not None else 2)
    except CaptureJobError as exc:
        fail(exc, label=application.error_label)
        finish(2)


@job_app.command("output", help="Replay bounded output retained by --detach.")
def job_output_command(
    context: typer.Context,
    job_id: Annotated[str, typer.Argument()],
    follow: Annotated[
        bool,
        typer.Option("--follow", help="Follow new retained bytes until the job finishes"),
    ] = False,
) -> None:
    application = command_context(context)
    try:
        if follow:
            _follow_capture_job_output(job_id, error_label=application.error_label)
            return
        output = read_capture_job_output(job_id)
        _write_output("stdout", output.stdout)
        _write_output("stdout", output.stdout_tail)
        _write_output("stderr", output.stderr)
        _write_output("stderr", output.stderr_tail)
        _report_capture_job_output_truncation(output, error_label=application.error_label)
    except KeyboardInterrupt:
        finish(128 + signal.SIGINT)
    except CaptureJobError as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("inspect", help="Inspect normalized execution evidence.", rich_help_panel="Analyze")
def inspect_command(
    context: typer.Context,
    runpack: Annotated[Path, typer.Argument()],
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
    tree: Annotated[
        bool,
        typer.Option("--tree", help="Print application and operation causal structure"),
    ] = False,
    raw_tree: Annotated[
        bool,
        typer.Option("--raw-tree", help="Print every stored event and causal link"),
    ] = False,
) -> None:
    application = command_context(context)
    try:
        if tree and raw_tree:
            raise RunpackError("--tree and --raw-tree are mutually exclusive")
        if (tree or raw_tree) and output_format is not OutputFormat.text:
            option = "--raw-tree" if raw_tree else "--tree"
            raise RunpackError(f"{option} is only available with text output")
        path = resolve_runpack_path(runpack)
        with RunpackReader(path) as reader:
            summary = inspect_reader(reader)
            causal_tree = (
                render_causal_tree_reader(reader, raw=raw_tree) if tree or raw_tree else None
            )
        rendered = render_summary(summary, output_format.value)
        if output_format is OutputFormat.json:
            print_json(rendered + "\n")
        else:
            print_text(rendered)
            if causal_tree is not None:
                print_text()
                print_text(causal_tree)
    except RunpackError as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("query", help="Run bounded read-only SQL over a runpack.", rich_help_panel="Analyze")
def query_command(
    context: typer.Context,
    runpack: Annotated[Path, typer.Argument()],
    sql: Annotated[str, typer.Argument()],
    limit: Annotated[int, typer.Option("--limit", help="Maximum rows (hard limit: 100000)")] = 1000,
    output_format: Annotated[str, typer.Option("--format", help="table, json, or jsonl")] = "table",
) -> None:
    application = command_context(context)
    try:
        if output_format not in {"table", "json", "jsonl"}:
            raise QueryError("--format must be table, json, or jsonl")
        result = query_runpack(runpack, sql, limit=limit)
        rendered = render_query(result, output_format)
        if rendered:
            if output_format in {"json", "jsonl"}:
                print_json(rendered + "\n")
            else:
                print_text(rendered)
        if output_format == "jsonl" and result.truncated:
            print_text(
                f"{application.error_label}: query result truncated at the requested row limit",
                stderr=True,
                style="yellow",
            )
    except (QueryError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("providers", help="List bundled evidence providers.")
def providers_command(context: typer.Context) -> None:
    application = command_context(context)
    try:
        registry = resolve_provider_registry(reserved_commands=CORE_COMMANDS)
        print_renderable(_provider_table(registry))
    except ProviderError as exc:
        fail(exc, label=application.error_label)
        finish(2)


def _provider_table(registry: ProviderRegistry) -> Table:
    table = Table(show_header=True, header_style="bold cyan", box=None)
    table.add_column("Provider")
    table.add_column("State")
    table.add_column("Source")
    table.add_column("Commands")
    for provider in registry.inventory:
        source = provider.source
        if provider.distribution is not None:
            source = f"{source} ({terminal_text(provider.distribution)})"
        table.add_row(
            provider.key,
            "enabled" if provider.enabled else "disabled",
            source,
            ", ".join(provider.commands) if provider.commands else "-",
        )
    return table


def _write_output(name: str, content: bytes) -> None:
    if not content:
        return
    binary = binary_stream(name)
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
    print_text(
        f"{error_label}: retained {stream} was truncated{detail}",
        stderr=True,
        style="yellow",
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


@broken_pipe_safe
def main(argv: list[str] | None = None) -> int:
    return run_cli(
        app,
        argv,
        prog="runtime",
        module="runtime_tools.cli",
        error_label="runtime",
        branded_commands=False,
    )


if __name__ == "__main__":
    raise SystemExit(main())
