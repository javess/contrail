"""Typed RunDiff capture and comparison commands."""

from __future__ import annotations

import signal
from pathlib import Path
from typing import Annotated

import typer

from runtime_tools._cli_support import (
    CaptureLevel,
    Instrumentation,
    OutputFormat,
    RecordOptions,
    command_context,
    fail,
    finish,
    run_cli,
    run_record_command,
    safe_runpack_name,
)
from runtime_tools.capture import CaptureError
from runtime_tools.capture_worker import capture_worker_client_event, run_capture_worker
from runtime_tools.console import print_json, print_text
from runtime_tools.rundiff.compare import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe

app = typer.Typer(
    name="rundiff",
    help="Capture and compare normalized runtime evidence.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)


@app.command(
    "record",
    help="Capture a named local execution.",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def record_command(
    context: typer.Context,
    name: Annotated[str, typer.Argument(help="Logical execution name")],
    command: Annotated[list[str], typer.Argument(help="Command after --")],
    output: Annotated[Path | None, typer.Option("--output")] = None,
    cwd: Annotated[Path | None, typer.Option("--cwd")] = None,
    detach: Annotated[bool, typer.Option("--detach")] = False,
    identify_env: Annotated[list[str] | None, typer.Option("--identify-env")] = None,
    include_output: Annotated[bool, typer.Option("--include-output")] = False,
    output_limit_bytes: Annotated[int | None, typer.Option("--output-limit-bytes")] = None,
    capture_level: Annotated[CaptureLevel | None, typer.Option("--capture-level")] = None,
    instrument: Annotated[Instrumentation | None, typer.Option("--instrument")] = None,
    observe_process_tree: Annotated[bool, typer.Option("--observe-process-tree")] = False,
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
        options = RecordOptions(
            cwd=cwd,
            detach=detach,
            identify_environment=tuple(identify_env or ()),
            include_output=include_output,
            output_limit_bytes=output_limit_bytes,
            capture_level=capture_level.value if capture_level is not None else None,
            instrument=instrument.value if instrument is not None else None,
            observe_process_tree=observe_process_tree,
        )
        finish(
            run_record_command(
                options,
                tuple(command),
                name=name,
                output=output or Path(f"{safe_runpack_name(name)}.runpack"),
                error_label=application.error_label,
                capture_worker_client=capture_worker_client,
            )
        )
    except KeyboardInterrupt:
        finish(128 + signal.SIGINT)
    except (CaptureError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("compare", help="Compare two .runpack artifacts.")
def compare_command(
    context: typer.Context,
    baseline: Annotated[Path, typer.Argument()],
    candidate: Annotated[Path, typer.Argument()],
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
    require_equivalent_outcome: Annotated[
        bool,
        typer.Option(
            "--require-equivalent-outcome",
            help="Exit 1 unless the behavioral outcome is equivalent",
        ),
    ] = False,
) -> None:
    application = command_context(context)
    try:
        diff = compare_runpacks(_resolve_runpack(baseline), _resolve_runpack(candidate))
        rendered = render_diff(diff, output_format.value)
        if output_format is OutputFormat.json:
            print_json(rendered + "\n")
        else:
            print_text(rendered)
        if require_equivalent_outcome and diff.outcome != "equivalent":
            finish(1)
    except (CaptureError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


def _resolve_runpack(path: Path) -> Path:
    try:
        is_file = path.is_file()
    except (OSError, RuntimeError) as exc:
        raise RunpackError(f"could not resolve runpack path: {path}") from exc
    if is_file or path.suffix == ".runpack":
        return path
    return path.with_name(f"{path.name}.runpack")


@broken_pipe_safe
def main(argv: list[str] | None = None) -> int:
    return run_cli(
        app,
        argv,
        prog="rundiff",
        module="runtime_tools.rundiff.cli",
        error_label="rundiff",
        branded_commands=False,
    )


if __name__ == "__main__":
    raise SystemExit(main())
