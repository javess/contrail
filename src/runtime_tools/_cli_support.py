"""Shared typed command support for Contrail's presentation layer."""

from __future__ import annotations

import re
import sys
import threading
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, cast

import typer

from runtime_tools._version import __version__
from runtime_tools.capture import CaptureError, record_process
from runtime_tools.capture_jobs import record_current_capture_job_artifacts
from runtime_tools.console import diagnostic, print_text
from runtime_tools.terminal import terminal_text


class CaptureLevel(StrEnum):
    passive = "passive"
    process = "process"
    sample = "sample"
    deep = "deep"


class Instrumentation(StrEnum):
    sample = "sample"
    deep = "deep"


class OutputFormat(StrEnum):
    text = "text"
    json = "json"


@dataclass(frozen=True, slots=True)
class CommandContext:
    module: str
    error_label: str
    launch_capture_worker: bool
    arguments: tuple[str, ...]
    branded_commands: bool = True


@dataclass(frozen=True, slots=True)
class RecordOptions:
    cwd: Path | None = None
    detach: bool = False
    identify_environment: tuple[str, ...] = ()
    include_output: bool = False
    output_limit_bytes: int | None = None
    capture_level: str | None = None
    instrument: str | None = None
    observe_process_tree: bool = False


def command_context(context: typer.Context) -> CommandContext:
    value = context.obj
    if not isinstance(value, CommandContext):
        raise RuntimeError("Contrail command context is unavailable")
    return value


def run_cli(
    app: typer.Typer,
    argv: list[str] | None,
    *,
    prog: str,
    module: str,
    error_label: str,
    branded_commands: bool = True,
    launch_capture_worker: bool | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments == ["--version"]:
        print_text(f"{prog} {__version__}")
        return 0
    if not arguments:
        arguments = ["--help"]
    context = CommandContext(
        module=module,
        error_label=error_label,
        launch_capture_worker=argv is None
        if launch_capture_worker is None
        else launch_capture_worker,
        arguments=tuple(arguments),
        branded_commands=branded_commands,
    )
    try:
        app(
            args=arguments,
            prog_name=prog,
            standalone_mode=True,
            obj=context,
        )
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    return 0


def finish(status: int) -> None:
    if status:
        raise typer.Exit(status)


def fail(error: object, *, label: str = "contrail") -> None:
    print_text(f"{label}: {terminal_text(error)}", stderr=True, style="bold red")


def capture_warning(
    capture_level: str | None,
    *,
    paired: bool = False,
    error_label: str = "contrail",
) -> None:
    if capture_level == "deep":
        target = " in both arms" if paired else ""
        diagnostic(
            "warning",
            "deep instrumentation is intrusive and can materially perturb timings; "
            "it observes every Python and native C call plus Python exception "
            f"propagation{target}",
            prefix=error_label,
        )
    elif capture_level == "sample":
        target = " in both arms" if paired else ""
        diagnostic(
            "note",
            f"sampling estimates Python hotspots{target} and may perturb timings",
            prefix=error_label,
        )


def safe_runpack_name(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-.")
    return normalized or "run"


def binary_stream(name: str) -> BinaryIO | None:
    stream = getattr(sys, name)
    return cast(BinaryIO | None, getattr(stream, "buffer", None))


def run_record_command(
    options: RecordOptions,
    command: tuple[str, ...],
    *,
    name: str,
    output: Path,
    error_label: str,
    capture_worker_client: threading.Event,
) -> int:
    if options.output_limit_bytes is not None and not options.include_output:
        raise CaptureError("--output-limit-bytes requires --include-output")
    if options.capture_level is not None and (
        options.instrument is not None or options.observe_process_tree
    ):
        raise CaptureError(
            "--capture-level cannot be combined with --instrument or --observe-process-tree"
        )
    capture_output_limit = (
        (options.output_limit_bytes if options.output_limit_bytes is not None else 1_048_576)
        if options.include_output
        else None
    )
    effective_instrument = (
        options.capture_level if options.capture_level in {"sample", "deep"} else options.instrument
    )
    capture_warning(effective_instrument, error_label=error_label)
    exit_code = record_process(
        command,
        output,
        name=name,
        cwd=options.cwd,
        stdout=binary_stream("stdout"),
        stderr=binary_stream("stderr"),
        capture_output_limit=capture_output_limit,
        identify_environment=options.identify_environment,
        capture_level=options.capture_level,
        instrument=options.instrument,
        observe_process_tree=options.observe_process_tree,
        _capture_client_disconnected=capture_worker_client,
    )
    record_current_capture_job_artifacts((output,))
    print_text(f"recorded {terminal_text(output)}", stderr=True, style="green")
    return 128 - exit_code if exit_code < 0 else exit_code
