"""Typed BatchScope analysis command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from runtime_tools._cli_support import (
    OutputFormat,
    command_context,
    fail,
    finish,
    run_cli,
)
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.report import render_analysis, render_analysis_summary
from runtime_tools.console import print_json, print_text
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe

app = typer.Typer(
    name="batchscope",
    help="Explain lifecycle, critical path, and bottlenecks.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)


@app.callback()
def root() -> None:
    """BatchScope commands."""


def analyze_command(
    context: typer.Context,
    runpack: Annotated[Path, typer.Argument()],
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            help="Include capture diagnostics and full evidence lists",
        ),
    ] = False,
) -> None:
    application = command_context(context)
    try:
        analysis = analyze_runpack(runpack)
        if output_format is OutputFormat.json:
            print_json(render_analysis(analysis, "json") + "\n")
        elif verbose:
            print_text(render_analysis(analysis, "text"))
        else:
            print_text(render_analysis_summary(analysis))
    except RunpackError as exc:
        fail(exc, label=application.error_label)
        finish(2)


app.command("inspect", help="Explain one finite execution.")(analyze_command)


@broken_pipe_safe
def main(argv: list[str] | None = None) -> int:
    return run_cli(
        app,
        argv,
        prog="batchscope",
        module="runtime_tools.batchscope.cli",
        error_label="batchscope",
        branded_commands=False,
    )


if __name__ == "__main__":
    raise SystemExit(main())
