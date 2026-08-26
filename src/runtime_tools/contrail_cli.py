"""The single installed Contrail command tree."""

from __future__ import annotations

import os
import shlex
import sys
from pathlib import Path
from typing import Annotated

import typer

from runtime_tools._cli_support import fail, finish, run_cli
from runtime_tools._version import __version__
from runtime_tools.batchscope.cli import analyze_command
from runtime_tools.cli import app as runtime_app
from runtime_tools.console import print_text
from runtime_tools.proofline.cli import app as proofline_app
from runtime_tools.rundiff.cli import compare_command
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe, terminal_text

app = typer.Typer(
    name="contrail",
    help=(
        "Capture runtime evidence, enforce behavioral contracts, and follow failures "
        "to the exact candidate events that explain them."
    ),
    epilog=(
        "Start with [bold]contrail demo[/bold] for a complete local walkthrough. "
        "The core workflow is [bold]record → compare → verify[/bold]."
    ),
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)
app.add_typer(runtime_app, name=None)
app.add_typer(proofline_app, name=None)
app.command(
    "compare",
    help="Compare baseline and candidate runpacks.",
    rich_help_panel="Analyze",
)(compare_command)
app.command(
    "analyze",
    help="Explain lifecycle, critical path, and bottlenecks.",
    rich_help_panel="Analyze",
)(analyze_command)


def _show_version(value: bool) -> None:
    if value:
        print_text(f"contrail {__version__}")
        raise typer.Exit()


@app.callback()
def root_options(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_show_version,
            is_eager=True,
            help="Show the installed Contrail version and exit",
        ),
    ] = False,
) -> None:
    """One terminal surface for capture, analysis, and verification."""


@app.command("demo", help="Run the installed five-minute walkthrough.", rich_help_panel="Start")
def demo_command(
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for generated demo artifacts")
    ] = Path("contrail-demo"),
) -> None:
    from runtime_tools.demo import DemoError, run_demo

    try:
        result = run_demo(output_dir)
    except KeyboardInterrupt:
        print_text(
            "contrail: demo interrupted; completed artifacts were retained",
            stderr=True,
            style="yellow",
        )
        finish(130)
        return
    except DemoError as exc:
        fail(exc)
        finish(2)
        return

    print_text("CONTRAIL DEMO READY", style="bold green")
    print_text()
    print_text("The candidate preserves its result but violates two runtime contracts:")
    print_text("  db.write operations:       3 → 30")
    print_text("  new metadata-db dependency: 0 → 1")
    print_text()
    print_text(f"baseline: {terminal_text(result.baseline_runpack)}")
    print_text(f"candidate: {terminal_text(result.candidate_runpack)}")
    print_text(f"adaptable workload: {terminal_text(result.workload)}")
    print_text(f"contract: {terminal_text(result.contract)}")
    print_text(f"explained report: {terminal_text(result.proofline_report)}")
    print_text()
    print_text("Compare the runtime change:", style="bold")
    print_text(
        "  " + _command("contrail", "compare", result.baseline_runpack, result.candidate_runpack)
    )
    print_text()
    print_text("Explain where the candidate spent its time:", style="bold")
    print_text("  " + _command("contrail", "analyze", result.candidate_runpack))
    print_text()
    print_text("Review one integrated diagnostic report:", style="bold")
    print_text(
        "  "
        + _command(
            "contrail",
            "report",
            result.baseline_runpack,
            result.candidate_runpack,
            "--contract",
            result.contract,
        )
    )
    print_text()
    _print_recapture_steps(result)


def _print_recapture_steps(result: object) -> None:
    from runtime_tools.demo import DemoResult

    if not isinstance(result, DemoResult):
        raise TypeError("unexpected demo result")
    recaptured_baseline = result.output_dir / "recaptured-baseline.runpack"
    recaptured_candidate = result.output_dir / "recaptured-candidate.runpack"
    recaptured_report = result.output_dir / "recaptured-report.json"
    print_text("Adapt workload.py and contract.yaml, then capture the two variants again:")
    print_text(
        "  "
        + _command(
            "contrail",
            "record",
            "--name",
            "demo-baseline",
            "--output",
            recaptured_baseline,
            "--",
            sys.executable,
            result.workload,
            "baseline",
        )
    )
    print_text(
        "  "
        + _command(
            "contrail",
            "record",
            "--name",
            "demo-candidate",
            "--output",
            recaptured_candidate,
            "--",
            sys.executable,
            result.workload,
            "candidate",
        )
    )
    print_text(
        "  "
        + _command(
            "contrail",
            "verify",
            result.contract,
            "--baseline",
            recaptured_baseline,
            "--candidate",
            recaptured_candidate,
            "--report",
            recaptured_report,
        )
    )
    print_text()
    print_text("Re-run the contract gate (expected exit 1):")
    print_text(
        "  "
        + _command(
            "contrail",
            "verify",
            result.contract,
            "--baseline",
            result.baseline_runpack,
            "--candidate",
            result.candidate_runpack,
            "--explain",
        )
    )


@app.command(
    "report",
    help="Combine candidate analysis, diff, and optional contract evidence.",
    rich_help_panel="Analyze",
)
def report_command(
    baseline: Annotated[Path, typer.Argument()],
    candidate: Annotated[Path, typer.Argument()],
    contract: Annotated[Path | None, typer.Option("--contract")] = None,
) -> None:
    from runtime_tools.proofline.contracts import ContractError
    from runtime_tools.suite_report import build_suite_report, render_suite_report

    try:
        report = build_suite_report(baseline, candidate, contract=contract)
    except (ContractError, RunpackError) as exc:
        fail(exc)
        finish(2)
        return
    print_text(render_suite_report(report))


def _shell_argument(argument: object) -> str:
    raw = str(argument)
    if raw.isprintable():
        return shlex.quote(raw)
    escaped = []
    for byte in os.fsencode(raw):
        if 0x20 <= byte < 0x7F and byte not in (0x27, 0x5C):
            escaped.append(chr(byte))
        elif byte == 0x27:
            escaped.append(r"\'")
        elif byte == 0x5C:
            escaped.append(r"\\")
        else:
            escaped.append(f"\\x{byte:02x}")
    return f"$'{''.join(escaped)}'"


def _command(*arguments: object) -> str:
    return " ".join(_shell_argument(argument) for argument in arguments)


@broken_pipe_safe
def main(argv: list[str] | None = None) -> int:
    return run_cli(
        app,
        argv,
        prog="contrail",
        module="runtime_tools.contrail_cli",
        error_label="contrail",
    )


if __name__ == "__main__":
    raise SystemExit(main())
