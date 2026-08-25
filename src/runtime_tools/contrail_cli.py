"""One branded command surface for the Contrail workflow."""

from __future__ import annotations

import argparse
import os
import shlex
import sys
from pathlib import Path

from runtime_tools._version import __version__
from runtime_tools.batchscope.cli import main as batchscope_main
from runtime_tools.cli import main as runtime_main
from runtime_tools.proofline.cli import main as proofline_main
from runtime_tools.rundiff.cli import main as rundiff_main
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe, terminal_text

_RUNTIME_COMMANDS = frozenset(
    {
        "record",
        "recover",
        "inspect",
        "job",
        "serve",
        "query",
        "import-otel",
        "enrich-kubernetes",
        "enrich-prometheus",
        "enrich-otel-logs",
        "enrich-temporal-history",
    }
)
_PROOFLINE_COMMANDS = frozenset({"validate", "verify", "run", "search"})
_HELP = """\
usage: contrail COMMAND [ARGS...]

Capture runtime evidence, enforce behavioral contracts, and follow failures to
the exact candidate events that explain them.

Start here:
  contrail demo              run the installed five-minute walkthrough

Core workflow (record → verify → serve):
  record                     capture one local process as a .runpack
  recover                    finish a retained post-exit capture checkpoint
  job                        discover, wait for, replay, or cancel a capture worker
  compare                    compare baseline and candidate runpacks
  report                     combine candidate analysis, diff, and contract evidence
  verify                     evaluate a behavioral contract
  serve                      open the local evidence timeline
  inspect                    inspect normalized execution evidence
  analyze                    explain lifecycle, critical path, and bottlenecks

Automation and adapters:
  validate, run, search, query, import-otel,
  enrich-kubernetes, enrich-prometheus, enrich-otel-logs,
  enrich-temporal-history

Run 'contrail COMMAND --help' for command-specific options.
"""


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


def _demo_main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="contrail demo",
        description="generate a self-contained baseline-to-evidence walkthrough",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("contrail-demo"))
    args = parser.parse_args(arguments)
    from runtime_tools.demo import DemoError, run_demo

    try:
        result = run_demo(args.output_dir)
    except KeyboardInterrupt:
        print("contrail: demo interrupted; completed artifacts were retained", file=sys.stderr)
        return 130
    except DemoError as exc:
        print(f"contrail: {terminal_text(exc)}", file=sys.stderr)
        return 2

    print("CONTRAIL DEMO READY")
    print()
    print("The candidate preserves its result but violates two runtime contracts:")
    print("  db.write operations:       3 → 30")
    print("  new metadata-db dependency: 0 → 1")
    print()
    print(f"baseline: {terminal_text(result.baseline_runpack)}")
    print(f"candidate: {terminal_text(result.candidate_runpack)}")
    print(f"adaptable workload: {terminal_text(result.workload)}")
    print(f"contract: {terminal_text(result.contract)}")
    print(f"explained report: {terminal_text(result.proofline_report)}")
    print()
    print("Compare the runtime change:")
    print("  " + _command("contrail", "compare", result.baseline_runpack, result.candidate_runpack))
    print()
    print("Explain where the candidate spent its time:")
    print("  " + _command("contrail", "analyze", result.candidate_runpack))
    print()
    print("Review one integrated diagnostic report:")
    print(
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
    print()
    recaptured_baseline = result.output_dir / "recaptured-baseline.runpack"
    recaptured_candidate = result.output_dir / "recaptured-candidate.runpack"
    recaptured_report = result.output_dir / "recaptured-report.json"
    print("Adapt workload.py and contract.yaml, then capture the two variants again:")
    print(
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
    print(
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
    print(
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
    print()
    print("Re-run the contract gate (expected exit 1):")
    print(
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
    print()
    print("Open the retained failure without rerunning the workload:")
    print(
        "  "
        + _command(
            "contrail",
            "serve",
            result.baseline_runpack,
            "--compare",
            result.candidate_runpack,
            "--proofline-report",
            result.proofline_report,
        )
    )
    return 0


def _report_main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="contrail report",
        description=(
            "combine candidate BatchScope analysis, RunDiff comparison, and optional "
            "Proofline evidence"
        ),
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--contract", type=Path)
    args = parser.parse_args(arguments)
    from runtime_tools.proofline.contracts import ContractError
    from runtime_tools.suite_report import build_suite_report, render_suite_report

    try:
        report = build_suite_report(
            args.baseline,
            args.candidate,
            contract=args.contract,
        )
    except (ContractError, RunpackError) as exc:
        print(f"contrail: {terminal_text(exc)}", file=sys.stderr)
        return 2
    print(render_suite_report(report))
    return 0


def _dispatch(
    command: str,
    arguments: list[str],
    *,
    launch_capture_worker: bool = False,
) -> int:
    if command in _RUNTIME_COMMANDS:
        return runtime_main(
            [command, *arguments],
            prog="contrail",
            error_label="contrail",
        )
    if command == "compare":
        return rundiff_main(
            [command, *arguments],
            prog="contrail",
            error_label="contrail",
        )
    if command == "analyze":
        return batchscope_main(
            [command, *arguments],
            prog="contrail",
            error_label="contrail",
            command_name="analyze",
        )
    if command in _PROOFLINE_COMMANDS:
        return proofline_main(
            [command, *arguments],
            prog="contrail",
            error_label="contrail",
            branded_commands=True,
            _launch_capture_worker=launch_capture_worker,
        )
    raise AssertionError(f"unhandled Contrail command: {command}")


@broken_pipe_safe
def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments == ["--help"] or arguments == ["-h"]:
        print(_HELP, end="")
        return 0
    if arguments == ["--version"]:
        print(f"contrail {__version__}")
        return 0
    command, *command_arguments = arguments
    if command == "demo":
        return _demo_main(command_arguments)
    if command == "report":
        return _report_main(command_arguments)
    if command not in _RUNTIME_COMMANDS | _PROOFLINE_COMMANDS | {"compare", "analyze"}:
        print(f"contrail: unknown command: {terminal_text(command)}", file=sys.stderr)
        print("Try 'contrail --help'.", file=sys.stderr)
        return 2
    return _dispatch(
        command,
        command_arguments,
        launch_capture_worker=argv is None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
