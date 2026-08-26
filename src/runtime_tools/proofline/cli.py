"""Typed Proofline validation, verification, and search commands."""

from __future__ import annotations

import json
import os
import shlex
import signal
import threading
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Annotated

import typer

from runtime_tools._cli_support import (
    CaptureLevel,
    OutputFormat,
    capture_warning,
    command_context,
    fail,
    finish,
    run_cli,
)
from runtime_tools.artifacts import (
    ArtifactError,
    AtomicArtifactPublication,
    prepare_atomic_artifact,
)
from runtime_tools.capture import CaptureError
from runtime_tools.capture_jobs import record_current_capture_job_artifacts
from runtime_tools.capture_worker import capture_worker_client_event, run_capture_worker
from runtime_tools.console import print_json, print_text
from runtime_tools.json_support import output_document
from runtime_tools.proofline import (
    ContractError,
    ExperimentError,
    ExperimentResult,
    run_experiment,
    search_counterexample,
    validate_inputs,
    verify_contracts,
)
from runtime_tools.proofline.experiments import default_output_directory
from runtime_tools.proofline.report import render_verification
from runtime_tools.proofline.validation import render_validation
from runtime_tools.proofline.verify import (
    VerificationArtifactBindings,
    VerificationReport,
    verify_contracts_with_artifact_bindings,
    verify_contracts_with_diff,
)
from runtime_tools.rundiff.compare import ExecutionDiff
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe, terminal_text

app = typer.Typer(
    name="proofline",
    help="Validate and enforce behavioral contracts over runtime evidence.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)


@dataclass(frozen=True, slots=True)
class VerifySource:
    baseline: Path
    candidate: Path


@dataclass(frozen=True, slots=True)
class RunSource:
    baseline_ref: str
    candidate_ref: str
    workload: Path
    workload_args: tuple[str, ...]
    output_dir: Path
    python_executable: Path | None
    capture_level: str | None
    capture_worker_client: threading.Event | None


type VerificationSource = VerifySource | RunSource


@app.command("validate", help="Validate contract and optional parameter inputs.")
def validate_command(
    context: typer.Context,
    contract: Annotated[Path, typer.Argument()],
    parameters: Annotated[Path | None, typer.Option("--parameters")] = None,
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
) -> None:
    application = command_context(context)
    try:
        rendered = render_validation(validate_inputs(contract, parameters), output_format.value)
        if output_format is OutputFormat.json:
            print_json(rendered + "\n")
        else:
            print_text(rendered)
    except (ArtifactError, ContractError, ExperimentError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("verify", help="Evaluate contracts over two runpacks.")
def verify_command(
    context: typer.Context,
    contract: Annotated[Path, typer.Argument()],
    baseline: Annotated[Path, typer.Option("--baseline")],
    candidate: Annotated[Path, typer.Option("--candidate")],
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
    explain: Annotated[
        bool,
        typer.Option("--explain", help="Include the runtime diff and deeper inspection commands"),
    ] = False,
    report: Annotated[
        Path | None,
        typer.Option(
            "--report",
            metavar="PATH",
            help="Atomically retain the artifact-bound explained JSON report",
        ),
    ] = None,
) -> None:
    application = command_context(context)
    try:
        finish(
            _verification(
                context,
                contract,
                VerifySource(baseline, candidate),
                output_format=output_format,
                explain=explain,
                report_destination=report,
            )
        )
    except (ArtifactError, CaptureError, ContractError, ExperimentError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("run", help="Capture and verify a workload at two Git refs.")
def run_command(
    context: typer.Context,
    contract: Annotated[Path, typer.Argument()],
    baseline_ref: Annotated[str, typer.Option("--baseline-ref")],
    candidate_ref: Annotated[str, typer.Option("--candidate-ref")],
    workload: Annotated[Path, typer.Option("--workload")],
    python_executable: Annotated[Path | None, typer.Option("--python")] = None,
    output_dir: Annotated[Path | None, typer.Option("--output-dir")] = None,
    detach: Annotated[bool, typer.Option("--detach")] = False,
    capture_level: Annotated[CaptureLevel | None, typer.Option("--capture-level")] = None,
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
    explain: Annotated[
        bool,
        typer.Option("--explain", help="Include the runtime diff and deeper inspection commands"),
    ] = False,
    report: Annotated[
        Path | None,
        typer.Option(
            "--report",
            metavar="PATH",
            help="Atomically retain the artifact-bound explained JSON report",
        ),
    ] = None,
    workload_arg: Annotated[
        list[str] | None,
        typer.Option("--workload-arg", help="Repeat for multiple workload arguments"),
    ] = None,
) -> None:
    application = command_context(context)
    try:
        launched, capture_worker_client = _capture_client(
            context,
            detach=detach,
            output_format=output_format,
        )
        if launched:
            return
        level = capture_level.value if capture_level is not None else None
        source = RunSource(
            baseline_ref=baseline_ref,
            candidate_ref=candidate_ref,
            workload=workload,
            workload_args=tuple(workload_arg or ()),
            output_dir=output_dir or default_output_directory(),
            python_executable=python_executable,
            capture_level=level,
            capture_worker_client=capture_worker_client,
        )
        finish(
            _verification(
                context,
                contract,
                source,
                output_format=output_format,
                explain=explain,
                report_destination=report,
            )
        )
    except KeyboardInterrupt:
        finish(128 + signal.SIGINT)
    except (ArtifactError, CaptureError, ContractError, ExperimentError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


@app.command("search", help="Find a contract counterexample within a bounded search.")
def search_command(
    context: typer.Context,
    contract: Annotated[Path, typer.Argument()],
    parameters: Annotated[Path, typer.Option("--parameters")],
    baseline_ref: Annotated[str, typer.Option("--baseline-ref")],
    candidate_ref: Annotated[str, typer.Option("--candidate-ref")],
    workload: Annotated[Path, typer.Option("--workload")],
    python_executable: Annotated[Path | None, typer.Option("--python")] = None,
    output_dir: Annotated[Path | None, typer.Option("--output-dir")] = None,
    detach: Annotated[bool, typer.Option("--detach")] = False,
    capture_level: Annotated[CaptureLevel | None, typer.Option("--capture-level")] = None,
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.text,
    max_examples: Annotated[
        int, typer.Option("--max-examples", help="Search bound (hard limit: 1000)")
    ] = 25,
) -> None:
    application = command_context(context)
    try:
        launched, capture_worker_client = _capture_client(
            context,
            detach=detach,
            output_format=output_format,
        )
        if launched:
            return
        level = capture_level.value if capture_level is not None else None
        capture_warning(level, paired=True, error_label=application.error_label)
        search = search_counterexample
        if capture_worker_client is not None:
            search = partial(search, _capture_client_disconnected=capture_worker_client)
        counterexample = search(
            contract,
            parameters,
            baseline_ref=baseline_ref,
            candidate_ref=candidate_ref,
            workload=workload,
            output_dir=output_dir or default_output_directory(),
            max_examples=max_examples,
            python_executable=python_executable,
            capture_level=level,
        )
        if counterexample is None:
            if output_format is OutputFormat.json:
                print_json(
                    json.dumps(
                        output_document(
                            "proofline.search",
                            {"counterexample": None, "max_examples": max_examples},
                        ),
                        allow_nan=False,
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n"
                )
            else:
                print_text(f"No counterexample found in {max_examples} examples.")
            return
        record_current_capture_job_artifacts(
            (
                counterexample.experiment.baseline_runpack,
                counterexample.experiment.candidate_runpack,
            )
        )
        if output_format is OutputFormat.json:
            print_json(
                json.dumps(
                    output_document(
                        "proofline.search",
                        {
                            "counterexample": counterexample.as_json_value(),
                            "max_examples": max_examples,
                        },
                    ),
                    allow_nan=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            )
            finish(1)
            return
        print_text("COUNTEREXAMPLE", style="bold red")
        print_text()
        for name, value in sorted(counterexample.parameters.items()):
            print_text(f"{terminal_text(name)}={value}")
        print_text()
        print_text(
            f"shrink_budget_exhausted: {str(counterexample.shrink_budget_exhausted).lower()}"
        )
        print_text()
        print_text(render_verification(counterexample.experiment.verification, "text"))
        print_text()
        print_text(f"baseline artifact:  {counterexample.experiment.baseline_runpack}")
        print_text(f"candidate artifact: {counterexample.experiment.candidate_runpack}")
        finish(1)
    except KeyboardInterrupt:
        finish(128 + signal.SIGINT)
    except (ArtifactError, CaptureError, ContractError, ExperimentError, RunpackError) as exc:
        fail(exc, label=application.error_label)
        finish(2)


def _capture_client(
    context: typer.Context,
    *,
    detach: bool,
    output_format: OutputFormat,
) -> tuple[bool, threading.Event | None]:
    application = command_context(context)
    if not application.launch_capture_worker:
        return False, None
    client = capture_worker_client_event()
    if client is not None:
        return False, client
    status = run_capture_worker(
        application.arguments,
        module=application.module,
        detached=detach,
        detached_format=output_format.value,
    )
    finish(status)
    return True, None


def _verification(
    context: typer.Context,
    contract: Path,
    source: VerificationSource,
    *,
    output_format: OutputFormat,
    explain: bool,
    report_destination: Path | None,
) -> int:
    application = command_context(context)
    publication: AtomicArtifactPublication | None = None
    try:
        if report_destination is not None:
            publication = prepare_atomic_artifact(report_destination, label="report")
        include_explanation = explain or publication is not None
        diff: ExecutionDiff | None = None
        bindings: VerificationArtifactBindings | None = None
        experiment: ExperimentResult | None = None
        if isinstance(source, RunSource):
            capture_warning(
                source.capture_level,
                paired=True,
                error_label=application.error_label,
            )
            execute = run_experiment
            if source.capture_worker_client is not None:
                execute = partial(
                    execute,
                    _capture_client_disconnected=source.capture_worker_client,
                )
            experiment = execute(
                contract,
                baseline_ref=source.baseline_ref,
                candidate_ref=source.candidate_ref,
                workload=source.workload,
                workload_args=source.workload_args,
                output_dir=source.output_dir,
                python_executable=source.python_executable,
                capture_level=source.capture_level,
                bind_artifacts=include_explanation
                and (output_format is OutputFormat.json or publication is not None),
            )
            record_current_capture_job_artifacts(
                (experiment.baseline_runpack, experiment.candidate_runpack)
            )
            verification = experiment.verification
            if include_explanation:
                if experiment.diff is None:
                    raise ExperimentError("runtime diff is unavailable for this experiment")
                diff = experiment.diff
                if publication is not None and experiment.artifact_bindings is None:
                    raise ExperimentError("artifact bindings are unavailable for this experiment")
        else:
            verification, diff, bindings = _verify_source(
                contract,
                source,
                include_explanation=include_explanation,
                bind_artifacts=output_format is OutputFormat.json or publication is not None,
            )

        serialized = None
        if output_format is OutputFormat.json or publication is not None:
            serialized = _verification_json(
                verification,
                diff,
                experiment=experiment,
                bindings=bindings,
                include_explanation=include_explanation,
            )
        if publication is not None:
            assert serialized is not None
            publication.publish(serialized.encode("utf-8"))
            if experiment is not None:
                assert report_destination is not None
                record_current_capture_job_artifacts(
                    (
                        experiment.baseline_runpack,
                        experiment.candidate_runpack,
                        report_destination,
                    )
                )

        if output_format is OutputFormat.json:
            assert serialized is not None
            print_json(serialized)
        else:
            print_text(
                render_verification(
                    verification,
                    "text",
                    include_evidence=include_explanation,
                )
            )
            if diff is not None:
                candidate = (
                    experiment.candidate_runpack
                    if experiment is not None
                    else source.candidate
                    if isinstance(source, VerifySource)
                    else Path("candidate.runpack")
                )
                print_text()
                print_text(
                    _render_explanation(
                        diff,
                        candidate,
                        branded_commands=application.branded_commands,
                    )
                )
        if experiment is not None and output_format is OutputFormat.text:
            print_text()
            print_text(f"baseline artifact:  {experiment.baseline_runpack}")
            print_text(f"candidate artifact: {experiment.candidate_runpack}")
        return 0 if verification.passed else 1
    finally:
        if publication is not None:
            publication.close()


def _verify_source(
    contract: Path,
    source: VerifySource,
    *,
    include_explanation: bool,
    bind_artifacts: bool,
) -> tuple[VerificationReport, ExecutionDiff | None, VerificationArtifactBindings | None]:
    if not include_explanation:
        return verify_contracts(contract, source.baseline, source.candidate), None, None
    if bind_artifacts:
        report, diff, bindings = verify_contracts_with_artifact_bindings(
            contract, source.baseline, source.candidate
        )
        return report, diff, bindings
    report, diff = verify_contracts_with_diff(contract, source.baseline, source.candidate)
    return report, diff, None


def _verification_json(
    report: VerificationReport,
    diff: ExecutionDiff | None,
    *,
    experiment: ExperimentResult | None,
    bindings: VerificationArtifactBindings | None,
    include_explanation: bool,
) -> str:
    payload = (
        experiment.as_json_value(include_evidence=include_explanation)
        if experiment is not None
        else report.as_json_value(
            include_evidence=include_explanation,
            artifact_bindings=bindings,
        )
    )
    if diff is not None:
        payload["diff"] = diff.as_json_value()
    return json.dumps(payload, allow_nan=False, indent=2, sort_keys=True) + "\n"


def _shell_path(path: Path) -> str:
    raw = str(path)
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


def _render_explanation(
    diff: ExecutionDiff,
    candidate: Path,
    *,
    branded_commands: bool = False,
) -> str:
    candidate_argument = _shell_path(candidate)
    analyze_command = "contrail analyze" if branded_commands else "batchscope inspect"
    inspect_command = "contrail inspect" if branded_commands else "runtime inspect"
    return "\n".join(
        (
            render_diff(diff, "text"),
            "",
            "Next steps:",
            f"{analyze_command} {candidate_argument}",
            f"{inspect_command} {candidate_argument} --tree",
        )
    )


@broken_pipe_safe
def main(
    argv: list[str] | None = None,
    *,
    _launch_capture_worker: bool | None = None,
) -> int:
    return run_cli(
        app,
        argv,
        prog="proofline",
        module="runtime_tools.proofline.cli",
        error_label="proofline",
        branded_commands=False,
        launch_capture_worker=_launch_capture_worker,
    )


if __name__ == "__main__":
    raise SystemExit(main())
