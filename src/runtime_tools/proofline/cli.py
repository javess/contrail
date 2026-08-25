"""Proofline command-line interface."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import sys
import threading
from functools import partial
from pathlib import Path

from runtime_tools._version import __version__
from runtime_tools.artifacts import (
    ArtifactError,
    AtomicArtifactPublication,
    prepare_atomic_artifact,
)
from runtime_tools.capture import (
    CAPTURE_LEVELS,
    CaptureError,
)
from runtime_tools.capture_jobs import record_current_capture_job_artifacts
from runtime_tools.capture_worker import capture_worker_client_event, run_capture_worker
from runtime_tools.json_support import output_document
from runtime_tools.proofline import (
    ContractError,
    ExperimentError,
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
    verify_contracts_with_artifact_bindings,
    verify_contracts_with_diff,
)
from runtime_tools.rundiff.compare import ExecutionDiff
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import broken_pipe_safe, terminal_text


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
    baseline: Path,
    candidate: Path,
    contract: Path,
    *,
    branded_commands: bool = False,
    proofline_report: Path | None = None,
) -> str:
    baseline_argument = _shell_path(baseline)
    candidate_argument = _shell_path(candidate)
    analyze_command = "contrail analyze" if branded_commands else "batchscope inspect"
    inspect_command = "contrail inspect" if branded_commands else "runtime inspect"
    serve_command = "contrail serve" if branded_commands else "runtime serve"
    if proofline_report is None:
        evidence_argument = f"--contract {_shell_path(contract)}"
    else:
        evidence_argument = f"--proofline-report {_shell_path(proofline_report)}"
    return "\n".join(
        (
            render_diff(diff, "text"),
            "",
            "Next steps:",
            f"{analyze_command} {candidate_argument}",
            f"{inspect_command} {candidate_argument} --tree",
            f"{serve_command} {baseline_argument} --compare {candidate_argument} "
            f"{evidence_argument}",
        )
    )


@broken_pipe_safe
def main(
    argv: list[str] | None = None,
    *,
    prog: str = "proofline",
    error_label: str = "proofline",
    branded_commands: bool = False,
    _launch_capture_worker: bool | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    launch_capture_worker = (
        argv is None if _launch_capture_worker is None else _launch_capture_worker
    )
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    validate = subparsers.add_parser(
        "validate", help="validate contract and optional parameter inputs without execution"
    )
    validate.add_argument("contract", type=Path)
    validate.add_argument("--parameters", type=Path)
    validate.add_argument("--format", choices=("text", "json"), default="text")

    verify = subparsers.add_parser("verify", help="evaluate contracts over two runpacks")
    verify.add_argument("contract", type=Path)
    verify.add_argument("--baseline", type=Path, required=True)
    verify.add_argument("--candidate", type=Path, required=True)
    verify.add_argument("--format", choices=("text", "json"), default="text")
    verify.add_argument(
        "--explain",
        action="store_true",
        help="include the runtime diff and commands for deeper inspection",
    )
    verify.add_argument(
        "--report",
        type=Path,
        metavar="PATH",
        help="atomically retain the artifact-bound explained JSON report (implies --explain)",
    )

    run = subparsers.add_parser("run", help="capture and verify a workload at two Git refs")
    run.add_argument("contract", type=Path)
    run.add_argument("--baseline-ref", required=True)
    run.add_argument("--candidate-ref", required=True)
    run.add_argument("--workload", type=Path, required=True)
    run.add_argument(
        "--python",
        dest="python_executable",
        type=Path,
        help="Python executable used for both workload refs (default: current Python)",
    )
    run.add_argument("--output-dir", type=Path)
    run.add_argument(
        "--detach",
        action="store_true",
        help=(
            "run in the background and privately retain bounded stdout/stderr (may contain secrets)"
        ),
    )
    run.add_argument(
        "--capture-level",
        choices=CAPTURE_LEVELS,
        help="use the same passive, process, sample, or expensive deep preset for both refs",
    )
    run.add_argument("--format", choices=("text", "json"), default="text")
    run.add_argument(
        "--explain",
        action="store_true",
        help="include the runtime diff and commands for deeper inspection",
    )
    run.add_argument(
        "--report",
        type=Path,
        metavar="PATH",
        help="atomically retain the artifact-bound explained JSON report (implies --explain)",
    )
    run.add_argument(
        "--workload-arg",
        action="append",
        default=[],
        help="argument passed to the workload; repeat for multiple arguments",
    )

    search = subparsers.add_parser(
        "search", help="find a contract counterexample within a bounded search"
    )
    search.add_argument("contract", type=Path)
    search.add_argument("--parameters", type=Path, required=True)
    search.add_argument("--baseline-ref", required=True)
    search.add_argument("--candidate-ref", required=True)
    search.add_argument("--workload", type=Path, required=True)
    search.add_argument(
        "--python",
        dest="python_executable",
        type=Path,
        help="Python executable used for every search workload (default: current Python)",
    )
    search.add_argument("--output-dir", type=Path)
    search.add_argument(
        "--detach",
        action="store_true",
        help=(
            "run in the background and privately retain bounded stdout/stderr (may contain secrets)"
        ),
    )
    search.add_argument(
        "--capture-level",
        choices=CAPTURE_LEVELS,
        help="use the same passive, process, sample, or expensive deep preset for every run",
    )
    search.add_argument("--format", choices=("text", "json"), default="text")
    search.add_argument(
        "--max-examples", type=int, default=25, help="search bound (hard limit: 1000)"
    )
    args = parser.parse_args(arguments)
    capture_worker_client: threading.Event | None = None
    if launch_capture_worker and args.subcommand in {"run", "search"}:
        try:
            capture_worker_client = capture_worker_client_event()
            if capture_worker_client is None:
                module = (
                    "runtime_tools.contrail_cli"
                    if branded_commands
                    else "runtime_tools.proofline.cli"
                )
                return run_capture_worker(
                    tuple(arguments),
                    module=module,
                    detached=args.detach,
                    detached_format=args.format,
                )
        except CaptureError as exc:
            print(f"{error_label}: {terminal_text(exc)}", file=sys.stderr)
            return 2
    diff: ExecutionDiff | None = None
    artifact_bindings: VerificationArtifactBindings | None = None
    report_publication: AtomicArtifactPublication | None = None
    try:
        report_destination = getattr(args, "report", None)
        if report_destination is not None:
            report_publication = prepare_atomic_artifact(report_destination, label="report")
        include_explanation = bool(getattr(args, "explain", False) or report_publication)
        capture_level = getattr(args, "capture_level", None)
        if capture_level == "deep":
            print(
                f"{error_label}: warning: deep capture observes every Python and native C call "
                "plus "
                "Python exception propagation "
                "in both "
                "arms; it is expensive, intrusive, and can materially perturb timings",
                file=sys.stderr,
            )
        elif capture_level == "sample":
            print(
                f"{error_label}: note: sampling estimates Python hotspots in both arms and "
                "may perturb timings",
                file=sys.stderr,
            )
        if args.subcommand == "validate":
            validation = validate_inputs(args.contract, args.parameters)
            print(render_validation(validation, args.format))
            return 0
        if args.subcommand == "search":
            search = search_counterexample
            if capture_worker_client is not None:
                search = partial(
                    search,
                    _capture_client_disconnected=capture_worker_client,
                )
            counterexample = search(
                args.contract,
                args.parameters,
                baseline_ref=args.baseline_ref,
                candidate_ref=args.candidate_ref,
                workload=args.workload,
                output_dir=args.output_dir or default_output_directory(),
                max_examples=args.max_examples,
                python_executable=args.python_executable,
                capture_level=args.capture_level,
            )
            if counterexample is None:
                if args.format == "json":
                    print(
                        json.dumps(
                            output_document(
                                "proofline.search",
                                {
                                    "counterexample": None,
                                    "max_examples": args.max_examples,
                                },
                            ),
                            allow_nan=False,
                            indent=2,
                            sort_keys=True,
                        )
                    )
                else:
                    print(f"No counterexample found in {args.max_examples} examples.")
                return 0
            record_current_capture_job_artifacts(
                (
                    counterexample.experiment.baseline_runpack,
                    counterexample.experiment.candidate_runpack,
                )
            )
            if args.format == "json":
                print(
                    json.dumps(
                        output_document(
                            "proofline.search",
                            {
                                "counterexample": counterexample.as_json_value(),
                                "max_examples": args.max_examples,
                            },
                        ),
                        allow_nan=False,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 1
            print("COUNTEREXAMPLE")
            print()
            for name, value in sorted(counterexample.parameters.items()):
                print(f"{terminal_text(name)}={value}")
            print()
            print(f"shrink_budget_exhausted: {str(counterexample.shrink_budget_exhausted).lower()}")
            print()
            print(render_verification(counterexample.experiment.verification, "text"))
            print()
            print(
                f"baseline artifact:  {terminal_text(counterexample.experiment.baseline_runpack)}"
            )
            print(
                f"candidate artifact: {terminal_text(counterexample.experiment.candidate_runpack)}"
            )
            return 1
        if args.subcommand == "run":
            execute_experiment = run_experiment
            if capture_worker_client is not None:
                execute_experiment = partial(
                    execute_experiment,
                    _capture_client_disconnected=capture_worker_client,
                )
            experiment = execute_experiment(
                args.contract,
                baseline_ref=args.baseline_ref,
                candidate_ref=args.candidate_ref,
                workload=args.workload,
                workload_args=tuple(args.workload_arg),
                output_dir=args.output_dir or default_output_directory(),
                python_executable=args.python_executable,
                capture_level=args.capture_level,
                bind_artifacts=include_explanation
                and (args.format == "json" or report_publication is not None),
            )
            record_current_capture_job_artifacts(
                (experiment.baseline_runpack, experiment.candidate_runpack)
            )
            report = experiment.verification
            if include_explanation:
                if experiment.diff is None:
                    raise ExperimentError("runtime diff is unavailable for this experiment")
                diff = experiment.diff
                if report_publication is not None and experiment.artifact_bindings is None:
                    raise ExperimentError("artifact bindings are unavailable for this experiment")
        else:
            experiment = None
            if include_explanation:
                if args.format == "json" or report_publication is not None:
                    report, diff, artifact_bindings = verify_contracts_with_artifact_bindings(
                        args.contract,
                        args.baseline,
                        args.candidate,
                    )
                else:
                    report, diff = verify_contracts_with_diff(
                        args.contract, args.baseline, args.candidate
                    )
            else:
                report = verify_contracts(args.contract, args.baseline, args.candidate)

        payload = None
        serialized_payload = None
        if args.format == "json" or report_publication is not None:
            if experiment is not None:
                payload = experiment.as_json_value(include_evidence=include_explanation)
            else:
                payload = report.as_json_value(
                    include_evidence=include_explanation,
                    artifact_bindings=artifact_bindings,
                )
            if diff is not None:
                payload["diff"] = diff.as_json_value()
            serialized_payload = (
                json.dumps(
                    payload,
                    allow_nan=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            )
        if report_publication is not None:
            assert serialized_payload is not None
            report_publication.publish(serialized_payload.encode("utf-8"))
            if experiment is not None:
                assert report_destination is not None
                record_current_capture_job_artifacts(
                    (
                        experiment.baseline_runpack,
                        experiment.candidate_runpack,
                        report_destination,
                    )
                )

        if args.format == "json":
            assert serialized_payload is not None
            print(serialized_payload, end="")
        else:
            print(render_verification(report, "text", include_evidence=include_explanation))
            if diff is not None:
                baseline = experiment.baseline_runpack if experiment is not None else args.baseline
                candidate = (
                    experiment.candidate_runpack if experiment is not None else args.candidate
                )
                print()
                print(
                    _render_explanation(
                        diff,
                        baseline,
                        candidate,
                        args.contract,
                        branded_commands=branded_commands,
                        proofline_report=report_destination,
                    )
                )
        if experiment is not None and args.format == "text":
            print()
            print(f"baseline artifact:  {terminal_text(experiment.baseline_runpack)}")
            print(f"candidate artifact: {terminal_text(experiment.candidate_runpack)}")
        return 0 if report.passed else 1
    except KeyboardInterrupt:
        if capture_worker_client is not None:
            return 128 + signal.SIGINT
        raise
    except (ArtifactError, CaptureError, ContractError, ExperimentError, RunpackError) as exc:
        print(f"{error_label}: {terminal_text(exc)}", file=sys.stderr)
        return 2
    finally:
        if report_publication is not None:
            report_publication.close()


if __name__ == "__main__":
    raise SystemExit(main())
