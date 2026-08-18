"""Proofline command-line interface."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from runtime_tools import __version__
from runtime_tools.proofline import (
    ContractError,
    ExperimentError,
    run_experiment,
    search_counterexample,
    verify_contracts,
)
from runtime_tools.proofline.experiments import default_output_directory
from runtime_tools.proofline.report import render_verification
from runtime_tools.storage import RunpackError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="proofline")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    verify = subparsers.add_parser("verify", help="evaluate contracts over two runpacks")
    verify.add_argument("contract", type=Path)
    verify.add_argument("--baseline", type=Path, required=True)
    verify.add_argument("--candidate", type=Path, required=True)
    verify.add_argument("--format", choices=("text", "json"), default="text")

    run = subparsers.add_parser("run", help="capture and verify a workload at two Git refs")
    run.add_argument("contract", type=Path)
    run.add_argument("--baseline-ref", required=True)
    run.add_argument("--candidate-ref", required=True)
    run.add_argument("--workload", type=Path, required=True)
    run.add_argument("--output-dir", type=Path)
    run.add_argument("--format", choices=("text", "json"), default="text")
    run.add_argument(
        "--workload-arg",
        action="append",
        default=[],
        help="argument passed to the workload; repeat for multiple arguments",
    )

    search = subparsers.add_parser("search", help="find a minimized contract counterexample")
    search.add_argument("contract", type=Path)
    search.add_argument("--parameters", type=Path, required=True)
    search.add_argument("--baseline-ref", required=True)
    search.add_argument("--candidate-ref", required=True)
    search.add_argument("--workload", type=Path, required=True)
    search.add_argument("--output-dir", type=Path)
    search.add_argument(
        "--max-examples", type=int, default=25, help="search bound (hard limit: 1000)"
    )
    args = parser.parse_args(argv)
    try:
        if args.subcommand == "search":
            counterexample = search_counterexample(
                args.contract,
                args.parameters,
                baseline_ref=args.baseline_ref,
                candidate_ref=args.candidate_ref,
                workload=args.workload,
                output_dir=args.output_dir or default_output_directory(),
                max_examples=args.max_examples,
            )
            if counterexample is None:
                print(f"No counterexample found in {args.max_examples} examples.")
                return 0
            print("COUNTEREXAMPLE")
            print()
            for name, value in sorted(counterexample.parameters.items()):
                print(f"{name}={value}")
            print()
            print(render_verification(counterexample.experiment.verification, "text"))
            print()
            print(f"baseline artifact:  {counterexample.experiment.baseline_runpack}")
            print(f"candidate artifact: {counterexample.experiment.candidate_runpack}")
            return 1
        if args.subcommand == "run":
            experiment = run_experiment(
                args.contract,
                baseline_ref=args.baseline_ref,
                candidate_ref=args.candidate_ref,
                workload=args.workload,
                workload_args=tuple(args.workload_arg),
                output_dir=args.output_dir or default_output_directory(),
            )
            report = experiment.verification
        else:
            experiment = None
            report = verify_contracts(args.contract, args.baseline, args.candidate)
    except (ContractError, ExperimentError, RunpackError) as exc:
        print(f"proofline: {exc}", file=sys.stderr)
        return 2
    if experiment is not None and args.format == "json":
        print(json.dumps(experiment.as_json_value(), indent=2, sort_keys=True))
    else:
        print(render_verification(report, args.format))
    if experiment is not None and args.format == "text":
        print()
        print(f"baseline artifact:  {experiment.baseline_runpack}")
        print(f"candidate artifact: {experiment.candidate_runpack}")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
