"""Proofline command-line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from runtime_tools.proofline import ContractError, verify_contracts
from runtime_tools.proofline.report import render_verification
from runtime_tools.storage import RunpackError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="proofline")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    verify = subparsers.add_parser("verify", help="evaluate contracts over two runpacks")
    verify.add_argument("contract", type=Path)
    verify.add_argument("--baseline", type=Path, required=True)
    verify.add_argument("--candidate", type=Path, required=True)
    verify.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)
    try:
        report = verify_contracts(args.contract, args.baseline, args.candidate)
    except (ContractError, RunpackError) as exc:
        print(f"proofline: {exc}", file=sys.stderr)
        return 2
    print(render_verification(report, args.format))
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
