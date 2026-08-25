"""Uninstrumented parent/worker workload for zero-code capture examples."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


def shared_hot(delay_seconds: float) -> None:
    """Represent one logical operation executed by the parent and every worker."""
    time.sleep(delay_seconds)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--delay", type=float, default=0.32)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    return parser


def main() -> None:
    parser = _parser()
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.delay <= 0:
        parser.error("--delay must be positive")
    if args.worker:
        shared_hot(args.delay)
        return

    workload = Path(__file__).resolve()
    children = [
        subprocess.Popen(
            (
                sys.executable,
                str(workload),
                "--worker",
                "--delay",
                str(args.delay),
            )
        )
        for _ in range(args.workers)
    ]
    shared_hot(args.delay + 0.02)
    exit_codes = [child.wait() for child in children]
    if any(exit_codes):
        raise SystemExit(next(code for code in exit_codes if code))
    print(json.dumps({"workers": args.workers, "completed": args.workers + 1}, sort_keys=True))


if __name__ == "__main__":
    main()
