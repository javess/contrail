"""Shared deterministic input and output for the sorting example."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable

DEFAULT_SIZE = 4_000
MAX_SIZE = 20_000

type SortAlgorithm = Callable[[list[int]], list[int]]


def _values(size: int) -> list[int]:
    return list(range(size, 0, -1))


def main(sort_values: SortAlgorithm) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=DEFAULT_SIZE)
    args = parser.parse_args()
    if not 1 <= args.size <= MAX_SIZE:
        parser.error(f"--size must be between 1 and {MAX_SIZE:,}")

    ordered = sort_values(_values(args.size))
    expected = list(range(1, args.size + 1))
    if ordered != expected:
        raise RuntimeError("sorting algorithm returned an incorrect result")
    print(
        json.dumps(
            {
                "checksum": sum(ordered),
                "count": len(ordered),
                "first": ordered[0],
                "last": ordered[-1],
            },
            sort_keys=True,
        )
    )
