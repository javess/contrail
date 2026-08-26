"""After: use CPython's optimized built-in Timsort implementation."""

from __future__ import annotations

from workload import main


def timsort(values: list[int]) -> list[int]:
    return sorted(values)


if __name__ == "__main__":
    main(timsort)
