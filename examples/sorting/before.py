"""Before: sort a descending list with deliberately quadratic bubble sort."""

from __future__ import annotations

from workload import main


def bubble_sort(values: list[int]) -> list[int]:
    ordered = values.copy()
    for end in range(len(ordered) - 1, 0, -1):
        swapped = False
        for index in range(end):
            if ordered[index] > ordered[index + 1]:
                ordered[index], ordered[index + 1] = ordered[index + 1], ordered[index]
                swapped = True
        if not swapped:
            break
    return ordered


if __name__ == "__main__":
    main(bubble_sort)
