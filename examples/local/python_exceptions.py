"""Zero-code Deep example with caught and propagated Python exceptions."""

from __future__ import annotations

import threading


def parse_record(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def process_partition(values: tuple[str, ...]) -> int:
    accepted = 0
    for value in values:
        if parse_record(value) is not None:
            accepted += 1
    return accepted


def main() -> None:
    values = ("10", "bad-a", "20", "bad-b", "30")
    worker = threading.Thread(target=process_partition, args=(values,))
    worker.start()
    accepted = process_partition(values)
    worker.join()
    print(f"accepted={accepted}")


if __name__ == "__main__":
    main()
