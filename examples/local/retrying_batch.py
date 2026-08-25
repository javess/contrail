"""Zero-code Deep example with deterministic transient retry churn."""

from __future__ import annotations


class TransientLoadError(RuntimeError):
    """A retryable local workload failure."""


def load_record(record_id: int, attempt: int) -> int:
    if attempt < 2:
        raise TransientLoadError("tenant-token-must-not-be-captured")
    return record_id * 2


def load_with_retry(record_id: int) -> tuple[int, int, int]:
    transient_failures = 0
    for attempt in range(3):
        try:
            return load_record(record_id, attempt), attempt + 1, transient_failures
        except TransientLoadError:
            transient_failures += 1
    raise AssertionError("retry loop exhausted")


def main() -> None:
    results: list[int] = []
    attempts = 0
    transient_failures = 0
    for record_id in range(16):
        result, record_attempts, record_failures = load_with_retry(record_id)
        results.append(result)
        attempts += record_attempts
        transient_failures += record_failures
    print(
        f"processed={len(results)} attempts={attempts} "
        f"transient_failures={transient_failures} checksum={sum(results)}"
    )


if __name__ == "__main__":
    main()
