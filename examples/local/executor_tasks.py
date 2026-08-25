"""Exercise zero-touch thread and process executor task capture."""

from __future__ import annotations

import argparse
import inspect
import json
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path


def thread_task(index: int, private_payload: str) -> int:
    time.sleep(0.01 * (index % 3 + 1))
    if index == 2:
        raise RuntimeError("executor-thread-error-message-must-not-be-captured")
    assert private_payload
    return index * 2


def process_task(index: int, private_payload: str) -> int:
    time.sleep(0.015 * (index + 1))
    assert private_payload
    return index * 3


def run_thread_pool(extra_tasks: int) -> tuple[list[int], int]:
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                thread_task,
                index,
                "executor-thread-payload-must-not-be-captured",
            )
            for index in range(3 + extra_tasks)
        ]
        results: list[int] = []
        failure_count = 0
        for future in futures:
            try:
                results.append(future.result())
            except RuntimeError:
                failure_count += 1
    return results, failure_count


def run_process_pool() -> list[int]:
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                process_task,
                index,
                "executor-process-payload-must-not-be-captured",
            )
            for index in range(2)
        ]
        return [future.result() for future in futures]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extra-thread-tasks", type=int, default=0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.extra_thread_tasks < 0:
        parser.error("--extra-thread-tasks must be non-negative")

    thread_results, thread_failures = run_thread_pool(args.extra_thread_tasks)
    result = {
        "process_results": run_process_pool(),
        "submit_parameters": list(inspect.signature(ThreadPoolExecutor.submit).parameters),
        "thread_failures": thread_failures,
        "thread_results": thread_results,
    }
    rendered = json.dumps(result, sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
