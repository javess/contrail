"""Exercise zero-touch asyncio task lifecycle capture."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
from pathlib import Path


async def scheduled_task(index: int, private_payload: str) -> int:
    await asyncio.sleep(0.01 * (index % 3 + 1))
    if index == 2:
        raise RuntimeError("async-task-error-message-must-not-be-captured")
    assert private_payload
    return index * 2


async def task_group_task(index: int, private_payload: str) -> int:
    await asyncio.sleep(0.015 * (index + 1))
    assert private_payload
    return index * 3


async def run_create_tasks(extra_tasks: int) -> tuple[list[int], int]:
    tasks = [
        asyncio.create_task(
            scheduled_task(index, "async-task-payload-must-not-be-captured"),
            name="async-task-name-must-not-be-captured",
        )
        for index in range(3 + extra_tasks)
    ]
    results: list[int] = []
    failure_count = 0
    for result in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(result, RuntimeError):
            failure_count += 1
        else:
            assert isinstance(result, int)
            results.append(result)
    return results, failure_count


async def run_task_group() -> list[int]:
    async with asyncio.TaskGroup() as group:
        tasks = [
            group.create_task(
                task_group_task(index, "task-group-payload-must-not-be-captured"),
                name="task-group-name-must-not-be-captured",
            )
            for index in range(2)
        ]
    return [task.result() for task in tasks]


async def run_ensure_future() -> int:
    return await asyncio.ensure_future(
        scheduled_task(3, "ensure-future-payload-must-not-be-captured")
    )


async def run_implicit_gather() -> list[int]:
    return list(
        await asyncio.gather(
            task_group_task(2, "gather-payload-must-not-be-captured"),
            task_group_task(3, "gather-payload-must-not-be-captured"),
        )
    )


async def run(extra_tasks: int) -> dict[str, object]:
    task_results, task_failures = await run_create_tasks(extra_tasks)
    return {
        "create_task_parameters": list(inspect.signature(asyncio.create_task).parameters),
        "ensure_future_parameters": list(inspect.signature(asyncio.ensure_future).parameters),
        "ensure_future_result": await run_ensure_future(),
        "gather_parameters": list(inspect.signature(asyncio.gather).parameters),
        "gather_results": await run_implicit_gather(),
        "task_failures": task_failures,
        "task_group_create_task_parameters": list(
            inspect.signature(asyncio.TaskGroup.create_task).parameters
        ),
        "task_group_results": await run_task_group(),
        "task_results": task_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extra-tasks", type=int, default=0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.extra_tasks < 0:
        parser.error("--extra-tasks must be non-negative")

    rendered = json.dumps(asyncio.run(run(args.extra_tasks)), sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
