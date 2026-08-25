"""Exercise zero-touch database and queue operation capture."""

from __future__ import annotations

import asyncio
import json
import queue
import sqlite3
import threading
import time


def run_database() -> int:
    database = sqlite3.connect(":memory:")
    assert type(database) is sqlite3.Connection
    try:
        database.execute("CREATE TABLE private_records (value TEXT)")
        database.executemany(
            "INSERT INTO private_records VALUES (?)",
            (("sensitive-row-one",), ("sensitive-row-two",)),
        )
        cursor = database.cursor()
        cursor.execute("SELECT value FROM private_records")
        assert len(cursor.fetchall()) == 2
        database.commit()
        try:
            database.execute("SELECT secret_missing_column FROM private_records")
        except sqlite3.OperationalError:
            pass
    finally:
        database.close()
    return 5


def run_blocking_queue() -> int:
    messages: queue.Queue[str] = queue.Queue()

    def publish() -> None:
        time.sleep(0.06)
        messages.put("sensitive-queue-item")

    publisher = threading.Thread(target=publish)
    publisher.start()
    assert messages.get(timeout=1.0) == "sensitive-queue-item"
    publisher.join()
    return 2


async def run_async_queue() -> int:
    messages: asyncio.Queue[str] = asyncio.Queue()

    async def receive() -> None:
        assert await messages.get() == "sensitive-async-item"

    receiver = asyncio.create_task(receive())
    await asyncio.sleep(0.03)
    await messages.put("sensitive-async-item")
    await receiver
    return 2


def main() -> None:
    database_operations = run_database()
    queue_operations = run_blocking_queue() + asyncio.run(run_async_queue())
    print(
        json.dumps(
            {
                "database_operations": database_operations,
                "queue_operations": queue_operations,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
