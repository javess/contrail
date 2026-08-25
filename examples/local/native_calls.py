"""Exercise zero-touch native extension and built-in call capture."""

from __future__ import annotations

import _sqlite3
import json
import tempfile
import threading
import time
import zlib


def run_database() -> int:
    database = _sqlite3.connect(":memory:")
    try:
        database.execute("CREATE TABLE private_native_records (value TEXT)")
        database.execute(
            "INSERT INTO private_native_records VALUES (?)",
            ("native-database-secret",),
        )
        database.execute("SELECT value FROM private_native_records").fetchone()
        try:
            database.execute("SELECT hidden_native_column FROM private_native_records")
        except _sqlite3.OperationalError:
            pass
    finally:
        database.close()
    return 4


def run_file_io() -> int:
    with tempfile.TemporaryFile() as stream:
        stream.write(b"native-file-secret")
        stream.seek(0)
        assert stream.read() == b"native-file-secret"
    return 3


def run_compression() -> int:
    compressed = zlib.compress(b"native-compression-secret" * 64)
    assert zlib.decompress(compressed) == b"native-compression-secret" * 64
    return 2


def run_lock_and_wait() -> int:
    lock = threading.Lock()
    assert lock.acquire(timeout=1.0)
    lock.release()
    time.sleep(0.04)
    return 3


def main() -> None:
    print(
        json.dumps(
            {
                "database_calls": run_database(),
                "file_calls": run_file_io(),
                "compression_calls": run_compression(),
                "synchronization_calls": run_lock_and_wait(),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
