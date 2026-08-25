"""Exercise pooled, reconnecting, retrying, and async connection lifecycles."""

from __future__ import annotations

import asyncio
import json
import socket
import threading


class _ProtocolServer:
    def __init__(self, connection_count: int) -> None:
        self._server = socket.socket()
        self._server.bind(("127.0.0.1", 0))
        self._server.listen()
        self._connection_count = connection_count
        self._handlers: list[threading.Thread] = []
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._accept_connections)
        self._thread.start()

    @property
    def port(self) -> int:
        address = self._server.getsockname()
        assert isinstance(address, tuple)
        port = address[1]
        assert isinstance(port, int) and not isinstance(port, bool)
        return port

    def _handle(self, connection: socket.socket) -> None:
        try:
            with connection:
                while connection.recv(1):
                    connection.sendall(b"1")
        except BaseException as error:
            self._error = error

    def _accept_connections(self) -> None:
        try:
            for _ in range(self._connection_count):
                connection, _address = self._server.accept()
                handler = threading.Thread(target=self._handle, args=(connection,))
                self._handlers.append(handler)
                handler.start()
        except BaseException as error:
            self._error = error

    def close(self) -> None:
        self._thread.join(timeout=5.0)
        for handler in self._handlers:
            handler.join(timeout=5.0)
        self._server.close()
        if self._thread.is_alive() or any(handler.is_alive() for handler in self._handlers):
            raise RuntimeError("protocol server did not finish")
        if self._error is not None:
            raise self._error


class PooledCacheClient:
    def __init__(self, port: int) -> None:
        self._connection = socket.create_connection(("127.0.0.1", port), timeout=2.0)

    def get(self) -> None:
        self._connection.sendall(b"1")
        assert self._connection.recv(1) == b"1"

    def close(self) -> None:
        self._connection.close()


class ReconnectingQueueClient:
    def __init__(self, port: int) -> None:
        self._port = port

    def publish(self) -> None:
        with socket.create_connection(("127.0.0.1", self._port), timeout=2.0) as connection:
            connection.sendall(b"1")
            assert connection.recv(1) == b"1"


def connect_with_retry(failure_port: int, success_port: int) -> None:
    for port in (failure_port, failure_port, success_port):
        try:
            connection = socket.create_connection(("127.0.0.1", port), timeout=0.05)
        except OSError:
            continue
        with connection:
            connection.sendall(b"1")
            assert connection.recv(1) == b"1"


async def publish_one(port: int) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"1")
    await writer.drain()
    assert await reader.readexactly(1) == b"1"
    writer.close()
    await writer.wait_closed()


async def publish_concurrently(port: int) -> None:
    await asyncio.gather(*(publish_one(port) for _ in range(8)))


def main() -> None:
    server = _ProtocolServer(22)
    refused = socket.socket()
    refused.bind(("127.0.0.1", 0))
    try:
        pooled = PooledCacheClient(server.port)
        try:
            for _ in range(3):
                pooled.get()
        finally:
            pooled.close()
        reconnecting = ReconnectingQueueClient(server.port)
        for _ in range(12):
            reconnecting.publish()
        connect_with_retry(refused.getsockname()[1], server.port)
        asyncio.run(publish_concurrently(server.port))
    finally:
        refused.close()
        server.close()
    print(
        json.dumps(
            {
                "async_operations": 8,
                "connection_attempts": 24,
                "pooled_operations": 3,
                "reconnecting_operations": 12,
                "retry_attempts": 3,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
