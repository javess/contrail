"""Self-contained zero-touch outbound connection capture example."""

from __future__ import annotations

import asyncio
import json
import socket
import threading


def _accept_connections(server: socket.socket, count: int) -> None:
    for _ in range(count):
        connection, _address = server.accept()
        with connection:
            connection.recv(1)


def write_cache_entry(port: int) -> None:
    with socket.create_connection(("127.0.0.1", port), timeout=2.0) as connection:
        connection.sendall(b"1")


async def publish_queue_message(port: int) -> None:
    _reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"1")
    await writer.drain()
    writer.close()
    await writer.wait_closed()


def main() -> None:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()
    serving = threading.Thread(target=_accept_connections, args=(server, 2))
    serving.start()
    try:
        port = server.getsockname()[1]
        write_cache_entry(port)
        asyncio.run(publish_queue_message(port))
    finally:
        serving.join()
        server.close()
    print(json.dumps({"connections": 2}, sort_keys=True))


if __name__ == "__main__":
    main()
