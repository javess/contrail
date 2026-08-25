"""Exercise zero-touch inbound Uvicorn ASGI HTTP capture."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Protocol

import uvicorn


class _Receive(Protocol):
    async def __call__(self) -> dict[str, object]: ...


class _Send(Protocol):
    async def __call__(self, message: dict[str, object]) -> None: ...


async def _consume_request(receive: _Receive) -> None:
    while True:
        message = await receive()
        if message.get("type") == "http.disconnect" or message.get("more_body") is not True:
            return


async def application(
    scope: dict[str, object],
    receive: _Receive,
    send: _Send,
) -> None:
    scope_type = scope.get("type")
    if scope_type == "lifespan":
        while True:
            message = await receive()
            if message.get("type") == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message.get("type") == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    if scope_type == "websocket":
        await receive()
        await send(
            {
                "type": "websocket.accept",
                "headers": [(b"x-private-websocket", b"private-websocket-response-header")],
            }
        )
        await send({"type": "websocket.close", "code": 1000})
        return
    if scope_type != "http":
        return

    await _consume_request(receive)
    path = scope.get("path")
    if path == "/private-failure-path":
        try:
            raise RuntimeError("private-asgi-exception-message")
        except RuntimeError:
            pass
        await send(
            {
                "type": "http.response.start",
                "status": 503,
                "headers": [
                    (b"x-private-response", b"private-failure-response-header"),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"private-failure-response-body",
            }
        )
        return

    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"x-private-response", b"private-success-response-header")],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": b"private-streaming-response-body-one",
            "more_body": True,
        }
    )
    await __import__("asyncio").sleep(0.06)
    await send(
        {
            "type": "http.response.body",
            "body": b"private-streaming-response-body-two",
        }
    )


def _http_request(port: int, path: str) -> int:
    request = (
        "PATCH "
        f"{path}?private-query=private-query-value HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{port}\r\n"
        "X-Private-Request: private-request-header-value\r\n"
        "Content-Length: 29\r\n"
        "Connection: close\r\n"
        "\r\n"
        "private-request-body-sentinel"
    ).encode("ascii")
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        connection.sendall(request)
        response = bytearray()
        while True:
            chunk = connection.recv(65_536)
            if not chunk:
                break
            response.extend(chunk)
    status_line = bytes(response).partition(b"\r\n")[0]
    return int(status_line.split()[1])


def _websocket_request(port: int) -> None:
    nonce = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        "GET /private-websocket-path?private-websocket-query=value HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {nonce}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "X-Private-WebSocket: private-websocket-request-header\r\n"
        "\r\n"
    ).encode("ascii")
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        connection.sendall(request)
        response = connection.recv(65_536)
    status_line = response.partition(b"\r\n")[0]
    if b" 101 " not in status_line:
        raise RuntimeError(f"unexpected WebSocket response: {status_line!r}")
    expected_accept = base64.b64encode(
        hashlib.sha1((nonce + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
    )
    if expected_accept not in response:
        raise RuntimeError("WebSocket response omitted its expected accept value")


def run_requests(http_protocol: str, extra_requests: int, websocket: bool) -> dict[str, object]:
    config = uvicorn.Config(
        application,
        host="127.0.0.1",
        port=0,
        http=http_protocol,
        interface="asgi3",
        lifespan="on",
        log_level="critical",
        access_log=False,
    )
    server = uvicorn.Server(config)
    serving = threading.Thread(target=server.run, name="local-asgi-server")
    serving.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if not serving.is_alive():
            raise RuntimeError("Uvicorn exited before startup")
        if time.monotonic() >= deadline:
            raise RuntimeError("Uvicorn did not start within 10 seconds")
        time.sleep(0.01)
    if not server.servers or not server.servers[0].sockets:
        raise RuntimeError("Uvicorn did not expose its bound socket")
    port = int(server.servers[0].sockets[0].getsockname()[1])
    paths = ["/private-success-path", "/private-failure-path"] + [
        "/private-extra-path"
    ] * extra_requests
    try:
        with ThreadPoolExecutor(max_workers=len(paths)) as executor:
            statuses = list(executor.map(lambda path: _http_request(port, path), paths))
        if websocket:
            _websocket_request(port)
    finally:
        server.should_exit = True
        serving.join(timeout=10)
        if serving.is_alive():
            raise RuntimeError("Uvicorn did not stop within 10 seconds")
    return {"request_count": len(statuses), "statuses": statuses, "websocket": websocket}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", choices=("h11", "httptools"), default="h11")
    parser.add_argument("--extra-requests", type=int, default=0)
    parser.add_argument("--websocket", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.extra_requests < 0:
        parser.error("--extra-requests must be non-negative")

    rendered = json.dumps(
        run_requests(args.http, args.extra_requests, args.websocket),
        sort_keys=True,
    )
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
