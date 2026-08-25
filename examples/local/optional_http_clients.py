"""Zero-touch HTTPX and aiohttp capture example with a local server."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import aiohttp
import httpx


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - method name is defined by BaseHTTPRequestHandler
        time.sleep(0.06)
        payload = b'{"accepted":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Example-Response", "not-retained")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        return


def fetch_httpx(port: int) -> int:
    with httpx.Client(trust_env=False) as client:
        response = client.get(
            f"http://127.0.0.1:{port}/httpx?token=not-retained",
            headers={"X-Example-Request": "not-retained"},
        )
        response.read()
        return response.status_code


async def fetch_async_clients(port: int) -> tuple[int, int]:
    async with httpx.AsyncClient(trust_env=False) as client:
        httpx_response = await client.get(f"http://127.0.0.1:{port}/httpx-async?token=not-retained")
        await httpx_response.aread()
    async with aiohttp.ClientSession(trust_env=False) as session:
        async with session.get(
            f"http://127.0.0.1:{port}/aiohttp?token=not-retained"
        ) as aiohttp_response:
            await aiohttp_response.read()
            aiohttp_status = aiohttp_response.status
    return httpx_response.status_code, aiohttp_status


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    try:
        sync_status = fetch_httpx(server.server_port)
        async_httpx_status, aiohttp_status = asyncio.run(fetch_async_clients(server.server_port))
    finally:
        server.shutdown()
        server.server_close()
        serving.join()
    print(
        json.dumps(
            {
                "aiohttp": aiohttp_status,
                "httpx_async": async_httpx_status,
                "httpx_sync": sync_status,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
