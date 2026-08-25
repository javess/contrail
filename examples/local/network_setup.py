"""Zero-touch DNS and TLS setup workload for Contrail capture examples."""

from __future__ import annotations

import asyncio
import json
import socket
import ssl
import threading
from pathlib import Path


class _TlsServer:
    def __init__(self, expected_connections: int) -> None:
        self._socket = socket.socket()
        self._socket.bind(("127.0.0.1", 0))
        self._socket.listen()
        self._expected_connections = expected_connections
        assets = Path(__file__).with_name("tls")
        self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._context.load_cert_chain(
            assets / "localhost-cert.pem",
            assets / "localhost-key.pem",
        )
        self._thread = threading.Thread(target=self._serve, name="example-tls-server")
        self._thread.start()

    @property
    def port(self) -> int:
        address = self._socket.getsockname()
        assert isinstance(address, tuple)
        port = address[1]
        assert isinstance(port, int)
        return port

    def _serve(self) -> None:
        for _ in range(self._expected_connections):
            peer, _address = self._socket.accept()
            with self._context.wrap_socket(peer, server_side=True) as secured:
                secured.recv(1)
                secured.sendall(b"x")

    def close(self) -> None:
        self._thread.join()
        self._socket.close()


def _client_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def connect_sync(port: int) -> None:
    addresses = socket.getaddrinfo(
        "localhost",
        port,
        family=socket.AF_INET,
        type=socket.SOCK_STREAM,
    )
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.connect(addresses[0][4])
    with _client_context().wrap_socket(raw, server_hostname="localhost") as secured:
        secured.sendall(b"s")
        assert secured.recv(1) == b"x"


async def connect_async(port: int) -> None:
    reader, writer = await asyncio.open_connection(
        "localhost",
        port,
        ssl=_client_context(),
        server_hostname="localhost",
        family=socket.AF_INET,
    )
    writer.write(b"a")
    await writer.drain()
    assert await reader.readexactly(1) == b"x"
    writer.close()
    try:
        await writer.wait_closed()
    except ConnectionResetError:
        pass


def main() -> None:
    server = _TlsServer(expected_connections=2)
    try:
        connect_sync(server.port)
        asyncio.run(connect_async(server.port))
    finally:
        server.close()
    print(
        json.dumps(
            {
                "dns_operations": 2,
                "tls_client_handshakes": 2,
                "tls_server_handshakes": 2,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
