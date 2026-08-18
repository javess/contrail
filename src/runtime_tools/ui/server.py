"""Serve the packaged local UI without a framework or network dependency."""

from __future__ import annotations

import ipaddress
import json
import socket
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from pathlib import Path

from runtime_tools.terminal import terminal_text
from runtime_tools.ui.data import TimelineError, build_timeline_payload

_ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
}


class _IPv6ThreadingHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6


def _server_type(host: str) -> type[ThreadingHTTPServer]:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return ThreadingHTTPServer
    return (
        _IPv6ThreadingHTTPServer
        if isinstance(address, ipaddress.IPv6Address)
        else ThreadingHTTPServer
    )


def _asset(name: str) -> bytes:
    return files("runtime_tools.ui").joinpath("static", name).read_bytes()


def _log_line(message_format: str, args: tuple[object, ...]) -> str:
    return f"runtime-ui: {terminal_text(message_format % args)}"


def _loopback_address(value: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return None
    return address if address.is_loopback else None


def _host_header_name(value: str) -> str | None:
    if not value or value.strip() != value or any(ord(character) <= 32 for character in value):
        return None
    if value.startswith("["):
        closing_bracket = value.find("]")
        if closing_bracket <= 1:
            return None
        hostname = value[1:closing_bracket]
        remainder = value[closing_bracket + 1 :]
        try:
            address = ipaddress.ip_address(hostname)
        except ValueError:
            return None
        if not isinstance(address, ipaddress.IPv6Address):
            return None
    else:
        if value.count(":") > 1:
            return None
        hostname, separator, port = value.partition(":")
        remainder = f":{port}" if separator else ""
        if hostname.endswith("."):
            hostname = hostname[:-1]
    if not hostname:
        return None
    if remainder:
        if not remainder.startswith(":"):
            return None
        port = remainder[1:]
        if (
            not port
            or len(port) > 5
            or not port.isascii()
            or not port.isdecimal()
            or not 1 <= int(port) <= 65_535
        ):
            return None
    return hostname.casefold()


def _request_host_allowed(host_headers: tuple[str, ...], bound_host: str) -> bool:
    bound_address = _loopback_address(bound_host)
    if bound_address is None:
        return True
    if len(host_headers) != 1:
        return False
    hostname = _host_header_name(host_headers[0])
    if hostname == "localhost":
        return True
    if hostname is None:
        return False
    try:
        requested_address = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return requested_address == bound_address


def create_server(
    baseline: Path,
    candidate: Path | None,
    *,
    contract: Path | None = None,
    proofline_report: Path | None = None,
    host: str,
    port: int,
) -> ThreadingHTTPServer:
    if not isinstance(host, str) or not host:
        raise TimelineError("local UI host must be a non-empty string")
    if not isinstance(port, int) or isinstance(port, bool):
        raise TimelineError("local UI port must be an integer")
    if not 0 <= port <= 65_535:
        raise TimelineError("local UI port must be between 0 and 65535")
    payload = json.dumps(
        build_timeline_payload(
            baseline,
            candidate,
            contract=contract,
            proofline_report=proofline_report,
        ),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assets = {
        route: (_asset(name), content_type) for route, (name, content_type) in _ASSETS.items()
    }
    bound_host = host

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            host_headers = tuple(self.headers.get_all("Host", []))
            if not _request_host_allowed(host_headers, bound_host):
                self.send_error(421, "Misdirected Request")
                return
            path = self.path.split("?", 1)[0]
            if path == "/api/data":
                self._respond(payload, "application/json; charset=utf-8")
                return
            asset = assets.get(path)
            if asset is None:
                self.send_error(404)
                return
            self._respond(*asset)

        def _respond(self, body: bytes, content_type: str) -> None:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'",
            )
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            print(_log_line(format, args), file=sys.stderr)

    try:
        server = _server_type(host)((host, port), Handler)
    except (OSError, OverflowError) as exc:
        raise TimelineError(f"could not bind local UI to {host}:{port}: {exc}") from exc
    raw_bound_host = server.server_address[0]
    bound_host = (
        bytes(raw_bound_host).decode()
        if isinstance(raw_bound_host, (bytes, bytearray))
        else raw_bound_host
    )
    return server


def serve_runpacks(
    baseline: Path,
    candidate: Path | None,
    *,
    contract: Path | None = None,
    proofline_report: Path | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = True,
) -> None:
    server = create_server(
        baseline,
        candidate,
        contract=contract,
        proofline_report=proofline_report,
        host=host,
        port=port,
    )
    raw_host, actual_port = server.server_address[:2]
    actual_host = bytes(raw_host).decode() if isinstance(raw_host, (bytes, bytearray)) else raw_host
    url_host = f"[{actual_host}]" if ":" in actual_host else actual_host
    url = f"http://{url_host}:{actual_port}"
    print(f"runtime UI: {terminal_text(url)}", file=sys.stderr)
    try:
        if open_browser:
            webbrowser.open(url)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
