"""Serve the packaged local UI without a framework or network dependency."""

from __future__ import annotations

import json
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


def _asset(name: str) -> bytes:
    return files("runtime_tools.ui").joinpath("static", name).read_bytes()


def _log_line(message_format: str, args: tuple[object, ...]) -> str:
    return f"runtime-ui: {terminal_text(message_format % args)}"


def create_server(
    baseline: Path,
    candidate: Path | None,
    *,
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
        build_timeline_payload(baseline, candidate),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assets = {
        route: (_asset(name), content_type) for route, (name, content_type) in _ASSETS.items()
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
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

        def log_message(self, message_format: str, *args: object) -> None:
            print(_log_line(message_format, args), file=sys.stderr)

    try:
        return ThreadingHTTPServer((host, port), Handler)
    except (OSError, OverflowError) as exc:
        raise TimelineError(f"could not bind local UI to {host}:{port}: {exc}") from exc


def serve_runpacks(
    baseline: Path,
    candidate: Path | None,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = True,
) -> None:
    server = create_server(baseline, candidate, host=host, port=port)
    raw_host, actual_port = server.server_address[:2]
    actual_host = raw_host.decode() if isinstance(raw_host, bytes) else raw_host
    url = f"http://{actual_host}:{actual_port}"
    print(f"runtime UI: {terminal_text(url)}", file=sys.stderr)
    try:
        if open_browser:
            webbrowser.open(url)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
