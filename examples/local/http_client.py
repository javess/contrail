"""Self-contained zero-touch outbound HTTP capture example."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - method name is defined by BaseHTTPRequestHandler
        content_length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(content_length)
        time.sleep(0.08)
        payload = b'{"accepted":true}'
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        return


def publish_batch(port: int) -> dict[str, object]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/batches?source=example",
        data=b'{"items":3}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=2.0) as response:
        return json.loads(response.read())


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    try:
        result = publish_batch(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        serving.join()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
