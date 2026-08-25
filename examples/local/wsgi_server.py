"""Exercise zero-touch inbound WSGI request capture."""

from __future__ import annotations

import argparse
import http.client
import json
import threading
import time
from collections.abc import Callable
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, format: str, *arguments: object) -> None:
        return


def application(
    environ: dict[str, object],
    start_response: Callable[[str, list[tuple[str, str]]], object],
) -> list[bytes]:
    path = environ["PATH_INFO"]
    if path == "/private-failure-path":
        time.sleep(0.06)
        start_response(
            "503 Service Unavailable",
            [("X-Private-Response", "private-response-header-value")],
        )
        return [b"private-failure-response-body"]
    start_response(
        "200 OK",
        [("X-Private-Response", "private-response-header-value")],
    )
    return [b"private-success-response-body"]


def run_requests(extra_requests: int) -> dict[str, object]:
    server = make_server("127.0.0.1", 0, application, handler_class=_QuietHandler)
    paths = ["/private-success-path", "/private-failure-path"] + [
        "/private-extra-path"
    ] * extra_requests

    def serve() -> None:
        for _ in paths:
            server.handle_request()

    serving = threading.Thread(target=serve)
    serving.start()
    connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
    statuses: list[int] = []
    try:
        for path in paths:
            connection.request(
                "GET",
                path,
                headers={"X-Private-Request": "private-request-header-value"},
            )
            response = connection.getresponse()
            statuses.append(response.status)
            response.read()
    finally:
        connection.close()
        serving.join()
        server.server_close()
    return {"request_count": len(statuses), "statuses": statuses}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extra-requests", type=int, default=0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.extra_requests < 0:
        parser.error("--extra-requests must be non-negative")

    rendered = json.dumps(run_requests(args.extra_requests), sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
