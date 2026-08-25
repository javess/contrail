"""Deterministic fixture generators for the release benchmark harness."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO

from runtime_tools.deep_profile import MAX_DEEP_PROFILE_FUNCTIONS
from runtime_tools.model import CausalEdge, Entity, Event, Execution
from runtime_tools.storage import RunpackWriter


def _write_json_array(stream: TextIO, values: Iterator[dict[str, object]]) -> None:
    write = stream.write
    first = True
    for value in values:
        if not first:
            write(",")
        first = False
        write(json.dumps(value, separators=(",", ":"), sort_keys=True))


def write_otlp_trace(path: Path, count: int) -> None:
    trace_id = "1" * 32
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write(
            '{"resourceSpans":[{"resource":{"attributes":['
            '{"key":"service.name","value":{"stringValue":"benchmark"}}]},'
            '"scopeSpans":[{"scope":{"name":"release-benchmark"},"spans":['
        )
        _write_json_array(
            stream,
            (
                {
                    "endTimeUnixNano": str(index + 2),
                    "name": "work",
                    "spanId": f"{index + 1:016x}",
                    "startTimeUnixNano": str(index + 1),
                    "traceId": trace_id,
                }
                for index in range(count)
            ),
        )
        stream.write("]}]}]}")


def write_prometheus_response(path: Path, count: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write(
            '{"status":"success","data":{"resultType":"matrix","result":['
            '{"metric":{"__name__":"benchmark_total","job":"release"},"values":['
        )
        first = True
        for index in range(count):
            if not first:
                stream.write(",")
            first = False
            stream.write(f'[{index + 1},"{index + 1}"]')
        stream.write("]}]}}")


def write_kubernetes_snapshot(path: Path, count: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write('{"apiVersion":"v1","kind":"List","items":[')
        _write_json_array(
            stream,
            (
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "creationTimestamp": "2024-01-01T00:00:00Z",
                        "name": f"pod-{index}",
                        "namespace": "benchmark",
                        "uid": f"benchmark-pod-{index}",
                    },
                    "spec": {"containers": []},
                    "status": {"phase": "Pending"},
                }
                for index in range(count)
            ),
        )
        stream.write("]}")


def write_temporal_history(path: Path, count: int) -> None:
    def events() -> Iterator[dict[str, object]]:
        yield {
            "eventId": "1",
            "eventTime": "2024-01-01T00:00:00Z",
            "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
            "workflowExecutionStartedEventAttributes": {
                "workflowId": "benchmark",
                "workflowType": {"name": "BenchmarkWorkflow"},
            },
        }
        for index in range(count):
            scheduled_id = index * 3 + 2
            started_id = scheduled_id + 1
            terminal_id = scheduled_id + 2
            activity_id = f"activity-{index}"
            yield {
                "activityTaskScheduledEventAttributes": {
                    "activityId": activity_id,
                    "activityType": {"name": "work"},
                },
                "eventId": str(scheduled_id),
                "eventTime": "2024-01-01T00:00:00Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            }
            yield {
                "activityTaskStartedEventAttributes": {
                    "attempt": 1,
                    "scheduledEventId": str(scheduled_id),
                },
                "eventId": str(started_id),
                "eventTime": "2024-01-01T00:00:00Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_STARTED",
            }
            yield {
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": str(scheduled_id),
                    "startedEventId": str(started_id),
                },
                "eventId": str(terminal_id),
                "eventTime": "2024-01-01T00:00:00Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
            }
        yield {
            "eventId": str(count * 3 + 2),
            "eventTime": "2024-01-01T00:00:00Z",
            "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED",
            "workflowExecutionCompletedEventAttributes": {},
        }

    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write('{"events":[')
        _write_json_array(stream, events())
        stream.write("]}")


def write_finished_runpack(path: Path, *, finished_at_ns: int) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "benchmark",
                "benchmark",
                0,
                finished_at_ns,
                (),
                str(path.parent),
                0,
                None,
                {},
            )
        )


def write_causal_chain(path: Path, count: int) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "benchmark-chain",
                "benchmark-chain",
                0,
                count,
                (),
                str(path.parent),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            Event(
                f"event-{index}",
                "operation",
                "work",
                "worker",
                index,
                index + 1,
                "benchmark",
                None,
                index,
                {},
            )
            for index in range(count)
        )
        writer.add_causal_edges(
            CausalEdge(
                f"event-{index}",
                f"event-{index + 1}",
                "parent",
                1.0,
                {},
            )
            for index in range(count - 1)
        )


def write_network_workload(path: Path, count: int) -> None:
    path.write_text(
        f"""
import socket
import threading

server = socket.socket()
server.bind(("127.0.0.1", 0))
server.listen()

def accept_connections():
    for _ in range({count}):
        peer, _address = server.accept()
        peer.close()

thread = threading.Thread(target=accept_connections)
thread.start()
try:
    for _ in range({count}):
        connection = socket.create_connection(("127.0.0.1", server.getsockname()[1]))
        connection.close()
finally:
    thread.join()
    server.close()
""".strip(),
        encoding="utf-8",
    )


def write_logical_operation_workload(path: Path, count: int) -> None:
    path.write_text(
        f"""
import sqlite3

database = sqlite3.connect(":memory:")
try:
    for index in range({count}):
        database.execute("SELECT ?", (index,)).fetchone()
finally:
    database.close()
""".strip(),
        encoding="utf-8",
    )


def write_optional_client_workload(path: Path, package_root: Path, count: int) -> None:
    redis_package = package_root / "redis"
    redis_package.mkdir(parents=True)
    (redis_package / "__init__.py").write_text(
        "from redis.client import Pipeline, Redis\n",
        encoding="utf-8",
    )
    (redis_package / "client.py").write_text(
        """
import time

class Redis:
    def execute_command(self, command, *arguments, **options):
        time.sleep(0.0005)
        return b"optional-client-benchmark-result"

class Pipeline:
    def execute(self, raise_on_error=True):
        return Redis().execute_command("GET", "optional-client-benchmark-nested-secret")
""".strip(),
        encoding="utf-8",
    )
    path.write_text(
        f"""
from redis.client import Redis

client = Redis()
for _ in range({count}):
    client.execute_command("GET", "optional-client-benchmark-secret")
""".strip(),
        encoding="utf-8",
    )


def write_native_call_workload(path: Path, count: int) -> None:
    path.write_text(
        f"""
import time
import zlib

payload = b"native-benchmark-secret" * 64
time.sleep(0.05)
for _ in range({count}):
    zlib.compress(payload)
""".strip(),
        encoding="utf-8",
    )


def write_python_exception_workload(path: Path, count: int) -> None:
    path.write_text(
        f"""
import asyncio
import time

def churn():
    for _ in range({count}):
        try:
            raise RuntimeError("python-exception-benchmark-secret")
        except RuntimeError:
            pass

async def normal_async_control_flow():
    for _ in range({min(count, 200)}):
        await asyncio.sleep(0)

time.sleep(0.05)
churn()
asyncio.run(normal_async_control_flow())
""".strip(),
        encoding="utf-8",
    )


def write_executor_task_workload(path: Path, count: int) -> None:
    path.write_text(
        f"""
import time
from concurrent.futures import ThreadPoolExecutor

def work(index, private_payload):
    assert private_payload
    return index * 2

time.sleep(0.05)
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [
        executor.submit(work, index, "executor-benchmark-secret")
        for index in range({count})
    ]
    assert sum(future.result() for future in futures) == {count * (count - 1)}
""".strip(),
        encoding="utf-8",
    )


def write_async_task_workload(path: Path, count: int) -> None:
    path.write_text(
        f"""
import asyncio
import time

async def work(index, private_payload):
    await asyncio.sleep(0)
    assert private_payload
    return index * 2

async def main():
    first_boundary = {count} // 4
    second_boundary = {count} // 2
    third_boundary = 3 * {count} // 4
    explicit = [
        asyncio.create_task(
            work(index, "async-task-benchmark-secret"),
            name="async-task-benchmark-name",
        )
        for index in range(first_boundary)
    ]
    results = list(await asyncio.gather(*explicit))
    results.extend(
        await asyncio.gather(
            *(work(index, "async-task-benchmark-secret") for index in range(
                first_boundary, second_boundary
            ))
        )
    )
    ensured = [
        asyncio.ensure_future(work(index, "async-task-benchmark-secret"))
        for index in range(second_boundary, third_boundary)
    ]
    results.extend(await asyncio.gather(*ensured))
    async with asyncio.TaskGroup() as group:
        grouped = [
            group.create_task(
                work(index, "async-task-benchmark-secret"),
                name="async-task-benchmark-name",
            )
            for index in range(third_boundary, {count})
        ]
    results.extend(task.result() for task in grouped)
    assert sum(results) == {count * (count - 1)}

time.sleep(0.05)
asyncio.run(main())
""".strip(),
        encoding="utf-8",
    )


def write_wsgi_server_workload(path: Path, count: int) -> None:
    path.write_text(
        f"""
import http.client
import threading
from wsgiref.simple_server import WSGIRequestHandler, make_server

class QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *arguments):
        pass

def application(environ, start_response):
    assert environ["PATH_INFO"] == "/wsgi-server-benchmark-private-path"
    start_response(
        "200 OK",
        [("X-Private-Response", "wsgi-server-benchmark-response-secret")],
    )
    return [b"wsgi-server-benchmark-response-body-secret"]

server = make_server("127.0.0.1", 0, application, handler_class=QuietHandler)

def serve():
    for _ in range({count}):
        server.handle_request()

thread = threading.Thread(target=serve)
thread.start()
connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
try:
    for _ in range({count}):
        connection.request(
            "GET",
            "/wsgi-server-benchmark-private-path",
            headers={{"X-Private-Request": "wsgi-server-benchmark-request-secret"}},
        )
        response = connection.getresponse()
        assert response.status == 200
        response.read()
finally:
    connection.close()
    thread.join()
    server.server_close()
""".strip(),
        encoding="utf-8",
    )


def write_asgi_server_workload(path: Path, count: int) -> None:
    package = path.parent / "uvicorn" / "protocols" / "http"
    package.mkdir(parents=True, exist_ok=True)
    for parent in (package.parent.parent, package.parent, package):
        (parent / "__init__.py").write_text("", encoding="utf-8")
    cycle_source = """
class RequestResponseCycle:
    def __init__(self):
        self.scope = {
            "type": "http",
            "method": "PRIVATE-BENCHMARK-METHOD",
            "path": "/asgi-server-benchmark-private-path",
            "query_string": b"asgi-server-benchmark-private-query",
            "headers": [(b"x-private", b"asgi-server-benchmark-request-secret")],
            "client": ("asgi-server-benchmark-private-client", 54321),
            "status": 200,
        }

    async def receive(self):
        return {
            "type": "http.request",
            "body": b"asgi-server-benchmark-request-body-secret",
            "more_body": False,
        }

    async def run_asgi(self, app):
        await app(self.scope, self.receive, self.send)

    async def send(self, message):
        if message["type"] == "http.response.body" and not message.get("more_body", False):
            self.response_complete = True
""".strip()
    for module_name in ("h11_impl.py", "httptools_impl.py"):
        (package / module_name).write_text(cycle_source, encoding="utf-8")
    path.write_text(
        f"""
import asyncio
import time
from uvicorn.protocols.http.h11_impl import RequestResponseCycle as H11Cycle
from uvicorn.protocols.http.httptools_impl import RequestResponseCycle as HttpToolsCycle

async def application(scope, receive, send):
    request = await receive()
    assert request["body"] == b"asgi-server-benchmark-request-body-secret"
    await send({{
        "type": "http.response.start",
        "status": scope["status"],
        "headers": [(b"x-private", b"asgi-server-benchmark-response-secret")],
    }})
    await send({{
        "type": "http.response.body",
        "body": b"asgi-server-benchmark-response-body-one-secret",
        "more_body": True,
    }})
    await asyncio.sleep(0)
    await send({{
        "type": "http.response.body",
        "body": b"asgi-server-benchmark-response-body-two-secret",
    }})

async def main():
    cycle_types = (H11Cycle, HttpToolsCycle)
    for index in range({count}):
        await cycle_types[index % 2]().run_asgi(application)

time.sleep(0.05)
asyncio.run(main())
""".strip(),
        encoding="utf-8",
    )


def write_deep_profile_reports(directory: Path, retained_function_count: int) -> None:
    overflow = (
        min(2_000, retained_function_count)
        if retained_function_count >= MAX_DEEP_PROFILE_FUNCTIONS
        else 0
    )
    total_function_count = retained_function_count + overflow
    for process_index, start in enumerate(range(0, total_function_count, 2_000)):
        process_function_count = min(2_000, total_function_count - start)
        selected_function_count = max(
            0,
            min(process_function_count, retained_function_count - start),
        )
        functions = []
        for local_index in range(process_function_count):
            global_index = start + local_index
            usefulness = 100_000 + global_index if global_index < retained_function_count else 1
            functions.append(
                {
                    "id": local_index,
                    "module": f"worker_{process_index}",
                    "qualname": f"function_{local_index}",
                    "filename": f"/work/worker_{process_index}.py",
                    "firstlineno": local_index + 1,
                    "scope": "application",
                    "call_count": 1,
                    "total_ns": usefulness,
                    "self_ns": usefulness,
                    "max_ns": usefulness,
                }
            )
        edges = (
            [
                {
                    "source_id": local_index,
                    "target_id": (local_index + offset) % selected_function_count,
                    "call_count": 1,
                    "total_ns": offset * 10 + local_index % 10,
                }
                for offset in (1, 2, 3)
                for local_index in range(selected_function_count)
            ]
            if selected_function_count > 1
            else []
        )
        document = {
            "format_version": 1,
            "pid": 1_000 + process_index,
            "truncated": False,
            "dropped_call_count": 0,
            "dropped_edge_count": 0,
            "callback_error_count": 0,
            "functions": functions,
            "edges": edges,
        }
        (directory / f"profile-{1_000 + process_index}.json").write_text(
            json.dumps(document, separators=(",", ":")),
            encoding="utf-8",
        )
