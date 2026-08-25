#!/usr/bin/env python3
"""Install a wheel into isolated tool/workload environments and exercise every CLI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import venv
from pathlib import Path
from typing import cast


def _run(command: tuple[str, ...], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, cwd=cwd, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        rendered = " ".join(command)
        raise RuntimeError(
            f"release smoke command failed ({completed.returncode}): {rendered}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def _json(command: tuple[str, ...], *, cwd: Path, document_type: str) -> dict[str, object]:
    value = json.loads(_run(command, cwd=cwd).stdout)
    if not isinstance(value, dict):
        raise RuntimeError(f"command did not emit a JSON object: {' '.join(command)}")
    if value.get("document_type") != document_type or value.get("format_version") != "1":
        raise RuntimeError(f"command emitted an unexpected JSON protocol: {' '.join(command)}")
    return cast(dict[str, object], value)


def _assert_artifact_bindings(
    document: dict[str, object],
    baseline: Path,
    candidate: Path,
) -> None:
    expected = {
        "baseline": {
            "size_bytes": baseline.stat().st_size,
            "sha256": hashlib.sha256(baseline.read_bytes()).hexdigest(),
        },
        "candidate": {
            "size_bytes": candidate.stat().st_size,
            "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
        },
    }
    if document.get("artifact_bindings") != expected:
        raise RuntimeError("installed Proofline report was not bound to its exact runpacks")


def _serve_payload(
    contrail: Path,
    baseline: Path,
    candidate: Path,
    report: Path,
    *,
    cwd: Path,
) -> dict[str, object]:
    process = subprocess.Popen(
        (
            str(contrail),
            "serve",
            str(baseline),
            "--compare",
            str(candidate),
            "--proofline-report",
            str(report),
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--no-open",
        ),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stderr, selectors.EVENT_READ)
    deadline = time.monotonic() + 10
    url: str | None = None
    try:
        while time.monotonic() < deadline and url is None:
            if process.poll() is not None:
                stderr = process.stderr.read()
                raise RuntimeError(
                    f"installed timeline server exited before startup ({process.returncode}): "
                    f"{stderr}"
                )
            for _ in selector.select(timeout=0.1):
                line = process.stderr.readline().strip()
                if line.startswith("runtime UI: http://"):
                    url = line.removeprefix("runtime UI: ")
                    break
        if url is None:
            raise RuntimeError("installed timeline server did not report its bound URL")
        with urllib.request.urlopen(f"{url}/api/data", timeout=5) as response:
            payload = json.loads(response.read())
        if not isinstance(payload, dict):
            raise RuntimeError("installed timeline server did not return a JSON object")
        return cast(dict[str, object], payload)
    finally:
        selector.close()
        if process.poll() is None:
            process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)


def _venv_executable(root: Path, name: str) -> Path:
    directory = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    return root / directory / f"{name}{suffix}"


def _install_wheel(
    python: Path,
    wheel: Path,
    *,
    constraints: Path | None,
    find_links: tuple[Path, ...],
    offline: bool,
    cwd: Path,
) -> None:
    command = [
        str(python),
        "-m",
        "pip",
        "--isolated",
        "install",
        "--disable-pip-version-check",
    ]
    if offline:
        command.append("--no-index")
    if constraints is not None:
        command.extend(("--constraint", str(constraints.resolve(strict=True))))
    for link in find_links:
        command.extend(("--find-links", str(link.resolve(strict=True))))
    command.append(str(wheel))
    _run(tuple(command), cwd=cwd)


def _write_optional_http_fakes(site_packages: Path) -> None:
    httpcore = site_packages / "httpcore"
    httpcore.mkdir()
    (httpcore / "__init__.py").write_text(
        """
import asyncio
import time

class URL:
    def __init__(self, scheme, port):
        self.scheme = scheme
        self.host = b"wheel-secret-host"
        self.port = port
        self.target = b"/wheel-secret-path?token=wheel-secret-query"

class Request:
    def __init__(self, method, scheme=b"https", port=9443):
        self.method = method
        self.url = URL(scheme, port)
        self.headers = [(b"Authorization", b"wheel-secret-header")]
        self.stream = [b"wheel-secret-body"]

class Response:
    def __init__(self, status):
        self.status = status
        self.headers = [(b"X-Secret", b"wheel-secret-response-header")]
        self.stream = [b"wheel-secret-response-body"]

class ConnectionPool:
    def handle_request(self, request):
        time.sleep(0.08)
        return Response(207)

class AsyncConnectionPool:
    async def handle_async_request(self, request):
        await asyncio.sleep(0.03)
        return Response(208)
""".strip(),
        encoding="utf-8",
    )
    aiohttp = site_packages / "aiohttp"
    aiohttp.mkdir()
    (aiohttp / "__init__.py").write_text(
        "from .client import ClientSession\n",
        encoding="utf-8",
    )
    (aiohttp / "client.py").write_text(
        """
import asyncio

class Response:
    def __init__(self, status):
        self.status = status
        self.body = b"wheel-secret-aiohttp-response-body"

class ClientSession:
    async def _request(self, method, url, **kwargs):
        await asyncio.sleep(0.03)
        return Response(209)

    async def request(self, method, url, **kwargs):
        return await self._request(method, url, **kwargs)
""".strip(),
        encoding="utf-8",
    )


def smoke(
    wheel: Path,
    *,
    constraints: Path | None,
    find_links: tuple[Path, ...],
    offline: bool,
) -> None:
    wheel = wheel.resolve(strict=True)
    if wheel.suffix != ".whl":
        raise ValueError(f"release smoke requires a wheel: {wheel}")
    with tempfile.TemporaryDirectory(prefix="contrail-wheel-smoke-") as directory:
        root = Path(directory)
        os.environ["_CONTRAIL_CAPTURE_JOB_ROOT"] = str(root / "capture-jobs")
        tool_environment = root / "tool-venv"
        workload_environment = root / "workload-venv"
        builder = venv.EnvBuilder(with_pip=True, clear=True)
        builder.create(tool_environment)
        builder.create(workload_environment)
        tool_python = _venv_executable(tool_environment, "python")
        workload_python = _venv_executable(workload_environment, "python")
        workload_site_packages = Path(
            _run(
                (
                    str(workload_python),
                    "-I",
                    "-c",
                    "import sysconfig; print(sysconfig.get_path('purelib'))",
                ),
                cwd=root,
            ).stdout.strip()
        )
        _install_wheel(
            tool_python,
            wheel,
            constraints=constraints,
            find_links=find_links,
            offline=offline,
            cwd=root,
        )

        contrail = _venv_executable(tool_environment, "contrail")
        runtime = _venv_executable(tool_environment, "runtime")
        rundiff = _venv_executable(tool_environment, "rundiff")
        batchscope = _venv_executable(tool_environment, "batchscope")
        proofline = _venv_executable(tool_environment, "proofline")
        for command in (contrail, runtime, rundiff, batchscope, proofline):
            version = _run((str(command), "--version"), cwd=root).stdout.strip()
            if not version.startswith(f"{command.stem} "):
                raise RuntimeError(f"unexpected version output from {command.name}: {version!r}")
        recovery_help = _run((str(contrail), "recover", "--help"), cwd=root).stdout
        if (
            "usage: contrail recover" not in recovery_help
            or "retained .runpack.tmp-* checkpoint" not in recovery_help
            or "--output OUTPUT" not in recovery_help
        ):
            raise RuntimeError("installed contrail command omitted capture recovery")

        runpack = root / "smoke.runpack"
        _run(
            (
                str(runtime),
                "record",
                "--name",
                "wheel-smoke",
                "--output",
                str(runpack),
                "--",
                str(workload_python),
                "-I",
                "-c",
                "print('wheel-smoke')",
            ),
            cwd=root,
        )
        inspected = _json(
            (str(runtime), "inspect", str(runpack), "--format", "json"),
            cwd=root,
            document_type="runtime.inspect",
        )
        if inspected.get("name") != "wheel-smoke":
            raise RuntimeError("installed runtime did not inspect the captured execution")
        jobs = _json(
            (str(contrail), "job", "list", "--format", "json"),
            cwd=root,
            document_type="runtime.capture_jobs",
        )
        retained_jobs = jobs.get("jobs")
        if (
            not isinstance(retained_jobs, list)
            or not retained_jobs
            or not isinstance(retained_jobs[0], dict)
            or retained_jobs[0].get("state") != "complete"
            or retained_jobs[0].get("artifacts") != [str(runpack)]
        ):
            raise RuntimeError("installed capture-job discovery omitted the recorded artifact")

        detached_runpack = root / "detached-smoke.runpack"
        detached_launch = _run(
            (
                str(contrail),
                "record",
                "--detach",
                "--name",
                "detached-wheel-smoke",
                "--output",
                str(detached_runpack),
                "--",
                str(workload_python),
                "-I",
                "-c",
                "print('detached-wheel-smoke')",
            ),
            cwd=root,
        )
        detached_job_id = next(
            (
                line.removeprefix("id:     ")
                for line in detached_launch.stdout.splitlines()
                if line.startswith("id:     ")
            ),
            None,
        )
        if detached_job_id is None:
            raise RuntimeError("installed detached capture omitted its job identity")
        detached_wait = _json(
            (
                str(contrail),
                "job",
                "wait",
                detached_job_id,
                "--format",
                "json",
            ),
            cwd=root,
            document_type="runtime.capture_job",
        )
        detached_job = detached_wait.get("job")
        detached_output_summary = (
            detached_job.get("output") if isinstance(detached_job, dict) else None
        )
        if (
            not isinstance(detached_job, dict)
            or detached_job.get("state") != "complete"
            or detached_job.get("detached") is not True
            or detached_job.get("artifacts") != [str(detached_runpack)]
            or not isinstance(detached_output_summary, dict)
            or detached_output_summary.get("retention_strategy") != "head-tail"
            or detached_output_summary.get("head_limit_bytes_per_stream") != 524_288
            or detached_output_summary.get("tail_limit_bytes_per_stream") != 524_288
            or detached_output_summary.get("stdout_omitted_bytes") != 0
            or detached_output_summary.get("stderr_omitted_bytes") != 0
        ):
            raise RuntimeError("installed detached capture did not publish its artifact")
        detached_output = _run(
            (str(contrail), "job", "output", detached_job_id, "--follow"),
            cwd=root,
        )
        if (
            detached_output.stdout != "detached-wheel-smoke\n"
            or f"recorded {detached_runpack}" not in detached_output.stderr
        ):
            raise RuntimeError("installed detached capture did not replay its bounded output")

        process_tree_runpack = root / "process-tree.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "process",
                "--name",
                "process-tree-wheel-smoke",
                "--output",
                str(process_tree_runpack),
                "--",
                str(workload_python),
                "-c",
                "import subprocess,sys,time; "
                "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(0.3)']); "
                "time.sleep(0.35); child.wait()",
            ),
            cwd=root,
        )
        process_measurements = _json(
            (
                str(contrail),
                "query",
                str(process_tree_runpack),
                "SELECT count(*) FROM measurements "
                "WHERE name IN ('process.memory.rss', 'process.cpu.total')",
                "--format",
                "json",
            ),
            cwd=root,
            document_type="runtime.query",
        )
        rows = process_measurements.get("rows")
        measurement_count = (
            rows[0][0]
            if isinstance(rows, list)
            and rows
            and isinstance(rows[0], list)
            and rows[0]
            and isinstance(rows[0][0], int)
            else 0
        )
        if measurement_count < 4:
            raise RuntimeError("installed process-tree observer did not retain resource samples")
        process_analysis = _run(
            (str(contrail), "analyze", str(process_tree_runpack)),
            cwd=root,
        )
        if "Process tree capture" not in process_analysis.stdout or "1 descendant" not in (
            process_analysis.stdout
        ):
            raise RuntimeError("installed BatchScope did not explain process-tree evidence")

        deep_runpack = root / "deep.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "deep",
                "--name",
                "deep-wheel-smoke",
                "--output",
                str(deep_runpack),
                "--",
                str(workload_python),
                "-c",
                (
                    "import subprocess\n"
                    "import sys\n"
                    "function_source = "
                    '"import time\\ndef useful(delay):\\n    time.sleep(delay)\\n"\n'
                    "exec(function_source)\n"
                    "child = subprocess.Popen([sys.executable, '-c', "
                    "function_source + 'useful(0.28)\\n'])\n"
                    "useful(0.30)\n"
                    "child.wait()\n"
                ),
            ),
            cwd=root,
        )
        deep_events = _json(
            (
                str(contrail),
                "query",
                str(deep_runpack),
                "SELECT kind, name FROM events "
                "WHERE kind = 'python.call.aggregate' AND name = '__main__.useful'",
                "--format",
                "json",
            ),
            cwd=root,
            document_type="runtime.query",
        )
        if deep_events.get("rows") != [["python.call.aggregate", "__main__.useful"]]:
            raise RuntimeError("installed Deep Capture did not profile an unmodified workload")
        deep_analysis = _run(
            (str(contrail), "analyze", str(deep_runpack)),
            cwd=root,
        )
        if "Python hotspots" not in deep_analysis.stdout or "intrusive" not in (
            deep_analysis.stdout
        ):
            raise RuntimeError("installed BatchScope did not explain Deep Capture evidence")
        if not all(
            marker in deep_analysis.stdout
            for marker in (
                "process coverage complete: 2 / 2 observed Python processes reported",
                "by process:",
                "root",
                "descendant",
            )
        ):
            raise RuntimeError("installed BatchScope did not attribute a shared hotspot by process")
        deep_document = _json(
            (
                str(contrail),
                "analyze",
                str(deep_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        deep_hotspots = deep_document.get("python_hotspots")
        useful_hotspots = (
            [
                hotspot
                for hotspot in deep_hotspots
                if isinstance(hotspot, dict) and hotspot.get("name") == "__main__.useful"
            ]
            if isinstance(deep_hotspots, list)
            else []
        )
        if len(useful_hotspots) != 1:
            raise RuntimeError("installed BatchScope omitted the shared Python hotspot")
        deep_profile = deep_document.get("deep_profile")
        deep_semantic = deep_document.get("semantic_capture")
        deep_subprocess_calls = deep_document.get("subprocess_calls")
        deep_caller_attribution = (
            deep_semantic.get("caller_attribution") if isinstance(deep_semantic, dict) else None
        )
        process_coverage = (
            deep_profile.get("process_coverage") if isinstance(deep_profile, dict) else None
        )
        if (
            not isinstance(process_coverage, dict)
            or process_coverage.get("status") != "complete"
            or process_coverage.get("profiled_process_count") != 2
            or process_coverage.get("observed_python_process_count") != 2
            or process_coverage.get("matched_process_count") != 2
        ):
            raise RuntimeError("installed BatchScope emitted incomplete process coverage")
        if (
            not isinstance(deep_caller_attribution, dict)
            or deep_caller_attribution.get("status") != "complete"
            or deep_caller_attribution.get("attributed_subprocess_count") != 1
            or not isinstance(deep_subprocess_calls, list)
            or len(deep_subprocess_calls) != 1
            or not isinstance(deep_subprocess_calls[0], dict)
            or not isinstance(deep_subprocess_calls[0].get("caller"), dict)
            or deep_subprocess_calls[0]["caller"].get("name") != "__main__.<module>"
            or deep_subprocess_calls[0]["caller"].get("observation") != "exact"
        ):
            raise RuntimeError("installed Deep Capture omitted exact subprocess caller causality")
        useful_hotspot = useful_hotspots[0]
        contributions = useful_hotspot.get("processes")
        if not isinstance(contributions, list) or not all(
            isinstance(contribution, dict) for contribution in contributions
        ):
            raise RuntimeError("installed BatchScope emitted malformed shared-hotspot attribution")
        contribution_objects = cast(list[dict[str, object]], contributions)
        if (
            useful_hotspot.get("process_attribution_status") != "complete"
            or len(contribution_objects) != 2
            or {contribution.get("role") for contribution in contribution_objects}
            != {
                "root",
                "descendant",
            }
            or not all(
                contribution.get("observed_in_process_tree") is True
                for contribution in contribution_objects
            )
        ):
            raise RuntimeError("installed BatchScope emitted incomplete shared-hotspot attribution")

        http_workload = root / "http-workload.py"
        http_workload.write_text(
            """
import http.server
import threading
import time
import urllib.request

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        time.sleep(0.08)
        self.send_response(202)
        self.send_header("X-Secret", "installed-response-secret")
        self.end_headers()
        self.wfile.write(b"installed-response-body-secret")

    def log_message(self, *_args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
thread = threading.Thread(target=server.serve_forever)
thread.start()

def fetch():
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.server_port}/installed-path-secret?token=query-secret",
        data=b"installed-request-body-secret",
        headers={"Authorization": "installed-header-secret"},
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        assert response.status == 202
        response.read()

try:
    fetch()
finally:
    server.shutdown()
    server.server_close()
    thread.join()
""".strip(),
            encoding="utf-8",
        )
        http_runpack = root / "http.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "deep",
                "--name",
                "http-wheel-smoke",
                "--output",
                str(http_runpack),
                "--",
                str(workload_python),
                str(http_workload),
            ),
            cwd=root,
        )
        http_document = _json(
            (
                str(contrail),
                "analyze",
                str(http_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        http_capture = http_document.get("http_capture")
        http_requests = http_document.get("http_requests")
        http_network_connections = http_document.get("network_connections")
        request = (
            http_requests[0]
            if isinstance(http_requests, list)
            and len(http_requests) == 1
            and isinstance(http_requests[0], dict)
            else None
        )
        caller = request.get("caller") if isinstance(request, dict) else None
        if (
            not isinstance(http_capture, dict)
            or http_capture.get("status") != "complete"
            or http_capture.get("request_count") != 1
            or http_capture.get("server_identity_policy") != "redact"
            or http_capture.get("server_address_captured") is not False
            or not isinstance(request, dict)
            or request.get("method") != "POST"
            or request.get("server_address") is not None
            or request.get("status_code") != 202
            or request.get("duration_boundary") != "response_headers"
            or not isinstance(caller, dict)
            or caller.get("name") != "__main__.fetch"
            or caller.get("observation") != "exact"
            or http_network_connections != []
        ):
            raise RuntimeError("installed Deep Capture omitted redacted HTTP caller causality")
        encoded_http_document = json.dumps(http_document)
        if any(
            secret in encoded_http_document or secret.encode() in http_runpack.read_bytes()
            for secret in (
                "installed-path-secret",
                "query-secret",
                "installed-request-body-secret",
                "installed-header-secret",
                "installed-response-secret",
                "installed-response-body-secret",
            )
        ):
            raise RuntimeError("installed HTTP capture retained private request or response data")

        network_workload = root / "network-workload.py"
        network_workload.write_text(
            """
import asyncio
import socket
import threading

server = socket.socket()
server.bind(("127.0.0.1", 0))
server.listen()

def accept_connections():
    for _ in range(12):
        peer, _address = server.accept()
        peer.close()

thread = threading.Thread(target=accept_connections)
thread.start()

def connect_sync():
    for _ in range(6):
        connection = socket.create_connection(("127.0.0.1", server.getsockname()[1]))
        connection.close()

async def connect_async():
    for _ in range(6):
        _reader, writer = await asyncio.open_connection(
            "localhost", server.getsockname()[1], family=socket.AF_INET
        )
        writer.close()
        await writer.wait_closed()

connect_sync()
asyncio.run(connect_async())
thread.join()
server.close()
""".strip(),
            encoding="utf-8",
        )
        network_runpack = root / "network.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "deep",
                "--name",
                "network-wheel-smoke",
                "--output",
                str(network_runpack),
                "--",
                str(workload_python),
                str(network_workload),
            ),
            cwd=root,
        )
        network_document = _json(
            (
                str(contrail),
                "analyze",
                str(network_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        network_capture = network_document.get("network_capture")
        network_connections = network_document.get("network_connections")
        network_hotspots = network_document.get("network_connection_hotspots")
        network_setup_capture = network_document.get("network_setup_capture")
        network_setup_phases = network_document.get("network_setup_phases")
        network_setup_hotspots = network_document.get("network_setup_hotspots")
        network_bottlenecks = network_document.get("bottlenecks")
        if (
            not isinstance(network_capture, dict)
            or network_capture.get("status") != "complete"
            or network_capture.get("connection_count") != 12
            or network_capture.get("connection_hotspot_count") != 2
            or network_capture.get("server_identity_policy") != "redact"
            or network_capture.get("server_address_captured") is not False
            or network_capture.get("path_captured") is not False
            or not isinstance(network_connections, list)
            or len(network_connections) != 12
            or not all(isinstance(connection, dict) for connection in network_connections)
            or not isinstance(network_hotspots, list)
            or len(network_hotspots) != 2
            or not all(isinstance(hotspot, dict) for hotspot in network_hotspots)
            or not isinstance(network_setup_capture, dict)
            or network_setup_capture.get("status") != "complete"
            or network_setup_capture.get("phase_count") != 12
            or network_setup_capture.get("hostname_captured") is not False
            or network_setup_capture.get("server_address_captured") is not False
            or not isinstance(network_setup_phases, list)
            or len(network_setup_phases) != 12
            or not all(isinstance(phase, dict) for phase in network_setup_phases)
            or not isinstance(network_setup_hotspots, list)
            or not network_setup_hotspots
            or not all(isinstance(hotspot, dict) for hotspot in network_setup_hotspots)
            or not isinstance(network_bottlenecks, list)
            or not any(
                isinstance(bottleneck, dict)
                and bottleneck.get("classification") == "connection_churn"
                for bottleneck in network_bottlenecks
            )
        ):
            raise RuntimeError("installed Deep Capture omitted redacted network connections")
        network_connection_objects = cast(list[dict[str, object]], network_connections)
        network_identities: set[tuple[object, object]] = set()
        for connection in network_connection_objects:
            network_caller = connection.get("caller")
            caller_name = network_caller.get("name") if isinstance(network_caller, dict) else None
            network_identities.add((connection.get("adapter"), caller_name))
            if (
                connection.get("server_address") is not None
                or connection.get("outcome") != "connected"
                or connection.get("transport") != "tcp"
            ):
                raise RuntimeError("installed network capture emitted invalid connection evidence")
        if network_identities != {
            ("stdlib.socket.connect", "__main__.connect_sync"),
            ("asyncio.create_connection", "__main__.connect_async"),
        }:
            raise RuntimeError("installed network capture emitted invalid connection evidence")
        network_hotspot_objects = cast(list[dict[str, object]], network_hotspots)
        hotspot_identities: set[tuple[object, object, object, object, object]] = set()
        for hotspot in network_hotspot_objects:
            hotspot_caller = hotspot.get("caller")
            caller_name = hotspot_caller.get("name") if isinstance(hotspot_caller, dict) else None
            hotspot_identities.add(
                (
                    hotspot.get("adapter"),
                    caller_name,
                    hotspot.get("connection_count"),
                    hotspot.get("connected_connection_count"),
                    hotspot.get("failed_connection_count"),
                )
            )
        if hotspot_identities != {
            ("stdlib.socket.connect", "__main__.connect_sync", 6, 6, 0),
            ("asyncio.create_connection", "__main__.connect_async", 6, 6, 0),
        }:
            raise RuntimeError("installed BatchScope omitted network connection hotspots")
        network_setup_phase_objects = cast(list[dict[str, object]], network_setup_phases)
        if any(
            phase.get("phase") != "dns"
            or phase.get("outcome") != "completed"
            or phase.get("hostname_captured") is not False
            or phase.get("server_address_captured") is not False
            for phase in network_setup_phase_objects
        ):
            raise RuntimeError("installed network setup capture emitted invalid DNS evidence")
        network_setup_hotspot_objects = cast(
            list[dict[str, object]],
            network_setup_hotspots,
        )
        network_setup_hotspot_total = 0
        for hotspot in network_setup_hotspot_objects:
            phase_count = hotspot.get("phase_count")
            if not isinstance(phase_count, int) or isinstance(phase_count, bool):
                raise RuntimeError("installed BatchScope emitted an invalid setup hotspot")
            network_setup_hotspot_total += phase_count
        if network_setup_hotspot_total != 12:
            raise RuntimeError("installed BatchScope omitted network setup hotspots")

        logical_workload = root / "logical-operation-workload.py"
        logical_workload.write_text(
            """
import asyncio
import http.client
import queue
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from wsgiref.simple_server import WSGIRequestHandler, make_server

database = sqlite3.connect(":memory:")
database.execute("CREATE TABLE wheel_private (value TEXT)")
database.executemany(
    "INSERT INTO wheel_private VALUES (?)",
    (("wheel-sql-secret",),),
)
cursor = database.cursor()
cursor.execute("SELECT value FROM wheel_private")
assert cursor.fetchone() == ("wheel-sql-secret",)
database.commit()
try:
    database.execute("SELECT wheel_missing_secret FROM wheel_private")
except sqlite3.OperationalError:
    pass
database.close()

blocking = queue.Queue()
blocking.put("wheel-queue-secret")
assert blocking.get() == "wheel-queue-secret"

async def scheduled(value, private_payload):
    await asyncio.sleep(0)
    if value == 2:
        raise RuntimeError("wheel-async-task-error-message")
    assert private_payload
    return value * 3

async def exchange():
    messages = asyncio.Queue()
    await messages.put("wheel-async-secret")
    assert await messages.get() == "wheel-async-secret"
    tasks = [
        asyncio.create_task(
            scheduled(value, "wheel-async-task-argument"),
            name="wheel-async-task-name",
        )
        for value in range(3)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert results[:2] == [0, 3]
    assert isinstance(results[2], RuntimeError)
    async with asyncio.TaskGroup() as group:
        grouped = [
            group.create_task(
                scheduled(value, "wheel-task-group-argument"),
                name="wheel-task-group-name",
            )
            for value in range(2)
        ]
    assert [task.result() for task in grouped] == [0, 3]
    ensured = asyncio.ensure_future(
        scheduled(3, "wheel-ensure-future-argument")
    )
    assert await ensured == 9
    gathered = await asyncio.gather(
        scheduled(4, "wheel-gather-argument"),
        scheduled(5, "wheel-gather-argument"),
    )
    assert gathered == [12, 15]

asyncio.run(exchange())

def task(value, private_payload):
    if value == 2:
        raise RuntimeError("wheel-executor-error-message")
    assert private_payload
    return value * 2

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [
        executor.submit(task, value, "wheel-executor-argument")
        for value in range(3)
    ]
    assert futures[0].result() == 0
    assert futures[1].result() == 2
    try:
        futures[2].result()
    except RuntimeError:
        pass

class QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *arguments):
        pass

def application(environ, start_response):
    if environ["PATH_INFO"] == "/wheel-private-server-failure":
        start_response(
            "503 Service Unavailable",
            [("X-Wheel-Private", "wheel-private-response-header")],
        )
        return [b"wheel-private-failure-response"]
    start_response(
        "200 OK",
        [("X-Wheel-Private", "wheel-private-response-header")],
    )
    return [b"wheel-private-success-response"]

server = make_server("127.0.0.1", 0, application, handler_class=QuietHandler)

def serve():
    for _ in range(2):
        server.handle_request()

thread = threading.Thread(target=serve)
thread.start()
connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
try:
    for path, expected in (
        ("/wheel-private-server-success", 200),
        ("/wheel-private-server-failure", 503),
    ):
        connection.request(
            "GET",
            path,
            headers={"X-Wheel-Private": "wheel-private-request-header"},
        )
        response = connection.getresponse()
        assert response.status == expected
        response.read()
finally:
    connection.close()
    thread.join()
    server.server_close()
""".strip(),
            encoding="utf-8",
        )
        logical_runpack = root / "logical-operation.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "deep",
                "--name",
                "logical-operation-wheel-smoke",
                "--output",
                str(logical_runpack),
                "--",
                str(workload_python),
                str(logical_workload),
            ),
            cwd=root,
        )
        logical_document = _json(
            (
                str(contrail),
                "analyze",
                str(logical_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        logical_capture = logical_document.get("logical_operation_capture")
        logical_operations = logical_document.get("logical_operations")
        logical_hotspots = logical_document.get("logical_operation_hotspots")
        if (
            not isinstance(logical_capture, dict)
            or logical_capture.get("status") != "complete"
            or logical_capture.get("operation_count") != 22
            or logical_capture.get("dropped_operation_count") != 0
            or logical_capture.get("statement_captured") is not False
            or logical_capture.get("queue_item_captured") is not False
            or logical_capture.get("callable_captured") is not False
            or logical_capture.get("awaitable_captured") is not False
            or logical_capture.get("task_name_captured") is not False
            or logical_capture.get("context_captured") is not False
            or logical_capture.get("arguments_captured") is not False
            or logical_capture.get("return_value_captured") is not False
            or logical_capture.get("exception_messages_captured") is not False
            or logical_capture.get("http_method_captured") is not False
            or logical_capture.get("route_captured") is not False
            or logical_capture.get("url_captured") is not False
            or logical_capture.get("headers_captured") is not False
            or logical_capture.get("body_captured") is not False
            or logical_capture.get("response_body_captured") is not False
            or logical_capture.get("client_address_captured") is not False
            or logical_capture.get("adapters")
            != [
                "stdlib.asyncio.Queue",
                "stdlib.asyncio.TaskGroup",
                "stdlib.asyncio.create_task",
                "stdlib.asyncio.ensure_future",
                "stdlib.asyncio.gather",
                "stdlib.concurrent.futures.ThreadPoolExecutor",
                "stdlib.queue.Queue",
                "stdlib.sqlite3.Connection",
                "stdlib.sqlite3.Cursor",
                "stdlib.wsgiref",
            ]
            or not isinstance(logical_operations, list)
            or len(logical_operations) != 22
            or not all(isinstance(operation, dict) for operation in logical_operations)
            or not isinstance(logical_hotspots, list)
            or not logical_hotspots
            or not all(isinstance(hotspot, dict) for hotspot in logical_hotspots)
        ):
            raise RuntimeError("installed Deep Capture omitted logical operation evidence")
        logical_operation_objects = cast(list[dict[str, object]], logical_operations)
        if (
            sum(operation.get("category") == "database" for operation in logical_operation_objects)
            != 5
            or sum(operation.get("category") == "queue" for operation in logical_operation_objects)
            != 4
            or sum(
                operation.get("category") == "executor"
                and operation.get("operation") == "task"
                and operation.get("duration_boundary") == "submission_to_completion"
                and operation.get("callable_captured") is False
                and operation.get("arguments_captured") is False
                for operation in logical_operation_objects
            )
            != 3
            or sum(
                operation.get("category") == "scheduler"
                and operation.get("operation") == "task"
                and operation.get("duration_boundary") == "creation_to_completion"
                and operation.get("awaitable_captured") is False
                and operation.get("task_name_captured") is False
                and operation.get("context_captured") is False
                for operation in logical_operation_objects
            )
            != 8
            or sum(
                operation.get("category") == "server"
                and operation.get("operation") == "request"
                and operation.get("duration_boundary") == "request_to_response_completion"
                and operation.get("http_method_captured") is False
                and operation.get("route_captured") is False
                and operation.get("url_captured") is False
                and operation.get("headers_captured") is False
                and operation.get("body_captured") is False
                and operation.get("response_body_captured") is False
                and operation.get("client_address_captured") is False
                and isinstance(operation.get("caller"), dict)
                and cast(dict[str, object], operation["caller"]).get("name")
                == "__main__.application"
                for operation in logical_operation_objects
            )
            != 2
            or {
                operation.get("status_code")
                for operation in logical_operation_objects
                if operation.get("category") == "server"
            }
            != {200, 503}
            or sum(
                operation.get("outcome") == "operation_error"
                for operation in logical_operation_objects
            )
            != 4
        ):
            raise RuntimeError("installed Deep Capture emitted invalid logical operations")
        encoded_logical = json.dumps(logical_document)
        if any(
            secret in encoded_logical or secret.encode() in logical_runpack.read_bytes()
            for secret in (
                "wheel_private",
                "wheel-sql-secret",
                "wheel_missing_secret",
                "wheel-queue-secret",
                "wheel-async-secret",
                "wheel-executor-argument",
                "wheel-executor-error-message",
                "wheel-async-task-argument",
                "wheel-async-task-name",
                "wheel-async-task-error-message",
                "wheel-task-group-argument",
                "wheel-task-group-name",
                "wheel-ensure-future-argument",
                "wheel-gather-argument",
                "wheel-private-server-success",
                "wheel-private-server-failure",
                "wheel-private-response-header",
                "wheel-private-success-response",
                "wheel-private-failure-response",
                "wheel-private-request-header",
            )
        ):
            raise RuntimeError("installed logical operation capture retained private data")

        redis_package = root / "redis"
        redis_package.mkdir()
        (redis_package / "__init__.py").write_text(
            "from redis.client import Pipeline, Redis\n",
            encoding="utf-8",
        )
        (redis_package / "client.py").write_text(
            """
import time

class Redis:
    def execute_command(self, command, *arguments, **options):
        time.sleep(0.002)
        if command == "WHEEL-REDIS-SECRET-FAIL":
            raise TimeoutError("wheel-redis-secret-error-message")
        return b"wheel-redis-secret-result"

class Pipeline:
    def execute(self, raise_on_error=True):
        return Redis().execute_command("GET", "wheel-redis-nested-secret-key")
""".strip(),
            encoding="utf-8",
        )
        optional_logical_workload = root / "optional-logical-workload.py"
        optional_logical_workload.write_text(
            """
from redis.client import Pipeline, Redis

client = Redis()
assert client.execute_command("GET", "wheel-redis-secret-key") == b"wheel-redis-secret-result"
try:
    client.execute_command("WHEEL-REDIS-SECRET-FAIL", "wheel-redis-secret-key")
except TimeoutError:
    pass
assert Pipeline().execute() == b"wheel-redis-secret-result"
""".strip(),
            encoding="utf-8",
        )
        optional_logical_runpack = root / "optional-logical.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "deep",
                "--name",
                "optional-logical-wheel-smoke",
                "--output",
                str(optional_logical_runpack),
                "--",
                str(workload_python),
                str(optional_logical_workload),
            ),
            cwd=root,
        )
        optional_logical_document = _json(
            (
                str(contrail),
                "analyze",
                str(optional_logical_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        optional_logical_capture = optional_logical_document.get("logical_operation_capture")
        optional_logical_operations = optional_logical_document.get("logical_operations")
        if (
            not isinstance(optional_logical_capture, dict)
            or optional_logical_capture.get("status") != "complete"
            or optional_logical_capture.get("operation_count") != 3
            or optional_logical_capture.get("adapters") != ["redis.Pipeline", "redis.Redis"]
            or not isinstance(optional_logical_operations, list)
            or len(optional_logical_operations) != 3
            or not all(isinstance(operation, dict) for operation in optional_logical_operations)
        ):
            raise RuntimeError("installed Deep Capture omitted optional logical adapters")
        optional_logical_operation_objects = cast(
            list[dict[str, object]],
            optional_logical_operations,
        )
        if (
            any(
                operation.get("category") != "cache"
                for operation in optional_logical_operation_objects
            )
            or sum(
                operation.get("outcome") == "operation_error"
                for operation in optional_logical_operation_objects
            )
            != 1
        ):
            raise RuntimeError("installed optional logical capture emitted invalid boundaries")
        encoded_optional_logical = json.dumps(optional_logical_document)
        if any(
            secret in encoded_optional_logical
            or secret.encode() in optional_logical_runpack.read_bytes()
            for secret in (
                "wheel-redis-secret-key",
                "wheel-redis-nested-secret-key",
                "wheel-redis-secret-result",
                "wheel-redis-secret-error-message",
            )
        ):
            raise RuntimeError("installed optional logical capture retained private data")

        native_workload = root / "native-call-workload.py"
        native_workload.write_text(
            """
import _sqlite3
import sys
import tempfile
import time
import zlib

def python_churn():
    for _ in range(4):
        try:
            raise RuntimeError("wheel-python-exception-secret")
        except RuntimeError:
            pass

database = _sqlite3.connect(":memory:")
try:
    database.execute("CREATE TABLE wheel_native_private (value TEXT)")
    database.execute("INSERT INTO wheel_native_private VALUES ('wheel-native-sql-secret')")
    try:
        database.execute("SELECT wheel_native_missing FROM wheel_native_private")
    except _sqlite3.OperationalError:
        pass
finally:
    database.close()

with tempfile.TemporaryFile() as stream:
    stream.write(b"wheel-native-file-secret")
    stream.seek(0)
    assert stream.read() == b"wheel-native-file-secret"

compressed = zlib.compress(b"wheel-native-compression-secret" * 32)
assert zlib.decompress(compressed) == b"wheel-native-compression-secret" * 32
python_churn()
sys.settrace(None)
time.sleep(0.01)
""".strip(),
            encoding="utf-8",
        )
        native_runpack = root / "native-call.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "deep",
                "--name",
                "native-call-wheel-smoke",
                "--output",
                str(native_runpack),
                "--",
                str(workload_python),
                str(native_workload),
            ),
            cwd=root,
        )
        native_document = _json(
            (
                str(contrail),
                "analyze",
                str(native_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        native_profile = native_document.get("deep_profile")
        native_hotspots = native_document.get("python_hotspots")
        native_capture = (
            native_profile.get("native_call_capture") if isinstance(native_profile, dict) else None
        )
        python_exception_capture = (
            native_profile.get("python_exception_capture")
            if isinstance(native_profile, dict)
            else None
        )
        control_flow_filter = (
            python_exception_capture.get("control_flow_filter")
            if isinstance(python_exception_capture, dict)
            else None
        )
        observer_integrity = (
            native_profile.get("observer_integrity") if isinstance(native_profile, dict) else None
        )
        if (
            not isinstance(native_capture, dict)
            or native_capture.get("status") != "complete"
            or not isinstance(native_capture.get("call_count"), int)
            or cast(int, native_capture["call_count"]) < 8
            or not isinstance(native_capture.get("exception_count"), int)
            or cast(int, native_capture["exception_count"]) < 1
            or native_capture.get("arguments_captured") is not False
            or native_capture.get("return_values_captured") is not False
            or native_capture.get("exception_messages_captured") is not False
            or not isinstance(native_hotspots, list)
            or not all(isinstance(hotspot, dict) for hotspot in native_hotspots)
            or not isinstance(python_exception_capture, dict)
            or python_exception_capture.get("status") != "partial"
            or not isinstance(python_exception_capture.get("event_count"), int)
            or cast(int, python_exception_capture["event_count"]) < 4
            or python_exception_capture.get("dropped_event_count") != 0
            or python_exception_capture.get("exception_values_captured") is not False
            or python_exception_capture.get("tracebacks_captured") is not False
            or python_exception_capture.get("line_events_enabled") is not False
            or python_exception_capture.get("opcode_events_enabled") is not False
            or not isinstance(control_flow_filter, dict)
            or control_flow_filter.get("status") != "partial"
            or control_flow_filter.get("filtered_exception_types")
            != ["GeneratorExit", "StopAsyncIteration", "StopIteration"]
            or control_flow_filter.get("exception_type_identity_inspected") is not True
            or control_flow_filter.get("exception_types_captured") is not False
            or not isinstance(control_flow_filter.get("non_control_flow_event_count"), int)
            or cast(int, control_flow_filter["non_control_flow_event_count"]) < 4
            or not isinstance(observer_integrity, dict)
            or observer_integrity.get("status") != "partial"
            or observer_integrity.get("profile_hook_setter_call_count") != 0
            or observer_integrity.get("trace_hook_setter_call_count") != 1
            or observer_integrity.get("trace_hook_setter_process_count") != 1
            or observer_integrity.get("hook_values_captured") is not False
        ):
            raise RuntimeError("installed Deep Capture omitted native/exception evidence")
        native_hotspot_objects = cast(list[dict[str, object]], native_hotspots)
        sqlite_execute = next(
            (
                hotspot
                for hotspot in native_hotspot_objects
                if hotspot.get("name") == "sqlite3.Connection.execute"
            ),
            None,
        )
        python_churn = next(
            (
                hotspot
                for hotspot in native_hotspot_objects
                if hotspot.get("name") == "__main__.python_churn"
            ),
            None,
        )
        if (
            sqlite_execute is None
            or sqlite_execute.get("implementation") != "native"
            or sqlite_execute.get("call_count") != 3
            or sqlite_execute.get("exception_count") != 1
            or python_churn is None
            or python_churn.get("implementation") != "python"
            or python_churn.get("call_count") != 1
            or python_churn.get("exception_count") != 4
            or python_churn.get("non_control_flow_exception_count") != 4
        ):
            raise RuntimeError("installed Deep Capture emitted invalid call/exception evidence")
        encoded_native = json.dumps(native_document)
        if any(
            secret in encoded_native or secret.encode() in native_runpack.read_bytes()
            for secret in (
                "wheel_native_private",
                "wheel-native-sql-secret",
                "wheel_native_missing",
                "wheel-native-file-secret",
                "wheel-native-compression-secret",
                "wheel-python-exception-secret",
            )
        ):
            raise RuntimeError("installed call/exception capture retained private data")

        _write_optional_http_fakes(workload_site_packages)
        optional_http_workload = root / "optional-http-workload.py"
        optional_http_workload.write_text(
            """
import asyncio
import aiohttp
import httpcore

def fetch_sync():
    return httpcore.ConnectionPool().handle_request(httpcore.Request(b"POST"))

async def fetch_async():
    core = await httpcore.AsyncConnectionPool().handle_async_request(
        httpcore.Request(b"PUT", scheme=b"http", port=9080)
    )
    aio = await aiohttp.ClientSession().request(
        "PATCH",
        "https://wheel-secret-aiohttp-host:9444/wheel-secret-aiohttp-path"
        "?token=wheel-secret-aiohttp-query",
        headers={"Authorization": "wheel-secret-aiohttp-header"},
        data=b"wheel-secret-aiohttp-body",
    )
    return core, aio

assert fetch_sync().status == 207
core_response, aio_response = asyncio.run(fetch_async())
assert (core_response.status, aio_response.status) == (208, 209)
""".strip(),
            encoding="utf-8",
        )
        optional_http_runpack = root / "optional-http.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "deep",
                "--name",
                "optional-http-wheel-smoke",
                "--output",
                str(optional_http_runpack),
                "--",
                str(workload_python),
                str(optional_http_workload),
            ),
            cwd=root,
        )
        optional_http_document = _json(
            (
                str(contrail),
                "analyze",
                str(optional_http_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        optional_http_capture = optional_http_document.get("http_capture")
        optional_http_requests = optional_http_document.get("http_requests")
        if (
            not isinstance(optional_http_capture, dict)
            or optional_http_capture.get("status") != "complete"
            or optional_http_capture.get("adapters")
            != [
                "aiohttp.async",
                "httpcore.async",
                "httpcore.sync",
                "stdlib.http.client",
            ]
            or not isinstance(optional_http_requests, list)
            or len(optional_http_requests) != 3
            or not all(isinstance(request, dict) for request in optional_http_requests)
        ):
            raise RuntimeError("installed Deep Capture omitted optional HTTP adapters")
        optional_request_objects = cast(list[dict[str, object]], optional_http_requests)
        if {
            (request.get("method"), request.get("status_code"), request.get("adapter"))
            for request in optional_request_objects
        } != {
            ("POST", 207, "httpcore.sync"),
            ("PUT", 208, "httpcore.async"),
            ("PATCH", 209, "aiohttp.async"),
        }:
            raise RuntimeError("installed optional HTTP capture emitted invalid boundaries")
        for request in optional_request_objects:
            optional_caller = request.get("caller")
            if (
                not isinstance(optional_caller, dict)
                or optional_caller.get("name")
                not in {"__main__.fetch_sync", "__main__.fetch_async"}
                or optional_caller.get("observation") != "exact"
            ):
                raise RuntimeError("installed optional HTTP capture omitted exact caller causality")
        encoded_optional_http = json.dumps(optional_http_document)
        if any(
            secret in encoded_optional_http or secret.encode() in optional_http_runpack.read_bytes()
            for secret in (
                "wheel-secret-host",
                "wheel-secret-path",
                "wheel-secret-query",
                "wheel-secret-header",
                "wheel-secret-body",
                "wheel-secret-response-header",
                "wheel-secret-response-body",
                "wheel-secret-aiohttp-host",
                "wheel-secret-aiohttp-path",
                "wheel-secret-aiohttp-query",
                "wheel-secret-aiohttp-header",
                "wheel-secret-aiohttp-body",
                "wheel-secret-aiohttp-response-body",
            )
        ):
            raise RuntimeError("installed optional HTTP capture retained private data")

        sample_runpack = root / "sample.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "sample",
                "--name",
                "sample-wheel-smoke",
                "--output",
                str(sample_runpack),
                "--",
                str(workload_python),
                "-c",
                (
                    "import os\nimport subprocess\nimport sys\nimport time\n"
                    "subprocess.run([sys.executable, '-c', "
                    "'import time; time.sleep(0.08)', "
                    "'semantic-secret'], check=True)\n"
                    "def useful(): time.sleep(0.65)\nuseful()\nos._exit(0)\n"
                ),
            ),
            cwd=root,
        )
        sample_events = _json(
            (
                str(contrail),
                "query",
                str(sample_runpack),
                "SELECT kind, name FROM events "
                "WHERE kind = 'python.stack.sample' AND name = '__main__.useful'",
                "--format",
                "json",
            ),
            cwd=root,
            document_type="runtime.query",
        )
        if sample_events.get("rows") != [["python.stack.sample", "__main__.useful"]]:
            raise RuntimeError("installed sampling did not observe an unmodified workload")
        sample_analysis = _run(
            (str(contrail), "analyze", str(sample_runpack)),
            cwd=root,
        )
        if "Sampled Python hotspots" not in sample_analysis.stdout or "statistical" not in (
            sample_analysis.stdout
        ):
            raise RuntimeError("installed BatchScope did not explain sampling evidence")
        if not all(
            marker in sample_analysis.stdout
            for marker in (
                "snapshots: controller-side Unix socket",
                "controller normalization:",
                "checkpoint-only: 1 process ended without a final report",
            )
        ):
            raise RuntimeError("installed sampling did not retain abrupt-exit evidence")
        sample_document = _json(
            (
                str(contrail),
                "analyze",
                str(sample_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        sample_profile = sample_document.get("sample_profile")
        semantic_capture = sample_document.get("semantic_capture")
        subprocess_calls = sample_document.get("subprocess_calls")
        sample_caller_attribution = (
            semantic_capture.get("caller_attribution")
            if isinstance(semantic_capture, dict)
            else None
        )
        sample_snapshot_metrics = (
            sample_profile.get("snapshot_metrics") if isinstance(sample_profile, dict) else None
        )
        sample_publication_metrics = (
            sample_profile.get("publication_metrics") if isinstance(sample_profile, dict) else None
        )
        sample_normalization_metrics = (
            sample_profile.get("normalization_metrics")
            if isinstance(sample_profile, dict)
            else None
        )
        if (
            not isinstance(sample_profile, dict)
            or sample_profile.get("status") != "partial"
            or sample_profile.get("checkpoint_process_count") != 1
            or sample_profile.get("transport") != "controller-unix-socket"
            or not isinstance(sample_snapshot_metrics, dict)
            or sample_snapshot_metrics.get("status") != "available"
            or not isinstance(sample_snapshot_metrics.get("checkpoint_message_count"), int)
            or sample_snapshot_metrics["checkpoint_message_count"] < 1
            or not isinstance(sample_publication_metrics, dict)
            or sample_publication_metrics.get("status") != "available"
            or sample_publication_metrics.get("fallback_process_count") != 0
            or not isinstance(sample_normalization_metrics, dict)
            or sample_normalization_metrics.get("status") != "available"
            or not isinstance(sample_normalization_metrics.get("duration_seconds"), (int, float))
            or sample_normalization_metrics["duration_seconds"] <= 0
            or not isinstance(sample_normalization_metrics.get("ranking_database_peak_bytes"), int)
            or sample_normalization_metrics["ranking_database_peak_bytes"] <= 0
        ):
            raise RuntimeError("installed sampling emitted incomplete checkpoint provenance")
        if (
            not isinstance(semantic_capture, dict)
            or semantic_capture.get("status") != "partial"
            or semantic_capture.get("subprocess_count") != 1
            or semantic_capture.get("arguments_captured") is not False
            or not isinstance(subprocess_calls, list)
            or len(subprocess_calls) != 1
            or not isinstance(sample_caller_attribution, dict)
            or sample_caller_attribution.get("status") != "partial"
            or sample_caller_attribution.get("attributed_subprocess_count") != 1
            or not isinstance(subprocess_calls[0], dict)
            or not isinstance(subprocess_calls[0].get("caller"), dict)
            or subprocess_calls[0]["caller"].get("observation") != "sampled"
            or "semantic-secret" in json.dumps(sample_document)
        ):
            raise RuntimeError("installed sampling did not retain private subprocess evidence")

        registration_runpack = root / "sample-registration.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--capture-level",
                "sample",
                "--name",
                "sample-registration-wheel-smoke",
                "--output",
                str(registration_runpack),
                "--",
                str(workload_python),
                "-c",
                "import os\nos._exit(0)\n",
            ),
            cwd=root,
        )
        registration_analysis = _run(
            (str(contrail), "analyze", str(registration_runpack)),
            cwd=root,
        )
        if "registration-only: 1 process loaded capture" not in registration_analysis.stdout:
            raise RuntimeError("installed sampling did not retain immediate-exit registration")
        registration_document = _json(
            (
                str(contrail),
                "analyze",
                str(registration_runpack),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="batchscope.inspect",
        )
        registration_profile = registration_document.get("sample_profile")
        registration_metrics = (
            registration_profile.get("snapshot_metrics")
            if isinstance(registration_profile, dict)
            else None
        )
        if (
            not isinstance(registration_profile, dict)
            or registration_profile.get("status") != "partial"
            or registration_profile.get("process_count") != 1
            or registration_profile.get("registration_only_process_count") != 1
            or not isinstance(registration_metrics, dict)
            or registration_metrics.get("status") != "available"
            or registration_metrics.get("message_count") != 1
        ):
            raise RuntimeError("installed sampling emitted incomplete registration provenance")

        temporal_history = root / "temporal-history.json"
        temporal_runpack = root / "temporal.runpack"
        temporal_history.write_text(
            json.dumps(
                {
                    "events": [
                        {
                            "eventId": "1",
                            "eventTime": "2024-01-01T00:00:00Z",
                            "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
                            "workflowExecutionStartedEventAttributes": {
                                "workflowId": "wheel-smoke",
                                "workflowType": {"name": "SmokeWorkflow"},
                            },
                        },
                        {
                            "eventId": "2",
                            "eventTime": "2024-01-01T00:00:01Z",
                            "eventType": "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
                            "activityTaskScheduledEventAttributes": {
                                "activityId": "smoke-activity",
                                "activityType": {"name": "smoke-activity"},
                            },
                        },
                        {
                            "eventId": "3",
                            "eventTime": "2024-01-01T00:00:02Z",
                            "eventType": "EVENT_TYPE_ACTIVITY_TASK_STARTED",
                            "activityTaskStartedEventAttributes": {
                                "attempt": 1,
                                "scheduledEventId": "2",
                            },
                        },
                        {
                            "eventId": "4",
                            "eventTime": "2024-01-01T00:00:03Z",
                            "eventType": "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
                            "activityTaskCompletedEventAttributes": {
                                "scheduledEventId": "2",
                                "startedEventId": "3",
                            },
                        },
                        {
                            "eventId": "5",
                            "eventTime": "2024-01-01T00:00:04Z",
                            "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED",
                            "workflowExecutionCompletedEventAttributes": {},
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )
        _run(
            (
                str(contrail),
                "enrich-temporal-history",
                str(runpack),
                str(temporal_history),
                "--output",
                str(temporal_runpack),
            ),
            cwd=root,
        )
        temporal_events = _json(
            (
                str(contrail),
                "query",
                str(temporal_runpack),
                "SELECT kind, name FROM events WHERE id LIKE 'temporal:%' ORDER BY kind, name",
                "--format",
                "json",
            ),
            cwd=root,
            document_type="runtime.query",
        )
        if temporal_events.get("rows") != [
            ["operation", "RunActivity:smoke-activity"],
            ["queue.wait", "smoke-activity queue"],
            ["run", "SmokeWorkflow"],
            ["temporal.activity", "smoke-activity"],
        ]:
            raise RuntimeError(
                "installed Temporal history adapter did not preserve lifecycle facts"
            )

        _install_wheel(
            workload_python,
            wheel,
            constraints=constraints,
            find_links=find_links,
            offline=offline,
            cwd=root,
        )
        annotated_runpack = root / "annotated.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--name",
                "annotated-wheel-smoke",
                "--output",
                str(annotated_runpack),
                "--",
                str(workload_python),
                "-I",
                "-c",
                "from runtime_tools import runtime; "
                "runtime.event('db.write', kind='client.request')",
            ),
            cwd=root,
        )
        annotated_events = _json(
            (
                str(contrail),
                "query",
                str(annotated_runpack),
                "SELECT kind, name FROM events WHERE kind = 'client.request' AND name = 'db.write'",
                "--format",
                "json",
            ),
            cwd=root,
            document_type="runtime.query",
        )
        if annotated_events.get("columns") != ["kind", "name"] or annotated_events.get("rows") != [
            ["client.request", "db.write"]
        ]:
            raise RuntimeError("installed workload package did not emit its domain annotation")
        _run(
            (
                str(tool_python),
                "-I",
                "-c",
                "import sys\n"
                "from runtime_tools import RunpackError, open_runpack\n"
                "reader = open_runpack(sys.argv[1])\n"
                "with reader as runpack:\n"
                "    events = [(event.kind, event.name) for event in runpack.events() "
                "if event.kind == 'client.request' and event.name == 'db.write']\n"
                "assert events == [('client.request', 'db.write')], events\n"
                "try:\n"
                "    reader.events()\n"
                "except RunpackError:\n"
                "    pass\n"
                "else:\n"
                "    raise AssertionError('closed runpack remained readable')\n",
                str(annotated_runpack),
            ),
            cwd=root,
        )
        _json(
            (str(rundiff), "compare", str(runpack), str(runpack), "--format", "json"),
            cwd=root,
            document_type="rundiff.compare",
        )
        _json(
            (str(batchscope), "inspect", str(runpack), "--format", "json"),
            cwd=root,
            document_type="batchscope.inspect",
        )

        contract = root / "contract.yaml"
        contract.write_text(
            "name: wheel-smoke\nassertions:\n  - type: output_equivalent\n",
            encoding="utf-8",
        )
        verification_command = (
            str(proofline),
            "verify",
            str(contract),
            "--baseline",
            str(runpack),
            "--candidate",
            str(runpack),
            "--format",
            "json",
        )
        _json(
            verification_command,
            cwd=root,
            document_type="proofline.verification",
        )
        explanation = _json(
            (
                *verification_command,
                "--explain",
            ),
            cwd=root,
            document_type="proofline.verification",
        )
        diff = explanation.get("diff")
        if not isinstance(diff, dict):
            raise RuntimeError("installed Proofline explanation did not contain a runtime diff")
        if diff.get("document_type") != "rundiff.compare" or diff.get("format_version") != "1":
            raise RuntimeError("installed Proofline explanation contained an invalid runtime diff")
        _assert_artifact_bindings(explanation, runpack, runpack)
        results = explanation.get("results")
        if not isinstance(results, list) or not results:
            raise RuntimeError("installed Proofline explanation did not contain claim results")
        evidence = results[0].get("evidence") if isinstance(results[0], dict) else None
        if evidence != [
            {
                "fact": {"equivalent": True},
                "diff_path": "/output_equivalent",
                "selector": {},
            }
        ]:
            raise RuntimeError("installed Proofline claim did not reference its runtime diff fact")

        workload_dependency = "contrail_release_smoke_workload_dependency"
        (workload_site_packages / f"{workload_dependency}.py").write_text(
            "VALUE = 'workload environment selected'\n",
            encoding="utf-8",
        )
        tool_dependency_probe = subprocess.run(
            (str(tool_python), "-I", "-c", f"import {workload_dependency}"),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if tool_dependency_probe.returncode == 0:
            raise RuntimeError("workload-only smoke dependency leaked into the tool environment")
        experiment_repo = root / "proofline-repo"
        experiment_repo.mkdir()
        _run(("git", "init", "-b", "main"), cwd=experiment_repo)
        _run(("git", "config", "user.name", "Contrail release smoke"), cwd=experiment_repo)
        _run(
            ("git", "config", "user.email", "release-smoke@example.invalid"),
            cwd=experiment_repo,
        )
        (experiment_repo / "workload.py").write_text(
            f"import {workload_dependency}\nprint({workload_dependency}.VALUE)\n",
            encoding="utf-8",
        )
        _run(("git", "add", "workload.py"), cwd=experiment_repo)
        _run(("git", "commit", "-m", "workload"), cwd=experiment_repo)
        experiment_contract = root / "experiment-contract.yaml"
        experiment_contract.write_text(
            "name: workload-python\nassertions:\n  - type: candidate_exit_success\n",
            encoding="utf-8",
        )
        experiment_output = root / "proofline-results"
        experiment = _json(
            (
                str(contrail),
                "run",
                str(experiment_contract),
                "--baseline-ref",
                "main",
                "--candidate-ref",
                "main",
                "--workload",
                "workload.py",
                "--python",
                str(workload_python),
                "--capture-level",
                "passive",
                "--output-dir",
                str(experiment_output),
                "--format",
                "json",
                "--explain",
            ),
            cwd=experiment_repo,
            document_type="proofline.experiment",
        )
        if experiment.get("baseline_exit_code") != 0 or experiment.get("candidate_exit_code") != 0:
            raise RuntimeError("installed Proofline did not use the selected workload Python")
        _assert_artifact_bindings(
            experiment,
            experiment_output / "baseline.runpack",
            experiment_output / "candidate.runpack",
        )
        selected_python = str(Path(os.path.abspath(workload_python)))
        for runpack_name in ("baseline.runpack", "candidate.runpack"):
            inspected_experiment = _json(
                (
                    str(contrail),
                    "inspect",
                    str(experiment_output / runpack_name),
                    "--format",
                    "json",
                ),
                cwd=root,
                document_type="runtime.inspect",
            )
            recorded_command = inspected_experiment.get("command")
            if not isinstance(recorded_command, list) or recorded_command[:1] != [selected_python]:
                raise RuntimeError("Proofline runpack did not record the selected workload Python")
            capture_query = _json(
                (
                    str(contrail),
                    "query",
                    str(experiment_output / runpack_name),
                    "SELECT metadata_json FROM executions",
                    "--format",
                    "json",
                ),
                cwd=root,
                document_type="runtime.query",
            )
            capture_rows = capture_query.get("rows")
            if (
                not isinstance(capture_rows, list)
                or len(capture_rows) != 1
                or not isinstance(capture_rows[0], list)
                or len(capture_rows[0]) != 1
                or not isinstance(capture_rows[0][0], str)
            ):
                raise RuntimeError("installed Proofline runpack did not expose capture metadata")
            capture_metadata = json.loads(capture_rows[0][0]).get("capture")
            if not isinstance(capture_metadata, dict) or capture_metadata.get("level") != "passive":
                raise RuntimeError("installed Proofline did not propagate the capture level")

        demo = root / "contrail-demo"
        demo_result = _run(
            (str(contrail), "demo", "--output-dir", str(demo)),
            cwd=root,
        )
        if not demo_result.stdout.startswith("CONTRAIL DEMO READY\n"):
            raise RuntimeError("installed Contrail demo did not report a ready walkthrough")
        demo_baseline = demo / "baseline.runpack"
        demo_candidate = demo / "candidate.runpack"
        demo_contract = demo / "contract.yaml"
        demo_workload = demo / "workload.py"
        if not demo_workload.is_file() or '"peer.service": "metadata-db"' not in (
            demo_workload.read_text(encoding="utf-8")
        ):
            raise RuntimeError("installed demo did not retain its adaptable workload")
        recaptured_baseline = demo / "recaptured-baseline.runpack"
        recaptured_candidate = demo / "recaptured-candidate.runpack"
        recaptured_report = demo / "recaptured-report.json"
        for name, variant, output in (
            ("demo-baseline", "baseline", recaptured_baseline),
            ("demo-candidate", "candidate", recaptured_candidate),
        ):
            _run(
                (
                    str(contrail),
                    "record",
                    "--name",
                    name,
                    "--output",
                    str(output),
                    "--",
                    str(tool_python),
                    str(demo_workload),
                    variant,
                ),
                cwd=root,
            )
        recaptured_gate = subprocess.run(
            (
                str(contrail),
                "verify",
                str(demo_contract),
                "--baseline",
                str(recaptured_baseline),
                "--candidate",
                str(recaptured_candidate),
                "--report",
                str(recaptured_report),
            ),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if recaptured_gate.returncode != 1:
            raise RuntimeError("installed demo template did not reproduce its failed gate")
        recaptured_document = json.loads(recaptured_report.read_text(encoding="utf-8"))
        _assert_artifact_bindings(
            recaptured_document,
            recaptured_baseline,
            recaptured_candidate,
        )
        report_path = root / "proofline-report.json"
        _json(
            (str(contrail), "inspect", str(demo_candidate), "--format", "json"),
            cwd=root,
            document_type="runtime.inspect",
        )
        _json(
            (
                str(contrail),
                "compare",
                str(demo_baseline),
                str(demo_candidate),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="rundiff.compare",
        )
        _json(
            (str(contrail), "analyze", str(demo_candidate), "--format", "json"),
            cwd=root,
            document_type="batchscope.inspect",
        )
        suite_report = _run(
            (
                str(contrail),
                "report",
                str(demo_baseline),
                str(demo_candidate),
                "--contract",
                str(demo_contract),
            ),
            cwd=root,
        )
        expected_report_text = (
            "CONTRAIL SUITE REPORT\n",
            "BATCHSCOPE\n",
            "RUNTIME DIFF\n",
            "PROOFLINE\n",
            "contracts: 3 passed, 2 failed, 0 unverifiable",
        )
        if any(text not in suite_report.stdout for text in expected_report_text):
            raise RuntimeError("installed Contrail suite report was incomplete")
        explained_failure = subprocess.run(
            (
                str(contrail),
                "verify",
                str(demo_contract),
                "--baseline",
                str(demo_baseline),
                "--candidate",
                str(demo_candidate),
                "--report",
                str(report_path),
            ),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if explained_failure.returncode != 1:
            raise RuntimeError(
                "installed Proofline regression did not preserve its failed-gate status"
            )
        failed_report = json.loads(report_path.read_text(encoding="utf-8"))
        _assert_artifact_bindings(failed_report, demo_baseline, demo_candidate)
        failed_types = {
            result["type"]
            for result in failed_report.get("results", [])
            if isinstance(result, dict) and result.get("status") == "fail"
        }
        if failed_types != {"forbid_new_dependency", "max_operation_count"}:
            raise RuntimeError("installed Contrail demo did not preserve its expected violations")
        payload = _serve_payload(
            contrail,
            demo_baseline,
            demo_candidate,
            report_path,
            cwd=root,
        )
        proofline_payload = payload.get("proofline")
        if not isinstance(proofline_payload, dict) or proofline_payload.get("source") != "report":
            raise RuntimeError("installed timeline did not replay the retained CLI report")
        findings = proofline_payload.get("findings")
        selections = proofline_payload.get("selections")
        if not isinstance(findings, list) or not isinstance(selections, dict):
            raise RuntimeError("installed timeline did not expose Proofline evidence")
        if {
            finding.get("report_assurance") for finding in findings if isinstance(finding, dict)
        } != {"artifact_bound_policy_replayed"}:
            raise RuntimeError("installed timeline did not verify exact artifact bindings")
        by_type = {finding["type"]: finding for finding in findings if isinstance(finding, dict)}
        dependency = selections[by_type["forbid_new_dependency"]["selection_id"]]
        operations = selections[by_type["max_operation_count"]["selection_id"]]
        if not isinstance(dependency, dict) or not isinstance(operations, dict):
            raise RuntimeError("installed timeline selections were malformed")
        if len(dependency["candidate_event_ids"]) != 1 or dependency["truncated"]:
            raise RuntimeError("installed dependency selection was incomplete")
        if len(operations["candidate_event_ids"]) != 30 or operations["truncated"]:
            raise RuntimeError("installed operation selection was incomplete")

        substituted_candidate = root / "substituted-candidate.runpack"
        shutil.copyfile(demo_candidate, substituted_candidate)
        with sqlite3.connect(substituted_candidate) as connection:
            connection.execute(
                "UPDATE executions SET working_directory = ?",
                ("/substituted/evidence",),
            )
        rejected = subprocess.run(
            (
                str(contrail),
                "serve",
                str(demo_baseline),
                "--compare",
                str(substituted_candidate),
                "--proofline-report",
                str(report_path),
                "--no-open",
                "--port",
                "0",
            ),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if rejected.returncode != 2 or "artifact binding" not in rejected.stderr:
            raise RuntimeError("installed timeline accepted substituted runpack evidence")
        _run(
            (
                str(tool_python),
                "-c",
                "from importlib.resources import files; "
                "p=files('runtime_tools.ui').joinpath('static'); "
                "assert all(p.joinpath(n).is_file() for n in "
                "('index.html','app.js','styles.css'))",
            ),
            cwd=root,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument(
        "--constraints",
        type=Path,
        help="pip constraints exported from the lockfile",
    )
    parser.add_argument(
        "--find-links",
        action="append",
        type=Path,
        default=[],
        help="directory containing dependency wheels; repeat as needed",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="disable package indexes; dependencies must be available via --find-links",
    )
    args = parser.parse_args(argv)
    smoke(
        args.wheel,
        constraints=args.constraints,
        find_links=tuple(args.find_links),
        offline=args.offline,
    )
    print(f"installed-wheel smoke passed: {args.wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
