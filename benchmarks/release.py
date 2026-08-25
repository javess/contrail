#!/usr/bin/env python3
"""Run deterministic, dependency-free release performance gates."""

from __future__ import annotations

import argparse
import json
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import cast

from generate import (
    write_asgi_server_workload,
    write_async_task_workload,
    write_causal_chain,
    write_deep_profile_reports,
    write_executor_task_workload,
    write_finished_runpack,
    write_kubernetes_snapshot,
    write_logical_operation_workload,
    write_native_call_workload,
    write_network_workload,
    write_optional_client_workload,
    write_otlp_trace,
    write_prometheus_response,
    write_python_exception_workload,
    write_temporal_history,
    write_wsgi_server_workload,
)

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.capture import record_process
from runtime_tools.deep_profile import DeepProfileSession, load_deep_profile
from runtime_tools.kubernetes import import_kubernetes_snapshot
from runtime_tools.otel import import_otlp_json
from runtime_tools.prometheus import import_prometheus_response
from runtime_tools.proofline.verify import verify_contracts_with_artifact_bindings
from runtime_tools.storage import RunpackReader
from runtime_tools.temporal import import_temporal_history
from runtime_tools.ui import build_timeline_payload


def _peak_rss_mib() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / (1024 * 1024) if sys.platform == "darwin" else value / 1024


def _worker(case: str, count: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="contrail-release-benchmark-") as directory:
        root = Path(directory)
        output: Path | None = None
        case_metrics: dict[str, object] = {}
        if case == "otel_import":
            source = root / "trace.json"
            output = root / "trace.runpack"
            write_otlp_trace(source, count)
            start = time.perf_counter()
            observed = import_otlp_json(source, output, name="benchmark").event_count
        elif case == "prometheus_import":
            runpack = root / "base.runpack"
            source = root / "prometheus.json"
            output = root / "prometheus.runpack"
            write_finished_runpack(runpack, finished_at_ns=(count + 1) * 1_000_000_000)
            write_prometheus_response(source, count)
            start = time.perf_counter()
            observed = import_prometheus_response(runpack, source, output).sample_count
        elif case == "kubernetes_import":
            runpack = root / "base.runpack"
            source = root / "kubernetes.json"
            output = root / "kubernetes.runpack"
            write_finished_runpack(runpack, finished_at_ns=1)
            write_kubernetes_snapshot(source, count)
            start = time.perf_counter()
            observed = import_kubernetes_snapshot(runpack, source, output).event_count
        elif case == "temporal_import":
            runpack = root / "base.runpack"
            source = root / "temporal.json"
            output = root / "temporal.runpack"
            write_finished_runpack(runpack, finished_at_ns=1)
            write_temporal_history(source, count)
            start = time.perf_counter()
            observed = import_temporal_history(runpack, source, output).activity_count
        elif case == "batchscope_analyze":
            runpack = root / "chain.runpack"
            write_causal_chain(runpack, count)
            start = time.perf_counter()
            analysis = analyze_runpack(runpack)
            observed = len(analysis.critical_path.event_ids) if analysis.critical_path else 0
        elif case == "profile_merge":
            write_deep_profile_reports(root, count)
            status = root.stat()
            session = DeepProfileSession(root, (status.st_dev, status.st_ino))
            start = time.perf_counter()
            profile = load_deep_profile(session, entity_id="process")
            observed = len(profile.events)
            if (
                profile.normalization_metrics_status != "available"
                or profile.normalization_duration_ns <= 0
                or profile.ranking_database_peak_bytes <= 0
            ):
                raise RuntimeError("profile_merge did not retain normalization metrics")
            case_metrics = {
                "normalization_seconds": round(
                    profile.normalization_duration_ns / 1_000_000_000,
                    6,
                ),
                "ranking_database_peak_mib": round(
                    profile.ranking_database_peak_bytes / (1024 * 1024),
                    3,
                ),
            }
        elif case == "network_capture":
            workload = root / "network-workload.py"
            passive = root / "network-passive.runpack"
            output = root / "network-sample.runpack"
            write_network_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="network-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="network-sample",
                capture_level="sample",
            )
            analysis = analyze_runpack(output)
            observed = (
                analysis.network_capture.connection_count
                if analysis.network_capture is not None
                else 0
            )
            if (
                analysis.network_setup_capture is None
                or analysis.network_setup_capture.status != "complete"
                or analysis.network_setup_capture.phase_count != count
                or analysis.network_setup_capture.adapters
                != (
                    "stdlib.socket.getaddrinfo",
                    "stdlib.ssl.SSLObject.do_handshake",
                    "stdlib.ssl.SSLSocket.do_handshake",
                )
            ):
                raise RuntimeError("network_capture did not retain complete DNS setup evidence")
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                sample_execution = reader.execution()
            if passive_execution.finished_at_ns is None or sample_execution.finished_at_ns is None:
                raise RuntimeError("network_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            sample_seconds = (
                sample_execution.finished_at_ns - sample_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("network_capture passive duration was not positive")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "sample_workload_seconds": round(sample_seconds, 6),
                "capture_overhead_ratio": round(sample_seconds / passive_seconds, 3),
            }
        elif case == "logical_operation_capture":
            workload = root / "logical-operation-workload.py"
            output = root / "logical-operation-deep.runpack"
            write_logical_operation_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                output,
                name="logical-operation-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            observed = (
                analysis.logical_operation_capture.operation_count
                if analysis.logical_operation_capture is not None
                else 0
            )
            if (
                analysis.logical_operation_capture is None
                or analysis.logical_operation_capture.status != "complete"
                or analysis.logical_operation_capture.dropped_operation_count != 0
                or analysis.logical_operation_capture.adapters
                != (
                    "stdlib.sqlite3.Connection",
                    "stdlib.sqlite3.Cursor",
                )
            ):
                raise RuntimeError(
                    "logical_operation_capture did not retain complete SQLite evidence"
                )
            if b"SELECT ?" in output.read_bytes():
                raise RuntimeError("logical_operation_capture retained SQL text")
        elif case == "optional_client_capture":
            workload = root / "optional-client-workload.py"
            package_root = root
            passive = root / "optional-client-passive.runpack"
            output = root / "optional-client-deep.runpack"
            write_optional_client_workload(workload, package_root, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="optional-client-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="optional-client-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            observed = (
                analysis.logical_operation_capture.operation_count
                if analysis.logical_operation_capture is not None
                else 0
            )
            if (
                analysis.logical_operation_capture is None
                or analysis.logical_operation_capture.status != "complete"
                or analysis.logical_operation_capture.dropped_operation_count != 0
                or analysis.logical_operation_capture.adapters != ("redis.Pipeline", "redis.Redis")
                or observed != count
            ):
                raise RuntimeError(
                    "optional_client_capture did not retain complete Redis-shaped evidence"
                )
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                deep_execution = reader.execution()
            if passive_execution.finished_at_ns is None or deep_execution.finished_at_ns is None:
                raise RuntimeError("optional_client_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            deep_seconds = (
                deep_execution.finished_at_ns - deep_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("optional_client_capture passive duration was not positive")
            if b"optional-client-benchmark-secret" in output.read_bytes():
                raise RuntimeError("optional_client_capture retained a cache key")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "deep_workload_seconds": round(deep_seconds, 6),
                "capture_overhead_ratio": round(deep_seconds / passive_seconds, 3),
            }
        elif case == "executor_task_capture":
            workload = root / "executor-task-workload.py"
            passive = root / "executor-task-passive.runpack"
            output = root / "executor-task-deep.runpack"
            write_executor_task_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="executor-task-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="executor-task-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            executor_tasks = tuple(
                operation
                for operation in analysis.logical_operations
                if operation.category == "executor"
            )
            summary = analysis.logical_operation_capture
            observed = summary.operation_count if summary is not None else 0
            if (
                summary is None
                or summary.status != "complete"
                or summary.operation_count != count
                or summary.dropped_operation_count != 0
                or summary.adapters
                != (
                    "stdlib.concurrent.futures.ThreadPoolExecutor",
                    "stdlib.queue.Queue",
                )
            ):
                raise RuntimeError(
                    "executor_task_capture did not retain complete executor evidence"
                )
            if (
                any(
                    operation.operation != "task"
                    or operation.outcome != "completed"
                    or operation.duration_seconds is None
                    or operation.caller is None
                    for operation in executor_tasks
                )
                or not executor_tasks
            ):
                raise RuntimeError("executor_task_capture retained inconsistent task evidence")
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                deep_execution = reader.execution()
            if passive_execution.finished_at_ns is None or deep_execution.finished_at_ns is None:
                raise RuntimeError("executor_task_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            deep_seconds = (
                deep_execution.finished_at_ns - deep_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("executor_task_capture passive duration was not positive")
            if b"executor-benchmark-secret" in output.read_bytes():
                raise RuntimeError("executor_task_capture retained a task argument")
            public_summary = summary.as_json_value()
            if (
                public_summary.get("callable_captured") is not False
                or public_summary.get("arguments_captured") is not False
                or public_summary.get("return_value_captured") is not False
                or public_summary.get("exception_messages_captured") is not False
            ):
                raise RuntimeError("executor_task_capture omitted privacy markers")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "deep_workload_seconds": round(deep_seconds, 6),
                "capture_overhead_ratio": round(deep_seconds / passive_seconds, 3),
            }
        elif case == "async_task_capture":
            workload = root / "async-task-workload.py"
            passive = root / "async-task-passive.runpack"
            output = root / "async-task-deep.runpack"
            write_async_task_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="async-task-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="async-task-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            scheduled_tasks = tuple(
                operation
                for operation in analysis.logical_operations
                if operation.category == "scheduler"
            )
            scheduler_hotspots = tuple(
                hotspot
                for hotspot in analysis.logical_operation_hotspots
                if hotspot.category == "scheduler"
            )
            summary = analysis.logical_operation_capture
            observed = summary.operation_count if summary is not None else 0
            if (
                summary is None
                or summary.status != "complete"
                or summary.operation_count != count
                or summary.dropped_operation_count != 0
                or summary.adapters
                != (
                    "stdlib.asyncio.Queue",
                    "stdlib.asyncio.TaskGroup",
                    "stdlib.asyncio.create_task",
                    "stdlib.asyncio.ensure_future",
                    "stdlib.asyncio.gather",
                )
            ):
                raise RuntimeError("async_task_capture did not retain complete scheduler evidence")
            if {hotspot.adapter for hotspot in scheduler_hotspots} != {
                "stdlib.asyncio.TaskGroup",
                "stdlib.asyncio.create_task",
                "stdlib.asyncio.ensure_future",
                "stdlib.asyncio.gather",
            } or sum(hotspot.operation_count for hotspot in scheduler_hotspots) != count:
                raise RuntimeError("async_task_capture omitted a public scheduling adapter")
            if (
                any(
                    operation.operation != "task"
                    or operation.outcome != "completed"
                    or operation.duration_seconds is None
                    or operation.caller is None
                    for operation in scheduled_tasks
                )
                or not scheduled_tasks
            ):
                raise RuntimeError("async_task_capture retained inconsistent task evidence")
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                deep_execution = reader.execution()
            if passive_execution.finished_at_ns is None or deep_execution.finished_at_ns is None:
                raise RuntimeError("async_task_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            deep_seconds = (
                deep_execution.finished_at_ns - deep_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("async_task_capture passive duration was not positive")
            runpack_bytes = output.read_bytes()
            if (
                b"async-task-benchmark-secret" in runpack_bytes
                or b"async-task-benchmark-name" in runpack_bytes
            ):
                raise RuntimeError("async_task_capture retained private task data")
            public_summary = summary.as_json_value()
            if (
                public_summary.get("awaitable_captured") is not False
                or public_summary.get("task_name_captured") is not False
                or public_summary.get("context_captured") is not False
                or public_summary.get("arguments_captured") is not False
                or public_summary.get("return_value_captured") is not False
                or public_summary.get("exception_messages_captured") is not False
            ):
                raise RuntimeError("async_task_capture omitted privacy markers")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "deep_workload_seconds": round(deep_seconds, 6),
                "capture_overhead_ratio": round(deep_seconds / passive_seconds, 3),
            }
        elif case == "wsgi_server_capture":
            workload = root / "wsgi-server-workload.py"
            passive = root / "wsgi-server-passive.runpack"
            output = root / "wsgi-server-deep.runpack"
            write_wsgi_server_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="wsgi-server-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="wsgi-server-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            summary = analysis.logical_operation_capture
            server_hotspots = tuple(
                hotspot
                for hotspot in analysis.logical_operation_hotspots
                if hotspot.category == "server"
            )
            server_requests = tuple(
                operation
                for operation in analysis.logical_operations
                if operation.category == "server"
            )
            observed = summary.operation_count if summary is not None else 0
            if (
                summary is None
                or summary.status != "complete"
                or summary.operation_count != count
                or summary.dropped_operation_count != 0
                or summary.adapters != ("stdlib.wsgiref",)
                or len(server_hotspots) != 1
                or server_hotspots[0].operation_count != count
            ):
                raise RuntimeError("wsgi_server_capture did not retain complete request evidence")
            if not server_requests or any(
                request.operation != "request"
                or request.outcome != "completed"
                or request.status_code != 200
                or request.duration_seconds is None
                or request.caller is None
                or request.caller.name != "__main__.application"
                for request in server_requests
            ):
                raise RuntimeError("wsgi_server_capture retained inconsistent request evidence")
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                deep_execution = reader.execution()
            if passive_execution.finished_at_ns is None or deep_execution.finished_at_ns is None:
                raise RuntimeError("wsgi_server_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            deep_seconds = (
                deep_execution.finished_at_ns - deep_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("wsgi_server_capture passive duration was not positive")
            runpack_bytes = output.read_bytes()
            if any(
                private_value in runpack_bytes
                for private_value in (
                    b"wsgi-server-benchmark-private-path",
                    b"wsgi-server-benchmark-response-secret",
                    b"wsgi-server-benchmark-response-body-secret",
                    b"wsgi-server-benchmark-request-secret",
                )
            ):
                raise RuntimeError("wsgi_server_capture retained private request data")
            public_summary = summary.as_json_value()
            if any(
                public_summary.get(marker) is not False
                for marker in (
                    "http_method_captured",
                    "route_captured",
                    "url_captured",
                    "headers_captured",
                    "body_captured",
                    "response_body_captured",
                    "client_address_captured",
                )
            ):
                raise RuntimeError("wsgi_server_capture omitted privacy markers")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "deep_workload_seconds": round(deep_seconds, 6),
                "capture_overhead_ratio": round(deep_seconds / passive_seconds, 3),
            }
        elif case == "asgi_server_capture":
            workload = root / "asgi-server-workload.py"
            passive = root / "asgi-server-passive.runpack"
            output = root / "asgi-server-deep.runpack"
            write_asgi_server_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="asgi-server-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="asgi-server-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            summary = analysis.logical_operation_capture
            server_requests = tuple(
                operation
                for operation in analysis.logical_operations
                if operation.category == "server"
            )
            server_hotspots = tuple(
                hotspot
                for hotspot in analysis.logical_operation_hotspots
                if hotspot.category == "server"
            )
            observed = summary.operation_count if summary is not None else 0
            if (
                summary is None
                or summary.status != "complete"
                or summary.operation_count != count
                or summary.dropped_operation_count != 0
                or not server_requests
                or {request.adapter for request in server_requests}
                != {"uvicorn.h11", "uvicorn.httptools"}
                or sum(hotspot.operation_count for hotspot in server_hotspots) != count
                or any(
                    request.operation != "request"
                    or request.outcome != "completed"
                    or request.status_code != 200
                    or request.duration_seconds is None
                    or request.caller is None
                    or request.caller.name != "__main__.application"
                    for request in server_requests
                )
            ):
                raise RuntimeError("asgi_server_capture did not retain complete request evidence")
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                deep_execution = reader.execution()
            if passive_execution.finished_at_ns is None or deep_execution.finished_at_ns is None:
                raise RuntimeError("asgi_server_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            deep_seconds = (
                deep_execution.finished_at_ns - deep_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("asgi_server_capture passive duration was not positive")
            runpack_bytes = output.read_bytes()
            if any(
                private_value in runpack_bytes
                for private_value in (
                    b"PRIVATE-BENCHMARK-METHOD",
                    b"asgi-server-benchmark-private-path",
                    b"asgi-server-benchmark-private-query",
                    b"asgi-server-benchmark-request-secret",
                    b"asgi-server-benchmark-private-client",
                    b"asgi-server-benchmark-request-body-secret",
                    b"asgi-server-benchmark-response-secret",
                    b"asgi-server-benchmark-response-body-one-secret",
                    b"asgi-server-benchmark-response-body-two-secret",
                )
            ):
                raise RuntimeError("asgi_server_capture retained private request data")
            public_summary = summary.as_json_value()
            if any(
                public_summary.get(marker) is not False
                for marker in (
                    "http_method_captured",
                    "route_captured",
                    "url_captured",
                    "headers_captured",
                    "body_captured",
                    "response_body_captured",
                    "client_address_captured",
                )
            ):
                raise RuntimeError("asgi_server_capture omitted privacy markers")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "deep_workload_seconds": round(deep_seconds, 6),
                "capture_overhead_ratio": round(deep_seconds / passive_seconds, 3),
            }
        elif case == "native_call_capture":
            workload = root / "native-call-workload.py"
            passive = root / "native-call-passive.runpack"
            output = root / "native-call-deep.runpack"
            write_native_call_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="native-call-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="native-call-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            compression = next(
                (
                    hotspot
                    for hotspot in analysis.python_hotspots
                    if hotspot.name == "zlib.compress"
                ),
                None,
            )
            observed = compression.call_count if compression is not None else 0
            if (
                compression is None
                or compression.implementation != "native"
                or compression.exception_count != 0
                or analysis.deep_profile is None
                or analysis.deep_profile.status != "complete"
                or analysis.deep_profile.native_call_capture is None
                or analysis.deep_profile.native_call_capture.status != "complete"
                or analysis.deep_profile.native_call_capture.call_count < count
                or analysis.deep_profile.observer_integrity is None
                or analysis.deep_profile.observer_integrity.status != "complete"
            ):
                raise RuntimeError("native_call_capture did not retain complete native evidence")
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                deep_execution = reader.execution()
            if passive_execution.finished_at_ns is None or deep_execution.finished_at_ns is None:
                raise RuntimeError("native_call_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            deep_seconds = (
                deep_execution.finished_at_ns - deep_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("native_call_capture passive duration was not positive")
            if b"native-benchmark-secret" in output.read_bytes():
                raise RuntimeError("native_call_capture retained a call argument")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "deep_workload_seconds": round(deep_seconds, 6),
                "capture_overhead_ratio": round(deep_seconds / passive_seconds, 3),
            }
        elif case == "python_exception_capture":
            workload = root / "python-exception-workload.py"
            passive = root / "python-exception-passive.runpack"
            output = root / "python-exception-deep.runpack"
            write_python_exception_workload(workload, count)
            start = time.perf_counter()
            record_process(
                (sys.executable, str(workload)),
                passive,
                name="python-exception-passive",
                capture_level="passive",
            )
            record_process(
                (sys.executable, str(workload)),
                output,
                name="python-exception-deep",
                capture_level="deep",
            )
            analysis = analyze_runpack(output)
            churn = next(
                (
                    hotspot
                    for hotspot in analysis.python_hotspots
                    if hotspot.name == "__main__.churn"
                ),
                None,
            )
            observed = (
                churn.non_control_flow_exception_count
                if churn is not None and churn.non_control_flow_exception_count is not None
                else 0
            )
            exception_capture = (
                analysis.deep_profile.python_exception_capture
                if analysis.deep_profile is not None
                else None
            )
            exception_churn = next(
                (
                    finding
                    for finding in analysis.bottlenecks
                    if finding.classification == "python_exception_churn"
                ),
                None,
            )
            if (
                churn is None
                or churn.implementation != "python"
                or churn.call_count != 1
                or churn.exception_count != count
                or churn.non_control_flow_exception_count != count
                or exception_churn is None
                or "built-in iterator completion is excluded" not in exception_churn.evidence
                or "events are not unique failures" not in exception_churn.evidence
                or exception_capture is None
                or exception_capture.status != "complete"
                or exception_capture.event_count < count
                or exception_capture.dropped_event_count != 0
                or exception_capture.control_flow_filter is None
                or exception_capture.control_flow_filter.status != "complete"
                or exception_capture.control_flow_filter.non_control_flow_event_count < count
                or exception_capture.control_flow_filter.filtered_event_count <= 0
                or exception_capture.control_flow_filter.dropped_non_control_flow_event_count != 0
                or analysis.deep_profile is None
                or analysis.deep_profile.observer_integrity is None
                or analysis.deep_profile.observer_integrity.status != "complete"
            ):
                raise RuntimeError(
                    "python_exception_capture did not retain complete exception evidence"
                )
            with RunpackReader(passive) as reader:
                passive_execution = reader.execution()
            with RunpackReader(output) as reader:
                deep_execution = reader.execution()
            if passive_execution.finished_at_ns is None or deep_execution.finished_at_ns is None:
                raise RuntimeError("python_exception_capture produced an unfinished execution")
            passive_seconds = (
                passive_execution.finished_at_ns - passive_execution.started_at_ns
            ) / 1_000_000_000
            deep_seconds = (
                deep_execution.finished_at_ns - deep_execution.started_at_ns
            ) / 1_000_000_000
            if passive_seconds <= 0:
                raise RuntimeError("python_exception_capture passive duration was not positive")
            if b"python-exception-benchmark-secret" in output.read_bytes():
                raise RuntimeError("python_exception_capture retained an exception message")
            case_metrics = {
                "passive_workload_seconds": round(passive_seconds, 6),
                "deep_workload_seconds": round(deep_seconds, 6),
                "capture_overhead_ratio": round(deep_seconds / passive_seconds, 3),
            }
        elif case == "ui_payload":
            runpack = root / "timeline.runpack"
            write_causal_chain(runpack, count)
            start = time.perf_counter()
            payload = build_timeline_payload(runpack)
            runs = payload["runs"]
            assert isinstance(runs, list) and isinstance(runs[0], dict)
            events = runs[0]["events"]
            assert isinstance(events, list)
            observed = len(events)
        elif case == "retained_report":
            baseline = root / "baseline.runpack"
            candidate = root / "candidate.runpack"
            contract = root / "contract.yaml"
            report_path = root / "proofline-report.json"
            write_causal_chain(baseline, count)
            write_causal_chain(candidate, count)
            contract.write_text(
                "name: retained-report\nassertions:\n  - type: output_equivalent\n",
                encoding="utf-8",
            )
            start = time.perf_counter()
            report, diff, bindings = verify_contracts_with_artifact_bindings(
                contract,
                baseline,
                candidate,
            )
            document = report.as_json_value(
                include_evidence=True,
                artifact_bindings=bindings,
            )
            document["diff"] = diff.as_json_value()
            report_path.write_text(
                json.dumps(document, allow_nan=False, sort_keys=True),
                encoding="utf-8",
            )
            payload = build_timeline_payload(
                baseline,
                candidate,
                proofline_report=report_path,
            )
            runs = payload["runs"]
            assert isinstance(runs, list) and isinstance(runs[1], dict)
            events = runs[1]["events"]
            assert isinstance(events, list)
            observed = len(events)
        else:
            raise ValueError(f"unknown benchmark case: {case}")
        elapsed = time.perf_counter() - start
        if observed != count:
            raise RuntimeError(f"{case} produced {observed} records; expected {count}")
        if case.endswith("import"):
            assert output is not None
            with RunpackReader(output) as reader:
                reader.execution()
        return {
            "case": case,
            "count": count,
            "seconds": round(elapsed, 6),
            "peak_rss_mib": round(_peak_rss_mib(), 3),
            **case_metrics,
        }


def _load_profile(path: Path, profile: str) -> dict[str, dict[str, float | int]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    profiles = document.get("profiles")
    if not isinstance(profiles, dict) or profile not in profiles:
        raise ValueError(f"benchmark profile is not defined: {profile}")
    selected = profiles[profile]
    if not isinstance(selected, dict):
        raise ValueError(f"benchmark profile must be an object: {profile}")
    return cast(dict[str, dict[str, float | int]], selected)


def _run_case(
    case: str,
    budget: dict[str, float | int],
) -> tuple[dict[str, object], tuple[str, ...]]:
    count = int(budget["count"])
    maximum_seconds = float(budget["max_seconds"])
    timeout = max(30.0, maximum_seconds * 4)
    completed = subprocess.run(
        (sys.executable, str(Path(__file__).resolve()), "--worker", case, str(count)),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"{case} worker failed:\n{completed.stderr or completed.stdout}")
    result = json.loads(completed.stdout)
    if not isinstance(result, dict):
        raise RuntimeError(f"{case} worker did not emit a JSON object")
    failures = []
    if float(result["seconds"]) > maximum_seconds:
        failures.append(f"time {result['seconds']}s > {maximum_seconds}s")
    maximum_rss = float(budget["max_rss_mib"])
    if float(result["peak_rss_mib"]) > maximum_rss:
        failures.append(f"RSS {result['peak_rss_mib']} MiB > {maximum_rss} MiB")
    maximum_overhead = budget.get("max_capture_overhead_ratio")
    if maximum_overhead is not None and (
        float(result.get("capture_overhead_ratio", float("inf"))) > float(maximum_overhead)
    ):
        failures.append(
            f"capture overhead {result.get('capture_overhead_ratio')}x > {float(maximum_overhead)}x"
        )
    result["max_seconds"] = maximum_seconds
    result["max_rss_mib"] = maximum_rss
    if maximum_overhead is not None:
        result["max_capture_overhead_ratio"] = float(maximum_overhead)
    return cast(dict[str, object], result), tuple(failures)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="pr", choices=("pr", "release"))
    parser.add_argument("--case", action="append", dest="cases")
    parser.add_argument("--budgets", type=Path, default=Path(__file__).with_name("budgets.json"))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--worker", nargs=2, metavar=("CASE", "COUNT"), help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.worker:
        print(json.dumps(_worker(args.worker[0], int(args.worker[1])), sort_keys=True))
        return 0

    profile = _load_profile(args.budgets, args.profile)
    selected = args.cases or list(profile)
    unknown = sorted(set(selected) - set(profile))
    if unknown:
        parser.error(f"cases not in {args.profile} profile: {', '.join(unknown)}")
    results = []
    failed = False
    for case in selected:
        result, failures = _run_case(case, profile[case])
        result["passed"] = not failures
        result["failures"] = list(failures)
        results.append(result)
        failed = failed or bool(failures)
        if not args.json:
            status = "PASS" if not failures else "FAIL"
            print(
                f"{status} {case}: {result['count']} records, {result['seconds']}s, "
                f"{result['peak_rss_mib']} MiB RSS"
            )
            for failure in failures:
                print(f"  {failure}")
    if args.json:
        print(json.dumps({"profile": args.profile, "results": results}, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
