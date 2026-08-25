from __future__ import annotations

import json
import signal
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Protocol

import pytest

import runtime_tools.capture as capture_module
import runtime_tools.deep_profile as deep_profile_module
from runtime_tools import CaptureError, record_process
from runtime_tools import _deep_profile_bootstrap as deep_profile_bootstrap
from runtime_tools import _sampling_profile_bootstrap as sampling_profile_bootstrap
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.deep_profile import (
    DeepProfileError,
    DeepProfileSession,
    load_deep_profile,
    load_sample_profile,
    prepare_sample_profile_session,
)
from runtime_tools.proofline.verify import verify_contracts
from runtime_tools.rundiff.compare import compare_runpacks
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import RunpackReader


class _SnapshotBootstrap(Protocol):
    _SOCKET_ENV: str
    _MAX_REPORT_BYTES: int

    def _send_to_collector(
        self,
        encoded: bytes,
        serialization_ns: int,
        snapshot_kind: int,
    ) -> bool: ...


def test_deep_capture_profiles_python_calls_without_workload_instrumentation(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "opaque_workload.py"
    workload.write_text(
        """
import time

def slow_leaf(secret):
    time.sleep(0.003)

def work():
    for _ in range(3):
        slow_leaf("must-not-be-captured")

work()
print("done")
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "deep.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="deep",
        instrument="deep",
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        events = {event.name: event for event in reader.events()}
        edges = reader.causal_edges()
    assert exit_code == 0
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    assert instrumentation["mode"] == "deep"
    assert instrumentation["intrusive"] is True
    assert instrumentation["status"] == "complete"
    assert instrumentation["process_count"] == 1
    assert isinstance(instrumentation["process_ids"], list)
    assert len(instrumentation["process_ids"]) == 1
    assert instrumentation["truncated"] is False
    observer_integrity = instrumentation["observer_integrity"]
    assert isinstance(observer_integrity, dict)
    assert observer_integrity["status"] == "complete"
    assert observer_integrity["profile_hook_setter_call_count"] == 0
    assert observer_integrity["trace_hook_setter_call_count"] == 0
    snapshot_metrics = instrumentation["snapshot_metrics"]
    assert isinstance(snapshot_metrics, dict)
    assert snapshot_metrics["status"] == "available"
    assert isinstance(snapshot_metrics["message_count"], int)
    assert snapshot_metrics["message_count"] >= 2
    assert isinstance(snapshot_metrics["payload_bytes"], int)
    assert isinstance(snapshot_metrics["max_payload_bytes"], int)
    assert snapshot_metrics["payload_bytes"] >= snapshot_metrics["max_payload_bytes"] > 0
    assert isinstance(snapshot_metrics["serialization_ns"], int)
    assert isinstance(snapshot_metrics["max_serialization_ns"], int)
    assert snapshot_metrics["serialization_ns"] >= snapshot_metrics["max_serialization_ns"] > 0
    publication_metrics = instrumentation["publication_metrics"]
    assert isinstance(publication_metrics, dict)
    assert publication_metrics["status"] == "available"
    assert publication_metrics["fallback_process_count"] == 0
    assert publication_metrics["fallback_process_ids"] == []
    assert publication_metrics["socket_attempted_process_count"] == 0
    normalization_metrics = instrumentation["normalization_metrics"]
    assert isinstance(normalization_metrics, dict)
    assert normalization_metrics["status"] == "available"
    assert isinstance(normalization_metrics["duration_ns"], int)
    assert normalization_metrics["duration_ns"] > 0
    ranking_peak_bytes = normalization_metrics["ranking_database_peak_bytes"]
    ranking_limit_bytes = normalization_metrics["ranking_database_limit_bytes"]
    assert isinstance(ranking_peak_bytes, int) and ranking_peak_bytes > 0
    assert isinstance(ranking_limit_bytes, int)
    assert ranking_limit_bytes == deep_profile_module.MAX_PROFILE_RANKING_BYTES
    assert ranking_peak_bytes <= ranking_limit_bytes

    work = events["__main__.work"]
    leaf = events["__main__.slow_leaf"]
    assert work.kind == "python.call.aggregate"
    assert work.started_at_ns is None
    assert work.finished_at_ns is None
    assert work.attributes["call_count"] == 1
    assert leaf.attributes["call_count"] == 3
    assert leaf.attributes["scope"] == "application"
    leaf_self_seconds = leaf.attributes["self_seconds"]
    leaf_total_seconds = leaf.attributes["total_seconds"]
    leaf_max_seconds = leaf.attributes["max_seconds"]
    assert isinstance(leaf_self_seconds, float) and leaf_self_seconds < 0.009
    assert isinstance(leaf_total_seconds, float) and leaf_total_seconds >= 0.009
    assert isinstance(leaf_max_seconds, float) and leaf_max_seconds >= 0.003
    native_sleep = events["time.sleep"]
    assert native_sleep.attributes["implementation"] == "native"
    assert native_sleep.attributes["filename"] == "<native>"
    assert native_sleep.attributes["call_count"] == 3
    assert native_sleep.attributes["exception_count"] == 0
    native_sleep_self_seconds = native_sleep.attributes["self_seconds"]
    assert isinstance(native_sleep_self_seconds, float) and native_sleep_self_seconds >= 0.009
    assert "must-not-be-captured" not in json.dumps(
        [event.attributes for event in events.values()], sort_keys=True
    )
    assert not any(name.startswith("sitecustomize.") for name in events)
    assert any(
        edge.source_event_id == work.id and edge.target_event_id == leaf.id and edge.kind == "calls"
        for edge in edges
    )
    assert any(
        edge.source_event_id == leaf.id
        and edge.target_event_id == native_sleep.id
        and edge.kind == "calls"
        for edge in edges
    )

    analysis = analyze_runpack(runpack)
    report = render_analysis(analysis, "text")
    assert analysis.deep_profile is not None
    assert analysis.deep_profile.status == "complete"
    assert analysis.deep_profile.publication_metrics.status == "available"
    assert analysis.deep_profile.publication_metrics.fallback_process_count == 0
    assert analysis.deep_profile.normalization_metrics.status == "available"
    assert analysis.deep_profile.normalization_metrics.duration_seconds > 0
    assert analysis.deep_profile.native_call_capture is not None
    assert analysis.deep_profile.native_call_capture.status == "complete"
    assert analysis.deep_profile.native_call_capture.call_count > 3
    assert analysis.deep_profile.native_call_capture.function_count > 0
    assert analysis.deep_profile.observer_integrity is not None
    assert analysis.deep_profile.observer_integrity.status == "complete"
    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "observed"
    analyzed_sleep = next(
        hotspot for hotspot in analysis.python_hotspots if hotspot.name == "time.sleep"
    )
    assert analyzed_sleep.implementation == "native"
    assert analyzed_sleep.self_seconds >= 0.009
    assert "Deep capture" in report
    assert "intrusive; timings include profiler overhead" in report
    assert "controller normalization:" in report
    assert "observer integrity complete: no tracing-hook setter calls detected" in report
    assert "Python hotspots" in report
    assert "native arguments, return values, and exception messages omitted" in report
    assert "__main__.slow_leaf" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    assert json_report["deep_profile"]["status"] == "complete"
    assert json_report["deep_profile"]["snapshot_metrics"]["status"] == "available"
    assert json_report["deep_profile"]["normalization_metrics"]["status"] == "available"
    assert any(
        hotspot["name"] == "time.sleep" and hotspot["implementation"] == "native"
        for hotspot in json_report["python_hotspots"]
    )


def test_deep_capture_counts_python_exception_events_without_exception_payloads(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "python_exceptions.py"
    workload.write_text(
        """
import threading

SECRET = "python-exception-secret-must-not-be-captured"

def churn(count):
    for _ in range(count):
        try:
            raise RuntimeError(SECRET)
        except RuntimeError:
            pass

thread = threading.Thread(target=churn, args=(3,))
thread.start()
churn(5)
thread.join()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "python-exceptions.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="python-exceptions",
        capture_level="deep",
    )

    assert exit_code == 0
    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        events = {event.name: event for event in reader.events()}
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    exception_capture = instrumentation["python_exception_capture"]
    assert isinstance(exception_capture, dict)
    assert exception_capture["enabled"] is True
    assert exception_capture["deep_only"] is True
    assert exception_capture["event_semantics"] == "per_propagated_frame"
    exception_event_count = exception_capture["event_count"]
    exception_function_count = exception_capture["function_count"]
    assert isinstance(exception_event_count, int) and exception_event_count >= 8
    assert isinstance(exception_function_count, int) and exception_function_count >= 1
    assert exception_capture["dropped_event_count"] == 0
    control_flow_filter = exception_capture["control_flow_filter"]
    assert isinstance(control_flow_filter, dict)
    assert control_flow_filter["format_version"] == 1
    assert control_flow_filter["status"] == "complete"
    assert control_flow_filter["event_semantics"] == "exact_type_identity"
    assert control_flow_filter["filtered_exception_types"] == [
        "GeneratorExit",
        "StopAsyncIteration",
        "StopIteration",
    ]
    assert control_flow_filter["exception_type_identity_inspected"] is True
    assert control_flow_filter["exception_types_captured"] is False
    non_control_flow_event_count = control_flow_filter["non_control_flow_event_count"]
    assert isinstance(non_control_flow_event_count, int)
    assert non_control_flow_event_count >= 8
    assert control_flow_filter["dropped_non_control_flow_event_count"] == 0
    for key in (
        "arguments_captured",
        "locals_captured",
        "exception_types_captured",
        "exception_values_captured",
        "exception_messages_captured",
        "tracebacks_captured",
        "line_events_enabled",
        "opcode_events_enabled",
    ):
        assert exception_capture[key] is False

    churn = events["__main__.churn"]
    assert churn.attributes["implementation"] == "python"
    assert churn.attributes["call_count"] == 2
    assert churn.attributes["exception_count"] == 8
    processes = churn.attributes["processes"]
    assert isinstance(processes, list)
    assert len(processes) == 1
    process = processes[0]
    assert isinstance(process, dict)
    assert process["exception_count"] == 8
    assert process["non_control_flow_exception_count"] == 8
    assert b"python-exception-secret-must-not-be-captured" not in runpack.read_bytes()

    analysis = analyze_runpack(runpack)
    assert analysis.deep_profile is not None
    python_exceptions = analysis.deep_profile.python_exception_capture
    assert python_exceptions is not None
    assert python_exceptions.status == "complete"
    assert python_exceptions.event_count >= 8
    assert python_exceptions.control_flow_filter is not None
    assert python_exceptions.control_flow_filter.non_control_flow_event_count >= 8
    analyzed_churn = next(
        hotspot for hotspot in analysis.python_hotspots if hotspot.name == "__main__.churn"
    )
    assert analyzed_churn.call_count == 2
    assert analyzed_churn.exception_count == 8
    assert analyzed_churn.non_control_flow_exception_count == 8
    assert not any(
        finding.classification == "python_exception_churn" for finding in analysis.bottlenecks
    )
    report = render_analysis(analysis, "text")
    assert "Python exceptions:" in report
    assert "propagation events" in report
    assert "type identity inspected only for control-flow filtering" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    assert json_report["deep_profile"]["python_exception_capture"]["event_count"] >= 8


def test_deep_capture_filters_normal_async_completion_from_exception_diagnosis(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "async_completion.py"
    workload.write_text(
        """
import asyncio

async def step():
    await asyncio.sleep(0)
    return 1

async def main():
    total = 0
    for _ in range(20):
        total += await step()
    print(total)

asyncio.run(main())
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "async-completion.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="async-completion",
        capture_level="deep",
    )

    assert exit_code == 0
    analysis = analyze_runpack(runpack)
    assert analysis.deep_profile is not None
    exception_capture = analysis.deep_profile.python_exception_capture
    assert exception_capture is not None
    assert exception_capture.status == "complete"
    assert exception_capture.control_flow_filter is not None
    assert exception_capture.control_flow_filter.status == "complete"
    assert exception_capture.control_flow_filter.filtered_event_count >= 40
    main = next(hotspot for hotspot in analysis.python_hotspots if hotspot.name == "__main__.main")
    step = next(hotspot for hotspot in analysis.python_hotspots if hotspot.name == "__main__.step")
    assert main.exception_count == 20
    assert main.non_control_flow_exception_count == 0
    assert step.exception_count == 20
    assert step.non_control_flow_exception_count == 0
    assert not any(
        finding.classification == "python_exception_churn" for finding in analysis.bottlenecks
    )
    report = render_analysis(analysis, "text")
    assert "built-in iterator-control events filtered" in report
    assert "type identity inspected only for control-flow filtering" in report


def test_deep_capture_diagnoses_real_async_exceptions_after_control_flow_filtering(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "async_exceptions.py"
    workload.write_text(
        """
import asyncio

async def retry():
    for _ in range(20):
        try:
            raise RuntimeError("async-exception-secret-must-not-be-captured")
        except RuntimeError:
            await asyncio.sleep(0)

asyncio.run(retry())
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "async-exceptions.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="async-exceptions",
        capture_level="deep",
    )

    assert exit_code == 0
    analysis = analyze_runpack(runpack)
    retry = next(
        hotspot for hotspot in analysis.python_hotspots if hotspot.name == "__main__.retry"
    )
    assert retry.exception_count == 40
    assert retry.non_control_flow_exception_count == 20
    finding = next(
        item for item in analysis.bottlenecks if item.classification == "python_exception_churn"
    )
    assert finding.evidence == (
        "__main__.retry recorded 20 non-control-flow Python exception propagation events "
        "across 21 calls (0.95 per call); built-in iterator completion is excluded and "
        "events are not unique failures"
    )
    assert b"async-exception-secret-must-not-be-captured" not in runpack.read_bytes()


@pytest.mark.parametrize(
    ("source", "profile_overrides", "trace_overrides", "profile_status"),
    (
        (
            """
import sys

def replacement(frame, event, argument):
    hidden = "trace-hook-value-must-not-be-captured"
    return replacement

sys.settrace(replacement)
sys.settrace(None)
""",
            0,
            2,
            "complete",
        ),
        (
            """
import sys

def replacement(frame, event, argument):
    hidden = "profile-hook-value-must-not-be-captured"

sys.setprofile(replacement)
sys.setprofile(None)
""",
            1,
            0,
            "truncated",
        ),
        (
            """
import threading

def profile_replacement(frame, event, argument):
    hidden = "thread-profile-hook-value-must-not-be-captured"

def trace_replacement(frame, event, argument):
    hidden = "thread-trace-hook-value-must-not-be-captured"
    return trace_replacement

threading.setprofile(profile_replacement)
threading.settrace(trace_replacement)
worker = threading.Thread(target=lambda: None)
worker.start()
worker.join()
""",
            1,
            1,
            "truncated",
        ),
    ),
)
def test_deep_capture_reports_tracing_hook_replacement(
    tmp_path: Path,
    source: str,
    profile_overrides: int,
    trace_overrides: int,
    profile_status: str,
) -> None:
    workload = tmp_path / "replace_observer.py"
    workload.write_text(source.strip(), encoding="utf-8")
    runpack = tmp_path / "replace-observer.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="replace-observer",
        capture_level="deep",
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    integrity = instrumentation["observer_integrity"]
    assert isinstance(integrity, dict)
    assert exit_code == 0
    assert instrumentation["status"] == profile_status
    assert integrity == {
        "format_version": 1,
        "status": "partial",
        "process_count": 1,
        "missing_process_count": 0,
        "profile_hook_setter_call_count": profile_overrides,
        "profile_hook_setter_process_count": int(profile_overrides > 0),
        "trace_hook_setter_call_count": trace_overrides,
        "trace_hook_setter_process_count": int(trace_overrides > 0),
        "arguments_captured": False,
        "locals_captured": False,
        "hook_values_captured": False,
    }
    assert b"hook-value-must-not-be-captured" not in runpack.read_bytes()

    analysis = analyze_runpack(runpack)
    assert analysis.deep_profile is not None
    analyzed_integrity = analysis.deep_profile.observer_integrity
    assert analyzed_integrity is not None
    assert analyzed_integrity.status == "partial"
    assert analyzed_integrity.profile_hook_setter_call_count == profile_overrides
    assert analyzed_integrity.trace_hook_setter_call_count == trace_overrides
    report = render_analysis(analysis, "text")
    if profile_overrides:
        assert "profile hook setter called" in report
        assert "exact call and caller evidence may be incomplete" in report
    if trace_overrides:
        assert "trace hook setter called" in report
        assert "Python exception evidence may be incomplete" in report
        assert analysis.deep_profile.python_exception_capture is not None
        assert analysis.deep_profile.python_exception_capture.status == "partial"
        control_flow_filter = analysis.deep_profile.python_exception_capture.control_flow_filter
        assert control_flow_filter is not None
        assert control_flow_filter.status == "partial"


def test_deep_capture_observes_native_clients_and_io_without_adapters(tmp_path: Path) -> None:
    workload = Path(__file__).parents[1] / "examples" / "local" / "native_calls.py"
    runpack = tmp_path / "native-calls.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="native-calls",
        capture_level="deep",
    )
    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        events = {event.name: event for event in reader.events()}
        edges = reader.causal_edges()
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    native_capture = instrumentation["native_call_capture"]
    assert isinstance(native_capture, dict)
    assert native_capture["enabled"] is True
    assert native_capture["deep_only"] is True
    native_function_count = native_capture["function_count"]
    native_call_count = native_capture["call_count"]
    native_exception_count = native_capture["exception_count"]
    assert isinstance(native_function_count, int) and native_function_count > 0
    assert isinstance(native_call_count, int) and native_call_count > 12
    assert isinstance(native_exception_count, int) and native_exception_count >= 1
    assert native_capture["arguments_captured"] is False
    assert native_capture["return_values_captured"] is False
    assert native_capture["exception_messages_captured"] is False

    database_execute = events["sqlite3.Connection.execute"]
    native_sleep = events["time.sleep"]
    native_compress = events["zlib.compress"]
    native_file_write = events["_io.BufferedRandom.write"]
    assert database_execute.attributes["implementation"] == "native"
    assert database_execute.attributes["call_count"] == 4
    assert database_execute.attributes["exception_count"] == 1
    native_sleep_seconds = native_sleep.attributes["self_seconds"]
    assert isinstance(native_sleep_seconds, float) and native_sleep_seconds >= 0.04
    assert native_compress.attributes["call_count"] == 1
    assert native_file_write.attributes["call_count"] == 1
    database_caller = events["__main__.run_database"]
    assert any(
        edge.source_event_id == database_caller.id
        and edge.target_event_id == database_execute.id
        and edge.kind == "calls"
        for edge in edges
    )

    assert analysis.deep_profile is not None
    assert analysis.deep_profile.native_call_capture is not None
    assert analysis.deep_profile.native_call_capture.status == "complete"
    assert analysis.deep_profile.native_call_capture.exception_count >= 1
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.operation_count == 0
    assert "observes every Python and native C call" in render_analysis(analysis, "text")
    runpack_bytes = runpack.read_bytes()
    for secret in (
        b"private_native_records",
        b"native-database-secret",
        b"hidden_native_column",
        b"native-file-secret",
        b"native-compression-secret",
    ):
        assert secret not in runpack_bytes


def test_sampling_capture_estimates_python_hotspots_without_workload_instrumentation(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "opaque_sampled_workload.py"
    workload.write_text(
        """
import time

def slow_leaf(secret):
    time.sleep(0.12)

def work():
    slow_leaf("must-not-be-captured")

work()
print("done")
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "sample.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="sample",
        instrument="sample",
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        events = {event.name: event for event in reader.events()}
        edges = reader.causal_edges()
    assert exit_code == 0
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    assert instrumentation["mode"] == "sample"
    assert instrumentation["observer"] == "python-stack-sampler"
    assert instrumentation["intrusive"] is True
    assert instrumentation["estimated"] is True
    assert instrumentation["per_call"] is False
    assert instrumentation["status"] == "complete"
    assert instrumentation["process_count"] == 1
    assert isinstance(instrumentation["process_ids"], list)
    assert len(instrumentation["process_ids"]) == 1
    assert isinstance(instrumentation["sample_count"], int)
    assert instrumentation["sample_count"] >= 3
    assert instrumentation["interval_ns"] == 10_000_000

    work = events["__main__.work"]
    leaf = events["__main__.slow_leaf"]
    assert work.kind == "python.stack.sample"
    assert work.started_at_ns is None
    leaf_sample_count = leaf.attributes["leaf_sample_count"]
    assert isinstance(leaf_sample_count, int) and leaf_sample_count >= 3
    assert leaf.attributes["scope"] == "application"
    assert "call_count" not in leaf.attributes
    assert "must-not-be-captured" not in json.dumps(
        [event.attributes for event in events.values()], sort_keys=True
    )
    assert any(
        edge.source_event_id == work.id
        and edge.target_event_id == leaf.id
        and edge.kind == "stack_parent"
        for edge in edges
    )

    analysis = analyze_runpack(runpack)
    report = render_analysis(analysis, "text")
    assert analysis.sample_profile is not None
    assert analysis.sample_profile.status == "complete"
    assert analysis.sample_profile.normalization_metrics.status == "available"
    assert analysis.sample_profile.normalization_metrics.duration_seconds > 0
    assert analysis.sample_profile.snapshot_metrics.status == "available"
    assert analysis.sample_profile.snapshot_metrics.checkpoint_message_count >= 1
    assert analysis.sample_profile.snapshot_metrics.max_checkpoint_serialization_seconds > 0
    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "observed"
    assert analysis.python_sample_hotspots[0].name == "__main__.slow_leaf"
    assert "Sampling capture" in report
    assert "statistical estimates; timings include sampler overhead" in report
    assert "controller normalization:" in report
    assert "Sampled Python hotspots" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    assert json_report["sample_profile"]["status"] == "complete"
    assert json_report["python_sample_hotspots"][0]["name"] == "__main__.slow_leaf"


@pytest.mark.parametrize("capture_level", ("sample", "deep"))
def test_zero_code_capture_records_privacy_bounded_subprocess_boundaries(
    tmp_path: Path,
    capture_level: str,
) -> None:
    workload = tmp_path / f"semantic-{capture_level}.py"
    workload.write_text(
        """
import subprocess
import sys

subprocess.run(
    [sys.executable, "-c", "import time; time.sleep(0.02)", "must-not-be-captured"],
    check=True,
)
failed = subprocess.run(
    [sys.executable, "-c", "raise SystemExit(7)", "must-not-be-captured-either"],
    check=False,
)
assert failed.returncode == 7
subprocess.run("exit 0", shell=True, check=True)
try:
    subprocess.run(["/definitely-missing-contrail"], check=True)
except OSError:
    pass
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / f"semantic-{capture_level}.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name=f"semantic-{capture_level}",
        capture_level=capture_level,
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        all_events = reader.events()
        operation_errors = reader.operation_error_counts()
    subprocess_events = tuple(event for event in all_events if event.kind == "subprocess.run")
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    semantic = instrumentation["semantic_capture"]
    assert isinstance(semantic, dict)
    assert exit_code == 0
    assert semantic == {
        "status": "complete",
        "observer": "python-subprocess-wrapper",
        "zero_code": True,
        "process_count": semantic["process_count"],
        "subprocess_count": 4,
        "dropped_subprocess_count": 0,
        "callback_error_count": 0,
        "arguments_captured": False,
        "environment_captured": False,
        "working_directory_captured": False,
        "caller_attribution": semantic["caller_attribution"],
        "limits": {
            "max_subprocesses_per_process": 256,
            "max_subprocess_events": 2_000,
        },
    }
    assert isinstance(semantic["process_count"], int) and semantic["process_count"] >= 1
    caller_attribution = semantic["caller_attribution"]
    assert isinstance(caller_attribution, dict)
    assert caller_attribution["arguments_captured"] is False
    assert caller_attribution["locals_captured"] is False
    assert caller_attribution["callback_error_count"] == 0
    assert caller_attribution["invalid_caller_count"] == 0
    attributed_count = caller_attribution["attributed_subprocess_count"]
    unattributed_count = caller_attribution["unattributed_subprocess_count"]
    assert isinstance(attributed_count, int) and not isinstance(attributed_count, bool)
    assert isinstance(unattributed_count, int) and not isinstance(unattributed_count, bool)
    assert attributed_count + unattributed_count == 4
    if capture_level == "deep":
        assert caller_attribution["status"] == "complete"
        assert attributed_count == 4
    else:
        assert caller_attribution["status"] in {"complete", "partial"}
    expected_profile_kind = (
        "python.stack.sample" if capture_level == "sample" else "python.call.aggregate"
    )
    assert instrumentation["function_count"] == sum(
        event.kind == expected_profile_kind for event in all_events
    )
    assert {event.name for event in subprocess_events} == {
        Path(sys.executable).name,
        "<shell>",
        "definitely-missing-contrail",
    }
    successful = next(
        event
        for event in subprocess_events
        if event.name == Path(sys.executable).name and event.attributes.get("exit_code") == 0
    )
    assert successful.started_at_ns is not None
    assert successful.finished_at_ns is not None
    assert successful.finished_at_ns > successful.started_at_ns
    assert successful.attributes["exit_code"] == 0
    assert successful.attributes["outcome"] == "exited"
    failed = next(
        event for event in subprocess_events if event.name == "definitely-missing-contrail"
    )
    assert failed.attributes["outcome"] == "launch_error"
    assert failed.attributes["error"] is True
    assert failed.attributes["error.type"] == "FileNotFoundError"
    failed_exit = next(
        event
        for event in subprocess_events
        if event.name == Path(sys.executable).name and event.attributes.get("exit_code") == 7
    )
    assert failed_exit.attributes["error"] is True
    assert failed_exit.attributes["error.type"] == "subprocess_exit"
    assert (
        sum(
            count
            for (_, _, operation_kind, _), count in operation_errors.items()
            if operation_kind == "subprocess.run"
        )
        == 2
    )
    encoded_subprocess_evidence = json.dumps(
        [event.attributes for event in subprocess_events],
        sort_keys=True,
    )
    assert "must-not-be-captured" not in encoded_subprocess_evidence

    analysis = analyze_runpack(runpack)
    report = render_analysis(analysis, "text")
    assert analysis.semantic_capture is not None
    assert analysis.semantic_capture.status == "complete"
    assert len(analysis.subprocess_calls) == 4
    assert analysis.semantic_capture.caller_attribution_status == caller_attribution["status"]
    assert "Automatic subprocess capture" in report
    assert "arguments, environment, and cwd omitted" in report
    assert "definitely-missing-contrail" in report
    assert "exit 7" in report
    assert analysis.critical_path is not None
    assert not any(
        name in analysis.critical_path.event_names
        for name in {"<shell>", "definitely-missing-contrail"}
    )


def test_semantic_subprocess_evidence_flows_into_rundiff_and_proofline(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "semantic-baseline.runpack"
    candidate = tmp_path / "semantic-candidate.runpack"
    record_process(
        (sys.executable, "-c", "pass"),
        baseline,
        name="baseline",
        capture_level="sample",
    )
    record_process(
        (
            sys.executable,
            "-c",
            "import subprocess,sys; subprocess.run([sys.executable, '-c', 'pass'], check=True)",
        ),
        candidate,
        name="candidate",
        capture_level="sample",
    )

    diff = compare_runpacks(baseline, candidate)
    operation_name = Path(sys.executable).name
    change = next(
        item
        for item in diff.operation_count_changes
        if item.operation_kind == "subprocess.run" and item.operation_name == operation_name
    )
    assert (change.baseline, change.candidate) == (0, 1)
    assert diff.baseline_semantic_capture_status == "complete"
    assert diff.candidate_semantic_capture_status == "complete"
    contract = tmp_path / "subprocess-contract.yaml"
    contract.write_text(
        "name: subprocess-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        f"    operation: {operation_name}\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )

    verification = verify_contracts(contract, baseline, candidate)

    assert verification.results[0].status == "fail"
    assert verification.results[0].observed == "baseline=1, candidate=2, limit=1"


def _http_request_workload(request_count: int) -> str:
    return f"""
import http.server
import threading
import urllib.request

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(204)
        self.end_headers()

    def log_message(self, *_args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
thread = threading.Thread(target=server.serve_forever)
thread.start()
try:
    for _ in range({request_count}):
        with urllib.request.urlopen(f"http://127.0.0.1:{{server.server_port}}/", timeout=2):
            pass
finally:
    server.shutdown()
    server.server_close()
    thread.join()
""".strip()


def test_semantic_http_evidence_flows_into_rundiff_and_proofline(tmp_path: Path) -> None:
    baseline = tmp_path / "http-baseline.runpack"
    candidate = tmp_path / "http-candidate.runpack"
    record_process(
        (sys.executable, "-c", "pass"),
        baseline,
        name="baseline",
        capture_level="sample",
    )
    record_process(
        (sys.executable, "-c", _http_request_workload(1)),
        candidate,
        name="candidate",
        capture_level="sample",
    )

    diff = compare_runpacks(baseline, candidate)
    change = next(
        item
        for item in diff.operation_count_changes
        if item.operation_kind == "http.client.request" and item.operation_name == "HTTP GET"
    )
    assert (change.baseline, change.candidate) == (0, 1)
    assert diff.baseline_http_capture_status == "complete"
    assert diff.candidate_http_capture_status == "complete"
    contract = tmp_path / "http-contract.yaml"
    contract.write_text(
        "name: http-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: HTTP GET\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )

    verification = verify_contracts(contract, baseline, candidate)

    assert verification.results[0].status == "fail"
    assert verification.results[0].observed == "baseline=0, candidate=1, limit=0"


def test_truncated_http_capture_makes_operation_contract_unverifiable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(deep_profile_module, "MAX_SEMANTIC_HTTP_REQUEST_EVENTS", 1)
    baseline = tmp_path / "bounded-http-baseline.runpack"
    candidate = tmp_path / "bounded-http-candidate.runpack"
    record_process(
        (sys.executable, "-c", _http_request_workload(1)),
        baseline,
        name="baseline",
        capture_level="sample",
    )
    record_process(
        (sys.executable, "-c", _http_request_workload(2)),
        candidate,
        name="candidate",
        capture_level="sample",
    )
    contract = tmp_path / "bounded-http-contract.yaml"
    contract.write_text(
        "name: http-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: HTTP GET\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )

    diff = compare_runpacks(baseline, candidate)
    verification = verify_contracts(contract, baseline, candidate)

    assert diff.candidate_http_capture_status == "truncated"
    assert diff.candidate_dropped_http_request_count == 1
    assert verification.results[0].status == "unverifiable"
    assert verification.results[0].observed == (
        "HTTP evidence incomplete (candidate: HTTP capture truncated, 1 omitted)"
    )


def _network_connection_workload(connection_count: int) -> str:
    return f"""
import socket
import threading

server = socket.socket()
server.bind(("127.0.0.1", 0))
server.listen()

def accept_connections():
    for _ in range({connection_count}):
        peer, _address = server.accept()
        peer.close()

thread = threading.Thread(target=accept_connections)
thread.start()
try:
    for _ in range({connection_count}):
        connection = socket.create_connection(("127.0.0.1", server.getsockname()[1]))
        connection.close()
finally:
    thread.join()
    server.close()
""".strip()


def test_network_connection_evidence_flows_into_rundiff_and_proofline(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "network-baseline.runpack"
    candidate = tmp_path / "network-candidate.runpack"
    record_process(
        (sys.executable, "-c", _network_connection_workload(1)),
        baseline,
        name="baseline",
        capture_level="sample",
    )
    record_process(
        (sys.executable, "-c", _network_connection_workload(2)),
        candidate,
        name="candidate",
        capture_level="sample",
    )

    diff = compare_runpacks(baseline, candidate)
    change = next(
        item
        for item in diff.operation_count_changes
        if item.operation_kind == "network.connect" and item.operation_name == "TCP connect"
    )
    assert (change.baseline, change.candidate) == (1, 2)
    assert diff.baseline_network_capture_status == "complete"
    assert diff.candidate_network_capture_status == "complete"
    contract = tmp_path / "network-contract.yaml"
    contract.write_text(
        "name: connection-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: TCP connect\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )

    verification = verify_contracts(contract, baseline, candidate)

    assert verification.results[0].status == "fail"
    assert verification.results[0].observed == "baseline=1, candidate=2, limit=1"


def test_zero_code_network_capture_surfaces_connection_churn(tmp_path: Path) -> None:
    runpack = tmp_path / "network-churn.runpack"
    record_process(
        (sys.executable, "-c", _network_connection_workload(12)),
        runpack,
        name="network-churn",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)

    finding = next(
        item for item in analysis.bottlenecks if item.classification == "connection_churn"
    )
    assert finding.evidence.startswith("12 outbound connection attempts occurred (")
    assert finding.evidence.endswith("/s); inspect pooling or retry behavior")
    assert finding.confidence == 0.65


def test_zero_code_network_setup_captures_dns_and_sync_async_tls(tmp_path: Path) -> None:
    workload = Path(__file__).parents[1] / "examples" / "local" / "network_setup.py"
    runpack = tmp_path / "network-setup.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="network-setup",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.network_setup_capture is not None
    assert analysis.network_setup_capture.status == "complete"
    assert analysis.network_setup_capture.phase_count == 6
    assert analysis.network_setup_capture.dropped_phase_count == 0
    assert {phase.phase for phase in analysis.network_setup_phases} == {"dns", "tls"}
    assert sum(phase.phase == "dns" for phase in analysis.network_setup_phases) == 2
    assert sum(phase.phase == "tls" for phase in analysis.network_setup_phases) == 4
    assert all(phase.outcome == "completed" for phase in analysis.network_setup_phases)
    assert analysis.network_setup_capture.adapters == (
        "stdlib.socket.getaddrinfo",
        "stdlib.ssl.SSLObject.do_handshake",
        "stdlib.ssl.SSLSocket.do_handshake",
    )
    rendered = render_analysis(analysis, "text")
    encoded = json.dumps(analysis.as_json_value(), sort_keys=True)
    assert "Automatic network setup capture" in rendered
    assert "Network setup hotspots" in rendered
    assert "DNS" in rendered
    assert "TLS" in rendered
    assert "localhost" not in encoded
    assert "BEGIN CERTIFICATE" not in encoded


def test_zero_code_network_setup_classifies_dns_and_tls_failures(tmp_path: Path) -> None:
    workload = tmp_path / "network-setup-failures.py"
    workload.write_text(
        """
import socket
import ssl
import threading

def fail_dns():
    try:
        socket.getaddrinfo(object(), 443)
    except TypeError:
        pass

server = socket.socket()
server.bind(("127.0.0.1", 0))
server.listen()

def serve_plaintext():
    peer, _address = server.accept()
    peer.sendall(b"not tls")
    peer.close()

thread = threading.Thread(target=serve_plaintext)
thread.start()

def fail_tls():
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    raw = socket.create_connection(("127.0.0.1", server.getsockname()[1]))
    try:
        context.wrap_socket(raw, server_hostname="redacted.example")
    except ssl.SSLError:
        pass

fail_dns()
fail_tls()
thread.join()
server.close()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "network-setup-failures.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="network-setup-failures",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.network_setup_capture is not None
    assert analysis.network_setup_capture.status == "complete"
    failures = {
        phase.phase: phase
        for phase in analysis.network_setup_phases
        if phase.outcome == "setup_error"
    }
    assert failures["dns"].error_type == "TypeError"
    assert failures["tls"].error_type in {"SSLError", "SSLEOFError"}
    findings = {finding.classification for finding in analysis.bottlenecks}
    assert {"dns_failures", "tls_failures"} <= findings
    encoded = json.dumps(analysis.as_json_value(), sort_keys=True)
    assert "redacted.example" not in encoded
    assert "not tls" not in encoded


def test_network_setup_capture_is_bounded_before_public_analysis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(deep_profile_module, "MAX_SEMANTIC_NETWORK_SETUP_EVENTS", 1)
    baseline = tmp_path / "bounded-network-setup-baseline.runpack"
    candidate = tmp_path / "bounded-network-setup-candidate.runpack"

    record_process(
        (sys.executable, "-c", "import socket; socket.getaddrinfo('127.0.0.1', 80)"),
        baseline,
        name="bounded-network-setup-baseline",
        capture_level="deep",
    )
    record_process(
        (
            sys.executable,
            "-c",
            "import socket; socket.getaddrinfo('127.0.0.1', 80); "
            "socket.getaddrinfo('127.0.0.1', 443)",
        ),
        candidate,
        name="bounded-network-setup-candidate",
        capture_level="deep",
    )
    analysis = analyze_runpack(candidate)
    contract = tmp_path / "network-setup-contract.yaml"
    contract.write_text(
        "name: dns-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: DNS resolution\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )
    diff = compare_runpacks(baseline, candidate)
    verification = verify_contracts(contract, baseline, candidate)

    assert analysis.network_setup_capture is not None
    assert analysis.network_setup_capture.status == "truncated"
    assert analysis.network_setup_capture.phase_count == 1
    assert analysis.network_setup_capture.dropped_phase_count == 1
    assert len(analysis.network_setup_phases) == 1
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "complete"
    assert diff.baseline_network_setup_capture_status == "complete"
    assert diff.candidate_network_setup_capture_status == "truncated"
    assert diff.candidate_dropped_network_setup_phase_count == 1
    assert verification.results[0].status == "unverifiable"
    assert verification.results[0].observed == (
        "network setup evidence incomplete (candidate: network setup capture truncated, 1 omitted)"
    )


def test_zero_code_deep_capture_observes_sqlite_and_queue_operations(tmp_path: Path) -> None:
    workload = Path(__file__).parents[1] / "examples" / "local" / "logical_operations.py"
    runpack = tmp_path / "logical-operations.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="logical-operations",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    assert analysis.logical_operation_capture.operation_count == 10
    assert analysis.logical_operation_capture.dropped_operation_count == 0
    assert analysis.logical_operation_capture.caller_attribution_status == "complete"
    assert analysis.logical_operation_capture.attributed_operation_count == 10
    assert analysis.logical_operation_capture.adapters == (
        "stdlib.asyncio.Queue",
        "stdlib.asyncio.TaskGroup",
        "stdlib.asyncio.create_task",
        "stdlib.asyncio.ensure_future",
        "stdlib.asyncio.gather",
        "stdlib.queue.Queue",
        "stdlib.sqlite3.Connection",
        "stdlib.sqlite3.Cursor",
    )
    assert sum(operation.category == "database" for operation in analysis.logical_operations) == 5
    assert sum(operation.category == "queue" for operation in analysis.logical_operations) == 4
    assert sum(operation.category == "scheduler" for operation in analysis.logical_operations) == 1
    failed = [
        operation
        for operation in analysis.logical_operations
        if operation.outcome == "operation_error"
    ]
    assert len(failed) == 1
    assert failed[0].operation == "execute"
    assert failed[0].error_type == "OperationalError"
    assert all(operation.caller is not None for operation in analysis.logical_operations)
    findings = {finding.classification for finding in analysis.bottlenecks}
    assert {"database_operation_failures", "queue_operation_latency"} <= findings
    rendered = render_analysis(analysis, "text")
    assert "Automatic logical operation capture" in rendered
    assert "Logical operation hotspots" in rendered
    assert "10 operations across 1 Python process" in rendered
    runpack_bytes = runpack.read_bytes()
    for secret in (
        b"private_records",
        b"secret_missing_column",
        b"sensitive-row-one",
        b"sensitive-queue-item",
        b"sensitive-async-item",
    ):
        assert secret not in runpack_bytes


def test_zero_code_deep_capture_observes_executor_tasks_across_products(
    tmp_path: Path,
) -> None:
    workload = Path(__file__).parents[1] / "examples" / "local" / "executor_tasks.py"
    passive_output = tmp_path / "passive.json"
    deep_output = tmp_path / "deep.json"
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload), "--output", str(passive_output)),
        tmp_path / "passive.runpack",
        name="executor-passive",
        capture_level="passive",
    )
    baseline_exit = record_process(
        (sys.executable, str(workload), "--output", str(deep_output)),
        baseline,
        name="executor-baseline",
        capture_level="deep",
    )
    candidate_exit = record_process(
        (
            sys.executable,
            str(workload),
            "--extra-thread-tasks",
            "2",
            "--output",
            str(tmp_path / "candidate.json"),
        ),
        candidate,
        name="executor-candidate",
        capture_level="deep",
    )

    assert passive_exit == baseline_exit == candidate_exit == 0
    assert passive_output.read_text(encoding="utf-8") == deep_output.read_text(encoding="utf-8")
    analysis = analyze_runpack(baseline)
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    executor_tasks = tuple(
        operation
        for operation in analysis.logical_operations
        if operation.category == "executor" and operation.operation == "task"
    )
    assert len(executor_tasks) == 5
    assert {operation.adapter for operation in executor_tasks} == {
        "stdlib.concurrent.futures.ProcessPoolExecutor",
        "stdlib.concurrent.futures.ThreadPoolExecutor",
    }
    assert sum(operation.outcome == "completed" for operation in executor_tasks) == 4
    failed = [operation for operation in executor_tasks if operation.outcome == "operation_error"]
    assert len(failed) == 1
    assert failed[0].error_type == "RuntimeError"
    assert all(operation.caller is not None for operation in executor_tasks)
    assert {operation.caller.name for operation in executor_tasks if operation.caller} == {
        "__main__.run_process_pool",
        "__main__.run_thread_pool",
    }
    assert not any(operation.category == "queue" for operation in analysis.logical_operations)
    assert not any(
        finding.classification == "queue_operation_failures" for finding in analysis.bottlenecks
    )
    assert any(
        finding.classification == "executor_operation_failures" for finding in analysis.bottlenecks
    )
    assert "EXECUTOR task" in render_analysis(analysis, "text")

    diff = compare_runpacks(baseline, candidate)
    task_change = next(
        change
        for change in diff.operation_count_changes
        if change.operation_kind == "executor.task" and change.operation_name == "Executor task"
    )
    assert (task_change.baseline, task_change.candidate) == (5, 7)
    contract = tmp_path / "executor-contract.yaml"
    contract.write_text(
        """
name: executor-task-amplification
assertions:
  - type: max_operation_count
    operation: Executor task
    relative_to: baseline
    factor: 1.2
""".strip(),
        encoding="utf-8",
    )
    verification = verify_contracts(contract, baseline, candidate)
    assert verification.results[0].status == "fail"
    assert verification.results[0].observed == "baseline=5, candidate=7, limit=6"

    for secret in (
        b"executor-thread-payload-must-not-be-captured",
        b"executor-process-payload-must-not-be-captured",
        b"executor-thread-error-message-must-not-be-captured",
    ):
        assert secret not in baseline.read_bytes()


def test_zero_code_deep_capture_observes_asyncio_tasks_across_products(
    tmp_path: Path,
) -> None:
    workload = Path(__file__).parents[1] / "examples" / "local" / "async_tasks.py"
    passive_output = tmp_path / "passive.json"
    deep_output = tmp_path / "deep.json"
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload), "--output", str(passive_output)),
        tmp_path / "passive.runpack",
        name="async-task-passive",
        capture_level="passive",
    )
    baseline_exit = record_process(
        (sys.executable, str(workload), "--output", str(deep_output)),
        baseline,
        name="async-task-baseline",
        capture_level="deep",
    )
    candidate_exit = record_process(
        (
            sys.executable,
            str(workload),
            "--extra-tasks",
            "2",
            "--output",
            str(tmp_path / "candidate.json"),
        ),
        candidate,
        name="async-task-candidate",
        capture_level="deep",
    )

    assert passive_exit == baseline_exit == candidate_exit == 0
    assert passive_output.read_text(encoding="utf-8") == deep_output.read_text(encoding="utf-8")
    analysis = analyze_runpack(baseline)
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    scheduled_tasks = tuple(
        operation
        for operation in analysis.logical_operations
        if operation.category == "scheduler" and operation.operation == "task"
    )
    assert len(scheduled_tasks) == 8
    assert {operation.adapter for operation in scheduled_tasks} == {
        "stdlib.asyncio.TaskGroup",
        "stdlib.asyncio.create_task",
        "stdlib.asyncio.ensure_future",
        "stdlib.asyncio.gather",
    }
    assert sum(operation.outcome == "completed" for operation in scheduled_tasks) == 7
    failed = [operation for operation in scheduled_tasks if operation.outcome == "operation_error"]
    assert len(failed) == 1
    assert failed[0].error_type == "RuntimeError"
    assert all(operation.caller is not None for operation in scheduled_tasks)
    assert {operation.caller.name for operation in scheduled_tasks if operation.caller} == {
        "__main__.run_create_tasks",
        "__main__.run_ensure_future",
        "__main__.run_implicit_gather",
        "__main__.run_task_group",
    }
    assert all(
        operation.as_json_value()["duration_boundary"] == "creation_to_completion"
        for operation in scheduled_tasks
    )
    assert any(
        finding.classification == "scheduler_operation_failures" for finding in analysis.bottlenecks
    )
    assert "SCHEDULER task" in render_analysis(analysis, "text")

    diff = compare_runpacks(baseline, candidate)
    task_change = next(
        change
        for change in diff.operation_count_changes
        if change.operation_kind == "scheduler.task" and change.operation_name == "Async task"
    )
    assert (task_change.baseline, task_change.candidate) == (8, 10)
    contract = tmp_path / "async-task-contract.yaml"
    contract.write_text(
        """
name: async-task-amplification
assertions:
  - type: max_operation_count
    operation: Async task
    relative_to: baseline
    factor: 1.2
""".strip(),
        encoding="utf-8",
    )
    verification = verify_contracts(contract, baseline, candidate)
    assert verification.results[0].status == "fail"
    assert verification.results[0].observed == "baseline=8, candidate=10, limit=9.6"

    for secret in (
        b"async-task-payload-must-not-be-captured",
        b"task-group-payload-must-not-be-captured",
        b"async-task-name-must-not-be-captured",
        b"task-group-name-must-not-be-captured",
        b"async-task-error-message-must-not-be-captured",
        b"ensure-future-payload-must-not-be-captured",
        b"gather-payload-must-not-be-captured",
    ):
        assert secret not in baseline.read_bytes()


def test_asyncio_task_capture_preserves_unretrieved_exception_warning(tmp_path: Path) -> None:
    workload = tmp_path / "unretrieved-async-task.py"
    workload.write_text(
        """
import asyncio

async def fail():
    raise RuntimeError("unretrieved-task-message")

async def main():
    asyncio.create_task(fail(), name="unretrieved-task-name")
    await asyncio.sleep(0)
    await asyncio.sleep(0)

asyncio.run(main())
""".strip(),
        encoding="utf-8",
    )
    passive = tmp_path / "unretrieved-passive.runpack"
    deep = tmp_path / "unretrieved-deep.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload)),
        passive,
        name="unretrieved-passive",
        capture_level="passive",
        capture_output_limit=8_192,
    )
    deep_exit = record_process(
        (sys.executable, str(workload)),
        deep,
        name="unretrieved-deep",
        capture_level="deep",
        capture_output_limit=8_192,
    )

    assert passive_exit == deep_exit == 0
    for runpack in (passive, deep):
        with RunpackReader(runpack) as reader:
            attachments = {attachment.name: attachment for attachment in reader.attachments()}
        stderr = attachments["stderr"].content
        assert b"Task exception was never retrieved" in stderr
        assert b"unretrieved-task-message" in stderr
    analysis = analyze_runpack(deep)
    task = next(
        operation for operation in analysis.logical_operations if operation.category == "scheduler"
    )
    assert task.outcome == "operation_error"
    assert task.error_type == "RuntimeError"


def test_asyncio_task_capture_observes_public_implicit_scheduling_without_duplicates(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "implicit-async-tasks.py"
    workload.write_text(
        """
import asyncio
import json
import sys

async def work(index, private_payload):
    await asyncio.sleep(0)
    assert private_payload
    return index

async def main():
    explicit = asyncio.create_task(
        work(0, "explicit-task-payload-must-not-be-captured"),
        name="explicit-task-name-must-not-be-captured",
    )
    gathered = await asyncio.gather(
        explicit,
        work(1, "gather-payload-one-must-not-be-captured"),
        work(2, "gather-payload-two-must-not-be-captured"),
    )
    ensured = asyncio.ensure_future(
        work(3, "ensure-future-payload-must-not-be-captured")
    )
    second_explicit = asyncio.create_task(
        work(4, "existing-future-payload-must-not-be-captured")
    )
    assert asyncio.ensure_future(second_explicit) is second_explicit
    return gathered + [await ensured, await second_explicit]

with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(asyncio.run(main()), output)
""".strip(),
        encoding="utf-8",
    )
    passive_output = tmp_path / "implicit-passive.json"
    deep_output = tmp_path / "implicit-deep.json"
    passive = tmp_path / "implicit-passive.runpack"
    deep = tmp_path / "implicit-deep.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload), str(passive_output)),
        passive,
        name="implicit-task-passive",
        capture_level="passive",
    )
    deep_exit = record_process(
        (sys.executable, str(workload), str(deep_output)),
        deep,
        name="implicit-task-deep",
        capture_level="deep",
    )

    assert passive_exit == deep_exit == 0
    assert passive_output.read_bytes() == deep_output.read_bytes()
    analysis = analyze_runpack(deep)
    tasks = tuple(
        operation for operation in analysis.logical_operations if operation.category == "scheduler"
    )
    assert len(tasks) == 5
    assert {
        adapter: sum(task.adapter == adapter for task in tasks)
        for adapter in {task.adapter for task in tasks}
    } == {
        "stdlib.asyncio.create_task": 2,
        "stdlib.asyncio.ensure_future": 1,
        "stdlib.asyncio.gather": 2,
    }
    assert all(task.outcome == "completed" for task in tasks)
    assert all(task.caller is not None for task in tasks)
    assert {task.caller.name for task in tasks if task.caller is not None} == {"__main__.main"}
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.callback_error_count == 0
    runpack_bytes = deep.read_bytes()
    for secret in (
        b"explicit-task-payload-must-not-be-captured",
        b"explicit-task-name-must-not-be-captured",
        b"gather-payload-one-must-not-be-captured",
        b"gather-payload-two-must-not-be-captured",
        b"ensure-future-payload-must-not-be-captured",
        b"existing-future-payload-must-not-be-captured",
    ):
        assert secret not in runpack_bytes


def test_asyncio_task_capture_retains_cancellation_and_rejected_creation_safely(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "cancel-async-tasks.py"
    workload.write_text(
        """
import asyncio

async def wait_forever():
    await asyncio.Event().wait()

async def cancel_task():
    task = asyncio.create_task(wait_forever(), name="cancelled-task-name-must-not-be-captured")
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

asyncio.run(cancel_task())
coroutine = wait_forever()
try:
    asyncio.create_task(coroutine, name="rejected-task-name-must-not-be-captured")
except RuntimeError:
    coroutine.close()
ensured_coroutine = wait_forever()
try:
    asyncio.ensure_future(ensured_coroutine)
except RuntimeError:
    ensured_coroutine.close()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "cancel-async-tasks.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="cancel-async-tasks",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)
    tasks = tuple(
        operation for operation in analysis.logical_operations if operation.category == "scheduler"
    )

    assert exit_code == 0
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    assert len(tasks) == 3
    assert {task.error_type for task in tasks} == {"CancelledError", "RuntimeError"}
    assert {task.adapter for task in tasks} == {
        "stdlib.asyncio.create_task",
        "stdlib.asyncio.ensure_future",
    }
    assert all(task.outcome == "operation_error" for task in tasks)
    assert all(task.duration_seconds is not None for task in tasks)
    assert all(task.caller is not None for task in tasks)
    runpack_bytes = runpack.read_bytes()
    assert b"cancelled-task-name-must-not-be-captured" not in runpack_bytes
    assert b"rejected-task-name-must-not-be-captured" not in runpack_bytes


def test_zero_code_deep_capture_observes_wsgi_server_requests_across_products(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "wsgi-server.py"
    workload.write_text(
        """
import argparse
import http.client
import json
import threading
import time
from wsgiref.simple_server import WSGIRequestHandler, make_server

class QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *arguments):
        pass

def application(environ, start_response):
    path = environ["PATH_INFO"]
    if path == "/private-failure-path":
        time.sleep(0.06)
        start_response(
            "503 Service Unavailable",
            [("X-Private-Response-Header", "private-response-header-value")],
        )
        return [b"private-failure-response-body"]
    start_response(
        "200 OK",
        [("X-Private-Response-Header", "private-response-header-value")],
    )
    return [b"private-success-response-body"]

parser = argparse.ArgumentParser()
parser.add_argument("output")
parser.add_argument("--extra-requests", type=int, default=0)
args = parser.parse_args()
server = make_server("127.0.0.1", 0, application, handler_class=QuietHandler)
request_count = 2 + args.extra_requests

def serve():
    for _ in range(request_count):
        server.handle_request()

thread = threading.Thread(target=serve)
thread.start()
connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
statuses = []
for path in ["/private-success-path", "/private-failure-path"] + [
    "/private-extra-path"
] * args.extra_requests:
    connection.request("GET", path, headers={"X-Private-Request": "request-secret"})
    response = connection.getresponse()
    statuses.append(response.status)
    response.read()
connection.close()
thread.join()
server.server_close()
with open(args.output, "w", encoding="utf-8") as output:
    json.dump(statuses, output)
""".strip(),
        encoding="utf-8",
    )
    passive_output = tmp_path / "wsgi-passive.json"
    deep_output = tmp_path / "wsgi-deep.json"
    passive = tmp_path / "wsgi-passive.runpack"
    baseline = tmp_path / "wsgi-baseline.runpack"
    candidate = tmp_path / "wsgi-candidate.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload), str(passive_output)),
        passive,
        name="wsgi-passive",
        capture_level="passive",
    )
    baseline_exit = record_process(
        (sys.executable, str(workload), str(deep_output)),
        baseline,
        name="wsgi-baseline",
        capture_level="deep",
    )
    candidate_exit = record_process(
        (
            sys.executable,
            str(workload),
            str(tmp_path / "wsgi-candidate.json"),
            "--extra-requests",
            "1",
        ),
        candidate,
        name="wsgi-candidate",
        capture_level="deep",
    )

    assert passive_exit == baseline_exit == candidate_exit == 0
    assert passive_output.read_bytes() == deep_output.read_bytes()
    analysis = analyze_runpack(baseline)
    requests = tuple(
        operation
        for operation in analysis.logical_operations
        if operation.category == "server" and operation.operation == "request"
    )
    assert len(requests) == 2
    assert {request.adapter for request in requests} == {"stdlib.wsgiref"}
    assert {request.outcome for request in requests} == {"completed", "operation_error"}
    assert {request.error_type for request in requests} == {None, "HTTPStatusError"}
    assert {request.as_json_value().get("status_code") for request in requests} == {200, 503}
    assert all(request.caller is not None for request in requests)
    assert {request.caller.name for request in requests if request.caller is not None} == {
        "__main__.application"
    }
    application_hotspot = next(
        hotspot for hotspot in analysis.python_hotspots if hotspot.name == "__main__.application"
    )
    assert application_hotspot.call_count == 2
    assert any(
        finding.classification == "server_operation_failures" for finding in analysis.bottlenecks
    )
    assert "SERVER request" in render_analysis(analysis, "text")

    diff = compare_runpacks(baseline, candidate)
    request_change = next(
        change
        for change in diff.operation_count_changes
        if change.operation_kind == "server.request"
        and change.operation_name == "Inbound HTTP request"
    )
    assert (request_change.baseline, request_change.candidate) == (2, 3)
    contract = tmp_path / "wsgi-request-contract.yaml"
    contract.write_text(
        """
name: inbound-request-amplification
assertions:
  - type: max_operation_count
    operation: Inbound HTTP request
    relative_to: baseline
    factor: 1.0
""".strip(),
        encoding="utf-8",
    )
    verification = verify_contracts(contract, baseline, candidate)
    assert verification.results[0].status == "fail"
    assert verification.results[0].observed == "baseline=2, candidate=3, limit=2"

    runpack_bytes = baseline.read_bytes()
    for secret in (
        b"private-success-path",
        b"private-failure-path",
        b"private-response-header-value",
        b"private-failure-response-body",
        b"private-success-response-body",
        b"request-secret",
    ):
        assert secret not in runpack_bytes


def _write_optional_logical_client_fakes(root: Path) -> None:
    files = {
        "sqlalchemy/__init__.py": "",
        "sqlalchemy/engine/__init__.py": "",
        "sqlalchemy/engine/base.py": """
import time

class Connection:
    def execute(self, statement, parameters=None):
        time.sleep(0.002)
        if statement == "sqlalchemy-secret-failure":
            raise RuntimeError("sqlalchemy-secret-error-message")
        return "sqlalchemy-secret-result"

    def exec_driver_sql(self, statement, parameters=None):
        time.sleep(0.002)
        return "sqlalchemy-secret-driver-result"

    def commit(self):
        time.sleep(0.002)

    def rollback(self):
        time.sleep(0.002)
""",
        "sqlalchemy/orm/__init__.py": "",
        "sqlalchemy/orm/session.py": """
from sqlalchemy.engine.base import Connection

class Session:
    def __init__(self):
        self.connection = Connection()

    def execute(self, statement, parameters=None):
        return self.connection.execute(statement, parameters)

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()
""",
        "sqlalchemy/ext/__init__.py": "",
        "sqlalchemy/ext/asyncio/__init__.py": "",
        "sqlalchemy/ext/asyncio/engine.py": """
import asyncio

class AsyncConnection:
    async def execute(self, statement, parameters=None):
        await asyncio.sleep(0.002)
        if statement == "sqlalchemy-async-secret-failure":
            raise RuntimeError("sqlalchemy-async-secret-error-message")
        return "sqlalchemy-async-secret-result"

    async def exec_driver_sql(self, statement, parameters=None):
        await asyncio.sleep(0.002)
        return "sqlalchemy-async-secret-driver-result"

    async def commit(self):
        await asyncio.sleep(0.002)

    async def rollback(self):
        await asyncio.sleep(0.002)
""",
        "sqlalchemy/ext/asyncio/session.py": """
from sqlalchemy.ext.asyncio.engine import AsyncConnection

class AsyncSession:
    def __init__(self):
        self.connection = AsyncConnection()

    async def execute(self, statement, parameters=None):
        return await self.connection.execute(statement, parameters)

    async def commit(self):
        return await self.connection.commit()

    async def rollback(self):
        return await self.connection.rollback()
""",
        "redis/__init__.py": "from redis.client import Pipeline, Redis\n",
        "redis/client.py": """
import time

class Redis:
    def execute_command(self, command, *arguments, **options):
        time.sleep(0.002)
        if command == "REDIS-SECRET-FAIL":
            raise TimeoutError("redis-secret-error-message")
        return b"redis-secret-result"

class Pipeline:
    def execute(self, raise_on_error=True):
        return Redis().execute_command("REDIS-SECRET-NESTED")
""",
        "redis/asyncio/__init__.py": "from redis.asyncio.client import Pipeline, Redis\n",
        "redis/asyncio/client.py": """
import asyncio

class Redis:
    async def execute_command(self, command, *arguments, **options):
        await asyncio.sleep(0.002)
        if command == "REDIS-ASYNC-SECRET-FAIL":
            raise TimeoutError("redis-async-secret-error-message")
        return b"redis-async-secret-result"

class Pipeline:
    async def execute(self, raise_on_error=True):
        return await Redis().execute_command("REDIS-ASYNC-SECRET-NESTED")
""",
        "pika/__init__.py": "",
        "pika/adapters/__init__.py": "",
        "pika/adapters/blocking_connection.py": """
import time

class BlockingChannel:
    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        time.sleep(0.002)
        if body == b"pika-secret-failure-body":
            raise OSError("pika-secret-error-message")

    def basic_get(self, queue, auto_ack=False):
        time.sleep(0.002)
        return object(), object(), b"pika-secret-result-body"
""",
        "aiokafka/__init__.py": "",
        "aiokafka/producer/__init__.py": "",
        "aiokafka/producer/producer.py": """
import asyncio

class AIOKafkaProducer:
    async def send_and_wait(self, topic, value=None, key=None, partition=None, **options):
        await asyncio.sleep(0.002)
        if value == b"kafka-secret-failure-value":
            raise TimeoutError("kafka-secret-error-message")
        return "kafka-secret-result"
""",
        "aiokafka/consumer/__init__.py": "",
        "aiokafka/consumer/consumer.py": """
import asyncio

class AIOKafkaConsumer:
    async def getone(self, *partitions):
        await asyncio.sleep(0.002)
        return b"kafka-secret-record"

    async def getmany(self, *partitions, timeout_ms=0, max_records=None):
        await asyncio.sleep(0.002)
        return {"kafka-secret-topic": [b"kafka-secret-record"]}
""",
    }
    for relative_path, source in files.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source.strip(), encoding="utf-8")


def test_optional_database_cache_and_broker_adapters_capture_public_client_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()
    _write_optional_logical_client_fakes(site_packages)
    monkeypatch.setenv("PYTHONPATH", str(site_packages))
    workload = tmp_path / "optional-logical-clients.py"
    workload.write_text(
        """
import asyncio
import importlib
import inspect
import json
import sys

from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.producer.producer import AIOKafkaProducer
from pika.adapters.blocking_connection import BlockingChannel
from redis.asyncio.client import Pipeline as AsyncPipeline
from redis.asyncio.client import Redis as AsyncRedis
from redis.client import Pipeline, Redis
from sqlalchemy.engine.base import Connection
from sqlalchemy.ext.asyncio.engine import AsyncConnection
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm.session import Session

def ignore_failure(operation):
    try:
        operation()
    except (OSError, RuntimeError, TimeoutError):
        pass

def exercise_sync():
    connection = Connection()
    connection.execute("sqlalchemy-secret-statement", {"secret": "sqlalchemy-secret-parameter"})
    connection.exec_driver_sql("sqlalchemy-secret-driver-statement")
    connection.commit()
    connection.rollback()
    session = Session()
    session.execute("sqlalchemy-secret-session-statement")
    ignore_failure(lambda: session.execute("sqlalchemy-secret-failure"))
    session.commit()
    session.rollback()
    Redis().execute_command("GET", "redis-secret-key")
    ignore_failure(lambda: Redis().execute_command("REDIS-SECRET-FAIL", "redis-secret-key"))
    Pipeline().execute()
    channel = BlockingChannel()
    channel.basic_publish("pika-secret-exchange", "pika-secret-routing-key", b"pika-secret-body")
    ignore_failure(
        lambda: channel.basic_publish(
            "pika-secret-exchange",
            "pika-secret-routing-key",
            b"pika-secret-failure-body",
        )
    )
    channel.basic_get("pika-secret-queue")

async def exercise_async():
    connection = AsyncConnection()
    await connection.execute("sqlalchemy-async-secret-statement")
    await connection.exec_driver_sql("sqlalchemy-async-secret-driver-statement")
    await connection.commit()
    await connection.rollback()
    session = AsyncSession()
    await session.execute("sqlalchemy-async-secret-session-statement")
    try:
        await session.execute("sqlalchemy-async-secret-failure")
    except RuntimeError:
        pass
    await session.commit()
    await session.rollback()
    await AsyncRedis().execute_command("GET", "redis-async-secret-key")
    try:
        await AsyncRedis().execute_command("REDIS-ASYNC-SECRET-FAIL", "redis-async-secret-key")
    except TimeoutError:
        pass
    await asyncio.gather(
        AsyncRedis().execute_command("GET", "redis-concurrent-secret-key-one"),
        AsyncRedis().execute_command("GET", "redis-concurrent-secret-key-two"),
    )
    await AsyncPipeline().execute()
    producer = AIOKafkaProducer()
    await producer.send_and_wait("kafka-secret-topic", b"kafka-secret-value")
    try:
        await producer.send_and_wait("kafka-secret-topic", b"kafka-secret-failure-value")
    except TimeoutError:
        pass
    consumer = AIOKafkaConsumer()
    await consumer.getone()
    await consumer.getmany(timeout_ms=10)

exercise_sync()
asyncio.run(exercise_async())

import redis.client
importlib.reload(redis.client)
redis.client.Redis().execute_command("GET", "redis-reload-secret-key")

with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(
        {
            "connection_signature": list(inspect.signature(Connection.execute).parameters),
            "redis_signature": list(inspect.signature(Redis.execute_command).parameters),
            "pika_signature": list(inspect.signature(BlockingChannel.basic_publish).parameters),
            "kafka_signature": list(inspect.signature(AIOKafkaProducer.send_and_wait).parameters),
            "loaders": [
                type(sys.modules["sqlalchemy.engine.base"].__loader__).__name__,
                type(sys.modules["redis.client"].__loader__).__name__,
                type(sys.modules["pika.adapters.blocking_connection"].__loader__).__name__,
                type(sys.modules["aiokafka.producer.producer"].__loader__).__name__,
            ],
        },
        output,
        sort_keys=True,
    )
""".strip(),
        encoding="utf-8",
    )
    passive_output = tmp_path / "optional-logical-passive.json"
    deep_output = tmp_path / "optional-logical-deep.json"
    passive_runpack = tmp_path / "optional-logical-passive.runpack"
    deep_runpack = tmp_path / "optional-logical-deep.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload), str(passive_output)),
        passive_runpack,
        name="optional-logical-passive",
        capture_level="passive",
    )
    deep_exit = record_process(
        (sys.executable, str(workload), str(deep_output)),
        deep_runpack,
        name="optional-logical-deep",
        capture_level="deep",
    )

    passive = json.loads(passive_output.read_text(encoding="utf-8"))
    observed = json.loads(deep_output.read_text(encoding="utf-8"))
    assert passive_exit == deep_exit == 0
    assert observed == passive
    assert all(loader != "_OptionalAdapterLoader" for loader in observed["loaders"])
    analysis = analyze_runpack(deep_runpack)
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    assert analysis.logical_operation_capture.operation_count == 34
    assert analysis.logical_operation_capture.attributed_operation_count == 34
    assert analysis.logical_operation_capture.adapters == (
        "aiokafka.AIOKafkaConsumer",
        "aiokafka.AIOKafkaProducer",
        "pika.BlockingChannel",
        "redis.Pipeline",
        "redis.Redis",
        "redis.asyncio.Pipeline",
        "redis.asyncio.Redis",
        "sqlalchemy.engine.Connection",
        "sqlalchemy.ext.asyncio.AsyncConnection",
        "sqlalchemy.ext.asyncio.AsyncSession",
        "sqlalchemy.orm.Session",
        "stdlib.asyncio.Queue",
        "stdlib.asyncio.TaskGroup",
        "stdlib.asyncio.create_task",
        "stdlib.asyncio.ensure_future",
        "stdlib.asyncio.gather",
    )
    assert sum(operation.category == "database" for operation in analysis.logical_operations) == 16
    assert sum(operation.category == "cache" for operation in analysis.logical_operations) == 9
    assert sum(operation.category == "broker" for operation in analysis.logical_operations) == 7
    assert sum(operation.category == "scheduler" for operation in analysis.logical_operations) == 2
    assert (
        sum(operation.outcome == "operation_error" for operation in analysis.logical_operations)
        == 6
    )
    assert {finding.classification for finding in analysis.bottlenecks} >= {
        "broker_operation_failures",
        "cache_operation_failures",
        "database_operation_failures",
    }
    report = render_analysis(analysis, "text")
    assert (
        "supported database, cache, queue, broker, executor, scheduler, and inbound server "
        "boundaries" in report
    )
    assert "CACHE command" in report
    assert "BROKER publish" in report
    runpack_bytes = deep_runpack.read_bytes()
    for secret in (
        b"sqlalchemy-secret-statement",
        b"sqlalchemy-secret-parameter",
        b"sqlalchemy-secret-error-message",
        b"redis-secret-key",
        b"redis-concurrent-secret-key-one",
        b"redis-concurrent-secret-key-two",
        b"redis-secret-result",
        b"redis-secret-error-message",
        b"pika-secret-exchange",
        b"pika-secret-routing-key",
        b"pika-secret-body",
        b"pika-secret-error-message",
        b"kafka-secret-topic",
        b"kafka-secret-value",
        b"kafka-secret-error-message",
    ):
        assert secret not in runpack_bytes


def test_unsupported_optional_client_shape_marks_logical_capture_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    site_packages = tmp_path / "site-packages"
    redis_package = site_packages / "redis"
    redis_package.mkdir(parents=True)
    (redis_package / "__init__.py").write_text("", encoding="utf-8")
    (redis_package / "client.py").write_text(
        """
class Redis:
    def execute_command(self, command, *arguments, **options):
        return b"unsupported-shape-secret-result"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("PYTHONPATH", str(site_packages))
    workload = tmp_path / "unsupported-optional-client.py"
    workload.write_text(
        """
from redis.client import Redis
assert Redis().execute_command("GET", "unsupported-shape-secret-key")
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "unsupported-optional-client.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="unsupported-optional-client",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "truncated"
    assert analysis.logical_operation_capture.operation_count == 1
    assert analysis.logical_operation_capture.callback_error_count == 1
    assert analysis.logical_operation_capture.adapters == ("redis.Redis",)
    assert len(analysis.logical_operations) == 1
    assert analysis.logical_operations[0].category == "cache"
    assert analysis.logical_operations[0].operation == "command"
    assert b"unsupported-shape-secret-key" not in runpack.read_bytes()
    assert b"unsupported-shape-secret-result" not in runpack.read_bytes()


def test_logical_operation_capture_is_deep_only(tmp_path: Path) -> None:
    workload = Path(__file__).parents[1] / "examples" / "local" / "logical_operations.py"
    runpack = tmp_path / "sample-logical-operations.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="sample-logical-operations",
        capture_level="sample",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "unavailable"
    assert analysis.logical_operation_capture.process_count == 0
    assert analysis.logical_operation_capture.operation_count == 0
    assert analysis.logical_operations == ()


def test_logical_operation_adapters_survive_supported_module_reload(tmp_path: Path) -> None:
    workload = tmp_path / "reload-logical-adapters.py"
    workload.write_text(
        """
import importlib
import asyncio
import asyncio.taskgroups
import asyncio.tasks
import concurrent.futures.thread
import queue
import sqlite3
import sqlite3.dbapi2

async def exercise_async(value):
    task = asyncio.tasks.create_task(asyncio.sleep(0, result=value))
    assert await task == value
    ensured = asyncio.ensure_future(asyncio.sleep(0, result=value))
    assert await ensured == value
    assert await asyncio.gather(
        asyncio.sleep(0, result=value),
        asyncio.sleep(0, result=value),
    ) == [value, value]
    async with asyncio.taskgroups.TaskGroup() as group:
        grouped = group.create_task(asyncio.sleep(0, result=value))
    assert grouped.result() == value

def exercise(value):
    connection = sqlite3.connect(":memory:")
    connection.execute("SELECT ?", (value,)).fetchone()
    connection.close()
    messages = queue.Queue()
    messages.put(value)
    assert messages.get() == value
    with concurrent.futures.thread.ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(len, value).result() == len(value)
    asyncio.run(exercise_async(value))

exercise("reload-secret-before")
importlib.reload(asyncio.tasks)
importlib.reload(asyncio.taskgroups)
importlib.reload(concurrent.futures.thread)
importlib.reload(queue)
importlib.reload(sqlite3.dbapi2)
importlib.reload(sqlite3)
exercise("reload-secret-after")
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "reload-logical-adapters.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="reload-logical-adapters",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    assert analysis.logical_operation_capture.operation_count == 18
    assert len(analysis.logical_operations) == 18
    assert all(operation.outcome == "completed" for operation in analysis.logical_operations)
    assert sum(operation.category == "executor" for operation in analysis.logical_operations) == 2
    assert sum(operation.category == "scheduler" for operation in analysis.logical_operations) == 10
    assert b"reload-secret" not in runpack.read_bytes()


def test_executor_task_capture_retains_cancellation_and_rejected_submission_safely(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "cancel-executor-tasks.py"
    workload.write_text(
        """
import threading
from concurrent.futures import ThreadPoolExecutor

gate = threading.Event()
executor = ThreadPoolExecutor(max_workers=1)
running = executor.submit(gate.wait)
cancelled = executor.submit(lambda: "cancelled-task-result-must-not-be-captured")
assert cancelled.cancel()
gate.set()
assert running.result(timeout=1)
executor.shutdown()
try:
    executor.submit(lambda: "rejected-task-result-must-not-be-captured")
except RuntimeError:
    pass
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "cancel-executor-tasks.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="cancel-executor-tasks",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)
    executor_tasks = tuple(
        operation for operation in analysis.logical_operations if operation.category == "executor"
    )

    assert exit_code == 0
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    assert len(executor_tasks) == 3
    assert sum(operation.outcome == "completed" for operation in executor_tasks) == 1
    assert {
        operation.error_type
        for operation in executor_tasks
        if operation.outcome == "operation_error"
    } == {"CancelledError", "RuntimeError"}
    assert all(operation.duration_seconds is not None for operation in executor_tasks)
    assert all(operation.caller is not None for operation in executor_tasks)
    runpack_bytes = runpack.read_bytes()
    assert b"cancelled-task-result-must-not-be-captured" not in runpack_bytes
    assert b"rejected-task-result-must-not-be-captured" not in runpack_bytes


def test_logical_operation_capture_is_bounded_before_cross_product_analysis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(deep_profile_module, "MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS", 1)
    baseline = tmp_path / "bounded-logical-baseline.runpack"
    candidate = tmp_path / "bounded-logical-candidate.runpack"
    baseline_workload = "import sqlite3; sqlite3.connect(':memory:').execute('select 1')"
    candidate_workload = (
        "import sqlite3; connection=sqlite3.connect(':memory:'); "
        "connection.execute('select 1'); connection.execute('select 2')"
    )

    record_process(
        (sys.executable, "-c", baseline_workload),
        baseline,
        name="bounded-logical-baseline",
        capture_level="deep",
    )
    record_process(
        (sys.executable, "-c", candidate_workload),
        candidate,
        name="bounded-logical-candidate",
        capture_level="deep",
    )
    analysis = analyze_runpack(candidate)
    diff = compare_runpacks(baseline, candidate)
    contract = tmp_path / "logical-operation-contract.yaml"
    contract.write_text(
        "name: logical-operation-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: Database execute\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )
    verification = verify_contracts(contract, baseline, candidate)

    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "truncated"
    assert analysis.logical_operation_capture.operation_count == 1
    assert analysis.logical_operation_capture.dropped_operation_count == 1
    assert diff.baseline_logical_operation_capture_status == "complete"
    assert diff.candidate_logical_operation_capture_status == "truncated"
    assert diff.candidate_dropped_logical_operation_count == 1
    assert "candidate: logical operation capture truncated (1 omitted)" in render_diff(diff, "text")
    assert verification.results[0].status == "unverifiable"
    assert verification.results[0].observed == (
        "logical operation evidence incomplete "
        "(candidate: logical operation capture truncated, 1 omitted)"
    )


def test_network_hotspots_aggregate_before_connection_detail_is_bounded(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "network-hotspot-bound.py"
    workload.write_text(
        """
import socket
import threading

server = socket.socket()
server.bind(("127.0.0.1", 0))
server.listen()

def accept_connections():
    for _ in range(101):
        peer, _address = server.accept()
        peer.close()

thread = threading.Thread(target=accept_connections)
thread.start()

def connect_batch():
    for _ in range(100):
        connection = socket.create_connection(("127.0.0.1", server.getsockname()[1]))
        connection.close()

def connect_final():
    connection = socket.create_connection(("127.0.0.1", server.getsockname()[1]))
    connection.close()

connect_batch()
connect_final()
thread.join()
server.close()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "network-hotspot-bound.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="network-hotspot-bound",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.network_capture is not None
    assert analysis.network_capture.connection_count == 101
    assert analysis.network_capture.connection_hotspot_count == 2
    assert len(analysis.network_connections) == 100
    assert len(analysis.network_connection_hotspots) == 2
    hotspots = {
        hotspot.caller.name: hotspot.connection_count
        for hotspot in analysis.network_connection_hotspots
        if hotspot.caller is not None
    }
    assert hotspots == {
        "__main__.connect_batch": 100,
        "__main__.connect_final": 1,
    }
    assert all(
        connection.caller is not None and connection.caller.name == "__main__.connect_batch"
        for connection in analysis.network_connections
    )
    assert "1 additional items omitted from text output" in render_analysis(analysis, "text")


def test_truncated_network_capture_makes_operation_contract_unverifiable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(deep_profile_module, "MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS", 1)
    baseline = tmp_path / "bounded-network-baseline.runpack"
    candidate = tmp_path / "bounded-network-candidate.runpack"
    record_process(
        (sys.executable, "-c", _network_connection_workload(1)),
        baseline,
        name="baseline",
        capture_level="sample",
    )
    record_process(
        (sys.executable, "-c", _network_connection_workload(2)),
        candidate,
        name="candidate",
        capture_level="sample",
    )
    contract = tmp_path / "bounded-network-contract.yaml"
    contract.write_text(
        "name: connection-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: TCP connect\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )

    diff = compare_runpacks(baseline, candidate)
    verification = verify_contracts(contract, baseline, candidate)

    assert diff.candidate_network_capture_status == "truncated"
    assert diff.candidate_dropped_network_connection_count == 1
    assert verification.results[0].status == "unverifiable"
    assert verification.results[0].observed == (
        "network evidence incomplete (candidate: network capture truncated, 1 omitted)"
    )


@pytest.mark.parametrize(
    ("capture_level", "observation", "confidence"),
    (("sample", "sampled", 0.9), ("deep", "exact", 1.0)),
)
def test_zero_code_subprocess_capture_attributes_the_initiating_function(
    tmp_path: Path,
    capture_level: str,
    observation: str,
    confidence: float,
) -> None:
    workload = tmp_path / f"caller-{capture_level}.py"
    workload.write_text(
        """
import subprocess
import sys

def launch_worker():
    subprocess.run(
        [sys.executable, "-c", "import time; time.sleep(0.08)"],
        check=True,
    )

launch_worker()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / f"caller-{capture_level}.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name=f"caller-{capture_level}",
        capture_level=capture_level,
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        callsites = tuple(event for event in reader.events() if event.kind == "python.callsite")
        launches = tuple(edge for edge in reader.causal_edges() if edge.kind == "launches")
        operation_counts = reader.operation_counts()
    assert exit_code == 0
    assert len(callsites) == len(launches) == 1
    assert callsites[0].name == "__main__.launch_worker"
    assert callsites[0].attributes["arguments_captured"] is False
    assert callsites[0].attributes["locals_captured"] is False
    assert callsites[0].attributes["filename"] == str(workload)
    assert launches[0].source_event_id == callsites[0].id
    assert launches[0].attributes["observation"] == observation
    assert launches[0].confidence == confidence
    assert not any(key[2] == "python.callsite" for key in operation_counts)
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    semantic = instrumentation["semantic_capture"]
    assert isinstance(semantic, dict)
    caller_metadata = semantic["caller_attribution"]
    assert isinstance(caller_metadata, dict)
    assert caller_metadata == {
        "status": "complete",
        "caller_count": 1,
        "attributed_subprocess_count": 1,
        "unattributed_subprocess_count": 0,
        "invalid_caller_count": 0,
        "callback_error_count": 0,
        "arguments_captured": False,
        "locals_captured": False,
    }
    analysis = analyze_runpack(runpack)
    assert analysis.semantic_capture is not None
    assert analysis.semantic_capture.caller_attribution_status == "complete"
    assert analysis.subprocess_calls[0].caller is not None
    assert analysis.subprocess_calls[0].caller.name == "__main__.launch_worker"
    assert analysis.subprocess_calls[0].caller.observation == observation
    report = render_analysis(analysis, "text")
    assert "caller attribution complete: 1 / 1 boundaries across 1 callsites" in report
    assert f"called by __main__.launch_worker [{observation}" in report


@pytest.mark.parametrize(
    ("capture_level", "observation", "confidence"),
    (("sample", "sampled", 0.9), ("deep", "exact", 1.0)),
)
def test_zero_code_http_capture_records_a_redacted_request_and_caller(
    tmp_path: Path,
    capture_level: str,
    observation: str,
    confidence: float,
) -> None:
    workload = tmp_path / f"http-caller-{capture_level}.py"
    workload.write_text(
        """
import http.server
import threading
import time
import urllib.error
import urllib.request

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        time.sleep(0.08)
        self.send_response(503)
        self.send_header("X-Response-Secret", "must-not-capture-response-header")
        self.end_headers()
        self.wfile.write(b"must-not-capture-response-body")

    def log_message(self, *_args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
thread = threading.Thread(target=server.serve_forever)
thread.start()

def fetch_orders():
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.server_port}/must-not-capture-path"
        "?token=must-not-capture-query",
        data=b"must-not-capture-request-body",
        headers={"Authorization": "must-not-capture-header"},
    )
    try:
        urllib.request.urlopen(request, timeout=2).read()
    except urllib.error.HTTPError as error:
        assert error.code == 503

try:
    fetch_orders()
finally:
    server.shutdown()
    server.server_close()
    thread.join()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / f"http-caller-{capture_level}.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name=f"http-caller-{capture_level}",
        capture_level=capture_level,
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        events = reader.events()
        edges = reader.causal_edges()
        operation_counts = reader.operation_counts()
    http_events = tuple(event for event in events if event.kind == "http.client.request")
    network_events = tuple(event for event in events if event.kind == "network.connect")
    http_callsites = tuple(
        event
        for event in events
        if event.kind == "python.callsite"
        and event.attributes.get("source") == "python-http-client-wrapper"
    )
    request_edges = tuple(edge for edge in edges if edge.kind == "requests")
    assert exit_code == 0
    assert len(http_events) == len(http_callsites) == len(request_edges) == 1
    assert network_events == ()
    request_event = http_events[0]
    caller_event = http_callsites[0]
    request_edge = request_edges[0]
    assert request_event.name == "HTTP POST"
    assert request_event.attributes["method"] == "POST"
    assert request_event.attributes["scheme"] == "http"
    assert request_event.attributes["server_address"] is None
    assert request_event.attributes["server_identity_policy"] == "redact"
    assert request_event.attributes["status_code"] == 503
    assert request_event.attributes["outcome"] == "response"
    assert request_event.attributes["duration_boundary"] == "response_headers"
    assert request_event.attributes["path_captured"] is False
    assert request_event.attributes["query_captured"] is False
    assert request_event.attributes["headers_captured"] is False
    assert request_event.attributes["body_captured"] is False
    assert request_event.attributes["response_body_captured"] is False
    assert request_event.started_at_ns is not None
    assert request_event.finished_at_ns is not None
    assert request_event.finished_at_ns > request_event.started_at_ns
    assert caller_event.name == "__main__.fetch_orders"
    assert caller_event.attributes["filename"] == str(workload)
    assert request_edge.source_event_id == caller_event.id
    assert request_edge.target_event_id == request_event.id
    assert request_edge.attributes["observation"] == observation
    assert request_edge.confidence == confidence
    assert not any(key[2] == "python.callsite" for key in operation_counts)
    assert any(
        key[2:] == ("http.client.request", "HTTP POST") and count == 1
        for key, count in operation_counts.items()
    )
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    http_capture = instrumentation["http_capture"]
    assert isinstance(http_capture, dict)
    assert http_capture["status"] == "complete"
    assert http_capture["request_count"] == 1
    assert http_capture["server_identity_policy"] == "redact"
    assert http_capture["server_address_captured"] is False
    caller_metadata = http_capture["caller_attribution"]
    assert isinstance(caller_metadata, dict)
    assert caller_metadata["status"] == "complete"
    assert caller_metadata["attributed_request_count"] == 1
    assert caller_metadata["unattributed_request_count"] == 0
    network_capture = instrumentation["network_capture"]
    assert isinstance(network_capture, dict)
    assert network_capture["status"] == "complete"
    assert network_capture["connection_count"] == 0

    analysis = analyze_runpack(runpack)
    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "complete"
    assert analysis.http_capture.caller_attribution_status == "complete"
    assert len(analysis.http_requests) == 1
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "complete"
    assert analysis.network_connections == ()
    assert analysis.http_requests[0].caller is not None
    assert analysis.http_requests[0].caller.name == "__main__.fetch_orders"
    assert analysis.http_requests[0].caller.observation == observation
    report = render_analysis(analysis, "text")
    assert "Automatic HTTP client capture" in report
    assert "POST http://<redacted>:" in report
    assert f"called by __main__.fetch_orders [{observation}" in report
    runpack_bytes = runpack.read_bytes()
    for secret in (
        b"must-not-capture-path",
        b"must-not-capture-query",
        b"must-not-capture-header",
        b"must-not-capture-request-body",
        b"must-not-capture-response-header",
        b"must-not-capture-response-body",
    ):
        assert secret not in runpack_bytes


def test_zero_code_http_capture_retains_connection_failures_as_operations(tmp_path: Path) -> None:
    workload = tmp_path / "http-failure.py"
    workload.write_text(
        """
import http.client
import socket

reserved = socket.socket()
reserved.bind(("127.0.0.1", 0))
port = reserved.getsockname()[1]
reserved.close()

def fetch_unavailable():
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
    try:
        connection.request("GET", "/must-not-capture-failure-path")
    except OSError:
        pass
    finally:
        connection.close()

fetch_unavailable()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "http-failure.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="http-failure",
        capture_level="deep",
    )

    with RunpackReader(runpack) as reader:
        request = next(event for event in reader.events() if event.kind == "http.client.request")
        errors = reader.operation_error_counts()
    assert exit_code == 0
    assert request.attributes["outcome"] == "request_error"
    assert request.attributes["error"] is True
    assert request.attributes["error.type"] in {"ConnectionRefusedError", "TimeoutError"}
    assert request.finished_at_ns is not None
    assert (
        sum(
            count
            for (_, _, kind, name), count in errors.items()
            if (kind, name) == ("http.client.request", "HTTP GET")
        )
        == 1
    )
    analysis = analyze_runpack(runpack)
    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "complete"
    assert analysis.http_requests[0].outcome == "request_error"
    assert analysis.http_requests[0].caller is not None
    assert analysis.http_requests[0].caller.name == "__main__.fetch_unavailable"
    assert b"must-not-capture-failure-path" not in runpack.read_bytes()


def test_zero_code_network_capture_records_sync_async_tcp_and_unix_connections(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "network-connections.py"
    workload.write_text(
        """
import asyncio
import json
import os
import socket
import tempfile
import threading

unix_directory = tempfile.TemporaryDirectory(prefix="ct-network-")
unix_path = os.path.join(unix_directory.name, "must-not-capture-network-path.sock")
tcp_server = socket.socket()
tcp_server.bind(("127.0.0.1", 0))
tcp_server.listen()
unix_server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
unix_server.bind(unix_path)
unix_server.listen()

def accept_connections(server):
    for _ in range(2):
        peer, _address = server.accept()
        peer.close()

tcp_thread = threading.Thread(target=accept_connections, args=(tcp_server,))
unix_thread = threading.Thread(target=accept_connections, args=(unix_server,))
tcp_thread.start()
unix_thread.start()

def connect_tcp_sync():
    connection = socket.create_connection(("127.0.0.1", tcp_server.getsockname()[1]))
    connection.close()

def connect_unix_sync():
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.connect(unix_path)
    connection.close()

async def connect_async():
    _reader, writer = await asyncio.open_connection(
        "127.0.0.1", tcp_server.getsockname()[1]
    )
    writer.close()
    await writer.wait_closed()
    _reader, writer = await asyncio.open_unix_connection(unix_path)
    writer.close()
    await writer.wait_closed()

connect_tcp_sync()
connect_unix_sync()
asyncio.run(connect_async())
tcp_thread.join()
unix_thread.join()
tcp_server.close()
unix_server.close()
unix_directory.cleanup()
print(json.dumps({"connected": 4}, sort_keys=True))
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "network-connections.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="network-connections",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    assert exit_code == 0
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "complete"
    assert analysis.network_capture.caller_attribution_status == "complete"
    assert analysis.network_capture.adapters == (
        "asyncio.create_connection",
        "asyncio.create_unix_connection",
        "stdlib.socket.connect",
    )
    assert len(analysis.network_connections) == 4
    assert {
        (connection.transport, connection.adapter) for connection in analysis.network_connections
    } == {
        ("tcp", "stdlib.socket.connect"),
        ("unix", "stdlib.socket.connect"),
        ("tcp", "asyncio.create_connection"),
        ("unix", "asyncio.create_unix_connection"),
    }
    assert all(
        connection.outcome == "connected"
        and connection.caller is not None
        and connection.caller.observation == "exact"
        for connection in analysis.network_connections
    )
    assert {
        connection.caller.name for connection in analysis.network_connections if connection.caller
    } == {
        "__main__.connect_tcp_sync",
        "__main__.connect_unix_sync",
        "__main__.connect_async",
    }
    tcp_connections = tuple(
        connection for connection in analysis.network_connections if connection.transport == "tcp"
    )
    unix_connections = tuple(
        connection for connection in analysis.network_connections if connection.transport == "unix"
    )
    assert len(tcp_connections) == 2
    tcp_ports = {connection.server_port for connection in tcp_connections}
    assert len(tcp_ports) == 1
    assert None not in tcp_ports
    assert all(connection.server_port is None for connection in unix_connections)
    report = render_analysis(analysis, "text")
    assert "Automatic network connection capture" in report
    assert "Outbound network connections" in report
    assert "tcp://<redacted>:" in report
    assert "unix://<redacted>" in report
    runpack_bytes = runpack.read_bytes()
    assert b"127.0.0.1" not in runpack_bytes
    assert b"must-not-capture-network-path.sock" not in runpack_bytes


def test_zero_code_network_capture_retains_only_safe_connection_failures(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "network-failures.py"
    workload.write_text(
        """
import asyncio
import os
import socket
import tempfile

unix_directory = tempfile.TemporaryDirectory(prefix="ct-network-")
missing_unix_path = os.path.join(
    unix_directory.name, "must-not-capture-missing-path.sock"
)
reserved = socket.socket()
reserved.bind(("127.0.0.1", 0))
port = reserved.getsockname()[1]
reserved.close()

def fail_sync():
    try:
        socket.create_connection(("127.0.0.1", port), timeout=0.5)
    except OSError:
        pass
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.connect(missing_unix_path)
    except OSError:
        pass
    finally:
        connection.close()

async def fail_async():
    try:
        await asyncio.open_connection("127.0.0.1", port)
    except OSError:
        pass
    try:
        await asyncio.open_unix_connection(missing_unix_path)
    except OSError:
        pass

fail_sync()
asyncio.run(fail_async())
unix_directory.cleanup()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "network-failures.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="network-failures",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    assert exit_code == 0
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "complete"
    assert len(analysis.network_connections) == 4
    assert all(
        connection.outcome == "connect_error"
        and connection.error_type in {"ConnectionRefusedError", "FileNotFoundError"}
        for connection in analysis.network_connections
    )
    failure = next(
        finding
        for finding in analysis.bottlenecks
        if finding.classification == "connection_failures"
    )
    assert failure.evidence == "4 of 4 retained outbound connection attempts failed"
    assert failure.confidence == 0.9
    runpack_bytes = runpack.read_bytes()
    assert b"127.0.0.1" not in runpack_bytes
    assert b"must-not-capture-missing-path.sock" not in runpack_bytes


def test_zero_code_network_capture_retains_async_cancellation_without_its_message(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "network-cancellation.py"
    workload.write_text(
        """
import asyncio
import socket
import threading

server = socket.socket()
server.bind(("127.0.0.1", 0))
server.listen()

def accept_connection():
    peer, _address = server.accept()
    peer.close()

thread = threading.Thread(target=accept_connection)
thread.start()

def cancel_protocol():
    raise asyncio.CancelledError("must-not-capture-cancel-message")

async def connect_cancelled():
    loop = asyncio.get_running_loop()
    try:
        await loop.create_connection(
            cancel_protocol, "127.0.0.1", server.getsockname()[1]
        )
    except asyncio.CancelledError:
        pass

asyncio.run(connect_cancelled())
thread.join()
server.close()
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "network-cancellation.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="network-cancellation",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    assert exit_code == 0
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "complete"
    assert len(analysis.network_connections) == 1
    connection = analysis.network_connections[0]
    assert connection.adapter == "asyncio.create_connection"
    assert connection.outcome == "connect_error"
    assert connection.error_type == "CancelledError"
    assert connection.caller is not None
    assert connection.caller.name == "__main__.connect_cancelled"
    assert b"must-not-capture-cancel-message" not in runpack.read_bytes()


def _write_optional_http_fakes(tmp_path: Path) -> None:
    httpcore = tmp_path / "httpcore"
    httpcore.mkdir()
    (httpcore / "__init__.py").write_text(
        """
import asyncio
import time

class URL:
    def __init__(self, scheme, port):
        self.scheme = scheme
        self.host = b"must-not-capture-httpcore-host"
        self.port = port
        self.target = b"/must-not-capture-httpcore-path?token=secret"

class Request:
    def __init__(self, method, scheme=b"https", port=9443):
        self.method = method
        self.url = URL(scheme, port)
        self.headers = [(b"Authorization", b"must-not-capture-httpcore-header")]
        self.stream = [b"must-not-capture-httpcore-body"]

class Response:
    def __init__(self, status):
        self.status = status
        self.headers = [(b"X-Secret", b"must-not-capture-httpcore-response-header")]
        self.stream = [b"must-not-capture-httpcore-response-body"]

class ConnectionPool:
    def handle_request(self, request):
        time.sleep(0.08)
        if request.method == b"FAIL":
            raise TimeoutError("must-not-capture-httpcore-error-message")
        return Response(207)

class AsyncConnectionPool:
    async def handle_async_request(self, request):
        await asyncio.sleep(0.03)
        if request.method == b"FAIL":
            raise TimeoutError("must-not-capture-httpcore-async-error-message")
        if request.method == b"CANCEL":
            raise asyncio.CancelledError("must-not-capture-httpcore-cancel-message")
        return Response(208)
""".strip(),
        encoding="utf-8",
    )
    aiohttp = tmp_path / "aiohttp"
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
        self.headers = {"X-Secret": "must-not-capture-aiohttp-response-header"}
        self.body = b"must-not-capture-aiohttp-response-body"

class ClientSession:
    def __init__(self, base_url=None):
        self._base_url = base_url

    async def _request(self, method, url, **kwargs):
        await asyncio.sleep(0.03)
        if method == "FAIL":
            raise ConnectionError("must-not-capture-aiohttp-error-message")
        return Response(209)

    async def request(self, method, url, **kwargs):
        return await self._request(method, url, **kwargs)
""".strip(),
        encoding="utf-8",
    )


@pytest.mark.parametrize("capture_level", ("sample", "deep"))
def test_optional_http_adapters_capture_sync_and_async_clients_without_dependencies(
    tmp_path: Path,
    capture_level: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()
    _write_optional_http_fakes(site_packages)
    monkeypatch.setenv("PYTHONPATH", str(site_packages))
    workload = tmp_path / "optional-http-clients.py"
    workload.write_text(
        """
import asyncio
import inspect
import json
import sys

import aiohttp
import aiohttp.client
import httpcore

def sync_fetch():
    return httpcore.ConnectionPool().handle_request(httpcore.Request(b"POST"))

async def async_fetch():
    core_response = await httpcore.AsyncConnectionPool().handle_async_request(
        httpcore.Request(b"PUT", scheme=b"http", port=9080)
    )
    aio_response = await aiohttp.ClientSession().request(
        "PATCH",
        "https://must-not-capture-aiohttp-host:9444/must-not-capture-aiohttp-path"
        "?token=must-not-capture-aiohttp-query",
        headers={"Authorization": "must-not-capture-aiohttp-header"},
        data=b"must-not-capture-aiohttp-body",
    )
    return core_response, aio_response

sync_response = sync_fetch()
core_response, aio_response = asyncio.run(async_fetch())
with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(
        {
            "statuses": [sync_response.status, core_response.status, aio_response.status],
            "signatures": {
                "sync": list(inspect.signature(httpcore.ConnectionPool.handle_request).parameters),
                "async": list(
                    inspect.signature(httpcore.AsyncConnectionPool.handle_async_request).parameters
                ),
                "aiohttp": list(inspect.signature(aiohttp.ClientSession._request).parameters),
            },
            "loaders": [
                type(httpcore.__loader__).__name__,
                type(aiohttp.client.__loader__).__name__,
            ],
        },
        output,
        sort_keys=True,
    )
""".strip(),
        encoding="utf-8",
    )
    passive_output = tmp_path / "optional-passive.json"
    instrumented_output = tmp_path / f"optional-{capture_level}.json"
    passive_runpack = tmp_path / "optional-passive.runpack"
    instrumented_runpack = tmp_path / f"optional-{capture_level}.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload), str(passive_output)),
        passive_runpack,
        name="optional-passive",
        capture_level="passive",
    )
    instrumented_exit = record_process(
        (sys.executable, str(workload), str(instrumented_output)),
        instrumented_runpack,
        name=f"optional-{capture_level}",
        capture_level=capture_level,
    )

    passive = json.loads(passive_output.read_text(encoding="utf-8"))
    instrumented = json.loads(instrumented_output.read_text(encoding="utf-8"))
    assert passive_exit == instrumented_exit == 0
    assert instrumented == passive
    assert instrumented["statuses"] == [207, 208, 209]
    assert all(loader != "_OptionalAdapterLoader" for loader in instrumented["loaders"])
    analysis = analyze_runpack(instrumented_runpack)
    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "complete"
    assert analysis.http_capture.adapters == (
        "aiohttp.async",
        "httpcore.async",
        "httpcore.sync",
        "stdlib.http.client",
    )
    assert {
        (request.method, request.status_code, request.adapter) for request in analysis.http_requests
    } == {
        ("POST", 207, "httpcore.sync"),
        ("PUT", 208, "httpcore.async"),
        ("PATCH", 209, "aiohttp.async"),
    }
    if capture_level == "deep":
        assert analysis.http_capture.caller_attribution_status == "complete"
        caller_names = {request.caller.name for request in analysis.http_requests if request.caller}
        assert caller_names == {"__main__.sync_fetch", "__main__.async_fetch"}
        assert all(
            request.caller is not None and request.caller.observation == "exact"
            for request in analysis.http_requests
        )
    else:
        assert analysis.http_capture.caller_attribution_status == "partial"
        sync_request = next(
            request for request in analysis.http_requests if request.adapter == "httpcore.sync"
        )
        assert sync_request.caller is not None
        assert sync_request.caller.observation == "sampled"
        assert all(
            request.caller is None
            for request in analysis.http_requests
            if request.adapter in {"httpcore.async", "aiohttp.async"}
        )
    report = render_analysis(analysis, "text")
    assert "active adapters: aiohttp.async, httpcore.async, httpcore.sync" in report
    assert "adapter httpcore.sync" in report
    runpack_bytes = instrumented_runpack.read_bytes()
    for secret in (
        b"must-not-capture-httpcore-host",
        b"must-not-capture-httpcore-path",
        b"must-not-capture-httpcore-header",
        b"must-not-capture-httpcore-body",
        b"must-not-capture-httpcore-response-header",
        b"must-not-capture-httpcore-response-body",
        b"must-not-capture-aiohttp-host",
        b"must-not-capture-aiohttp-path",
        b"must-not-capture-aiohttp-query",
        b"must-not-capture-aiohttp-header",
        b"must-not-capture-aiohttp-body",
    ):
        assert secret not in runpack_bytes


def test_optional_http_adapters_retain_safe_exception_classes(tmp_path: Path) -> None:
    _write_optional_http_fakes(tmp_path)
    workload = tmp_path / "optional-http-failures.py"
    workload.write_text(
        """
import asyncio
import aiohttp
import httpcore

def fail_sync():
    try:
        httpcore.ConnectionPool().handle_request(httpcore.Request(b"FAIL"))
    except TimeoutError:
        pass

async def fail_async():
    try:
        await httpcore.AsyncConnectionPool().handle_async_request(httpcore.Request(b"FAIL"))
    except TimeoutError:
        pass
    try:
        await httpcore.AsyncConnectionPool().handle_async_request(httpcore.Request(b"CANCEL"))
    except asyncio.CancelledError:
        pass
    try:
        await aiohttp.ClientSession().request(
            "FAIL", "https://must-not-capture-failure-host/failure-path"
        )
    except ConnectionError:
        pass

fail_sync()
asyncio.run(fail_async())
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "optional-http-failures.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="optional-http-failures",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    assert exit_code == 0
    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "complete"
    assert {
        (request.adapter, request.outcome, request.error_type) for request in analysis.http_requests
    } == {
        ("httpcore.sync", "request_error", "TimeoutError"),
        ("httpcore.async", "request_error", "TimeoutError"),
        ("httpcore.async", "request_error", "CancelledError"),
        ("aiohttp.async", "request_error", "ConnectionError"),
    }
    runpack_bytes = runpack.read_bytes()
    for secret in (
        b"must-not-capture-failure-host",
        b"must-not-capture-httpcore-error-message",
        b"must-not-capture-httpcore-async-error-message",
        b"must-not-capture-httpcore-cancel-message",
        b"must-not-capture-aiohttp-error-message",
    ):
        assert secret not in runpack_bytes


def test_optional_http_adapter_survives_module_reload_without_duplicate_capture(
    tmp_path: Path,
) -> None:
    _write_optional_http_fakes(tmp_path)
    workload = tmp_path / "optional-http-reload.py"
    workload.write_text(
        """
import importlib
import httpcore

def fetch():
    return httpcore.ConnectionPool().handle_request(httpcore.Request(b"GET"))

assert fetch().status == 207
importlib.reload(httpcore)
assert fetch().status == 207
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "optional-http-reload.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="optional-http-reload",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    assert exit_code == 0
    assert [request.adapter for request in analysis.http_requests] == [
        "httpcore.sync",
        "httpcore.sync",
    ]
    assert all(
        request.caller is not None and request.caller.name == "__main__.fetch"
        for request in analysis.http_requests
    )


def test_optional_http_boundary_survives_abrupt_exit_via_shared_checkpoint(
    tmp_path: Path,
) -> None:
    _write_optional_http_fakes(tmp_path)
    workload = tmp_path / "optional-http-checkpoint.py"
    workload.write_text(
        """
import os
import time
import httpcore

def fetch_before_crash():
    return httpcore.ConnectionPool().handle_request(httpcore.Request(b"GET"))

assert fetch_before_crash().status == 207
time.sleep(0.65)
os._exit(0)
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "optional-http-checkpoint.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="optional-http-checkpoint",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    assert exit_code == 0
    assert analysis.deep_profile is not None
    assert analysis.deep_profile.status == "partial"
    assert analysis.deep_profile.checkpoint_process_count == 1
    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "partial"
    assert len(analysis.http_requests) == 1
    request = analysis.http_requests[0]
    assert (request.adapter, request.status_code) == ("httpcore.sync", 207)
    assert request.caller is not None
    assert request.caller.name == "__main__.fetch_before_crash"


@pytest.mark.parametrize("capture_level", ("sample", "deep"))
def test_semantic_observer_does_not_evaluate_user_arguments_or_change_signature(
    tmp_path: Path,
    capture_level: str,
) -> None:
    workload = tmp_path / "observer_transparency.py"
    workload.write_text(
        """
import inspect
import http.client
import json
import subprocess
import sys
from pathlib import Path

class CountingPath:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    def __fspath__(self):
        self.calls += 1
        return self.value

class CountingShell:
    def __init__(self):
        self.calls = 0

    def __bool__(self):
        self.calls += 1
        return False

audit_counts = {"sys._getframe": 0, "tb_frame": 0}
def audit(event, arguments):
    if event == "sys._getframe":
        audit_counts[event] += 1
    elif event == "object.__getattr__" and arguments[1] == "tb_frame":
        audit_counts["tb_frame"] += 1
sys.addaudithook(audit)

executable = CountingPath(sys.executable)
subprocess.run([executable, "-c", "pass"], check=True)
shell = CountingShell()
subprocess.run([sys.executable, "-c", "pass"], shell=shell, check=True)
Path(sys.argv[1]).write_text(
    json.dumps(
        {
            "path_calls": executable.calls,
            "shell_calls": shell.calls,
            "signature": list(inspect.signature(subprocess.Popen).parameters),
            "method_signatures": {
                name: list(inspect.signature(getattr(subprocess.Popen, name)).parameters)
                for name in ("wait", "poll", "communicate")
            },
            "http_method_signatures": {
                name: list(inspect.signature(getattr(http.client.HTTPConnection, name)).parameters)
                for name in ("putrequest", "endheaders", "getresponse", "close")
            },
            "audit_counts": audit_counts,
        }
    ),
    encoding="utf-8",
)
""".strip(),
        encoding="utf-8",
    )
    passive_result = tmp_path / "passive.json"
    instrumented_result = tmp_path / f"{capture_level}.json"
    instrumented_runpack = tmp_path / f"{capture_level}.runpack"

    passive_exit = record_process(
        (sys.executable, str(workload), str(passive_result)),
        tmp_path / "passive.runpack",
        name="passive",
        capture_level="passive",
    )
    instrumented_exit = record_process(
        (sys.executable, str(workload), str(instrumented_result)),
        instrumented_runpack,
        name=capture_level,
        capture_level=capture_level,
    )

    passive = json.loads(passive_result.read_text(encoding="utf-8"))
    instrumented = json.loads(instrumented_result.read_text(encoding="utf-8"))
    assert passive_exit == instrumented_exit == 0
    assert instrumented["path_calls"] == passive["path_calls"]
    assert instrumented["shell_calls"] == passive["shell_calls"]
    assert instrumented["signature"] == passive["signature"]
    assert instrumented["method_signatures"] == passive["method_signatures"]
    assert instrumented["http_method_signatures"] == passive["http_method_signatures"]
    assert instrumented["audit_counts"] == passive["audit_counts"]
    assert instrumented["signature"][:3] == ["args", "bufsize", "executable"]
    analysis = analyze_runpack(instrumented_runpack)
    assert analysis.semantic_capture is not None
    assert analysis.semantic_capture.status == "partial"
    assert sum(call.shell is None for call in analysis.subprocess_calls) == 1
    assert "shell use unknown" in render_analysis(analysis, "text")
    if capture_level == "sample":
        known_runpack = tmp_path / "known.runpack"
        known_exit = record_process(
            (
                sys.executable,
                "-c",
                "import subprocess,sys; subprocess.run([sys.executable, '-c', 'pass'])",
            ),
            known_runpack,
            name="known",
            capture_level="sample",
        )
        contract = tmp_path / "identity-contract.yaml"
        contract.write_text(
            "name: identity-completeness\nassertions:\n"
            "  - type: max_operation_count\n"
            f"    operation: {Path(sys.executable).name}\n"
            "    relative_to: baseline\n"
            "    factor: 1\n",
            encoding="utf-8",
        )

        verification = verify_contracts(contract, known_runpack, instrumented_runpack)

        assert known_exit == 0
        assert verification.results[0].status == "unverifiable"
        assert "candidate: subprocess capture partial" in verification.results[0].observed


def test_truncated_semantic_capture_makes_operation_contract_unverifiable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(deep_profile_module, "MAX_SEMANTIC_SUBPROCESS_EVENTS", 1)
    baseline = tmp_path / "bounded-baseline.runpack"
    candidate = tmp_path / "bounded-candidate.runpack"
    record_process(
        (sys.executable, "-c", "pass"),
        baseline,
        name="baseline",
        capture_level="sample",
    )
    record_process(
        (
            sys.executable,
            "-c",
            "import subprocess,sys; "
            "subprocess.run([sys.executable, '-c', 'pass']); "
            "subprocess.run([sys.executable, '-c', 'pass'])",
        ),
        candidate,
        name="candidate",
        capture_level="sample",
    )
    operation_name = Path(sys.executable).name
    contract = tmp_path / "bounded-subprocess-contract.yaml"
    contract.write_text(
        "name: subprocess-budget\nassertions:\n"
        "  - type: max_operation_count\n"
        f"    operation: {operation_name}\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )

    diff = compare_runpacks(baseline, candidate)
    verification = verify_contracts(contract, baseline, candidate)

    assert diff.candidate_semantic_capture_status == "truncated"
    assert diff.candidate_dropped_subprocess_count == 1
    assert verification.results[0].status == "unverifiable"
    assert verification.results[0].observed == (
        "subprocess evidence incomplete (candidate: subprocess capture truncated, 1 omitted)"
    )


@pytest.mark.parametrize(
    ("capture_level", "expected_instrument", "expects_process_observer"),
    (
        ("passive", None, False),
        ("process", None, True),
        ("sample", "sample", True),
        ("deep", "deep", True),
    ),
)
def test_capture_level_presets_form_one_progressive_zero_code_ladder(
    tmp_path: Path,
    capture_level: str,
    expected_instrument: str | None,
    expects_process_observer: bool,
) -> None:
    runpack = tmp_path / f"{capture_level}.runpack"

    record_process(
        (
            sys.executable,
            "-c",
            "import time\ndef useful(): time.sleep(0.22)\nuseful()",
        ),
        runpack,
        name=capture_level,
        capture_level=capture_level,
    )

    with RunpackReader(runpack) as reader:
        capture = reader.execution().metadata["capture"]
        event_kinds = {event.kind for event in reader.events()}
    assert isinstance(capture, dict)
    assert capture["level"] == capture_level
    assert ("process_observer" in capture) is expects_process_observer
    instrumentation = capture.get("instrumentation")
    if expected_instrument is None:
        assert instrumentation is None
    else:
        assert isinstance(instrumentation, dict)
        assert instrumentation["mode"] == expected_instrument
        assert instrumentation["status"] == "complete"
        expected_kind = (
            "python.stack.sample" if expected_instrument == "sample" else "python.call.aggregate"
        )
        assert expected_kind in event_kinds
    if expects_process_observer:
        observer = capture["process_observer"]
        assert isinstance(observer, dict)
        assert observer["requested"] is True
        assert observer["status"] == "complete"


def test_capture_level_rejects_low_level_overrides_before_running_workload(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "conflict.runpack"

    with pytest.raises(CaptureError, match="cannot be combined"):
        record_process(
            (sys.executable, "-c", "raise RuntimeError('must not run')"),
            runpack,
            name="conflict",
            capture_level="sample",
            instrument="deep",
        )

    assert not runpack.exists()


def test_capture_level_rejects_unknown_presets_before_running_workload(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unknown-level.runpack"

    with pytest.raises(CaptureError, match="capture level must be one of"):
        record_process(
            (sys.executable, "-c", "raise RuntimeError('must not run')"),
            runpack,
            name="unknown-level",
            capture_level="everything",
        )

    assert not runpack.exists()


def test_deep_capture_remains_explicit_when_python_disables_site_startup(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "disabled.runpack"

    exit_code = record_process(
        (sys.executable, "-S", "-c", "print('unprofiled')"),
        runpack,
        name="disabled",
        instrument="deep",
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        deep_events = tuple(
            event for event in reader.events() if event.kind == "python.call.aggregate"
        )
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    assert exit_code == 0
    assert instrumentation["status"] == "unavailable"
    assert instrumentation["process_count"] == 0
    assert deep_events == ()
    report = render_analysis(analyze_runpack(runpack), "text")
    assert "no Python profile was observed" in report


def test_sampling_capture_remains_explicit_when_python_disables_site_startup(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "disabled-sample.runpack"

    exit_code = record_process(
        (sys.executable, "-S", "-c", "print('unsampled')"),
        runpack,
        name="disabled-sample",
        instrument="sample",
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        sample_events = tuple(
            event for event in reader.events() if event.kind == "python.stack.sample"
        )
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    assert exit_code == 0
    assert instrumentation["mode"] == "sample"
    assert instrumentation["status"] == "unavailable"
    assert instrumentation["process_count"] == 0
    assert sample_events == ()
    report = render_analysis(analyze_runpack(runpack), "text")
    assert "no Python samples were observed" in report


def test_record_process_rejects_unknown_instrumentation_without_starting_workload(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "invalid.runpack"

    try:
        record_process(
            (sys.executable, "-c", "raise RuntimeError('must not run')"),
            runpack,
            name="invalid",
            instrument="everything",
        )
    except ValueError as exc:
        assert str(exc) == "instrument must be 'sample', 'deep', or None"
    else:
        raise AssertionError("unknown instrumentation was accepted")
    assert not runpack.exists()


def test_deep_profile_normalization_marks_function_budget_truncation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [
            {
                "id": index,
                "module": "application",
                "qualname": name,
                "filename": "/work/workload.py",
                "firstlineno": index + 1,
                "scope": "application",
                "call_count": index + 2,
                "exception_count": 5 if index == 0 else 1,
                "total_ns": 100,
                "self_ns": 100,
                "max_ns": 100,
            }
            for index, name in enumerate(("first", "second"))
        ],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))
    monkeypatch.setattr(deep_profile_module, "MAX_DEEP_PROFILE_FUNCTIONS", 1)

    result = load_deep_profile(session, entity_id="process")

    assert result.truncated is True
    assert result.dropped_call_count == 2
    assert result.dropped_python_exception_event_count == 5
    assert result.python_exception_event_count == 1
    assert result.python_exception_filter_status == "unavailable"
    assert result.observer_integrity_status == "unavailable"
    assert [event.name for event in result.events] == ["application.second"]
    metadata = result.as_metadata()
    assert metadata["status"] == "truncated"
    exception_capture = metadata["python_exception_capture"]
    assert isinstance(exception_capture, dict)
    assert exception_capture["dropped_event_count"] == 5
    assert "control_flow_filter" not in exception_capture
    observer_integrity = metadata["observer_integrity"]
    assert isinstance(observer_integrity, dict)
    assert observer_integrity["status"] == "unavailable"


def test_deep_profile_normalization_tracks_non_control_flow_exception_drops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "dropped_exception_event_count": 0,
        "dropped_non_control_flow_exception_event_count": 0,
        "callback_error_count": 0,
        "python_exception_filter": {
            "format_version": 1,
            "event_semantics": "exact_type_identity",
            "filtered_exception_types": [
                "GeneratorExit",
                "StopAsyncIteration",
                "StopIteration",
            ],
            "exception_type_identity_inspected": True,
            "exception_types_captured": False,
        },
        "functions": [
            {
                "id": index,
                "module": "application",
                "qualname": name,
                "filename": "/work/workload.py",
                "firstlineno": index + 1,
                "scope": "application",
                "call_count": index + 2,
                "exception_count": 5 if index == 0 else 1,
                "non_control_flow_exception_count": 5 if index == 0 else 1,
                "total_ns": 100,
                "self_ns": 100,
                "max_ns": 100,
            }
            for index, name in enumerate(("first", "second"))
        ],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))
    monkeypatch.setattr(deep_profile_module, "MAX_DEEP_PROFILE_FUNCTIONS", 1)

    result = load_deep_profile(session, entity_id="process")

    assert result.python_exception_filter_status == "complete"
    assert result.python_non_control_flow_exception_event_count == 1
    assert result.dropped_python_non_control_flow_exception_event_count == 5
    metadata = result.as_metadata()
    exception_capture = metadata["python_exception_capture"]
    assert isinstance(exception_capture, dict)
    control_flow_filter = exception_capture["control_flow_filter"]
    assert isinstance(control_flow_filter, dict)
    assert control_flow_filter["status"] == "truncated"
    assert control_flow_filter["non_control_flow_event_count"] == 1
    assert control_flow_filter["dropped_non_control_flow_event_count"] == 5


@pytest.mark.parametrize(
    ("filename", "native", "exception_count", "message"),
    (
        (
            "/work/workload.py",
            True,
            0,
            "deep-profile native function identity is inconsistent",
        ),
        (
            "<native>",
            True,
            2,
            "deep-profile function timings are inconsistent",
        ),
    ),
)
def test_deep_profile_rejects_inconsistent_native_call_evidence(
    tmp_path: Path,
    filename: str,
    native: bool,
    exception_count: int,
    message: str,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [
            {
                "id": 0,
                "module": "native_driver",
                "qualname": "execute",
                "filename": filename,
                "firstlineno": 0,
                "scope": "library",
                "native": native,
                "call_count": 1,
                "exception_count": exception_count,
                "total_ns": 100,
                "self_ns": 100,
                "max_ns": 100,
            }
        ],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(DeepProfileError, match=message):
        load_deep_profile(session, entity_id="process")


def test_deep_profile_rejects_unknown_observer_integrity_version(tmp_path: Path) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "observer_integrity": {
            "format_version": 2,
            "profile_hook_setter_call_count": 0,
            "trace_hook_setter_call_count": 0,
        },
        "functions": [],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(
        DeepProfileError,
        match="observer integrity format version is unsupported",
    ):
        load_deep_profile(session, entity_id="process")


def test_deep_profile_rejects_unknown_python_exception_filter_version(
    tmp_path: Path,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "dropped_exception_event_count": 0,
        "dropped_non_control_flow_exception_event_count": 0,
        "callback_error_count": 0,
        "python_exception_filter": {
            "format_version": 2,
            "event_semantics": "exact_type_identity",
            "filtered_exception_types": [
                "GeneratorExit",
                "StopAsyncIteration",
                "StopIteration",
            ],
            "exception_type_identity_inspected": True,
            "exception_types_captured": False,
        },
        "functions": [],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(
        DeepProfileError,
        match="Python exception filter format version is unsupported",
    ):
        load_deep_profile(session, entity_id="process")


def test_sample_profile_normalization_marks_function_budget_truncation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = {
        "format_version": 1,
        "mode": "sample",
        "pid": 42,
        "interval_ns": 10_000_000,
        "truncated": False,
        "sample_count": 4,
        "thread_sample_count": 4,
        "dropped_frame_sample_count": 0,
        "dropped_edge_sample_count": 0,
        "callback_error_count": 0,
        "functions": [
            {
                "id": index,
                "module": "application",
                "qualname": name,
                "filename": "/work/workload.py",
                "firstlineno": index + 1,
                "scope": "application",
                "sample_count": index + 2,
                "leaf_sample_count": index + 1,
            }
            for index, name in enumerate(("first", "second"))
        ],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), "sample")
    monkeypatch.setattr(deep_profile_module, "MAX_DEEP_PROFILE_FUNCTIONS", 1)

    result = load_sample_profile(session, entity_id="process")

    assert result.truncated is True
    assert result.dropped_call_count == 2
    assert [event.name for event in result.events] == ["application.second"]
    assert result.as_metadata()["status"] == "truncated"
    assert result.as_metadata()["dropped_frame_sample_count"] == 2


def test_semantic_subprocess_normalization_keeps_failures_at_the_global_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = {
        "format_version": 1,
        "mode": "sample",
        "pid": 42,
        "interval_ns": 10_000_000,
        "truncated": False,
        "sample_count": 0,
        "thread_sample_count": 0,
        "dropped_frame_sample_count": 0,
        "dropped_edge_sample_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
        "semantic_capture": {
            "format_version": 1,
            "observer": "python-subprocess-wrapper",
            "subprocess_count": 2,
            "dropped_subprocess_count": 0,
            "callback_error_count": 0,
            "limits": {"max_subprocesses": 256},
            "subprocesses": [
                {
                    "id": 0,
                    "name": "successful",
                    "parent_pid": 42,
                    "child_pid": 43,
                    "shell": False,
                    "started_at_ns": 100,
                    "duration_ns": 1_000_000,
                    "exit_code": 0,
                    "outcome": "exited",
                },
                {
                    "id": 1,
                    "name": "missing",
                    "parent_pid": 42,
                    "child_pid": None,
                    "shell": False,
                    "started_at_ns": 200,
                    "duration_ns": 1,
                    "exit_code": None,
                    "outcome": "launch_error",
                    "error_type": "FileNotFoundError",
                },
            ],
        },
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), "sample")
    monkeypatch.setattr(deep_profile_module, "MAX_SEMANTIC_SUBPROCESS_EVENTS", 1)

    result = load_sample_profile(session, entity_id="process", root_process_id=42)

    assert [(event.kind, event.name) for event in result.events] == [("subprocess.run", "missing")]
    metadata = result.as_metadata()
    semantic = metadata["semantic_capture"]
    assert isinstance(semantic, dict)
    assert semantic["status"] == "truncated"
    assert semantic["subprocess_count"] == 1
    assert semantic["dropped_subprocess_count"] == 1
    http_capture = metadata["http_capture"]
    assert isinstance(http_capture, dict)
    assert http_capture["status"] == "unavailable"
    assert metadata["function_count"] == 0


def test_invalid_semantic_subprocess_payload_does_not_discard_valid_profile(
    tmp_path: Path,
) -> None:
    document = {
        "format_version": 1,
        "mode": "sample",
        "pid": 42,
        "interval_ns": 10_000_000,
        "truncated": False,
        "sample_count": 1,
        "thread_sample_count": 1,
        "dropped_frame_sample_count": 0,
        "dropped_edge_sample_count": 0,
        "callback_error_count": 0,
        "functions": [
            {
                "id": 0,
                "module": "application",
                "qualname": "work",
                "filename": "/work/workload.py",
                "firstlineno": 1,
                "scope": "application",
                "sample_count": 1,
                "leaf_sample_count": 1,
            }
        ],
        "edges": [],
        "semantic_capture": {
            "format_version": 1,
            "observer": "python-subprocess-wrapper",
            "subprocess_count": 1,
            "dropped_subprocess_count": 0,
            "callback_error_count": 0,
            "limits": {"max_subprocesses": 256},
            "subprocesses": [],
        },
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), "sample")

    result = load_sample_profile(session, entity_id="process", root_process_id=42)

    assert [event.name for event in result.events] == ["application.work"]
    metadata = result.as_metadata()
    semantic = metadata["semantic_capture"]
    assert isinstance(semantic, dict)
    assert semantic["status"] == "invalid"
    assert metadata["status"] == "complete"


def test_invalid_semantic_caller_does_not_discard_valid_subprocess_evidence(
    tmp_path: Path,
) -> None:
    document = {
        "format_version": 1,
        "mode": "sample",
        "pid": 42,
        "interval_ns": 10_000_000,
        "truncated": False,
        "sample_count": 0,
        "thread_sample_count": 0,
        "dropped_frame_sample_count": 0,
        "dropped_edge_sample_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
        "semantic_capture": {
            "format_version": 1,
            "observer": "python-subprocess-wrapper",
            "subprocess_count": 1,
            "dropped_subprocess_count": 0,
            "callback_error_count": 0,
            "caller_callback_error_count": 0,
            "limits": {"max_subprocesses": 256},
            "subprocesses": [
                {
                    "id": 0,
                    "name": "python",
                    "parent_pid": 42,
                    "child_pid": 43,
                    "shell": False,
                    "started_at_ns": 100,
                    "duration_ns": 1_000_000,
                    "exit_code": 0,
                    "outcome": "exited",
                    "caller": {
                        "module": 7,
                        "qualname": "launch",
                        "filename": "/work/work.py",
                        "firstlineno": 4,
                        "scope": "application",
                        "observation": "sampled",
                    },
                }
            ],
        },
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), "sample")

    result = load_sample_profile(session, entity_id="process", root_process_id=42)

    assert [(event.kind, event.name) for event in result.events] == [("subprocess.run", "python")]
    assert result.edges == ()
    semantic = result.as_metadata()["semantic_capture"]
    assert isinstance(semantic, dict)
    assert semantic["status"] == "complete"
    caller = semantic["caller_attribution"]
    assert isinstance(caller, dict)
    assert caller["status"] == "invalid"
    assert caller["invalid_caller_count"] == 1
    assert caller["attributed_subprocess_count"] == 0
    assert caller["unattributed_subprocess_count"] == 1


def test_invalid_http_adapter_metadata_does_not_discard_valid_subprocess_evidence(
    tmp_path: Path,
) -> None:
    document = {
        "format_version": 1,
        "mode": "sample",
        "pid": 42,
        "interval_ns": 10_000_000,
        "truncated": False,
        "sample_count": 0,
        "thread_sample_count": 0,
        "dropped_frame_sample_count": 0,
        "dropped_edge_sample_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
        "semantic_capture": {
            "format_version": 2,
            "observer": "python-runtime-boundary-wrapper",
            "subprocess_count": 1,
            "dropped_subprocess_count": 0,
            "callback_error_count": 0,
            "caller_callback_error_count": 0,
            "http_request_count": 0,
            "dropped_http_request_count": 0,
            "http_callback_error_count": 0,
            "http_caller_callback_error_count": 0,
            "http_adapters": ["stdlib.http.client", "unsafe.native"],
            "server_identity_policy": "redact",
            "limits": {"max_subprocesses": 256, "max_http_requests": 256},
            "subprocesses": [
                {
                    "id": 0,
                    "name": "python",
                    "parent_pid": 42,
                    "child_pid": 43,
                    "shell": False,
                    "started_at_ns": 100,
                    "duration_ns": 1_000_000,
                    "exit_code": 0,
                    "outcome": "exited",
                    "caller": None,
                }
            ],
            "http_requests": [],
        },
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), "sample")

    result = load_sample_profile(session, entity_id="process", root_process_id=42)

    assert [(event.kind, event.name) for event in result.events] == [("subprocess.run", "python")]
    metadata = result.as_metadata()
    semantic = metadata["semantic_capture"]
    http = metadata["http_capture"]
    network = metadata["network_capture"]
    assert isinstance(semantic, dict)
    assert isinstance(http, dict)
    assert isinstance(network, dict)
    assert semantic["status"] == "complete"
    assert semantic["subprocess_count"] == 1
    assert http["status"] == "invalid"
    assert http["request_count"] == 0
    assert network["status"] == "unavailable"
    assert network["connection_count"] == 0
    assert metadata["status"] == "complete"


def test_invalid_optional_logical_adapter_category_does_not_discard_profile(
    tmp_path: Path,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
        "semantic_capture": {
            "format_version": 2,
            "observer": "python-runtime-boundary-wrapper",
            "logical_operation_capture_enabled": True,
            "logical_operation_count": 1,
            "dropped_logical_operation_count": 0,
            "logical_operation_callback_error_count": 0,
            "logical_operation_caller_callback_error_count": 0,
            "logical_operation_adapters": ["redis.Redis"],
            "limits": {"max_logical_operations": 256},
            "logical_operations": [
                {
                    "id": 0,
                    "category": "database",
                    "operation": "execute",
                    "adapter": "redis.Redis",
                    "parent_pid": 42,
                    "started_at_ns": 100,
                    "duration_ns": 5,
                    "outcome": "completed",
                    "caller": None,
                }
            ],
        },
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), "deep")

    result = load_deep_profile(session, entity_id="process", root_process_id=42)

    assert result.events == ()
    metadata = result.as_metadata()
    logical = metadata["logical_operation_capture"]
    assert isinstance(logical, dict)
    assert logical["status"] == "invalid"
    assert logical["operation_count"] == 0
    assert metadata["status"] == "complete"


@pytest.mark.parametrize("adapter", ("stdlib.wsgiref", "uvicorn.h11", "uvicorn.httptools"))
def test_server_request_without_status_retains_duration_but_marks_capture_partial(
    tmp_path: Path,
    adapter: str,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
        "semantic_capture": {
            "format_version": 2,
            "observer": "python-runtime-boundary-wrapper",
            "logical_operation_capture_enabled": True,
            "logical_operation_count": 1,
            "dropped_logical_operation_count": 0,
            "logical_operation_callback_error_count": 0,
            "logical_operation_caller_callback_error_count": 0,
            "logical_operation_adapters": [adapter],
            "limits": {"max_logical_operations": 256},
            "logical_operations": [
                {
                    "id": 0,
                    "category": "server",
                    "operation": "request",
                    "adapter": adapter,
                    "parent_pid": 42,
                    "started_at_ns": 100,
                    "duration_ns": 5,
                    "outcome": "completed",
                    "status_code": None,
                    "caller": None,
                }
            ],
        },
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), "deep")

    result = load_deep_profile(session, entity_id="process", root_process_id=42)

    request = next(event for event in result.events if event.kind == "server.request")
    assert request.finished_at_ns == 105
    assert "status_code" not in request.attributes
    logical = result.as_metadata()["logical_operation_capture"]
    assert isinstance(logical, dict)
    assert logical["status"] == "partial"
    assert logical["operation_count"] == 1


def test_deep_profile_reports_bounded_ranking_workspace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))
    monkeypatch.setattr(deep_profile_module, "MAX_PROFILE_RANKING_BYTES", 4_096)

    with pytest.raises(DeepProfileError, match="bounded profile-ranking workspace"):
        load_deep_profile(session, entity_id="process")


def test_exact_ranker_uses_capture_local_storage_without_a_temp_sort(tmp_path: Path) -> None:
    with deep_profile_module._AggregateRanker(tmp_path) as ranker:
        ranker.add(b"a", b"a", 2, 0, "test rank")
        ranker.add(b"b", b"b", 3, 0, "test rank")
        ranker.add(b"c", b"c", 1, 0, "test rank")

        selected = ranker.selected_keys(2)
        query_plan = ranker._connection.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT candidate_key, primary_score, secondary_score FROM candidates"
        ).fetchall()
        database_entries = ranker._connection.execute("PRAGMA database_list").fetchall()

        assert selected == {b"a", b"b"}
        assert database_entries == [(0, "main", str(ranker._path))]
        assert ranker._path.parent == tmp_path
        assert ranker._path.stat().st_mode & 0o777 == 0o600
        assert not any("TEMP B-TREE" in str(row) for row in query_plan)

    assert tuple(tmp_path.iterdir()) == ()


def test_malformed_publication_metrics_do_not_discard_valid_profile(tmp_path: Path) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "publication_metrics_version": 1,
        "publication_fallback": {
            "socket_attempted": False,
            "socket_failure_ns": 1,
            "snapshot_kind": "final",
        },
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    result = load_deep_profile(session, entity_id="process")

    assert result.process_count == 1
    assert result.publication_metrics_status == "invalid"
    assert result.as_metadata()["status"] == "complete"
    publication_metrics = result.as_metadata()["publication_metrics"]
    assert isinstance(publication_metrics, dict)
    assert publication_metrics["status"] == "invalid"


def test_deep_profile_rejects_aggregate_ranking_integer_overflow(tmp_path: Path) -> None:
    maximum = (1 << 63) - 1
    for pid, score in ((1, maximum), (2, 1)):
        document = {
            "format_version": 1,
            "pid": pid,
            "truncated": False,
            "dropped_call_count": 0,
            "dropped_edge_count": 0,
            "callback_error_count": 0,
            "functions": [
                {
                    "id": 0,
                    "module": "application",
                    "qualname": "overflow",
                    "filename": "/work/workload.py",
                    "firstlineno": 1,
                    "scope": "application",
                    "call_count": 1,
                    "total_ns": score,
                    "self_ns": score,
                    "max_ns": score,
                }
            ],
            "edges": [],
        }
        (tmp_path / f"profile-{pid}.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(
        DeepProfileError,
        match="aggregate function rank exceeds the supported integer range",
    ):
        load_deep_profile(session, entity_id="process")


@pytest.mark.parametrize("mode", ("deep", "sample"))
def test_profile_function_retention_is_usefulness_ranked_across_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: deep_profile_module.PythonProfileMode,
) -> None:
    common = {
        "format_version": 1,
        "mode": mode,
        "interval_ns": 10_000_000,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "sample_count": 8,
        "thread_sample_count": 8,
        "dropped_frame_sample_count": 0,
        "dropped_edge_sample_count": 0,
        "callback_error_count": 0,
        "edges": [],
    }
    early = {
        **common,
        "pid": 1,
        "functions": [
            {
                "id": 0,
                "module": "application",
                "qualname": "cold",
                "filename": "/work/workload.py",
                "firstlineno": 1,
                "scope": "application",
                "call_count": 2,
                "total_ns": 10,
                "self_ns": 10,
                "max_ns": 10,
                "sample_count": 2,
                "leaf_sample_count": 1,
            },
            {
                "id": 1,
                "module": "application",
                "qualname": "hot",
                "filename": "/work/workload.py",
                "firstlineno": 2,
                "scope": "application",
                "call_count": 3,
                "total_ns": 50,
                "self_ns": 50,
                "max_ns": 50,
                "sample_count": 3,
                "leaf_sample_count": 2,
            },
        ],
    }
    late = {
        **common,
        "pid": 9,
        "functions": [
            {
                "id": 0,
                "module": "application",
                "qualname": "hot",
                "filename": "/work/workload.py",
                "firstlineno": 2,
                "scope": "application",
                "call_count": 5,
                "total_ns": 1_000,
                "self_ns": 1_000,
                "max_ns": 1_000,
                "sample_count": 5,
                "leaf_sample_count": 5,
            }
        ],
    }
    (tmp_path / "profile-1.json").write_text(json.dumps(early), encoding="utf-8")
    (tmp_path / "profile-9.json").write_text(json.dumps(late), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), mode)
    monkeypatch.setattr(deep_profile_module, "MAX_DEEP_PROFILE_FUNCTIONS", 1)

    if mode == "deep":
        default_order = load_deep_profile(session, entity_id="process")
        root_first = load_deep_profile(session, entity_id="process", root_process_id=9)
        count_key = "call_count"
    else:
        default_order = load_sample_profile(session, entity_id="process")
        root_first = load_sample_profile(session, entity_id="process", root_process_id=9)
        count_key = "sample_count"

    for result in (default_order, root_first):
        assert [event.name for event in result.events] == ["application.hot"]
        assert result.events[0].attributes[count_key] == 8
        processes = result.events[0].attributes["processes"]
        assert isinstance(processes, list)
        assert [process["pid"] for process in processes if isinstance(process, dict)] == [1, 9]
        assert result.dropped_call_count == 2
        assert result.truncated is True


@pytest.mark.parametrize("mode", ("deep", "sample"))
def test_profile_function_retention_uses_the_fleet_wide_aggregate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: deep_profile_module.PythonProfileMode,
) -> None:
    def function(
        identifier: int,
        name: str,
        score: int,
        count: int,
    ) -> dict[str, object]:
        return {
            "id": identifier,
            "module": "application",
            "qualname": name,
            "filename": "/work/workload.py",
            "firstlineno": identifier + 1,
            "scope": "application",
            "call_count": count,
            "total_ns": score,
            "self_ns": score,
            "max_ns": score,
            "sample_count": score,
            "leaf_sample_count": score,
        }

    for pid in (1, 2, 3):
        functions = [function(0, "steady", 60, 1)]
        if pid == 1:
            functions.append(function(1, "spike", 100, 1))
        document = {
            "format_version": 1,
            "mode": mode,
            "pid": pid,
            "interval_ns": 10_000_000,
            "truncated": False,
            "dropped_call_count": 0,
            "dropped_edge_count": 0,
            "sample_count": 160 if pid == 1 else 60,
            "thread_sample_count": 1,
            "dropped_frame_sample_count": 0,
            "dropped_edge_sample_count": 0,
            "callback_error_count": 0,
            "functions": functions,
            "edges": [],
        }
        (tmp_path / f"profile-{pid}.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), mode)
    monkeypatch.setattr(deep_profile_module, "MAX_DEEP_PROFILE_FUNCTIONS", 1)

    result = (
        load_deep_profile(session, entity_id="process")
        if mode == "deep"
        else load_sample_profile(session, entity_id="process")
    )

    assert [event.name for event in result.events] == ["application.steady"]
    count_key = "call_count" if mode == "deep" else "sample_count"
    assert result.events[0].attributes[count_key] == (3 if mode == "deep" else 180)
    processes = result.events[0].attributes["processes"]
    assert isinstance(processes, list)
    assert [process["pid"] for process in processes if isinstance(process, dict)] == [1, 2, 3]
    assert result.dropped_call_count == (1 if mode == "deep" else 100)
    assert result.truncated is True


@pytest.mark.parametrize("mode", ("deep", "sample"))
def test_profile_edge_retention_uses_the_fleet_wide_aggregate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: deep_profile_module.PythonProfileMode,
) -> None:
    functions = [
        {
            "id": identifier,
            "module": "application",
            "qualname": name,
            "filename": "/work/workload.py",
            "firstlineno": identifier + 1,
            "scope": "application",
            "call_count": 1,
            "total_ns": 1,
            "self_ns": 1,
            "max_ns": 1,
            "sample_count": 1,
            "leaf_sample_count": 1,
        }
        for identifier, name in enumerate(("parent", "steady", "spike"))
    ]
    for pid in (1, 2, 3):
        edges = [
            {
                "source_id": 0,
                "target_id": 1,
                "call_count": 60,
                "total_ns": 60,
                "sample_count": 60,
            }
        ]
        if pid == 1:
            edges.append(
                {
                    "source_id": 0,
                    "target_id": 2,
                    "call_count": 100,
                    "total_ns": 100,
                    "sample_count": 100,
                }
            )
        document = {
            "format_version": 1,
            "mode": mode,
            "pid": pid,
            "interval_ns": 10_000_000,
            "truncated": False,
            "dropped_call_count": 0,
            "dropped_edge_count": 0,
            "sample_count": 3,
            "thread_sample_count": 1,
            "dropped_frame_sample_count": 0,
            "dropped_edge_sample_count": 0,
            "callback_error_count": 0,
            "functions": functions,
            "edges": edges,
        }
        (tmp_path / f"profile-{pid}.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), mode)
    monkeypatch.setattr(deep_profile_module, "MAX_DEEP_PROFILE_EDGES", 1)

    result = (
        load_deep_profile(session, entity_id="process")
        if mode == "deep"
        else load_sample_profile(session, entity_id="process")
    )

    event_names = {event.id: event.name for event in result.events}
    assert len(result.edges) == 1
    assert event_names[result.edges[0].target_event_id] == "application.steady"
    count_key = "call_count" if mode == "deep" else "sample_count"
    assert result.edges[0].attributes[count_key] == 180
    assert result.dropped_edge_count == 100
    assert result.truncated is True


@pytest.mark.parametrize("mode", ("deep", "sample"))
def test_profile_edge_retention_is_usefulness_ranked_across_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: deep_profile_module.PythonProfileMode,
) -> None:
    def function(identifier: int, name: str) -> dict[str, object]:
        return {
            "id": identifier,
            "module": "application",
            "qualname": name,
            "filename": "/work/workload.py",
            "firstlineno": identifier + 1,
            "scope": "application",
            "call_count": 1,
            "total_ns": 1,
            "self_ns": 1,
            "max_ns": 1,
            "sample_count": 1,
            "leaf_sample_count": 1,
        }

    functions = [function(0, "parent"), function(1, "hot"), function(2, "cold")]
    common = {
        "format_version": 1,
        "mode": mode,
        "interval_ns": 10_000_000,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "sample_count": 3,
        "thread_sample_count": 3,
        "dropped_frame_sample_count": 0,
        "dropped_edge_sample_count": 0,
        "callback_error_count": 0,
        "functions": functions,
    }
    documents = (
        {
            **common,
            "pid": 1,
            "edges": [
                {
                    "source_id": 0,
                    "target_id": 2,
                    "call_count": 1,
                    "total_ns": 2,
                    "sample_count": 2,
                },
                {
                    "source_id": 0,
                    "target_id": 1,
                    "call_count": 2,
                    "total_ns": 1,
                    "sample_count": 1,
                },
            ],
        },
        {
            **common,
            "pid": 9,
            "edges": [
                {
                    "source_id": 0,
                    "target_id": 1,
                    "call_count": 3,
                    "total_ns": 100,
                    "sample_count": 100,
                }
            ],
        },
    )
    for document in documents:
        (tmp_path / f"profile-{document['pid']}.json").write_text(
            json.dumps(document), encoding="utf-8"
        )
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), mode)
    monkeypatch.setattr(deep_profile_module, "MAX_DEEP_PROFILE_EDGES", 1)

    result = (
        load_deep_profile(session, entity_id="process")
        if mode == "deep"
        else load_sample_profile(session, entity_id="process")
    )

    event_names = {event.id: event.name for event in result.events}
    assert len(result.edges) == 1
    assert event_names[result.edges[0].source_event_id] == "application.parent"
    assert event_names[result.edges[0].target_event_id] == "application.hot"
    if mode == "deep":
        assert result.edges[0].attributes == {
            "source": "deep-profile",
            "call_count": 5,
            "total_seconds": 101 / 1_000_000_000,
        }
        assert result.dropped_edge_count == 1
    else:
        assert result.edges[0].attributes == {
            "source": "python-sampler",
            "sample_count": 101,
            "estimated_seconds": 1.01,
        }
        assert result.dropped_edge_count == 2
    assert result.truncated is True


@pytest.mark.parametrize("instrument", ("sample", "deep"))
def test_performance_comparison_rejects_mixed_instrumentation_modes(
    tmp_path: Path,
    instrument: str,
) -> None:
    baseline = tmp_path / "passive.runpack"
    candidate = tmp_path / f"{instrument}.runpack"
    command = (sys.executable, "-c", "sum(range(1000))")
    record_process(command, baseline, name="passive")
    record_process(command, candidate, name=instrument, instrument=instrument)
    contract = tmp_path / "contract.yaml"
    contract.write_text(
        "name: timing\nassertions:\n"
        "  - type: max_runtime_regression\n"
        "    name: comparable-runtime\n"
        "    percent: 100\n",
        encoding="utf-8",
    )

    diff = compare_runpacks(baseline, candidate)
    verification = verify_contracts(contract, baseline, candidate)

    assert diff.baseline_instrumentation_mode == "passive"
    assert diff.candidate_instrumentation_mode == instrument
    assert diff.timing_comparable is False
    assert diff.as_json_value()["instrumentation"] == {
        "baseline_mode": "passive",
        "candidate_mode": instrument,
        "timing_comparable": False,
    }
    assert diff.operation_count_changes == ()
    assert diff.edge_count_changes == ()
    assert (
        f"timing is not comparable across passive and {instrument} instrumentation"
        in render_diff(diff, "text")
    )
    assert verification.results[0].status == "unverifiable"
    assert verification.results[0].observed == (
        f"timing instrumentation differs (baseline=passive, candidate={instrument})"
    )


def test_deep_profile_setup_failure_is_a_public_capture_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_setup(_parent: Path) -> DeepProfileSession:
        raise DeepProfileError("profile bootstrap unavailable")

    monkeypatch.setattr(capture_module, "prepare_deep_profile_session", fail_setup)

    with pytest.raises(CaptureError, match="profile bootstrap unavailable"):
        record_process(
            (sys.executable, "-c", "pass"),
            tmp_path / "failed.runpack",
            name="failed",
            instrument="deep",
        )


def test_sample_profile_setup_failure_is_a_public_capture_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_setup(_parent: Path) -> DeepProfileSession:
        raise DeepProfileError("sampling bootstrap unavailable")

    monkeypatch.setattr(capture_module, "prepare_sample_profile_session", fail_setup)

    with pytest.raises(CaptureError, match="sampling bootstrap unavailable"):
        record_process(
            (sys.executable, "-c", "pass"),
            tmp_path / "failed-sample.runpack",
            name="failed-sample",
            instrument="sample",
        )


def test_deep_capture_resets_aggregates_in_a_forked_child(tmp_path: Path) -> None:
    runpack = tmp_path / "fork.runpack"
    source = """
import os
import sys

def before_fork():
    return 1

def child_only():
    try:
        raise RuntimeError("".join(("fork", "-exception-secret")))
    except RuntimeError:
        return 2

before_fork()
sys.settrace(None)
pid = os.fork()
if pid == 0:
    child_only()
    raise SystemExit(0)
os.waitpid(pid, 0)
"""

    record_process(
        (sys.executable, "-c", source),
        runpack,
        name="fork",
        instrument="deep",
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        events = {event.name: event for event in reader.events()}
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    assert instrumentation["process_count"] == 2
    integrity = instrumentation["observer_integrity"]
    assert isinstance(integrity, dict)
    assert integrity["status"] == "partial"
    assert integrity["process_count"] == 2
    assert integrity["trace_hook_setter_call_count"] == 1
    assert integrity["trace_hook_setter_process_count"] == 1
    assert events["__main__.before_fork"].attributes["call_count"] == 1
    assert events["__main__.child_only"].attributes["call_count"] == 1
    assert events["__main__.child_only"].attributes["exception_count"] == 1
    assert events["__main__.child_only"].attributes["non_control_flow_exception_count"] == 1
    assert b"fork-exception-secret" not in runpack.read_bytes()


def test_sampling_capture_restarts_in_a_forked_child(tmp_path: Path) -> None:
    runpack = tmp_path / "sample-fork.runpack"
    source = """
import os
import time

def parent_only():
    time.sleep(0.12)

def child_only():
    time.sleep(0.12)

parent_only()
pid = os.fork()
if pid == 0:
    child_only()
    raise SystemExit(0)
os.waitpid(pid, 0)
"""

    record_process(
        (sys.executable, "-c", source),
        runpack,
        name="sample-fork",
        instrument="sample",
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        events = {event.name: event for event in reader.events()}
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    assert isinstance(instrumentation, dict)
    assert instrumentation["process_count"] == 2
    assert events["__main__.parent_only"].attributes["process_count"] == 1
    assert events["__main__.child_only"].attributes["process_count"] == 1


def test_deep_capture_attributes_one_shared_function_to_parent_and_child_processes(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "deep-process-attribution.runpack"
    source = """
import subprocess
import sys

function_source = "import time\\ndef shared_hot(delay):\\n    time.sleep(delay)\\n"
exec(function_source)
child = subprocess.Popen([sys.executable, "-c", function_source + "shared_hot(0.28)\\n"])
shared_hot(0.30)
child.wait()
"""

    record_process(
        (sys.executable, "-c", source),
        runpack,
        name="deep-process-attribution",
        capture_level="deep",
    )

    with RunpackReader(runpack) as reader:
        shared = next(event for event in reader.events() if event.name == "__main__.shared_hot")
    raw_processes = shared.attributes["processes"]
    assert isinstance(raw_processes, list)
    assert {process["role"] for process in raw_processes if isinstance(process, dict)} == {
        "root",
        "descendant",
    }
    call_counts: list[int] = []
    for process in raw_processes:
        if not isinstance(process, dict):
            continue
        call_count = process.get("call_count")
        if isinstance(call_count, int) and not isinstance(call_count, bool):
            call_counts.append(call_count)
    assert sum(call_counts) == shared.attributes["call_count"]

    analysis = analyze_runpack(runpack)
    hotspot = next(item for item in analysis.python_hotspots if item.name == shared.name)
    assert hotspot.process_attribution_status == "complete"
    assert {process.role for process in hotspot.processes} == {"root", "descendant"}
    assert all(process.observed_in_process_tree for process in hotspot.processes)
    assert all(process.process_name for process in hotspot.processes)
    report = render_analysis(analysis, "text")
    assert "by process:" in report
    assert "root" in report
    assert "descendant" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    json_hotspot = next(
        item for item in json_report["python_hotspots"] if item["name"] == shared.name
    )
    assert json_hotspot["process_attribution_status"] == "complete"
    assert len(json_hotspot["processes"]) == 2


def test_sampling_attributes_one_shared_function_to_parent_and_child_processes(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "sample-process-attribution.runpack"
    source = """
import subprocess
import sys

function_source = "import time\\ndef shared_hot(delay):\\n    time.sleep(delay)\\n"
exec(function_source)
child = subprocess.Popen([sys.executable, "-c", function_source + "shared_hot(0.28)\\n"])
shared_hot(0.30)
child.wait()
"""

    record_process(
        (sys.executable, "-c", source),
        runpack,
        name="sample-process-attribution",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    hotspot = next(
        item for item in analysis.python_sample_hotspots if item.name == "__main__.shared_hot"
    )
    assert hotspot.process_attribution_status == "complete"
    assert {process.role for process in hotspot.processes} == {"root", "descendant"}
    assert all(process.observed_in_process_tree for process in hotspot.processes)
    assert all(process.leaf_sample_count >= 3 for process in hotspot.processes)
    assert "by process:" in render_analysis(analysis, "text")


def test_sampling_reports_complete_coverage_for_a_python_worker_pool(tmp_path: Path) -> None:
    runpack = tmp_path / "sample-worker-pool.runpack"
    workload = Path(__file__).parents[1] / "examples" / "local" / "worker_pool.py"

    record_process(
        (sys.executable, str(workload)),
        runpack,
        name="sample-worker-pool",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert profile is not None
    assert profile.process_count == 4
    assert len(profile.process_ids) == 4
    coverage = profile.process_coverage
    assert coverage.status == "complete"
    assert coverage.profiled_process_count == 4
    assert coverage.observed_python_process_count == 4
    assert coverage.matched_process_count == 4
    assert coverage.unprofiled_process_count == 0
    assert coverage.unobserved_profile_process_ids == ()
    shared = next(
        item for item in analysis.python_sample_hotspots if item.name == "__main__.shared_hot"
    )
    assert len(shared.processes) == 4
    report = render_analysis(analysis, "text")
    assert "process coverage complete: 4 / 4 observed Python processes reported" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    assert json_report["sample_profile"]["process_coverage"] == {
        "status": "complete",
        "profiled_process_count": 4,
        "observed_python_process_count": 4,
        "matched_process_count": 4,
        "unprofiled_process_count": 0,
        "unprofiled_processes": [],
        "unobserved_profile_process_ids": [],
    }


@pytest.mark.parametrize(
    ("capture_level", "observer_name"),
    (
        ("sample", "sampler"),
        ("deep", "profiler"),
    ),
)
def test_capture_reports_python_worker_that_disabled_site_startup(
    tmp_path: Path,
    capture_level: str,
    observer_name: str,
) -> None:
    runpack = tmp_path / f"{capture_level}-missing-worker.runpack"
    source = """
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-S", "-c", "import time; time.sleep(0.32)"]
)
time.sleep(0.34)
child.wait()
"""

    record_process(
        (sys.executable, "-c", source),
        runpack,
        name=f"{capture_level}-missing-worker",
        capture_level=capture_level,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if capture_level == "sample" else analysis.deep_profile
    assert profile is not None
    coverage = profile.process_coverage
    assert coverage.status == "partial"
    assert coverage.profiled_process_count == 1
    assert coverage.observed_python_process_count == 2
    assert coverage.matched_process_count == 1
    assert coverage.unprofiled_process_count == 1
    assert len(coverage.unprofiled_processes) == 1
    assert coverage.unprofiled_processes[0].parent_name is not None
    report = render_analysis(analysis, "text")
    assert "process coverage partial: 1 / 2 observed Python processes reported" in report
    assert f"did not load the {observer_name}" in report


@pytest.mark.parametrize(
    ("capture_level", "expected_message"),
    (
        ("sample", "no Python process was observed; no samples were expected"),
        ("deep", "no Python process was observed; no profile was expected"),
    ),
)
def test_python_capture_level_does_not_treat_non_python_processes_as_missing(
    tmp_path: Path,
    capture_level: str,
    expected_message: str,
) -> None:
    runpack = tmp_path / f"{capture_level}-non-python.runpack"

    record_process(
        ("/bin/sh", "-c", "sleep 0.25"),
        runpack,
        name=f"{capture_level}-non-python",
        capture_level=capture_level,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if capture_level == "sample" else analysis.deep_profile
    assert profile is not None
    assert profile.status == "unavailable"
    assert profile.process_count == 0
    coverage = profile.process_coverage
    assert coverage.status == "complete"
    assert coverage.observed_python_process_count == 0
    assert coverage.unprofiled_process_count == 0
    report = render_analysis(analysis, "text")
    assert expected_message in report
    assert "process coverage complete: no observed Python processes" in report


def test_sampling_retains_checkpoint_when_workload_uses_os_exit(tmp_path: Path) -> None:
    runpack = tmp_path / "sample-os-exit.runpack"
    source = """
import os
import time

def abrupt_work():
    time.sleep(0.65)

abrupt_work()
os._exit(0)
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name="sample-os-exit",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.process_count == 1
    assert profile.checkpoint_process_count == 1
    assert profile.transport == "controller-unix-socket"
    assert profile.checkpoint_interval_seconds == 0.5
    assert profile.collector_error_count == 0
    assert profile.process_coverage.status == "complete"
    abrupt = next(
        item for item in analysis.python_sample_hotspots if item.name == "__main__.abrupt_work"
    )
    assert abrupt.leaf_sample_count >= 20
    report = render_analysis(analysis, "text")
    assert (
        "snapshots: controller-side Unix socket, first evidence after 50.0ms, then every 500.0ms"
    ) in report
    assert "checkpoint-only: 1 process ended without a final report" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    assert json_report["sample_profile"]["status"] == "partial"
    assert json_report["sample_profile"]["checkpoint_process_count"] == 1
    assert json_report["sample_profile"]["first_checkpoint_delay_seconds"] == 0.05
    assert json_report["sample_profile"]["checkpoint_interval_seconds"] == 0.5
    assert json_report["sample_profile"]["transport"] == "controller-unix-socket"


@pytest.mark.parametrize("capture_level", ("sample", "deep"))
def test_immediate_os_exit_retains_capture_registration(
    tmp_path: Path,
    capture_level: str,
) -> None:
    runpack = tmp_path / f"{capture_level}-immediate-os-exit.runpack"

    exit_code = record_process(
        (sys.executable, "-c", "import os\nos._exit(0)"),
        runpack,
        name=f"{capture_level}-immediate-os-exit",
        capture_level=capture_level,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if capture_level == "sample" else analysis.deep_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.process_count == 1
    assert profile.checkpoint_process_count == 1
    assert profile.registration_only_process_count == 1
    assert profile.process_coverage.profiled_process_count == 1
    assert len(profile.process_ids) == 1
    report = render_analysis(analysis, "text")
    assert "registration-only: 1 process loaded capture" in report
    assert "ended before the first periodic checkpoint" in report
    assert "no Python hotspot evidence was retained" in report
    assert "checkpoint-only:" not in report
    json_report = json.loads(render_analysis(analysis, "json"))
    profile_key = "sample_profile" if capture_level == "sample" else "deep_profile"
    assert json_report[profile_key]["registration_only_process_count"] == 1


@pytest.mark.parametrize("capture_level", ("sample", "deep"))
def test_early_checkpoint_retains_evidence_before_regular_cadence(
    tmp_path: Path,
    capture_level: str,
) -> None:
    runpack = tmp_path / f"{capture_level}-early-checkpoint.runpack"
    if capture_level == "sample":
        source = """
import os
import time

def early_work():
    time.sleep(0.12)

early_work()
os._exit(0)
"""
    else:
        source = """
import os
import time

def early_work():
    try:
        raise RuntimeError("".join(("checkpoint", "-exception-secret")))
    except RuntimeError:
        return sum(range(100))

for _ in range(40):
    early_work()
time.sleep(0.12)
os._exit(0)
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name=f"{capture_level}-early-checkpoint",
        capture_level=capture_level,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if capture_level == "sample" else analysis.deep_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.checkpoint_process_count == 1
    assert profile.registration_only_process_count == 0
    assert profile.first_checkpoint_delay_seconds == 0.05
    if capture_level == "sample":
        hotspot = next(
            item for item in analysis.python_sample_hotspots if item.name == "__main__.early_work"
        )
        assert hotspot.leaf_sample_count >= 2
    else:
        hotspot = next(
            item for item in analysis.python_hotspots if item.name == "__main__.early_work"
        )
        assert hotspot.call_count == 40
        assert hotspot.exception_count == 40
        assert hotspot.non_control_flow_exception_count == 40
        assert b"checkpoint-exception-secret" not in runpack.read_bytes()
    report = render_analysis(analysis, "text")
    assert "first evidence after 50.0ms, then every 500.0ms" in report
    assert "checkpoint-only: 1 process ended without a final report" in report
    assert "registration-only:" not in report


def test_deep_checkpoint_retains_profile_hook_replacement(tmp_path: Path) -> None:
    workload = tmp_path / "checkpoint-hook-replacement.py"
    workload.write_text(
        """
import os
import sys
import time

def replacement(frame, event, argument):
    hidden = "checkpoint-hook-value-must-not-be-captured"

sys.setprofile(replacement)
time.sleep(0.12)
os._exit(0)
""".strip(),
        encoding="utf-8",
    )
    runpack = tmp_path / "checkpoint-hook-replacement.runpack"

    exit_code = record_process(
        (sys.executable, str(workload)),
        runpack,
        name="checkpoint-hook-replacement",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    assert exit_code == 0
    assert analysis.deep_profile is not None
    profile = analysis.deep_profile
    assert profile.status == "partial"
    assert profile.truncated is True
    assert profile.checkpoint_process_count == 1
    assert profile.observer_integrity is not None
    assert profile.observer_integrity.status == "partial"
    assert profile.observer_integrity.profile_hook_setter_call_count == 1
    assert profile.observer_integrity.profile_hook_setter_process_count == 1
    assert "profile hook setter called" in render_analysis(analysis, "text")
    assert b"checkpoint-hook-value-must-not-be-captured" not in runpack.read_bytes()


@pytest.mark.parametrize(
    ("capture_level", "observation"),
    (("sample", "sampled"), ("deep", "exact")),
)
def test_http_boundary_survives_abrupt_exit_via_periodic_checkpoint(
    tmp_path: Path,
    capture_level: str,
    observation: str,
) -> None:
    runpack = tmp_path / f"{capture_level}-http-checkpoint.runpack"
    source = """
import http.server
import os
import threading
import time
import urllib.request

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(0.08)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()

def fetch_before_crash():
    with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/", timeout=2):
        pass

fetch_before_crash()
time.sleep(0.65)
os._exit(0)
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name=f"{capture_level}-http-checkpoint",
        capture_level=capture_level,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if capture_level == "sample" else analysis.deep_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.checkpoint_process_count == 1
    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "partial"
    assert analysis.http_capture.request_count == 1
    assert len(analysis.http_requests) == 1
    request = analysis.http_requests[0]
    assert request.outcome == "response"
    assert request.status_code == 200
    assert request.caller is not None
    assert request.caller.name == "__main__.fetch_before_crash"
    assert request.caller.observation == observation


@pytest.mark.parametrize(
    ("capture_level", "observation"),
    (("sample", "sampled"), ("deep", "exact")),
)
def test_network_boundary_survives_abrupt_exit_via_periodic_checkpoint(
    tmp_path: Path,
    capture_level: str,
    observation: str,
) -> None:
    runpack = tmp_path / f"{capture_level}-network-checkpoint.runpack"
    source = """
import os
import socket
import threading
import time

server = socket.socket()
server.bind(("127.0.0.1", 0))
server.listen()

def accept_connection():
    peer, _address = server.accept()
    peer.close()

threading.Thread(target=accept_connection, daemon=True).start()

def connect_before_crash():
    connection = socket.create_connection(("127.0.0.1", server.getsockname()[1]))
    connection.close()

connect_before_crash()
time.sleep(0.65)
os._exit(0)
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name=f"{capture_level}-network-checkpoint",
        capture_level=capture_level,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if capture_level == "sample" else analysis.deep_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.checkpoint_process_count == 1
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "partial"
    assert analysis.network_capture.connection_count == 1
    assert len(analysis.network_connections) == 1
    connection = analysis.network_connections[0]
    assert connection.outcome == "connected"
    assert connection.transport == "tcp"
    if connection.caller is not None:
        assert connection.caller.name == "__main__.connect_before_crash"
        assert connection.caller.observation == observation
    else:
        assert capture_level == "sample"
        assert analysis.network_capture.caller_attribution_status == "partial"


@pytest.mark.parametrize("capture_level", ("sample", "deep"))
def test_forked_child_that_exits_immediately_retains_registration(
    tmp_path: Path,
    capture_level: str,
) -> None:
    runpack = tmp_path / f"{capture_level}-fork-registration.runpack"
    source = """
import os

pid = os.fork()
if pid == 0:
    os._exit(0)
os.waitpid(pid, 0)
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name=f"{capture_level}-fork-registration",
        capture_level=capture_level,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if capture_level == "sample" else analysis.deep_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.process_count == 2
    assert profile.checkpoint_process_count == 1
    assert profile.registration_only_process_count == 1


def test_sampling_retains_early_checkpoints_from_short_worker_burst(tmp_path: Path) -> None:
    runpack = tmp_path / "sample-short-worker-burst.runpack"
    source = """
import subprocess
import sys

worker = "import os, time; time.sleep(0.12); os._exit(0)"
children = [subprocess.Popen([sys.executable, "-c", worker]) for _ in range(8)]
for child in children:
    child.wait()
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name="sample-short-worker-burst",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.process_count == 9
    assert profile.checkpoint_process_count == 8
    assert profile.registration_only_process_count == 0
    assert profile.sample_count >= 8
    assert profile.collector_error_count == 0
    assert "checkpoint-only: 8 processes ended without a final report" in render_analysis(
        analysis, "text"
    )


def test_sampling_large_worker_pool_degrades_process_overflow_to_partial_evidence(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "sample-worker-overflow.runpack"
    source = """
import subprocess
import sys

worker = "import os; os._exit(0)"
children = [subprocess.Popen([sys.executable, "-c", worker]) for _ in range(128)]
for child in children:
    child.wait()
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name="sample-worker-overflow",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert exit_code == 0
    assert profile is not None
    assert profile.status == "partial"
    assert profile.process_count == 128
    assert profile.checkpoint_process_count == 127
    assert profile.dropped_profile_process_count == 1
    assert profile.dropped_profile_process_count_truncated is False
    assert profile.collector_error_count == 0
    assert profile.snapshot_metrics.status == "available"
    assert profile.snapshot_metrics.message_count >= 130
    report = render_analysis(analysis, "text")
    assert "process limit: 1 profile process report omitted after the 128-process bound" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    assert json_report["sample_profile"]["dropped_profile_process_count"] == 1


def test_deep_capture_retains_completed_calls_when_workload_is_terminated(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "deep-sigterm.runpack"
    source = """
import os
import signal
import time

def completed_work():
    return sum(range(1000))

for _ in range(200):
    completed_work()
time.sleep(0.65)
os.kill(os.getpid(), signal.SIGTERM)
"""

    exit_code = record_process(
        (sys.executable, "-c", source),
        runpack,
        name="deep-sigterm",
        capture_level="deep",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.deep_profile
    assert exit_code == -signal.SIGTERM
    assert profile is not None
    assert profile.status == "partial"
    assert profile.checkpoint_process_count == 1
    assert profile.open_call_count >= 1
    assert profile.transport == "controller-unix-socket"
    completed = next(
        item for item in analysis.python_hotspots if item.name == "__main__.completed_work"
    )
    assert completed.call_count == 200
    report = render_analysis(analysis, "text")
    assert "calls were still open at the latest snapshot" in report
    assert "checkpoint-only: 1 process ended without a final report" in report


def test_checkpoint_falls_back_to_atomic_workload_file_when_collector_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runpack = tmp_path / "sample-checkpoint-fallback.runpack"
    monkeypatch.setattr(deep_profile_module, "_prepare_snapshot_collector", lambda _path: None)

    record_process(
        (
            sys.executable,
            "-c",
            "import os, time\ntime.sleep(0.65)\nos._exit(0)",
        ),
        runpack,
        name="sample-checkpoint-fallback",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert profile is not None
    assert profile.status == "partial"
    assert profile.checkpoint_process_count == 1
    assert profile.transport == "workload-file"
    assert profile.snapshot_metrics.status == "unavailable"
    assert "workload-side atomic file fallback" in render_analysis(analysis, "text")


def test_registration_falls_back_to_atomic_workload_file_when_collector_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runpack = tmp_path / "sample-registration-fallback.runpack"
    monkeypatch.setattr(deep_profile_module, "_prepare_snapshot_collector", lambda _path: None)

    record_process(
        (sys.executable, "-c", "import os\nos._exit(0)"),
        runpack,
        name="sample-registration-fallback",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert profile is not None
    assert profile.status == "partial"
    assert profile.registration_only_process_count == 1
    assert profile.transport == "workload-file"
    assert profile.publication_metrics.status == "available"
    assert profile.publication_metrics.fallback_process_count == 1
    assert profile.publication_metrics.socket_attempted_process_count == 0
    assert "registration-only: 1 process loaded capture" in render_analysis(analysis, "text")


@pytest.mark.parametrize("instrument", ("sample", "deep"))
def test_final_snapshot_records_fallback_after_collector_death(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    instrument: str,
) -> None:
    original_handle = deep_profile_module._ProfileSnapshotCollector._handle

    def stop_after_registration(
        collector: deep_profile_module._ProfileSnapshotCollector,
        connection: socket.socket,
    ) -> None:
        original_handle(collector, connection)
        if collector.message_count == 1:
            collector.listener.close()

    monkeypatch.setattr(
        deep_profile_module._ProfileSnapshotCollector,
        "_handle",
        stop_after_registration,
    )
    runpack = tmp_path / f"{instrument}-collector-death.runpack"

    record_process(
        (sys.executable, "-c", "import time\ntime.sleep(0.12)"),
        runpack,
        name=f"{instrument}-collector-death",
        instrument=instrument,
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile if instrument == "sample" else analysis.deep_profile
    assert profile is not None
    assert profile.status == "complete"
    assert profile.transport == "mixed"
    assert profile.publication_metrics.status == "available"
    assert profile.publication_metrics.fallback_process_count == 1
    assert profile.publication_metrics.socket_attempted_process_count == 1
    assert profile.publication_metrics.socket_failure_seconds > 0
    assert profile.publication_metrics.max_socket_failure_seconds > 0
    assert len(profile.publication_metrics.fallback_process_ids) == 1
    report = render_analysis(analysis, "text")
    assert "retained snapshot fallback: 1 process" in report
    assert "controller socket attempted by 1 process" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    profile_key = "sample_profile" if instrument == "sample" else "deep_profile"
    publication_metrics = json_report[profile_key]["publication_metrics"]
    assert publication_metrics["status"] == "available"
    assert publication_metrics["fallback_process_count"] == 1
    assert publication_metrics["socket_attempted_process_count"] == 1


def test_snapshot_collector_rejects_malformed_message_without_losing_final_report(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "sample-malformed-snapshot.runpack"
    source = """
import os
import socket
import time

connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
connection.connect(os.environ["_CONTRAIL_PROFILE_SNAPSHOT_SOCKET"])
connection.sendall(b"malformed")
connection.close()
time.sleep(0.12)
"""

    record_process(
        (sys.executable, "-c", source),
        runpack,
        name="sample-malformed-snapshot",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert profile is not None
    assert profile.status == "complete"
    assert profile.checkpoint_process_count == 0
    assert profile.collector_error_count == 1
    assert "snapshot collector errors: 1" in render_analysis(analysis, "text")


def test_snapshot_collector_rejects_unbounded_serialization_duration(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "sample-invalid-snapshot-duration.runpack"
    source = """
import os
import socket
import struct
import time

header = struct.Struct("!8sQQQQ")
connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
connection.connect(os.environ["_CONTRAIL_PROFILE_SNAPSHOT_SOCKET"])
connection.sendall(header.pack(b"CTRP0002", os.getpid(), 2, 3_600_000_000_001, 2) + b"{}")
connection.close()
time.sleep(0.12)
"""

    record_process(
        (sys.executable, "-c", source),
        runpack,
        name="sample-invalid-snapshot-duration",
        capture_level="sample",
    )

    analysis = analyze_runpack(runpack)
    profile = analysis.sample_profile
    assert profile is not None
    assert profile.status == "complete"
    assert profile.collector_error_count == 1
    assert profile.snapshot_metrics.status == "available"
    assert profile.snapshot_metrics.message_count >= 2


def test_snapshot_collector_releases_a_stalled_connection_quickly(tmp_path: Path) -> None:
    session = prepare_sample_profile_session(tmp_path)
    assert session.collector is not None
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.connect(str(session.collector.socket_path))
        connection.sendall(b"x")
        started = time.perf_counter()
        deadline = started + 0.5
        while session.collector.error_count == 0 and time.perf_counter() < deadline:
            time.sleep(0.01)

        assert session.collector.error_count == 1
        assert time.perf_counter() - started < 0.5
    finally:
        connection.close()
        session.close()


@pytest.mark.parametrize(
    "bootstrap",
    (sampling_profile_bootstrap, deep_profile_bootstrap),
    ids=("sample", "deep"),
)
def test_snapshot_sender_abandons_a_stalled_controller_quickly(
    monkeypatch: pytest.MonkeyPatch,
    bootstrap: _SnapshotBootstrap,
) -> None:
    with tempfile.TemporaryDirectory(prefix="ct-socket-") as directory:
        socket_path = str(Path(directory) / "s")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(socket_path)
        listener.listen(1)
        release = threading.Event()

        def hold_connection() -> None:
            connection, _ = listener.accept()
            with connection:
                release.wait(1.0)

        thread = threading.Thread(target=hold_connection)
        thread.start()
        monkeypatch.setenv(bootstrap._SOCKET_ENV, socket_path)
        started = time.perf_counter()
        try:
            sent = bootstrap._send_to_collector(
                b"x" * bootstrap._MAX_REPORT_BYTES,
                0,
                2,
            )
        finally:
            elapsed = time.perf_counter() - started
            release.set()
            thread.join(timeout=1.0)
            listener.close()

        assert sent is False
        assert elapsed < 0.5
        assert not thread.is_alive()


def test_deep_profile_rejects_duplicate_process_documents(tmp_path: Path) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
    }
    (tmp_path / "profile-first.json").write_text(json.dumps(document), encoding="utf-8")
    (tmp_path / "profile-second.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(DeepProfileError, match="process ids must be unique"):
        load_deep_profile(session, entity_id="process")


@pytest.mark.parametrize(
    ("process_count", "expected_dropped", "expected_truncated"),
    ((128, 0, False), (129, 1, True)),
)
def test_sample_profile_file_fallback_enforces_process_limit_without_losing_root(
    tmp_path: Path,
    process_count: int,
    expected_dropped: int,
    expected_truncated: bool,
) -> None:
    for pid in range(1, process_count + 1):
        document = {
            "format_version": 1,
            "mode": "sample",
            "pid": pid,
            "interval_ns": 10_000_000,
            "truncated": False,
            "sample_count": 0,
            "thread_sample_count": 0,
            "dropped_frame_sample_count": 0,
            "dropped_edge_sample_count": 0,
            "callback_error_count": 0,
            "functions": [],
            "edges": [],
        }
        (tmp_path / f"profile-{pid}.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino), mode="sample")

    profile = load_sample_profile(
        session,
        entity_id="process",
        root_process_id=process_count,
    )

    assert profile.process_count == 128
    assert process_count in profile.process_ids
    assert profile.dropped_profile_process_count == expected_dropped
    assert profile.truncated is expected_truncated
    assert profile.snapshot_metrics_status == "unavailable"


def test_deep_profile_rejects_unsupported_checkpoint_interval(tmp_path: Path) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "snapshot_kind": "checkpoint",
        "checkpoint_interval_ns": 1,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(DeepProfileError, match="checkpoint interval is unsupported"):
        load_deep_profile(session, entity_id="process")


def test_deep_profile_rejects_unsupported_first_checkpoint_delay(tmp_path: Path) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "snapshot_kind": "checkpoint",
        "checkpoint_interval_ns": 500_000_000,
        "first_checkpoint_delay_ns": 1,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(DeepProfileError, match="first checkpoint delay is unsupported"):
        load_deep_profile(session, entity_id="process")


def test_deep_profile_rejects_registration_that_contains_profile_evidence(
    tmp_path: Path,
) -> None:
    document = {
        "format_version": 1,
        "pid": 42,
        "snapshot_kind": "checkpoint",
        "registration_only": True,
        "checkpoint_interval_ns": 500_000_000,
        "truncated": False,
        "dropped_call_count": 0,
        "dropped_edge_count": 0,
        "callback_error_count": 0,
        "functions": [
            {
                "id": 0,
                "module": "example",
                "qualname": "work",
                "filename": "example.py",
                "firstlineno": 1,
                "scope": "application",
                "call_count": 1,
                "total_ns": 1,
                "self_ns": 1,
                "max_ns": 1,
            }
        ],
        "edges": [],
    }
    (tmp_path / "profile-42.json").write_text(json.dumps(document), encoding="utf-8")
    status = tmp_path.stat()
    session = DeepProfileSession(tmp_path, (status.st_dev, status.st_ino))

    with pytest.raises(DeepProfileError, match="registration must not contain profile evidence"):
        load_deep_profile(session, entity_id="process")


def test_profile_session_close_removes_controller_socket_and_private_directory(
    tmp_path: Path,
) -> None:
    session = prepare_sample_profile_session(tmp_path)
    assert session.collector is not None
    socket_path = session.collector.socket_path
    directory = session.directory
    assert socket_path.exists()
    assert directory.exists()

    session.close()

    assert not socket_path.exists()
    assert not directory.exists()


def test_recovered_profile_session_rejects_inconsistent_transport_metrics(tmp_path: Path) -> None:
    session = prepare_sample_profile_session(tmp_path)
    try:
        checkpoint = deep_profile_module.profile_session_checkpoint(session)
        transport = checkpoint["transport"]
        assert isinstance(transport, dict)
        transport["metrics_status"] = "unavailable"
        transport["message_count"] = 1

        with pytest.raises(DeepProfileError, match="unavailable.*transport metrics"):
            deep_profile_module.recover_profile_session(checkpoint)
    finally:
        session.close()
