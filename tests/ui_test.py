from __future__ import annotations

import json
import sys
import threading
import urllib.request
from importlib.resources import files
from pathlib import Path

import pytest

import runtime_tools.ui.server as server_module
from runtime_tools import record_process
from runtime_tools.model import Entity, Event, Execution
from runtime_tools.storage import RunpackWriter
from runtime_tools.ui import TimelineError, build_timeline_payload, create_server, serve_runpacks
from runtime_tools.ui.server import _log_line


def test_timeline_payload_exposes_normalized_evidence_and_comparison(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process((sys.executable, "-c", "print('same')"), baseline, name="baseline")
    record_process((sys.executable, "-c", "print('same')"), candidate, name="candidate")

    payload = build_timeline_payload(baseline, candidate)

    runs = payload["runs"]
    assert isinstance(runs, list)
    assert len(runs) == 2
    baseline_value = runs[0]
    assert isinstance(baseline_value, dict)
    summary = baseline_value["summary"]
    assert isinstance(summary, dict)
    assert summary["name"] == "baseline"
    timeline_duration_ns = baseline_value["timeline_duration_ns"]
    assert isinstance(timeline_duration_ns, int)
    assert timeline_duration_ns > 0
    events = baseline_value["events"]
    assert isinstance(events, list)
    first_event = events[0]
    assert isinstance(first_event, dict)
    assert first_event["kind"] == "process.run"
    assert first_event["clock_domain"] == "host.wall"
    assert first_event["uncertainty_ns"] is None
    measurements = baseline_value["measurements"]
    assert isinstance(measurements, list)
    assert {item["name"] for item in measurements if isinstance(item, dict)} == {
        "process.cpu.system",
        "process.cpu.user",
        "process.memory.peak",
        "process.stderr.bytes",
        "process.stdout.bytes",
        "process.wall_time",
    }
    analysis = baseline_value["analysis"]
    assert isinstance(analysis, dict)
    critical_path = analysis["critical_path"]
    assert isinstance(critical_path, dict)
    assert critical_path["event_ids"] == [first_event["id"]]
    comparison = payload["comparison"]
    assert isinstance(comparison, dict)
    assert comparison["outcome"] == "equivalent"


def test_timeline_derives_an_extent_for_open_executions(tmp_path: Path) -> None:
    runpack = tmp_path / "open.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("open", "open", 100, None, (), str(tmp_path), None, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            Event("work", "operation", "work", "worker", 110, 150, "test", None, None, {})
        )

    payload = build_timeline_payload(runpack)

    runs = payload["runs"]
    assert isinstance(runs, list)
    run = runs[0]
    assert isinstance(run, dict)
    assert run["timeline_duration_ns"] == 50
    summary = run["summary"]
    assert isinstance(summary, dict)
    assert summary["wall_time_seconds"] is None


def test_local_ui_serves_packaged_assets_and_read_only_data(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")
    server = create_server(runpack, None, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    port = server.server_address[1]
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2) as response:
            html = response.read().decode()
            assert (
                response.headers["Content-Security-Policy"]
                == "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                "frame-ancestors 'none'"
            )
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/data", timeout=2) as response:
            payload = json.load(response)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert "Runtime timeline" in html
    assert payload["runs"][0]["summary"]["name"] == "served"


def test_local_ui_escapes_terminal_controls_in_request_logs() -> None:
    line = _log_line('"%s" %s', ("GET /\x1b[31m", 400))

    assert "\x1b" not in line
    assert r"GET /\x1b[31m" in line


def test_local_ui_closes_server_when_browser_launch_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")
    server = create_server(runpack, None, host="127.0.0.1", port=0)

    def fail_browser_launch(url: str) -> None:
        raise RuntimeError("simulated browser failure")

    monkeypatch.setattr(server_module, "create_server", lambda *args, **kwargs: server)
    monkeypatch.setattr("runtime_tools.ui.server.webbrowser.open", fail_browser_launch)

    with pytest.raises(RuntimeError, match="simulated browser failure"):
        serve_runpacks(runpack, None)

    assert server.fileno() == -1


def test_packaged_ui_renders_batchscope_analysis() -> None:
    static = files("runtime_tools.ui").joinpath("static")
    html = static.joinpath("index.html").read_text()
    javascript = static.joinpath("app.js").read_text()

    assert 'id="analysis"' in html
    assert "renderAnalysis()" in javascript
    assert "Path waiting" in javascript
    assert "diff.cpu_time.baseline" in javascript
    assert "run.summary.cpu_user_seconds + run.summary.cpu_system_seconds" in javascript
    assert "Duration shifts" in javascript
    assert "stderr_equivalent" in javascript
    assert "operation_errors_equivalent" in javascript
    assert "nsToSeconds(event.duration_ns)" in javascript
    assert "run.summary.record_counts.attachments" in javascript
    assert "Evidence warning" in javascript
    assert "diff.candidate_annotation_error" in javascript
    assert "diff.environment_changes" in javascript
    assert "diff.entity_count_changes" in javascript
    assert "diff.operation_concurrency_changes" in javascript
    assert "diff.operation_error_count_changes" in javascript
    assert "diff.baseline_incomplete_streams" in javascript
    assert "diff.candidate_missing_causal_references" in javascript
    assert "diff.candidate_dropped_attribute_count" in javascript
    assert "run.summary.dropped_attribute_count" in javascript
    assert "Remaining after compute" in javascript
    assert "No constraint classified from available evidence" in javascript
    assert "Untimed evidence" in javascript
    assert "event.start_offset_ns != null" in javascript
    assert "run.timeline_duration_ns" in javascript
    assert "Causal links" in javascript
    assert "Clock domain" in javascript
    assert "Resource measurements" in javascript
    assert "escapeDisplayControls(value).replace" in javascript
    assert r"[\p{Cc}\p{Cf}\p{Cs}]" in javascript


def test_timeline_rejects_oversized_artifact_before_analysis(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process(
        (
            sys.executable,
            "-c",
            "from runtime_tools import runtime; runtime.event('extra')",
        ),
        runpack,
        name="served",
    )

    with pytest.raises(TimelineError, match="timeline has 2 events; local UI limit is 1"):
        build_timeline_payload(runpack, event_limit=1)


def test_timeline_rejects_boolean_limits_before_loading_artifacts(tmp_path: Path) -> None:
    with pytest.raises(TimelineError, match="timeline limits must be integers"):
        build_timeline_payload(tmp_path / "missing.runpack", event_limit=True)


def test_timeline_rejects_oversized_edge_sets_before_analysis(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process(
        (
            sys.executable,
            "-c",
            "from runtime_tools import runtime; runtime.event('one'); runtime.event('two')",
        ),
        runpack,
        name="served",
    )

    with pytest.raises(TimelineError, match="timeline has 2 edges; local UI limit is 1"):
        build_timeline_payload(runpack, edge_limit=1)


def test_timeline_rejects_oversized_measurement_sets_before_analysis(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(TimelineError, match="timeline has 6 measurements; local UI limit is 1"):
        build_timeline_payload(runpack, measurement_limit=1)


def test_timeline_rejects_large_aggregate_normalized_json_before_loading_rows(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(
        TimelineError,
        match=r"timeline has [\d,]+ normalized JSON bytes; local UI limit is 1",
    ):
        build_timeline_payload(runpack, json_byte_limit=1)


def test_timeline_rejects_large_aggregate_normalized_text_before_loading_rows(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(
        TimelineError,
        match=r"timeline has [\d,]+ normalized text bytes; local UI limit is 1",
    ):
        build_timeline_payload(runpack, text_byte_limit=1)


def test_timeline_rejects_large_serialized_payload(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(
        TimelineError,
        match=r"timeline payload is [\d,]+ bytes; local UI limit is 1",
    ):
        build_timeline_payload(runpack, payload_byte_limit=1)


@pytest.mark.parametrize(
    ("host", "port", "message"),
    (
        ("", 0, "host must be a non-empty string"),
        ("127.0.0.1", True, "port must be an integer"),
        ("127.0.0.1", 70_000, "port must be between 0 and 65535"),
    ),
)
def test_local_ui_validates_bind_arguments_before_loading_artifacts(
    tmp_path: Path,
    host: str,
    port: int,
    message: str,
) -> None:
    with pytest.raises(TimelineError, match=message):
        create_server(tmp_path / "missing.runpack", None, host=host, port=port)
