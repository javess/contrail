from __future__ import annotations

import json
import sys
import threading
import urllib.request
from importlib.resources import files
from pathlib import Path

import pytest

from runtime_tools import record_process
from runtime_tools.ui import TimelineError, build_timeline_payload, create_server


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
    events = baseline_value["events"]
    assert isinstance(events, list)
    first_event = events[0]
    assert isinstance(first_event, dict)
    assert first_event["kind"] == "process.run"
    analysis = baseline_value["analysis"]
    assert isinstance(analysis, dict)
    critical_path = analysis["critical_path"]
    assert isinstance(critical_path, dict)
    assert critical_path["event_ids"] == [first_event["id"]]
    comparison = payload["comparison"]
    assert isinstance(comparison, dict)
    assert comparison["outcome"] == "equivalent"


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


def test_packaged_ui_renders_batchscope_analysis() -> None:
    static = files("runtime_tools.ui").joinpath("static")
    html = static.joinpath("index.html").read_text()
    javascript = static.joinpath("app.js").read_text()

    assert 'id="analysis"' in html
    assert "renderAnalysis()" in javascript
    assert "Path waiting" in javascript
    assert "Duration shifts" in javascript
    assert "stderr_equivalent" in javascript
    assert "nsToSeconds(event.duration_ns)" in javascript
    assert "run.summary.record_counts.attachments" in javascript
    assert "Evidence warning" in javascript
    assert "diff.candidate_annotation_error" in javascript
    assert "diff.environment_changes" in javascript
    assert "diff.baseline_incomplete_streams" in javascript
    assert "Remaining after compute" in javascript
    assert "No constraint classified from available evidence" in javascript


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


def test_local_ui_reports_invalid_bind_without_low_level_error(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(TimelineError, match="could not bind local UI to 127.0.0.1:70000"):
        create_server(runpack, None, host="127.0.0.1", port=70_000)
