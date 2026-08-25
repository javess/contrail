from __future__ import annotations

import hashlib
import json
import stat
import sys
from pathlib import Path
from typing import Any, cast

import pytest

import runtime_tools.demo as demo_module
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.capture import CaptureError, record_process
from runtime_tools.demo import DemoError, run_demo
from runtime_tools.model import JsonValue
from runtime_tools.proofline.contracts import load_contracts
from runtime_tools.storage import RunpackReader
from runtime_tools.ui import build_timeline_payload


def _finding(payload: dict[str, JsonValue], assertion_type: str) -> dict[str, Any]:
    proofline = cast(dict[str, Any], payload["proofline"])
    return next(
        cast(dict[str, Any], finding)
        for finding in proofline["findings"]
        if finding["type"] == assertion_type
    )


def test_run_demo_creates_and_proves_the_installed_traceability_story(tmp_path: Path) -> None:
    output = tmp_path / "demo"

    result = run_demo(output)

    assert result.status == "ready"
    assert result.violation_count == 2
    assert result.baseline_exit_code == result.candidate_exit_code == 0
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert {path.name for path in output.iterdir()} == {
        "baseline.runpack",
        "candidate.runpack",
        "contract.yaml",
        "proofline-report.json",
        "workload.py",
    }
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in output.iterdir())
    assert result.output_dir == output
    assert result.workload == output / "workload.py"
    assert result.baseline_runpack == output / "baseline.runpack"
    assert result.candidate_runpack == output / "candidate.runpack"
    assert result.contract == output / "contract.yaml"
    assert result.proofline_report == output / "proofline-report.json"
    workload_source = result.workload.read_text(encoding="utf-8")
    assert 'runtime.run("local-pipeline", total_work=100)' in workload_source
    assert 'runtime.stage("result-aggregation", phase="draining", concurrency=1)' in workload_source
    assert 'runtime.event("db.write", kind="client.request")' in workload_source
    assert '"peer.service": "metadata-db"' in workload_source

    contracts = load_contracts(result.contract)
    assert [assertion.type for assertion in contracts[0].assertions] == [
        "candidate_exit_success",
        "exit_code_equivalent",
        "output_equivalent",
        "forbid_new_dependency",
        "max_operation_count",
    ]
    dependency = contracts[0].assertions[3]
    assert dependency.config["from"] == Path(sys.executable).name
    assert dependency.config["to"] == "metadata-db"

    with (
        RunpackReader(result.baseline_runpack) as baseline,
        RunpackReader(result.candidate_runpack) as candidate,
    ):
        baseline_command = baseline.execution().command
        candidate_command = candidate.execution().command
        baseline_writes = sum(
            count
            for (*_, operation), count in baseline.operation_counts().items()
            if operation == "db.write"
        )
        candidate_writes = sum(
            count
            for (*_, operation), count in candidate.operation_counts().items()
            if operation == "db.write"
        )
        baseline_stdout = next(item for item in baseline.attachments() if item.name == "stdout")
        candidate_stdout = next(item for item in candidate.attachments() if item.name == "stdout")
        baseline_dependencies = baseline.peer_service_edge_counts()
        candidate_dependencies = candidate.peer_service_edge_counts()
        candidate_events = {event.id: event for event in candidate.events()}
    assert (baseline_writes, candidate_writes) == (3, 30)
    assert baseline_command == (sys.executable, str(result.workload), "baseline")
    assert candidate_command == (sys.executable, str(result.workload), "candidate")
    assert baseline_stdout.content == candidate_stdout.content == b"pipeline result: 42\n"
    dependency_identity = ("process", Path(sys.executable).name, "service", "metadata-db", "calls")
    assert baseline_dependencies.get(dependency_identity, 0) == 0
    assert candidate_dependencies[dependency_identity] == 1

    analysis = analyze_runpack(result.candidate_runpack)
    assert [phase.name for phase in analysis.lifecycle] == ["compute", "result-aggregation"]
    assert analysis.throughput is not None
    assert analysis.throughput.remaining_at_compute_completion == 20
    assert analysis.throughput.post_compute_seconds is not None
    assert analysis.throughput.post_compute_seconds > 0
    assert any(
        bottleneck.classification == "serialized_stage"
        and "result-aggregation" in bottleneck.evidence
        for bottleneck in analysis.bottlenecks
    )

    report = json.loads(result.proofline_report.read_text(encoding="utf-8"))
    assert report["document_type"] == "proofline.verification"
    assert report["diff"]["document_type"] == "rundiff.compare"
    assert report["artifact_bindings"] == {
        "baseline": {
            "size_bytes": result.baseline_runpack.stat().st_size,
            "sha256": hashlib.sha256(result.baseline_runpack.read_bytes()).hexdigest(),
        },
        "candidate": {
            "size_bytes": result.candidate_runpack.stat().st_size,
            "sha256": hashlib.sha256(result.candidate_runpack.read_bytes()).hexdigest(),
        },
    }
    assert [(item["type"], item["status"]) for item in report["results"]] == [
        ("candidate_exit_success", "pass"),
        ("exit_code_equivalent", "pass"),
        ("output_equivalent", "pass"),
        ("forbid_new_dependency", "fail"),
        ("max_operation_count", "fail"),
    ]
    assert all(item["assertion"]["type"] == item["type"] for item in report["results"])

    payload = build_timeline_payload(
        result.baseline_runpack,
        result.candidate_runpack,
        proofline_report=result.proofline_report,
    )
    proofline = cast(dict[str, Any], payload["proofline"])
    dependency_finding = _finding(payload, "forbid_new_dependency")
    count_finding = _finding(payload, "max_operation_count")
    assert {finding["report_assurance"] for finding in proofline["findings"]} == {
        "artifact_bound_policy_replayed"
    }
    assert dependency_finding["evidence"][0]["fact"] == {"baseline": 0, "candidate": 1}
    assert count_finding["evidence"][0]["fact"] == {
        "baseline": 3,
        "candidate": 30,
        "limit": 3.0,
    }
    dependency_selection = proofline["selections"][dependency_finding["selection_id"]]
    count_selection = proofline["selections"][count_finding["selection_id"]]
    assert dependency_selection["matched_event_count"] == 1
    assert count_selection["matched_event_count"] == 30
    assert {
        candidate_events[event_id].name for event_id in dependency_selection["candidate_event_ids"]
    } == {"metadata.lookup"}
    assert {
        candidate_events[event_id].name for event_id in count_selection["candidate_event_ids"]
    } == {"db.write"}


def test_run_demo_result_is_deterministic_across_fresh_directories(tmp_path: Path) -> None:
    first = run_demo(tmp_path / "first")
    second = run_demo(tmp_path / "second")

    assert (first.status, first.violation_count) == (second.status, second.violation_count)
    assert [
        result.as_json_value(include_evidence=True) for result in first.verification.results
    ] == [result.as_json_value(include_evidence=True) for result in second.verification.results]


@pytest.mark.parametrize("existing_kind", ("directory", "file", "dangling-symlink"))
def test_run_demo_rejects_every_existing_output_before_workload_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing_kind: str,
) -> None:
    output = tmp_path / "demo"
    if existing_kind == "directory":
        output.mkdir()
    elif existing_kind == "file":
        output.write_text("owned", encoding="utf-8")
    else:
        output.symlink_to(tmp_path / "missing-target")

    def unexpected_workload(*args: Any, **kwargs: Any) -> int:
        raise AssertionError("workload must not run")

    monkeypatch.setattr(demo_module, "record_process", unexpected_workload)

    with pytest.raises(DemoError, match="refusing to reuse demo output directory"):
        run_demo(output)

    if existing_kind == "dangling-symlink":
        assert output.is_symlink()


def test_run_demo_rejects_a_missing_parent_before_workload_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "missing" / "demo"

    def unexpected_workload(*args: Any, **kwargs: Any) -> int:
        raise AssertionError("workload must not run")

    monkeypatch.setattr(demo_module, "record_process", unexpected_workload)

    with pytest.raises(DemoError, match="demo output parent directory does not exist"):
        run_demo(output)

    assert not output.parent.exists()


def test_run_demo_retains_a_normalized_usable_partial_directory_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "demo"
    real_record_process = record_process
    call_count = 0

    def fail_candidate(*args: Any, **kwargs: Any) -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise CaptureError("candidate\nfailure")
        return real_record_process(*args, **kwargs)

    monkeypatch.setattr(demo_module, "record_process", fail_candidate)

    with pytest.raises(DemoError) as error:
        run_demo(output)

    assert "candidate\\nfailure" in str(error.value)
    assert "candidate\nfailure" not in str(error.value)
    assert "partial demo retained" in str(error.value)
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert {path.name for path in output.iterdir()} == {
        "baseline.runpack",
        "contract.yaml",
        "workload.py",
    }


def test_run_demo_does_not_publish_a_report_that_failed_interactive_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "demo"
    monkeypatch.setattr(demo_module, "build_timeline_payload", lambda *args, **kwargs: {})

    with pytest.raises(DemoError, match="Proofline evidence is not an object"):
        run_demo(output)

    assert {path.name for path in output.iterdir()} == {
        "baseline.runpack",
        "candidate.runpack",
        "contract.yaml",
        "workload.py",
    }
    assert not (output / "proofline-report.json").exists()
    assert not (output / ".proofline-report.json.tmp").exists()
