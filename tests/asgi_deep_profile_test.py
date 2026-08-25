from __future__ import annotations

import sys
from pathlib import Path

import pytest

from runtime_tools import record_process
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.proofline import verify_contracts
from runtime_tools.rundiff import compare_runpacks


def _require_uvicorn(protocol: str) -> None:
    pytest.importorskip("uvicorn", minversion="0.30")
    if protocol == "httptools":
        pytest.importorskip("httptools")


def test_deep_capture_does_not_import_uvicorn_into_an_unmodified_workload(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "no-uvicorn.py"
    output = tmp_path / "imports.txt"
    runpack = tmp_path / "no-uvicorn.runpack"
    workload.write_text(
        """
import sys
from pathlib import Path

before = any(name == "uvicorn" or name.startswith("uvicorn.") for name in sys.modules)
after = any(name == "uvicorn" or name.startswith("uvicorn.") for name in sys.modules)
Path(sys.argv[1]).write_text(f"{before},{after}\\n", encoding="utf-8")
""".strip(),
        encoding="utf-8",
    )

    exit_code = record_process(
        (sys.executable, str(workload), str(output)),
        runpack,
        name="no-uvicorn-import",
        capture_level="deep",
    )

    assert exit_code == 0
    assert output.read_text(encoding="utf-8") == "False,False\n"


@pytest.mark.parametrize(
    ("protocol", "adapter"),
    (("h11", "uvicorn.h11"), ("httptools", "uvicorn.httptools")),
)
def test_zero_touch_uvicorn_capture_preserves_behavior_and_private_http_boundaries(
    tmp_path: Path,
    protocol: str,
    adapter: str,
) -> None:
    _require_uvicorn(protocol)
    example = Path(__file__).parents[1] / "examples" / "local" / "asgi_server.py"
    passive_output = tmp_path / f"{protocol}-passive.json"
    deep_output = tmp_path / f"{protocol}-deep.json"
    passive = tmp_path / f"{protocol}-passive.runpack"
    deep = tmp_path / f"{protocol}-deep.runpack"
    arguments = (str(example), "--http", protocol, "--websocket")

    passive_exit = record_process(
        (sys.executable, *arguments, "--output", str(passive_output)),
        passive,
        name=f"uvicorn-{protocol}-passive",
        capture_level="passive",
    )
    deep_exit = record_process(
        (sys.executable, *arguments, "--output", str(deep_output)),
        deep,
        name=f"uvicorn-{protocol}-deep",
        capture_level="deep",
    )

    assert passive_exit == deep_exit == 0
    assert passive_output.read_bytes() == deep_output.read_bytes()
    assert b'"statuses": [200, 503]' in deep_output.read_bytes()
    analysis = analyze_runpack(deep)
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    assert adapter in analysis.logical_operation_capture.adapters
    requests = tuple(
        operation for operation in analysis.logical_operations if operation.category == "server"
    )
    assert len(requests) == 2
    assert {request.status_code for request in requests} == {200, 503}
    assert {request.outcome for request in requests} == {"completed", "operation_error"}
    assert {request.error_type for request in requests} == {None, "HTTPStatusError"}
    assert all(
        request.adapter == adapter
        and request.as_json_value()["duration_boundary"] == "request_to_response_completion"
        and request.caller is not None
        and request.caller.name == "__main__.application"
        and request.caller.observation == "exact"
        for request in requests
    )
    streamed = next(request for request in requests if request.status_code == 200)
    assert streamed.duration_seconds is not None and streamed.duration_seconds >= 0.05
    assert any(
        hotspot.name == "__main__._consume_request" and hotspot.call_count == 2
        for hotspot in analysis.python_hotspots
    )
    assert any(
        finding.classification == "server_operation_failures" for finding in analysis.bottlenecks
    )
    assert f"adapter {adapter}" in render_analysis(analysis, "text")

    runpack_bytes = deep.read_bytes()
    for private_value in (
        b"PATCH",
        b"private-success-path",
        b"private-failure-path",
        b"private-query-value",
        b"private-request-header-value",
        b"private-request-body-sentinel",
        b"private-success-response-header",
        b"private-failure-response-header",
        b"private-streaming-response-body-one",
        b"private-streaming-response-body-two",
        b"private-failure-response-body",
        b"private-asgi-exception-message",
        b"private-websocket-path",
        b"private-websocket-query",
        b"private-websocket-request-header",
        b"private-websocket-response-header",
    ):
        assert private_value not in runpack_bytes


def test_uvicorn_requests_flow_through_rundiff_and_proofline_without_duplicates(
    tmp_path: Path,
) -> None:
    _require_uvicorn("h11")
    example = Path(__file__).parents[1] / "examples" / "local" / "asgi_server.py"
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "request-contract.yaml"
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

    baseline_exit = record_process(
        (sys.executable, str(example), "--http", "h11"),
        baseline,
        name="uvicorn-h11-baseline",
        capture_level="deep",
    )
    candidate_exit = record_process(
        (
            sys.executable,
            str(example),
            "--http",
            "h11",
            "--extra-requests",
            "1",
        ),
        candidate,
        name="uvicorn-h11-candidate",
        capture_level="deep",
    )

    assert baseline_exit == candidate_exit == 0
    diff = compare_runpacks(baseline, candidate)
    request_change = next(
        change
        for change in diff.operation_count_changes
        if change.operation_kind == "server.request"
        and change.operation_name == "Inbound HTTP request"
    )
    assert (request_change.baseline, request_change.candidate) == (2, 3)
    verification = verify_contracts(contract, baseline, candidate)
    assert verification.results[0].status == "fail"
    assert verification.results[0].observed == "baseline=2, candidate=3, limit=2"
