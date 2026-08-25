from __future__ import annotations

import sys
from pathlib import Path

from runtime_tools import record_process
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.proofline import verify_contracts
from runtime_tools.providers.builtins.otel import import_otlp_json
from runtime_tools.rundiff import compare_runpacks


def test_local_pipeline_demonstrates_equivalent_output_and_runtime_regression(
    tmp_path: Path,
) -> None:
    example = Path(__file__).parents[1] / "examples" / "local" / "pipeline.py"
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process((sys.executable, str(example)), baseline, name="baseline")
    record_process((sys.executable, str(example), "--regression"), candidate, name="candidate")

    diff = compare_runpacks(baseline, candidate)
    analysis = analyze_runpack(candidate)
    contract = example.with_name("contracts.yaml")
    verification = verify_contracts(contract, baseline, candidate)

    assert diff.outcome == "equivalent"
    writes = next(
        change for change in diff.operation_count_changes if change.operation_name == "db.write"
    )
    assert (writes.baseline, writes.candidate) == (3, 30)
    assert any(
        edge.source_name == Path(sys.executable).name
        and edge.target_name == "metadata-db"
        and edge.change_kind == "new"
        for edge in diff.edge_count_changes
    )
    assert not any(
        edge.source_name == edge.target_name and edge.relation == "parent"
        for edge in diff.edge_count_changes
    )
    assert [phase.name for phase in analysis.lifecycle] == ["read", "transform", "persist"]
    assert any(item.classification == "serialized_stage" for item in analysis.bottlenecks)
    statuses = {result.type: result.status for result in verification.results}
    assert statuses["exit_code_equivalent"] == "pass"
    assert statuses["output_equivalent"] == "pass"
    assert statuses["forbid_new_dependency"] == "fail"
    assert statuses["max_operation_count"] == "fail"


def test_batch_drain_example_exposes_post_compute_backlog(tmp_path: Path) -> None:
    example = Path(__file__).parents[1] / "examples" / "local" / "batch_drain.py"
    runpack = tmp_path / "batch-drain.runpack"

    exit_code = record_process((sys.executable, str(example)), runpack, name="batch-drain")
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert [phase.name for phase in analysis.lifecycle] == ["compute", "result-drain"]
    assert analysis.throughput is not None
    assert analysis.throughput.completed == 100
    assert analysis.throughput.total == 100
    assert analysis.throughput.remaining_at_compute_completion == 20
    assert analysis.throughput.post_compute_seconds is not None
    assert analysis.throughput.post_compute_seconds > 0
    assert any(
        item.classification == "serialized_stage" and "result-drain" in item.evidence
        for item in analysis.bottlenecks
    )


def test_retrying_batch_example_exposes_safe_exception_churn_diagnosis(
    tmp_path: Path,
) -> None:
    example = Path(__file__).parents[1] / "examples" / "local" / "retrying_batch.py"
    runpack = tmp_path / "retrying-batch.runpack"

    exit_code = record_process(
        (sys.executable, str(example)),
        runpack,
        name="retrying-batch",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.deep_profile is not None
    assert analysis.deep_profile.status == "complete"
    assert analysis.deep_profile.python_exception_capture is not None
    assert analysis.deep_profile.python_exception_capture.status == "complete"
    assert analysis.deep_profile.observer_integrity is not None
    assert analysis.deep_profile.observer_integrity.status == "complete"
    finding = next(
        item for item in analysis.bottlenecks if item.classification == "python_exception_churn"
    )
    assert finding.evidence == (
        "__main__.load_with_retry recorded 32 non-control-flow Python exception propagation "
        "events across 16 calls (2.00 per call); built-in iterator completion is excluded "
        "and events are not unique failures"
    )
    assert b"tenant-token-must-not-be-captured" not in runpack.read_bytes()


def test_http_client_example_exposes_redacted_zero_touch_boundary(tmp_path: Path) -> None:
    example = Path(__file__).parents[1] / "examples" / "local" / "http_client.py"
    runpack = tmp_path / "http-client.runpack"

    exit_code = record_process(
        (sys.executable, str(example)),
        runpack,
        name="http-client",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "complete"
    assert analysis.http_capture.caller_attribution_status == "complete"
    assert len(analysis.http_requests) == 1
    request = analysis.http_requests[0]
    assert (request.method, request.scheme, request.status_code) == ("POST", "http", 202)
    assert request.duration_seconds is not None and request.duration_seconds >= 0.08
    assert request.caller is not None
    assert request.caller.name == "__main__.publish_batch"
    assert request.caller.observation == "exact"
    assert analysis.network_connections == ()


def test_wsgi_server_example_exposes_redacted_zero_touch_inbound_boundaries(
    tmp_path: Path,
) -> None:
    example = Path(__file__).parents[1] / "examples" / "local" / "wsgi_server.py"
    runpack = tmp_path / "wsgi-server.runpack"

    exit_code = record_process(
        (sys.executable, str(example)),
        runpack,
        name="wsgi-server",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "complete"
    requests = tuple(
        operation for operation in analysis.logical_operations if operation.category == "server"
    )
    assert len(requests) == 2
    assert {request.status_code for request in requests} == {200, 503}
    assert {request.outcome for request in requests} == {"completed", "operation_error"}
    assert all(
        request.adapter == "stdlib.wsgiref"
        and request.caller is not None
        and request.caller.name == "__main__.application"
        for request in requests
    )
    runpack_bytes = runpack.read_bytes()
    for private_value in (
        b"private-success-path",
        b"private-failure-path",
        b"private-response-header-value",
        b"private-request-header-value",
        b"private-failure-response-body",
        b"private-success-response-body",
    ):
        assert private_value not in runpack_bytes


def test_network_connection_example_exposes_redacted_zero_touch_boundaries(
    tmp_path: Path,
) -> None:
    example = Path(__file__).parents[1] / "examples" / "local" / "network_connections.py"
    runpack = tmp_path / "network-connections.runpack"

    exit_code = record_process(
        (sys.executable, str(example)),
        runpack,
        name="network-connections",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "complete"
    assert len(analysis.network_connections) == 2
    assert {
        (connection.adapter, connection.caller.name if connection.caller else None)
        for connection in analysis.network_connections
    } == {
        ("stdlib.socket.connect", "__main__.write_cache_entry"),
        ("asyncio.create_connection", "__main__.publish_queue_message"),
    }
    assert all(
        connection.transport == "tcp"
        and connection.outcome == "connected"
        and connection.server_port is not None
        for connection in analysis.network_connections
    )


def test_protocol_client_shapes_distinguish_pooling_reconnects_retries_and_async(
    tmp_path: Path,
) -> None:
    example = Path(__file__).parents[1] / "examples" / "local" / "protocol_client_shapes.py"
    runpack = tmp_path / "protocol-client-shapes.runpack"

    exit_code = record_process(
        (sys.executable, str(example)),
        runpack,
        name="protocol-client-shapes",
        capture_level="deep",
    )
    analysis = analyze_runpack(runpack)

    assert exit_code == 0
    assert analysis.network_capture is not None
    assert analysis.network_capture.status == "complete"
    assert analysis.network_capture.connection_count == 24
    hotspots = {
        hotspot.caller.name: hotspot
        for hotspot in analysis.network_connection_hotspots
        if hotspot.caller is not None
    }
    expected = {
        "__main__.PooledCacheClient.__init__": (1, 1, 0),
        "__main__.ReconnectingQueueClient.publish": (12, 12, 0),
        "__main__.connect_with_retry": (3, 1, 2),
        "__main__.publish_one": (8, 8, 0),
    }
    assert {
        name: (
            hotspot.connection_count,
            hotspot.connected_connection_count,
            hotspot.failed_connection_count,
        )
        for name, hotspot in hotspots.items()
    } == expected
    assert all(
        hotspot.caller is not None and hotspot.caller.observation == "exact"
        for hotspot in hotspots.values()
    )
    findings = {finding.classification for finding in analysis.bottlenecks}
    assert {"connection_failures", "connection_churn"} <= findings
    assert analysis.http_requests == ()
    report = render_analysis(analysis, "text")
    assert "Network connection hotspots" in report
    assert "__main__.PooledCacheClient.__init__" in report
    assert "1 attempt: 1 connected, 0 failed, 0 unfinished" in report
    assert "__main__.ReconnectingQueueClient.publish" in report
    assert "12 attempts: 12 connected, 0 failed, 0 unfinished" in report


def test_temporal_otel_example_finds_a_straggler_without_inventing_progress(
    tmp_path: Path,
) -> None:
    source = Path(__file__).parents[1] / "examples" / "otel" / "temporal-python-fanout.json"
    runpack = tmp_path / "temporal.runpack"

    result = import_otlp_json(source, runpack, name="temporal-fanout")
    analysis = analyze_runpack(runpack)

    assert result.event_count == 15
    assert analysis.total_seconds == 45.1
    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"
    assert analysis.throughput is None
    assert [(phase.name, phase.source) for phase in analysis.lifecycle] == [
        ("executing", "derived")
    ]
    assert [item.classification for item in analysis.bottlenecks] == ["straggler_tail"]
    assert analysis.bottlenecks[0].evidence == (
        "RunActivity:compute-chunk max duration 18.800s versus 6.850s median across 5 operations"
    )
