from __future__ import annotations

import copy
import json
import math
import re
from dataclasses import replace
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

import pytest

from runtime_tools.batchscope.analysis import (
    BatchAnalysis,
    Bottleneck,
    CriticalPath,
    HttpCaller,
    HttpCaptureSummary,
    HttpRequest,
    LifecyclePhase,
    LogicalOperation,
    LogicalOperationCaptureSummary,
    LogicalOperationHotspot,
    NetworkCaller,
    NetworkCaptureSummary,
    NetworkConnection,
    NetworkConnectionHotspot,
    NetworkSetupCaptureSummary,
    NetworkSetupHotspot,
    NetworkSetupPhase,
    SemanticCaptureSummary,
    SubprocessCall,
    SubprocessCaller,
    Throughput,
)
from runtime_tools.capture_jobs import CaptureJob, capture_job_document, capture_jobs_document
from runtime_tools.inspect import ExecutionSummary
from runtime_tools.json_support import OUTPUT_FORMAT_VERSION, output_document
from runtime_tools.proofline import cli as proofline_cli
from runtime_tools.proofline.contracts import Assertion
from runtime_tools.proofline.counterexamples import CounterexampleResult
from runtime_tools.proofline.experiments import ExperimentResult
from runtime_tools.proofline.validation import ValidationReport
from runtime_tools.proofline.verify import (
    ClaimResult,
    DiffEvidenceReference,
    VerificationArtifactBindings,
    VerificationReport,
)
from runtime_tools.query import QueryResult
from runtime_tools.rundiff.compare import (
    EdgeCountChange,
    EntityCountChange,
    EnvironmentChange,
    ExecutionDiff,
    OperationConcurrencyChange,
    OperationCountChange,
    OperationDurationChange,
    ValueChange,
)
from runtime_tools.storage import RunpackArtifactIdentity

GOLDEN_DIRECTORY = Path(__file__).parent / "fixtures" / "golden"
PUBLIC_DOCUMENT_TYPES = (
    "runtime.inspect",
    "runtime.query",
    "runtime.capture_job",
    "runtime.capture_jobs",
    "rundiff.compare",
    "batchscope.inspect",
    "proofline.verification",
    "proofline.experiment",
    "proofline.search",
    "proofline.validation",
)


def _summary() -> ExecutionSummary:
    return ExecutionSummary(
        schema_version="1.1",
        producer_version="0.9.0",
        id="execution-1",
        name="example",
        command=("python", "workload.py"),
        working_directory="/work",
        revision="abc123",
        started_at_ns=1_000_000_000,
        finished_at_ns=3_500_000_000,
        exit_code=0,
        wall_time_seconds=2.5,
        cpu_user_seconds=1.25,
        cpu_system_seconds=0.25,
        peak_memory_bytes=4096,
        stdout_bytes=12,
        stdout_sha256="a" * 64,
        stdout_complete=True,
        stdout_relay_error=None,
        stderr_bytes=3,
        stderr_sha256="b" * 64,
        stderr_complete=False,
        stderr_relay_error="relay stopped",
        annotation_error=None,
        missing_causal_references=1,
        dropped_attribute_count=2,
        record_counts={"entities": 2, "events": 3, "executions": 1},
    )


def _diff() -> ExecutionDiff:
    return ExecutionDiff(
        baseline_id="baseline-1",
        baseline_name="baseline",
        candidate_id="candidate-1",
        candidate_name="candidate",
        match_level="structural",
        baseline_annotation_error=None,
        candidate_annotation_error="partial annotations",
        baseline_missing_causal_references=0,
        candidate_missing_causal_references=2,
        baseline_dropped_attribute_count=1,
        candidate_dropped_attribute_count=3,
        baseline_semantic_capture_status="complete",
        candidate_semantic_capture_status="truncated",
        baseline_dropped_subprocess_count=0,
        candidate_dropped_subprocess_count=2,
        baseline_http_capture_status="complete",
        candidate_http_capture_status="truncated",
        baseline_dropped_http_request_count=0,
        candidate_dropped_http_request_count=1,
        baseline_network_capture_status="complete",
        candidate_network_capture_status="truncated",
        baseline_dropped_network_connection_count=0,
        candidate_dropped_network_connection_count=2,
        baseline_network_setup_capture_status="complete",
        candidate_network_setup_capture_status="truncated",
        baseline_dropped_network_setup_phase_count=0,
        candidate_dropped_network_setup_phase_count=3,
        baseline_logical_operation_capture_status="complete",
        candidate_logical_operation_capture_status="truncated",
        baseline_dropped_logical_operation_count=0,
        candidate_dropped_logical_operation_count=4,
        baseline_stdout_relay_error=None,
        baseline_stderr_relay_error=None,
        candidate_stdout_relay_error="relay stopped",
        candidate_stderr_relay_error=None,
        baseline_incomplete_streams=("stderr",),
        candidate_incomplete_streams=("stdout",),
        outcome="different",
        baseline_exit_code=0,
        candidate_exit_code=2,
        exit_code_equivalent=False,
        output_equivalent=None,
        stderr_equivalent=True,
        operation_errors_equivalent=False,
        wall_time=ValueChange(2.0, 3.0, 50.0),
        cpu_time=ValueChange(1.0, None, None),
        critical_path=ValueChange(1.5, 2.0, 100.0 / 3.0),
        baseline_critical_path_certainty="observed",
        candidate_critical_path_certainty="inferred",
        peak_memory=ValueChange(1024.0, 2048.0, 100.0),
        entity_count_changes=(EntityCountChange("service", "worker", 1, 2),),
        operation_count_changes=(
            OperationCountChange("service", "worker", "task", "run", 2, 3, 50.0),
        ),
        operation_error_count_changes=(
            OperationCountChange("service", "worker", "task", "run", 0, 1, None),
        ),
        operation_concurrency_changes=(
            OperationConcurrencyChange("service", "worker", "task", "run", 1, 2, 100.0),
        ),
        operation_duration_changes=(
            OperationDurationChange("service", "worker", "task", "run", 0.5, 0.75, 50.0),
        ),
        edge_count_changes=(EdgeCountChange("service", "api", "service", "worker", "calls", 0, 1),),
        environment_changes=(EnvironmentChange("WORKERS", True, False),),
    )


def _batch_analysis() -> BatchAnalysis:
    return BatchAnalysis(
        execution_id="execution-1",
        name="example",
        total_seconds=2.5,
        lifecycle=(LifecyclePhase("compute", 1.5, "explicit"),),
        critical_path=CriticalPath(
            duration_seconds=2.0,
            active_seconds=1.5,
            waiting_seconds=0.5,
            parallel_slack_seconds=0.25,
            event_ids=("event-1", "event-2"),
            event_names=("prepare", "run"),
            certainty="observed",
            cycle_detected=False,
        ),
        throughput=Throughput(
            completed=8.0,
            total=10.0,
            rate_per_second=4.0,
            remaining=2.0,
            estimated_drain_seconds=0.5,
            compute_finished_at_ns=3_000_000_000,
            remaining_at_compute_completion=4.0,
            post_compute_seconds=0.5,
            post_compute_rate_per_second=4.0,
        ),
        bottlenecks=(Bottleneck("compute", "critical path dominates", 0.9),),
    )


def _verification() -> VerificationReport:
    return VerificationReport(
        "baseline-1",
        "candidate-1",
        (
            ClaimResult(
                "latency",
                "wall time",
                "relative_change",
                "pass",
                "at most 10%",
                "5%",
            ),
            ClaimResult(
                "output",
                "stdout",
                "output_equal",
                "unverifiable",
                "equal",
                "candidate stdout incomplete",
            ),
        ),
    )


def _experiment() -> ExperimentResult:
    return ExperimentResult(
        Path("/artifacts/baseline.runpack"),
        Path("/artifacts/candidate.runpack"),
        0,
        1,
        _verification(),
    )


def _counterexample() -> CounterexampleResult:
    return CounterexampleResult({"workers": 2, "batches": 4}, _experiment(), True)


def _documents() -> dict[str, dict[str, Any]]:
    capture_job = CaptureJob(
        "1" * 32,
        "runtime record",
        "complete",
        1234,
        1_000_000_000,
        2_000_000_000,
        True,
        0,
        ("/artifacts/example.runpack",),
        detached=True,
        stdout_size_bytes=128,
        stderr_size_bytes=64,
        stderr_truncated=True,
        output_retention="head-tail",
        stdout_head_size_bytes=128,
        stderr_head_size_bytes=64,
    )
    return {
        "runtime.inspect": _summary().as_json_value(),
        "runtime.query": QueryResult(
            ("nothing", "enabled", "count", "ratio", "message", "blob"),
            ((None, True, 3, 1.25, "ok", {"encoding": "hex", "value": "ff"}),),
            False,
        ).as_json_value(),
        "runtime.capture_job": capture_job_document(capture_job),
        "runtime.capture_jobs": capture_jobs_document((capture_job,)),
        "rundiff.compare": _diff().as_json_value(),
        "batchscope.inspect": _batch_analysis().as_json_value(),
        "proofline.verification": _verification().as_json_value(),
        "proofline.experiment": _experiment().as_json_value(),
        "proofline.search": output_document(
            "proofline.search",
            {"counterexample": _counterexample().as_json_value(), "max_examples": 7},
        ),
        "proofline.validation": ValidationReport(
            1, 2, ("output_equal", "relative_change"), 2
        ).as_json_value(),
    }


def _load_json(path: Path) -> dict[str, Any]:
    def reject_nonfinite(value: str) -> None:
        raise ValueError(f"non-finite JSON number: {value}")

    result = json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_nonfinite)
    assert isinstance(result, dict)
    return result


def _schema() -> dict[str, Any]:
    schema_path = files("runtime_tools").joinpath("schemas", "contrail-output-v1.schema.json")
    result = json.loads(schema_path.read_text(encoding="utf-8"))
    assert isinstance(result, dict)
    return result


def _matches_type(value: object, expected: str) -> bool:
    if expected == "null":
        return value is None
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return (
            isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        )
    if expected == "string":
        return isinstance(value, str)
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict) and all(isinstance(key, str) for key in value)
    raise AssertionError(f"unsupported schema type in compatibility test: {expected}")


def _validate(value: object, rule: dict[str, Any], root: dict[str, Any], path: str = "$") -> None:
    if "$ref" in rule:
        reference = rule["$ref"]
        assert isinstance(reference, str) and reference.startswith("#/$defs/")
        _validate(value, root["$defs"][reference.removeprefix("#/$defs/")], root, path)
        return
    if "oneOf" in rule:
        matches = 0
        for option in rule["oneOf"]:
            try:
                _validate(value, option, root, path)
            except AssertionError:
                continue
            matches += 1
        assert matches == 1, f"{path}: expected exactly one schema match, got {matches}"
        return
    if "anyOf" in rule:
        for option in rule["anyOf"]:
            try:
                _validate(value, option, root, path)
            except AssertionError:
                continue
            return
        raise AssertionError(f"{path}: did not match any allowed schema")

    if "const" in rule:
        assert value == rule["const"], f"{path}: expected {rule['const']!r}, got {value!r}"
    if "enum" in rule:
        assert value in rule["enum"], f"{path}: {value!r} is not an allowed value"
    if "type" in rule:
        expected_types = rule["type"]
        if isinstance(expected_types, str):
            expected_types = [expected_types]
        assert any(_matches_type(value, item) for item in expected_types), (
            f"{path}: expected {expected_types}, got {type(value).__name__}"
        )
    if "minimum" in rule:
        assert isinstance(value, (int, float)) and value >= rule["minimum"], (
            f"{path}: expected at least {rule['minimum']}"
        )
    if "pattern" in rule:
        assert isinstance(value, str) and re.fullmatch(rule["pattern"], value), (
            f"{path}: did not match {rule['pattern']!r}"
        )
    if isinstance(value, dict):
        required = rule.get("required", ())
        missing = set(required) - value.keys()
        assert not missing, f"{path}: missing required properties {sorted(missing)}"
        properties = rule.get("properties", {})
        additional = rule.get("additionalProperties", True)
        for key, item in value.items():
            if key in properties:
                _validate(item, properties[key], root, f"{path}.{key}")
            elif isinstance(additional, dict):
                _validate(item, additional, root, f"{path}.{key}")
            else:
                assert additional, f"{path}: unexpected property {key!r}"
    if isinstance(value, list) and "items" in rule:
        for index, item in enumerate(value):
            _validate(item, rule["items"], root, f"{path}[{index}]")


def test_public_models_match_complete_golden_documents() -> None:
    actual = _documents()

    assert tuple(actual) == PUBLIC_DOCUMENT_TYPES
    for document_type, value in actual.items():
        assert value == _load_json(GOLDEN_DIRECTORY / f"{document_type}.json")


def test_packaged_schema_validates_all_golden_documents_without_optional_dependency() -> None:
    schema = _schema()
    public_definitions = {
        reference["$ref"].removeprefix("#/$defs/") for reference in schema["oneOf"]
    }

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert public_definitions == set(PUBLIC_DOCUMENT_TYPES)
    for document_type in PUBLIC_DOCUMENT_TYPES:
        document = _load_json(GOLDEN_DIRECTORY / f"{document_type}.json")
        definition = schema["$defs"][document_type]
        assert document["document_type"] == document_type
        assert definition["properties"]["document_type"] == {"const": document_type}
        assert definition["properties"]["format_version"] == {"const": OUTPUT_FORMAT_VERSION}
        assert set(document) == set(definition["required"])
        assert set(document) <= set(definition["properties"])
        _validate(document, definition, schema)
        _validate(document, schema, schema)


def test_packaged_schema_types_optional_semantic_boundary_output() -> None:
    schema = _schema()
    analysis = replace(
        _batch_analysis(),
        semantic_capture=SemanticCaptureSummary(
            "complete",
            1,
            1,
            0,
            0,
            0,
            "complete",
            1,
            1,
            0,
            0,
            0,
        ),
        subprocess_calls=(
            SubprocessCall(
                "semantic:subprocess:42:0",
                "python",
                42,
                "root",
                43,
                True,
                None,
                "exited",
                0,
                None,
                1_000_000_000,
                0.25,
                SubprocessCaller(
                    "semantic:caller:abc",
                    "application.run",
                    "application",
                    "run",
                    "/work/application.py",
                    4,
                    "application",
                    "exact",
                    1.0,
                ),
            ),
        ),
        http_capture=HttpCaptureSummary(
            "complete",
            1,
            1,
            0,
            0,
            0,
            "complete",
            1,
            1,
            0,
            0,
            0,
            ("httpcore.sync", "stdlib.http.client"),
        ),
        http_requests=(
            HttpRequest(
                "semantic:http:42:0",
                "GET",
                "https",
                443,
                42,
                "root",
                "response",
                200,
                None,
                1_000_000_000,
                0.1,
                HttpCaller(
                    "semantic:http-caller:abc",
                    "application.fetch",
                    "application",
                    "fetch",
                    "/work/application.py",
                    8,
                    "application",
                    "sampled",
                    0.9,
                ),
                "httpcore.sync",
            ),
        ),
        network_capture=NetworkCaptureSummary(
            "complete",
            1,
            1,
            0,
            0,
            0,
            "complete",
            1,
            1,
            0,
            0,
            0,
            ("stdlib.socket.connect",),
            1,
        ),
        network_connections=(
            NetworkConnection(
                "semantic:network:42:0",
                "stdlib.socket.connect",
                "tcp",
                "ipv4",
                5432,
                None,
                42,
                "root",
                "connected",
                None,
                1_000_000_000,
                0.05,
                NetworkCaller(
                    "semantic:network-caller:abc",
                    "application.connect_database",
                    "application",
                    "connect_database",
                    "/work/application.py",
                    12,
                    "application",
                    "exact",
                    1.0,
                ),
            ),
        ),
        network_connection_hotspots=(
            NetworkConnectionHotspot(
                "stdlib.socket.connect",
                1,
                1,
                0,
                0,
                0.05,
                0.05,
                NetworkCaller(
                    "semantic:network-caller:abc",
                    "application.connect_database",
                    "application",
                    "connect_database",
                    "/work/application.py",
                    12,
                    "application",
                    "exact",
                    1.0,
                ),
            ),
        ),
        network_setup_capture=NetworkSetupCaptureSummary(
            "complete",
            1,
            1,
            0,
            0,
            0,
            "complete",
            1,
            1,
            0,
            0,
            0,
            ("stdlib.socket.getaddrinfo",),
            1,
        ),
        network_setup_phases=(
            NetworkSetupPhase(
                "semantic:network-setup:42:0",
                "dns",
                "stdlib.socket.getaddrinfo",
                42,
                "root",
                "completed",
                None,
                1_000_000_000,
                0.02,
                NetworkCaller(
                    "semantic:network-setup-caller:abc",
                    "application.resolve_database",
                    "application",
                    "resolve_database",
                    "/work/application.py",
                    16,
                    "application",
                    "exact",
                    1.0,
                ),
            ),
        ),
        network_setup_hotspots=(
            NetworkSetupHotspot(
                "dns",
                "stdlib.socket.getaddrinfo",
                1,
                1,
                0,
                0,
                0.02,
                0.02,
                NetworkCaller(
                    "semantic:network-setup-caller:abc",
                    "application.resolve_database",
                    "application",
                    "resolve_database",
                    "/work/application.py",
                    16,
                    "application",
                    "exact",
                    1.0,
                ),
            ),
        ),
        logical_operation_capture=LogicalOperationCaptureSummary(
            "complete",
            1,
            1,
            0,
            0,
            0,
            "complete",
            1,
            1,
            0,
            0,
            0,
            ("stdlib.sqlite3.Connection",),
            1,
        ),
        logical_operations=(
            LogicalOperation(
                "semantic:logical-operation:42:0",
                "database",
                "execute",
                "stdlib.sqlite3.Connection",
                42,
                "root",
                "completed",
                None,
                None,
                1_000_000_000,
                0.03,
                NetworkCaller(
                    "semantic:logical-operation-caller:abc",
                    "application.execute_database",
                    "application",
                    "execute_database",
                    "/work/application.py",
                    20,
                    "application",
                    "exact",
                    1.0,
                ),
            ),
        ),
        logical_operation_hotspots=(
            LogicalOperationHotspot(
                "database",
                "execute",
                "stdlib.sqlite3.Connection",
                1,
                1,
                0,
                0,
                0.03,
                0.03,
                NetworkCaller(
                    "semantic:logical-operation-caller:abc",
                    "application.execute_database",
                    "application",
                    "execute_database",
                    "/work/application.py",
                    20,
                    "application",
                    "exact",
                    1.0,
                ),
            ),
        ),
    )
    document = analysis.as_json_value()

    http_capture = document["http_capture"]
    assert isinstance(http_capture, dict)
    assert http_capture["adapters"] == ["httpcore.sync", "stdlib.http.client"]
    http_requests = document["http_requests"]
    assert isinstance(http_requests, list)
    http_request = http_requests[0]
    assert isinstance(http_request, dict)
    assert http_request["adapter"] == "httpcore.sync"
    network_capture = document["network_capture"]
    assert isinstance(network_capture, dict)
    assert network_capture["adapters"] == ["stdlib.socket.connect"]
    assert network_capture["connection_hotspot_count"] == 1
    network_connections = document["network_connections"]
    assert isinstance(network_connections, list)
    network_connection = network_connections[0]
    assert isinstance(network_connection, dict)
    assert network_connection["server_address"] is None
    assert network_connection["server_port"] == 5432
    network_hotspots = document["network_connection_hotspots"]
    assert isinstance(network_hotspots, list)
    network_hotspot = network_hotspots[0]
    assert isinstance(network_hotspot, dict)
    assert network_hotspot["connection_count"] == 1
    assert network_hotspot["connected_connection_count"] == 1
    assert network_hotspot["failed_connection_count"] == 0
    network_setup_capture = document["network_setup_capture"]
    assert isinstance(network_setup_capture, dict)
    assert network_setup_capture["hostname_captured"] is False
    assert network_setup_capture["hotspot_count"] == 1
    network_setup_phases = document["network_setup_phases"]
    assert isinstance(network_setup_phases, list)
    network_setup_phase = network_setup_phases[0]
    assert isinstance(network_setup_phase, dict)
    assert network_setup_phase["phase"] == "dns"
    network_setup_hotspots = document["network_setup_hotspots"]
    assert isinstance(network_setup_hotspots, list)
    network_setup_hotspot = network_setup_hotspots[0]
    assert isinstance(network_setup_hotspot, dict)
    assert network_setup_hotspot["completed_phase_count"] == 1
    logical_operation_capture = document["logical_operation_capture"]
    assert isinstance(logical_operation_capture, dict)
    assert logical_operation_capture["statement_captured"] is False
    assert logical_operation_capture["awaitable_captured"] is False
    assert logical_operation_capture["task_name_captured"] is False
    assert logical_operation_capture["context_captured"] is False
    assert logical_operation_capture["route_captured"] is False
    assert logical_operation_capture["headers_captured"] is False
    assert logical_operation_capture["hotspot_count"] == 1
    logical_operations = document["logical_operations"]
    assert isinstance(logical_operations, list)
    logical_operation = logical_operations[0]
    assert isinstance(logical_operation, dict)
    assert logical_operation["operation"] == "execute"
    logical_operation_hotspots = document["logical_operation_hotspots"]
    assert isinstance(logical_operation_hotspots, list)
    logical_operation_hotspot = logical_operation_hotspots[0]
    assert isinstance(logical_operation_hotspot, dict)
    assert logical_operation_hotspot["completed_operation_count"] == 1

    _validate(document, schema["$defs"]["batchscope.inspect"], schema)
    _validate(document, schema, schema)

    for category, operation, adapter in (
        ("cache", "command", "redis.Redis"),
        ("broker", "publish", "pika.BlockingChannel"),
        ("scheduler", "task", "stdlib.asyncio.create_task"),
        ("scheduler", "task", "stdlib.asyncio.ensure_future"),
        ("scheduler", "task", "stdlib.asyncio.gather"),
        ("server", "request", "stdlib.wsgiref"),
    ):
        optional_client_document = copy.deepcopy(document)
        optional_capture = cast(
            dict[str, Any], optional_client_document["logical_operation_capture"]
        )
        optional_capture["adapters"] = [adapter]
        optional_operations = cast(
            list[dict[str, Any]], optional_client_document["logical_operations"]
        )
        optional_operations[0]["category"] = category
        optional_operations[0]["operation"] = operation
        optional_operations[0]["adapter"] = adapter
        optional_operations[0]["duration_boundary"] = (
            "creation_to_completion" if category == "scheduler" else "logical_operation"
        )
        if category == "server":
            optional_operations[0]["duration_boundary"] = "request_to_response_completion"
            optional_operations[0]["status_code"] = 200
        optional_hotspots = cast(
            list[dict[str, Any]], optional_client_document["logical_operation_hotspots"]
        )
        optional_hotspots[0]["category"] = category
        optional_hotspots[0]["operation"] = operation
        optional_hotspots[0]["adapter"] = adapter
        _validate(optional_client_document, schema["$defs"]["batchscope.inspect"], schema)
        _validate(optional_client_document, schema, schema)


def test_packaged_schema_types_native_call_output() -> None:
    schema = _schema()
    document = _batch_analysis().as_json_value()
    document["deep_profile"] = {
        "status": "complete",
        "native_call_capture": {
            "status": "complete",
            "enabled": True,
            "deep_only": True,
            "function_count": 1,
            "call_count": 4,
            "exception_count": 1,
            "arguments_captured": False,
            "return_values_captured": False,
            "exception_messages_captured": False,
            "max_functions_per_process": 2_000,
            "max_edges_per_process": 10_000,
        },
        "python_exception_capture": {
            "status": "complete",
            "enabled": True,
            "deep_only": True,
            "event_semantics": "per_propagated_frame",
            "function_count": 1,
            "event_count": 2,
            "dropped_event_count": 0,
            "arguments_captured": False,
            "locals_captured": False,
            "exception_types_captured": False,
            "exception_values_captured": False,
            "exception_messages_captured": False,
            "tracebacks_captured": False,
            "line_events_enabled": False,
            "opcode_events_enabled": False,
            "max_functions_per_process": 2_000,
        },
        "observer_integrity": {
            "format_version": 1,
            "status": "complete",
            "process_count": 1,
            "missing_process_count": 0,
            "profile_hook_setter_call_count": 0,
            "profile_hook_setter_process_count": 0,
            "trace_hook_setter_call_count": 0,
            "trace_hook_setter_process_count": 0,
            "arguments_captured": False,
            "locals_captured": False,
            "hook_values_captured": False,
        },
    }
    document["python_hotspots"] = [
        {
            "name": "sqlite3.Connection.execute",
            "filename": "<native>",
            "firstlineno": 0,
            "scope": "runtime",
            "call_count": 4,
            "total_seconds": 0.002,
            "self_seconds": 0.002,
            "max_seconds": 0.001,
            "process_attribution_status": "complete",
            "processes": [
                {
                    "pid": 42,
                    "role": "root",
                    "process_name": "python",
                    "parent_name": None,
                    "observed_in_process_tree": True,
                    "call_count": 4,
                    "total_seconds": 0.002,
                    "self_seconds": 0.002,
                    "max_seconds": 0.001,
                    "exception_count": 1,
                }
            ],
            "implementation": "native",
            "exception_count": 1,
        }
    ]

    _validate(document, schema["$defs"]["batchscope.inspect"], schema)
    _validate(document, schema, schema)

    invalid = cast(dict[str, Any], copy.deepcopy(document))
    invalid_profile = cast(dict[str, Any], invalid["deep_profile"])
    invalid_native = cast(dict[str, Any], invalid_profile["native_call_capture"])
    invalid_native["arguments_captured"] = True
    with pytest.raises(AssertionError):
        _validate(invalid, schema["$defs"]["batchscope.inspect"], schema)

    invalid_integrity = cast(dict[str, Any], copy.deepcopy(document))
    invalid_profile = cast(dict[str, Any], invalid_integrity["deep_profile"])
    integrity = cast(dict[str, Any], invalid_profile["observer_integrity"])
    integrity["hook_values_captured"] = True
    with pytest.raises(AssertionError):
        _validate(invalid_integrity, schema["$defs"]["batchscope.inspect"], schema)


def test_packaged_schema_types_optional_proofline_explanations() -> None:
    schema = _schema()
    diff = _diff().as_json_value()
    documents = {
        "proofline.verification": _verification().as_json_value(),
        "proofline.experiment": _experiment().as_json_value(),
    }

    for document_type, document in documents.items():
        definition = schema["$defs"][document_type]
        assert definition["properties"]["diff"] == {"$ref": "#/$defs/rundiff.compare"}
        document["diff"] = diff
        _validate(document, definition, schema)
        _validate(document, schema, schema)

        invalid = copy.deepcopy(document)
        invalid["diff"] = _verification().as_json_value()
        with pytest.raises(AssertionError, match=r"\$\.diff:"):
            _validate(invalid, definition, schema)


def test_proofline_artifact_bindings_are_explicit_and_strictly_typed() -> None:
    schema = _schema()
    baseline = RunpackArtifactIdentity(123, "a" * 64)
    candidate = RunpackArtifactIdentity(456, "b" * 64)
    bindings = VerificationArtifactBindings(baseline, candidate)

    default_document = _verification().as_json_value(include_evidence=True)
    assert "artifact_bindings" not in default_document

    verification = _verification().as_json_value(
        include_evidence=True,
        artifact_bindings=bindings,
    )
    assert verification["artifact_bindings"] == {
        "baseline": {"size_bytes": 123, "sha256": "a" * 64},
        "candidate": {"size_bytes": 456, "sha256": "b" * 64},
    }
    _validate(verification, schema["$defs"]["proofline.verification"], schema)

    experiment = _experiment().as_json_value(include_evidence=True)
    experiment["artifact_bindings"] = bindings.as_json_value()
    _validate(experiment, schema["$defs"]["proofline.experiment"], schema)

    invalid_bindings: tuple[dict[str, Any], ...] = (
        {"baseline": baseline.as_json_value()},
        {
            "baseline": baseline.as_json_value(),
            "candidate": {"size_bytes": -1, "sha256": "b" * 64},
        },
        {
            "baseline": baseline.as_json_value(),
            "candidate": {"size_bytes": 456, "sha256": "B" * 64},
        },
    )
    for invalid in invalid_bindings:
        document = copy.deepcopy(verification)
        document["artifact_bindings"] = invalid
        with pytest.raises(AssertionError):
            _validate(document, schema["$defs"]["proofline.verification"], schema)


def test_packaged_schema_types_claim_evidence_references() -> None:
    schema = _schema()
    report = VerificationReport(
        "baseline",
        "candidate",
        (
            ClaimResult(
                "contract",
                "writes",
                "max_operation_count",
                "fail",
                "no write growth",
                "baseline=1, candidate=3",
                (
                    DiffEvidenceReference(
                        "/operation_count_changes",
                        (("operation_name", "db.write"),),
                        (("baseline", 1), ("candidate", 3), ("limit", 1.2)),
                    ),
                ),
                Assertion(
                    "max_operation_count",
                    "writes",
                    {
                        "type": "max_operation_count",
                        "name": "writes",
                        "operation": "db.write",
                        "relative_to": "baseline",
                        "factor": 1.2,
                    },
                ),
            ),
        ),
    )
    document = cast(dict[str, Any], report.as_json_value(include_evidence=True))

    _validate(document, schema["$defs"]["proofline.verification"], schema)
    assert document["results"][0]["evidence"] == [
        {
            "fact": {"baseline": 1, "candidate": 3, "limit": 1.2},
            "diff_path": "/operation_count_changes",
            "selector": {"operation_name": "db.write"},
        }
    ]
    assert document["results"][0]["assertion"] == {
        "type": "max_operation_count",
        "name": "writes",
        "operation": "db.write",
        "relative_to": "baseline",
        "factor": 1.2,
    }

    invalid_path = copy.deepcopy(document)
    invalid_path["results"][0]["evidence"][0]["diff_path"] = "/not-a-diff-field"
    with pytest.raises(AssertionError, match="not an allowed value"):
        _validate(invalid_path, schema["$defs"]["proofline.verification"], schema)

    invalid_selector = copy.deepcopy(document)
    invalid_selector["results"][0]["evidence"][0]["selector"] = {"operation_name": 3}
    with pytest.raises(AssertionError, match="expected.*string"):
        _validate(invalid_selector, schema["$defs"]["proofline.verification"], schema)

    invalid_assertion = copy.deepcopy(document)
    del invalid_assertion["results"][0]["assertion"]["factor"]
    with pytest.raises(AssertionError, match="expected exactly one schema match"):
        _validate(invalid_assertion, schema["$defs"]["proofline.verification"], schema)


def test_schema_rejects_missing_required_property_and_wrong_type() -> None:
    schema = _schema()
    definition = schema["$defs"]["runtime.inspect"]
    document = _load_json(GOLDEN_DIRECTORY / "runtime.inspect.json")
    missing = copy.deepcopy(document)
    del missing["started_at_ns"]
    wrong_type = copy.deepcopy(document)
    wrong_type["started_at_ns"] = "1000000000"

    with pytest.raises(AssertionError, match="missing required properties"):
        _validate(missing, definition, schema)
    with pytest.raises(AssertionError, match="expected.*integer"):
        _validate(wrong_type, definition, schema)


def test_proofline_search_cli_matches_golden_document(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        proofline_cli, "search_counterexample", lambda *args, **kwargs: _counterexample()
    )

    result = proofline_cli.main(
        [
            "search",
            "contract.yaml",
            "--parameters",
            "parameters.yaml",
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "feature",
            "--workload",
            "workload.py",
            "--output-dir",
            "/artifacts",
            "--max-examples",
            "7",
            "--format",
            "json",
        ]
    )

    assert result == 1
    assert json.loads(capsys.readouterr().out) == _load_json(
        GOLDEN_DIRECTORY / "proofline.search.json"
    )


def test_output_document_reserves_protocol_member_names() -> None:
    for reserved in ("document_type", "format_version"):
        with pytest.raises(
            ValueError, match="output document body contains reserved protocol fields"
        ):
            output_document("test", {reserved: "collision"})
