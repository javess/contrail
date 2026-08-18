from __future__ import annotations

import copy
import json
import math
import re
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

import pytest

from runtime_tools.batchscope.analysis import (
    BatchAnalysis,
    Bottleneck,
    CriticalPath,
    LifecyclePhase,
    Throughput,
)
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
    return {
        "runtime.inspect": _summary().as_json_value(),
        "runtime.query": QueryResult(
            ("nothing", "enabled", "count", "ratio", "message", "blob"),
            ((None, True, 3, 1.25, "ok", {"encoding": "hex", "value": "ff"}),),
            False,
        ).as_json_value(),
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
