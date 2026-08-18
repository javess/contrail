from __future__ import annotations

import sys
from pathlib import Path

import pytest

from runtime_tools import record_process
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.proofline import verify_contracts
from runtime_tools.rundiff import compare_runpacks


@pytest.mark.parametrize(
    (
        "example_name",
        "script_name",
        "operation",
        "baseline_count",
        "candidate_count",
        "dependency",
        "failures",
    ),
    (
        (
            "checkout",
            "checkout_service.py",
            "payment.authorize",
            1,
            3,
            "legacy-risk-db",
            {"forbid_new_dependency", "max_operation_count", "max_operation_error_count"},
        ),
        (
            "analytics",
            "analytics_pipeline.py",
            "warehouse.upsert",
            8,
            32,
            "raw-profile-api",
            {"forbid_new_dependency", "max_operation_count"},
        ),
        (
            "inference",
            "inference_service.py",
            "feature.lookup",
            4,
            24,
            "fallback-model-registry",
            {"forbid_new_dependency", "max_operation_count"},
        ),
    ),
)
def test_complex_examples_preserve_results_and_explain_multiple_regressions(
    tmp_path: Path,
    example_name: str,
    script_name: str,
    operation: str,
    baseline_count: int,
    candidate_count: int,
    dependency: str,
    failures: set[str],
) -> None:
    root = Path(__file__).parents[1] / "examples" / "complex"
    example = root / script_name
    baseline = tmp_path / f"{example_name}-baseline.runpack"
    candidate = tmp_path / f"{example_name}-candidate.runpack"
    record_process((sys.executable, str(example)), baseline, name=f"{example_name}-baseline")
    record_process(
        (sys.executable, str(example), "--candidate"),
        candidate,
        name=f"{example_name}-candidate",
    )

    diff = compare_runpacks(baseline, candidate)
    verification = verify_contracts(
        root / f"{example_name}_contract.yaml",
        baseline,
        candidate,
    )

    assert diff.exit_code_equivalent is True
    assert diff.output_equivalent is True
    count_change = next(
        change for change in diff.operation_count_changes if change.operation_name == operation
    )
    assert (count_change.baseline, count_change.candidate) == (
        baseline_count,
        candidate_count,
    )
    assert any(
        edge.target_name == dependency and edge.change_kind == "new"
        for edge in diff.edge_count_changes
    )
    assert {result.type for result in verification.results if result.status == "fail"} == failures


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
