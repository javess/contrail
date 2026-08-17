from __future__ import annotations

import sys
from pathlib import Path

from runtime_tools import record_process
from runtime_tools.batchscope import analyze_runpack
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
