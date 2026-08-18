from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from runtime_tools.model import Entity, Event, Execution, Measurement
from runtime_tools.proofline import ContractError, verify_contracts
from runtime_tools.storage import RunpackWriter


def _write_runpack(path: Path, *, candidate: bool) -> None:
    finished_at_ns = 200_000_000 if candidate else 100_000_000
    events = [
        Event(
            "root",
            "server.request",
            "request",
            "app",
            0,
            finished_at_ns,
            "test",
            None,
            None,
            {},
        )
    ]
    for index in range(3 if candidate else 1):
        events.append(
            Event(
                f"write-{index}",
                "client.request",
                "db.write",
                "app",
                index + 1,
                index + 2,
                "test",
                None,
                index,
                {"peer.service": "results-db"},
            )
        )
    if candidate:
        events.append(
            Event(
                "metadata",
                "client.request",
                "metadata.lookup",
                "app",
                10,
                11,
                "test",
                None,
                None,
                {"peer.service": "metadata-db"},
            )
        )
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "candidate" if candidate else "baseline",
                "candidate" if candidate else "baseline",
                0,
                finished_at_ns,
                (),
                str(path.parent),
                0,
                None,
                {
                    "output": {
                        "stdout": {"bytes": 4, "sha256": "same"},
                        "stderr": {"bytes": 0, "sha256": "empty"},
                    }
                },
            )
        )
        writer.add_entity(Entity("app", "service", "app", None, {}))
        for event in events:
            writer.add_event(event)
        writer.add_measurement(
            Measurement(
                "process.memory.peak",
                110.0 if candidate else 100.0,
                "By",
                finished_at_ns,
                "app",
                {},
            )
        )


def _write_contract(path: Path) -> None:
    path.write_text(
        """
name: regression-contract
assertions:
  - type: output_equivalent
  - type: max_runtime_regression
    percent: 50
  - type: max_peak_memory_regression
    percent: 20
  - type: forbid_new_dependency
    from: app
    to: metadata-db
  - type: max_operation_count
    operation: db.write
    relative_to: baseline
    factor: 1.2
""".strip(),
        encoding="utf-8",
    )


def test_proofline_evaluates_deterministic_contract_types(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)

    report = verify_contracts(contract, baseline, candidate)

    assert report.passed is False
    assert [(result.type, result.status) for result in report.results] == [
        ("output_equivalent", "pass"),
        ("max_runtime_regression", "fail"),
        ("max_peak_memory_regression", "pass"),
        ("forbid_new_dependency", "fail"),
        ("max_operation_count", "fail"),
    ]
    operation = report.results[-1]
    assert operation.observed == "baseline=1, candidate=3, limit=1.2"


def test_proofline_cli_returns_failure_and_machine_readable_evidence(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)

    result = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.proofline.cli",
            "verify",
            str(contract),
            "--baseline",
            str(baseline),
            "--candidate",
            str(candidate),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["claim_count"] == 5
    assert payload["passed"] is False
    assert payload["results"][3]["observed"] == "candidate count=1"


def test_proofline_rejects_unsupported_assertion_type(tmp_path: Path) -> None:
    contract = tmp_path / "unsupported.yaml"
    contract.write_text(
        "name: unsupported\nassertions:\n  - type: ask_an_llm\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="unsupported assertion type"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_rejects_recursive_yaml_values(tmp_path: Path) -> None:
    contract = tmp_path / "recursive.yaml"
    contract.write_text(
        """
name: recursive
assertions:
  - &claim
    type: output_equivalent
    nested: *claim
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="recursive"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_rejects_negative_regression_thresholds(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "negative.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    contract.write_text(
        "name: negative\nassertions:\n  - type: max_runtime_regression\n    percent: -1\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="percent cannot be negative"):
        verify_contracts(contract, baseline, candidate)


def test_proofline_rejects_duplicate_contract_keys(tmp_path: Path) -> None:
    contract = tmp_path / "duplicate.yaml"
    contract.write_text(
        """
name: duplicate
assertions:
  - type: max_runtime_regression
    percent: 10
    percent: 100
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="found duplicate key 'percent'"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_normalizes_numeric_threshold_overflow(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "overflow.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    contract.write_text(
        f"name: overflow\nassertions:\n  - type: max_runtime_regression\n    percent: {10**400}\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="percent exceeds the numeric range"):
        verify_contracts(contract, baseline, candidate)


def test_proofline_keeps_structural_claims_unverifiable_after_annotation_failure(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "operation.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    with sqlite3.connect(candidate) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["capture"] = {"annotation_error": "invalid annotation JSON on line 1"}
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))
    contract.write_text(
        """
name: incomplete
assertions:
  - type: max_operation_count
    operation: db.write
    relative_to: baseline
    factor: 1.2
""".strip(),
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == "unverifiable"
    assert report.results[0].observed == "annotation evidence incomplete"
