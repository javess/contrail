from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import runtime_tools.proofline.contracts as contracts_module
from runtime_tools.model import Entity, Event, Execution, Measurement
from runtime_tools.proofline import ContractError, verify_contracts
from runtime_tools.storage import RunpackWriter

_STDOUT_IDENTITY = "a" * 64
_STDERR_IDENTITY = "b" * 64


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
                {"peer.service": "results-db", "error": candidate and index == 0},
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
                        "stdout": {"bytes": 4, "sha256": _STDOUT_IDENTITY},
                        "stderr": {"bytes": 0, "sha256": _STDERR_IDENTITY},
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
        writer.add_measurements(
            (
                Measurement(
                    "process.cpu.user",
                    2.0 if candidate else 1.0,
                    "s",
                    finished_at_ns,
                    "app",
                    {},
                ),
                Measurement(
                    "process.cpu.system",
                    1.0 if candidate else 0.5,
                    "s",
                    finished_at_ns,
                    "app",
                    {},
                ),
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


def test_proofline_enforces_cpu_time_regression_limits(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "cpu-contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    contract.write_text(
        "name: cpu\nassertions:\n  - type: max_cpu_time_regression\n    percent: 50\n",
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == "fail"
    assert report.results[0].expected == "candidate CPU time <= baseline + 50%"
    assert report.results[0].observed == "baseline=1.5, candidate=3, limit=2.25"


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


@pytest.mark.parametrize(
    "document",
    (
        """
name: typo
assertions:
  - type: max_runtime_regression
    percent: 10
    percnet: 100
""",
        """
name: typo
description: ignored
contracts:
  - name: nested
    assertions:
      - type: output_equivalent
""",
    ),
)
def test_proofline_rejects_unknown_contract_fields(tmp_path: Path, document: str) -> None:
    contract = tmp_path / "unknown-field.yaml"
    contract.write_text(document, encoding="utf-8")

    with pytest.raises(ContractError, match="contains unsupported fields"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


@pytest.mark.parametrize(
    ("assertion", "message"),
    (
        ("type: max_runtime_regression", "requires fields: percent"),
        (
            "type: max_operation_count\n"
            "    operation: work\n"
            "    relative_to: candidate\n"
            "    factor: 1",
            "relative_to must be baseline",
        ),
    ),
)
def test_proofline_validates_assertion_fields_before_loading_runpacks(
    tmp_path: Path, assertion: str, message: str
) -> None:
    contract = tmp_path / "invalid-fields.yaml"
    contract.write_text(
        f"name: invalid\nassertions:\n  - {assertion}\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match=message):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_rejects_yaml_aliases_before_expansion(tmp_path: Path) -> None:
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

    with pytest.raises(ContractError, match="YAML aliases are not supported"):
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


def test_proofline_rejects_oversized_contracts_before_yaml_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = tmp_path / "oversized.yaml"
    contract.write_bytes(b"{" + b" " * 32 + b"}")
    monkeypatch.setattr(contracts_module, "MAX_CONTRACT_BYTES", 32)

    with pytest.raises(ContractError, match="contract file exceeds the 32-byte input limit"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_rejects_too_many_assertions_before_loading_runpacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = tmp_path / "too-many-assertions.yaml"
    contract.write_text(
        "name: bounded\nassertions:\n  - type: output_equivalent\n  - type: result_equivalence\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(contracts_module, "MAX_CONTRACT_ASSERTIONS", 1)

    with pytest.raises(ContractError, match="exceeds the 1-assertion input limit"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_rejects_non_utf8_contracts(tmp_path: Path) -> None:
    contract = tmp_path / "non-utf8.yaml"
    contract.write_bytes(b"\xff")

    with pytest.raises(ContractError, match="contract file must be UTF-8"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_rejects_non_utf8_yaml_strings(tmp_path: Path) -> None:
    contract = tmp_path / "non-utf8-string.yaml"
    contract.write_text(
        'name: "bad-\\uD800"\nassertions:\n  - type: output_equivalent\n',
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="string that is not valid UTF-8"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


def test_proofline_normalizes_excessive_contract_nesting(tmp_path: Path) -> None:
    contract = tmp_path / "nested.yaml"
    contract.write_text("value: " + "[" * 2_000 + "0" + "]" * 2_000, encoding="utf-8")

    with pytest.raises(ContractError, match="contract file nesting is too deep"):
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


def test_proofline_does_not_treat_an_unobserved_operation_as_zero(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "missing-operation.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    contract.write_text(
        """
name: missing-operation
assertions:
  - type: max_operation_count
    operation: db.delete
    relative_to: baseline
    factor: 1
""".strip(),
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == "unverifiable"
    assert report.results[0].observed == "operation db.delete not observed in either run"


def test_proofline_limits_explicit_operation_failures(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "operation-errors.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    contract.write_text(
        """
name: operation-errors
assertions:
  - type: max_operation_error_count
    operation: db.write
    relative_to: baseline
    factor: 1
""".strip(),
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == "fail"
    assert report.results[0].expected == "candidate db.write error count <= baseline x 1"
    assert report.results[0].observed == "baseline=0, candidate=1, limit=0"


def test_proofline_reports_incomplete_output_identity_as_unverifiable(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "output.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    with sqlite3.connect(baseline) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["output"]["stdout"]["pipe_open_after_exit"] = True
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == "unverifiable"
    assert report.results[0].observed == "output identity incomplete (baseline: stdout)"


def test_proofline_keeps_dependency_claims_unverifiable_with_causal_gaps(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "dependency.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    with sqlite3.connect(candidate) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["otel"] = {"missing_parent_count": 2, "missing_link_count": 1}
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))
    contract.write_text(
        """
name: dependencies
assertions:
  - type: forbid_new_dependency
    from: api
    to: database
""".strip(),
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == "unverifiable"
    assert report.results[0].observed == (
        "causal evidence incomplete (candidate: 3 unresolved references)"
    )


def test_proofline_keeps_attribute_dependent_claims_unverifiable_after_otlp_loss(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "attributes.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    with sqlite3.connect(candidate) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["otel"] = {"dropped_attribute_count": 2}
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))
    contract.write_text(
        """
name: attributes
assertions:
  - type: forbid_new_dependency
    from: app
    to: database
  - type: max_operation_error_count
    operation: db.write
    relative_to: baseline
    factor: 1
""".strip(),
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert [result.status for result in report.results] == ["unverifiable", "unverifiable"]
    assert all(
        result.observed == "semantic evidence incomplete (candidate: 2 dropped attributes)"
        for result in report.results
    )
