from __future__ import annotations

import hashlib
import json
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import runtime_tools.proofline.contracts as contracts_module
import runtime_tools.storage as storage
from runtime_tools.model import CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.proofline import ContractError, verify_contracts
from runtime_tools.proofline import cli as proofline_cli
from runtime_tools.proofline.experiments import ExperimentResult
from runtime_tools.proofline.report import render_verification
from runtime_tools.proofline.verify import (
    ClaimResult,
    VerificationReport,
    verify_contracts_with_artifact_bindings,
    verify_contracts_with_diff,
)
from runtime_tools.storage import RunpackReader, RunpackWriter

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


def test_proofline_explanation_maps_every_assertion_to_its_diff_fact(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "all-claims.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    contract.write_text(
        """
name: all-claims
assertions:
  - type: candidate_exit_success
  - type: exit_code_equivalent
  - type: output_equivalent
  - type: result_equivalence
  - type: max_runtime_regression
    percent: 0
  - type: max_cpu_time_regression
    percent: 0
  - type: max_peak_memory_regression
    percent: 0
  - type: forbid_new_dependency
    from: app
    to: metadata-db
  - type: max_operation_count
    operation: db.write
    relative_to: baseline
    factor: 1
  - type: max_operation_error_count
    operation: db.write
    relative_to: baseline
    factor: 0
""".strip(),
        encoding="utf-8",
    )

    report, diff = verify_contracts_with_diff(contract, baseline, candidate)
    payload = cast(dict[str, Any], report.as_json_value(include_evidence=True))
    diff_payload = cast(dict[str, Any], diff.as_json_value())

    assert {result["type"]: result["assertion"] for result in payload["results"]} == {
        "candidate_exit_success": {
            "type": "candidate_exit_success",
            "name": "candidate-exit-success",
        },
        "exit_code_equivalent": {
            "type": "exit_code_equivalent",
            "name": "exit-code-equivalent",
        },
        "output_equivalent": {
            "type": "output_equivalent",
            "name": "output-equivalent",
        },
        "result_equivalence": {
            "type": "result_equivalence",
            "name": "result-equivalence",
        },
        "max_runtime_regression": {
            "type": "max_runtime_regression",
            "name": "max-runtime-regression",
            "percent": 0,
        },
        "max_cpu_time_regression": {
            "type": "max_cpu_time_regression",
            "name": "max-cpu-time-regression",
            "percent": 0,
        },
        "max_peak_memory_regression": {
            "type": "max_peak_memory_regression",
            "name": "max-peak-memory-regression",
            "percent": 0,
        },
        "forbid_new_dependency": {
            "type": "forbid_new_dependency",
            "name": "forbid-new-dependency",
            "from": "app",
            "to": "metadata-db",
        },
        "max_operation_count": {
            "type": "max_operation_count",
            "name": "max-operation-count",
            "operation": "db.write",
            "relative_to": "baseline",
            "factor": 1,
        },
        "max_operation_error_count": {
            "type": "max_operation_error_count",
            "name": "max-operation-error-count",
            "operation": "db.write",
            "relative_to": "baseline",
            "factor": 0,
        },
    }
    assert {result["type"]: result["evidence"] for result in payload["results"]} == {
        "candidate_exit_success": [
            {
                "fact": {"candidate_exit_code": 0},
                "diff_path": "/candidate/exit_code",
                "selector": {},
            }
        ],
        "exit_code_equivalent": [
            {
                "fact": {
                    "baseline_exit_code": 0,
                    "candidate_exit_code": 0,
                    "equivalent": True,
                },
                "diff_path": "/exit_code_equivalent",
                "selector": {},
            }
        ],
        "output_equivalent": [
            {
                "fact": {"equivalent": True},
                "diff_path": "/output_equivalent",
                "selector": {},
            }
        ],
        "result_equivalence": [
            {
                "fact": {"equivalent": True},
                "diff_path": "/output_equivalent",
                "selector": {},
            }
        ],
        "max_runtime_regression": [
            {
                "fact": {"baseline": 0.1, "candidate": 0.2, "limit": 0.1},
                "diff_path": "/wall_time",
                "selector": {},
            }
        ],
        "max_cpu_time_regression": [
            {
                "fact": {"baseline": 1.5, "candidate": 3.0, "limit": 1.5},
                "diff_path": "/cpu_time",
                "selector": {},
            }
        ],
        "max_peak_memory_regression": [
            {
                "fact": {"baseline": 100.0, "candidate": 110.0, "limit": 100.0},
                "diff_path": "/peak_memory",
                "selector": {},
            }
        ],
        "forbid_new_dependency": [
            {
                "fact": {"baseline": 0, "candidate": 1},
                "diff_path": "/edge_count_changes",
                "selector": {"source_name": "app", "target_name": "metadata-db"},
            }
        ],
        "max_operation_count": [
            {
                "fact": {"baseline": 1, "candidate": 3, "limit": 1.0},
                "diff_path": "/operation_count_changes",
                "selector": {"operation_name": "db.write"},
            }
        ],
        "max_operation_error_count": [
            {
                "fact": {"baseline": 0, "candidate": 1, "limit": 0.0},
                "diff_path": "/operation_error_count_changes",
                "selector": {"operation_name": "db.write"},
            }
        ],
    }
    for result in payload["results"]:
        reference = result["evidence"][0]
        value = diff_payload
        for token in reference["diff_path"].removeprefix("/").split("/"):
            value = value[token]
        selector = reference["selector"]
        if selector:
            assert isinstance(value, list)
            assert any(
                all(item.get(key) == expected for key, expected in selector.items())
                for item in value
            )


def test_proofline_diff_and_assertion_totals_share_one_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    contract.write_text(
        "name: stable\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: db.write\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )
    real_close = RunpackReader.close
    mutated = False

    def mutate_candidate_after_snapshot(reader: RunpackReader) -> None:
        nonlocal mutated
        real_close(reader)
        if reader.path == candidate.resolve() and not mutated:
            mutated = True
            with RunpackWriter.open_existing(candidate) as writer:
                writer.add_event(
                    Event(
                        "late-write",
                        "client.request",
                        "db.write",
                        "app",
                        20,
                        21,
                        "test",
                        None,
                        99,
                        {},
                    )
                )

    monkeypatch.setattr(RunpackReader, "close", mutate_candidate_after_snapshot)

    report, diff = verify_contracts_with_diff(contract, baseline, candidate)

    assert report.passed is True
    assert report.results[0].observed == "baseline=1, candidate=1, limit=1"
    payload = cast(dict[str, Any], report.as_json_value(include_evidence=True))
    assert payload["results"][0]["evidence"][0]["fact"] == {
        "baseline": 1,
        "candidate": 1,
        "limit": 1.0,
    }
    assert diff.operation_count_changes == ()
    with sqlite3.connect(candidate) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 3


def test_proofline_preserves_exact_aggregate_facts_when_diff_rows_split_by_entity(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "aggregate.yaml"
    for path, counts in ((baseline, (5, 0)), (candidate, (3, 4))):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 100, (), str(tmp_path), 0, None, {})
            )
            writer.add_entities(
                (
                    Entity("worker-a", "service", "worker-a", None, {}),
                    Entity("worker-b", "service", "worker-b", None, {}),
                )
            )
            for entity_id, count in zip(("worker-a", "worker-b"), counts, strict=True):
                for index in range(count):
                    writer.add_event(
                        Event(
                            f"{entity_id}-{index}",
                            "client.request",
                            "db.write",
                            entity_id,
                            index,
                            index + 1,
                            "test",
                            None,
                            index,
                            {},
                        )
                    )
    contract.write_text(
        "name: aggregate\nassertions:\n"
        "  - type: max_operation_count\n"
        "    operation: db.write\n"
        "    relative_to: baseline\n"
        "    factor: 1\n",
        encoding="utf-8",
    )

    report, diff = verify_contracts_with_diff(contract, baseline, candidate)
    payload = cast(dict[str, Any], report.as_json_value(include_evidence=True))
    evidence = payload["results"][0]["evidence"][0]

    assert report.results[0].observed == "baseline=5, candidate=7, limit=5"
    assert evidence["fact"] == {"baseline": 5, "candidate": 7, "limit": 5.0}
    assert evidence["selector"] == {"operation_name": "db.write"}
    assert {
        (change.entity_name, change.baseline, change.candidate)
        for change in diff.operation_count_changes
    } == {("worker-a", 5, 3), ("worker-b", 0, 4)}


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


@pytest.mark.parametrize(
    ("baseline_exit_code", "candidate_exit_code", "status", "observed"),
    (
        (0, 0, "pass", "baseline=exit 0, candidate=exit 0"),
        (0, 2, "fail", "baseline=exit 0, candidate=exit 2"),
        (
            0,
            -signal.SIGTERM,
            "fail",
            f"baseline=exit 0, candidate=signal {signal.SIGTERM.value}",
        ),
        (None, 0, "unverifiable", "baseline=unavailable, candidate=exit 0"),
        (0, None, "unverifiable", "baseline=exit 0, candidate=unavailable"),
    ),
)
def test_proofline_compares_exit_statuses(
    tmp_path: Path,
    baseline_exit_code: int | None,
    candidate_exit_code: int | None,
    status: str,
    observed: str,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "exit-status.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    for path, exit_code in (
        (baseline, baseline_exit_code),
        (candidate, candidate_exit_code),
    ):
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE executions SET exit_code = ?", (exit_code,))
    contract.write_text(
        "name: exit-status\nassertions:\n  - type: exit_code_equivalent\n",
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == status
    assert report.results[0].expected == "equivalent exit status"
    assert report.results[0].observed == observed
    assert json.loads(render_verification(report, "json"))["results"][0]["observed"] == observed
    text_report = render_verification(report, "text")
    assert f"{status.upper():<12} exit-code-equivalent" in text_report
    if status != "pass":
        assert observed in text_report


@pytest.mark.parametrize(
    ("candidate_exit_code", "status", "observed"),
    (
        (0, "pass", "candidate=exit 0"),
        (2, "fail", "candidate=exit 2"),
        (-signal.SIGTERM, "fail", f"candidate=signal {signal.SIGTERM.value}"),
        (None, "unverifiable", "candidate=unavailable"),
    ),
)
def test_proofline_requires_a_successful_candidate_when_requested(
    tmp_path: Path,
    candidate_exit_code: int | None,
    status: str,
    observed: str,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "candidate-health.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    with sqlite3.connect(baseline) as connection:
        connection.execute("UPDATE executions SET exit_code = 7")
    with sqlite3.connect(candidate) as connection:
        connection.execute("UPDATE executions SET exit_code = ?", (candidate_exit_code,))
    contract.write_text(
        "name: candidate-health\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )

    result = verify_contracts(contract, baseline, candidate).results[0]

    assert result.status == status
    assert result.expected == "candidate exits successfully"
    assert result.observed == observed


def test_proofline_text_report_bounds_claim_details() -> None:
    results = tuple(
        ClaimResult("contract", f"claim-{index}", "output_equivalent", "fail", "same", "different")
        for index in range(101)
    )
    report = VerificationReport("baseline", "candidate", results)

    text_report = render_verification(report, "text")

    assert "claim-99" in text_report
    assert "claim-100" not in text_report
    assert "1 additional claims omitted from text output" in text_report
    assert len(json.loads(render_verification(report, "json"))["results"]) == 101


def test_proofline_text_report_prioritizes_late_failures() -> None:
    passing = tuple(
        ClaimResult("contract", f"pass-{index}", "output_equivalent", "pass", "same", "same")
        for index in range(100)
    )
    failure = ClaimResult(
        "contract",
        "late-failure",
        "output_equivalent",
        "fail",
        "same",
        "different",
    )

    text_report = render_verification(
        VerificationReport("baseline", "candidate", (*passing, failure)),
        "text",
    )

    assert "late-failure" in text_report
    assert "Failure\n  Contract: contract" in text_report
    assert "pass-99" not in text_report


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
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert set(payload) == {
        "baseline_id",
        "candidate_id",
        "claim_count",
        "document_type",
        "format_version",
        "passed",
        "results",
    }
    assert payload["document_type"] == "proofline.verification"
    assert payload["format_version"] == "1"
    assert payload["claim_count"] == 5
    assert payload["passed"] is False
    assert payload["results"][3]["observed"] == "candidate count=1"
    assert all("evidence" not in claim for claim in payload["results"])


def test_proofline_verify_explain_connects_a_failed_claim_to_deeper_tools(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "trace \x1b artifacts"
    artifacts.mkdir()
    baseline = artifacts / "baseline run.runpack"
    candidate = artifacts / "candidate run.runpack"
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
            "--explain",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert result.stderr == ""
    assert result.stdout.startswith("PROOFLINE\n\n5 claims evaluated")
    assert "\n\nRUNTIME DIFF\n" in result.stdout
    assert "\x1b" not in result.stdout
    commands = tuple(
        line for line in result.stdout.splitlines() if line.startswith(("batchscope ", "runtime "))
    )
    assert tuple(tuple(command.split(" ", 2)[:2]) for command in commands) == (
        ("batchscope", "inspect"),
        ("runtime", "inspect"),
        ("runtime", "serve"),
    )

    shell = shutil.which("bash")
    assert shell is not None
    inspect_arguments = commands[1].removeprefix("runtime ")
    inspected = subprocess.run(
        (shell, "-c", f"{shlex.quote(sys.executable)} -m runtime_tools.cli {inspect_arguments}"),
        check=False,
        capture_output=True,
        text=True,
    )
    assert inspected.returncode == 0, inspected.stderr
    assert "db.write" in inspected.stdout


def test_proofline_verify_explain_adds_the_runtime_diff_to_json(tmp_path: Path) -> None:
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
            "--explain",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["document_type"] == "proofline.verification"
    assert payload["passed"] is False
    assert payload["diff"]["document_type"] == "rundiff.compare"
    assert payload["diff"]["candidate"]["name"] == "candidate"
    assert payload["diff"]["operation_count_changes"][0]["operation_name"] == "db.write"
    assert payload["artifact_bindings"] == {
        "baseline": {
            "size_bytes": baseline.stat().st_size,
            "sha256": hashlib.sha256(baseline.read_bytes()).hexdigest(),
        },
        "candidate": {
            "size_bytes": candidate.stat().st_size,
            "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
        },
    }
    assert [result["assertion"] for result in payload["results"]] == [
        {"type": "output_equivalent", "name": "output-equivalent"},
        {
            "type": "max_runtime_regression",
            "name": "max-runtime-regression",
            "percent": 50,
        },
        {
            "type": "max_peak_memory_regression",
            "name": "max-peak-memory-regression",
            "percent": 20,
        },
        {
            "type": "forbid_new_dependency",
            "name": "forbid-new-dependency",
            "from": "app",
            "to": "metadata-db",
        },
        {
            "type": "max_operation_count",
            "name": "max-operation-count",
            "operation": "db.write",
            "relative_to": "baseline",
            "factor": 1.2,
        },
    ]
    assert [result["evidence"] for result in payload["results"]] == [
        [{"fact": {"equivalent": True}, "diff_path": "/output_equivalent", "selector": {}}],
        [
            {
                "fact": {
                    "baseline": 0.1,
                    "candidate": 0.2,
                    "limit": 0.15000000000000002,
                },
                "diff_path": "/wall_time",
                "selector": {},
            }
        ],
        [
            {
                "fact": {"baseline": 100.0, "candidate": 110.0, "limit": 120.0},
                "diff_path": "/peak_memory",
                "selector": {},
            }
        ],
        [
            {
                "fact": {"baseline": 0, "candidate": 1},
                "diff_path": "/edge_count_changes",
                "selector": {"source_name": "app", "target_name": "metadata-db"},
            }
        ],
        [
            {
                "fact": {"baseline": 1, "candidate": 3, "limit": 1.2},
                "diff_path": "/operation_count_changes",
                "selector": {"operation_name": "db.write"},
            }
        ],
    ]

    for result in payload["results"]:
        reference = result["evidence"][0]
        value = payload["diff"]
        for token in reference["diff_path"].removeprefix("/").split("/"):
            value = value[token]
        selector = reference["selector"]
        if selector:
            assert isinstance(value, list)
            assert any(
                all(item.get(key) == expected for key, expected in selector.items())
                for item in value
            )


def test_proofline_verify_report_retains_the_exact_explained_json(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    retained = tmp_path / "retained report.json"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)

    expected = subprocess.run(
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
            "--explain",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
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
            "--report",
            str(retained),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert expected.returncode == result.returncode == 1
    assert expected.stderr == result.stderr == ""
    assert retained.read_text(encoding="utf-8") == expected.stdout
    assert json.loads(retained.read_text(encoding="utf-8"))["artifact_bindings"]
    assert retained.stat().st_mode & 0o777 == 0o600
    assert result.stdout.startswith("PROOFLINE\n\n5 claims evaluated")
    serve_command = next(
        line for line in result.stdout.splitlines() if line.startswith("runtime serve ")
    )
    assert f"--proofline-report {shlex.quote(str(retained))}" in serve_command
    assert "--contract" not in serve_command


def test_proofline_verify_json_stdout_matches_the_retained_report_bytes(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    retained = tmp_path / "retained.json"
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
            "--report",
            str(retained),
        ),
        check=False,
        capture_output=True,
    )

    assert result.returncode == 1
    assert result.stderr == b""
    assert retained.read_bytes() == result.stdout


def test_proofline_invalid_runpack_preflight_publishes_no_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    retained = tmp_path / "retained.json"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)
    with sqlite3.connect(candidate) as connection:
        connection.execute(
            "UPDATE executions SET metadata_json = ?",
            (json.dumps({"oversized": "x" * 1024}),),
        )
    monkeypatch.setattr(storage, "MAX_RUNPACK_JSON_BYTES", 512)

    status = proofline_cli.main(
        [
            "verify",
            str(contract),
            "--baseline",
            str(baseline),
            "--candidate",
            str(candidate),
            "--report",
            str(retained),
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert "runpack JSON exceeds" in captured.err
    assert not retained.exists()
    assert not tuple(tmp_path.glob(".contrail-report.tmp-*"))


def test_proofline_report_publication_failure_is_normalized_and_cleaned_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    retained = tmp_path / "retained.json"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)

    def fail_publication(*args: object, **kwargs: object) -> None:
        raise OSError("simulated publication failure")

    monkeypatch.setattr("runtime_tools.artifacts.os.link", fail_publication)

    status = proofline_cli.main(
        [
            "verify",
            str(contract),
            "--baseline",
            str(baseline),
            "--candidate",
            str(candidate),
            "--report",
            str(retained),
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert captured.err.startswith("proofline: could not publish report")
    assert not retained.exists()
    assert not tuple(tmp_path.glob(".contrail-report.tmp-*"))


def test_proofline_report_never_overwrites_a_destination_created_during_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    retained = tmp_path / "retained.json"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)
    verification = verify_contracts_with_artifact_bindings(contract, baseline, candidate)

    def collide(*args: object, **kwargs: object) -> object:
        retained.write_bytes(b"concurrent report")
        return verification

    monkeypatch.setattr(proofline_cli, "verify_contracts_with_artifact_bindings", collide)

    status = proofline_cli.main(
        [
            "verify",
            str(contract),
            "--baseline",
            str(baseline),
            "--candidate",
            str(candidate),
            "--report",
            str(retained),
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert captured.err.startswith("proofline: refusing to overwrite existing report:")
    assert retained.read_bytes() == b"concurrent report"
    assert not tuple(tmp_path.glob(".contrail-report.tmp-*"))


def test_proofline_verify_default_text_remains_the_contract_report(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)
    expected = render_verification(verify_contracts(contract, baseline, candidate), "text")

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
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert result.stdout == f"{expected}\n"
    assert result.stderr == ""


def test_proofline_verify_explain_text_links_failures_to_diff_facts(tmp_path: Path) -> None:
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
            "--explain",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert (
        '  Evidence: baseline=0, candidate=1; diff/edge_count_changes where source_name="app", '
        'target_name="metadata-db"' in result.stdout
    )
    assert (
        "  Evidence: baseline=1, candidate=3, limit=1.2; "
        'diff/operation_count_changes where operation_name="db.write"' in result.stdout
    )


def test_proofline_run_explain_uses_the_captured_artifact_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifacts = tmp_path / "captured artifacts"
    artifacts.mkdir()
    baseline = artifacts / "baseline run.runpack"
    candidate = artifacts / "candidate run.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)
    report, diff = verify_contracts_with_diff(contract, baseline, candidate)
    experiment = SimpleNamespace(
        baseline_runpack=baseline,
        candidate_runpack=candidate,
        baseline_exit_code=0,
        candidate_exit_code=0,
        verification=report,
        diff=diff,
    )
    monkeypatch.setattr(proofline_cli, "run_experiment", lambda *args, **kwargs: experiment)

    status = proofline_cli.main(
        [
            "run",
            str(contract),
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "candidate",
            "--workload",
            "workload.py",
            "--explain",
        ]
    )

    captured = capsys.readouterr()
    assert status == 1
    assert captured.err == ""
    assert "\n\nRUNTIME DIFF\n" in captured.out
    assert f"batchscope inspect {shlex.quote(str(candidate))}" in captured.out
    assert f"runtime inspect {shlex.quote(str(candidate))} --tree" in captured.out
    assert (
        f"runtime serve {shlex.quote(str(baseline))} --compare {shlex.quote(str(candidate))} "
        f"--contract {shlex.quote(str(contract))}" in captured.out
    )
    assert f"baseline artifact:  {baseline}" in captured.out
    assert f"candidate artifact: {candidate}" in captured.out


def test_proofline_run_json_explanation_keeps_claim_links_with_the_top_level_diff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)
    report, diff, bindings = verify_contracts_with_artifact_bindings(
        contract,
        baseline,
        candidate,
    )
    experiment = ExperimentResult(baseline, candidate, 0, 0, report, diff, bindings)
    calls: list[dict[str, object]] = []

    def run(*args: object, **kwargs: object) -> ExperimentResult:
        calls.append(kwargs)
        return experiment

    monkeypatch.setattr(proofline_cli, "run_experiment", run)

    status = proofline_cli.main(
        [
            "run",
            str(contract),
            "--baseline-ref",
            "baseline",
            "--candidate-ref",
            "candidate",
            "--workload",
            "workload.py",
            "--format",
            "json",
            "--explain",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert status == 1
    assert captured.err == ""
    assert calls[0]["bind_artifacts"] is True
    assert payload["diff"]["document_type"] == "rundiff.compare"
    assert payload["artifact_bindings"] == bindings.as_json_value()
    assert payload["verification"]["results"][-1]["evidence"] == [
        {
            "fact": {"baseline": 1, "candidate": 3, "limit": 1.2},
            "diff_path": "/operation_count_changes",
            "selector": {"operation_name": "db.write"},
        }
    ]


def test_proofline_run_report_implies_an_artifact_bound_explanation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    retained = tmp_path / "run report.json"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)
    report, diff, bindings = verify_contracts_with_artifact_bindings(
        contract,
        baseline,
        candidate,
    )
    experiment = ExperimentResult(baseline, candidate, 0, 0, report, diff, bindings)
    calls: list[dict[str, object]] = []

    def run(*args: object, **kwargs: object) -> ExperimentResult:
        calls.append(kwargs)
        return experiment

    monkeypatch.setattr(proofline_cli, "run_experiment", run)

    status = proofline_cli.main(
        [
            "run",
            str(contract),
            "--baseline-ref",
            "baseline",
            "--candidate-ref",
            "candidate",
            "--workload",
            "workload.py",
            "--report",
            str(retained),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(retained.read_text(encoding="utf-8"))
    assert status == 1
    assert captured.err == ""
    assert calls[0]["bind_artifacts"] is True
    assert payload["document_type"] == "proofline.experiment"
    assert payload["artifact_bindings"] == bindings.as_json_value()
    assert payload["diff"]["document_type"] == "rundiff.compare"
    assert payload["verification"]["results"][-1]["evidence"]
    serve_command = next(
        line for line in captured.out.splitlines() if line.startswith("runtime serve ")
    )
    assert f"--proofline-report {shlex.quote(str(retained))}" in serve_command


@pytest.mark.parametrize("destination", ("existing", "dangling", "missing-parent"))
def test_proofline_run_rejects_an_invalid_report_destination_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    destination: str,
) -> None:
    retained = tmp_path / "report.json"
    if destination == "existing":
        retained.write_bytes(b"existing report")
    elif destination == "dangling":
        retained.symlink_to(tmp_path / "missing-target")
    else:
        retained = tmp_path / "missing-parent" / "report.json"
    calls = 0

    def run(*args: object, **kwargs: object) -> ExperimentResult:
        nonlocal calls
        calls += 1
        raise AssertionError("workload must not execute")

    monkeypatch.setattr(proofline_cli, "run_experiment", run)

    status = proofline_cli.main(
        [
            "run",
            str(tmp_path / "missing-contract.yaml"),
            "--baseline-ref",
            "invalid",
            "--candidate-ref",
            "invalid",
            "--workload",
            "missing.py",
            "--report",
            str(retained),
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert calls == 0
    assert captured.out == ""
    assert captured.err.startswith("proofline: ")
    if destination == "existing":
        assert retained.read_bytes() == b"existing report"
    elif destination == "dangling":
        assert retained.is_symlink()
        assert not retained.exists()
    else:
        assert not retained.parent.exists()


def test_proofline_invalid_contract_leaves_no_report_or_temporary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    contract = tmp_path / "invalid.yaml"
    report = tmp_path / "report.json"
    contract.write_text(
        "name: invalid\nassertions:\n  - type: unsupported\n",
        encoding="utf-8",
    )

    status = proofline_cli.main(
        [
            "verify",
            str(contract),
            "--baseline",
            str(tmp_path / "missing-baseline.runpack"),
            "--candidate",
            str(tmp_path / "missing-candidate.runpack"),
            "--report",
            str(report),
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert captured.err == "proofline: unsupported assertion type: unsupported\n"
    assert not report.exists()
    assert not tuple(tmp_path.glob(".contrail-report.tmp-*"))


def test_proofline_run_default_text_remains_the_report_and_artifact_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    report = VerificationReport(
        "baseline",
        "candidate",
        (
            ClaimResult(
                "contract",
                "candidate-exit-success",
                "candidate_exit_success",
                "pass",
                "candidate exits successfully",
                "candidate=exit 0",
            ),
        ),
    )
    experiment = SimpleNamespace(
        baseline_runpack=baseline,
        candidate_runpack=candidate,
        verification=report,
    )
    monkeypatch.setattr(proofline_cli, "run_experiment", lambda *args, **kwargs: experiment)

    status = proofline_cli.main(
        [
            "run",
            str(tmp_path / "contract.yaml"),
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "candidate",
            "--workload",
            "workload.py",
        ]
    )

    captured = capsys.readouterr()
    assert status == 0
    assert captured.out == (
        f"{render_verification(report, 'text')}\n\n"
        f"baseline artifact:  {baseline}\n"
        f"candidate artifact: {candidate}\n"
    )
    assert captured.err == ""


@pytest.mark.parametrize("subcommand", ("verify", "run"))
def test_proofline_help_advertises_explanations(subcommand: str) -> None:
    result = subprocess.run(
        (sys.executable, "-m", "runtime_tools.proofline.cli", subcommand, "--help"),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "--explain" in result.stdout
    assert result.stderr == ""


@pytest.mark.parametrize("subcommand", ("run", "search"))
def test_proofline_help_advertises_workload_python_selection(subcommand: str) -> None:
    result = subprocess.run(
        (sys.executable, "-m", "runtime_tools.proofline.cli", subcommand, "--help"),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "--python" in result.stdout
    assert result.stderr == ""


def test_proofline_cli_returns_success_with_machine_readable_evidence(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=False)
    contract.write_text(
        "name: candidate-health\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )

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

    assert result.returncode == 0
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["document_type"] == "proofline.verification"
    assert payload["format_version"] == "1"
    assert payload["passed"] is True
    assert payload["results"][0]["type"] == "candidate_exit_success"
    assert "assertion" not in payload["results"][0]
    assert "evidence" not in payload["results"][0]


def test_proofline_cli_normalizes_overlong_runpack_paths(tmp_path: Path) -> None:
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)
    verified = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.proofline.cli",
            "verify",
            str(contract),
            "--baseline",
            "a" * 5000,
            "--candidate",
            "candidate.runpack",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert verified.returncode == 2
    assert verified.stdout == ""
    assert verified.stderr.startswith("proofline: could not resolve runpack path: ")
    assert "Traceback" not in verified.stderr


def test_proofline_validate_parses_inputs_without_execution(tmp_path: Path) -> None:
    contract = tmp_path / "contract.yaml"
    parameters = tmp_path / "parameters.yaml"
    _write_contract(contract)
    parameters.write_text(
        "parameters:\n  jobs:\n    type: integer\n    min: 1\n    max: 4\n",
        encoding="utf-8",
    )

    validated = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.proofline.cli",
            "validate",
            str(contract),
            "--parameters",
            str(parameters),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert validated.returncode == 0
    assert validated.stderr == ""
    assert json.loads(validated.stdout) == {
        "assertion_count": 5,
        "assertion_types": [
            "forbid_new_dependency",
            "max_operation_count",
            "max_peak_memory_regression",
            "max_runtime_regression",
            "output_equivalent",
        ],
        "contract_count": 1,
        "document_type": "proofline.validation",
        "format_version": "1",
        "parameter_count": 1,
    }


def test_proofline_validate_reports_invalid_inputs_as_a_cli_error(tmp_path: Path) -> None:
    contract = tmp_path / "invalid.yaml"
    contract.write_text(
        "name: invalid\nassertions:\n  - type: unsupported\n",
        encoding="utf-8",
    )

    validated = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.proofline.cli",
            "validate",
            str(contract),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert validated.returncode == 2
    assert validated.stdout == ""
    assert validated.stderr == "proofline: unsupported assertion type: unsupported\n"


def test_proofline_validate_renders_a_text_summary(tmp_path: Path) -> None:
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    validated = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.proofline.cli",
            "validate",
            str(contract),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert validated.returncode == 0
    assert validated.stderr == ""
    assert validated.stdout == (
        "PROOFLINE INPUTS VALID\n\ncontracts:  1\nassertions: 5\nparameters: not provided\n"
    )


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


@pytest.mark.parametrize(
    "document",
    (
        "name: bounded\nassertions:\n"
        "  - type: output_equivalent\n"
        "  - type: unsupported-beyond-limit\n",
        "contracts:\n"
        "  - name: first\n"
        "    assertions:\n"
        "      - type: output_equivalent\n"
        "  - name: second\n"
        "    assertions:\n"
        "      - type: unsupported-beyond-limit\n",
    ),
)
def test_proofline_rejects_too_many_assertions_before_parsing_excess_assertions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    document: str,
) -> None:
    contract = tmp_path / "too-many-assertions.yaml"
    contract.write_text(document, encoding="utf-8")
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


def test_proofline_normalizes_yaml_integer_parser_limits(tmp_path: Path) -> None:
    contract = tmp_path / "oversized-integer.yaml"
    contract.write_text(
        "name: overflow\nassertions:\n  - type: max_runtime_regression\n    percent: "
        + "1" * 5_000
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="contract file contains an invalid scalar"):
        verify_contracts(contract, tmp_path / "missing-a", tmp_path / "missing-b")


@pytest.mark.parametrize(
    ("assertion", "evidence"),
    (
        ("type: max_runtime_regression\n    percent: 1.0e+308", "runtime"),
        (
            (
                "type: max_operation_count\n"
                "    operation: db.write\n"
                "    relative_to: baseline\n"
                "    factor: 1.0e+308"
            ),
            "operation",
        ),
    ),
)
def test_proofline_rejects_thresholds_with_overflowed_limits(
    tmp_path: Path,
    assertion: str,
    evidence: str,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "overflowed-limit.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    with sqlite3.connect(baseline) as connection:
        if evidence == "runtime":
            connection.execute(
                "UPDATE executions SET finished_at_ns = ?",
                ((1 << 63) - 1,),
            )
        else:
            connection.execute(
                """
                INSERT INTO events(
                    id, kind, name, entity_id, started_at_ns, finished_at_ns,
                    clock_domain, uncertainty_ns, sequence, attributes_json
                ) VALUES ('write-extra', 'client.request', 'db.write', 'app',
                          3, 4, 'test', NULL, 2, '{}')
                """
            )
    contract.write_text(
        f"name: overflowed-limit\nassertions:\n  - {assertion}\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="threshold exceeds the numeric range"):
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


def test_proofline_does_not_treat_new_evidence_as_a_new_named_dependency(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "dependency.yaml"
    for path, add_peer_evidence in ((baseline, False), (candidate, True)):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(path.stem, path.stem, 0, 10, (), str(tmp_path), 0, None, {})
            )
            writer.add_entities(
                (
                    Entity("app", "service", "app", None, {}),
                    Entity("database", "service", "database", None, {}),
                )
            )
            writer.add_events(
                (
                    Event("request", "server.request", "work", "app", 0, 10, "test", None, 0, {}),
                    Event(
                        "query",
                        "server.request",
                        "query",
                        "database",
                        1,
                        9,
                        "test",
                        None,
                        1,
                        {},
                    ),
                )
            )
            writer.add_causal_edge(CausalEdge("request", "query", "parent", 1.0, {}))
            if add_peer_evidence:
                writer.add_event(
                    Event(
                        "client-query",
                        "client.request",
                        "query",
                        "app",
                        2,
                        3,
                        "test",
                        None,
                        2,
                        {"peer.service": "database"},
                    )
                )
    contract.write_text(
        """
name: dependencies
assertions:
  - type: forbid_new_dependency
    from: app
    to: database
""".strip(),
        encoding="utf-8",
    )

    report = verify_contracts(contract, baseline, candidate)

    assert report.results[0].status == "pass"
    assert report.results[0].observed == "not observed"


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
