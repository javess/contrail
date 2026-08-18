"""Generate a self-contained, installed-package Proofline demonstration."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml

from runtime_tools.artifacts import artifact_exists, publish_without_overwrite, remove_best_effort
from runtime_tools.capture import CaptureError, record_process
from runtime_tools.model import JsonValue
from runtime_tools.proofline.contracts import ContractError
from runtime_tools.proofline.verify import (
    VerificationArtifactBindings,
    VerificationReport,
    verify_contracts_with_artifact_bindings,
)
from runtime_tools.storage import RunpackError
from runtime_tools.terminal import terminal_text
from runtime_tools.ui import TimelineError, build_timeline_payload

type DemoStatus = Literal["ready"]

_BASELINE_WRITES = 3
_CANDIDATE_WRITES = 30
_EXPECTED_VIOLATIONS = frozenset({"forbid_new_dependency", "max_operation_count"})
_WORKLOAD_SOURCE = '''\
"""Adaptable workload used by ``contrail demo``."""

from __future__ import annotations

import argparse

from runtime_tools import runtime


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("variant", choices=("baseline", "candidate"))
    args = parser.parse_args()

    write_count = 3 if args.variant == "baseline" else 30
    # A named run groups related operations into one traceable unit of work.
    with runtime.run("local-pipeline"):
        for _ in range(write_count):
            # Stable operation names make cardinality contracts reusable.
            runtime.event("db.write", kind="client.request")
        if args.variant == "candidate":
            # peer.service turns a client operation into dependency evidence.
            runtime.event(
                "metadata.lookup",
                kind="client.request",
                **{"peer.service": "metadata-db"},
            )

    # The result stays identical; the regression is the extra runtime work.
    print("pipeline result: 42")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''


class DemoError(ValueError):
    """Raised when the local demonstration cannot be generated or proven."""


@dataclass(frozen=True, slots=True)
class DemoResult:
    output_dir: Path
    workload: Path
    baseline_runpack: Path
    candidate_runpack: Path
    contract: Path
    proofline_report: Path
    baseline_exit_code: int
    candidate_exit_code: int
    verification: VerificationReport
    status: DemoStatus = "ready"
    violation_count: int = len(_EXPECTED_VIOLATIONS)


def _contract_value(process_name: str) -> dict[str, JsonValue]:
    return {
        "name": "installed-local-regression",
        "description": (
            "Preserve the result without amplifying database writes or adding metadata access."
        ),
        "assertions": [
            {"type": "candidate_exit_success", "name": "candidate-succeeds"},
            {"type": "exit_code_equivalent", "name": "exit-status-preserved"},
            {"type": "output_equivalent", "name": "output-preserved"},
            {
                "type": "forbid_new_dependency",
                "name": "no-metadata-access",
                "from": process_name,
                "to": "metadata-db",
            },
            {
                "type": "max_operation_count",
                "name": "bounded-database-writes",
                "operation": "db.write",
                "relative_to": "baseline",
                "factor": 1,
            },
        ],
    }


def _write_contract(path: Path, process_name: str) -> None:
    try:
        rendered = yaml.safe_dump(
            _contract_value(process_name),
            allow_unicode=False,
            sort_keys=False,
        )
        path.write_text(rendered, encoding="utf-8")
        os.chmod(path, 0o600)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise DemoError(f"could not write demo contract: {terminal_text(exc)}") from exc


def _write_workload(path: Path) -> None:
    try:
        path.write_text(_WORKLOAD_SOURCE, encoding="utf-8")
        os.chmod(path, 0o600)
    except (OSError, UnicodeError) as exc:
        raise DemoError(f"could not write demo workload: {terminal_text(exc)}") from exc


def _write_report(
    path: Path,
    report: VerificationReport,
    diff: JsonValue,
    artifact_bindings: VerificationArtifactBindings,
) -> None:
    document = report.as_json_value(
        include_evidence=True,
        artifact_bindings=artifact_bindings,
    )
    document["diff"] = diff
    try:
        path.write_text(
            json.dumps(document, allow_nan=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.chmod(path, 0o600)
    except (OSError, TypeError, UnicodeError, ValueError) as exc:
        raise DemoError(f"could not write demo Proofline report: {terminal_text(exc)}") from exc


def _object(value: JsonValue | None, label: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise DemoError(f"generated demo {label} is not an object")
    return value


def _list(value: JsonValue | None, label: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise DemoError(f"generated demo {label} is not a list")
    return value


def _prove_interactive_evidence(payload: dict[str, JsonValue]) -> None:
    proofline = _object(payload.get("proofline"), "Proofline evidence")
    if proofline.get("source") != "report":
        raise DemoError("generated demo did not validate its retained Proofline report")
    findings = _list(proofline.get("findings"), "Proofline findings")
    if any(
        _object(finding, "Proofline finding").get("report_assurance")
        != "artifact_bound_policy_replayed"
        for finding in findings
    ):
        raise DemoError("generated demo report is not bound to the retained runpacks")
    selections = _object(proofline.get("selections"), "Proofline selections")
    by_type: dict[str, dict[str, JsonValue]] = {}
    for raw_finding in findings:
        finding = _object(raw_finding, "Proofline finding")
        assertion_type = finding.get("type")
        if isinstance(assertion_type, str):
            by_type[assertion_type] = finding

    expected = {
        "forbid_new_dependency": {
            "fact": {"baseline": 0, "candidate": 1},
            "relationship": "dependency",
            "matched_event_count": 1,
        },
        "max_operation_count": {
            "fact": {
                "baseline": _BASELINE_WRITES,
                "candidate": _CANDIDATE_WRITES,
                "limit": float(_BASELINE_WRITES),
            },
            "relationship": "operation",
            "matched_event_count": _CANDIDATE_WRITES,
        },
    }
    for assertion_type, proof in expected.items():
        expected_finding = by_type.get(assertion_type)
        if expected_finding is None or expected_finding.get("status") != "fail":
            raise DemoError(f"generated demo did not produce the expected {assertion_type} failure")
        evidence = _list(expected_finding.get("evidence"), f"{assertion_type} evidence")
        if (
            not evidence
            or _object(evidence[0], f"{assertion_type} evidence").get("fact") != proof["fact"]
        ):
            raise DemoError(f"generated demo {assertion_type} facts are not deterministic")
        selection_id = expected_finding.get("selection_id")
        selection = selections.get(selection_id) if isinstance(selection_id, str) else None
        selection_value = _object(selection, f"{assertion_type} selection")
        if (
            selection_value.get("relationship") != proof["relationship"]
            or selection_value.get("matched_event_count") != proof["matched_event_count"]
            or selection_value.get("truncated") is not False
        ):
            raise DemoError(f"generated demo {assertion_type} event mapping is incomplete")
        selected_ids = _list(
            selection_value.get("candidate_event_ids"), f"{assertion_type} event ids"
        )
        if len(selected_ids) != proof["matched_event_count"]:
            raise DemoError(f"generated demo {assertion_type} event mapping is incomplete")


def _validate_verification(report: VerificationReport) -> None:
    statuses = {result.type: result.status for result in report.results}
    if set(statuses) != {
        "candidate_exit_success",
        "exit_code_equivalent",
        "output_equivalent",
        *_EXPECTED_VIOLATIONS,
    }:
        raise DemoError("generated demo contract results are incomplete")
    failed = frozenset(
        assertion_type for assertion_type, status in statuses.items() if status == "fail"
    )
    if failed != _EXPECTED_VIOLATIONS or any(
        status != "pass"
        for assertion_type, status in statuses.items()
        if assertion_type not in _EXPECTED_VIOLATIONS
    ):
        raise DemoError("generated demo did not produce the expected deterministic result")


def run_demo(output_dir: Path) -> DemoResult:
    """Create and validate a local baseline/candidate regression demonstration."""
    if not isinstance(output_dir, Path):
        raise DemoError("demo output directory must be a path")
    if artifact_exists(output_dir):
        raise DemoError(f"refusing to reuse demo output directory: {output_dir}")
    if not output_dir.parent.is_dir():
        raise DemoError(f"demo output parent directory does not exist: {output_dir.parent}")
    try:
        output_dir.mkdir(mode=0o700)
        os.chmod(output_dir, 0o700)
    except FileExistsError as exc:
        raise DemoError(f"refusing to reuse demo output directory: {output_dir}") from exc
    except OSError as exc:
        raise DemoError(f"could not create demo output directory: {terminal_text(exc)}") from exc

    workload = output_dir / "workload.py"
    baseline_runpack = output_dir / "baseline.runpack"
    candidate_runpack = output_dir / "candidate.runpack"
    contract = output_dir / "contract.yaml"
    proofline_report = output_dir / "proofline-report.json"
    temporary_report = output_dir / ".proofline-report.json.tmp"
    try:
        process_name = Path(sys.executable).name
        _write_workload(workload)
        _write_contract(contract, process_name)
        command_prefix = (sys.executable, str(workload))
        baseline_exit_code = record_process(
            (*command_prefix, "baseline"),
            baseline_runpack,
            name="demo-baseline",
            cwd=output_dir,
            capture_output_limit=4_096,
        )
        os.chmod(baseline_runpack, 0o600)
        candidate_exit_code = record_process(
            (*command_prefix, "candidate"),
            candidate_runpack,
            name="demo-candidate",
            cwd=output_dir,
            capture_output_limit=4_096,
        )
        os.chmod(candidate_runpack, 0o600)
        verification, diff, artifact_bindings = verify_contracts_with_artifact_bindings(
            contract, baseline_runpack, candidate_runpack
        )
        _validate_verification(verification)
        try:
            _write_report(
                temporary_report,
                verification,
                diff.as_json_value(),
                artifact_bindings,
            )
            payload = build_timeline_payload(
                baseline_runpack,
                candidate_runpack,
                proofline_report=temporary_report,
            )
            _prove_interactive_evidence(payload)
            publish_without_overwrite(temporary_report, proofline_report)
        finally:
            remove_best_effort(temporary_report)
    except DemoError as exc:
        raise DemoError(
            f"{terminal_text(exc)}; partial demo retained at {terminal_text(output_dir)}"
        ) from exc
    except (CaptureError, ContractError, RunpackError, TimelineError, OSError, ValueError) as exc:
        raise DemoError(
            f"could not generate demo: {terminal_text(exc)}; "
            f"partial demo retained at {terminal_text(output_dir)}"
        ) from exc

    return DemoResult(
        output_dir=output_dir,
        workload=workload,
        baseline_runpack=baseline_runpack,
        candidate_runpack=candidate_runpack,
        contract=contract,
        proofline_report=proofline_report,
        baseline_exit_code=baseline_exit_code,
        candidate_exit_code=candidate_exit_code,
        verification=verification,
    )
