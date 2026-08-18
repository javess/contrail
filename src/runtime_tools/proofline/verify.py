"""Mechanically evaluate Proofline assertions from structured evidence."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from runtime_tools.model import JsonValue
from runtime_tools.proofline.contracts import Assertion, Contract, ContractError, load_contracts
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.rundiff.compare import ExecutionDiff, ValueChange
from runtime_tools.storage import RunpackReader

type ClaimStatus = Literal["pass", "fail", "unverifiable"]


@dataclass(frozen=True, slots=True)
class ClaimResult:
    contract: str
    name: str
    type: str
    status: ClaimStatus
    expected: str
    observed: str

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "contract": self.contract,
            "name": self.name,
            "type": self.type,
            "status": self.status,
            "expected": self.expected,
            "observed": self.observed,
        }


@dataclass(frozen=True, slots=True)
class VerificationReport:
    baseline_id: str
    candidate_id: str
    results: tuple[ClaimResult, ...]

    @property
    def passed(self) -> bool:
        return all(result.status == "pass" for result in self.results)

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "baseline_id": self.baseline_id,
            "candidate_id": self.candidate_id,
            "passed": self.passed,
            "claim_count": len(self.results),
            "results": [result.as_json_value() for result in self.results],
        }


def _number(config: dict[str, JsonValue], key: str, assertion: Assertion) -> float:
    value = config.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ContractError(f"{assertion.type} requires numeric {key}")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ContractError(f"{assertion.type} {key} exceeds the numeric range") from exc
    if not math.isfinite(number):
        raise ContractError(f"{assertion.type} {key} must be finite")
    return number


def _string(config: dict[str, JsonValue], key: str, assertion: Assertion) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value:
        raise ContractError(f"{assertion.type} requires string {key}")
    return value


def _result(
    contract: Contract,
    assertion: Assertion,
    status: ClaimStatus,
    expected: str,
    observed: str,
) -> ClaimResult:
    return ClaimResult(contract.name, assertion.name, assertion.type, status, expected, observed)


def _finite_limit(value: float, assertion: Assertion) -> float:
    if not math.isfinite(value):
        raise ContractError(f"{assertion.type} threshold exceeds the numeric range")
    return value


def _max_regression(
    contract: Contract,
    assertion: Assertion,
    change: ValueChange,
    label: str,
) -> ClaimResult:
    percent = _number(assertion.config, "percent", assertion)
    if percent < 0:
        raise ContractError(f"{assertion.type} percent cannot be negative")
    expected = f"candidate {label} <= baseline + {percent:g}%"
    if change.baseline is None or change.candidate is None:
        return _result(contract, assertion, "unverifiable", expected, f"{label} unavailable")
    limit = _finite_limit(change.baseline * (1 + percent / 100), assertion)
    status: ClaimStatus = "pass" if change.candidate <= limit else "fail"
    return _result(
        contract,
        assertion,
        status,
        expected,
        f"baseline={change.baseline:g}, candidate={change.candidate:g}, limit={limit:g}",
    )


def _operation_totals(path: Path, operation: str, *, errors_only: bool = False) -> int:
    with RunpackReader(path) as reader:
        counts = reader.operation_error_counts() if errors_only else reader.operation_counts()
        return sum(
            count
            for (_, _, _, operation_name), count in counts.items()
            if operation_name == operation
        )


def _incomplete_output_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    if "stdout" in diff.baseline_incomplete_streams:
        incomplete.append("baseline: stdout")
    if "stdout" in diff.candidate_incomplete_streams:
        incomplete.append("candidate: stdout")
    if not incomplete:
        return None
    return f"output identity incomplete ({'; '.join(incomplete)})"


def _incomplete_causal_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    for side, count in (
        ("baseline", diff.baseline_missing_causal_references),
        ("candidate", diff.candidate_missing_causal_references),
    ):
        if count is None:
            incomplete.append(f"{side}: completeness unknown")
        elif count:
            incomplete.append(f"{side}: {count} unresolved references")
    if not incomplete:
        return None
    return f"causal evidence incomplete ({'; '.join(incomplete)})"


def _incomplete_semantic_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    for side, count in (
        ("baseline", diff.baseline_dropped_attribute_count),
        ("candidate", diff.candidate_dropped_attribute_count),
    ):
        if count is None:
            incomplete.append(f"{side}: completeness unknown")
        elif count:
            incomplete.append(f"{side}: {count} dropped attributes")
    if not incomplete:
        return None
    return f"semantic evidence incomplete ({'; '.join(incomplete)})"


def _evaluate(
    contract: Contract,
    assertion: Assertion,
    diff: ExecutionDiff,
    baseline: Path,
    candidate: Path,
) -> ClaimResult:
    if assertion.type in {"output_equivalent", "result_equivalence"}:
        if diff.output_equivalent is None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                "equivalent output",
                _incomplete_output_observation(diff) or "output identity unavailable",
            )
        return _result(
            contract,
            assertion,
            "pass" if diff.output_equivalent else "fail",
            "equivalent output",
            "equivalent" if diff.output_equivalent else "different",
        )
    if assertion.type == "max_runtime_regression":
        return _max_regression(contract, assertion, diff.wall_time, "runtime")
    if assertion.type == "max_cpu_time_regression":
        return _max_regression(contract, assertion, diff.cpu_time, "CPU time")
    if assertion.type == "max_peak_memory_regression":
        return _max_regression(contract, assertion, diff.peak_memory, "peak memory")
    if assertion.type == "forbid_new_dependency":
        source = _string(assertion.config, "from", assertion)
        target = _string(assertion.config, "to", assertion)
        if diff.baseline_annotation_error or diff.candidate_annotation_error:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"no new dependency {source} -> {target}",
                "annotation evidence incomplete",
            )
        semantic_observation = _incomplete_semantic_observation(diff)
        if semantic_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"no new dependency {source} -> {target}",
                semantic_observation,
            )
        causal_observation = _incomplete_causal_observation(diff)
        if causal_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"no new dependency {source} -> {target}",
                causal_observation,
            )
        violation = next(
            (
                edge
                for edge in diff.edge_count_changes
                if edge.source_name == source
                and edge.target_name == target
                and edge.baseline == 0
                and edge.candidate > 0
            ),
            None,
        )
        return _result(
            contract,
            assertion,
            "fail" if violation else "pass",
            f"no new dependency {source} -> {target}",
            f"candidate count={violation.candidate}" if violation else "not observed",
        )
    if assertion.type in {"max_operation_count", "max_operation_error_count"}:
        operation = _string(assertion.config, "operation", assertion)
        errors_only = assertion.type == "max_operation_error_count"
        count_label = f"{operation} error" if errors_only else operation
        relative_to = _string(assertion.config, "relative_to", assertion)
        if relative_to != "baseline":
            raise ContractError(f"{assertion.type} relative_to must be baseline")
        factor = _number(assertion.config, "factor", assertion)
        if factor < 0:
            raise ContractError(f"{assertion.type} factor cannot be negative")
        if diff.baseline_annotation_error or diff.candidate_annotation_error:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"candidate {count_label} count <= baseline x {factor:g}",
                "annotation evidence incomplete",
            )
        if errors_only:
            semantic_observation = _incomplete_semantic_observation(diff)
            if semantic_observation is not None:
                return _result(
                    contract,
                    assertion,
                    "unverifiable",
                    f"candidate {count_label} count <= baseline x {factor:g}",
                    semantic_observation,
                )
        baseline_operations = _operation_totals(baseline, operation)
        candidate_operations = _operation_totals(candidate, operation)
        if baseline_operations == 0 and candidate_operations == 0:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"candidate {count_label} count <= baseline x {factor:g}",
                f"operation {operation} not observed in either run",
            )
        before = (
            _operation_totals(baseline, operation, errors_only=True)
            if errors_only
            else baseline_operations
        )
        after = (
            _operation_totals(candidate, operation, errors_only=True)
            if errors_only
            else candidate_operations
        )
        try:
            limit = _finite_limit(before * factor, assertion)
        except OverflowError as exc:
            raise ContractError(f"{assertion.type} threshold exceeds the numeric range") from exc
        return _result(
            contract,
            assertion,
            "pass" if after <= limit else "fail",
            f"candidate {count_label} count <= baseline x {factor:g}",
            f"baseline={before}, candidate={after}, limit={limit:g}",
        )
    raise ContractError(f"unsupported assertion type: {assertion.type}")


def verify_contracts(
    contract_path: Path,
    baseline: Path,
    candidate: Path,
) -> VerificationReport:
    contracts = load_contracts(contract_path)
    diff = compare_runpacks(baseline, candidate)
    results = tuple(
        _evaluate(contract, assertion, diff, baseline, candidate)
        for contract in contracts
        for assertion in contract.assertions
    )
    return VerificationReport(diff.baseline_id, diff.candidate_id, results)
