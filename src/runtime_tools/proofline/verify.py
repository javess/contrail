"""Mechanically evaluate Proofline assertions from structured evidence."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from runtime_tools.json_support import JsonValueModel, output_document
from runtime_tools.model import JsonValue
from runtime_tools.proofline.contracts import Assertion, Contract, ContractError, load_contracts
from runtime_tools.rundiff.compare import ExecutionDiff, ValueChange, compare_readers
from runtime_tools.storage import (
    RunpackArtifactIdentity,
    RunpackReader,
    open_runpack_snapshot,
    resolve_runpack_path,
)

type ClaimStatus = Literal["pass", "fail", "unverifiable"]


@dataclass(frozen=True, slots=True)
class DiffEvidenceReference:
    """Exact evaluated facts plus a stable pointer to related RunDiff detail."""

    diff_path: str
    selector: tuple[tuple[str, str], ...] = ()
    fact: tuple[tuple[str, JsonValue], ...] = ()

    def with_fact(self, **fact: JsonValue) -> DiffEvidenceReference:
        return DiffEvidenceReference(self.diff_path, self.selector, tuple(fact.items()))

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "diff_path": self.diff_path,
            "selector": dict(self.selector),
            "fact": dict(self.fact),
        }


@dataclass(frozen=True, slots=True)
class ClaimResult:
    contract: str
    name: str
    type: str
    status: ClaimStatus
    expected: str
    observed: str
    evidence: tuple[DiffEvidenceReference, ...] = ()
    assertion: Assertion | None = None

    def as_json_value(self, *, include_evidence: bool = False) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "contract": self.contract,
            "name": self.name,
            "type": self.type,
            "status": self.status,
            "expected": self.expected,
            "observed": self.observed,
        }
        if include_evidence:
            value["evidence"] = [reference.as_json_value() for reference in self.evidence]
            if self.assertion is not None:
                value["assertion"] = self.assertion.as_json_value()
        return value


@dataclass(frozen=True, slots=True)
class VerificationArtifactBindings(JsonValueModel):
    """Exact baseline and candidate artifacts used for one verification."""

    baseline: RunpackArtifactIdentity
    candidate: RunpackArtifactIdentity


@dataclass(frozen=True, slots=True)
class VerificationReport:
    baseline_id: str
    candidate_id: str
    results: tuple[ClaimResult, ...]

    @property
    def passed(self) -> bool:
        return all(result.status == "pass" for result in self.results)

    def as_json_value(
        self,
        *,
        include_evidence: bool = False,
        artifact_bindings: VerificationArtifactBindings | None = None,
    ) -> dict[str, JsonValue]:
        document = output_document(
            "proofline.verification",
            {
                "baseline_id": self.baseline_id,
                "candidate_id": self.candidate_id,
                "passed": self.passed,
                "claim_count": len(self.results),
                "results": [
                    result.as_json_value(include_evidence=include_evidence)
                    for result in self.results
                ],
            },
        )
        if artifact_bindings is not None:
            document["artifact_bindings"] = artifact_bindings.as_json_value()
        return document


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
    evidence: DiffEvidenceReference,
) -> ClaimResult:
    return ClaimResult(
        contract.name,
        assertion.name,
        assertion.type,
        status,
        expected,
        observed,
        (evidence,),
        assertion,
    )


def _evidence(diff_path: str, **selector: str) -> DiffEvidenceReference:
    return DiffEvidenceReference(diff_path, tuple(sorted(selector.items())))


def _finite_limit(value: float, assertion: Assertion) -> float:
    if not math.isfinite(value):
        raise ContractError(f"{assertion.type} threshold exceeds the numeric range")
    return value


def _exit_status(exit_code: int | None) -> str:
    if exit_code is None:
        return "unavailable"
    if exit_code < 0:
        return f"signal {-exit_code}"
    return f"exit {exit_code}"


def _max_regression(
    contract: Contract,
    assertion: Assertion,
    change: ValueChange,
    label: str,
    evidence: DiffEvidenceReference,
    instrumentation_modes: tuple[str, str] | None = None,
) -> ClaimResult:
    percent = _number(assertion.config, "percent", assertion)
    if percent < 0:
        raise ContractError(f"{assertion.type} percent cannot be negative")
    expected = f"candidate {label} <= baseline + {percent:g}%"
    if instrumentation_modes is not None and instrumentation_modes[0] != instrumentation_modes[1]:
        baseline_mode, candidate_mode = instrumentation_modes
        return _result(
            contract,
            assertion,
            "unverifiable",
            expected,
            "timing instrumentation differs "
            f"(baseline={baseline_mode}, candidate={candidate_mode})",
            evidence.with_fact(
                baseline_instrumentation=baseline_mode,
                candidate_instrumentation=candidate_mode,
                limit=None,
            ),
        )
    if change.baseline is None or change.candidate is None:
        return _result(
            contract,
            assertion,
            "unverifiable",
            expected,
            f"{label} unavailable",
            evidence.with_fact(
                baseline=change.baseline,
                candidate=change.candidate,
                limit=None,
            ),
        )
    limit = _finite_limit(change.baseline * (1 + percent / 100), assertion)
    status: ClaimStatus = "pass" if change.candidate <= limit else "fail"
    return _result(
        contract,
        assertion,
        status,
        expected,
        f"baseline={change.baseline:g}, candidate={change.candidate:g}, limit={limit:g}",
        evidence.with_fact(baseline=change.baseline, candidate=change.candidate, limit=limit),
    )


def _operation_totals(reader: RunpackReader, operation: str, *, errors_only: bool = False) -> int:
    counts = reader.operation_error_counts() if errors_only else reader.operation_counts()
    return sum(
        count for (_, _, _, operation_name), count in counts.items() if operation_name == operation
    )


def _dependency_total(reader: RunpackReader, source: str, target: str) -> int:
    edge_counts = reader.edge_counts()
    peer_counts = reader.peer_service_edge_counts()
    return sum(
        count
        for counts in (edge_counts, peer_counts)
        for (_, source_name, _, target_name, _), count in counts.items()
        if source_name == source and target_name == target
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


def _incomplete_subprocess_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    semantic_statuses = (
        (
            "baseline",
            diff.baseline_semantic_capture_status,
            diff.baseline_dropped_subprocess_count,
        ),
        (
            "candidate",
            diff.candidate_semantic_capture_status,
            diff.candidate_dropped_subprocess_count,
        ),
    )
    if any(status is not None for _, status, _ in semantic_statuses):
        for side, status, dropped_count in semantic_statuses:
            if status is None:
                incomplete.append(f"{side}: subprocess capture unavailable")
            elif status != "complete":
                omitted = (
                    f", {dropped_count} omitted"
                    if dropped_count is not None and dropped_count > 0
                    else ""
                )
                incomplete.append(f"{side}: subprocess capture {status}{omitted}")
    if not incomplete:
        return None
    return f"subprocess evidence incomplete ({'; '.join(incomplete)})"


def _incomplete_http_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    capture_statuses = (
        (
            "baseline",
            diff.baseline_http_capture_status,
            diff.baseline_dropped_http_request_count,
        ),
        (
            "candidate",
            diff.candidate_http_capture_status,
            diff.candidate_dropped_http_request_count,
        ),
    )
    if any(status is not None for _, status, _ in capture_statuses):
        for side, status, dropped_count in capture_statuses:
            if status is None:
                incomplete.append(f"{side}: HTTP capture unavailable")
            elif status != "complete":
                omitted = (
                    f", {dropped_count} omitted"
                    if dropped_count is not None and dropped_count > 0
                    else ""
                )
                incomplete.append(f"{side}: HTTP capture {status}{omitted}")
    if not incomplete:
        return None
    return f"HTTP evidence incomplete ({'; '.join(incomplete)})"


def _incomplete_network_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    capture_statuses = (
        (
            "baseline",
            diff.baseline_network_capture_status,
            diff.baseline_dropped_network_connection_count,
        ),
        (
            "candidate",
            diff.candidate_network_capture_status,
            diff.candidate_dropped_network_connection_count,
        ),
    )
    if any(status is not None for _, status, _ in capture_statuses):
        for side, status, dropped_count in capture_statuses:
            if status is None:
                incomplete.append(f"{side}: network capture unavailable")
            elif status != "complete":
                omitted = (
                    f", {dropped_count} omitted"
                    if dropped_count is not None and dropped_count > 0
                    else ""
                )
                incomplete.append(f"{side}: network capture {status}{omitted}")
    if not incomplete:
        return None
    return f"network evidence incomplete ({'; '.join(incomplete)})"


def _incomplete_network_setup_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    capture_statuses = (
        (
            "baseline",
            diff.baseline_network_setup_capture_status,
            diff.baseline_dropped_network_setup_phase_count,
        ),
        (
            "candidate",
            diff.candidate_network_setup_capture_status,
            diff.candidate_dropped_network_setup_phase_count,
        ),
    )
    if any(status is not None for _, status, _ in capture_statuses):
        for side, status, dropped_count in capture_statuses:
            if status is None:
                incomplete.append(f"{side}: network setup capture unavailable")
            elif status != "complete":
                omitted = (
                    f", {dropped_count} omitted"
                    if dropped_count is not None and dropped_count > 0
                    else ""
                )
                incomplete.append(f"{side}: network setup capture {status}{omitted}")
    if not incomplete:
        return None
    return f"network setup evidence incomplete ({'; '.join(incomplete)})"


def _incomplete_logical_operation_observation(diff: ExecutionDiff) -> str | None:
    incomplete = []
    capture_statuses = (
        (
            "baseline",
            diff.baseline_logical_operation_capture_status,
            diff.baseline_dropped_logical_operation_count,
        ),
        (
            "candidate",
            diff.candidate_logical_operation_capture_status,
            diff.candidate_dropped_logical_operation_count,
        ),
    )
    if any(status not in {None, "unavailable"} for _, status, _ in capture_statuses):
        for side, status, dropped_count in capture_statuses:
            if status in {None, "unavailable"}:
                incomplete.append(f"{side}: logical operation capture unavailable")
            elif status != "complete":
                omitted = (
                    f", {dropped_count} omitted"
                    if dropped_count is not None and dropped_count > 0
                    else ""
                )
                incomplete.append(f"{side}: logical operation capture {status}{omitted}")
    if not incomplete:
        return None
    return f"logical operation evidence incomplete ({'; '.join(incomplete)})"


def _evaluate(
    contract: Contract,
    assertion: Assertion,
    diff: ExecutionDiff,
    baseline: RunpackReader,
    candidate: RunpackReader,
) -> ClaimResult:
    if assertion.type == "candidate_exit_success":
        evidence = _evidence("/candidate/exit_code")
        expected = "candidate exits successfully"
        observed = f"candidate={_exit_status(diff.candidate.exit_code)}"
        evidence = evidence.with_fact(candidate_exit_code=diff.candidate.exit_code)
        if diff.candidate.exit_code is None:
            return _result(contract, assertion, "unverifiable", expected, observed, evidence)
        return _result(
            contract,
            assertion,
            "pass" if diff.candidate.exit_code == 0 else "fail",
            expected,
            observed,
            evidence,
        )
    if assertion.type == "exit_code_equivalent":
        evidence = _evidence("/exit_code_equivalent")
        expected = "equivalent exit status"
        observed = (
            f"baseline={_exit_status(diff.baseline.exit_code)}, "
            f"candidate={_exit_status(diff.candidate.exit_code)}"
        )
        evidence = evidence.with_fact(
            baseline_exit_code=diff.baseline.exit_code,
            candidate_exit_code=diff.candidate.exit_code,
            equivalent=diff.exit_code_equivalent,
        )
        if diff.exit_code_equivalent is None:
            return _result(contract, assertion, "unverifiable", expected, observed, evidence)
        return _result(
            contract,
            assertion,
            "pass" if diff.exit_code_equivalent else "fail",
            expected,
            observed,
            evidence,
        )
    if assertion.type in {"output_equivalent", "result_equivalence"}:
        evidence = _evidence("/output_equivalent").with_fact(equivalent=diff.output_equivalent)
        if diff.output_equivalent is None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                "equivalent output",
                _incomplete_output_observation(diff) or "output identity unavailable",
                evidence,
            )
        return _result(
            contract,
            assertion,
            "pass" if diff.output_equivalent else "fail",
            "equivalent output",
            "equivalent" if diff.output_equivalent else "different",
            evidence,
        )
    if assertion.type == "max_runtime_regression":
        return _max_regression(
            contract,
            assertion,
            diff.wall_time,
            "runtime",
            _evidence("/wall_time"),
            (diff.instrumentation.baseline_mode, diff.instrumentation.candidate_mode),
        )
    if assertion.type == "max_cpu_time_regression":
        return _max_regression(
            contract,
            assertion,
            diff.cpu_time,
            "CPU time",
            _evidence("/cpu_time"),
            (diff.instrumentation.baseline_mode, diff.instrumentation.candidate_mode),
        )
    if assertion.type == "max_peak_memory_regression":
        return _max_regression(
            contract,
            assertion,
            diff.peak_memory,
            "peak memory",
            _evidence("/peak_memory"),
            (diff.instrumentation.baseline_mode, diff.instrumentation.candidate_mode),
        )
    if assertion.type == "forbid_new_dependency":
        source = _string(assertion.config, "from", assertion)
        target = _string(assertion.config, "to", assertion)
        evidence = _evidence("/edge_count_changes", source_name=source, target_name=target)
        if diff.baseline_annotation_error or diff.candidate_annotation_error:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"no new dependency {source} -> {target}",
                "annotation evidence incomplete",
                evidence.with_fact(baseline=None, candidate=None),
            )
        semantic_observation = _incomplete_semantic_observation(diff)
        if semantic_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"no new dependency {source} -> {target}",
                semantic_observation,
                evidence.with_fact(baseline=None, candidate=None),
            )
        causal_observation = _incomplete_causal_observation(diff)
        if causal_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"no new dependency {source} -> {target}",
                causal_observation,
                evidence.with_fact(baseline=None, candidate=None),
            )
        baseline_count = _dependency_total(baseline, source, target)
        candidate_count = _dependency_total(candidate, source, target)
        violation = baseline_count == 0 and candidate_count > 0
        return _result(
            contract,
            assertion,
            "fail" if violation else "pass",
            f"no new dependency {source} -> {target}",
            f"candidate count={candidate_count}" if violation else "not observed",
            evidence.with_fact(baseline=baseline_count, candidate=candidate_count),
        )
    if assertion.type in {"max_operation_count", "max_operation_error_count"}:
        operation = _string(assertion.config, "operation", assertion)
        errors_only = assertion.type == "max_operation_error_count"
        evidence = _evidence(
            "/operation_error_count_changes" if errors_only else "/operation_count_changes",
            operation_name=operation,
        )
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
                evidence.with_fact(baseline=None, candidate=None, limit=None),
            )
        subprocess_observation = _incomplete_subprocess_observation(diff)
        if subprocess_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"candidate {count_label} count <= baseline x {factor:g}",
                subprocess_observation,
                evidence.with_fact(baseline=None, candidate=None, limit=None),
            )
        http_observation = _incomplete_http_observation(diff)
        if http_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"candidate {count_label} count <= baseline x {factor:g}",
                http_observation,
                evidence.with_fact(baseline=None, candidate=None, limit=None),
            )
        network_observation = _incomplete_network_observation(diff)
        if network_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"candidate {count_label} count <= baseline x {factor:g}",
                network_observation,
                evidence.with_fact(baseline=None, candidate=None, limit=None),
            )
        network_setup_observation = _incomplete_network_setup_observation(diff)
        if network_setup_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"candidate {count_label} count <= baseline x {factor:g}",
                network_setup_observation,
                evidence.with_fact(baseline=None, candidate=None, limit=None),
            )
        logical_operation_observation = _incomplete_logical_operation_observation(diff)
        if logical_operation_observation is not None:
            return _result(
                contract,
                assertion,
                "unverifiable",
                f"candidate {count_label} count <= baseline x {factor:g}",
                logical_operation_observation,
                evidence.with_fact(baseline=None, candidate=None, limit=None),
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
                    evidence.with_fact(baseline=None, candidate=None, limit=None),
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
                evidence.with_fact(baseline=0, candidate=0, limit=0.0),
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
            evidence.with_fact(baseline=before, candidate=after, limit=limit),
        )
    raise ContractError(f"unsupported assertion type: {assertion.type}")


def _verify_contracts(
    contracts: tuple[Contract, ...],
    baseline: Path,
    candidate: Path,
) -> tuple[VerificationReport, ExecutionDiff]:
    baseline = resolve_runpack_path(baseline)
    candidate = resolve_runpack_path(candidate)
    with (
        RunpackReader(baseline) as baseline_reader,
        RunpackReader(candidate) as candidate_reader,
    ):
        diff = compare_readers(baseline_reader, candidate_reader)
        report = verify_contracts_readers(
            contracts,
            baseline_reader,
            candidate_reader,
            diff,
        )
        return report, diff


def verify_contracts_readers(
    contracts: tuple[Contract, ...],
    baseline_reader: RunpackReader,
    candidate_reader: RunpackReader,
    diff: ExecutionDiff,
) -> VerificationReport:
    """Evaluate loaded contracts against the readers that produced ``diff``."""
    baseline_id = baseline_reader.execution().id
    candidate_id = candidate_reader.execution().id
    if diff.baseline.id != baseline_id or diff.candidate.id != candidate_id:
        raise ContractError("runtime diff does not describe the supplied runpack readers")
    results = tuple(
        _evaluate(contract, assertion, diff, baseline_reader, candidate_reader)
        for contract in contracts
        for assertion in contract.assertions
    )
    return VerificationReport(diff.baseline.id, diff.candidate.id, results)


def verify_loaded_contracts_with_artifact_bindings(
    contracts: tuple[Contract, ...],
    baseline: Path,
    candidate: Path,
) -> tuple[VerificationReport, ExecutionDiff, VerificationArtifactBindings]:
    """Evaluate loaded contracts and bind the result to the same byte snapshots."""
    with (
        open_runpack_snapshot(baseline) as (baseline_reader, baseline_identity),
        open_runpack_snapshot(candidate) as (candidate_reader, candidate_identity),
    ):
        diff = compare_readers(baseline_reader, candidate_reader)
        report = verify_contracts_readers(
            contracts,
            baseline_reader,
            candidate_reader,
            diff,
        )
        bindings = VerificationArtifactBindings(baseline_identity, candidate_identity)
        return report, diff, bindings


def verify_contracts_with_artifact_bindings(
    contract_path: Path,
    baseline: Path,
    candidate: Path,
) -> tuple[VerificationReport, ExecutionDiff, VerificationArtifactBindings]:
    """Load contracts and evaluate them against exact artifact-bound snapshots."""
    return verify_loaded_contracts_with_artifact_bindings(
        load_contracts(contract_path),
        baseline,
        candidate,
    )


def verify_contracts_with_diff(
    contract_path: Path,
    baseline: Path,
    candidate: Path,
) -> tuple[VerificationReport, ExecutionDiff]:
    return _verify_contracts(load_contracts(contract_path), baseline, candidate)


def verify_contracts(
    contract_path: Path,
    baseline: Path,
    candidate: Path,
) -> VerificationReport:
    report, _ = verify_contracts_with_diff(contract_path, baseline, candidate)
    return report
