"""Bind Proofline findings to exact candidate timeline evidence."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Literal, cast

from runtime_tools.json_support import reject_duplicate_object
from runtime_tools.model import Entity, Event, JsonValue
from runtime_tools.proofline.contracts import (
    MAX_CONTRACT_ASSERTIONS,
    SUPPORTED_ASSERTIONS,
    Contract,
    ContractError,
    load_contracts,
    parse_assertion,
)
from runtime_tools.proofline.verify import verify_contracts_readers
from runtime_tools.rundiff.compare import ExecutionDiff
from runtime_tools.semantics import is_operation_error
from runtime_tools.storage import RunpackArtifactIdentity, RunpackReader

MAX_PROOFLINE_REPORT_BYTES = 64 * 1024 * 1024
MAX_SELECTION_EVENT_IDS = 2_000
MAX_TOTAL_SELECTION_EVENT_IDS = 10_000
MAX_TOTAL_SELECTION_ID_BYTES = 4 * 1024 * 1024

type ProoflineSource = Literal["contract", "report"]
type ReportAssurance = Literal[
    "artifact_bound_policy_replayed",
    "policy_replayed_against_current_evidence",
    "report_authored_policy_runtime_consistent",
]


class ProoflineDataError(ValueError):
    """Raised when Proofline evidence cannot be bound to the current snapshots."""


_EVIDENCE_SHAPES: dict[str, tuple[str, frozenset[str]]] = {
    "candidate_exit_success": ("/candidate/exit_code", frozenset()),
    "exit_code_equivalent": ("/exit_code_equivalent", frozenset()),
    "output_equivalent": ("/output_equivalent", frozenset()),
    "result_equivalence": ("/output_equivalent", frozenset()),
    "max_runtime_regression": ("/wall_time", frozenset()),
    "max_cpu_time_regression": ("/cpu_time", frozenset()),
    "max_peak_memory_regression": ("/peak_memory", frozenset()),
    "forbid_new_dependency": (
        "/edge_count_changes",
        frozenset(("source_name", "target_name")),
    ),
    "max_operation_count": (
        "/operation_count_changes",
        frozenset(("operation_name",)),
    ),
    "max_operation_error_count": (
        "/operation_error_count_changes",
        frozenset(("operation_name",)),
    ),
}


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _finite_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"non-finite JSON number: {value}")
    return result


def _read_report(path: Path) -> dict[str, JsonValue]:
    try:
        with path.open("rb") as source:
            encoded = source.read(MAX_PROOFLINE_REPORT_BYTES + 1)
    except OSError as exc:
        raise ProoflineDataError(f"could not read Proofline report {path}: {exc}") from exc
    if len(encoded) > MAX_PROOFLINE_REPORT_BYTES:
        raise ProoflineDataError(
            f"Proofline report exceeds the {MAX_PROOFLINE_REPORT_BYTES:,}-byte input limit"
        )
    try:
        decoded = encoded.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=reject_duplicate_object,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
        )
    except (UnicodeError, TypeError, ValueError, RecursionError) as exc:
        raise ProoflineDataError("invalid Proofline report JSON") from exc
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ProoflineDataError("Proofline report must be a JSON object")
    return cast(dict[str, JsonValue], value)


def _object(value: JsonValue | None, label: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise ProoflineDataError(f"{label} must be an object")
    return value


def _string(value: JsonValue | None, label: str, *, nonempty: bool = False) -> str:
    if not isinstance(value, str) or nonempty and not value:
        qualifier = "non-empty " if nonempty else ""
        raise ProoflineDataError(f"{label} must be a {qualifier}string")
    return value


def _integer(value: JsonValue | None, label: str, *, minimum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProoflineDataError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise ProoflineDataError(f"{label} must be at least {minimum}")
    return value


def _artifact_identity(value: JsonValue | None, label: str) -> RunpackArtifactIdentity:
    identity = _object(value, label)
    if identity.keys() != {"size_bytes", "sha256"}:
        raise ProoflineDataError(f"{label} must contain exactly size_bytes and sha256")
    size_bytes = _integer(identity.get("size_bytes"), f"{label} size", minimum=0)
    sha256 = _string(identity.get("sha256"), f"{label} SHA-256")
    if len(sha256) != 64 or any(character not in "0123456789abcdef" for character in sha256):
        raise ProoflineDataError(f"{label} SHA-256 must be 64 lowercase hexadecimal characters")
    return RunpackArtifactIdentity(size_bytes, sha256)


def _artifact_bindings(
    value: JsonValue | None,
) -> tuple[RunpackArtifactIdentity, RunpackArtifactIdentity]:
    bindings = _object(value, "Proofline artifact bindings")
    if bindings.keys() != {"baseline", "candidate"}:
        raise ProoflineDataError(
            "Proofline artifact bindings must contain exactly baseline and candidate"
        )
    return (
        _artifact_identity(bindings.get("baseline"), "Proofline baseline artifact binding"),
        _artifact_identity(bindings.get("candidate"), "Proofline candidate artifact binding"),
    )


def _validate_evidence(result: dict[str, JsonValue], assertion_type: str) -> None:
    raw_evidence = result.get("evidence")
    if not isinstance(raw_evidence, list) or not raw_evidence:
        raise ProoflineDataError("Proofline report claims require explained evidence")
    expected_path, expected_selector = _EVIDENCE_SHAPES[assertion_type]
    for raw_reference in raw_evidence:
        reference = _object(raw_reference, "Proofline evidence reference")
        diff_path = _string(reference.get("diff_path"), "Proofline evidence diff path")
        selector = _object(reference.get("selector"), "Proofline evidence selector")
        _object(reference.get("fact"), "Proofline evidence fact")
        if diff_path != expected_path:
            raise ProoflineDataError(
                f"Proofline evidence path does not match claim type {assertion_type}"
            )
        if selector.keys() != expected_selector or not all(
            isinstance(item, str) for item in selector.values()
        ):
            raise ProoflineDataError(
                f"Proofline evidence selector does not match claim type {assertion_type}"
            )


def _validate_verification(value: JsonValue | None) -> dict[str, JsonValue]:
    verification = _object(value, "Proofline verification")
    if verification.get("document_type") != "proofline.verification":
        raise ProoflineDataError("unsupported Proofline verification document type")
    if verification.get("format_version") != "1":
        raise ProoflineDataError("unsupported Proofline report format version")
    _string(verification.get("baseline_id"), "Proofline baseline ID", nonempty=True)
    _string(verification.get("candidate_id"), "Proofline candidate ID", nonempty=True)
    if not isinstance(verification.get("passed"), bool):
        raise ProoflineDataError("Proofline passed verdict must be a boolean")
    claim_count = _integer(verification.get("claim_count"), "Proofline claim count", minimum=0)
    raw_results = verification.get("results")
    if not isinstance(raw_results, list):
        raise ProoflineDataError("Proofline results must be an array")
    if claim_count != len(raw_results):
        raise ProoflineDataError("Proofline claim count does not match its results")
    if claim_count > MAX_CONTRACT_ASSERTIONS:
        raise ProoflineDataError(
            f"Proofline report exceeds the {MAX_CONTRACT_ASSERTIONS:,}-claim input limit"
        )
    statuses: list[str] = []
    for raw_result in raw_results:
        result = _object(raw_result, "Proofline claim")
        for field in ("contract", "name", "expected", "observed"):
            _string(result.get(field), f"Proofline claim {field}")
        assertion_type = _string(result.get("type"), "Proofline claim type")
        if assertion_type not in SUPPORTED_ASSERTIONS:
            raise ProoflineDataError(f"unsupported Proofline claim type: {assertion_type}")
        status = _string(result.get("status"), "Proofline claim status")
        if status not in {"pass", "fail", "unverifiable"}:
            raise ProoflineDataError(f"unsupported Proofline claim status: {status}")
        if {"result_index", "focus", "selection_id", "report_assurance"} & result.keys():
            raise ProoflineDataError("Proofline claim contains reserved timeline fields")
        if "assertion" in result:
            _object(result["assertion"], "Proofline claim assertion")
        statuses.append(status)
        _validate_evidence(result, assertion_type)
    if verification["passed"] != all(status == "pass" for status in statuses):
        raise ProoflineDataError("Proofline passed verdict does not match its claim results")
    return verification


def _verification_from_report(
    path: Path,
    current_diff: ExecutionDiff,
    baseline_reader: RunpackReader,
    candidate_reader: RunpackReader,
    baseline_identity: RunpackArtifactIdentity,
    candidate_identity: RunpackArtifactIdentity,
) -> tuple[dict[str, JsonValue], ReportAssurance]:
    report = _read_report(path)
    document_type = report.get("document_type")
    raw_artifact_bindings: list[JsonValue] = []
    if report.get("format_version") != "1":
        raise ProoflineDataError("unsupported Proofline report format version")
    if document_type == "proofline.verification":
        verification = dict(report)
        verification.pop("diff", None)
        if "artifact_bindings" in report:
            raw_artifact_bindings.append(report["artifact_bindings"])
    elif document_type == "proofline.experiment":
        _string(report.get("baseline_runpack"), "Proofline baseline runpack")
        _string(report.get("candidate_runpack"), "Proofline candidate runpack")
        _integer(report.get("baseline_exit_code"), "Proofline baseline exit code")
        _integer(report.get("candidate_exit_code"), "Proofline candidate exit code")
        nested_verification = _validate_verification(report.get("verification"))
        verification = dict(nested_verification)
        if "artifact_bindings" in report:
            raw_artifact_bindings.append(report["artifact_bindings"])
        if "artifact_bindings" in nested_verification:
            raw_artifact_bindings.append(nested_verification["artifact_bindings"])
        nested_diff = verification.pop("diff", None)
        if nested_diff is not None and not _same_json(nested_diff, current_diff.as_json_value()):
            raise ProoflineDataError("Proofline report diff does not match the current runpacks")
    else:
        raise ProoflineDataError("unsupported Proofline report document type")
    embedded_diff = report.get("diff")
    if embedded_diff is None:
        raise ProoflineDataError("Proofline report requires an embedded explained diff")
    if not _same_json(embedded_diff, current_diff.as_json_value()):
        raise ProoflineDataError("Proofline report diff does not match the current runpacks")
    verification = _validate_verification(verification)
    if (
        verification["baseline_id"] != current_diff.baseline_id
        or verification["candidate_id"] != current_diff.candidate_id
    ):
        raise ProoflineDataError("Proofline verification runpack IDs do not match current runpacks")
    parsed_bindings = tuple(_artifact_bindings(value) for value in raw_artifact_bindings)
    if any(bindings != parsed_bindings[0] for bindings in parsed_bindings[1:]):
        raise ProoflineDataError("Proofline artifact bindings disagree within the report")
    if parsed_bindings:
        baseline_binding, candidate_binding = parsed_bindings[0]
        if baseline_binding != baseline_identity:
            raise ProoflineDataError(
                "Proofline baseline artifact binding does not match the current runpack"
            )
        if candidate_binding != candidate_identity:
            raise ProoflineDataError(
                "Proofline candidate artifact binding does not match the current runpack"
            )
    replayable = _report_is_replayable(verification)
    if replayable:
        _replay_report_claims(verification, current_diff, baseline_reader, candidate_reader)
    else:
        _ReportClaimValidator(current_diff, baseline_reader, candidate_reader).validate(
            verification
        )
    if not replayable:
        assurance: ReportAssurance = "report_authored_policy_runtime_consistent"
    elif parsed_bindings:
        assurance = "artifact_bound_policy_replayed"
    else:
        assurance = "policy_replayed_against_current_evidence"
    return verification, assurance


def _report_is_replayable(verification: dict[str, JsonValue]) -> bool:
    results = cast(list[JsonValue], verification["results"])
    assertion_presence = ["assertion" in cast(dict[str, JsonValue], item) for item in results]
    if any(assertion_presence) and not all(assertion_presence):
        raise ProoflineDataError(
            "Proofline report must include assertion policy for every claim or none"
        )
    return bool(assertion_presence) and all(assertion_presence)


def _replay_report_claims(
    verification: dict[str, JsonValue],
    diff: ExecutionDiff,
    baseline_reader: RunpackReader,
    candidate_reader: RunpackReader,
) -> None:
    raw_results = cast(list[JsonValue], verification["results"])
    contracts: list[Contract] = []
    try:
        for index, raw_result in enumerate(raw_results, 1):
            result = cast(dict[str, JsonValue], raw_result)
            assertion = parse_assertion(
                result["assertion"], label=f"Proofline claim {index} assertion"
            )
            contracts.append(Contract(cast(str, result["contract"]), None, (assertion,)))
    except ContractError as exc:
        raise ProoflineDataError(f"invalid Proofline claim assertion: {exc}") from exc

    replayed = verify_contracts_readers(
        tuple(contracts), baseline_reader, candidate_reader, diff
    ).results
    for raw_result, replayed_result in zip(raw_results, replayed, strict=True):
        result = cast(dict[str, JsonValue], raw_result)
        expected = replayed_result.as_json_value(include_evidence=True)
        for field, expected_value in expected.items():
            if field not in result or not _same_json(result[field], expected_value):
                if field == "evidence":
                    mismatch = "evidence fact does not match replayed policy"
                elif field == "status":
                    mismatch = "claim status does not match replayed policy"
                else:
                    mismatch = f"claim {field} does not match replayed policy"
                raise ProoflineDataError(f"Proofline {replayed_result.type} {mismatch}")


def _same_json(left: JsonValue, right: JsonValue) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _same_json(left_item, right_item)
            for left_item, right_item in zip(left, right, strict=True)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _same_json(left[key], right[key]) for key in left
        )
    return left == right


def _entity_identity(event: Event, entities: dict[str, Entity]) -> tuple[str, str]:
    if event.entity_id is None:
        return "unowned", "unowned"
    entity = entities[event.entity_id]
    return entity.kind, entity.name


class _ReportClaimValidator:
    """Validate reported facts using the same completeness gates as Proofline."""

    def __init__(
        self,
        diff: ExecutionDiff,
        baseline_reader: RunpackReader,
        candidate_reader: RunpackReader,
    ) -> None:
        self._diff = diff
        self._baseline_reader = baseline_reader
        self._candidate_reader = candidate_reader
        self._operation_counts: (
            tuple[
                dict[str, int],
                dict[str, int],
            ]
            | None
        ) = None
        self._operation_error_counts: (
            tuple[
                dict[str, int],
                dict[str, int],
            ]
            | None
        ) = None
        self._dependency_counts: (
            tuple[
                dict[tuple[str, str], int],
                dict[tuple[str, str], int],
            ]
            | None
        ) = None

    def validate(self, verification: dict[str, JsonValue]) -> None:
        results = cast(list[JsonValue], verification["results"])
        for raw_result in results:
            result = cast(dict[str, JsonValue], raw_result)
            assertion_type = cast(str, result["type"])
            evidence = cast(list[JsonValue], result["evidence"])
            for raw_reference in evidence:
                reference = cast(dict[str, JsonValue], raw_reference)
                selector = cast(dict[str, JsonValue], reference["selector"])
                fact = cast(dict[str, JsonValue], reference["fact"])
                derived_status = self._validate_fact(assertion_type, selector, fact)
                if result["status"] != derived_status:
                    raise ProoflineDataError(
                        f"Proofline {assertion_type} claim status does not match current evidence"
                    )

    def _validate_fact(
        self,
        assertion_type: str,
        selector: dict[str, JsonValue],
        fact: dict[str, JsonValue],
    ) -> str:
        if assertion_type == "candidate_exit_success":
            self._require_fact(
                assertion_type,
                fact,
                {"candidate_exit_code": self._diff.candidate_exit_code},
            )
            if self._diff.candidate_exit_code is None:
                return "unverifiable"
            return "pass" if self._diff.candidate_exit_code == 0 else "fail"
        if assertion_type == "exit_code_equivalent":
            self._require_fact(
                assertion_type,
                fact,
                {
                    "baseline_exit_code": self._diff.baseline_exit_code,
                    "candidate_exit_code": self._diff.candidate_exit_code,
                    "equivalent": self._diff.exit_code_equivalent,
                },
            )
            return self._equivalence_status(self._diff.exit_code_equivalent)
        if assertion_type in {"output_equivalent", "result_equivalence"}:
            self._require_fact(assertion_type, fact, {"equivalent": self._diff.output_equivalent})
            return self._equivalence_status(self._diff.output_equivalent)
        if assertion_type == "max_runtime_regression":
            return self._validate_threshold_fact(
                assertion_type,
                fact,
                self._diff.wall_time.baseline,
                self._diff.wall_time.candidate,
            )
        if assertion_type == "max_cpu_time_regression":
            return self._validate_threshold_fact(
                assertion_type,
                fact,
                self._diff.cpu_time.baseline,
                self._diff.cpu_time.candidate,
            )
        if assertion_type == "max_peak_memory_regression":
            return self._validate_threshold_fact(
                assertion_type,
                fact,
                self._diff.peak_memory.baseline,
                self._diff.peak_memory.candidate,
            )
        if assertion_type == "forbid_new_dependency":
            return self._validate_dependency_fact(
                assertion_type,
                fact,
                cast(str, selector["source_name"]),
                cast(str, selector["target_name"]),
            )
        if assertion_type in {"max_operation_count", "max_operation_error_count"}:
            return self._validate_operation_fact(
                assertion_type,
                fact,
                cast(str, selector["operation_name"]),
                errors_only=assertion_type == "max_operation_error_count",
            )
        raise ProoflineDataError(f"unsupported Proofline claim type: {assertion_type}")

    @staticmethod
    def _equivalence_status(equivalent: bool | None) -> str:
        if equivalent is None:
            return "unverifiable"
        return "pass" if equivalent else "fail"

    @staticmethod
    def _fact_number(value: JsonValue, assertion_type: str) -> int | float:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ProoflineDataError(
                f"Proofline {assertion_type} evidence fact does not match current runpacks"
            )
        return value

    @staticmethod
    def _require_fact(
        assertion_type: str,
        actual: dict[str, JsonValue],
        expected: dict[str, JsonValue],
    ) -> None:
        if not _same_json(actual, expected):
            raise ProoflineDataError(
                f"Proofline {assertion_type} evidence fact does not match current runpacks"
            )

    def _validate_threshold_fact(
        self,
        assertion_type: str,
        fact: dict[str, JsonValue],
        baseline: float | None,
        candidate: float | None,
    ) -> str:
        if (
            fact.keys() != {"baseline", "candidate", "limit"}
            or not _same_json(fact.get("baseline"), baseline)
            or not _same_json(fact.get("candidate"), candidate)
        ):
            self._fact_mismatch(assertion_type)
        if baseline is None or candidate is None:
            if fact.get("limit") is not None:
                self._fact_mismatch(assertion_type)
            return "unverifiable"
        # The report does not carry the contract percentage, so the exact limit
        # cannot be reconstructed. Its verdict can still be authenticated against
        # the reported finite limit and the current baseline/candidate values.
        limit = self._fact_number(fact["limit"], assertion_type)
        return "pass" if candidate <= limit else "fail"

    def _validate_operation_fact(
        self,
        assertion_type: str,
        fact: dict[str, JsonValue],
        operation: str,
        *,
        errors_only: bool,
    ) -> str:
        if self._diff.baseline_annotation_error or self._diff.candidate_annotation_error:
            self._require_fact(
                assertion_type, fact, {"baseline": None, "candidate": None, "limit": None}
            )
            return "unverifiable"
        if errors_only and self._semantic_evidence_incomplete():
            self._require_fact(
                assertion_type, fact, {"baseline": None, "candidate": None, "limit": None}
            )
            return "unverifiable"
        baseline_operations, candidate_operations = self._operation_totals(
            operation, errors_only=False
        )
        if baseline_operations == 0 and candidate_operations == 0:
            self._require_fact(assertion_type, fact, {"baseline": 0, "candidate": 0, "limit": 0.0})
            return "unverifiable"
        before, after = (
            self._operation_totals(operation, errors_only=True)
            if errors_only
            else (baseline_operations, candidate_operations)
        )
        if (
            fact.keys() != {"baseline", "candidate", "limit"}
            or not _same_json(fact.get("baseline"), before)
            or not _same_json(fact.get("candidate"), after)
        ):
            self._fact_mismatch(assertion_type)
        limit = self._fact_number(fact["limit"], assertion_type)
        if limit < 0:
            self._fact_mismatch(assertion_type)
        return "pass" if after <= limit else "fail"

    def _validate_dependency_fact(
        self,
        assertion_type: str,
        fact: dict[str, JsonValue],
        source: str,
        target: str,
    ) -> str:
        if (
            self._diff.baseline_annotation_error
            or self._diff.candidate_annotation_error
            or self._semantic_evidence_incomplete()
            or self._causal_evidence_incomplete()
        ):
            self._require_fact(assertion_type, fact, {"baseline": None, "candidate": None})
            return "unverifiable"
        before, after = self._dependency_totals(source, target)
        self._require_fact(assertion_type, fact, {"baseline": before, "candidate": after})
        return "fail" if before == 0 and after > 0 else "pass"

    def _operation_totals(self, operation: str, *, errors_only: bool) -> tuple[int, int]:
        if self._operation_counts is None:
            self._operation_counts = (
                self._aggregate_operations(self._baseline_reader.operation_counts()),
                self._aggregate_operations(self._candidate_reader.operation_counts()),
            )
        counts = self._operation_counts
        if errors_only:
            if self._operation_error_counts is None:
                self._operation_error_counts = (
                    self._aggregate_operations(self._baseline_reader.operation_error_counts()),
                    self._aggregate_operations(self._candidate_reader.operation_error_counts()),
                )
            counts = self._operation_error_counts
        return counts[0].get(operation, 0), counts[1].get(operation, 0)

    @staticmethod
    def _aggregate_operations(
        counts: dict[tuple[str, str, str, str], int],
    ) -> dict[str, int]:
        totals: dict[str, int] = {}
        for (_, _, _, operation_name), count in counts.items():
            totals[operation_name] = totals.get(operation_name, 0) + count
        return totals

    def _dependency_totals(self, source: str, target: str) -> tuple[int, int]:
        if self._dependency_counts is None:
            combined: list[dict[tuple[str, str], int]] = []
            for reader in (self._baseline_reader, self._candidate_reader):
                counts = reader.edge_counts()
                for key, count in reader.peer_service_edge_counts().items():
                    counts[key] = counts.get(key, 0) + count
                totals: dict[tuple[str, str], int] = {}
                for (_, source_name, _, target_name, _), count in counts.items():
                    identity = (source_name, target_name)
                    totals[identity] = totals.get(identity, 0) + count
                combined.append(totals)
            self._dependency_counts = (combined[0], combined[1])
        identity = (source, target)
        return (
            self._dependency_counts[0].get(identity, 0),
            self._dependency_counts[1].get(identity, 0),
        )

    def _semantic_evidence_incomplete(self) -> bool:
        return any(
            count is None or count > 0
            for count in (
                self._diff.baseline_dropped_attribute_count,
                self._diff.candidate_dropped_attribute_count,
            )
        )

    def _causal_evidence_incomplete(self) -> bool:
        return any(
            count is None or count > 0
            for count in (
                self._diff.baseline_missing_causal_references,
                self._diff.candidate_missing_causal_references,
            )
        )

    @staticmethod
    def _fact_mismatch(assertion_type: str) -> None:
        raise ProoflineDataError(
            f"Proofline {assertion_type} evidence fact does not match current runpacks"
        )


class _CandidateEvidenceIndex:
    """Resolve semantic selectors with one pass over each candidate evidence set."""

    def __init__(self, reader: RunpackReader) -> None:
        events = reader.events()
        entities = {entity.id: entity for entity in reader.entities()}
        events_by_id = {event.id: event for event in events}
        event_order = {event.id: index for index, event in enumerate(events)}
        operations: dict[str, list[str]] = {}
        operation_errors: dict[str, list[str]] = {}
        for event in events:
            if event.kind == "log.record":
                continue
            operations.setdefault(event.name, []).append(event.id)
            if is_operation_error(event.attributes):
                operation_errors.setdefault(event.name, []).append(event.id)

        explicit_calls: set[tuple[str, str, str]] = set()
        dependencies: dict[tuple[str, str], set[str]] = {}
        for edge in reader.causal_edges():
            source = events_by_id[edge.source_event_id]
            target = events_by_id[edge.target_event_id]
            if (
                source.kind == "log.record"
                or target.kind == "log.record"
                or source.entity_id == target.entity_id
            ):
                continue
            _, source_name = _entity_identity(source, entities)
            target_kind, target_name = _entity_identity(target, entities)
            if edge.kind == "calls":
                explicit_calls.add((source.id, target_kind, target_name))
            dependencies.setdefault((source_name, target_name), set()).update(
                (source.id, target.id)
            )
        for event in events:
            if event.kind != "client.request":
                continue
            _, source_name = _entity_identity(event, entities)
            peer = event.attributes.get("peer.service")
            if (
                not isinstance(peer, str)
                or not peer
                or (event.id, "service", peer) in explicit_calls
            ):
                continue
            dependencies.setdefault((source_name, peer), set()).add(event.id)

        self._operations = {name: tuple(ids) for name, ids in operations.items()}
        self._operation_errors = {name: tuple(ids) for name, ids in operation_errors.items()}
        self._dependencies = {
            identity: tuple(sorted(ids, key=event_order.__getitem__))
            for identity, ids in dependencies.items()
        }

    def operation(self, name: str, *, errors_only: bool) -> tuple[str, ...]:
        values = self._operation_errors if errors_only else self._operations
        return values.get(name, ())

    def dependency(self, source_name: str, target_name: str) -> tuple[str, ...]:
        return self._dependencies.get((source_name, target_name), ())


class _Selections:
    def __init__(self) -> None:
        self.values: dict[str, JsonValue] = {}
        self._ids: dict[tuple[str, tuple[str, ...]], str] = {}
        self._remaining = MAX_TOTAL_SELECTION_EVENT_IDS
        self._remaining_bytes = MAX_TOTAL_SELECTION_ID_BYTES

    def add(self, relationship: str, event_ids: tuple[str, ...]) -> str:
        identity = (relationship, event_ids)
        existing = self._ids.get(identity)
        if existing is not None:
            return existing
        selection_id = f"selection-{len(self.values)}"
        retained_values: list[str] = []
        for event_id in event_ids[: min(MAX_SELECTION_EVENT_IDS, self._remaining)]:
            serialized_bytes = len(json.dumps(event_id).encode("utf-8")) + 1
            if serialized_bytes > self._remaining_bytes:
                break
            retained_values.append(event_id)
            self._remaining_bytes -= serialized_bytes
        retained = tuple(retained_values)
        retained_count = len(retained)
        self._remaining -= retained_count
        self.values[selection_id] = {
            "relationship": relationship,
            "candidate_event_ids": list(retained),
            "matched_event_count": len(event_ids),
            "truncated": retained_count < len(event_ids),
        }
        self._ids[identity] = selection_id
        return selection_id


def _claim_selector(result: dict[str, JsonValue]) -> dict[str, JsonValue]:
    evidence = cast(list[JsonValue], result["evidence"])
    reference = cast(dict[str, JsonValue], evidence[0])
    return cast(dict[str, JsonValue], reference["selector"])


def _findings(
    verification: dict[str, JsonValue],
    candidate_reader: RunpackReader,
    *,
    source: ProoflineSource,
    report_assurance: ReportAssurance | None = None,
) -> tuple[list[JsonValue], dict[str, JsonValue]]:
    evidence = _CandidateEvidenceIndex(candidate_reader)
    selections = _Selections()
    findings: list[JsonValue] = []
    results = cast(list[JsonValue], verification["results"])
    indexed_results = list(enumerate(results))
    if source == "report":
        indexed_results.sort(
            key=lambda item: cast(dict[str, JsonValue], item[1])["status"] == "pass"
        )
    for index, raw_result in indexed_results:
        result = cast(dict[str, JsonValue], raw_result)
        if source == "contract" and result["status"] == "pass":
            continue
        finding = dict(result)
        finding["result_index"] = index
        assertion_type = cast(str, result["type"])
        if source == "report":
            assert report_assurance is not None
            finding["report_assurance"] = report_assurance
        selector = _claim_selector(result)
        if assertion_type in {"max_operation_count", "max_operation_error_count"}:
            errors_only = assertion_type == "max_operation_error_count"
            event_ids = evidence.operation(
                cast(str, selector["operation_name"]), errors_only=errors_only
            )
            finding["focus"] = "candidate_events"
            finding["selection_id"] = selections.add(
                "operation_error" if errors_only else "operation", event_ids
            )
        elif assertion_type == "forbid_new_dependency":
            event_ids = evidence.dependency(
                cast(str, selector["source_name"]),
                cast(str, selector["target_name"]),
            )
            finding["focus"] = "candidate_events"
            finding["selection_id"] = selections.add("dependency", event_ids)
        else:
            finding["focus"] = "candidate_summary"
        findings.append(finding)
    return findings, selections.values


def build_proofline_payload(
    source: ProoflineSource,
    source_path: Path,
    baseline_reader: RunpackReader,
    candidate_reader: RunpackReader,
    diff: ExecutionDiff,
    baseline_identity: RunpackArtifactIdentity | None,
    candidate_identity: RunpackArtifactIdentity | None,
) -> dict[str, JsonValue]:
    """Build a UI-safe Proofline view from the current open reader snapshots."""
    if source == "contract":
        report = verify_contracts_readers(
            load_contracts(source_path), baseline_reader, candidate_reader, diff
        )
        verification = report.as_json_value(include_evidence=True)
        report_assurance = None
    else:
        if baseline_identity is None or candidate_identity is None:
            raise ProoflineDataError(
                "Proofline report replay requires artifact identities from the open snapshots"
            )
        verification, report_assurance = _verification_from_report(
            source_path,
            diff,
            baseline_reader,
            candidate_reader,
            baseline_identity,
            candidate_identity,
        )
    findings, selections = _findings(
        verification,
        candidate_reader,
        source=source,
        report_assurance=report_assurance,
    )
    return {
        "source": source,
        "verification": verification,
        "findings": findings,
        "selections": selections,
    }
