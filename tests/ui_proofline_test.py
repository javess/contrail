from __future__ import annotations

import copy
import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import pytest

import runtime_tools.storage as storage
import runtime_tools.ui.data as ui_data
import runtime_tools.ui.proofline as ui_proofline
from runtime_tools.model import CausalEdge, Entity, Event, Execution
from runtime_tools.proofline.verify import VerificationArtifactBindings
from runtime_tools.rundiff.compare import compare_readers
from runtime_tools.storage import (
    RunpackError,
    RunpackReader,
    RunpackWriter,
    open_runpack_snapshot,
)
from runtime_tools.ui import TimelineError, build_timeline_payload


def _write_runpack(path: Path, *, candidate: bool) -> None:
    events = (
        (
            Event(
                "write-ok",
                "client.request",
                "db.write",
                "app",
                10,
                20,
                "test",
                None,
                0,
                {},
            ),
            Event(
                "write-error",
                "client.request",
                "db.write",
                "app",
                21,
                30,
                "test",
                None,
                1,
                {"otel.status.code": "STATUS_CODE_ERROR"},
            ),
            Event(
                "dependency-source",
                "client.request",
                "metadata.fetch",
                "app",
                31,
                40,
                "test",
                None,
                2,
                {"peer.service": "metadata"},
            ),
            Event(
                "dependency-target",
                "server.request",
                "metadata.fetch",
                "metadata",
                32,
                39,
                "test",
                None,
                3,
                {},
            ),
            Event(
                "peer-only",
                "client.request",
                "cache.fetch",
                "app",
                41,
                50,
                "test",
                None,
                4,
                {"peer.service": "cache"},
            ),
        )
        if candidate
        else (
            Event(
                "baseline-write",
                "client.request",
                "db.write",
                "app",
                10,
                20,
                "test",
                None,
                0,
                {},
            ),
        )
    )
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "candidate" if candidate else "baseline",
                "candidate" if candidate else "baseline",
                0,
                200 if candidate else 100,
                (),
                str(path.parent),
                0,
                None,
                {},
            )
        )
        writer.add_entities(
            (
                Entity("app", "service", "app", None, {}),
                Entity("metadata", "service", "metadata", None, {}),
            )
        )
        writer.add_events(events)
        if candidate:
            writer.add_causal_edge(
                CausalEdge("dependency-source", "dependency-target", "calls", 1.0, {})
            )


def _write_contract(path: Path) -> None:
    path.write_text(
        """
name: debug-regressions
assertions:
  - type: max_runtime_regression
    percent: 0
  - type: max_operation_count
    operation: db.write
    relative_to: baseline
    factor: 1
  - type: max_operation_error_count
    operation: db.write
    relative_to: baseline
    factor: 0
  - type: forbid_new_dependency
    from: app
    to: metadata
  - type: forbid_new_dependency
    from: app
    to: cache
""".strip(),
        encoding="utf-8",
    )


def _inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    _write_runpack(baseline, candidate=False)
    _write_runpack(candidate, candidate=True)
    _write_contract(contract)
    return baseline, candidate, contract


def _proofline(payload: dict[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], payload["proofline"])


def _artifact_bindings(baseline: Path, candidate: Path) -> dict[str, Any]:
    with (
        open_runpack_snapshot(baseline) as (_, baseline_identity),
        open_runpack_snapshot(candidate) as (_, candidate_identity),
    ):
        return cast(
            dict[str, Any],
            VerificationArtifactBindings(
                baseline_identity,
                candidate_identity,
            ).as_json_value(),
        )


def _legacy_verification(verification: dict[str, Any]) -> dict[str, Any]:
    legacy = copy.deepcopy(verification)
    for result in legacy["results"]:
        result.pop("assertion")
    return legacy


def test_timeline_contract_maps_failed_claims_to_exact_candidate_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    compare_calls = 0
    real_compare = compare_readers

    def count_compare(*args: Any, **kwargs: Any) -> Any:
        nonlocal compare_calls
        compare_calls += 1
        return real_compare(*args, **kwargs)

    def reject_artifact_hashing(path: Path) -> Any:
        raise AssertionError(f"contract timeline unexpectedly hashed {path}")

    monkeypatch.setattr(ui_data, "compare_readers", count_compare)
    monkeypatch.setattr(ui_data, "open_runpack_snapshot", reject_artifact_hashing)

    payload = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))

    proofline = _proofline(payload)
    assert compare_calls == 1
    assert proofline["source"] == "contract"
    assert proofline["verification"]["document_type"] == "proofline.verification"
    assert proofline["verification"]["format_version"] == "1"
    assert proofline["verification"]["baseline_id"] == "baseline"
    assert proofline["verification"]["candidate_id"] == "candidate"
    findings = proofline["findings"]
    assert [finding["result_index"] for finding in findings] == [0, 1, 2, 3, 4]
    assert findings[0]["focus"] == "candidate_summary"
    assert "selection_id" not in findings[0]
    assert findings[1]["evidence"][0]["fact"] == {
        "baseline": 1,
        "candidate": 2,
        "limit": 1.0,
    }
    assert findings[3]["evidence"][0]["fact"] == {
        "baseline": 0,
        "candidate": 1,
    }
    assert all(finding["status"] == "fail" for finding in findings)
    assert all("report_assurance" not in finding for finding in findings)
    assert proofline["selections"] == {
        "selection-0": {
            "relationship": "operation",
            "candidate_event_ids": ["write-ok", "write-error"],
            "matched_event_count": 2,
            "truncated": False,
        },
        "selection-1": {
            "relationship": "operation_error",
            "candidate_event_ids": ["write-error"],
            "matched_event_count": 1,
            "truncated": False,
        },
        "selection-2": {
            "relationship": "dependency",
            "candidate_event_ids": ["dependency-source", "dependency-target"],
            "matched_event_count": 2,
            "truncated": False,
        },
        "selection-3": {
            "relationship": "dependency",
            "candidate_event_ids": ["peer-only"],
            "matched_event_count": 1,
            "truncated": False,
        },
    }
    assert [finding.get("selection_id") for finding in findings] == [
        None,
        "selection-0",
        "selection-1",
        "selection-2",
        "selection-3",
    ]


def test_timeline_report_reuses_explained_claims_only_after_binding_current_runpacks(
    tmp_path: Path,
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    generated_proofline = _proofline(generated)
    report = dict(generated_proofline["verification"])
    report["diff"] = generated["comparison"]
    report["producer_note"] = "retained"
    report["results"][1]["diagnostic_note"] = "full explained claim"
    report_path = tmp_path / "proofline.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = cast(
        dict[str, Any],
        build_timeline_payload(baseline, candidate, proofline_report=report_path),
    )

    proofline = _proofline(payload)
    assert proofline["source"] == "report"
    assert proofline["verification"]["producer_note"] == "retained"
    assert "diff" not in proofline["verification"]
    assert proofline["findings"][1]["diagnostic_note"] == "full explained claim"
    assert {finding["report_assurance"] for finding in proofline["findings"]} == {
        "policy_replayed_against_current_evidence"
    }
    assert proofline["selections"] == generated_proofline["selections"]


def test_timeline_distinguishes_an_artifact_bound_replayed_report(tmp_path: Path) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["artifact_bindings"] = _artifact_bindings(baseline, candidate)
    report_path = tmp_path / "artifact-bound.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = cast(
        dict[str, Any],
        build_timeline_payload(baseline, candidate, proofline_report=report_path),
    )

    assert {finding["report_assurance"] for finding in _proofline(payload)["findings"]} == {
        "artifact_bound_policy_replayed"
    }


def test_retained_report_timeline_rejects_a_runpack_that_fails_bounded_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    with sqlite3.connect(candidate) as connection:
        connection.execute(
            "UPDATE executions SET metadata_json = ?",
            (json.dumps({"oversized": "x" * 1024}),),
        )
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["artifact_bindings"] = _artifact_bindings(baseline, candidate)
    report_path = tmp_path / "artifact-bound.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(storage, "MAX_RUNPACK_JSON_BYTES", 512)

    with pytest.raises(RunpackError, match="runpack JSON exceeds"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


@pytest.mark.parametrize(
    ("statement", "parameters"),
    (
        ("UPDATE executions SET command_json = ?", ('["substituted"]',)),
        ("UPDATE executions SET working_directory = ?", ("/substituted",)),
        ("UPDATE executions SET revision = ?", ("substituted",)),
        (
            """
            INSERT INTO attachments(id, kind, name, media_type, content, attributes_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("substitute", "raw", "substitute", "text/plain", b"different", "{}"),
        ),
    ),
)
def test_timeline_rejects_semantically_equivalent_candidate_artifact_substitution(
    tmp_path: Path,
    statement: str,
    parameters: tuple[object, ...],
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["artifact_bindings"] = _artifact_bindings(baseline, candidate)
    report_path = tmp_path / "bound-before-substitution.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with sqlite3.connect(candidate) as connection:
        connection.execute(statement, parameters)

    with pytest.raises(TimelineError, match="candidate artifact binding does not match"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_rejects_a_semantically_equivalent_baseline_artifact_substitution(
    tmp_path: Path,
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["artifact_bindings"] = _artifact_bindings(baseline, candidate)
    report_path = tmp_path / "bound-before-baseline-substitution.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with sqlite3.connect(baseline) as connection:
        connection.execute("UPDATE executions SET revision = ?", ("substituted",))

    with pytest.raises(TimelineError, match="baseline artifact binding does not match"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


@pytest.mark.parametrize(
    "artifact_bindings",
    (
        {"baseline": {"size_bytes": 1, "sha256": "a" * 64}},
        {
            "baseline": {"size_bytes": 1, "sha256": "a" * 64},
            "candidate": {"size_bytes": 1, "sha256": "A" * 64},
        },
        {
            "baseline": {"size_bytes": True, "sha256": "a" * 64},
            "candidate": {"size_bytes": 1, "sha256": "b" * 64},
        },
    ),
)
def test_timeline_rejects_partial_or_malformed_artifact_bindings(
    tmp_path: Path,
    artifact_bindings: dict[str, Any],
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["artifact_bindings"] = artifact_bindings
    report_path = tmp_path / "malformed-artifact-bindings.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match="artifact binding"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_uses_one_open_generation_for_bound_identity_and_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["artifact_bindings"] = _artifact_bindings(baseline, candidate)
    report_path = tmp_path / "bound-before-path-replacement.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    replacement = tmp_path / "replacement.runpack"
    with RunpackWriter(replacement) as writer:
        writer.add_execution(
            Execution("replacement", "replacement", 0, 1, (), str(tmp_path), 0, None, {})
        )
    real_open_snapshot = open_runpack_snapshot

    @contextmanager
    def replace_baseline_before_candidate_open(
        path: Path,
    ) -> Iterator[tuple[Any, Any]]:
        if path == candidate:
            os.replace(replacement, baseline)
        with real_open_snapshot(path) as snapshot:
            yield snapshot

    monkeypatch.setattr(
        ui_data,
        "open_runpack_snapshot",
        replace_baseline_before_candidate_open,
    )

    payload = cast(
        dict[str, Any],
        build_timeline_payload(baseline, candidate, proofline_report=report_path),
    )

    assert payload["runs"][0]["summary"]["id"] == "baseline"
    assert {finding["report_assurance"] for finding in _proofline(payload)["findings"]} == {
        "artifact_bound_policy_replayed"
    }


def test_timeline_accepts_an_explained_experiment_report(tmp_path: Path) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    verification = _proofline(generated)["verification"]
    report = {
        "document_type": "proofline.experiment",
        "format_version": "1",
        "baseline_runpack": "baseline.runpack",
        "candidate_runpack": "candidate.runpack",
        "baseline_exit_code": 0,
        "candidate_exit_code": 0,
        "verification": verification,
        "diff": generated["comparison"],
        "artifact_bindings": _artifact_bindings(baseline, candidate),
    }
    report_path = tmp_path / "experiment.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = cast(
        dict[str, Any],
        build_timeline_payload(baseline, candidate, proofline_report=report_path),
    )

    proofline = _proofline(payload)
    assert proofline["verification"] == verification
    assert {finding["report_assurance"] for finding in proofline["findings"]} == {
        "artifact_bound_policy_replayed"
    }


@pytest.mark.parametrize(
    ("result_index", "field", "forged_value"),
    (
        (0, "baseline", 999.0),
        (1, "candidate", 999),
        (2, "baseline", 999),
        (3, "candidate", 999),
        (4, "baseline", 999),
    ),
)
def test_timeline_rejects_forged_report_facts_for_current_runtime_evidence(
    tmp_path: Path, result_index: int, field: str, forged_value: Any
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["results"][result_index]["evidence"][0]["fact"][field] = forged_value
    report_path = tmp_path / f"forged-fact-{result_index}.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match="evidence fact does not match"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


@pytest.mark.parametrize("result_index", range(5))
def test_timeline_rejects_forged_report_statuses(tmp_path: Path, result_index: int) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["results"][result_index]["status"] = "pass"
    report_path = tmp_path / f"forged-status-{result_index}.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match="claim status does not match"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_rejects_a_combined_threshold_and_status_change_when_policy_is_replayable(
    tmp_path: Path,
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["results"][1]["evidence"][0]["fact"]["limit"] = 999.0
    report["results"][1]["status"] = "pass"
    report_path = tmp_path / "reported-threshold-pass.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match="claim status does not match replayed policy"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_keeps_configless_v1_reports_on_the_degraded_legacy_path(
    tmp_path: Path,
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = _legacy_verification(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    report["artifact_bindings"] = _artifact_bindings(baseline, candidate)
    report["results"][1]["evidence"][0]["fact"]["limit"] = 999.0
    report["results"][1]["status"] = "pass"
    report_path = tmp_path / "legacy-reported-threshold-pass.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = cast(
        dict[str, Any],
        build_timeline_payload(baseline, candidate, proofline_report=report_path),
    )

    reported_pass = _proofline(payload)["findings"][-1]
    assert reported_pass["status"] == "pass"
    assert reported_pass["report_assurance"] == "report_authored_policy_runtime_consistent"
    assert "assertion" not in reported_pass


def test_timeline_labels_a_configless_selector_as_report_authored_policy(
    tmp_path: Path,
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = _legacy_verification(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    changed = report["results"][3]
    changed["expected"] = "no new dependency app -> cache"
    changed["evidence"][0]["selector"]["target_name"] = "cache"
    report_path = tmp_path / "legacy-selector-change.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = cast(
        dict[str, Any],
        build_timeline_payload(baseline, candidate, proofline_report=report_path),
    )

    finding = next(item for item in _proofline(payload)["findings"] if item["result_index"] == 3)
    assert finding["report_assurance"] == "report_authored_policy_runtime_consistent"
    selection = _proofline(payload)["selections"][finding["selection_id"]]
    assert selection["candidate_event_ids"] == ["peer-only"]


@pytest.mark.parametrize(
    "mutation",
    (
        lambda result: result["assertion"].update({"factor": 4}),
        lambda result: result.update({"expected": "anything"}),
        lambda result: result.update({"observed": "anything"}),
        lambda result: result["evidence"][0]["selector"].update({"operation_name": "other"}),
        lambda result: result["evidence"][0]["fact"].update({"limit": 999.0}),
    ),
)
def test_timeline_replays_embedded_policy_and_rejects_claim_tampering(
    tmp_path: Path, mutation: Any
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    mutation(report["results"][1])
    report_path = tmp_path / "tampered-replayable-report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match="does not match replayed policy"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_authenticates_pass_and_unverifiable_report_facts(tmp_path: Path) -> None:
    baseline, candidate, _ = _inputs(tmp_path)
    contract = tmp_path / "summary-contract.yaml"
    contract.write_text(
        """
name: summary-evidence
assertions:
  - type: candidate_exit_success
  - type: exit_code_equivalent
  - type: output_equivalent
  - type: max_cpu_time_regression
    percent: 0
  - type: max_peak_memory_regression
    percent: 0
""".strip(),
        encoding="utf-8",
    )
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    verification = _legacy_verification(_proofline(generated)["verification"])
    assert [result["status"] for result in verification["results"]] == [
        "pass",
        "pass",
        "unverifiable",
        "unverifiable",
        "unverifiable",
    ]

    authentic_report = copy.deepcopy(verification)
    authentic_report["diff"] = generated["comparison"]
    authentic_path = tmp_path / "authentic-summary.json"
    authentic_path.write_text(json.dumps(authentic_report), encoding="utf-8")
    authentic_payload = cast(
        dict[str, Any],
        build_timeline_payload(baseline, candidate, proofline_report=authentic_path),
    )
    findings = _proofline(authentic_payload)["findings"]
    assert [finding["result_index"] for finding in findings] == [2, 3, 4, 0, 1]
    assert {finding["report_assurance"] for finding in findings} == {
        "report_authored_policy_runtime_consistent"
    }

    mutations = (
        (0, "candidate_exit_code", 7),
        (1, "equivalent", False),
        (2, "equivalent", True),
        (3, "baseline", 0.0),
        (4, "limit", 0.0),
    )
    for result_index, field, forged_value in mutations:
        report = copy.deepcopy(verification)
        report["diff"] = generated["comparison"]
        report["results"][result_index]["evidence"][0]["fact"][field] = forged_value
        report_path = tmp_path / f"forged-summary-{result_index}.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        with pytest.raises(TimelineError, match="evidence fact does not match"):
            build_timeline_payload(baseline, candidate, proofline_report=report_path)

    report = copy.deepcopy(verification)
    report["diff"] = generated["comparison"]
    report["results"][2]["status"] = "fail"
    report_path = tmp_path / "forged-unverifiable-status.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(TimelineError, match="claim status does not match"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_rejects_partially_stripped_assertion_policies(tmp_path: Path) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = copy.deepcopy(_proofline(generated)["verification"])
    report["results"][0].pop("assertion")
    report["diff"] = generated["comparison"]
    report_path = tmp_path / "partially-stripped.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match="assertion policy for every claim or none"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda report: report["diff"].update({"candidate": {"id": "other"}}), "diff"),
        (lambda report: report["diff"]["candidate"].update({"exit_code": False}), "diff"),
        (lambda report: report.update({"candidate_id": "other"}), "runpack IDs"),
        (lambda report: report["results"][0].pop("evidence"), "explained evidence"),
        (lambda report: report.update({"format_version": "2"}), "format version"),
    ),
)
def test_timeline_rejects_a_stale_or_malformed_proofline_report(
    tmp_path: Path, mutation: Any, message: str
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = dict(_proofline(generated)["verification"])
    report["results"] = [dict(result) for result in report["results"]]
    report["diff"] = generated["comparison"]
    mutation(report)
    report_path = tmp_path / "bad.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match=message):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


@pytest.mark.parametrize("invalid", ("duplicate", "nonfinite", "overflow"))
def test_timeline_rejects_ambiguous_or_nonfinite_report_json(tmp_path: Path, invalid: str) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = dict(_proofline(generated)["verification"])
    report["diff"] = generated["comparison"]
    encoded = json.dumps(report)
    if invalid == "duplicate":
        encoded = encoded.replace(
            '"document_type": "proofline.verification"',
            '"document_type": "proofline.verification", "document_type": "proofline.verification"',
            1,
        )
    elif invalid == "nonfinite":
        encoded = encoded.replace('"claim_count": 5', '"claim_count": NaN', 1)
    else:
        encoded = encoded.replace('"claim_count": 5', '"claim_count": 1e400', 1)
    report_path = tmp_path / "invalid.json"
    report_path.write_text(encoded, encoding="utf-8")

    with pytest.raises(TimelineError, match="invalid Proofline report JSON"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_bounds_selection_ids_per_selection_and_globally(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    monkeypatch.setattr(ui_proofline, "MAX_SELECTION_EVENT_IDS", 1)
    monkeypatch.setattr(ui_proofline, "MAX_TOTAL_SELECTION_EVENT_IDS", 2)

    payload = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))

    selections = _proofline(payload)["selections"]
    assert selections["selection-0"] == {
        "relationship": "operation",
        "candidate_event_ids": ["write-ok"],
        "matched_event_count": 2,
        "truncated": True,
    }
    assert selections["selection-1"]["candidate_event_ids"] == ["write-error"]
    assert selections["selection-2"] == {
        "relationship": "dependency",
        "candidate_event_ids": [],
        "matched_event_count": 2,
        "truncated": True,
    }


def test_timeline_bounds_the_global_serialized_selection_id_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    first_id_bytes = len(json.dumps("write-ok").encode("utf-8")) + 1
    monkeypatch.setattr(ui_proofline, "MAX_TOTAL_SELECTION_ID_BYTES", first_id_bytes)

    payload = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))

    selections = _proofline(payload)["selections"]
    assert selections["selection-0"] == {
        "relationship": "operation",
        "candidate_event_ids": ["write-ok"],
        "matched_event_count": 2,
        "truncated": True,
    }
    assert selections["selection-1"]["candidate_event_ids"] == []
    assert selections["selection-1"]["matched_event_count"] == 1
    assert selections["selection-1"]["truncated"] is True


def test_timeline_rejects_reports_above_the_contract_claim_limit(tmp_path: Path) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    generated = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))
    report = dict(_proofline(generated)["verification"])
    report["results"] = [report["results"][0]] * 1_001
    report["claim_count"] = 1_001
    report["passed"] = False
    report["diff"] = generated["comparison"]
    report_path = tmp_path / "too-many-claims.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(TimelineError, match="exceeds the 1,000-claim input limit"):
        build_timeline_payload(baseline, candidate, proofline_report=report_path)


def test_timeline_proofline_evidence_uses_one_snapshot_before_candidate_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)
    real_close = RunpackReader.close
    mutated = False

    def mutate_after_candidate_snapshot(reader: RunpackReader) -> None:
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
                        60,
                        70,
                        "test",
                        None,
                        99,
                        {"error": True},
                    )
                )

    monkeypatch.setattr(RunpackReader, "close", mutate_after_candidate_snapshot)

    payload = cast(dict[str, Any], build_timeline_payload(baseline, candidate, contract=contract))

    candidate_run = payload["runs"][1]
    assert [event["id"] for event in candidate_run["events"]] == [
        "write-ok",
        "write-error",
        "dependency-source",
        "dependency-target",
        "peer-only",
    ]
    db_change = next(
        change
        for change in payload["comparison"]["operation_count_changes"]
        if change["operation_name"] == "db.write"
    )
    assert db_change["candidate"] == 2
    proofline = _proofline(payload)
    assert proofline["findings"][1]["evidence"][0]["fact"]["candidate"] == 2
    assert proofline["selections"]["selection-0"]["candidate_event_ids"] == [
        "write-ok",
        "write-error",
    ]
    assert mutated is True
    with RunpackReader(candidate) as reader:
        assert "late-write" in {event.id for event in reader.events()}


def test_timeline_rejects_oversized_reports_before_parsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline, candidate, _ = _inputs(tmp_path)
    report = tmp_path / "large.json"
    report.write_bytes(b"{} " * 10)
    monkeypatch.setattr(ui_proofline, "MAX_PROOFLINE_REPORT_BYTES", 8)

    with pytest.raises(TimelineError, match="Proofline report exceeds the 8-byte input limit"):
        build_timeline_payload(baseline, candidate, proofline_report=report)


def test_timeline_proofline_inputs_are_mutually_exclusive_and_require_a_candidate(
    tmp_path: Path,
) -> None:
    baseline, candidate, contract = _inputs(tmp_path)

    assert build_timeline_payload(baseline)["proofline"] is None
    with pytest.raises(TimelineError, match="requires a candidate"):
        build_timeline_payload(baseline, contract=contract)
    with pytest.raises(TimelineError, match="requires a candidate"):
        build_timeline_payload(baseline, proofline_report=tmp_path / "report.json")
    with pytest.raises(TimelineError, match="mutually exclusive"):
        build_timeline_payload(
            baseline,
            candidate,
            contract=contract,
            proofline_report=tmp_path / "report.json",
        )
