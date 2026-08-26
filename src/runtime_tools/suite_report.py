"""Compose one human-readable view of the Contrail product suite."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from runtime_tools.batchscope.analysis import BatchAnalysis, analyze_reader
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.proofline.contracts import load_contracts
from runtime_tools.proofline.report import render_verification
from runtime_tools.proofline.verify import VerificationReport, verify_contracts_readers
from runtime_tools.rundiff.compare import ExecutionDiff, compare_readers
from runtime_tools.rundiff.report import render_diff
from runtime_tools.storage import open_runpack_snapshot
from runtime_tools.terminal import terminal_text


@dataclass(frozen=True, slots=True)
class SuiteReport:
    """BatchScope, RunDiff, and optional Proofline facts from shared snapshots."""

    candidate_analysis: BatchAnalysis
    diff: ExecutionDiff
    verification: VerificationReport | None


def build_suite_report(
    baseline: Path,
    candidate: Path,
    *,
    contract: Path | None = None,
) -> SuiteReport:
    """Evaluate the suite against one stable snapshot of each runpack."""
    contracts = load_contracts(contract) if contract is not None else None
    with (
        open_runpack_snapshot(baseline) as (baseline_reader, _),
        open_runpack_snapshot(candidate) as (candidate_reader, _),
    ):
        diff = compare_readers(baseline_reader, candidate_reader)
        candidate_analysis = analyze_reader(candidate_reader)
        verification = (
            verify_contracts_readers(
                contracts,
                baseline_reader,
                candidate_reader,
                diff,
            )
            if contracts is not None
            else None
        )
    return SuiteReport(candidate_analysis, diff, verification)


def _render_executive_summary(report: SuiteReport) -> str:
    lines = [
        "Executive summary",
        f"  runtime behavior: {report.diff.outcome}",
    ]
    if report.candidate_analysis.bottlenecks:
        bottleneck = report.candidate_analysis.bottlenecks[0]
        lines.extend(
            (
                "  candidate bottleneck: "
                f"{terminal_text(bottleneck.classification)} ({bottleneck.confidence:.0%})",
                f"    {terminal_text(bottleneck.evidence)}",
            )
        )
    else:
        lines.append("  candidate bottleneck: none classified from available evidence")
    if report.verification is None:
        lines.append("  contracts: not evaluated (pass --contract PATH to include Proofline)")
    else:
        passed = sum(result.status == "pass" for result in report.verification.results)
        failed = sum(result.status == "fail" for result in report.verification.results)
        unverifiable = sum(
            result.status == "unverifiable" for result in report.verification.results
        )
        lines.append(f"  contracts: {passed} passed, {failed} failed, {unverifiable} unverifiable")
    lines.append("  mode: diagnostic; use 'contrail verify' for a contract gate")
    return "\n".join(lines)


def render_suite_report(report: SuiteReport) -> str:
    """Render the intentionally human-facing integrated report."""
    sections = [
        "\n".join(
            (
                "CONTRAIL SUITE REPORT",
                f"baseline:  {terminal_text(report.diff.baseline.name)} "
                f"({terminal_text(report.diff.baseline.id[:8])})",
                f"candidate: {terminal_text(report.diff.candidate.name)} "
                f"({terminal_text(report.diff.candidate.id[:8])})",
            )
        ),
        _render_executive_summary(report),
        render_analysis(report.candidate_analysis, "text"),
        render_diff(report.diff, "text"),
    ]
    if report.verification is not None:
        sections.append(render_verification(report.verification, "text", include_evidence=True))
    return "\n\n".join(sections)
