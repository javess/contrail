"""Render Proofline contract results."""

from __future__ import annotations

import json

from runtime_tools.proofline.verify import DiffEvidenceReference, VerificationReport
from runtime_tools.terminal import terminal_text

MAX_TEXT_RESULTS = 100


def _render_fact_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:g}"
    return json.dumps(value, ensure_ascii=True)


def _render_evidence(reference: DiffEvidenceReference) -> str:
    fact = ", ".join(
        f"{terminal_text(key)}={terminal_text(_render_fact_value(value))}"
        for key, value in reference.fact
    )
    selector = ", ".join(
        f"{terminal_text(key)}={terminal_text(json.dumps(value, ensure_ascii=True))}"
        for key, value in reference.selector
    )
    suffix = f" where {selector}" if selector else ""
    return f"  Evidence: {fact}; diff{terminal_text(reference.diff_path)}{suffix}"


def render_verification(
    report: VerificationReport, output_format: str, *, include_evidence: bool = False
) -> str:
    if output_format == "json":
        return json.dumps(report.as_json_value(), allow_nan=False, indent=2, sort_keys=True)
    lines = ["PROOFLINE", "", f"{len(report.results)} claims evaluated", ""]
    prioritized = sorted(
        enumerate(report.results),
        key=lambda item: (item[1].status == "pass", item[0]),
    )[:MAX_TEXT_RESULTS]
    visible_results = tuple(result for _, result in sorted(prioritized))
    for result in visible_results:
        lines.append(f"{result.status.upper():<12} {terminal_text(result.name)}")
    omitted = len(report.results) - MAX_TEXT_RESULTS
    if omitted > 0:
        lines.append(f"… {omitted:,} additional claims omitted from text output")
    failures = tuple(result for result in visible_results if result.status != "pass")
    for result in failures:
        lines.extend(
            (
                "",
                "Failure" if result.status == "fail" else "Unverifiable",
                f"  Contract: {terminal_text(result.contract)}",
                f"  Claim:    {terminal_text(result.name)}",
                f"  Expected: {terminal_text(result.expected)}",
                f"  Observed: {terminal_text(result.observed)}",
            )
        )
        if include_evidence:
            lines.extend(_render_evidence(reference) for reference in result.evidence)
    return "\n".join(lines)
