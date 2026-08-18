"""Render Proofline contract results."""

from __future__ import annotations

import json

from runtime_tools.proofline.verify import VerificationReport
from runtime_tools.terminal import terminal_text

MAX_TEXT_RESULTS = 100


def render_verification(report: VerificationReport, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report.as_json_value(), indent=2, sort_keys=True)
    lines = ["PROOFLINE", "", f"{len(report.results)} claims evaluated", ""]
    visible_results = report.results[:MAX_TEXT_RESULTS]
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
    return "\n".join(lines)
