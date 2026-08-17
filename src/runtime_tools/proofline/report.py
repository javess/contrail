"""Render Proofline contract results."""

from __future__ import annotations

import json

from runtime_tools.proofline.verify import VerificationReport


def render_verification(report: VerificationReport, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report.as_json_value(), indent=2, sort_keys=True)
    lines = ["PROOFLINE", "", f"{len(report.results)} claims evaluated", ""]
    for result in report.results:
        lines.append(f"{result.status.upper():<12} {result.name}")
    failures = tuple(result for result in report.results if result.status != "pass")
    for result in failures:
        lines.extend(
            (
                "",
                "Failure" if result.status == "fail" else "Unverifiable",
                f"  Contract: {result.contract}",
                f"  Claim:    {result.name}",
                f"  Expected: {result.expected}",
                f"  Observed: {result.observed}",
            )
        )
    return "\n".join(lines)
