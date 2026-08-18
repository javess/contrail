"""Render BatchScope analysis facts."""

from __future__ import annotations

import json

from runtime_tools.batchscope.analysis import BatchAnalysis
from runtime_tools.terminal import terminal_text


def render_analysis(analysis: BatchAnalysis, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(analysis.as_json_value(), indent=2, sort_keys=True)
    lines = [
        "BATCHSCOPE",
        f"run:   {terminal_text(analysis.name)} ({terminal_text(analysis.execution_id[:8])})",
        f"total: {_duration(analysis.total_seconds)}",
        "",
        "Lifecycle",
    ]
    for phase in analysis.lifecycle:
        lines.append(
            f"  {terminal_text(phase.name):<24} "
            f"{_duration(phase.duration_seconds):>10}  {phase.source}"
        )
    lines.extend(("", "Critical path"))
    if analysis.critical_path is None:
        lines.append("  unavailable")
    else:
        path = analysis.critical_path
        lines.append(f"  {path.certainty}: {_duration(path.duration_seconds)}")
        lines.append(f"  active execution: {_duration(path.active_seconds)}")
        lines.append(f"  causal waiting: {_duration(path.waiting_seconds)}")
        lines.append(f"  parallel slack: {_duration(path.parallel_slack_seconds)}")
        lines.append(f"  {' → '.join(terminal_text(name) for name in path.event_names)}")
    lines.extend(("", "Throughput"))
    if analysis.throughput is None:
        lines.append("  no progress evidence")
    else:
        throughput = analysis.throughput
        lines.append(f"  completed: {throughput.completed:g} / {throughput.total:g}")
        lines.append(f"  rate: {_rate(throughput.rate_per_second)}")
        lines.append(f"  remaining: {throughput.remaining:g}")
        lines.append(f"  estimated drain: {_duration(throughput.estimated_drain_seconds)}")
        if throughput.compute_finished_at_ns is not None:
            lines.append(
                "  remaining at compute completion: "
                f"{_number(throughput.remaining_at_compute_completion)}"
            )
            lines.append(f"  post-compute wall time: {_duration(throughput.post_compute_seconds)}")
            lines.append(f"  post-compute rate: {_rate(throughput.post_compute_rate_per_second)}")
    lines.extend(("", "Bottlenecks"))
    if not analysis.bottlenecks:
        lines.append("  none classified from available evidence")
    for bottleneck in analysis.bottlenecks:
        lines.append(f"  {terminal_text(bottleneck.classification)} ({bottleneck.confidence:.0%})")
        lines.append(f"    {terminal_text(bottleneck.evidence)}")
    return "\n".join(lines)


def _duration(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 1:
        return f"{value * 1000:.1f}ms"
    return f"{value:.3f}s"


def _rate(value: float | None) -> str:
    return "unknown" if value is None else f"{value:,.2f}/s"


def _number(value: float | None) -> str:
    return "unknown" if value is None else f"{value:g}"
