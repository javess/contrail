"""Concise terminal summary for one BatchScope analysis."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.batchscope.analysis import BatchAnalysis
from runtime_tools.batchscope.analysis._boundary_models import CallerAttribution
from runtime_tools.batchscope.report._support import _duration
from runtime_tools.terminal import terminal_text

MAX_SUMMARY_ITEMS = 5


def render_analysis_summary(analysis: BatchAnalysis) -> str:
    """Render findings and useful evidence without capture diagnostics."""
    lines = [
        f"BATCHSCOPE {terminal_text(analysis.name)} ({terminal_text(analysis.execution_id[:8])})",
        f"total: {_duration(analysis.total_seconds)}",
    ]
    _append_findings(lines, analysis)
    _append_logical_failures(lines, analysis)
    _append_logical_hotspots(lines, analysis)
    _append_python_hotspots(lines, analysis)
    _append_capture_summary(lines, analysis)
    lines.extend(("", "details: use --verbose for capture diagnostics and full evidence"))
    return "\n".join(lines)


def _append_findings(lines: list[str], analysis: BatchAnalysis) -> None:
    lines.extend(("", "Findings"))
    if not analysis.bottlenecks:
        lines.append("  none classified from available evidence")
        return
    for finding in analysis.bottlenecks[:MAX_SUMMARY_ITEMS]:
        label = terminal_text(finding.classification.replace("_", " "))
        lines.append(f"  {label} [{finding.confidence:.0%} confidence]")
        lines.append(f"    {terminal_text(finding.evidence)}")
    _append_summary_omission(lines, len(analysis.bottlenecks))


def _append_logical_failures(lines: list[str], analysis: BatchAnalysis) -> None:
    failures = tuple(
        operation
        for operation in analysis.logical_operations
        if operation.outcome == "operation_error"
        or (operation.status_code is not None and operation.status_code >= 500)
    )
    if not failures:
        return
    lines.extend(("", "Failures"))
    for operation in failures[:MAX_SUMMARY_ITEMS]:
        error = (
            operation.error_type
            or (f"status {operation.status_code}" if operation.status_code is not None else None)
            or "unknown error"
        )
        location = _caller_location(operation.caller)
        suffix = f" · {location}" if location else ""
        lines.append(
            f"  {operation.category.upper()} {operation.operation} · "
            f"{terminal_text(error)} · {_duration(operation.duration_seconds)}{suffix}"
        )
    _append_summary_omission(lines, len(failures))


def _append_logical_hotspots(lines: list[str], analysis: BatchAnalysis) -> None:
    if not analysis.logical_operation_hotspots:
        return
    lines.extend(("", "Logical operation hotspots"))
    for hotspot in analysis.logical_operation_hotspots[:MAX_SUMMARY_ITEMS]:
        calls = "call" if hotspot.operation_count == 1 else "calls"
        facts = [f"{hotspot.operation_count:,} {calls}"]
        if hotspot.failed_operation_count:
            facts.append(f"{hotspot.failed_operation_count:,} failed")
        if hotspot.unfinished_operation_count:
            facts.append(f"{hotspot.unfinished_operation_count:,} unfinished")
        facts.append(f"{_duration(hotspot.total_duration_seconds)} total")
        if hotspot.operation_count > 1:
            facts.append(f"{_duration(hotspot.max_duration_seconds)} max")
        caller = terminal_text(hotspot.caller.name) if hotspot.caller is not None else None
        if caller:
            facts.append(caller)
        lines.append(f"  {hotspot.category.upper()} {hotspot.operation} · " + " · ".join(facts))
    _append_summary_omission(lines, len(analysis.logical_operation_hotspots))


def _append_python_hotspots(lines: list[str], analysis: BatchAnalysis) -> None:
    if analysis.python_hotspots:
        application = tuple(
            hotspot for hotspot in analysis.python_hotspots if hotspot.scope == "application"
        )
        hotspots = application or analysis.python_hotspots
        lines.extend(("", "Python hotspots"))
        for hotspot in hotspots[:MAX_SUMMARY_ITEMS]:
            lines.append(
                f"  {terminal_text(hotspot.name)} · "
                f"{_duration(hotspot.self_seconds)} self · "
                f"{_duration(hotspot.total_seconds)} total"
            )
        _append_summary_omission(lines, len(hotspots))
        return
    if not analysis.python_sample_hotspots:
        return
    application = tuple(
        hotspot for hotspot in analysis.python_sample_hotspots if hotspot.scope == "application"
    )
    hotspots = application or analysis.python_sample_hotspots
    lines.extend(("", "Sampled Python hotspots"))
    for hotspot in hotspots[:MAX_SUMMARY_ITEMS]:
        lines.append(
            f"  {terminal_text(hotspot.name)} · "
            f"~{_duration(hotspot.estimated_leaf_seconds)} leaf · "
            f"~{_duration(hotspot.estimated_total_seconds)} on stack"
        )
    _append_summary_omission(lines, len(hotspots))


def _append_capture_summary(lines: list[str], analysis: BatchAnalysis) -> None:
    facts: list[str] = []
    observer = analysis.process_observer
    if observer is not None:
        facts.append(
            f"process tree {terminal_text(observer.status)} · "
            f"{_count(observer.process_count, 'process')}"
        )
    semantic = analysis.semantic_capture
    if semantic is not None:
        _append_boundary_capture(
            facts,
            "subprocesses",
            semantic.status,
            semantic.subprocess_count,
            semantic.process_count,
        )
    http = analysis.http_capture
    if http is not None:
        _append_boundary_capture(
            facts, "HTTP requests", http.status, http.request_count, http.process_count
        )
    network = analysis.network_capture
    if network is not None:
        _append_boundary_capture(
            facts,
            "network connections",
            network.status,
            network.connection_count,
            network.process_count,
        )
    setup = analysis.network_setup_capture
    if setup is not None:
        _append_boundary_capture(
            facts, "network setup phases", setup.status, setup.phase_count, setup.process_count
        )
    logical = analysis.logical_operation_capture
    if logical is not None:
        _append_boundary_capture(
            facts,
            "logical operations",
            logical.status,
            logical.operation_count,
            logical.process_count,
        )
    profile = analysis.deep_profile
    if profile is not None:
        facts.append(
            f"deep profile {terminal_text(profile.status)} · "
            f"{_count(profile.function_count, 'function')} across "
            f"{_count(profile.process_count, 'process')}"
        )
    sample = analysis.sample_profile
    if sample is not None:
        facts.append(
            f"sampling {terminal_text(sample.status)} · "
            f"{_count(sample.sample_count, 'sample')} across "
            f"{_count(sample.process_count, 'process')}"
        )
    if facts:
        lines.extend(("", "Capture"))
        lines.extend(f"  {fact}" for fact in facts)


def _append_boundary_capture(
    facts: list[str],
    label: str,
    status: str,
    count: int,
    process_count: int,
) -> None:
    if status == "complete" and count == 0:
        return
    facts.append(
        f"{label} {terminal_text(status)} · {count:,} observed across "
        f"{_count(process_count, 'process')}"
    )


def _caller_location(caller: CallerAttribution | None) -> str | None:
    if caller is None:
        return None
    filename = Path(caller.filename).name or caller.filename
    return f"{terminal_text(caller.name)} ({terminal_text(filename)}:{caller.firstlineno})"


def _count(value: int, singular: str) -> str:
    suffix = singular if value == 1 else f"{singular}s"
    return f"{value:,} {suffix}"


def _append_summary_omission(lines: list[str], total: int) -> None:
    omitted = total - MAX_SUMMARY_ITEMS
    if omitted > 0:
        lines.append(f"  … {omitted:,} more")
