"""Human and machine-readable RunDiff reports."""

from __future__ import annotations

import json
from collections.abc import Callable

from runtime_tools.rundiff.compare import EdgeCountChange, ExecutionDiff, ValueChange


def render_diff(diff: ExecutionDiff, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(diff.as_json_value(), indent=2, sort_keys=True)
    lines = [
        "RUNTIME DIFF",
        f"baseline:  {diff.baseline_name} ({diff.baseline_id[:8]})",
        f"candidate: {diff.candidate_name} ({diff.candidate_id[:8]})",
        f"matching:  {diff.match_level}",
        "",
        "Outcome",
        f"  {diff.outcome}",
        f"  exit status: {_equivalence(diff.exit_code_equivalent)}",
        f"  stdout:      {_equivalence(diff.output_equivalent)}",
        "",
        "Runtime",
        f"  {_format_change(diff.wall_time, _duration)}",
        "",
        "Critical path",
        f"  {_format_change(diff.critical_path, _duration)}",
        "",
        "Peak memory",
        f"  {_format_change(diff.peak_memory, _bytes)}",
    ]
    if diff.operation_count_changes:
        lines.extend(("", "Operation count changes"))
        for index, change in enumerate(diff.operation_count_changes, 1):
            percent = _percent(change.percent, change.baseline, change.candidate)
            lines.append(
                f"  {index}. {change.entity_name} :: {change.operation_name} "
                f"[{change.operation_kind}]"
            )
            lines.append(f"     {change.baseline:,} → {change.candidate:,} {percent}")
    new_edges = tuple(change for change in diff.edge_count_changes if change.change_kind == "new")
    removed_edges = tuple(
        change for change in diff.edge_count_changes if change.change_kind == "removed"
    )
    changed_edges = tuple(
        change for change in diff.edge_count_changes if change.change_kind == "changed"
    )
    _append_edges(lines, "New runtime dependencies", new_edges)
    _append_edges(lines, "Removed runtime dependencies", removed_edges)
    _append_edges(lines, "Changed runtime dependencies", changed_edges)
    if not diff.operation_count_changes and not diff.edge_count_changes:
        lines.extend(("", "No structural or operation-count changes."))
    return "\n".join(lines)


def _append_edges(lines: list[str], title: str, edges: tuple[EdgeCountChange, ...]) -> None:
    if not edges:
        return
    lines.extend(("", title))
    for edge in edges:
        lines.append(
            f"  {edge.source_name} → {edge.target_name} [{edge.relation}]: "
            f"{edge.baseline:,} → {edge.candidate:,}"
        )


def _equivalence(value: bool | None) -> str:
    if value is None:
        return "unknown"
    return "equivalent" if value else "different"


def _format_change(change: ValueChange, formatter: Callable[[float], str]) -> str:
    if change.baseline is None or change.candidate is None:
        return "unknown"
    return (
        f"{formatter(change.baseline)} → {formatter(change.candidate)} "
        f"{_percent(change.percent, change.baseline, change.candidate)}"
    )


def _percent(percent: float | None, baseline: float | int, candidate: float | int) -> str:
    if percent is None:
        return "(new)" if baseline == 0 and candidate != 0 else ""
    return f"({percent:+.1f}%)"


def _duration(value: float) -> str:
    if value < 1:
        return f"{value * 1000:.1f}ms"
    return f"{value:.3f}s"


def _bytes(value: float) -> str:
    if value < 1024:
        return f"{value:.0f} B"
    if value < 1024**2:
        return f"{value / 1024:.1f} KiB"
    return f"{value / 1024**2:.1f} MiB"
