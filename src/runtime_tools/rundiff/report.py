"""Human and machine-readable RunDiff reports."""

from __future__ import annotations

import json
from collections.abc import Callable

from runtime_tools.rundiff.compare import EdgeCountChange, ExecutionDiff, ValueChange
from runtime_tools.terminal import terminal_text

MAX_TEXT_SECTION_ITEMS = 100


def render_diff(diff: ExecutionDiff, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(diff.as_json_value(), indent=2, sort_keys=True)
    lines = [
        "RUNTIME DIFF",
        f"baseline:  {terminal_text(diff.baseline_name)} ({terminal_text(diff.baseline_id[:8])})",
        f"candidate: {terminal_text(diff.candidate_name)} ({terminal_text(diff.candidate_id[:8])})",
        f"matching:  {diff.match_level}",
    ]
    warnings: list[str] = []
    annotation_errors = (
        ("baseline", diff.baseline_annotation_error),
        ("candidate", diff.candidate_annotation_error),
    )
    warnings.extend(
        f"  {side}: annotations ignored ({terminal_text(error)})"
        for side, error in annotation_errors
        if error is not None
    )
    incomplete_streams = (
        ("baseline", diff.baseline_incomplete_streams),
        ("candidate", diff.candidate_incomplete_streams),
    )
    warnings.extend(
        f"  {side}: incomplete {', '.join(streams)} identity"
        for side, streams in incomplete_streams
        if streams
    )
    causal_references = (
        ("baseline", diff.baseline_missing_causal_references),
        ("candidate", diff.candidate_missing_causal_references),
    )
    warnings.extend(
        (
            f"  {side}: causal completeness metadata invalid"
            if count is None
            else f"  {side}: {count} unresolved causal references"
        )
        for side, count in causal_references
        if count is None or count > 0
    )
    dropped_attributes = (
        ("baseline", diff.baseline_dropped_attribute_count),
        ("candidate", diff.candidate_dropped_attribute_count),
    )
    warnings.extend(
        (
            f"  {side}: dropped-attribute metadata invalid"
            if count is None
            else f"  {side}: {count} exporter-dropped OTLP attributes"
        )
        for side, count in dropped_attributes
        if count is None or count > 0
    )
    if warnings:
        lines.extend(("", "Evidence warnings", *warnings))
    lines.extend(
        (
            "",
            "Outcome",
            f"  {diff.outcome}",
            f"  exit status: {_equivalence(diff.exit_code_equivalent)}",
            f"  stdout:      {_equivalence(diff.output_equivalent)}",
            f"  stderr:      {_equivalence(diff.stderr_equivalent)}",
            f"  op errors:   {_equivalence(diff.operation_errors_equivalent)}",
            "",
            "Runtime",
            f"  {_format_change(diff.wall_time, _duration)}",
            "",
            "CPU time",
            f"  {_format_change(diff.cpu_time, _duration)}",
            "",
            (
                "Critical path "
                f"({_certainty(diff.baseline_critical_path_certainty)} → "
                f"{_certainty(diff.candidate_critical_path_certainty)})"
            ),
            f"  {_format_change(diff.critical_path, _duration)}",
            "",
            "Peak memory",
            f"  {_format_change(diff.peak_memory, _bytes)}",
        )
    )
    if diff.entity_count_changes:
        lines.extend(("", "Entity changes"))
        lines.extend(
            f"  {terminal_text(change.entity_name)} [{terminal_text(change.entity_kind)}]: "
            f"{change.baseline:,} → {change.candidate:,} ({change.change_kind})"
            for change in diff.entity_count_changes[:MAX_TEXT_SECTION_ITEMS]
        )
        _append_omitted(lines, len(diff.entity_count_changes))
    if diff.operation_count_changes:
        lines.extend(("", "Operation count changes"))
        for index, change in enumerate(diff.operation_count_changes[:MAX_TEXT_SECTION_ITEMS], 1):
            percent = _percent(change.percent, change.baseline, change.candidate)
            lines.append(
                f"  {index}. {terminal_text(change.entity_name)} :: "
                f"{terminal_text(change.operation_name)} "
                f"[{terminal_text(change.operation_kind)}]"
            )
            lines.append(f"     {change.baseline:,} → {change.candidate:,} {percent}")
        _append_omitted(lines, len(diff.operation_count_changes))
    if diff.operation_error_count_changes:
        lines.extend(("", "Failed operation changes"))
        for index, change in enumerate(
            diff.operation_error_count_changes[:MAX_TEXT_SECTION_ITEMS], 1
        ):
            percent = _percent(change.percent, change.baseline, change.candidate)
            lines.append(
                f"  {index}. {terminal_text(change.entity_name)} :: "
                f"{terminal_text(change.operation_name)} "
                f"[{terminal_text(change.operation_kind)}]"
            )
            lines.append(f"     {change.baseline:,} → {change.candidate:,} {percent}")
        _append_omitted(lines, len(diff.operation_error_count_changes))
    if diff.operation_concurrency_changes:
        lines.extend(("", "Observed max concurrency changes"))
        for index, concurrency_change in enumerate(
            diff.operation_concurrency_changes[:MAX_TEXT_SECTION_ITEMS], 1
        ):
            percent = _percent(
                concurrency_change.percent,
                concurrency_change.baseline,
                concurrency_change.candidate,
            )
            lines.append(
                f"  {index}. {terminal_text(concurrency_change.entity_name)} :: "
                f"{terminal_text(concurrency_change.operation_name)} "
                f"[{terminal_text(concurrency_change.operation_kind)}]"
            )
            lines.append(
                f"     {concurrency_change.baseline:,} → {concurrency_change.candidate:,} {percent}"
            )
        _append_omitted(lines, len(diff.operation_concurrency_changes))
    if diff.operation_duration_changes:
        lines.extend(("", "Aggregate operation duration changes"))
        for index, duration_change in enumerate(
            diff.operation_duration_changes[:MAX_TEXT_SECTION_ITEMS], 1
        ):
            percent = _percent(
                duration_change.percent,
                duration_change.baseline_seconds,
                duration_change.candidate_seconds,
            )
            lines.append(
                f"  {index}. {terminal_text(duration_change.entity_name)} :: "
                f"{terminal_text(duration_change.operation_name)} "
                f"[{terminal_text(duration_change.operation_kind)}]"
            )
            lines.append(
                f"     {_duration(duration_change.baseline_seconds)} → "
                f"{_duration(duration_change.candidate_seconds)} {percent}"
            )
        _append_omitted(lines, len(diff.operation_duration_changes))
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
    if diff.environment_changes:
        lines.extend(("", "Environment changes"))
        lines.extend(
            f"  {terminal_text(change.variable)}: {change.change_kind}"
            for change in diff.environment_changes[:MAX_TEXT_SECTION_ITEMS]
        )
        _append_omitted(lines, len(diff.environment_changes))
    if (
        not diff.entity_count_changes
        and not diff.operation_count_changes
        and not diff.operation_error_count_changes
        and not diff.operation_concurrency_changes
        and not diff.operation_duration_changes
        and not diff.edge_count_changes
    ):
        lines.extend(
            (
                "",
                "No entity, structural, error, concurrency, duration, or operation-count changes.",
            )
        )
    return "\n".join(lines)


def _append_edges(lines: list[str], title: str, edges: tuple[EdgeCountChange, ...]) -> None:
    if not edges:
        return
    lines.extend(("", title))
    for edge in edges[:MAX_TEXT_SECTION_ITEMS]:
        lines.append(
            f"  {terminal_text(edge.source_name)} → {terminal_text(edge.target_name)} "
            f"[{terminal_text(edge.relation)}]: "
            f"{edge.baseline:,} → {edge.candidate:,}"
        )
    _append_omitted(lines, len(edges))


def _append_omitted(lines: list[str], total: int) -> None:
    omitted = total - MAX_TEXT_SECTION_ITEMS
    if omitted > 0:
        lines.append(f"  … {omitted:,} additional items omitted from text output")


def _equivalence(value: bool | None) -> str:
    if value is None:
        return "unknown"
    return "equivalent" if value else "different"


def _certainty(value: str | None) -> str:
    return value or "unavailable"


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
