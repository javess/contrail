"""Shared formatting and bounded-section helpers for BatchScope reports."""

from __future__ import annotations

from runtime_tools.batchscope.analysis import BatchAnalysis
from runtime_tools.batchscope.analysis._profile_models import (
    ProfileNormalizationMetrics,
    ProfilePublicationMetrics,
    ProfileSnapshotMetrics,
    PythonHotspot,
    PythonProfileCoverage,
    PythonSampleHotspot,
)
from runtime_tools.deep_profile import MAX_DEEP_PROFILE_FILES
from runtime_tools.terminal import terminal_text

MAX_TEXT_SECTION_ITEMS = 100
MAX_TEXT_PROCESS_CONTRIBUTIONS = 5


def _append_observation(lines: list[str], analysis: BatchAnalysis) -> None:
    throughput = analysis.throughput
    if throughput is None:
        lines.append("  no explicit progress evidence")
        return
    if throughput.compute_finished_at_ns is None:
        lines.append("  progress evidence did not identify a compute boundary")
        return
    lines.append(
        "  compute completed with "
        f"{_number(throughput.remaining_at_compute_completion)} / "
        f"{_number(throughput.total)} work items remaining"
    )
    if throughput.post_compute_seconds is not None:
        lines.append(
            f"  {_duration(throughput.post_compute_seconds)} of post-compute wall time followed"
        )


def _append_omitted(lines: list[str], total: int) -> None:
    omitted = total - MAX_TEXT_SECTION_ITEMS
    if omitted > 0:
        lines.append(f"  … {omitted:,} additional items omitted from text output")


def _append_profile_process_coverage(
    lines: list[str],
    coverage: PythonProfileCoverage,
    *,
    observer_name: str,
    process_observer_status: str | None,
) -> None:
    if coverage.status == "unavailable":
        lines.append(
            "  process coverage unavailable: process-tree evidence or reporter IDs missing"
        )
        return
    if coverage.status == "invalid":
        lines.append("  process coverage invalid: reporter IDs were inconsistent")
        return
    observed_count = coverage.observed_python_process_count
    assert observed_count is not None
    if coverage.status == "complete" and observed_count == 0:
        lines.append("  process coverage complete: no observed Python processes")
        return
    lines.append(
        f"  process coverage {coverage.status}: {coverage.matched_process_count:,} / "
        f"{observed_count:,} observed Python processes reported"
    )
    for gap in coverage.unprofiled_processes[:MAX_TEXT_PROCESS_CONTRIBUTIONS]:
        parent = f", parent {terminal_text(gap.parent_name)}" if gap.parent_name is not None else ""
        lines.append(
            f"    {terminal_text(gap.process_name)} (pid {gap.pid}{parent}) "
            f"did not load the {observer_name}"
        )
    rendered_gap_count = min(
        len(coverage.unprofiled_processes),
        MAX_TEXT_PROCESS_CONTRIBUTIONS,
    )
    omitted = coverage.unprofiled_process_count - rendered_gap_count
    if omitted > 0:
        lines.append(f"    … {omitted:,} additional unprofiled processes omitted")
    if coverage.unobserved_profile_process_ids:
        pids = ", ".join(
            str(pid)
            for pid in coverage.unobserved_profile_process_ids[:MAX_TEXT_PROCESS_CONTRIBUTIONS]
        )
        omitted_pids = len(coverage.unobserved_profile_process_ids) - MAX_TEXT_PROCESS_CONTRIBUTIONS
        suffix = f", plus {omitted_pids:,} more" if omitted_pids > 0 else ""
        lines.append(
            f"    reporting processes not observed in the process tree: pids {pids}{suffix}"
        )
    if (
        coverage.status == "partial"
        and coverage.unprofiled_process_count == 0
        and not coverage.unobserved_profile_process_ids
        and process_observer_status is not None
    ):
        lines.append(
            "    process coverage is partial because process-tree observation was "
            f"{terminal_text(process_observer_status)}"
        )


def _append_profile_transport(
    lines: list[str],
    *,
    transport: str,
    first_checkpoint_delay_seconds: float,
    checkpoint_interval_seconds: float,
    checkpoint_process_count: int,
    registration_only_process_count: int,
    dropped_profile_process_count: int,
    dropped_profile_process_count_truncated: bool,
    collector_error_count: int,
    snapshot_metrics: ProfileSnapshotMetrics,
    publication_metrics: ProfilePublicationMetrics,
    normalization_metrics: ProfileNormalizationMetrics,
) -> None:
    if transport != "unknown":
        if transport == "controller-unix-socket":
            label = "controller-side Unix socket"
        elif transport == "mixed":
            label = "controller socket with retained workload-file fallback"
        else:
            label = "workload-side atomic file fallback"
        cadence = f"every {_duration(checkpoint_interval_seconds)}"
        if first_checkpoint_delay_seconds > 0:
            cadence = (
                f"first evidence after {_duration(first_checkpoint_delay_seconds)}, then {cadence}"
            )
        lines.append(f"  snapshots: {label}, {cadence}")
    if snapshot_metrics.status == "available":
        lines.append(
            f"  snapshot transport: {snapshot_metrics.message_count:,} messages, "
            f"{_bytes(float(snapshot_metrics.payload_bytes))} total, largest "
            f"{_bytes(float(snapshot_metrics.max_payload_bytes))}"
        )
        if snapshot_metrics.checkpoint_message_count:
            lines.append(
                f"  evidence checkpoints: {snapshot_metrics.checkpoint_message_count:,}, "
                f"{_bytes(float(snapshot_metrics.checkpoint_payload_bytes))} transferred; "
                f"serialization {_duration(snapshot_metrics.checkpoint_serialization_seconds)} "
                f"total, max {_duration(snapshot_metrics.max_checkpoint_serialization_seconds)}"
            )
    elif snapshot_metrics.status == "invalid":
        lines.append("  snapshot transport metrics were invalid and were ignored")
    if publication_metrics.status == "available" and publication_metrics.fallback_process_count:
        process_label = (
            "process" if publication_metrics.fallback_process_count == 1 else "processes"
        )
        line = (
            "  retained snapshot fallback: "
            f"{publication_metrics.fallback_process_count:,} {process_label}"
        )
        if publication_metrics.socket_attempted_process_count:
            attempt_label = (
                "process"
                if publication_metrics.socket_attempted_process_count == 1
                else "processes"
            )
            line += (
                "; controller socket attempted by "
                f"{publication_metrics.socket_attempted_process_count:,} {attempt_label}, "
                f"failed after {_duration(publication_metrics.socket_failure_seconds)} total, "
                f"max {_duration(publication_metrics.max_socket_failure_seconds)}"
            )
        unavailable_count = (
            publication_metrics.fallback_process_count
            - publication_metrics.socket_attempted_process_count
        )
        if unavailable_count:
            line += f"; controller socket unavailable for {unavailable_count:,} " + (
                "process" if unavailable_count == 1 else "processes"
            )
        lines.append(line)
    elif publication_metrics.status == "invalid":
        lines.append("  snapshot publication metrics were invalid and were ignored")
    if normalization_metrics.status == "available":
        lines.append(
            f"  controller normalization: {_duration(normalization_metrics.duration_seconds)}; "
            "ranking database peak "
            f"{_bytes(float(normalization_metrics.ranking_database_peak_bytes))} / "
            f"{_bytes(float(normalization_metrics.ranking_database_limit_bytes))} limit"
        )
    elif normalization_metrics.status == "invalid":
        lines.append("  profile normalization metrics were invalid and were ignored")
    if registration_only_process_count:
        process_label = "process" if registration_only_process_count == 1 else "processes"
        lines.append(
            f"  registration-only: {registration_only_process_count:,} {process_label} loaded "
            "capture but ended before the first periodic checkpoint; no Python hotspot "
            "evidence was retained"
        )
    evidence_checkpoint_process_count = checkpoint_process_count - registration_only_process_count
    if evidence_checkpoint_process_count > 0:
        process_label = "process" if evidence_checkpoint_process_count == 1 else "processes"
        lines.append(
            f"  checkpoint-only: {evidence_checkpoint_process_count:,} {process_label} ended "
            "without a final report; using the latest checkpoint"
        )
    if dropped_profile_process_count:
        qualifier = "at least " if dropped_profile_process_count_truncated else ""
        process_label = "report" if dropped_profile_process_count == 1 else "reports"
        lines.append(
            f"  process limit: {qualifier}{dropped_profile_process_count:,} profile process "
            f"{process_label} omitted after the {MAX_DEEP_PROFILE_FILES:,}-process bound"
        )
    if collector_error_count:
        lines.append(
            f"  snapshot collector errors: {collector_error_count:,}; final-file fallback remains"
        )


def _process_label(
    *,
    pid: int,
    role: str,
    process_name: str | None,
    parent_name: str | None,
    observed_in_process_tree: bool,
) -> str:
    name = terminal_text(process_name or "unmatched process")
    details = [f"pid {pid}", role]
    if parent_name is not None:
        details.append(f"parent {terminal_text(parent_name)}")
    if not observed_in_process_tree:
        details.append("not observed in process tree")
    return f"{name} ({', '.join(details)})"


def _append_call_process_contributions(lines: list[str], hotspot: PythonHotspot) -> None:
    if hotspot.process_attribution_status != "complete":
        lines.append(f"    process attribution: {hotspot.process_attribution_status}")
        return
    lines.append("    by process:")
    for process in hotspot.processes[:MAX_TEXT_PROCESS_CONTRIBUTIONS]:
        label = _process_label(
            pid=process.pid,
            role=process.role,
            process_name=process.process_name,
            parent_name=process.parent_name,
            observed_in_process_tree=process.observed_in_process_tree,
        )
        lines.append(
            f"      {label}: self {_duration(process.self_seconds)}, "
            f"total {_duration(process.total_seconds)}, {process.call_count:,} calls"
        )
    omitted = len(hotspot.processes) - MAX_TEXT_PROCESS_CONTRIBUTIONS
    if omitted > 0:
        lines.append(f"      … {omitted:,} additional process contributors omitted")


def _append_sample_process_contributions(lines: list[str], hotspot: PythonSampleHotspot) -> None:
    if hotspot.process_attribution_status != "complete":
        lines.append(f"    process attribution: {hotspot.process_attribution_status}")
        return
    lines.append("    by process:")
    for process in hotspot.processes[:MAX_TEXT_PROCESS_CONTRIBUTIONS]:
        label = _process_label(
            pid=process.pid,
            role=process.role,
            process_name=process.process_name,
            parent_name=process.parent_name,
            observed_in_process_tree=process.observed_in_process_tree,
        )
        lines.append(
            f"      {label}: leaf {process.leaf_sample_count:,} samples "
            f"(~{_duration(process.estimated_leaf_seconds)}), "
            f"on stack {process.sample_count:,} "
            f"(~{_duration(process.estimated_total_seconds)})"
        )
    omitted = len(hotspot.processes) - MAX_TEXT_PROCESS_CONTRIBUTIONS
    if omitted > 0:
        lines.append(f"      … {omitted:,} additional process contributors omitted")


def _duration(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.001:
        return f"{value * 1_000_000:.1f}µs"
    if value < 1:
        return f"{value * 1000:.1f}ms"
    return f"{value:.3f}s"


def _rate(value: float | None) -> str:
    return "unknown" if value is None else f"{value:,.2f}/s"


def _number(value: float | None) -> str:
    return "unknown" if value is None else f"{value:g}"


def _bytes(value: float) -> str:
    if value < 1_024:
        return f"{value:.0f} B"
    if value < 1_024 * 1_024:
        return f"{value / 1_024:.1f} KiB"
    return f"{value / (1_024 * 1_024):.1f} MiB"
