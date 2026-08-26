"""Structured and terminal inspection of a runpack."""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.json_support import JsonDocumentModel
from runtime_tools.model import CausalEdge, Event, JsonValue
from runtime_tools.storage import RunpackReader
from runtime_tools.terminal import terminal_text

_MAX_TREE_INDENT_DEPTH = 40
MAX_CAUSAL_TREE_ITEMS = 10_000
_MAX_COMPLETENESS_COUNT = (1 << 63) - 1
_PROFILE_EVENT_KINDS = frozenset({"python.call.aggregate", "python.callsite"})


@dataclass(frozen=True, slots=True)
class ExecutionSummary(JsonDocumentModel):
    document_type = "runtime.inspect"

    schema_version: str
    producer_version: str
    id: str
    name: str
    command: tuple[str, ...]
    working_directory: str
    revision: str | None
    started_at_ns: int
    finished_at_ns: int | None
    exit_code: int | None
    wall_time_seconds: float | None
    cpu_user_seconds: float | None
    cpu_system_seconds: float | None
    peak_memory_bytes: int | None
    stdout_bytes: int | None
    stdout_sha256: str | None
    stdout_complete: bool | None
    stdout_relay_error: str | None
    stderr_bytes: int | None
    stderr_sha256: str | None
    stderr_complete: bool | None
    stderr_relay_error: str | None
    annotation_error: str | None
    missing_causal_references: int | None
    dropped_attribute_count: int | None
    record_counts: dict[str, int]


def _nested_output(metadata: dict[str, JsonValue], stream: str, field: str) -> str | int | None:
    output = metadata.get("output")
    if not isinstance(output, dict):
        return None
    stream_data = output.get(stream)
    if not isinstance(stream_data, dict):
        return None
    value = stream_data.get(field)
    return value if isinstance(value, (str, int)) else None


def inspect_reader(reader: RunpackReader) -> ExecutionSummary:
    """Summarize one run from the reader's stable snapshot."""
    manifest = reader.manifest()
    execution = reader.execution()
    measurements = reader.first_measurement_values(
        (
            ("process.cpu.user", "s"),
            ("process.cpu.system", "s"),
            ("process.memory.peak", "By"),
        )
    )
    peak_memory = _peak_memory_bytes(measurements.get(("process.memory.peak", "By")))
    wall_time = (
        (execution.finished_at_ns - execution.started_at_ns) / 1_000_000_000
        if execution.finished_at_ns is not None
        else None
    )
    return ExecutionSummary(
        schema_version=manifest["schema_version"],
        producer_version=manifest.get("producer_version", "unknown"),
        id=execution.id,
        name=execution.name,
        command=execution.command,
        working_directory=execution.working_directory,
        revision=execution.revision,
        started_at_ns=execution.started_at_ns,
        finished_at_ns=execution.finished_at_ns,
        exit_code=execution.exit_code,
        wall_time_seconds=wall_time,
        cpu_user_seconds=_nonnegative(measurements.get(("process.cpu.user", "s"))),
        cpu_system_seconds=_nonnegative(measurements.get(("process.cpu.system", "s"))),
        peak_memory_bytes=peak_memory,
        stdout_bytes=_as_int(_nested_output(execution.metadata, "stdout", "bytes")),
        stdout_sha256=_as_sha256(_nested_output(execution.metadata, "stdout", "sha256")),
        stdout_complete=_stream_complete(execution.metadata, "stdout"),
        stdout_relay_error=_as_nonempty_text(
            _nested_output(execution.metadata, "stdout", "relay_error")
        ),
        stderr_bytes=_as_int(_nested_output(execution.metadata, "stderr", "bytes")),
        stderr_sha256=_as_sha256(_nested_output(execution.metadata, "stderr", "sha256")),
        stderr_complete=_stream_complete(execution.metadata, "stderr"),
        stderr_relay_error=_as_nonempty_text(
            _nested_output(execution.metadata, "stderr", "relay_error")
        ),
        annotation_error=_annotation_error(execution.metadata),
        missing_causal_references=_missing_causal_references(execution.metadata),
        dropped_attribute_count=_dropped_attribute_count(execution.metadata),
        record_counts=reader.counts(),
    )


def inspect_runpack(path: Path) -> ExecutionSummary:
    with RunpackReader(path) as reader:
        return inspect_reader(reader)


def render_causal_tree_reader(reader: RunpackReader, *, raw: bool = False) -> str:
    """Render semantic causal structure, or every stored event when ``raw``."""
    events = reader.events()
    all_edges = reader.causal_edges()
    if not raw:
        events, all_edges = _semantic_causal_graph(events, all_edges)
    entity_names = {entity.id: entity.name for entity in reader.entities()}
    inconsistency_count = reader.clock_inconsistency_count()
    hierarchy_kinds = {"parent"} if raw else {"parent", "performs"}
    edges = tuple(edge for edge in all_edges if edge.kind in hierarchy_kinds)
    other_edges = tuple(edge for edge in all_edges if edge.kind not in hierarchy_kinds)
    by_id = {event.id: event for event in events}
    children: dict[str, list[str]] = {event.id: [] for event in events}
    incoming: set[str] = set()
    for edge in edges:
        if edge.source_event_id in children and edge.target_event_id in by_id:
            children[edge.source_event_id].append(edge.target_event_id)
            incoming.add(edge.target_event_id)
    roots = [event.id for event in events if event.id not in incoming]

    def sort_key(event_id: str) -> tuple[int, int, str]:
        event = by_id[event_id]
        started_at_ns = event.started_at_ns
        context_priority = 1 if not raw and event.kind == "python.callsite" else 0
        return (
            context_priority,
            -1 if started_at_ns is None else started_at_ns,
            event.name,
        )

    for child_ids in children.values():
        child_ids.sort(key=sort_key)
    roots.sort(key=sort_key)
    lines = ["CAUSAL STRUCTURE"]
    visited: set[str] = set()
    rendered_tree_items = 0
    tree_truncated = False

    def append_tree(event_id: str) -> None:
        nonlocal rendered_tree_items, tree_truncated
        active: set[str] = set()
        stack = [(event_id, 0, False)]
        while stack:
            current_id, depth, exiting = stack.pop()
            if exiting:
                active.remove(current_id)
                continue
            if rendered_tree_items >= MAX_CAUSAL_TREE_ITEMS:
                tree_truncated = True
                return
            rendered_tree_items += 1
            event = by_id[current_id]
            if current_id in active:
                marker = " (cycle)"
            elif current_id in visited:
                marker = " (already shown)"
            else:
                marker = ""
            service = terminal_text(entity_names.get(event.entity_id or "", "unowned"))
            if not raw and event.kind == "python.callsite":
                label = f"{service} :: {terminal_text(event.name)} [application callsite]{marker}"
            else:
                label = (
                    f"{service} :: {terminal_text(event.name)} "
                    f"[{terminal_text(event.kind)}] {_event_duration(event)}"
                    f"{_event_status(event) if not raw else ''}{marker}"
                )
            visible_depth = min(depth, _MAX_TREE_INDENT_DEPTH)
            depth_marker = f"… depth {depth} … " if depth > _MAX_TREE_INDENT_DEPTH else ""
            lines.append(f"{'  ' * visible_depth}{depth_marker}{label}")
            if marker:
                continue
            visited.add(current_id)
            active.add(current_id)
            stack.append((current_id, depth, True))
            stack.extend(
                (child_id, depth + 1, False) for child_id in reversed(children[current_id])
            )

    for root in roots:
        append_tree(root)
        if tree_truncated:
            break
    for event in events:
        if tree_truncated:
            break
        if event.id not in visited:
            append_tree(event.id)
    if tree_truncated:
        lines.append("  … additional causal structure omitted from text output")
    if other_edges:
        lines.append("CAUSAL LINKS")
        for edge in other_edges[:MAX_CAUSAL_TREE_ITEMS]:
            source = by_id.get(edge.source_event_id)
            target = by_id.get(edge.target_event_id)
            if source is None or target is None:
                continue
            lines.append(
                f"  {terminal_text(source.name)} → {terminal_text(target.name)} "
                f"[{terminal_text(edge.kind)}, confidence {edge.confidence:.2f}]"
            )
        omitted_links = len(other_edges) - MAX_CAUSAL_TREE_ITEMS
        if omitted_links > 0:
            lines.append(f"  … {omitted_links:,} additional links omitted from text output")
    lines.append(f"clock inconsistencies: {inconsistency_count}")
    return "\n".join(lines)


def render_causal_tree(path: Path, *, raw: bool = False) -> str:
    with RunpackReader(path) as reader:
        return render_causal_tree_reader(reader, raw=raw)


def _semantic_causal_graph(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[Event, ...], tuple[CausalEdge, ...]]:
    by_id = {event.id: event for event in events}
    visible_ids = {event.id for event in events if event.kind not in _PROFILE_EVENT_KINDS}
    for edge in edges:
        if edge.kind != "performs" or edge.target_event_id not in visible_ids:
            continue
        source = by_id.get(edge.source_event_id)
        if source is not None and source.kind == "python.callsite":
            visible_ids.add(source.id)
    visible_events = tuple(event for event in events if event.id in visible_ids)
    visible_edges = tuple(
        edge
        for edge in edges
        if edge.kind != "calls"
        and edge.source_event_id in visible_ids
        and edge.target_event_id in visible_ids
    )
    return visible_events, visible_edges


def _event_duration(event: Event) -> str:
    if event.started_at_ns is None or event.finished_at_ns is None:
        return "duration unknown"
    return f"{(event.finished_at_ns - event.started_at_ns) / 1_000_000:.3f}ms"


def _event_status(event: Event) -> str:
    if event.attributes.get("error") is not True:
        return ""
    error_type = event.attributes.get("error.type")
    label = terminal_text(error_type) if isinstance(error_type, str) else "unknown"
    return f", error {label}"


def _as_int(value: str | int | None) -> int | None:
    return (
        value
        if isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= _MAX_COMPLETENESS_COUNT
        else None
    )


def _as_sha256(value: str | int | None) -> str | None:
    if not isinstance(value, str) or len(value) != 64:
        return None
    try:
        decoded = bytes.fromhex(value)
    except ValueError:
        return None
    if len(decoded) != 32:
        return None
    return value.lower()


def _as_nonempty_text(value: str | int | None) -> str | None:
    return value if isinstance(value, str) and value else None


def _nonnegative(value: float | None) -> float | None:
    return value if value is not None and value >= 0 else None


def _peak_memory_bytes(value: float | None) -> int | None:
    if value is None or value < 0 or value > _MAX_COMPLETENESS_COUNT or not value.is_integer():
        return None
    return int(value)


def _annotation_error(metadata: dict[str, JsonValue]) -> str | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    value = capture.get("annotation_error")
    return value if isinstance(value, str) and value else None


def _missing_causal_references(metadata: dict[str, JsonValue]) -> int | None:
    otel = metadata.get("otel")
    if otel is None:
        return 0
    if not isinstance(otel, dict):
        return None
    total = 0
    for key in ("missing_parent_count", "missing_link_count", "missing_log_span_count"):
        value = otel.get(key, 0)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > _MAX_COMPLETENESS_COUNT
        ):
            return None
        total += value
        if total > _MAX_COMPLETENESS_COUNT:
            return None
    return total


def _dropped_attribute_count(metadata: dict[str, JsonValue]) -> int | None:
    otel = metadata.get("otel")
    if otel is None:
        return 0
    if not isinstance(otel, dict):
        return None
    value = otel.get("dropped_attribute_count", 0)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > _MAX_COMPLETENESS_COUNT
    ):
        return None
    return value


def _stream_complete(metadata: dict[str, JsonValue], stream: str) -> bool | None:
    output = metadata.get("output")
    if not isinstance(output, dict):
        return None
    stream_data = output.get(stream)
    if not isinstance(stream_data, dict):
        return None
    if "pipe_open_after_exit" not in stream_data:
        return True
    inherited = stream_data["pipe_open_after_exit"]
    if inherited is True:
        return False
    if inherited is False:
        return True
    return None


def render_summary(summary: ExecutionSummary, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(summary.as_json_value(), allow_nan=False, indent=2, sort_keys=True)
    duration = _format_runtime(summary)
    peak = (
        "unknown" if summary.peak_memory_bytes is None else _format_bytes(summary.peak_memory_bytes)
    )
    stdout = _format_output(summary.stdout_bytes, summary.stdout_sha256, summary.stdout_complete)
    stderr = _format_output(summary.stderr_bytes, summary.stderr_sha256, summary.stderr_complete)
    revision = summary.revision or "unknown"
    command = (
        terminal_text(shlex.join(summary.command)) if summary.command else "(telemetry import)"
    )
    lines = [
        f"RUN {terminal_text(summary.name)}",
        f"id:       {terminal_text(summary.id)}",
        f"schema:   {terminal_text(summary.schema_version)} "
        f"(producer {terminal_text(summary.producer_version)})",
        f"command:  {command}",
        f"revision: {terminal_text(revision)}",
        f"outcome:  {_format_outcome(summary)}",
        f"runtime:  {duration}",
        f"cpu:      {_format_cpu(summary)}",
        f"memory:   {peak} peak",
        f"stdout:   {stdout}",
        f"stderr:   {stderr}",
        _relay_error_summary("stdout", summary.stdout_relay_error),
        _relay_error_summary("stderr", summary.stderr_relay_error),
        (
            f"annotations: ignored ({terminal_text(summary.annotation_error)})"
            if summary.annotation_error
            else None
        ),
        _causality_summary(summary.missing_causal_references),
        _semantic_completeness_summary(summary.dropped_attribute_count),
        (
            "records:  "
            f"{summary.record_counts['entities']} entities, "
            f"{summary.record_counts['events']} events, "
            f"{summary.record_counts['causal_edges']} edges, "
            f"{summary.record_counts['measurements']} measurements, "
            f"{summary.record_counts['attachments']} attachments"
        ),
    ]
    return "\n".join(line for line in lines if line is not None)


def _relay_error_summary(stream: str, error: str | None) -> str | None:
    return f"{stream} relay: failed ({terminal_text(error)})" if error is not None else None


def _causality_summary(missing_references: int | None) -> str | None:
    if missing_references is None:
        return "causality: invalid OTLP completeness metadata"
    if missing_references:
        return f"causality: {missing_references} unresolved OTLP references"
    return None


def _semantic_completeness_summary(dropped_attributes: int | None) -> str | None:
    if dropped_attributes is None:
        return "semantics: invalid OTLP dropped-attribute metadata"
    if dropped_attributes:
        return f"semantics: {dropped_attributes} exporter-dropped OTLP attributes"
    return None


def _format_outcome(summary: ExecutionSummary) -> str:
    if summary.exit_code is None:
        return "in progress" if summary.finished_at_ns is None else "unknown (no exit status)"
    if summary.exit_code < 0:
        return f"failed (signal {-summary.exit_code})"
    return "success (exit 0)" if summary.exit_code == 0 else f"failed (exit {summary.exit_code})"


def _format_runtime(summary: ExecutionSummary) -> str:
    if summary.wall_time_seconds is not None:
        return f"{summary.wall_time_seconds:.3f}s"
    if summary.finished_at_ns is None and summary.exit_code is None:
        return "in progress"
    return "unknown"


def _format_cpu(summary: ExecutionSummary) -> str:
    if summary.cpu_user_seconds is None or summary.cpu_system_seconds is None:
        return "unknown"
    return f"{summary.cpu_user_seconds:.3f}s user, {summary.cpu_system_seconds:.3f}s system"


def _format_output(byte_count: int | None, digest: str | None, complete: bool | None) -> str:
    if byte_count is None or digest is None:
        return "unknown"
    if complete is None:
        suffix = " (completeness unknown)"
    elif complete is False:
        suffix = " (incomplete: pipe remained open after exit)"
    else:
        suffix = ""
    return f"{byte_count} B, sha256:{digest[:12]}{suffix}"


def _format_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024**2:
        return f"{value / 1024:.1f} KiB"
    return f"{value / 1024**2:.1f} MiB"
