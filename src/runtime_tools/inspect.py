"""Structured and terminal inspection of a runpack."""

from __future__ import annotations

import json
import shlex
from dataclasses import asdict, dataclass
from pathlib import Path

from runtime_tools.model import Event, JsonValue
from runtime_tools.storage import RunpackReader


@dataclass(frozen=True, slots=True)
class ExecutionSummary:
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
    stderr_bytes: int | None
    stderr_sha256: str | None
    record_counts: dict[str, int]

    def as_json_value(self) -> dict[str, JsonValue]:
        value = asdict(self)
        value["command"] = list(self.command)
        return value


def _nested_output(metadata: dict[str, JsonValue], stream: str, field: str) -> str | int | None:
    output = metadata.get("output")
    if not isinstance(output, dict):
        return None
    stream_data = output.get(stream)
    if not isinstance(stream_data, dict):
        return None
    value = stream_data.get(field)
    return value if isinstance(value, (str, int)) else None


def inspect_runpack(path: Path) -> ExecutionSummary:
    with RunpackReader(path) as reader:
        execution = reader.execution()
        measurements = {item.name: item.value for item in reader.measurements()}
        peak_memory = measurements.get("process.memory.peak")
        wall_time = measurements.get("process.wall_time")
        if wall_time is None and execution.finished_at_ns is not None:
            wall_time = (execution.finished_at_ns - execution.started_at_ns) / 1_000_000_000
        return ExecutionSummary(
            id=execution.id,
            name=execution.name,
            command=execution.command,
            working_directory=execution.working_directory,
            revision=execution.revision,
            started_at_ns=execution.started_at_ns,
            finished_at_ns=execution.finished_at_ns,
            exit_code=execution.exit_code,
            wall_time_seconds=wall_time,
            cpu_user_seconds=measurements.get("process.cpu.user"),
            cpu_system_seconds=measurements.get("process.cpu.system"),
            peak_memory_bytes=int(peak_memory) if peak_memory is not None else None,
            stdout_bytes=_as_int(_nested_output(execution.metadata, "stdout", "bytes")),
            stdout_sha256=_as_str(_nested_output(execution.metadata, "stdout", "sha256")),
            stderr_bytes=_as_int(_nested_output(execution.metadata, "stderr", "bytes")),
            stderr_sha256=_as_str(_nested_output(execution.metadata, "stderr", "sha256")),
            record_counts=reader.counts(),
        )


def render_causal_tree(path: Path) -> str:
    with RunpackReader(path) as reader:
        events = reader.events()
        edges = tuple(edge for edge in reader.causal_edges() if edge.kind == "parent")
        entity_names = {entity.id: entity.name for entity in reader.entities()}
        inconsistency_count = reader.clock_inconsistency_count()
    by_id = {event.id: event for event in events}
    children: dict[str, list[str]] = {event.id: [] for event in events}
    incoming: set[str] = set()
    for edge in edges:
        if edge.source_event_id in children and edge.target_event_id in by_id:
            children[edge.source_event_id].append(edge.target_event_id)
            incoming.add(edge.target_event_id)
    roots = [event.id for event in events if event.id not in incoming]

    def sort_key(event_id: str) -> tuple[int, str]:
        return (by_id[event_id].started_at_ns or -1, by_id[event_id].name)

    for child_ids in children.values():
        child_ids.sort(key=sort_key)
    roots.sort(key=sort_key)
    lines = ["CAUSAL STRUCTURE"]
    visited: set[str] = set()

    def append_event(event_id: str, depth: int) -> None:
        event = by_id[event_id]
        marker = " (cycle)" if event_id in visited else ""
        service = entity_names.get(event.entity_id or "", "unowned")
        label = f"{service} :: {event.name} [{event.kind}] {_event_duration(event)}{marker}"
        lines.append(f"{'  ' * depth}{label}")
        if marker:
            return
        visited.add(event_id)
        for child_id in children[event_id]:
            append_event(child_id, depth + 1)

    for root in roots:
        append_event(root, 0)
    for event in events:
        if event.id not in visited:
            append_event(event.id, 0)
    lines.append(f"clock inconsistencies: {inconsistency_count}")
    return "\n".join(lines)


def _event_duration(event: Event) -> str:
    if event.started_at_ns is None or event.finished_at_ns is None:
        return "duration unknown"
    return f"{(event.finished_at_ns - event.started_at_ns) / 1_000_000:.3f}ms"


def _as_int(value: str | int | None) -> int | None:
    return value if isinstance(value, int) else None


def _as_str(value: str | int | None) -> str | None:
    return value if isinstance(value, str) else None


def render_summary(summary: ExecutionSummary, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(summary.as_json_value(), indent=2, sort_keys=True)
    duration = (
        "in progress" if summary.wall_time_seconds is None else f"{summary.wall_time_seconds:.3f}s"
    )
    peak = (
        "unknown" if summary.peak_memory_bytes is None else _format_bytes(summary.peak_memory_bytes)
    )
    revision = summary.revision or "unknown"
    command = shlex.join(summary.command) if summary.command else "(telemetry import)"
    lines = [
        f"RUN {summary.name}",
        f"id:       {summary.id}",
        f"command:  {command}",
        f"revision: {revision}",
        f"outcome:  {_format_outcome(summary)}",
        f"runtime:  {duration}",
        f"cpu:      {_format_cpu(summary)}",
        f"memory:   {peak} peak",
        f"stdout:   {_format_output(summary.stdout_bytes, summary.stdout_sha256)}",
        f"stderr:   {_format_output(summary.stderr_bytes, summary.stderr_sha256)}",
        (
            "records:  "
            f"{summary.record_counts['entities']} entities, "
            f"{summary.record_counts['events']} events, "
            f"{summary.record_counts['causal_edges']} edges, "
            f"{summary.record_counts['measurements']} measurements, "
            f"{summary.record_counts['attachments']} attachments"
        ),
    ]
    return "\n".join(lines)


def _format_outcome(summary: ExecutionSummary) -> str:
    if summary.exit_code is None:
        return "in progress" if summary.finished_at_ns is None else "unknown (no exit status)"
    return "success (exit 0)" if summary.exit_code == 0 else f"failed (exit {summary.exit_code})"


def _format_cpu(summary: ExecutionSummary) -> str:
    if summary.cpu_user_seconds is None or summary.cpu_system_seconds is None:
        return "unknown"
    return f"{summary.cpu_user_seconds:.3f}s user, {summary.cpu_system_seconds:.3f}s system"


def _format_output(byte_count: int | None, digest: str | None) -> str:
    if byte_count is None or digest is None:
        return "unknown"
    return f"{byte_count} B, sha256:{digest[:12]}"


def _format_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024**2:
        return f"{value / 1024:.1f} KiB"
    return f"{value / 1024**2:.1f} MiB"
