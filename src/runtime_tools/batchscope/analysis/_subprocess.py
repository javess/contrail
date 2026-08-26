"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

from runtime_tools.batchscope.analysis._boundary_models import (
    CallerAttribution,
    SubprocessCall,
    _ObservedProcessIdentity,
)
from runtime_tools.batchscope.analysis._common import MAX_SUBPROCESS_CALL_SUMMARIES
from runtime_tools.batchscope.analysis._profile_models import SemanticCaptureSummary
from runtime_tools.batchscope.analysis._profile_summary import (
    _caller_attribution_counts,
    _capture_status,
    _semantic_count,
)
from runtime_tools.model import CausalEdge, Event, JsonValue


@dataclass(frozen=True, slots=True)
class _CallerContract:
    source: str
    count_attribute: str
    relations: tuple[tuple[str | None, str], ...]

    def targets(self, event: Event) -> bool:
        kinds = tuple(kind for kind, _ in self.relations if kind is not None)
        if kinds:
            return event.kind in kinds
        return event.kind != "python.callsite" and event.attributes.get("source") == self.source

    def relation_for(self, event: Event) -> str | None:
        for kind, relation in self.relations:
            if kind is None or kind == event.kind:
                return relation
        return None


_SUBPROCESS_CALLER_CONTRACT = _CallerContract(
    "python-subprocess-wrapper", "subprocess_count", (("subprocess.run", "launches"),)
)


_HTTP_CALLER_CONTRACT = _CallerContract(
    "python-http-client-wrapper", "http_request_count", (("http.client.request", "requests"),)
)


_NETWORK_CALLER_CONTRACT = _CallerContract(
    "python-network-connection-wrapper",
    "network_connection_count",
    (("network.connect", "connects"),),
)


_NETWORK_SETUP_CALLER_CONTRACT = _CallerContract(
    "python-network-setup-wrapper",
    "network_setup_phase_count",
    (("network.resolve", "resolves"), ("network.tls_handshake", "handshakes")),
)


_LOGICAL_OPERATION_CALLER_CONTRACT = _CallerContract(
    "python-logical-operation-wrapper", "logical_operation_count", ((None, "performs"),)
)


def _operation_callers(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
    contract: _CallerContract,
) -> tuple[dict[str, CallerAttribution], int]:
    by_id = {event.id: event for event in events}
    targets = {event.id: event for event in events if contract.targets(event)}
    callers: dict[str, CallerAttribution] = {}
    invalid_targets: set[str] = set()
    for target_id, event in targets.items():
        caller_event_id = event.attributes.get("caller_event_id")
        if caller_event_id is not None and (
            not isinstance(caller_event_id, str) or not caller_event_id
        ):
            invalid_targets.add(target_id)
    for edge in edges:
        source = by_id.get(edge.source_event_id)
        target = by_id.get(edge.target_event_id)
        if source is None or source.kind != "python.callsite":
            continue
        if target is None or target.id not in targets:
            continue
        if edge.kind != contract.relation_for(target):
            invalid_targets.add(target.id)
            continue
        module = source.attributes.get("module")
        qualname = source.attributes.get("qualname")
        filename = source.attributes.get("filename")
        firstlineno = source.attributes.get("firstlineno")
        declared_count = source.attributes.get(contract.count_attribute)
        raw_scope = source.attributes.get("scope")
        raw_observation = edge.attributes.get("observation")
        expected_confidence = 1.0 if raw_observation == "exact" else 0.9
        if (
            target.id in callers
            or target.attributes.get("caller_event_id") != source.id
            or source.attributes.get("source") != contract.source
            or source.attributes.get("arguments_captured") is not False
            or source.attributes.get("locals_captured") is not False
            or not isinstance(module, str)
            or not module
            or not isinstance(qualname, str)
            or not qualname
            or not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(declared_count, int)
            or isinstance(declared_count, bool)
            or declared_count <= 0
            or raw_scope not in {"application", "library", "runtime"}
            or raw_observation not in {"exact", "sampled"}
            or edge.attributes.get("source") != contract.source
            or not math.isclose(edge.confidence, expected_confidence)
            or source.name != f"{module}.{qualname}"
        ):
            invalid_targets.add(target.id)
            callers.pop(target.id, None)
            continue
        scope: Literal["application", "library", "runtime"]
        if raw_scope == "application":
            scope = "application"
        elif raw_scope == "library":
            scope = "library"
        else:
            scope = "runtime"
        observation: Literal["exact", "sampled"] = (
            "exact" if raw_observation == "exact" else "sampled"
        )
        callers[target.id] = CallerAttribution(
            source.id,
            source.name,
            module,
            qualname,
            filename,
            firstlineno,
            scope,
            observation,
            edge.confidence,
        )
    targets_by_caller: dict[str, list[str]] = {}
    for target_id, caller in callers.items():
        targets_by_caller.setdefault(caller.event_id, []).append(target_id)
    for caller_event_id, target_ids in targets_by_caller.items():
        source = by_id[caller_event_id]
        if source.attributes.get(contract.count_attribute) != len(target_ids):
            invalid_targets.update(target_ids)
    for target_id, event in targets.items():
        if event.attributes.get("caller_event_id") is not None and target_id not in callers:
            invalid_targets.add(target_id)
    for target_id in invalid_targets:
        callers.pop(target_id, None)
    return callers, len(invalid_targets)


def _subprocess_calls(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> tuple[tuple[SubprocessCall, ...], int, int, int, int]:
    caller_by_subprocess, invalid_caller_count = _operation_callers(
        events, edges, _SUBPROCESS_CALLER_CONTRACT
    )
    calls: list[SubprocessCall] = []
    invalid_event_count = 0
    for event in events:
        if event.kind != "subprocess.run":
            continue
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        child_pid = event.attributes.get("child_pid")
        shell = event.attributes.get("shell")
        raw_outcome = event.attributes.get("outcome")
        exit_code = event.attributes.get("exit_code")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        if (
            not event.name
            or event.started_at_ns is None
            or event.attributes.get("source") != "python-subprocess-wrapper"
            or event.attributes.get("arguments_captured") is not False
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or (
                child_pid is not None
                and (
                    not isinstance(child_pid, int) or isinstance(child_pid, bool) or child_pid <= 0
                )
            )
            or (shell is not None and not isinstance(shell, bool))
            or raw_outcome not in {"exited", "launch_error", "unknown"}
            or (
                exit_code is not None
                and (not isinstance(exit_code, int) or isinstance(exit_code, bool))
            )
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
        ):
            invalid_event_count += 1
            continue
        role: Literal["root", "descendant", "unknown"]
        if raw_role == "root":
            role = "root"
        elif raw_role == "descendant":
            role = "descendant"
        else:
            role = "unknown"
        outcome: Literal["exited", "launch_error", "unknown"]
        if raw_outcome == "exited":
            outcome = "exited"
        elif raw_outcome == "launch_error":
            outcome = "launch_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "exited"
                and (
                    event.finished_at_ns is None
                    or child_pid is None
                    or exit_code is None
                    or (exit_code == 0 and (error_type is not None or error is True))
                    or (exit_code != 0 and (error_type != "subprocess_exit" or error is not True))
                )
            )
            or (
                outcome == "launch_error"
                and (
                    event.finished_at_ns is None
                    or child_pid is not None
                    or exit_code is not None
                    or error_type is None
                    or error is not True
                )
            )
            or (
                outcome == "unknown"
                and (
                    event.finished_at_ns is not None
                    or child_pid is None
                    or exit_code is not None
                    or error_type is not None
                    or error is True
                )
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        calls.append(
            SubprocessCall(
                event.id,
                event.name,
                parent_pid,
                role,
                child_pid,
                child_pid in observed_processes if child_pid is not None else False,
                shell,
                outcome,
                exit_code,
                error_type,
                event.started_at_ns,
                duration_seconds,
                caller_by_subprocess.get(event.id),
            )
        )
    calls.sort(key=lambda call: (call.started_at_ns, call.parent_pid, call.event_id))
    return (
        tuple(calls[:MAX_SUBPROCESS_CALL_SUMMARIES]),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_subprocess.values()}),
        len(caller_by_subprocess),
    )


def _semantic_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    subprocess_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_subprocess_count: int,
    invalid_caller_count: int,
) -> SemanticCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    semantic = instrumentation.get("semantic_capture")
    if not isinstance(semantic, dict):
        return None
    status = _capture_status(semantic.get("status"))
    raw_counts = {
        key: _semantic_count(semantic.get(key))
        for key in (
            "process_count",
            "subprocess_count",
            "dropped_subprocess_count",
            "callback_error_count",
        )
    }
    declared_subprocess_count = raw_counts["subprocess_count"]
    if (
        semantic.get("observer") != "python-subprocess-wrapper"
        or semantic.get("zero_code") is not True
        or semantic.get("arguments_captured") is not False
        or semantic.get("environment_captured") is not False
        or semantic.get("working_directory_captured") is not False
        or any(value is None for value in raw_counts.values())
        or declared_subprocess_count != subprocess_event_count
        or invalid_event_count
    ):
        status = "invalid"
    caller = _caller_attribution_counts(
        semantic.get("caller_attribution"),
        target_name="subprocess",
        target_count=subprocess_event_count,
        caller_count=caller_count,
        attributed_count=attributed_subprocess_count,
        invalid_count=invalid_caller_count,
    )
    return SemanticCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_subprocess_count or 0,
        raw_counts["dropped_subprocess_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller.status,
        caller.caller_count,
        caller.attributed_count,
        caller.unattributed_count,
        caller.invalid_count,
        caller.callback_error_count,
    )
