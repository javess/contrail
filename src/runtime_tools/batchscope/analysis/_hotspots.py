"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

import math
from typing import Literal

from runtime_tools.batchscope.analysis._boundary_models import _ObservedProcessIdentity
from runtime_tools.batchscope.analysis._common import (
    MAX_PROFILE_PROCESS_CONTRIBUTIONS,
    PYTHON_EXCEPTION_CHURN_MIN_EVENTS,
    PYTHON_EXCEPTION_CHURN_MIN_EVENTS_PER_CALL,
)
from runtime_tools.batchscope.analysis._profile_models import (
    Bottleneck,
    DeepProfileSummary,
    PythonCallProcessContribution,
    PythonHotspot,
    PythonSampleHotspot,
    PythonSampleProcessContribution,
)
from runtime_tools.batchscope.analysis._values import (
    event_number as _event_number,
)
from runtime_tools.batchscope.analysis._values import (
    nonnegative_number as _nonnegative_number,
)
from runtime_tools.model import Event, JsonValue


def _process_identity(
    pid: int,
    role: Literal["root", "descendant", "unknown"],
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[str | None, str | None, bool]:
    identity = observed_processes.get(pid)
    if identity is not None:
        return identity.name, identity.parent_name, True
    return (root_process_name if role == "root" else None), None, False


def _process_role(value: JsonValue | None) -> Literal["root", "descendant", "unknown"] | None:
    if value == "root":
        return "root"
    if value == "descendant":
        return "descendant"
    if value == "unknown":
        return "unknown"
    return None


def _deep_process_contributions(
    event: Event,
    *,
    implementation: Literal["python", "native"],
    call_count: int,
    total_seconds: float,
    self_seconds: float,
    max_seconds: float,
    exception_count: int,
    non_control_flow_exception_count: int | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[
    Literal["complete", "unavailable", "invalid"],
    tuple[PythonCallProcessContribution, ...],
]:
    raw_processes = event.attributes.get("processes")
    if raw_processes is None:
        return "unavailable", ()
    if (
        not isinstance(raw_processes, list)
        or not raw_processes
        or len(raw_processes) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return "invalid", ()
    raw_process_count = event.attributes.get("process_count")
    if (
        not isinstance(raw_process_count, int)
        or isinstance(raw_process_count, bool)
        or raw_process_count != len(raw_processes)
    ):
        return "invalid", ()
    contributions: list[PythonCallProcessContribution] = []
    seen_pids: set[int] = set()
    root_count = 0
    for raw_process in raw_processes:
        if not isinstance(raw_process, dict):
            return "invalid", ()
        pid = raw_process.get("pid")
        role = _process_role(raw_process.get("role"))
        process_call_count = raw_process.get("call_count")
        process_total = _nonnegative_number(raw_process.get("total_seconds"))
        process_self = _nonnegative_number(raw_process.get("self_seconds"))
        process_max = _nonnegative_number(raw_process.get("max_seconds"))
        process_exception_count = raw_process.get("exception_count", 0)
        process_non_control_flow_exception_count = raw_process.get(
            "non_control_flow_exception_count"
        )
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or pid in seen_pids
            or role is None
            or not isinstance(process_call_count, int)
            or isinstance(process_call_count, bool)
            or process_call_count < 0
            or not isinstance(process_exception_count, int)
            or isinstance(process_exception_count, bool)
            or process_exception_count < 0
            or (implementation == "native" and process_exception_count > process_call_count)
            or (
                non_control_flow_exception_count is None
                and process_non_control_flow_exception_count is not None
            )
            or (
                non_control_flow_exception_count is not None
                and (
                    not isinstance(process_non_control_flow_exception_count, int)
                    or isinstance(process_non_control_flow_exception_count, bool)
                    or not 0 <= process_non_control_flow_exception_count <= process_exception_count
                )
            )
            or process_total is None
            or process_self is None
            or process_max is None
            or process_self > process_total
            or process_max > process_total
        ):
            return "invalid", ()
        seen_pids.add(pid)
        root_count += role == "root"
        if root_count > 1:
            return "invalid", ()
        process_name, parent_name, observed = _process_identity(
            pid,
            role,
            observed_processes,
            root_process_name,
        )
        contributions.append(
            PythonCallProcessContribution(
                pid,
                role,
                process_name,
                parent_name,
                observed,
                process_call_count,
                process_total,
                process_self,
                process_max,
                process_exception_count,
                (
                    process_non_control_flow_exception_count
                    if isinstance(process_non_control_flow_exception_count, int)
                    and not isinstance(process_non_control_flow_exception_count, bool)
                    else None
                ),
            )
        )
    if (
        sum(item.call_count for item in contributions) != call_count
        or sum(item.exception_count for item in contributions) != exception_count
        or (
            non_control_flow_exception_count is not None
            and sum(item.non_control_flow_exception_count or 0 for item in contributions)
            != non_control_flow_exception_count
        )
        or not math.isclose(
            sum(item.total_seconds for item in contributions),
            total_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
        or not math.isclose(
            sum(item.self_seconds for item in contributions),
            self_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
        or not math.isclose(
            max(item.max_seconds for item in contributions),
            max_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
    ):
        return "invalid", ()
    contributions.sort(key=lambda item: (-item.self_seconds, -item.total_seconds, item.pid))
    return "complete", tuple(contributions)


def _sample_process_contributions(
    event: Event,
    *,
    sample_count: int,
    leaf_sample_count: int,
    estimated_total_seconds: float,
    estimated_leaf_seconds: float,
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[
    Literal["complete", "unavailable", "invalid"],
    tuple[PythonSampleProcessContribution, ...],
]:
    raw_processes = event.attributes.get("processes")
    if raw_processes is None:
        return "unavailable", ()
    if (
        not isinstance(raw_processes, list)
        or not raw_processes
        or len(raw_processes) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return "invalid", ()
    raw_process_count = event.attributes.get("process_count")
    if (
        not isinstance(raw_process_count, int)
        or isinstance(raw_process_count, bool)
        or raw_process_count != len(raw_processes)
    ):
        return "invalid", ()
    contributions: list[PythonSampleProcessContribution] = []
    seen_pids: set[int] = set()
    root_count = 0
    for raw_process in raw_processes:
        if not isinstance(raw_process, dict):
            return "invalid", ()
        pid = raw_process.get("pid")
        role = _process_role(raw_process.get("role"))
        process_sample_count = raw_process.get("sample_count")
        process_leaf_count = raw_process.get("leaf_sample_count")
        process_total = _nonnegative_number(raw_process.get("estimated_total_seconds"))
        process_leaf = _nonnegative_number(raw_process.get("estimated_leaf_seconds"))
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or pid in seen_pids
            or role is None
            or not isinstance(process_sample_count, int)
            or isinstance(process_sample_count, bool)
            or process_sample_count < 0
            or not isinstance(process_leaf_count, int)
            or isinstance(process_leaf_count, bool)
            or not 0 <= process_leaf_count <= process_sample_count
            or process_total is None
            or process_leaf is None
            or process_leaf > process_total
        ):
            return "invalid", ()
        seen_pids.add(pid)
        root_count += role == "root"
        if root_count > 1:
            return "invalid", ()
        process_name, parent_name, observed = _process_identity(
            pid,
            role,
            observed_processes,
            root_process_name,
        )
        contributions.append(
            PythonSampleProcessContribution(
                pid,
                role,
                process_name,
                parent_name,
                observed,
                process_sample_count,
                process_leaf_count,
                process_total,
                process_leaf,
            )
        )
    if (
        sum(item.sample_count for item in contributions) != sample_count
        or sum(item.leaf_sample_count for item in contributions) != leaf_sample_count
        or not math.isclose(
            sum(item.estimated_total_seconds for item in contributions),
            estimated_total_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
        or not math.isclose(
            sum(item.estimated_leaf_seconds for item in contributions),
            estimated_leaf_seconds,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
    ):
        return "invalid", ()
    contributions.sort(key=lambda item: (-item.leaf_sample_count, -item.sample_count, item.pid))
    return "complete", tuple(contributions)


def _python_hotspots(
    events: tuple[Event, ...],
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[PythonHotspot, ...]:
    hotspots: list[PythonHotspot] = []
    for event in events:
        if event.kind != "python.call.aggregate":
            continue
        filename = event.attributes.get("filename")
        firstlineno = event.attributes.get("firstlineno")
        scope = event.attributes.get("scope")
        call_count = event.attributes.get("call_count")
        implementation = event.attributes.get("implementation", "python")
        exception_count = event.attributes.get("exception_count", 0)
        non_control_flow_exception_count = event.attributes.get("non_control_flow_exception_count")
        total_seconds = _event_number(event, "total_seconds")
        self_seconds = _event_number(event, "self_seconds")
        max_seconds = _event_number(event, "max_seconds")
        if (
            not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(scope, str)
            or scope not in {"application", "library", "runtime"}
            or not isinstance(call_count, int)
            or isinstance(call_count, bool)
            or call_count < 0
            or implementation not in {"python", "native"}
            or (implementation == "native") != (filename == "<native>")
            or not isinstance(exception_count, int)
            or isinstance(exception_count, bool)
            or exception_count < 0
            or (implementation == "native" and exception_count > call_count)
            or (
                non_control_flow_exception_count is not None
                and (
                    implementation != "python"
                    or not isinstance(non_control_flow_exception_count, int)
                    or isinstance(non_control_flow_exception_count, bool)
                    or not 0 <= non_control_flow_exception_count <= exception_count
                )
            )
            or total_seconds is None
            or self_seconds is None
            or max_seconds is None
        ):
            continue
        implementation_value: Literal["python", "native"] = (
            "native" if implementation == "native" else "python"
        )
        attribution_status, processes = _deep_process_contributions(
            event,
            implementation=implementation_value,
            call_count=call_count,
            total_seconds=total_seconds,
            self_seconds=self_seconds,
            max_seconds=max_seconds,
            exception_count=exception_count,
            non_control_flow_exception_count=(
                non_control_flow_exception_count
                if isinstance(non_control_flow_exception_count, int)
                and not isinstance(non_control_flow_exception_count, bool)
                else None
            ),
            observed_processes=observed_processes,
            root_process_name=root_process_name,
        )
        hotspots.append(
            PythonHotspot(
                event.name,
                filename,
                firstlineno,
                scope,
                call_count,
                total_seconds,
                self_seconds,
                max_seconds,
                attribution_status,
                processes,
                implementation_value,
                exception_count,
                (
                    non_control_flow_exception_count
                    if isinstance(non_control_flow_exception_count, int)
                    and not isinstance(non_control_flow_exception_count, bool)
                    else None
                ),
            )
        )
    hotspots.sort(
        key=lambda item: (
            item.scope != "application",
            -item.self_seconds,
            -item.total_seconds,
            item.name,
        )
    )
    return tuple(hotspots)


def _python_sample_hotspots(
    events: tuple[Event, ...],
    observed_processes: dict[int, _ObservedProcessIdentity],
    root_process_name: str | None,
) -> tuple[PythonSampleHotspot, ...]:
    hotspots: list[PythonSampleHotspot] = []
    for event in events:
        if event.kind != "python.stack.sample":
            continue
        filename = event.attributes.get("filename")
        firstlineno = event.attributes.get("firstlineno")
        scope = event.attributes.get("scope")
        sample_count = event.attributes.get("sample_count")
        leaf_sample_count = event.attributes.get("leaf_sample_count")
        estimated_total_seconds = _event_number(event, "estimated_total_seconds")
        estimated_leaf_seconds = _event_number(event, "estimated_leaf_seconds")
        if (
            not isinstance(filename, str)
            or not filename
            or not isinstance(firstlineno, int)
            or isinstance(firstlineno, bool)
            or firstlineno < 0
            or not isinstance(scope, str)
            or scope not in {"application", "library", "runtime"}
            or not isinstance(sample_count, int)
            or isinstance(sample_count, bool)
            or sample_count < 0
            or not isinstance(leaf_sample_count, int)
            or isinstance(leaf_sample_count, bool)
            or not 0 <= leaf_sample_count <= sample_count
            or estimated_total_seconds is None
            or estimated_leaf_seconds is None
        ):
            continue
        attribution_status, processes = _sample_process_contributions(
            event,
            sample_count=sample_count,
            leaf_sample_count=leaf_sample_count,
            estimated_total_seconds=estimated_total_seconds,
            estimated_leaf_seconds=estimated_leaf_seconds,
            observed_processes=observed_processes,
            root_process_name=root_process_name,
        )
        hotspots.append(
            PythonSampleHotspot(
                event.name,
                filename,
                firstlineno,
                scope,
                sample_count,
                leaf_sample_count,
                estimated_total_seconds,
                estimated_leaf_seconds,
                attribution_status,
                processes,
            )
        )
    hotspots.sort(
        key=lambda item: (
            item.scope != "application",
            -item.leaf_sample_count,
            -item.sample_count,
            item.name,
        )
    )
    return tuple(hotspots[:100])


def _python_hotspot_bottleneck(
    hotspots: tuple[PythonHotspot, ...], total_seconds: float | None
) -> Bottleneck | None:
    application = next((item for item in hotspots if item.scope == "application"), None)
    if (
        application is None
        or total_seconds is None
        or total_seconds <= 0
        or application.self_seconds / total_seconds < 0.25
    ):
        return None
    return Bottleneck(
        "python_hotspot",
        f"{application.name} accumulated {application.self_seconds:.3f}s self time "
        f"across {application.call_count} calls under intrusive deep capture",
        0.6,
    )


def _python_exception_churn_bottleneck(
    hotspots: tuple[PythonHotspot, ...],
    profile: DeepProfileSummary | None,
) -> Bottleneck | None:
    if (
        profile is None
        or profile.status != "complete"
        or profile.truncated
        or profile.dropped_call_count != 0
        or profile.python_exception_capture is None
        or profile.python_exception_capture.status != "complete"
        or profile.python_exception_capture.dropped_event_count != 0
        or profile.python_exception_capture.control_flow_filter is None
        or profile.python_exception_capture.control_flow_filter.status != "complete"
        or profile.python_exception_capture.control_flow_filter.dropped_non_control_flow_event_count
        != 0
        or profile.observer_integrity is None
        or profile.observer_integrity.status != "complete"
    ):
        return None
    python_hotspots = tuple(item for item in hotspots if item.implementation == "python")
    exception_capture = profile.python_exception_capture
    control_flow_filter = exception_capture.control_flow_filter
    assert control_flow_filter is not None
    diagnostic_hotspots = tuple(
        (item, item.non_control_flow_exception_count)
        for item in python_hotspots
        if item.non_control_flow_exception_count is not None
    )
    if (
        len(hotspots) != profile.function_count
        or sum(item.exception_count for item in python_hotspots) != exception_capture.event_count
        or sum(item.exception_count > 0 for item in python_hotspots)
        != exception_capture.function_count
        or len(diagnostic_hotspots) != len(python_hotspots)
        or sum(count for _, count in diagnostic_hotspots)
        != control_flow_filter.non_control_flow_event_count
        or sum(count > 0 for _, count in diagnostic_hotspots)
        != control_flow_filter.non_control_flow_function_count
    ):
        return None
    candidates = tuple(
        (item, count)
        for item, count in diagnostic_hotspots
        if item.scope == "application"
        and item.call_count > 0
        and count >= PYTHON_EXCEPTION_CHURN_MIN_EVENTS
        and count / item.call_count >= PYTHON_EXCEPTION_CHURN_MIN_EVENTS_PER_CALL
    )
    if not candidates:
        return None
    hotspot, non_control_flow_exception_count = min(
        candidates,
        key=lambda candidate: (
            -candidate[1],
            -(candidate[1] / candidate[0].call_count),
            -candidate[0].self_seconds,
            candidate[0].name,
        ),
    )
    events_per_call = non_control_flow_exception_count / hotspot.call_count
    return Bottleneck(
        "python_exception_churn",
        f"{hotspot.name} recorded {non_control_flow_exception_count} non-control-flow Python "
        f"exception propagation events across {hotspot.call_count} calls "
        f"({events_per_call:.2f} per call); built-in iterator completion is excluded and "
        "events are not unique failures",
        0.6,
    )


def _python_sample_hotspot_bottleneck(
    hotspots: tuple[PythonSampleHotspot, ...],
) -> Bottleneck | None:
    application = next((item for item in hotspots if item.scope == "application"), None)
    total_leaf_samples = sum(item.leaf_sample_count for item in hotspots)
    if (
        application is None
        or application.leaf_sample_count < 3
        or total_leaf_samples <= 0
        or application.leaf_sample_count / total_leaf_samples < 0.25
    ):
        return None
    return Bottleneck(
        "python_sample_hotspot",
        f"{application.name} was the leaf Python frame in "
        f"{application.leaf_sample_count} / {total_leaf_samples} thread samples",
        0.5,
    )
