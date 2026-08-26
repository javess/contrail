"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import math
import time
from dataclasses import replace
from typing import Literal

from runtime_tools.deep_profile._aggregation import (
    _aggregate_deep_functions_and_select_edges,
    _aggregate_sample_functions_and_select_edges,
    _deep_edge,
    _deep_functions,
    _deep_process_contributions,
    _event_id,
    _function_ranking_key,
    _profile_result,
    _sample_edge,
    _sample_event_id,
    _sample_functions,
    _sample_process_contributions,
    _select_deep_function_keys,
    _select_sample_function_keys,
    _validate_root_process_id,
)
from runtime_tools.deep_profile._common import DeepProfileError, _bounded_sum
from runtime_tools.deep_profile._evidence import _FunctionIdentity
from runtime_tools.deep_profile._logical import _boundary_evidence
from runtime_tools.deep_profile._parsing import (
    _observer_integrity,
    _parse_document,
    _profile_payloads,
    _python_exception_filter,
    _registration_only,
    _snapshot_kind,
)
from runtime_tools.deep_profile._ranking import _EdgeAggregate, _SampleEdgeAggregate
from runtime_tools.deep_profile._result import DeepProfileResult
from runtime_tools.deep_profile._session import (
    DeepProfileSession,
    _boolean,
    _integer,
    _list,
    _PublicationMetricsAccumulator,
    _snapshot_transport_stats,
)
from runtime_tools.model import CausalEdge, Event


def load_deep_profile(
    session: DeepProfileSession,
    *,
    entity_id: str,
    root_process_id: int | None = None,
) -> DeepProfileResult:
    if session.mode != "deep":
        raise DeepProfileError("deep-profile loader requires a deep-profile session")
    normalization_started_ns = time.perf_counter_ns()
    session.finish_collection()
    _validate_root_process_id(root_process_id)
    truncated = False
    dropped_call_count = 0
    dropped_edge_count = 0
    dropped_exception_event_count = 0
    dropped_non_control_flow_exception_event_count = 0
    callback_error_count = 0
    open_call_count = 0
    observer_integrity_process_count = 0
    profile_hook_setter_call_count = 0
    profile_hook_setter_process_count = 0
    trace_hook_setter_call_count = 0
    trace_hook_setter_process_count = 0
    python_exception_filter_process_count = 0
    payloads, dropped_file_count = _profile_payloads(session, root_process_id)
    transport_stats = _snapshot_transport_stats(session, dropped_file_count)
    truncated = transport_stats.dropped_profile_process_count > 0
    process_ids: set[int] = set()
    checkpoint_process_ids: set[int] = set()
    registration_only_process_ids: set[int] = set()
    publication_accumulator = _PublicationMetricsAccumulator()
    for payload in payloads:
        document = _parse_document(payload)
        if document.get("format_version") != 1:
            raise DeepProfileError("generated deep-profile format version is unsupported")
        if document.get("mode", "deep") != "deep":
            raise DeepProfileError("generated deep-profile mode is unsupported")
        pid = _integer(document.get("pid"), "deep-profile process id")
        if pid <= 0:
            raise DeepProfileError("deep-profile process id must be positive")
        if pid in process_ids:
            raise DeepProfileError("deep-profile process ids must be unique")
        process_ids.add(pid)
        snapshot_kind = _snapshot_kind(document, "deep-profile")
        registration_only = _registration_only(document, "deep-profile", snapshot_kind)
        publication_accumulator.add(
            document,
            pid=pid,
            snapshot_kind=snapshot_kind,
            registration_only=registration_only,
        )
        if snapshot_kind == "checkpoint":
            checkpoint_process_ids.add(pid)
        if registration_only:
            registration_only_process_ids.add(pid)
        document_truncated = _boolean(document.get("truncated"), "deep-profile truncation")
        document_dropped_call_count = _integer(
            document.get("dropped_call_count"), "deep-profile dropped call count"
        )
        document_dropped_edge_count = _integer(
            document.get("dropped_edge_count"), "deep-profile dropped edge count"
        )
        document_dropped_exception_event_count = _integer(
            document.get("dropped_exception_event_count", 0),
            "deep-profile dropped Python exception event count",
        )
        document_exception_filter = _python_exception_filter(document)
        if document_exception_filter:
            python_exception_filter_process_count += 1
            document_dropped_non_control_flow_exception_event_count = _integer(
                document.get("dropped_non_control_flow_exception_event_count"),
                "deep-profile dropped Python non-control-flow exception event count",
            )
            if (
                document_dropped_non_control_flow_exception_event_count
                > document_dropped_exception_event_count
            ):
                raise DeepProfileError(
                    "deep-profile dropped Python exception counts are inconsistent"
                )
        else:
            if "dropped_non_control_flow_exception_event_count" in document:
                raise DeepProfileError(
                    "deep-profile Python exception filter evidence is inconsistent"
                )
            document_dropped_non_control_flow_exception_event_count = 0
        document_callback_error_count = _integer(
            document.get("callback_error_count"), "deep-profile callback error count"
        )
        document_open_call_count = _integer(
            document.get("open_call_count", 0), "deep-profile open call count"
        )
        observer_integrity = _observer_integrity(document)
        document_profile_hook_setter_call_count = 0
        document_trace_hook_setter_call_count = 0
        if observer_integrity is not None:
            observer_integrity_process_count += 1
            (
                document_profile_hook_setter_call_count,
                document_trace_hook_setter_call_count,
            ) = observer_integrity
            profile_hook_setter_call_count = _bounded_sum(
                profile_hook_setter_call_count,
                document_profile_hook_setter_call_count,
                "deep-profile profile-hook setter call count",
            )
            trace_hook_setter_call_count = _bounded_sum(
                trace_hook_setter_call_count,
                document_trace_hook_setter_call_count,
                "deep-profile trace-hook setter call count",
            )
            profile_hook_setter_process_count += document_profile_hook_setter_call_count > 0
            trace_hook_setter_process_count += document_trace_hook_setter_call_count > 0
        raw_functions = _list(document.get("functions"), "deep-profile functions")
        raw_edges = _list(document.get("edges"), "deep-profile edges")
        if registration_only and (
            document_truncated
            or document_dropped_call_count
            or document_dropped_edge_count
            or document_dropped_exception_event_count
            or document_dropped_non_control_flow_exception_event_count
            or document_callback_error_count
            or document_open_call_count
            or document_profile_hook_setter_call_count
            or document_trace_hook_setter_call_count
            or raw_functions
            or raw_edges
        ):
            raise DeepProfileError("deep-profile registration must not contain profile evidence")
        truncated = document_truncated or truncated
        dropped_call_count = _bounded_sum(
            dropped_call_count,
            document_dropped_call_count,
            "deep-profile dropped call count",
        )
        dropped_edge_count = _bounded_sum(
            dropped_edge_count,
            document_dropped_edge_count,
            "deep-profile dropped edge count",
        )
        dropped_exception_event_count = _bounded_sum(
            dropped_exception_event_count,
            document_dropped_exception_event_count,
            "deep-profile dropped Python exception event count",
        )
        dropped_non_control_flow_exception_event_count = _bounded_sum(
            dropped_non_control_flow_exception_event_count,
            document_dropped_non_control_flow_exception_event_count,
            "deep-profile dropped Python non-control-flow exception event count",
        )
        callback_error_count = _bounded_sum(
            callback_error_count,
            document_callback_error_count,
            "deep-profile callback error count",
        )
        open_call_count = _bounded_sum(
            open_call_count,
            document_open_call_count,
            "deep-profile open call count",
        )
        by_identifier = _deep_functions(
            raw_functions,
            exception_filter=document_exception_filter,
        )
        for raw_edge in raw_edges:
            _deep_edge(raw_edge, by_identifier)

    boundary_evidence = _boundary_evidence(
        payloads, entity_id=entity_id, root_process_id=root_process_id
    )
    publication_metrics = publication_accumulator.result()
    selected_function_keys, function_ranking_bytes = _select_deep_function_keys(
        payloads,
        session.directory,
    )
    (
        functions,
        selection_dropped_call_count,
        selection_dropped_exception_event_count,
        selection_dropped_non_control_flow_exception_event_count,
        selected_edge_keys,
        edge_ranking_bytes,
    ) = _aggregate_deep_functions_and_select_edges(
        payloads,
        selected_function_keys,
        session.directory,
    )
    if selection_dropped_call_count:
        truncated = True
        dropped_call_count = _bounded_sum(
            dropped_call_count,
            selection_dropped_call_count,
            "deep-profile dropped call count",
        )
    if selection_dropped_exception_event_count:
        truncated = True
        dropped_exception_event_count = _bounded_sum(
            dropped_exception_event_count,
            selection_dropped_exception_event_count,
            "deep-profile dropped Python exception event count",
        )
    if selection_dropped_non_control_flow_exception_event_count:
        dropped_non_control_flow_exception_event_count = _bounded_sum(
            dropped_non_control_flow_exception_event_count,
            selection_dropped_non_control_flow_exception_event_count,
            "deep-profile dropped Python non-control-flow exception event count",
        )
    edges: dict[tuple[_FunctionIdentity, _FunctionIdentity], _EdgeAggregate] = {}
    for payload in payloads:
        document = _parse_document(payload)
        by_identifier = _deep_functions(_list(document.get("functions"), "deep-profile functions"))
        for raw_edge in _list(document.get("edges"), "deep-profile edges"):
            source, target, call_count, total_ns = _deep_edge(raw_edge, by_identifier)
            key = (source, target)
            source_key = _function_ranking_key(source)[0]
            target_key = _function_ranking_key(target)[0]
            if (
                source == target
                or source_key not in selected_function_keys
                or target_key not in selected_function_keys
            ):
                continue
            if source_key + target_key not in selected_edge_keys:
                truncated = True
                dropped_edge_count = _bounded_sum(
                    dropped_edge_count,
                    call_count,
                    "deep-profile dropped edge count",
                )
                continue
            aggregate_edge = edges.get(key)
            if aggregate_edge is None:
                aggregate_edge = _EdgeAggregate()
                edges[key] = aggregate_edge
            aggregate_edge.call_count = _bounded_sum(
                aggregate_edge.call_count,
                call_count,
                "deep-profile edge call count",
            )
            aggregate_edge.total_ns = _bounded_sum(
                aggregate_edge.total_ns,
                total_ns,
                "deep-profile edge total time",
            )
    profile_events = tuple(
        Event(
            id=_event_id(identity),
            kind="python.call.aggregate",
            name=identity.name,
            entity_id=entity_id,
            started_at_ns=None,
            finished_at_ns=None,
            clock_domain=None,
            uncertainty_ns=None,
            sequence=None,
            attributes={
                "source": "deep-profile",
                "scope": identity.scope,
                "module": identity.module,
                "qualname": identity.qualname,
                "filename": identity.filename,
                "firstlineno": identity.firstlineno,
                "implementation": "native" if identity.native else "python",
                "call_count": aggregate.call_count,
                "exception_count": aggregate.exception_count,
                **(
                    {
                        "non_control_flow_exception_count": (
                            aggregate.non_control_flow_exception_count
                        )
                    }
                    if python_exception_filter_process_count == len(process_ids)
                    and process_ids
                    and not identity.native
                    else {}
                ),
                "total_seconds": aggregate.total_ns / 1_000_000_000,
                "self_seconds": aggregate.self_ns / 1_000_000_000,
                "max_seconds": aggregate.max_ns / 1_000_000_000,
                "process_count": len(aggregate.processes or ()),
                "processes": _deep_process_contributions(
                    aggregate,
                    root_process_id,
                    include_non_control_flow_exceptions=(
                        python_exception_filter_process_count == len(process_ids)
                        and bool(process_ids)
                        and not identity.native
                    ),
                ),
            },
        )
        for identity, aggregate in sorted(functions.items(), key=lambda item: item[0].name)
    )
    normalized_events = profile_events + boundary_evidence.events
    event_ids = {identity: _event_id(identity) for identity in functions}
    profile_edges = tuple(
        CausalEdge(
            event_ids[source],
            event_ids[target],
            "calls",
            1.0,
            {
                "source": "deep-profile",
                "call_count": aggregate.call_count,
                "total_seconds": aggregate.total_ns / 1_000_000_000,
            },
        )
        for (source, target), aggregate in sorted(
            edges.items(), key=lambda item: (item[0][0].name, item[0][1].name)
        )
    )
    normalized_edges = profile_edges + boundary_evidence.edges
    if any(
        not math.isfinite(value)
        for event in profile_events
        for value in (
            event.attributes["total_seconds"],
            event.attributes["self_seconds"],
            event.attributes["max_seconds"],
        )
        if isinstance(value, float)
    ):
        raise DeepProfileError("deep-profile normalized timings exceed the numeric range")
    normalization_duration_ns = max(0, time.perf_counter_ns() - normalization_started_ns)
    observer_integrity_status: Literal["complete", "partial", "unavailable"]
    if observer_integrity_process_count == 0:
        observer_integrity_status = "unavailable"
    elif (
        observer_integrity_process_count != len(process_ids)
        or profile_hook_setter_call_count
        or trace_hook_setter_call_count
    ):
        observer_integrity_status = "partial"
    else:
        observer_integrity_status = "complete"
    result = _profile_result(
        session,
        boundary_evidence,
        events=normalized_events,
        edges=normalized_edges,
        process_ids=process_ids,
        truncated=truncated,
        dropped_call_count=dropped_call_count,
        dropped_edge_count=dropped_edge_count,
        callback_error_count=callback_error_count,
        checkpoint_process_count=len(checkpoint_process_ids),
        registration_only_process_count=len(registration_only_process_ids),
        transport_stats=transport_stats,
        publication_metrics=publication_metrics,
        normalization_duration_ns=normalization_duration_ns,
        ranking_database_peak_bytes=max(function_ranking_bytes, edge_ranking_bytes),
    )
    return replace(
        result,
        native_function_count=sum(identity.native for identity in functions),
        native_call_count=sum(
            aggregate.call_count for identity, aggregate in functions.items() if identity.native
        ),
        native_exception_count=sum(
            aggregate.exception_count
            for identity, aggregate in functions.items()
            if identity.native
        ),
        python_exception_function_count=sum(
            not identity.native and aggregate.exception_count > 0
            for identity, aggregate in functions.items()
        ),
        python_exception_event_count=sum(
            aggregate.exception_count
            for identity, aggregate in functions.items()
            if not identity.native
        ),
        dropped_python_exception_event_count=dropped_exception_event_count,
        python_exception_filter_status=(
            "complete"
            if python_exception_filter_process_count == len(process_ids) and process_ids
            else "unavailable"
        ),
        python_non_control_flow_exception_function_count=sum(
            not identity.native and aggregate.non_control_flow_exception_count > 0
            for identity, aggregate in functions.items()
        ),
        python_non_control_flow_exception_event_count=sum(
            aggregate.non_control_flow_exception_count
            for identity, aggregate in functions.items()
            if not identity.native
        ),
        dropped_python_non_control_flow_exception_event_count=(
            dropped_non_control_flow_exception_event_count
        ),
        observer_integrity_status=observer_integrity_status,
        observer_integrity_process_count=observer_integrity_process_count,
        profile_hook_setter_call_count=profile_hook_setter_call_count,
        profile_hook_setter_process_count=profile_hook_setter_process_count,
        trace_hook_setter_call_count=trace_hook_setter_call_count,
        trace_hook_setter_process_count=trace_hook_setter_process_count,
        open_call_count=open_call_count,
    )


def load_sample_profile(
    session: DeepProfileSession,
    *,
    entity_id: str,
    root_process_id: int | None = None,
) -> DeepProfileResult:
    if session.mode != "sample":
        raise DeepProfileError("sample-profile loader requires a sample-profile session")
    normalization_started_ns = time.perf_counter_ns()
    session.finish_collection()
    _validate_root_process_id(root_process_id)
    truncated = False
    dropped_frame_sample_count = 0
    dropped_edge_sample_count = 0
    callback_error_count = 0
    sample_count = 0
    thread_sample_count = 0
    interval_ns = 0
    payloads, dropped_file_count = _profile_payloads(session, root_process_id)
    transport_stats = _snapshot_transport_stats(session, dropped_file_count)
    truncated = transport_stats.dropped_profile_process_count > 0
    process_ids: set[int] = set()
    checkpoint_process_ids: set[int] = set()
    registration_only_process_ids: set[int] = set()
    publication_accumulator = _PublicationMetricsAccumulator()
    for payload in payloads:
        document = _parse_document(payload)
        if document.get("format_version") != 1:
            raise DeepProfileError("generated sample-profile format version is unsupported")
        if document.get("mode") != "sample":
            raise DeepProfileError("generated sample-profile mode is unsupported")
        pid = _integer(document.get("pid"), "sample-profile process id")
        if pid <= 0:
            raise DeepProfileError("sample-profile process id must be positive")
        if pid in process_ids:
            raise DeepProfileError("sample-profile process ids must be unique")
        process_ids.add(pid)
        snapshot_kind = _snapshot_kind(document, "sample-profile")
        registration_only = _registration_only(document, "sample-profile", snapshot_kind)
        publication_accumulator.add(
            document,
            pid=pid,
            snapshot_kind=snapshot_kind,
            registration_only=registration_only,
        )
        if snapshot_kind == "checkpoint":
            checkpoint_process_ids.add(pid)
        if registration_only:
            registration_only_process_ids.add(pid)
        document_interval_ns = _integer(document.get("interval_ns"), "sample-profile interval")
        if document_interval_ns <= 0 or document_interval_ns > 1_000_000_000:
            raise DeepProfileError("sample-profile interval is unsupported")
        if interval_ns and document_interval_ns != interval_ns:
            raise DeepProfileError("sample-profile intervals must be consistent")
        interval_ns = document_interval_ns
        document_truncated = _boolean(document.get("truncated"), "sample-profile truncation")
        document_sample_count = _integer(
            document.get("sample_count"), "sample-profile sample count"
        )
        document_thread_sample_count = _integer(
            document.get("thread_sample_count"),
            "sample-profile thread sample count",
        )
        document_dropped_frame_sample_count = _integer(
            document.get("dropped_frame_sample_count"),
            "sample-profile dropped frame sample count",
        )
        document_dropped_edge_sample_count = _integer(
            document.get("dropped_edge_sample_count"),
            "sample-profile dropped edge sample count",
        )
        document_callback_error_count = _integer(
            document.get("callback_error_count"),
            "sample-profile callback error count",
        )
        raw_functions = _list(document.get("functions"), "sample-profile functions")
        raw_edges = _list(document.get("edges"), "sample-profile edges")
        if registration_only and (
            document_truncated
            or document_sample_count
            or document_thread_sample_count
            or document_dropped_frame_sample_count
            or document_dropped_edge_sample_count
            or document_callback_error_count
            or raw_functions
            or raw_edges
        ):
            raise DeepProfileError("sample-profile registration must not contain sample evidence")
        truncated = document_truncated or truncated
        sample_count = _bounded_sum(
            sample_count,
            document_sample_count,
            "sample-profile sample count",
        )
        thread_sample_count = _bounded_sum(
            thread_sample_count,
            document_thread_sample_count,
            "sample-profile thread sample count",
        )
        dropped_frame_sample_count = _bounded_sum(
            dropped_frame_sample_count,
            document_dropped_frame_sample_count,
            "sample-profile dropped frame sample count",
        )
        dropped_edge_sample_count = _bounded_sum(
            dropped_edge_sample_count,
            document_dropped_edge_sample_count,
            "sample-profile dropped edge sample count",
        )
        callback_error_count = _bounded_sum(
            callback_error_count,
            document_callback_error_count,
            "sample-profile callback error count",
        )
        by_identifier = _sample_functions(raw_functions)
        for raw_edge in raw_edges:
            _sample_edge(raw_edge, by_identifier)

    boundary_evidence = _boundary_evidence(
        payloads, entity_id=entity_id, root_process_id=root_process_id
    )
    publication_metrics = publication_accumulator.result()
    selected_function_keys, function_ranking_bytes = _select_sample_function_keys(
        payloads,
        session.directory,
    )
    functions, selection_dropped_sample_count, selected_edge_keys, edge_ranking_bytes = (
        _aggregate_sample_functions_and_select_edges(
            payloads,
            selected_function_keys,
            session.directory,
        )
    )
    if selection_dropped_sample_count:
        truncated = True
        dropped_frame_sample_count = _bounded_sum(
            dropped_frame_sample_count,
            selection_dropped_sample_count,
            "sample-profile dropped frame sample count",
        )
    edges: dict[tuple[_FunctionIdentity, _FunctionIdentity], _SampleEdgeAggregate] = {}
    for payload in payloads:
        document = _parse_document(payload)
        by_identifier = _sample_functions(
            _list(document.get("functions"), "sample-profile functions")
        )
        for raw_edge in _list(document.get("edges"), "sample-profile edges"):
            source, target, edge_sample_count = _sample_edge(raw_edge, by_identifier)
            key = (source, target)
            source_key = _function_ranking_key(source)[0]
            target_key = _function_ranking_key(target)[0]
            if (
                source == target
                or source_key not in selected_function_keys
                or target_key not in selected_function_keys
            ):
                continue
            if source_key + target_key not in selected_edge_keys:
                truncated = True
                dropped_edge_sample_count = _bounded_sum(
                    dropped_edge_sample_count,
                    edge_sample_count,
                    "sample-profile dropped edge sample count",
                )
                continue
            aggregate_edge = edges.get(key)
            if aggregate_edge is None:
                aggregate_edge = _SampleEdgeAggregate()
                edges[key] = aggregate_edge
            aggregate_edge.sample_count = _bounded_sum(
                aggregate_edge.sample_count,
                edge_sample_count,
                "sample-profile edge sample count",
            )
    interval_seconds = interval_ns / 1_000_000_000
    profile_events = tuple(
        Event(
            id=_sample_event_id(identity),
            kind="python.stack.sample",
            name=identity.name,
            entity_id=entity_id,
            started_at_ns=None,
            finished_at_ns=None,
            clock_domain=None,
            uncertainty_ns=None,
            sequence=None,
            attributes={
                "source": "python-sampler",
                "scope": identity.scope,
                "module": identity.module,
                "qualname": identity.qualname,
                "filename": identity.filename,
                "firstlineno": identity.firstlineno,
                "sample_count": aggregate.sample_count,
                "leaf_sample_count": aggregate.leaf_sample_count,
                "estimated_total_seconds": aggregate.sample_count * interval_seconds,
                "estimated_leaf_seconds": aggregate.leaf_sample_count * interval_seconds,
                "process_count": len(aggregate.processes or ()),
                "interval_seconds": interval_seconds,
                "processes": _sample_process_contributions(
                    aggregate,
                    root_process_id,
                    interval_seconds,
                ),
            },
        )
        for identity, aggregate in sorted(functions.items(), key=lambda item: item[0].name)
    )
    normalized_events = profile_events + boundary_evidence.events
    event_ids = {identity: _sample_event_id(identity) for identity in functions}
    profile_edges = tuple(
        CausalEdge(
            event_ids[source],
            event_ids[target],
            "stack_parent",
            1.0,
            {
                "source": "python-sampler",
                "sample_count": aggregate.sample_count,
                "estimated_seconds": aggregate.sample_count * interval_seconds,
            },
        )
        for (source, target), aggregate in sorted(
            edges.items(), key=lambda item: (item[0][0].name, item[0][1].name)
        )
    )
    normalized_edges = profile_edges + boundary_evidence.edges
    if any(
        not math.isfinite(value)
        for event in profile_events
        for value in (
            event.attributes["estimated_total_seconds"],
            event.attributes["estimated_leaf_seconds"],
        )
        if isinstance(value, float)
    ):
        raise DeepProfileError("sample-profile normalized estimates exceed the numeric range")
    normalization_duration_ns = max(0, time.perf_counter_ns() - normalization_started_ns)
    result = _profile_result(
        session,
        boundary_evidence,
        events=normalized_events,
        edges=normalized_edges,
        process_ids=process_ids,
        truncated=truncated,
        dropped_call_count=dropped_frame_sample_count,
        dropped_edge_count=dropped_edge_sample_count,
        callback_error_count=callback_error_count,
        checkpoint_process_count=len(checkpoint_process_ids),
        registration_only_process_count=len(registration_only_process_ids),
        transport_stats=transport_stats,
        publication_metrics=publication_metrics,
        normalization_duration_ns=normalization_duration_ns,
        ranking_database_peak_bytes=max(function_ranking_bytes, edge_ranking_bytes),
    )
    return replace(
        result,
        mode="sample",
        sample_count=sample_count,
        thread_sample_count=thread_sample_count,
        interval_ns=interval_ns,
    )


def load_python_profile(
    session: DeepProfileSession,
    *,
    entity_id: str,
    root_process_id: int | None = None,
) -> DeepProfileResult:
    if session.mode == "sample":
        return load_sample_profile(
            session,
            entity_id=entity_id,
            root_process_id=root_process_id,
        )
    return load_deep_profile(
        session,
        entity_id=entity_id,
        root_process_id=root_process_id,
    )
