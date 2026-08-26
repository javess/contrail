"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from runtime_tools.deep_profile._common import (
    _MAX_INTEGER,
    MAX_DEEP_PROFILE_EDGES,
    MAX_DEEP_PROFILE_FUNCTIONS,
    DeepProfileError,
    _bounded_sum,
    _DeepFunctionValues,
    _process_role,
)
from runtime_tools.deep_profile._evidence import _BoundaryEvidence, _FunctionIdentity
from runtime_tools.deep_profile._parsing import _parse_document
from runtime_tools.deep_profile._ranking import (
    _AggregateRanker,
    _FunctionAggregate,
    _SampleFunctionAggregate,
)
from runtime_tools.deep_profile._result import DeepProfileResult
from runtime_tools.deep_profile._session import (
    DeepProfileSession,
    _boolean,
    _integer,
    _list,
    _object,
    _profile_transport,
    _PublicationMetrics,
    _SnapshotTransportStats,
    _text,
)
from runtime_tools.model import CausalEdge, Event, JsonValue


def _function_identity(
    value: object,
) -> tuple[int, _FunctionIdentity, _DeepFunctionValues]:
    item = _object(value, "deep-profile function")
    identifier = _integer(item.get("id"), "deep-profile function id")
    scope = _text(item.get("scope"), "deep-profile function scope")
    if scope not in {"application", "library", "runtime"}:
        raise DeepProfileError("deep-profile function scope is unsupported")
    identity = _FunctionIdentity(
        _text(item.get("module"), "deep-profile function module"),
        _text(item.get("qualname"), "deep-profile function qualname"),
        _text(item.get("filename"), "deep-profile function filename"),
        _integer(item.get("firstlineno"), "deep-profile function first line"),
        scope,
    )
    native = _boolean(item.get("native", False), "deep-profile native function marker")
    if native != identity.native:
        raise DeepProfileError("deep-profile native function identity is inconsistent")
    values = (
        _integer(item.get("call_count"), "deep-profile function call count"),
        _integer(item.get("total_ns"), "deep-profile function total time"),
        _integer(item.get("self_ns"), "deep-profile function self time"),
        _integer(item.get("max_ns"), "deep-profile function maximum time"),
        _integer(item.get("exception_count", 0), "deep-profile function exception count"),
        _integer(
            item.get("non_control_flow_exception_count", 0),
            "deep-profile function non-control-flow exception count",
        ),
    )
    if (
        values[2] > values[1]
        or values[3] > values[1]
        or values[5] > values[4]
        or (native and (values[4] > values[0] or values[5] != 0))
    ):
        raise DeepProfileError("deep-profile function timings are inconsistent")
    return identifier, identity, values


def _sample_function_identity(
    value: object,
) -> tuple[int, _FunctionIdentity, tuple[int, int]]:
    item = _object(value, "sample-profile function")
    identifier = _integer(item.get("id"), "sample-profile function id")
    scope = _text(item.get("scope"), "sample-profile function scope")
    if scope not in {"application", "library", "runtime"}:
        raise DeepProfileError("sample-profile function scope is unsupported")
    identity = _FunctionIdentity(
        _text(item.get("module"), "sample-profile function module"),
        _text(item.get("qualname"), "sample-profile function qualname"),
        _text(item.get("filename"), "sample-profile function filename"),
        _integer(item.get("firstlineno"), "sample-profile function first line"),
        scope,
    )
    values = (
        _integer(item.get("sample_count"), "sample-profile function sample count"),
        _integer(item.get("leaf_sample_count"), "sample-profile function leaf sample count"),
    )
    if values[1] > values[0]:
        raise DeepProfileError("sample-profile function sample counts are inconsistent")
    return identifier, identity, values


def _function_ranking_key(identity: _FunctionIdentity) -> tuple[bytes, bytes]:
    encoded = json.dumps(
        [
            identity.module,
            identity.qualname,
            identity.filename,
            identity.firstlineno,
            identity.scope,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).digest(), encoded


def _edge_ranking_key(
    edge: tuple[_FunctionIdentity, _FunctionIdentity],
) -> bytes:
    source, target = edge
    return _function_ranking_key(source)[0] + _function_ranking_key(target)[0]


def _deep_functions(
    raw_functions: list[object],
    *,
    exception_filter: bool | None = None,
) -> dict[int, tuple[_FunctionIdentity, _DeepFunctionValues]]:
    by_identifier: dict[int, tuple[_FunctionIdentity, _DeepFunctionValues]] = {}
    for raw_function in raw_functions:
        item = _object(raw_function, "deep-profile function")
        if exception_filter is not None and (
            ("non_control_flow_exception_count" in item) != exception_filter
        ):
            raise DeepProfileError("deep-profile Python exception filter evidence is inconsistent")
        identifier, identity, values = _function_identity(raw_function)
        if identifier in by_identifier:
            raise DeepProfileError("deep-profile function ids must be unique per process")
        by_identifier[identifier] = (identity, values)
    return by_identifier


def _sample_functions(
    raw_functions: list[object],
) -> dict[int, tuple[_FunctionIdentity, tuple[int, int]]]:
    by_identifier: dict[int, tuple[_FunctionIdentity, tuple[int, int]]] = {}
    for raw_function in raw_functions:
        identifier, identity, values = _sample_function_identity(raw_function)
        if identifier in by_identifier:
            raise DeepProfileError("sample-profile function ids must be unique per process")
        by_identifier[identifier] = (identity, values)
    return by_identifier


def _deep_edge(
    value: object,
    functions: dict[int, tuple[_FunctionIdentity, _DeepFunctionValues]],
) -> tuple[_FunctionIdentity, _FunctionIdentity, int, int]:
    edge = _object(value, "deep-profile edge")
    source_entry = functions.get(_integer(edge.get("source_id"), "deep-profile edge source"))
    target_entry = functions.get(_integer(edge.get("target_id"), "deep-profile edge target"))
    if source_entry is None or target_entry is None:
        raise DeepProfileError("deep-profile edge references an unknown function")
    return (
        source_entry[0],
        target_entry[0],
        _integer(edge.get("call_count"), "deep-profile edge call count"),
        _integer(edge.get("total_ns"), "deep-profile edge total time"),
    )


def _sample_edge(
    value: object,
    functions: dict[int, tuple[_FunctionIdentity, tuple[int, int]]],
) -> tuple[_FunctionIdentity, _FunctionIdentity, int]:
    edge = _object(value, "sample-profile edge")
    source_entry = functions.get(_integer(edge.get("source_id"), "sample-profile edge source"))
    target_entry = functions.get(_integer(edge.get("target_id"), "sample-profile edge target"))
    if source_entry is None or target_entry is None:
        raise DeepProfileError("sample-profile edge references an unknown function")
    return (
        source_entry[0],
        target_entry[0],
        _integer(edge.get("sample_count"), "sample-profile edge sample count"),
    )


def _select_deep_function_keys(
    payloads: tuple[bytes, ...],
    workspace_directory: Path,
) -> tuple[frozenset[bytes], int]:
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            functions = _deep_functions(_list(document.get("functions"), "deep-profile functions"))
            for identity, values in functions.values():
                key, collision_identity = _function_ranking_key(identity)
                ranker.add(
                    key,
                    collision_identity,
                    values[2],
                    values[1],
                    "deep-profile aggregate function rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_FUNCTIONS)
        return selected, ranker.database_bytes()


def _select_sample_function_keys(
    payloads: tuple[bytes, ...],
    workspace_directory: Path,
) -> tuple[frozenset[bytes], int]:
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            functions = _sample_functions(
                _list(document.get("functions"), "sample-profile functions")
            )
            for identity, values in functions.values():
                key, collision_identity = _function_ranking_key(identity)
                ranker.add(
                    key,
                    collision_identity,
                    values[1],
                    values[0],
                    "sample-profile aggregate function rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_FUNCTIONS)
        return selected, ranker.database_bytes()


def _aggregate_deep_functions_and_select_edges(
    payloads: tuple[bytes, ...],
    selected_function_keys: frozenset[bytes],
    workspace_directory: Path,
) -> tuple[
    dict[_FunctionIdentity, _FunctionAggregate],
    int,
    int,
    int,
    frozenset[bytes],
    int,
]:
    functions: dict[_FunctionIdentity, _FunctionAggregate] = {}
    dropped_call_count = 0
    dropped_exception_event_count = 0
    dropped_non_control_flow_exception_event_count = 0
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            pid = _integer(document.get("pid"), "deep-profile process id")
            by_identifier = _deep_functions(
                _list(document.get("functions"), "deep-profile functions")
            )
            for identity, values in by_identifier.values():
                function_key = _function_ranking_key(identity)[0]
                if function_key not in selected_function_keys:
                    dropped_call_count = _bounded_sum(
                        dropped_call_count,
                        values[0],
                        "deep-profile dropped call count",
                    )
                    if not identity.native:
                        dropped_exception_event_count = _bounded_sum(
                            dropped_exception_event_count,
                            values[4],
                            "deep-profile dropped Python exception event count",
                        )
                        dropped_non_control_flow_exception_event_count = _bounded_sum(
                            dropped_non_control_flow_exception_event_count,
                            values[5],
                            "deep-profile dropped Python non-control-flow exception event count",
                        )
                    continue
                aggregate = functions.get(identity)
                if aggregate is None:
                    aggregate = _FunctionAggregate()
                    functions[identity] = aggregate
                aggregate.add(*values, pid)
            for raw_edge in _list(document.get("edges"), "deep-profile edges"):
                source, target, call_count, total_ns = _deep_edge(raw_edge, by_identifier)
                source_key = _function_ranking_key(source)[0]
                target_key = _function_ranking_key(target)[0]
                if (
                    source == target
                    or source_key not in selected_function_keys
                    or target_key not in selected_function_keys
                ):
                    continue
                edge_key = source_key + target_key
                ranker.add(
                    edge_key,
                    edge_key,
                    total_ns,
                    call_count,
                    "deep-profile aggregate edge rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_EDGES)
        return (
            functions,
            dropped_call_count,
            dropped_exception_event_count,
            dropped_non_control_flow_exception_event_count,
            selected,
            ranker.database_bytes(),
        )


def _aggregate_sample_functions_and_select_edges(
    payloads: tuple[bytes, ...],
    selected_function_keys: frozenset[bytes],
    workspace_directory: Path,
) -> tuple[
    dict[_FunctionIdentity, _SampleFunctionAggregate],
    int,
    frozenset[bytes],
    int,
]:
    functions: dict[_FunctionIdentity, _SampleFunctionAggregate] = {}
    dropped_sample_count = 0
    with _AggregateRanker(workspace_directory) as ranker:
        for payload in payloads:
            document = _parse_document(payload)
            pid = _integer(document.get("pid"), "sample-profile process id")
            by_identifier = _sample_functions(
                _list(document.get("functions"), "sample-profile functions")
            )
            for identity, values in by_identifier.values():
                function_key = _function_ranking_key(identity)[0]
                if function_key not in selected_function_keys:
                    dropped_sample_count = _bounded_sum(
                        dropped_sample_count,
                        values[0],
                        "sample-profile dropped frame sample count",
                    )
                    continue
                aggregate = functions.get(identity)
                if aggregate is None:
                    aggregate = _SampleFunctionAggregate()
                    functions[identity] = aggregate
                aggregate.add(*values, pid)
            for raw_edge in _list(document.get("edges"), "sample-profile edges"):
                source, target, edge_sample_count = _sample_edge(raw_edge, by_identifier)
                source_key = _function_ranking_key(source)[0]
                target_key = _function_ranking_key(target)[0]
                if (
                    source == target
                    or source_key not in selected_function_keys
                    or target_key not in selected_function_keys
                ):
                    continue
                edge_key = source_key + target_key
                ranker.add(
                    edge_key,
                    edge_key,
                    edge_sample_count,
                    0,
                    "sample-profile aggregate edge rank",
                )
        selected = ranker.selected_keys(MAX_DEEP_PROFILE_EDGES)
        return functions, dropped_sample_count, selected, ranker.database_bytes()


def _event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"deep:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _sample_event_id(identity: _FunctionIdentity) -> str:
    value = "\0".join(
        (
            identity.module,
            identity.qualname,
            identity.filename,
            str(identity.firstlineno),
            identity.scope,
        )
    )
    return f"sample:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _validate_root_process_id(root_process_id: int | None) -> None:
    if root_process_id is None:
        return
    if (
        not isinstance(root_process_id, int)
        or isinstance(root_process_id, bool)
        or root_process_id <= 0
        or root_process_id > _MAX_INTEGER
    ):
        raise DeepProfileError("profile root process id must be a positive integer")


def _deep_process_contributions(
    aggregate: _FunctionAggregate,
    root_process_id: int | None,
    *,
    include_non_control_flow_exceptions: bool,
) -> list[JsonValue]:
    return [
        {
            "pid": pid,
            "role": _process_role(pid, root_process_id),
            "call_count": process.call_count,
            "total_seconds": process.total_ns / 1_000_000_000,
            "self_seconds": process.self_ns / 1_000_000_000,
            "max_seconds": process.max_ns / 1_000_000_000,
            "exception_count": process.exception_count,
            **(
                {"non_control_flow_exception_count": (process.non_control_flow_exception_count)}
                if include_non_control_flow_exceptions
                else {}
            ),
        }
        for pid, process in sorted((aggregate.processes or {}).items())
    ]


def _sample_process_contributions(
    aggregate: _SampleFunctionAggregate,
    root_process_id: int | None,
    interval_seconds: float,
) -> list[JsonValue]:
    return [
        {
            "pid": pid,
            "role": _process_role(pid, root_process_id),
            "sample_count": process.sample_count,
            "leaf_sample_count": process.leaf_sample_count,
            "estimated_total_seconds": process.sample_count * interval_seconds,
            "estimated_leaf_seconds": process.leaf_sample_count * interval_seconds,
        }
        for pid, process in sorted((aggregate.processes or {}).items())
    ]


def _profile_result(
    session: DeepProfileSession,
    boundary: _BoundaryEvidence,
    *,
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
    process_ids: set[int],
    truncated: bool,
    dropped_call_count: int,
    dropped_edge_count: int,
    callback_error_count: int,
    checkpoint_process_count: int,
    registration_only_process_count: int,
    transport_stats: _SnapshotTransportStats,
    publication_metrics: _PublicationMetrics,
    normalization_duration_ns: int,
    ranking_database_peak_bytes: int,
) -> DeepProfileResult:
    semantic = boundary.semantic
    http = boundary.http
    network = boundary.network
    network_setup = boundary.network_setup
    logical = boundary.logical_operation
    return DeepProfileResult(
        events=events,
        edges=edges,
        process_count=len(process_ids),
        process_ids=tuple(sorted(process_ids)),
        truncated=truncated,
        dropped_call_count=dropped_call_count,
        dropped_edge_count=dropped_edge_count,
        callback_error_count=callback_error_count,
        checkpoint_process_count=checkpoint_process_count,
        registration_only_process_count=registration_only_process_count,
        dropped_profile_process_count=transport_stats.dropped_profile_process_count,
        dropped_profile_process_count_truncated=(
            transport_stats.dropped_profile_process_count_truncated
        ),
        transport=_profile_transport(session, publication_metrics),
        collector_error_count=session.collector_error_count,
        snapshot_metrics_status=transport_stats.metrics_status,
        snapshot_message_count=transport_stats.message_count,
        snapshot_payload_bytes=transport_stats.payload_bytes,
        max_snapshot_payload_bytes=transport_stats.max_payload_bytes,
        snapshot_serialization_ns=transport_stats.serialization_ns,
        max_snapshot_serialization_ns=transport_stats.max_serialization_ns,
        checkpoint_snapshot_message_count=transport_stats.checkpoint_message_count,
        checkpoint_snapshot_payload_bytes=transport_stats.checkpoint_payload_bytes,
        max_checkpoint_snapshot_payload_bytes=transport_stats.max_checkpoint_payload_bytes,
        checkpoint_snapshot_serialization_ns=transport_stats.checkpoint_serialization_ns,
        max_checkpoint_snapshot_serialization_ns=transport_stats.max_checkpoint_serialization_ns,
        publication_metrics_status=publication_metrics.status,
        publication_fallback_process_ids=publication_metrics.fallback_process_ids,
        publication_socket_attempted_process_count=(
            publication_metrics.socket_attempted_process_count
        ),
        publication_socket_failure_ns=publication_metrics.socket_failure_ns,
        max_publication_socket_failure_ns=publication_metrics.max_socket_failure_ns,
        normalization_metrics_status="available",
        normalization_duration_ns=normalization_duration_ns,
        ranking_database_peak_bytes=ranking_database_peak_bytes,
        semantic_capture_status=semantic.status,
        semantic_capture_process_count=semantic.process_count,
        subprocess_event_count=len(semantic.subprocess_events),
        dropped_subprocess_count=semantic.dropped_subprocess_count,
        semantic_callback_error_count=semantic.callback_error_count,
        semantic_caller_event_count=len(semantic.caller_events),
        semantic_caller_edge_count=len(semantic.caller_edges),
        caller_attribution_status=semantic.caller_attribution_status,
        attributed_subprocess_count=semantic.attributed_subprocess_count,
        unattributed_subprocess_count=semantic.unattributed_subprocess_count,
        invalid_caller_count=semantic.invalid_caller_count,
        caller_callback_error_count=semantic.caller_callback_error_count,
        http_capture_status=http.status,
        http_capture_process_count=http.process_count,
        http_request_event_count=len(http.request_events),
        dropped_http_request_count=http.dropped_request_count,
        http_callback_error_count=http.callback_error_count,
        http_caller_event_count=len(http.caller_events),
        http_caller_edge_count=len(http.caller_edges),
        http_caller_attribution_status=http.caller_attribution_status,
        attributed_http_request_count=http.attributed_request_count,
        unattributed_http_request_count=http.unattributed_request_count,
        invalid_http_caller_count=http.invalid_caller_count,
        http_caller_callback_error_count=http.caller_callback_error_count,
        http_adapters=http.adapters,
        network_capture_status=network.status,
        network_capture_process_count=network.process_count,
        network_connection_event_count=len(network.connection_events),
        dropped_network_connection_count=network.dropped_connection_count,
        network_callback_error_count=network.callback_error_count,
        network_caller_event_count=len(network.caller_events),
        network_caller_edge_count=len(network.caller_edges),
        network_caller_attribution_status=network.caller_attribution_status,
        attributed_network_connection_count=network.attributed_connection_count,
        unattributed_network_connection_count=network.unattributed_connection_count,
        invalid_network_caller_count=network.invalid_caller_count,
        network_caller_callback_error_count=network.caller_callback_error_count,
        network_adapters=network.adapters,
        network_setup_capture_status=network_setup.status,
        network_setup_capture_process_count=network_setup.process_count,
        network_setup_event_count=len(network_setup.phase_events),
        dropped_network_setup_count=network_setup.dropped_phase_count,
        network_setup_callback_error_count=network_setup.callback_error_count,
        network_setup_caller_event_count=len(network_setup.caller_events),
        network_setup_caller_edge_count=len(network_setup.caller_edges),
        network_setup_caller_attribution_status=network_setup.caller_attribution_status,
        attributed_network_setup_count=network_setup.attributed_phase_count,
        unattributed_network_setup_count=network_setup.unattributed_phase_count,
        invalid_network_setup_caller_count=network_setup.invalid_caller_count,
        network_setup_caller_callback_error_count=network_setup.caller_callback_error_count,
        network_setup_adapters=network_setup.adapters,
        logical_operation_capture_status=logical.status,
        logical_operation_capture_process_count=logical.process_count,
        logical_operation_event_count=len(logical.operation_events),
        dropped_logical_operation_count=logical.dropped_operation_count,
        logical_operation_callback_error_count=logical.callback_error_count,
        logical_operation_caller_event_count=len(logical.caller_events),
        logical_operation_caller_edge_count=len(logical.caller_edges),
        logical_operation_caller_attribution_status=logical.caller_attribution_status,
        attributed_logical_operation_count=logical.attributed_operation_count,
        unattributed_logical_operation_count=logical.unattributed_operation_count,
        invalid_logical_operation_caller_count=logical.invalid_caller_count,
        logical_operation_caller_callback_error_count=logical.caller_callback_error_count,
        logical_operation_adapters=logical.adapters,
    )
