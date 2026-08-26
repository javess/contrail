"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from runtime_tools.batchscope.analysis._boundary_models import _ObservedProcessIdentity
from runtime_tools.batchscope.analysis._common import (
    MAX_NATIVE_EDGES_PER_PROCESS,
    MAX_NATIVE_FUNCTIONS_PER_PROCESS,
    MAX_PROFILE_COVERAGE_GAPS,
    MAX_PROFILE_PROCESS_CONTRIBUTIONS,
    MAX_PYTHON_FUNCTIONS_PER_PROCESS,
    CaptureStatus,
)
from runtime_tools.batchscope.analysis._profile_models import (
    DeepProfileSummary,
    NativeCallCaptureSummary,
    ObserverIntegritySummary,
    ProcessObserverSummary,
    ProfileNormalizationMetrics,
    ProfilePublicationMetrics,
    ProfileSnapshotMetrics,
    PythonExceptionCaptureSummary,
    PythonExceptionControlFlowFilterSummary,
    PythonProfileCoverage,
    PythonProfileCoverageGap,
    SampleProfileSummary,
    _CallerAttributionCounts,
)
from runtime_tools.deep_profile import (
    FILTERED_CONTROL_FLOW_EXCEPTION_TYPES,
    PYTHON_EXCEPTION_FILTER_VERSION,
)
from runtime_tools.model import JsonValue


def _metadata_count(value: JsonValue | None) -> int:
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= (1 << 63) - 1
        else 0
    )


def _semantic_count(value: JsonValue | None) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > (1 << 63) - 1:
        return None
    return value


def _capture_status(value: JsonValue | None) -> CaptureStatus:
    if value == "complete":
        return "complete"
    if value == "partial":
        return "partial"
    if value == "truncated":
        return "truncated"
    if value == "unavailable":
        return "unavailable"
    return "invalid"


def _caller_attribution_counts(
    raw: JsonValue | None,
    *,
    target_name: str,
    target_count: int,
    caller_count: int,
    attributed_count: int,
    invalid_count: int,
) -> _CallerAttributionCounts:
    fallback = _CallerAttributionCounts("unavailable", 0, 0, target_count, 0, 0)
    if raw is None:
        return fallback
    if not isinstance(raw, dict):
        return _CallerAttributionCounts(
            "invalid",
            fallback.caller_count,
            fallback.attributed_count,
            fallback.unattributed_count,
            fallback.invalid_count,
            fallback.callback_error_count,
        )

    attributed_key = f"attributed_{target_name}_count"
    unattributed_key = f"unattributed_{target_name}_count"
    parsed = tuple(
        _semantic_count(raw.get(key))
        for key in (
            "caller_count",
            attributed_key,
            unattributed_key,
            "invalid_caller_count",
            "callback_error_count",
        )
    )
    if any(value is None for value in parsed):
        return _CallerAttributionCounts(
            "invalid",
            fallback.caller_count,
            fallback.attributed_count,
            fallback.unattributed_count,
            fallback.invalid_count,
            fallback.callback_error_count,
        )
    normalized = tuple(value or 0 for value in parsed)
    counts = _CallerAttributionCounts(_capture_status(raw.get("status")), *normalized)
    if (
        raw.get("arguments_captured") is not False
        or raw.get("locals_captured") is not False
        or counts.caller_count != caller_count
        or counts.attributed_count != attributed_count
        or counts.attributed_count + counts.unattributed_count != target_count
        or counts.invalid_count < invalid_count
        or invalid_count
    ):
        return _CallerAttributionCounts(
            "invalid",
            counts.caller_count,
            counts.attributed_count,
            counts.unattributed_count,
            counts.invalid_count,
            counts.callback_error_count,
        )
    return counts


def _profile_snapshot_metrics(instrumentation: dict[str, JsonValue]) -> ProfileSnapshotMetrics:
    raw = instrumentation.get("snapshot_metrics")
    unavailable = ProfileSnapshotMetrics("unavailable", 0, 0, 0, 0.0, 0.0, 0, 0, 0, 0.0, 0.0)
    invalid = ProfileSnapshotMetrics("invalid", 0, 0, 0, 0.0, 0.0, 0, 0, 0, 0.0, 0.0)
    if raw is None:
        return unavailable
    if not isinstance(raw, dict):
        return invalid
    status = raw.get("status")
    if status == "available":
        normalized_status: Literal["available", "unavailable"] = "available"
    elif status == "unavailable":
        normalized_status = "unavailable"
    else:
        return invalid
    values: dict[str, int] = {}
    for key in (
        "message_count",
        "payload_bytes",
        "max_payload_bytes",
        "serialization_ns",
        "max_serialization_ns",
        "checkpoint_message_count",
        "checkpoint_payload_bytes",
        "max_checkpoint_payload_bytes",
        "checkpoint_serialization_ns",
        "max_checkpoint_serialization_ns",
    ):
        value = raw.get(key)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > (1 << 63) - 1
        ):
            return invalid
        values[key] = value
    if (
        values["max_payload_bytes"] > values["payload_bytes"]
        or values["max_serialization_ns"] > values["serialization_ns"]
        or values["checkpoint_message_count"] > values["message_count"]
        or values["checkpoint_payload_bytes"] > values["payload_bytes"]
        or values["max_checkpoint_payload_bytes"] > values["checkpoint_payload_bytes"]
        or values["checkpoint_serialization_ns"] > values["serialization_ns"]
        or values["max_checkpoint_serialization_ns"] > values["checkpoint_serialization_ns"]
        or (normalized_status == "unavailable" and any(values.values()))
    ):
        return invalid
    return ProfileSnapshotMetrics(
        normalized_status,
        values["message_count"],
        values["payload_bytes"],
        values["max_payload_bytes"],
        values["serialization_ns"] / 1_000_000_000,
        values["max_serialization_ns"] / 1_000_000_000,
        values["checkpoint_message_count"],
        values["checkpoint_payload_bytes"],
        values["max_checkpoint_payload_bytes"],
        values["checkpoint_serialization_ns"] / 1_000_000_000,
        values["max_checkpoint_serialization_ns"] / 1_000_000_000,
    )


def _profile_publication_metrics(
    instrumentation: dict[str, JsonValue],
) -> ProfilePublicationMetrics:
    raw = instrumentation.get("publication_metrics")
    unavailable = ProfilePublicationMetrics("unavailable", 0, (), 0, 0.0, 0.0)
    invalid = ProfilePublicationMetrics("invalid", 0, (), 0, 0.0, 0.0)
    if raw is None:
        return unavailable
    if not isinstance(raw, dict):
        return invalid
    status = raw.get("status")
    if status == "available":
        normalized_status: Literal["available", "unavailable"] = "available"
    elif status == "unavailable":
        normalized_status = "unavailable"
    else:
        return invalid
    values: dict[str, int] = {}
    for key in (
        "fallback_process_count",
        "socket_attempted_process_count",
        "socket_failure_ns",
        "max_socket_failure_ns",
    ):
        value = raw.get(key)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > (1 << 63) - 1
        ):
            return invalid
        values[key] = value
    raw_process_ids = raw.get("fallback_process_ids")
    if (
        not isinstance(raw_process_ids, list)
        or len(raw_process_ids) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return invalid
    process_ids: list[int] = []
    seen: set[int] = set()
    for raw_process_id in raw_process_ids:
        if (
            not isinstance(raw_process_id, int)
            or isinstance(raw_process_id, bool)
            or raw_process_id <= 0
            or raw_process_id in seen
        ):
            return invalid
        seen.add(raw_process_id)
        process_ids.append(raw_process_id)
    if (
        values["fallback_process_count"] != len(process_ids)
        or values["socket_attempted_process_count"] > values["fallback_process_count"]
        or values["max_socket_failure_ns"] > values["socket_failure_ns"]
        or (
            values["socket_attempted_process_count"] == 0
            and (values["socket_failure_ns"] or values["max_socket_failure_ns"])
        )
        or (normalized_status == "unavailable" and (process_ids or any(values.values())))
    ):
        return invalid
    return ProfilePublicationMetrics(
        normalized_status,
        values["fallback_process_count"],
        tuple(process_ids),
        values["socket_attempted_process_count"],
        values["socket_failure_ns"] / 1_000_000_000,
        values["max_socket_failure_ns"] / 1_000_000_000,
    )


def _profile_normalization_metrics(
    instrumentation: dict[str, JsonValue],
) -> ProfileNormalizationMetrics:
    raw = instrumentation.get("normalization_metrics")
    unavailable = ProfileNormalizationMetrics("unavailable", 0.0, 0, 0)
    invalid = ProfileNormalizationMetrics("invalid", 0.0, 0, 0)
    if raw is None:
        return unavailable
    if not isinstance(raw, dict):
        return invalid
    status = raw.get("status")
    if status == "available":
        normalized_status: Literal["available", "unavailable"] = "available"
    elif status == "unavailable":
        normalized_status = "unavailable"
    else:
        return invalid
    values: dict[str, int] = {}
    for key in (
        "duration_ns",
        "ranking_database_peak_bytes",
        "ranking_database_limit_bytes",
    ):
        value = raw.get(key)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > (1 << 63) - 1
        ):
            return invalid
        values[key] = value
    if (
        values["ranking_database_limit_bytes"] <= 0
        or values["ranking_database_peak_bytes"] > values["ranking_database_limit_bytes"]
        or (
            normalized_status == "unavailable"
            and (values["duration_ns"] or values["ranking_database_peak_bytes"])
        )
    ):
        return invalid
    return ProfileNormalizationMetrics(
        normalized_status,
        values["duration_ns"] / 1_000_000_000,
        values["ranking_database_peak_bytes"],
        values["ranking_database_limit_bytes"],
    )


def _profile_process_ids(
    instrumentation: dict[str, JsonValue],
    process_count: int,
) -> tuple[Literal["complete", "unavailable", "invalid"], tuple[int, ...]]:
    raw_process_ids = instrumentation.get("process_ids")
    if raw_process_ids is None:
        return "unavailable", ()
    if (
        not isinstance(raw_process_ids, list)
        or len(raw_process_ids) != process_count
        or len(raw_process_ids) > MAX_PROFILE_PROCESS_CONTRIBUTIONS
    ):
        return "invalid", ()
    process_ids: list[int] = []
    seen: set[int] = set()
    for raw_process_id in raw_process_ids:
        if (
            not isinstance(raw_process_id, int)
            or isinstance(raw_process_id, bool)
            or raw_process_id <= 0
            or raw_process_id in seen
        ):
            return "invalid", ()
        seen.add(raw_process_id)
        process_ids.append(raw_process_id)
    return "complete", tuple(sorted(process_ids))


def _is_python_process_name(name: str) -> bool:
    normalized = Path(name).name.casefold()
    return normalized.startswith("python") or normalized.startswith("pypy")


def _python_profile_coverage(
    instrumentation: dict[str, JsonValue],
    *,
    profile_status: str,
    process_count: int,
    process_observer: ProcessObserverSummary | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> tuple[tuple[int, ...], PythonProfileCoverage]:
    identity_status, process_ids = _profile_process_ids(instrumentation, process_count)
    if profile_status == "invalid" or identity_status == "invalid":
        return (), PythonProfileCoverage("invalid", process_count, None, 0, 0, (), ())
    if (
        identity_status == "unavailable"
        or process_observer is None
        or process_observer.status in {"unavailable", "invalid"}
    ):
        return process_ids, PythonProfileCoverage(
            "unavailable",
            process_count,
            None,
            0,
            0,
            (),
            (),
        )
    profile_id_set = set(process_ids)
    observed_python_ids = {
        pid
        for pid, identity in observed_processes.items()
        if pid in profile_id_set or _is_python_process_name(identity.name)
    }
    unprofiled_ids = observed_python_ids - profile_id_set
    unprofiled = tuple(
        PythonProfileCoverageGap(
            pid,
            observed_processes[pid].name,
            observed_processes[pid].parent_name,
        )
        for pid in sorted(unprofiled_ids)[:MAX_PROFILE_COVERAGE_GAPS]
    )
    unobserved_profile_ids = tuple(sorted(profile_id_set - observed_processes.keys()))
    coverage_status: Literal["complete", "partial"] = (
        "complete"
        if process_observer.status == "complete"
        and not unprofiled_ids
        and not unobserved_profile_ids
        else "partial"
    )
    return process_ids, PythonProfileCoverage(
        coverage_status,
        process_count,
        len(observed_python_ids),
        len(profile_id_set & observed_python_ids),
        len(unprofiled_ids),
        unprofiled,
        unobserved_profile_ids,
    )


def _native_call_capture_summary(
    instrumentation: dict[str, JsonValue],
    *,
    profile_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"],
    profile_function_count: int,
) -> NativeCallCaptureSummary | None:
    raw_capture = instrumentation.get("native_call_capture")
    if raw_capture is None:
        return None

    def invalid() -> NativeCallCaptureSummary:
        return NativeCallCaptureSummary(
            "invalid",
            0,
            0,
            0,
            MAX_NATIVE_FUNCTIONS_PER_PROCESS,
            MAX_NATIVE_EDGES_PER_PROCESS,
        )

    if not isinstance(raw_capture, dict):
        return invalid()
    function_count = raw_capture.get("function_count")
    call_count = raw_capture.get("call_count")
    exception_count = raw_capture.get("exception_count")
    limits = raw_capture.get("limits")
    if (
        raw_capture.get("enabled") is not True
        or raw_capture.get("deep_only") is not True
        or raw_capture.get("arguments_captured") is not False
        or raw_capture.get("return_values_captured") is not False
        or raw_capture.get("exception_messages_captured") is not False
        or not isinstance(function_count, int)
        or isinstance(function_count, bool)
        or not 0 <= function_count <= profile_function_count
        or not isinstance(call_count, int)
        or isinstance(call_count, bool)
        or call_count < function_count
        or not isinstance(exception_count, int)
        or isinstance(exception_count, bool)
        or not 0 <= exception_count <= call_count
        or not isinstance(limits, dict)
        or limits.get("max_functions_per_process") != MAX_NATIVE_FUNCTIONS_PER_PROCESS
        or limits.get("max_edges_per_process") != MAX_NATIVE_EDGES_PER_PROCESS
    ):
        return invalid()
    return NativeCallCaptureSummary(
        profile_status,
        function_count,
        call_count,
        exception_count,
        MAX_NATIVE_FUNCTIONS_PER_PROCESS,
        MAX_NATIVE_EDGES_PER_PROCESS,
    )


def _python_exception_capture_summary(
    instrumentation: dict[str, JsonValue],
    *,
    profile_status: Literal["complete", "partial", "truncated", "unavailable", "invalid"],
    profile_function_count: int,
    observer_integrity: ObserverIntegritySummary | None,
) -> PythonExceptionCaptureSummary | None:
    raw_capture = instrumentation.get("python_exception_capture")
    if raw_capture is None:
        return None

    def invalid() -> PythonExceptionCaptureSummary:
        return PythonExceptionCaptureSummary(
            "invalid",
            0,
            0,
            0,
            MAX_PYTHON_FUNCTIONS_PER_PROCESS,
        )

    if not isinstance(raw_capture, dict):
        return invalid()
    function_count = raw_capture.get("function_count")
    event_count = raw_capture.get("event_count")
    dropped_event_count = raw_capture.get("dropped_event_count")
    limits = raw_capture.get("limits")
    if (
        raw_capture.get("enabled") is not True
        or raw_capture.get("deep_only") is not True
        or raw_capture.get("event_semantics") != "per_propagated_frame"
        or raw_capture.get("arguments_captured") is not False
        or raw_capture.get("locals_captured") is not False
        or raw_capture.get("exception_types_captured") is not False
        or raw_capture.get("exception_values_captured") is not False
        or raw_capture.get("exception_messages_captured") is not False
        or raw_capture.get("tracebacks_captured") is not False
        or raw_capture.get("line_events_enabled") is not False
        or raw_capture.get("opcode_events_enabled") is not False
        or not isinstance(function_count, int)
        or isinstance(function_count, bool)
        or not 0 <= function_count <= profile_function_count
        or not isinstance(event_count, int)
        or isinstance(event_count, bool)
        or event_count < function_count
        or not isinstance(dropped_event_count, int)
        or isinstance(dropped_event_count, bool)
        or dropped_event_count < 0
        or not isinstance(limits, dict)
        or limits.get("max_functions_per_process") != MAX_PYTHON_FUNCTIONS_PER_PROCESS
    ):
        return invalid()
    exception_status = profile_status
    if observer_integrity is not None:
        if observer_integrity.status == "invalid":
            exception_status = "invalid"
        elif observer_integrity.status == "partial" and (
            observer_integrity.trace_hook_setter_call_count
            or observer_integrity.missing_process_count
        ):
            exception_status = "partial"
    control_flow_filter: PythonExceptionControlFlowFilterSummary | None = None
    raw_control_flow_filter = raw_capture.get("control_flow_filter")
    if raw_control_flow_filter is not None:
        if not isinstance(raw_control_flow_filter, dict):
            return invalid()
        raw_filter_status = raw_control_flow_filter.get("status")
        non_control_flow_function_count = raw_control_flow_filter.get(
            "non_control_flow_function_count"
        )
        non_control_flow_event_count = raw_control_flow_filter.get("non_control_flow_event_count")
        filtered_event_count = raw_control_flow_filter.get("filtered_event_count")
        dropped_non_control_flow_event_count = raw_control_flow_filter.get(
            "dropped_non_control_flow_event_count"
        )
        dropped_filtered_event_count = raw_control_flow_filter.get("dropped_filtered_event_count")
        filter_counts = (
            non_control_flow_function_count,
            non_control_flow_event_count,
            filtered_event_count,
            dropped_non_control_flow_event_count,
            dropped_filtered_event_count,
        )
        if (
            raw_control_flow_filter.get("format_version") != PYTHON_EXCEPTION_FILTER_VERSION
            or raw_filter_status != profile_status
            or raw_control_flow_filter.get("event_semantics") != "exact_type_identity"
            or raw_control_flow_filter.get("filtered_exception_types")
            != list(FILTERED_CONTROL_FLOW_EXCEPTION_TYPES)
            or raw_control_flow_filter.get("exception_type_identity_inspected") is not True
            or raw_control_flow_filter.get("exception_types_captured") is not False
            or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 0
                for value in filter_counts
            )
        ):
            return invalid()
        assert isinstance(non_control_flow_function_count, int)
        assert isinstance(non_control_flow_event_count, int)
        assert isinstance(filtered_event_count, int)
        assert isinstance(dropped_non_control_flow_event_count, int)
        assert isinstance(dropped_filtered_event_count, int)
        if (
            non_control_flow_function_count > function_count
            or non_control_flow_event_count < non_control_flow_function_count
            or non_control_flow_event_count > event_count
            or filtered_event_count != event_count - non_control_flow_event_count
            or dropped_non_control_flow_event_count > dropped_event_count
            or dropped_filtered_event_count
            != dropped_event_count - dropped_non_control_flow_event_count
        ):
            return invalid()
        control_flow_filter = PythonExceptionControlFlowFilterSummary(
            exception_status,
            non_control_flow_function_count,
            non_control_flow_event_count,
            filtered_event_count,
            dropped_non_control_flow_event_count,
            dropped_filtered_event_count,
        )
    return PythonExceptionCaptureSummary(
        exception_status,
        function_count,
        event_count,
        dropped_event_count,
        MAX_PYTHON_FUNCTIONS_PER_PROCESS,
        control_flow_filter,
    )


def _observer_integrity_summary(
    instrumentation: dict[str, JsonValue],
    *,
    profile_process_count: int,
) -> ObserverIntegritySummary | None:
    raw_integrity = instrumentation.get("observer_integrity")
    if raw_integrity is None:
        return None

    def invalid() -> ObserverIntegritySummary:
        return ObserverIntegritySummary("invalid", 0, profile_process_count, 0, 0, 0, 0)

    if not isinstance(raw_integrity, dict):
        return invalid()
    raw_status = raw_integrity.get("status")
    process_count = raw_integrity.get("process_count")
    missing_process_count = raw_integrity.get("missing_process_count")
    profile_hook_setter_call_count = raw_integrity.get("profile_hook_setter_call_count")
    profile_hook_setter_process_count = raw_integrity.get("profile_hook_setter_process_count")
    trace_hook_setter_call_count = raw_integrity.get("trace_hook_setter_call_count")
    trace_hook_setter_process_count = raw_integrity.get("trace_hook_setter_process_count")
    values = (
        process_count,
        missing_process_count,
        profile_hook_setter_call_count,
        profile_hook_setter_process_count,
        trace_hook_setter_call_count,
        trace_hook_setter_process_count,
    )
    if (
        raw_integrity.get("format_version") != 1
        or raw_status not in {"complete", "partial", "unavailable"}
        or raw_integrity.get("arguments_captured") is not False
        or raw_integrity.get("locals_captured") is not False
        or raw_integrity.get("hook_values_captured") is not False
        or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values
        )
    ):
        return invalid()
    assert isinstance(process_count, int)
    assert isinstance(missing_process_count, int)
    assert isinstance(profile_hook_setter_call_count, int)
    assert isinstance(profile_hook_setter_process_count, int)
    assert isinstance(trace_hook_setter_call_count, int)
    assert isinstance(trace_hook_setter_process_count, int)
    expected_status = (
        "unavailable"
        if process_count == 0
        else (
            "partial"
            if (
                missing_process_count
                or profile_hook_setter_call_count
                or trace_hook_setter_call_count
            )
            else "complete"
        )
    )
    if (
        process_count + missing_process_count != profile_process_count
        or profile_hook_setter_process_count > process_count
        or trace_hook_setter_process_count > process_count
        or profile_hook_setter_call_count < profile_hook_setter_process_count
        or trace_hook_setter_call_count < trace_hook_setter_process_count
        or raw_status != expected_status
    ):
        return invalid()
    status: Literal["complete", "partial", "unavailable", "invalid"] = expected_status
    return ObserverIntegritySummary(
        status,
        process_count,
        missing_process_count,
        profile_hook_setter_call_count,
        profile_hook_setter_process_count,
        trace_hook_setter_call_count,
        trace_hook_setter_process_count,
    )


def _deep_profile_summary(
    metadata: dict[str, JsonValue],
    process_observer: ProcessObserverSummary | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> DeepProfileSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict) or instrumentation.get("mode") != "deep":
        return None
    raw_status = instrumentation.get("status")
    status = (
        raw_status
        if isinstance(raw_status, str)
        and raw_status in {"complete", "partial", "truncated", "unavailable", "invalid"}
        else "invalid"
    )
    process_count = _metadata_count(instrumentation.get("process_count"))
    checkpoint_interval_ns = _metadata_count(instrumentation.get("checkpoint_interval_ns"))
    first_checkpoint_delay_ns = _metadata_count(instrumentation.get("first_checkpoint_delay_ns"))
    raw_transport = instrumentation.get("transport")
    transport = raw_transport if isinstance(raw_transport, str) and raw_transport else "unknown"
    process_ids, process_coverage = _python_profile_coverage(
        instrumentation,
        profile_status=status,
        process_count=process_count,
        process_observer=process_observer,
        observed_processes=observed_processes,
    )
    native_call_capture = _native_call_capture_summary(
        instrumentation,
        profile_status=status,
        profile_function_count=_metadata_count(instrumentation.get("function_count")),
    )
    observer_integrity = _observer_integrity_summary(
        instrumentation,
        profile_process_count=process_count,
    )
    python_exception_capture = _python_exception_capture_summary(
        instrumentation,
        profile_status=status,
        profile_function_count=_metadata_count(instrumentation.get("function_count")),
        observer_integrity=observer_integrity,
    )
    return DeepProfileSummary(
        status=status,
        process_count=process_count,
        function_count=_metadata_count(instrumentation.get("function_count")),
        edge_count=_metadata_count(instrumentation.get("edge_count")),
        truncated=instrumentation.get("truncated") is True,
        dropped_call_count=_metadata_count(instrumentation.get("dropped_call_count")),
        open_call_count=_metadata_count(instrumentation.get("open_call_count")),
        checkpoint_process_count=_metadata_count(instrumentation.get("checkpoint_process_count")),
        registration_only_process_count=_metadata_count(
            instrumentation.get("registration_only_process_count")
        ),
        dropped_profile_process_count=_metadata_count(
            instrumentation.get("dropped_profile_process_count")
        ),
        dropped_profile_process_count_truncated=(
            instrumentation.get("dropped_profile_process_count_truncated") is True
        ),
        first_checkpoint_delay_seconds=first_checkpoint_delay_ns / 1_000_000_000,
        checkpoint_interval_seconds=checkpoint_interval_ns / 1_000_000_000,
        transport=transport,
        collector_error_count=_metadata_count(instrumentation.get("collector_error_count")),
        snapshot_metrics=_profile_snapshot_metrics(instrumentation),
        publication_metrics=_profile_publication_metrics(instrumentation),
        normalization_metrics=_profile_normalization_metrics(instrumentation),
        process_ids=process_ids,
        process_coverage=process_coverage,
        native_call_capture=native_call_capture,
        python_exception_capture=python_exception_capture,
        observer_integrity=observer_integrity,
    )


def _sample_profile_summary(
    metadata: dict[str, JsonValue],
    process_observer: ProcessObserverSummary | None,
    observed_processes: dict[int, _ObservedProcessIdentity],
) -> SampleProfileSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict) or instrumentation.get("mode") != "sample":
        return None
    raw_status = instrumentation.get("status")
    status = (
        raw_status
        if isinstance(raw_status, str)
        and raw_status in {"complete", "partial", "truncated", "unavailable", "invalid"}
        else "invalid"
    )
    interval_ns = _metadata_count(instrumentation.get("interval_ns"))
    checkpoint_interval_ns = _metadata_count(instrumentation.get("checkpoint_interval_ns"))
    first_checkpoint_delay_ns = _metadata_count(instrumentation.get("first_checkpoint_delay_ns"))
    raw_transport = instrumentation.get("transport")
    transport = raw_transport if isinstance(raw_transport, str) and raw_transport else "unknown"
    process_count = _metadata_count(instrumentation.get("process_count"))
    process_ids, process_coverage = _python_profile_coverage(
        instrumentation,
        profile_status=status,
        process_count=process_count,
        process_observer=process_observer,
        observed_processes=observed_processes,
    )
    return SampleProfileSummary(
        status=status,
        process_count=process_count,
        function_count=_metadata_count(instrumentation.get("function_count")),
        edge_count=_metadata_count(instrumentation.get("edge_count")),
        truncated=instrumentation.get("truncated") is True,
        sample_count=_metadata_count(instrumentation.get("sample_count")),
        thread_sample_count=_metadata_count(instrumentation.get("thread_sample_count")),
        dropped_frame_sample_count=_metadata_count(
            instrumentation.get("dropped_frame_sample_count")
        ),
        interval_seconds=interval_ns / 1_000_000_000,
        checkpoint_process_count=_metadata_count(instrumentation.get("checkpoint_process_count")),
        registration_only_process_count=_metadata_count(
            instrumentation.get("registration_only_process_count")
        ),
        dropped_profile_process_count=_metadata_count(
            instrumentation.get("dropped_profile_process_count")
        ),
        dropped_profile_process_count_truncated=(
            instrumentation.get("dropped_profile_process_count_truncated") is True
        ),
        first_checkpoint_delay_seconds=first_checkpoint_delay_ns / 1_000_000_000,
        checkpoint_interval_seconds=checkpoint_interval_ns / 1_000_000_000,
        transport=transport,
        collector_error_count=_metadata_count(instrumentation.get("collector_error_count")),
        snapshot_metrics=_profile_snapshot_metrics(instrumentation),
        publication_metrics=_profile_publication_metrics(instrumentation),
        normalization_metrics=_profile_normalization_metrics(instrumentation),
        process_ids=process_ids,
        process_coverage=process_coverage,
    )


def _process_observer_summary(metadata: dict[str, JsonValue]) -> ProcessObserverSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    observer = capture.get("process_observer")
    if not isinstance(observer, dict) or observer.get("requested") is not True:
        return None
    raw_status = observer.get("status")
    status = (
        raw_status
        if isinstance(raw_status, str)
        and raw_status in {"complete", "truncated", "partial", "unavailable", "invalid"}
        else "invalid"
    )
    interval_ns = _metadata_count(observer.get("interval_ns"))
    raw_error = observer.get("error")
    error = raw_error if isinstance(raw_error, str) and raw_error else None
    return ProcessObserverSummary(
        status,
        _metadata_count(observer.get("process_count")),
        _metadata_count(observer.get("descendant_process_count")),
        _metadata_count(observer.get("sample_count")),
        _metadata_count(observer.get("poll_count")),
        observer.get("truncated") is True,
        _metadata_count(observer.get("dropped_process_count")),
        interval_ns / 1_000_000_000,
        error,
    )
