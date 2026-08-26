"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from runtime_tools.deep_profile._common import (
    FILTERED_CONTROL_FLOW_EXCEPTION_TYPES,
    MAX_DEEP_PROFILE_EDGES,
    MAX_DEEP_PROFILE_FILES,
    MAX_DEEP_PROFILE_FUNCTIONS,
    MAX_DEEP_PROFILE_NATIVE_EDGES_PER_PROCESS,
    MAX_DEEP_PROFILE_NATIVE_FUNCTIONS_PER_PROCESS,
    MAX_DEEP_PROFILE_PYTHON_FUNCTIONS_PER_PROCESS,
    MAX_DEEP_PROFILE_TOTAL_BYTES,
    MAX_PROFILE_RANKING_BYTES,
    MAX_PROFILE_SNAPSHOT_MESSAGES,
    MAX_SEMANTIC_HTTP_REQUEST_EVENTS,
    MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS,
    MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS,
    MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS,
    MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS,
    MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS,
    MAX_SEMANTIC_NETWORK_SETUP_EVENTS,
    MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS,
    MAX_SEMANTIC_SUBPROCESS_EVENTS,
    MAX_SEMANTIC_SUBPROCESS_PER_PROCESS,
    OBSERVER_INTEGRITY_VERSION,
    PROFILE_CHECKPOINT_INTERVAL_NS,
    PROFILE_FIRST_CHECKPOINT_DELAY_NS,
    PYTHON_EXCEPTION_FILTER_VERSION,
    PythonProfileMode,
    SemanticCaptureStatus,
)
from runtime_tools.model import CausalEdge, Event, JsonValue


@dataclass(frozen=True, slots=True)
class DeepProfileResult:
    events: tuple[Event, ...]
    edges: tuple[CausalEdge, ...]
    process_count: int
    process_ids: tuple[int, ...]
    truncated: bool
    dropped_call_count: int
    dropped_edge_count: int
    callback_error_count: int
    mode: PythonProfileMode = "deep"
    native_function_count: int = 0
    native_call_count: int = 0
    native_exception_count: int = 0
    python_exception_function_count: int = 0
    python_exception_event_count: int = 0
    dropped_python_exception_event_count: int = 0
    python_exception_filter_status: Literal["complete", "unavailable"] = "unavailable"
    python_non_control_flow_exception_function_count: int = 0
    python_non_control_flow_exception_event_count: int = 0
    dropped_python_non_control_flow_exception_event_count: int = 0
    observer_integrity_status: Literal["complete", "partial", "unavailable"] = "unavailable"
    observer_integrity_process_count: int = 0
    profile_hook_setter_call_count: int = 0
    profile_hook_setter_process_count: int = 0
    trace_hook_setter_call_count: int = 0
    trace_hook_setter_process_count: int = 0
    sample_count: int = 0
    thread_sample_count: int = 0
    interval_ns: int = 0
    open_call_count: int = 0
    checkpoint_process_count: int = 0
    registration_only_process_count: int = 0
    dropped_profile_process_count: int = 0
    dropped_profile_process_count_truncated: bool = False
    transport: str = "workload-file"
    collector_error_count: int = 0
    snapshot_metrics_status: Literal["available", "unavailable"] = "unavailable"
    snapshot_message_count: int = 0
    snapshot_payload_bytes: int = 0
    max_snapshot_payload_bytes: int = 0
    snapshot_serialization_ns: int = 0
    max_snapshot_serialization_ns: int = 0
    checkpoint_snapshot_message_count: int = 0
    checkpoint_snapshot_payload_bytes: int = 0
    max_checkpoint_snapshot_payload_bytes: int = 0
    checkpoint_snapshot_serialization_ns: int = 0
    max_checkpoint_snapshot_serialization_ns: int = 0
    publication_metrics_status: Literal["available", "unavailable", "invalid"] = "unavailable"
    publication_fallback_process_ids: tuple[int, ...] = ()
    publication_socket_attempted_process_count: int = 0
    publication_socket_failure_ns: int = 0
    max_publication_socket_failure_ns: int = 0
    normalization_metrics_status: Literal["available", "unavailable"] = "unavailable"
    normalization_duration_ns: int = 0
    ranking_database_peak_bytes: int = 0
    semantic_capture_status: SemanticCaptureStatus = "unavailable"
    semantic_capture_process_count: int = 0
    subprocess_event_count: int = 0
    dropped_subprocess_count: int = 0
    semantic_callback_error_count: int = 0
    semantic_caller_event_count: int = 0
    semantic_caller_edge_count: int = 0
    caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_subprocess_count: int = 0
    unattributed_subprocess_count: int = 0
    invalid_caller_count: int = 0
    caller_callback_error_count: int = 0
    http_capture_status: SemanticCaptureStatus = "unavailable"
    http_capture_process_count: int = 0
    http_request_event_count: int = 0
    dropped_http_request_count: int = 0
    http_callback_error_count: int = 0
    http_caller_event_count: int = 0
    http_caller_edge_count: int = 0
    http_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_http_request_count: int = 0
    unattributed_http_request_count: int = 0
    invalid_http_caller_count: int = 0
    http_caller_callback_error_count: int = 0
    http_adapters: tuple[str, ...] = ()
    network_capture_status: SemanticCaptureStatus = "unavailable"
    network_capture_process_count: int = 0
    network_connection_event_count: int = 0
    dropped_network_connection_count: int = 0
    network_callback_error_count: int = 0
    network_caller_event_count: int = 0
    network_caller_edge_count: int = 0
    network_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_network_connection_count: int = 0
    unattributed_network_connection_count: int = 0
    invalid_network_caller_count: int = 0
    network_caller_callback_error_count: int = 0
    network_adapters: tuple[str, ...] = ()
    network_setup_capture_status: SemanticCaptureStatus = "unavailable"
    network_setup_capture_process_count: int = 0
    network_setup_event_count: int = 0
    dropped_network_setup_count: int = 0
    network_setup_callback_error_count: int = 0
    network_setup_caller_event_count: int = 0
    network_setup_caller_edge_count: int = 0
    network_setup_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_network_setup_count: int = 0
    unattributed_network_setup_count: int = 0
    invalid_network_setup_caller_count: int = 0
    network_setup_caller_callback_error_count: int = 0
    network_setup_adapters: tuple[str, ...] = ()
    logical_operation_capture_status: SemanticCaptureStatus = "unavailable"
    logical_operation_capture_process_count: int = 0
    logical_operation_event_count: int = 0
    dropped_logical_operation_count: int = 0
    logical_operation_callback_error_count: int = 0
    logical_operation_caller_event_count: int = 0
    logical_operation_caller_edge_count: int = 0
    logical_operation_caller_attribution_status: SemanticCaptureStatus = "unavailable"
    attributed_logical_operation_count: int = 0
    unattributed_logical_operation_count: int = 0
    invalid_logical_operation_caller_count: int = 0
    logical_operation_caller_callback_error_count: int = 0
    logical_operation_adapters: tuple[str, ...] = ()

    def as_metadata(self) -> dict[str, JsonValue]:
        if self.process_count == 0:
            status = "unavailable"
        elif self.checkpoint_process_count:
            status = "partial"
        elif self.truncated:
            status = "truncated"
        else:
            status = "complete"
        common: dict[str, JsonValue] = {
            "mode": self.mode,
            "status": status,
            "process_count": self.process_count,
            "process_ids": list(self.process_ids),
            "function_count": (
                len(self.events)
                - self.subprocess_event_count
                - self.semantic_caller_event_count
                - self.http_request_event_count
                - self.http_caller_event_count
                - self.network_connection_event_count
                - self.network_caller_event_count
                - self.network_setup_event_count
                - self.network_setup_caller_event_count
                - self.logical_operation_event_count
                - self.logical_operation_caller_event_count
            ),
            "edge_count": (
                len(self.edges)
                - self.semantic_caller_edge_count
                - self.http_caller_edge_count
                - self.network_caller_edge_count
                - self.network_setup_caller_edge_count
                - self.logical_operation_caller_edge_count
            ),
            "truncated": self.truncated,
            "callback_error_count": self.callback_error_count,
            "open_call_count": self.open_call_count,
            "checkpoint_process_count": self.checkpoint_process_count,
            "registration_only_process_count": self.registration_only_process_count,
            "dropped_profile_process_count": self.dropped_profile_process_count,
            "dropped_profile_process_count_truncated": (
                self.dropped_profile_process_count_truncated
            ),
            "checkpoint_interval_ns": PROFILE_CHECKPOINT_INTERVAL_NS,
            "first_checkpoint_delay_ns": PROFILE_FIRST_CHECKPOINT_DELAY_NS,
            "transport": self.transport,
            "collector_error_count": self.collector_error_count,
            "snapshot_metrics": {
                "status": self.snapshot_metrics_status,
                "message_count": self.snapshot_message_count,
                "payload_bytes": self.snapshot_payload_bytes,
                "max_payload_bytes": self.max_snapshot_payload_bytes,
                "serialization_ns": self.snapshot_serialization_ns,
                "max_serialization_ns": self.max_snapshot_serialization_ns,
                "checkpoint_message_count": self.checkpoint_snapshot_message_count,
                "checkpoint_payload_bytes": self.checkpoint_snapshot_payload_bytes,
                "max_checkpoint_payload_bytes": self.max_checkpoint_snapshot_payload_bytes,
                "checkpoint_serialization_ns": self.checkpoint_snapshot_serialization_ns,
                "max_checkpoint_serialization_ns": (self.max_checkpoint_snapshot_serialization_ns),
            },
            "publication_metrics": {
                "status": self.publication_metrics_status,
                "fallback_process_count": len(self.publication_fallback_process_ids),
                "fallback_process_ids": list(self.publication_fallback_process_ids),
                "socket_attempted_process_count": (self.publication_socket_attempted_process_count),
                "socket_failure_ns": self.publication_socket_failure_ns,
                "max_socket_failure_ns": self.max_publication_socket_failure_ns,
            },
            "normalization_metrics": {
                "status": self.normalization_metrics_status,
                "duration_ns": self.normalization_duration_ns,
                "ranking_database_peak_bytes": self.ranking_database_peak_bytes,
                "ranking_database_limit_bytes": MAX_PROFILE_RANKING_BYTES,
            },
            "semantic_capture": {
                "status": self.semantic_capture_status,
                "observer": "python-subprocess-wrapper",
                "zero_code": True,
                "process_count": self.semantic_capture_process_count,
                "subprocess_count": self.subprocess_event_count,
                "dropped_subprocess_count": self.dropped_subprocess_count,
                "callback_error_count": self.semantic_callback_error_count,
                "arguments_captured": False,
                "environment_captured": False,
                "working_directory_captured": False,
                "caller_attribution": {
                    "status": self.caller_attribution_status,
                    "caller_count": self.semantic_caller_event_count,
                    "attributed_subprocess_count": self.attributed_subprocess_count,
                    "unattributed_subprocess_count": self.unattributed_subprocess_count,
                    "invalid_caller_count": self.invalid_caller_count,
                    "callback_error_count": self.caller_callback_error_count,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_subprocesses_per_process": MAX_SEMANTIC_SUBPROCESS_PER_PROCESS,
                    "max_subprocess_events": MAX_SEMANTIC_SUBPROCESS_EVENTS,
                },
            },
            "http_capture": {
                "status": self.http_capture_status,
                "observer": "python-http-client-wrapper",
                "zero_code": True,
                "process_count": self.http_capture_process_count,
                "request_count": self.http_request_event_count,
                "dropped_request_count": self.dropped_http_request_count,
                "callback_error_count": self.http_callback_error_count,
                "adapters": list(self.http_adapters),
                "server_identity_policy": "redact",
                "method_captured": True,
                "scheme_captured": True,
                "server_address_captured": False,
                "path_captured": False,
                "query_captured": False,
                "headers_captured": False,
                "body_captured": False,
                "response_body_captured": False,
                "caller_attribution": {
                    "status": self.http_caller_attribution_status,
                    "caller_count": self.http_caller_event_count,
                    "attributed_request_count": self.attributed_http_request_count,
                    "unattributed_request_count": self.unattributed_http_request_count,
                    "invalid_caller_count": self.invalid_http_caller_count,
                    "callback_error_count": self.http_caller_callback_error_count,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_requests_per_process": MAX_SEMANTIC_HTTP_REQUESTS_PER_PROCESS,
                    "max_request_events": MAX_SEMANTIC_HTTP_REQUEST_EVENTS,
                },
            },
            "network_capture": {
                "status": self.network_capture_status,
                "observer": "python-network-connection-wrapper",
                "zero_code": True,
                "process_count": self.network_capture_process_count,
                "connection_count": self.network_connection_event_count,
                "dropped_connection_count": self.dropped_network_connection_count,
                "callback_error_count": self.network_callback_error_count,
                "adapters": list(self.network_adapters),
                "server_identity_policy": "redact",
                "server_address_captured": False,
                "path_captured": False,
                "credentials_captured": False,
                "caller_attribution": {
                    "status": self.network_caller_attribution_status,
                    "caller_count": self.network_caller_event_count,
                    "attributed_connection_count": self.attributed_network_connection_count,
                    "unattributed_connection_count": (self.unattributed_network_connection_count),
                    "invalid_caller_count": self.invalid_network_caller_count,
                    "callback_error_count": self.network_caller_callback_error_count,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_connections_per_process": (MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS),
                    "max_connection_events": MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS,
                },
            },
            "network_setup_capture": {
                "status": self.network_setup_capture_status,
                "observer": "python-network-setup-wrapper",
                "zero_code": True,
                "process_count": self.network_setup_capture_process_count,
                "phase_count": self.network_setup_event_count,
                "dropped_phase_count": self.dropped_network_setup_count,
                "callback_error_count": self.network_setup_callback_error_count,
                "adapters": list(self.network_setup_adapters),
                "hostname_captured": False,
                "server_address_captured": False,
                "sni_captured": False,
                "certificate_captured": False,
                "credentials_captured": False,
                "caller_attribution": {
                    "status": self.network_setup_caller_attribution_status,
                    "caller_count": self.network_setup_caller_event_count,
                    "attributed_phase_count": self.attributed_network_setup_count,
                    "unattributed_phase_count": self.unattributed_network_setup_count,
                    "invalid_caller_count": self.invalid_network_setup_caller_count,
                    "callback_error_count": (self.network_setup_caller_callback_error_count),
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_phases_per_process": MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS,
                    "max_phase_events": MAX_SEMANTIC_NETWORK_SETUP_EVENTS,
                },
            },
            "logical_operation_capture": {
                "status": self.logical_operation_capture_status,
                "observer": "python-logical-operation-wrapper",
                "zero_code": True,
                "deep_only": True,
                "process_count": self.logical_operation_capture_process_count,
                "operation_count": self.logical_operation_event_count,
                "dropped_operation_count": self.dropped_logical_operation_count,
                "callback_error_count": self.logical_operation_callback_error_count,
                "adapters": list(self.logical_operation_adapters),
                "statement_captured": False,
                "parameters_captured": False,
                "payload_captured": False,
                "queue_item_captured": False,
                "queue_identity_captured": False,
                "callable_captured": False,
                "awaitable_captured": False,
                "task_name_captured": False,
                "context_captured": False,
                "arguments_captured": False,
                "return_value_captured": False,
                "exception_messages_captured": False,
                "http_method_captured": False,
                "route_captured": False,
                "url_captured": False,
                "headers_captured": False,
                "body_captured": False,
                "response_body_captured": False,
                "client_address_captured": False,
                "caller_attribution": {
                    "status": self.logical_operation_caller_attribution_status,
                    "caller_count": self.logical_operation_caller_event_count,
                    "attributed_operation_count": self.attributed_logical_operation_count,
                    "unattributed_operation_count": self.unattributed_logical_operation_count,
                    "invalid_caller_count": self.invalid_logical_operation_caller_count,
                    "callback_error_count": (self.logical_operation_caller_callback_error_count),
                    "arguments_captured": False,
                    "locals_captured": False,
                },
                "limits": {
                    "max_operations_per_process": (MAX_SEMANTIC_LOGICAL_OPERATIONS_PER_PROCESS),
                    "max_operation_events": MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS,
                },
            },
            "limits": {
                "max_profile_files": MAX_DEEP_PROFILE_FILES,
                "max_functions": MAX_DEEP_PROFILE_FUNCTIONS,
                "max_edges": MAX_DEEP_PROFILE_EDGES,
                "max_native_functions_per_process": (MAX_DEEP_PROFILE_NATIVE_FUNCTIONS_PER_PROCESS),
                "max_native_edges_per_process": MAX_DEEP_PROFILE_NATIVE_EDGES_PER_PROCESS,
                "max_total_bytes": MAX_DEEP_PROFILE_TOTAL_BYTES,
                "max_ranking_bytes": MAX_PROFILE_RANKING_BYTES,
                "max_snapshot_messages": MAX_PROFILE_SNAPSHOT_MESSAGES,
                "max_subprocess_events": MAX_SEMANTIC_SUBPROCESS_EVENTS,
                "max_http_request_events": MAX_SEMANTIC_HTTP_REQUEST_EVENTS,
                "max_network_connection_events": MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS,
                "max_network_setup_events": MAX_SEMANTIC_NETWORK_SETUP_EVENTS,
                "max_logical_operation_events": MAX_SEMANTIC_LOGICAL_OPERATION_EVENTS,
            },
        }
        if self.mode == "sample":
            return {
                **common,
                "observer": "python-stack-sampler",
                "intrusive": True,
                "estimated": True,
                "per_call": False,
                "sample_count": self.sample_count,
                "thread_sample_count": self.thread_sample_count,
                "interval_ns": self.interval_ns,
                "dropped_frame_sample_count": self.dropped_call_count,
                "dropped_edge_sample_count": self.dropped_edge_count,
            }
        return {
            **common,
            "observer": "python-sys-setprofile-and-settrace",
            "intrusive": True,
            "per_call": True,
            "observer_integrity": {
                "format_version": OBSERVER_INTEGRITY_VERSION,
                "status": self.observer_integrity_status,
                "process_count": self.observer_integrity_process_count,
                "missing_process_count": max(
                    0,
                    self.process_count - self.observer_integrity_process_count,
                ),
                "profile_hook_setter_call_count": self.profile_hook_setter_call_count,
                "profile_hook_setter_process_count": self.profile_hook_setter_process_count,
                "trace_hook_setter_call_count": self.trace_hook_setter_call_count,
                "trace_hook_setter_process_count": self.trace_hook_setter_process_count,
                "arguments_captured": False,
                "locals_captured": False,
                "hook_values_captured": False,
            },
            "python_exception_capture": {
                "enabled": True,
                "deep_only": True,
                "event_semantics": "per_propagated_frame",
                "function_count": self.python_exception_function_count,
                "event_count": self.python_exception_event_count,
                "dropped_event_count": self.dropped_python_exception_event_count,
                "arguments_captured": False,
                "locals_captured": False,
                "exception_types_captured": False,
                "exception_values_captured": False,
                "exception_messages_captured": False,
                "tracebacks_captured": False,
                "line_events_enabled": False,
                "opcode_events_enabled": False,
                **(
                    {
                        "control_flow_filter": {
                            "format_version": PYTHON_EXCEPTION_FILTER_VERSION,
                            "status": status,
                            "event_semantics": "exact_type_identity",
                            "filtered_exception_types": list(FILTERED_CONTROL_FLOW_EXCEPTION_TYPES),
                            "exception_type_identity_inspected": True,
                            "exception_types_captured": False,
                            "non_control_flow_function_count": (
                                self.python_non_control_flow_exception_function_count
                            ),
                            "non_control_flow_event_count": (
                                self.python_non_control_flow_exception_event_count
                            ),
                            "filtered_event_count": (
                                self.python_exception_event_count
                                - self.python_non_control_flow_exception_event_count
                            ),
                            "dropped_non_control_flow_event_count": (
                                self.dropped_python_non_control_flow_exception_event_count
                            ),
                            "dropped_filtered_event_count": (
                                self.dropped_python_exception_event_count
                                - self.dropped_python_non_control_flow_exception_event_count
                            ),
                        }
                    }
                    if self.python_exception_filter_status == "complete"
                    else {}
                ),
                "limits": {
                    "max_functions_per_process": (MAX_DEEP_PROFILE_PYTHON_FUNCTIONS_PER_PROCESS),
                },
            },
            "native_call_capture": {
                "enabled": True,
                "deep_only": True,
                "function_count": self.native_function_count,
                "call_count": self.native_call_count,
                "exception_count": self.native_exception_count,
                "arguments_captured": False,
                "return_values_captured": False,
                "exception_messages_captured": False,
                "limits": {
                    "max_functions_per_process": (MAX_DEEP_PROFILE_NATIVE_FUNCTIONS_PER_PROCESS),
                    "max_edges_per_process": MAX_DEEP_PROFILE_NATIVE_EDGES_PER_PROCESS,
                },
            },
            "dropped_call_count": self.dropped_call_count,
            "dropped_edge_count": self.dropped_edge_count,
        }
