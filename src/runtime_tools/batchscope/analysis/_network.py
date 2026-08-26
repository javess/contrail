"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from typing import Literal

from runtime_tools.batchscope.analysis._boundary_models import (
    NetworkCaptureSummary,
    NetworkConnection,
    NetworkConnectionHotspot,
    NetworkSetupCaptureSummary,
    NetworkSetupHotspot,
    NetworkSetupPhase,
    _NetworkConnectionHotspotAggregate,
    _NetworkSetupHotspotAggregate,
)
from runtime_tools.batchscope.analysis._common import (
    NETWORK_CAPTURE_ADAPTERS,
    NETWORK_CHURN_MIN_CONNECTIONS,
    NETWORK_CHURN_MIN_RATE_PER_SECOND,
    NETWORK_SETUP_CAPTURE_ADAPTERS,
    NETWORK_SETUP_MIN_RUN_RATIO,
    NETWORK_SETUP_MIN_SECONDS,
)
from runtime_tools.batchscope.analysis._profile_models import Bottleneck
from runtime_tools.batchscope.analysis._profile_summary import (
    _caller_attribution_counts,
    _capture_status,
    _semantic_count,
)
from runtime_tools.batchscope.analysis._subprocess import (
    _NETWORK_CALLER_CONTRACT,
    _NETWORK_SETUP_CALLER_CONTRACT,
    _operation_callers,
)
from runtime_tools.model import CausalEdge, Event, JsonValue


def _network_connections(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[NetworkConnection, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_connection, invalid_caller_count = _operation_callers(
        events, edges, _NETWORK_CALLER_CONTRACT
    )
    connections: list[NetworkConnection] = []
    invalid_event_count = 0
    for event in events:
        if event.kind != "network.connect":
            continue
        adapter = event.attributes.get("adapter")
        raw_transport = event.attributes.get("transport")
        raw_family = event.attributes.get("address_family")
        server_port = event.attributes.get("server_port")
        tls_requested = event.attributes.get("tls_requested")
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        raw_outcome = event.attributes.get("outcome")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        if (
            event.started_at_ns is None
            or event.attributes.get("source") != "python-network-connection-wrapper"
            or not isinstance(adapter, str)
            or adapter not in NETWORK_CAPTURE_ADAPTERS
            or not isinstance(raw_transport, str)
            or raw_transport not in {"tcp", "unix"}
            or not isinstance(raw_family, str)
            or raw_family not in {"ipv4", "ipv6", "unix", "unknown"}
            or (raw_transport == "unix") != (raw_family == "unix")
            or event.name != f"{str(raw_transport).upper()} connect"
            or event.attributes.get("server_address") is not None
            or event.attributes.get("server_identity_policy") != "redact"
            or (
                server_port is not None
                and (
                    not isinstance(server_port, int)
                    or isinstance(server_port, bool)
                    or not 0 < server_port <= 65_535
                )
            )
            or (raw_transport == "unix" and server_port is not None)
            or (tls_requested is not None and not isinstance(tls_requested, bool))
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or raw_outcome not in {"connected", "connect_error", "unknown"}
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
            or event.attributes.get("path_captured") is not False
            or event.attributes.get("credentials_captured") is not False
            or event.attributes.get("duration_boundary") != "connection_ready"
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
        transport: Literal["tcp", "unix"] = "tcp" if raw_transport == "tcp" else "unix"
        address_family: Literal["ipv4", "ipv6", "unix", "unknown"]
        if raw_family == "ipv4":
            address_family = "ipv4"
        elif raw_family == "ipv6":
            address_family = "ipv6"
        elif raw_family == "unix":
            address_family = "unix"
        else:
            address_family = "unknown"
        outcome: Literal["connected", "connect_error", "unknown"]
        if raw_outcome == "connected":
            outcome = "connected"
        elif raw_outcome == "connect_error":
            outcome = "connect_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "connected"
                and (event.finished_at_ns is None or error_type is not None or error is True)
            )
            or (
                outcome == "connect_error"
                and (event.finished_at_ns is None or error_type is None or error is not True)
            )
            or (
                outcome == "unknown"
                and (event.finished_at_ns is not None or error_type is not None or error is True)
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        connections.append(
            NetworkConnection(
                event.id,
                adapter,
                transport,
                address_family,
                server_port,
                tls_requested,
                parent_pid,
                role,
                outcome,
                error_type,
                event.started_at_ns,
                duration_seconds,
                caller_by_connection.get(event.id),
            )
        )
    connections.sort(
        key=lambda connection: (
            connection.started_at_ns,
            connection.parent_pid,
            connection.event_id,
        )
    )
    return (
        tuple(connections),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_connection.values()}),
        len(caller_by_connection),
        tuple(sorted({connection.adapter for connection in connections})),
    )


def _network_setup_phases(
    events: tuple[Event, ...],
    edges: tuple[CausalEdge, ...],
) -> tuple[tuple[NetworkSetupPhase, ...], int, int, int, int, tuple[str, ...]]:
    caller_by_phase, invalid_caller_count = _operation_callers(
        events, edges, _NETWORK_SETUP_CALLER_CONTRACT
    )
    phases: list[NetworkSetupPhase] = []
    invalid_event_count = 0
    for event in events:
        if event.kind not in {"network.resolve", "network.tls_handshake"}:
            continue
        raw_phase = event.attributes.get("phase")
        adapter = event.attributes.get("adapter")
        parent_pid = event.attributes.get("parent_pid")
        raw_role = event.attributes.get("role")
        raw_outcome = event.attributes.get("outcome")
        error_type = event.attributes.get("error.type")
        error = event.attributes.get("error")
        expected_kind = (
            "network.resolve"
            if raw_phase == "dns"
            else "network.tls_handshake"
            if raw_phase == "tls"
            else None
        )
        expected_name = (
            "DNS resolution"
            if raw_phase == "dns"
            else "TLS handshake"
            if raw_phase == "tls"
            else None
        )
        if (
            event.started_at_ns is None
            or event.attributes.get("source") != "python-network-setup-wrapper"
            or expected_kind != event.kind
            or expected_name != event.name
            or not isinstance(adapter, str)
            or adapter not in NETWORK_SETUP_CAPTURE_ADAPTERS
            or (raw_phase == "dns") != (adapter == "stdlib.socket.getaddrinfo")
            or not isinstance(parent_pid, int)
            or isinstance(parent_pid, bool)
            or parent_pid <= 0
            or raw_role not in {"root", "descendant", "unknown"}
            or raw_outcome not in {"completed", "setup_error", "unknown"}
            or (error_type is not None and (not isinstance(error_type, str) or not error_type))
            or event.attributes.get("hostname_captured") is not False
            or event.attributes.get("server_address_captured") is not False
            or event.attributes.get("sni_captured") is not False
            or event.attributes.get("certificate_captured") is not False
            or event.attributes.get("credentials_captured") is not False
            or event.attributes.get("duration_boundary") != raw_phase
        ):
            invalid_event_count += 1
            continue
        phase: Literal["dns", "tls"] = "dns" if raw_phase == "dns" else "tls"
        role: Literal["root", "descendant", "unknown"]
        if raw_role == "root":
            role = "root"
        elif raw_role == "descendant":
            role = "descendant"
        else:
            role = "unknown"
        outcome: Literal["completed", "setup_error", "unknown"]
        if raw_outcome == "completed":
            outcome = "completed"
        elif raw_outcome == "setup_error":
            outcome = "setup_error"
        else:
            outcome = "unknown"
        if (
            (
                outcome == "completed"
                and (event.finished_at_ns is None or error_type is not None or error is True)
            )
            or (
                outcome == "setup_error"
                and (event.finished_at_ns is None or error_type is None or error is not True)
            )
            or (
                outcome == "unknown"
                and (event.finished_at_ns is not None or error_type is not None or error is True)
            )
        ):
            invalid_event_count += 1
            continue
        duration_seconds = (
            None
            if event.finished_at_ns is None
            else max(0, event.finished_at_ns - event.started_at_ns) / 1_000_000_000
        )
        phases.append(
            NetworkSetupPhase(
                event.id,
                phase,
                adapter,
                parent_pid,
                role,
                outcome,
                error_type,
                event.started_at_ns,
                duration_seconds,
                caller_by_phase.get(event.id),
            )
        )
    phases.sort(key=lambda phase: (phase.started_at_ns, phase.parent_pid, phase.event_id))
    return (
        tuple(phases),
        invalid_event_count,
        invalid_caller_count,
        len({caller.event_id for caller in caller_by_phase.values()}),
        len(caller_by_phase),
        tuple(sorted({phase.adapter for phase in phases})),
    )


def _network_setup_hotspots(
    phases: tuple[NetworkSetupPhase, ...],
) -> tuple[NetworkSetupHotspot, ...]:
    aggregates: dict[
        tuple[str | None, Literal["dns", "tls"], str], _NetworkSetupHotspotAggregate
    ] = {}
    for phase in phases:
        caller_event_id = phase.caller.event_id if phase.caller is not None else None
        key = (caller_event_id, phase.phase, phase.adapter)
        aggregate = aggregates.get(key)
        if aggregate is None:
            aggregate = _NetworkSetupHotspotAggregate(
                phase.phase,
                phase.adapter,
                phase.caller,
            )
            aggregates[key] = aggregate
        aggregate.phase_count += 1
        if phase.outcome == "completed":
            aggregate.completed_phase_count += 1
        elif phase.outcome == "setup_error":
            aggregate.failed_phase_count += 1
        else:
            aggregate.unfinished_phase_count += 1
        if phase.duration_seconds is not None:
            aggregate.total_duration_seconds += phase.duration_seconds
            aggregate.max_duration_seconds = max(
                aggregate.max_duration_seconds,
                phase.duration_seconds,
            )
    hotspots = tuple(
        NetworkSetupHotspot(
            aggregate.phase,
            aggregate.adapter,
            aggregate.phase_count,
            aggregate.completed_phase_count,
            aggregate.failed_phase_count,
            aggregate.unfinished_phase_count,
            aggregate.total_duration_seconds,
            aggregate.max_duration_seconds,
            aggregate.caller,
        )
        for aggregate in aggregates.values()
    )
    return tuple(
        sorted(
            hotspots,
            key=lambda hotspot: (
                -hotspot.failed_phase_count,
                -hotspot.phase_count,
                -hotspot.total_duration_seconds,
                hotspot.phase,
                hotspot.caller.name if hotspot.caller is not None else "",
                hotspot.adapter,
            ),
        )
    )


def _network_setup_bottlenecks(
    phases: tuple[NetworkSetupPhase, ...],
    capture: NetworkSetupCaptureSummary | None,
    total_seconds: float | None,
) -> tuple[Bottleneck, ...]:
    if (
        capture is None
        or capture.status in {"unavailable", "invalid"}
        or total_seconds is None
        or total_seconds <= 0
    ):
        return ()
    findings: list[Bottleneck] = []
    for phase_name, label in (("dns", "DNS resolution"), ("tls", "TLS handshake")):
        matching = tuple(phase for phase in phases if phase.phase == phase_name)
        failed_count = sum(phase.outcome == "setup_error" for phase in matching)
        if failed_count:
            findings.append(
                Bottleneck(
                    f"{phase_name}_failures",
                    (
                        f"{failed_count:,} of {len(matching):,} retained "
                        f"{label.lower()} attempts failed"
                    ),
                    0.9 if capture.status == "complete" else 0.7,
                )
            )
        completed = tuple(
            phase
            for phase in matching
            if phase.outcome == "completed" and phase.duration_seconds is not None
        )
        if not completed:
            continue
        slowest = max(completed, key=lambda phase: phase.duration_seconds or 0.0)
        duration_seconds = slowest.duration_seconds or 0.0
        ratio = duration_seconds / total_seconds
        if duration_seconds >= NETWORK_SETUP_MIN_SECONDS and ratio >= NETWORK_SETUP_MIN_RUN_RATIO:
            findings.append(
                Bottleneck(
                    f"{phase_name}_latency",
                    f"{label} took {duration_seconds:.3f}s ({ratio:.0%} of the run)",
                    0.8,
                )
            )
    return tuple(findings)


_LOGICAL_OPERATION_IDENTITIES = {
    ("cache", "batch"): "Cache batch",
    ("cache", "command"): "Cache command",
    ("database", "commit"): "Database commit",
    ("broker", "consume"): "Broker consume",
    ("database", "execute"): "Database execute",
    ("database", "executemany"): "Database executemany",
    ("database", "executescript"): "Database script",
    ("queue", "get"): "Queue get",
    ("broker", "publish"): "Broker publish",
    ("queue", "put"): "Queue put",
    ("database", "rollback"): "Database rollback",
    ("executor", "task"): "Executor task",
    ("scheduler", "task"): "Async task",
    ("server", "request"): "Inbound HTTP request",
}


def _network_bottlenecks(
    connections: tuple[NetworkConnection, ...],
    capture: NetworkCaptureSummary | None,
    total_seconds: float | None,
) -> tuple[Bottleneck, ...]:
    if (
        capture is None
        or capture.status in {"unavailable", "invalid"}
        or total_seconds is None
        or total_seconds <= 0
    ):
        return ()
    findings: list[Bottleneck] = []
    failed_count = sum(connection.outcome == "connect_error" for connection in connections)
    if failed_count:
        attempt_label = "attempt" if capture.connection_count == 1 else "attempts"
        findings.append(
            Bottleneck(
                "connection_failures",
                (
                    f"{failed_count:,} of {capture.connection_count:,} retained outbound "
                    f"connection {attempt_label} failed"
                ),
                0.9 if capture.status == "complete" else 0.7,
            )
        )
    completed_connections = tuple(
        connection
        for connection in connections
        if connection.outcome == "connected" and connection.duration_seconds is not None
    )
    if completed_connections:
        slowest = max(
            completed_connections,
            key=lambda connection: connection.duration_seconds or 0.0,
        )
        duration_seconds = slowest.duration_seconds or 0.0
        ratio = duration_seconds / total_seconds
        if duration_seconds >= NETWORK_SETUP_MIN_SECONDS and ratio >= NETWORK_SETUP_MIN_RUN_RATIO:
            target = (
                f"{slowest.transport}://<redacted>:{slowest.server_port}"
                if slowest.server_port is not None
                else f"{slowest.transport}://<redacted>"
            )
            findings.append(
                Bottleneck(
                    "connection_setup",
                    (
                        f"{target} took {duration_seconds:.3f}s to become ready "
                        f"({ratio:.0%} of the run)"
                    ),
                    0.8,
                )
            )
    connection_rate = capture.connection_count / total_seconds
    if (
        capture.connection_count >= NETWORK_CHURN_MIN_CONNECTIONS
        and connection_rate >= NETWORK_CHURN_MIN_RATE_PER_SECOND
    ):
        qualifier = "at least " if capture.status != "complete" else ""
        findings.append(
            Bottleneck(
                "connection_churn",
                (
                    f"{qualifier}{capture.connection_count:,} outbound connection attempts "
                    f"occurred ({connection_rate:,.1f}/s); inspect pooling or retry behavior"
                ),
                0.65 if capture.status == "complete" else 0.5,
            )
        )
    return tuple(findings)


def _network_connection_hotspots(
    connections: tuple[NetworkConnection, ...],
) -> tuple[NetworkConnectionHotspot, ...]:
    aggregates: dict[tuple[str | None, str], _NetworkConnectionHotspotAggregate] = {}
    for connection in connections:
        caller_event_id = connection.caller.event_id if connection.caller is not None else None
        key = (caller_event_id, connection.adapter)
        aggregate = aggregates.get(key)
        if aggregate is None:
            aggregate = _NetworkConnectionHotspotAggregate(
                connection.adapter,
                connection.caller,
            )
            aggregates[key] = aggregate
        aggregate.connection_count += 1
        if connection.outcome == "connected":
            aggregate.connected_connection_count += 1
        elif connection.outcome == "connect_error":
            aggregate.failed_connection_count += 1
        else:
            aggregate.unfinished_connection_count += 1
        if connection.duration_seconds is not None:
            aggregate.total_duration_seconds += connection.duration_seconds
            aggregate.max_duration_seconds = max(
                aggregate.max_duration_seconds,
                connection.duration_seconds,
            )
    hotspots = tuple(
        NetworkConnectionHotspot(
            aggregate.adapter,
            aggregate.connection_count,
            aggregate.connected_connection_count,
            aggregate.failed_connection_count,
            aggregate.unfinished_connection_count,
            aggregate.total_duration_seconds,
            aggregate.max_duration_seconds,
            aggregate.caller,
        )
        for aggregate in aggregates.values()
    )
    return tuple(
        sorted(
            hotspots,
            key=lambda hotspot: (
                -hotspot.failed_connection_count,
                -hotspot.connection_count,
                -hotspot.total_duration_seconds,
                hotspot.caller.name if hotspot.caller is not None else "",
                hotspot.adapter,
            ),
        )
    )


def _network_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    connection_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_connection_count: int,
    invalid_caller_count: int,
    observed_adapters: tuple[str, ...],
    connection_hotspot_count: int,
) -> NetworkCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    raw_network = instrumentation.get("network_capture")
    if not isinstance(raw_network, dict):
        return None
    status = _capture_status(raw_network.get("status"))

    raw_counts = {
        key: _semantic_count(raw_network.get(key))
        for key in (
            "process_count",
            "connection_count",
            "dropped_connection_count",
            "callback_error_count",
        )
    }
    raw_adapters = raw_network.get("adapters", ["stdlib.socket.connect"])
    adapters: tuple[str, ...] = ()
    adapters_invalid = not isinstance(raw_adapters, list)
    if isinstance(raw_adapters, list):
        adapter_values = [adapter for adapter in raw_adapters if isinstance(adapter, str)]
        if len(raw_adapters) > len(NETWORK_CAPTURE_ADAPTERS) or len(adapter_values) != len(
            raw_adapters
        ):
            adapters_invalid = True
        else:
            adapters = tuple(sorted(adapter_values))
            adapters_invalid = (
                len(adapters) != len(set(adapters))
                or "stdlib.socket.connect" not in adapters
                or any(adapter not in NETWORK_CAPTURE_ADAPTERS for adapter in adapters)
                or any(adapter not in adapters for adapter in observed_adapters)
            )
    declared_connection_count = raw_counts["connection_count"]
    if (
        raw_network.get("observer") != "python-network-connection-wrapper"
        or raw_network.get("zero_code") is not True
        or raw_network.get("server_identity_policy") != "redact"
        or raw_network.get("server_address_captured") is not False
        or raw_network.get("path_captured") is not False
        or raw_network.get("credentials_captured") is not False
        or any(value is None for value in raw_counts.values())
        or adapters_invalid
        or declared_connection_count != connection_event_count
        or invalid_event_count
    ):
        status = "invalid"
    caller = _caller_attribution_counts(
        raw_network.get("caller_attribution"),
        target_name="connection",
        target_count=connection_event_count,
        caller_count=caller_count,
        attributed_count=attributed_connection_count,
        invalid_count=invalid_caller_count,
    )

    return NetworkCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_connection_count or 0,
        raw_counts["dropped_connection_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller.status,
        caller.caller_count,
        caller.attributed_count,
        caller.unattributed_count,
        caller.invalid_count,
        caller.callback_error_count,
        adapters,
        connection_hotspot_count,
    )


def _network_setup_capture_summary(
    metadata: dict[str, JsonValue],
    *,
    phase_event_count: int,
    invalid_event_count: int,
    caller_count: int,
    attributed_phase_count: int,
    invalid_caller_count: int,
    observed_adapters: tuple[str, ...],
    hotspot_count: int,
) -> NetworkSetupCaptureSummary | None:
    capture = metadata.get("capture")
    if not isinstance(capture, dict):
        return None
    instrumentation = capture.get("instrumentation")
    if not isinstance(instrumentation, dict):
        return None
    raw_setup = instrumentation.get("network_setup_capture")
    if not isinstance(raw_setup, dict):
        return None
    status = _capture_status(raw_setup.get("status"))

    raw_counts = {
        key: _semantic_count(raw_setup.get(key))
        for key in (
            "process_count",
            "phase_count",
            "dropped_phase_count",
            "callback_error_count",
        )
    }
    raw_adapters = raw_setup.get("adapters")
    adapters: tuple[str, ...] = ()
    adapters_invalid = not isinstance(raw_adapters, list)
    if isinstance(raw_adapters, list):
        adapter_values = [adapter for adapter in raw_adapters if isinstance(adapter, str)]
        if len(raw_adapters) > len(NETWORK_SETUP_CAPTURE_ADAPTERS) or len(adapter_values) != len(
            raw_adapters
        ):
            adapters_invalid = True
        else:
            adapters = tuple(sorted(adapter_values))
            adapters_invalid = (
                len(adapters) != len(set(adapters))
                or "stdlib.socket.getaddrinfo" not in adapters
                or any(adapter not in NETWORK_SETUP_CAPTURE_ADAPTERS for adapter in adapters)
                or any(adapter not in adapters for adapter in observed_adapters)
            )
    declared_phase_count = raw_counts["phase_count"]
    if (
        raw_setup.get("observer") != "python-network-setup-wrapper"
        or raw_setup.get("zero_code") is not True
        or raw_setup.get("hostname_captured") is not False
        or raw_setup.get("server_address_captured") is not False
        or raw_setup.get("sni_captured") is not False
        or raw_setup.get("certificate_captured") is not False
        or raw_setup.get("credentials_captured") is not False
        or any(value is None for value in raw_counts.values())
        or adapters_invalid
        or declared_phase_count != phase_event_count
        or invalid_event_count
    ):
        status = "invalid"
    caller = _caller_attribution_counts(
        raw_setup.get("caller_attribution"),
        target_name="phase",
        target_count=phase_event_count,
        caller_count=caller_count,
        attributed_count=attributed_phase_count,
        invalid_count=invalid_caller_count,
    )

    return NetworkSetupCaptureSummary(
        status,
        raw_counts["process_count"] or 0,
        declared_phase_count or 0,
        raw_counts["dropped_phase_count"] or 0,
        raw_counts["callback_error_count"] or 0,
        invalid_event_count,
        caller.status,
        caller.caller_count,
        caller.attributed_count,
        caller.unattributed_count,
        caller.invalid_count,
        caller.callback_error_count,
        adapters,
        hotspot_count,
    )
