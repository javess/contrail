"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import heapq
from typing import Literal

from runtime_tools.deep_profile._common import (
    MAX_SEMANTIC_EXECUTABLE_CHARACTERS,
    MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS,
    MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS,
    MAX_SEMANTIC_NETWORK_SETUP_EVENTS,
    MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS,
    SEMANTIC_NETWORK_ADAPTERS,
    SEMANTIC_NETWORK_SETUP_ADAPTERS,
    DeepProfileError,
    _bounded_sum,
    _process_role,
)
from runtime_tools.deep_profile._evidence import (
    _NETWORK_CALLER_CONTRACT,
    _NETWORK_SETUP_CALLER_CONTRACT,
    _NetworkCaptureEvidence,
    _NetworkConnectionRecord,
    _NetworkSetupCaptureEvidence,
    _NetworkSetupRecord,
    _SubprocessCaller,
)
from runtime_tools.deep_profile._parsing import (
    _caller_capture_status,
    _caller_event_id,
    _caller_evidence,
    _optional_nonnegative_integer,
    _parse_document,
    _retained_capture_status,
    _subprocess_caller,
)
from runtime_tools.deep_profile._session import (
    _boolean,
    _integer,
    _list,
    _object,
    _optional_boolean,
    _text,
)
from runtime_tools.model import Event, JsonValue


def _network_connection_record(
    value: object,
    *,
    document_pid: int,
) -> _NetworkConnectionRecord:
    item = _object(value, "semantic network connection record")
    identifier = _integer(item.get("id"), "semantic network connection id")
    adapter = _text(item.get("adapter"), "semantic network adapter")
    if adapter not in SEMANTIC_NETWORK_ADAPTERS:
        raise DeepProfileError("semantic network adapter is unsupported")
    raw_transport = item.get("transport")
    transport: Literal["tcp", "unix"]
    if raw_transport == "tcp":
        transport = "tcp"
    elif raw_transport == "unix":
        transport = "unix"
    else:
        raise DeepProfileError("semantic network transport is unsupported")
    raw_family = item.get("address_family")
    address_family: Literal["ipv4", "ipv6", "unix", "unknown"]
    if raw_family == "ipv4":
        address_family = "ipv4"
    elif raw_family == "ipv6":
        address_family = "ipv6"
    elif raw_family == "unix":
        address_family = "unix"
    elif raw_family == "unknown":
        address_family = "unknown"
    else:
        raise DeepProfileError("semantic network address family is unsupported")
    if (transport == "unix") != (address_family == "unix"):
        raise DeepProfileError("semantic network transport and address family are inconsistent")
    if item.get("server_address") is not None or item.get("server_identity_policy") != "redact":
        raise DeepProfileError("semantic network server identity is not redacted")
    server_port = _optional_nonnegative_integer(
        item.get("server_port"),
        "semantic network server port",
    )
    if server_port is not None and not 0 < server_port <= 65_535:
        raise DeepProfileError("semantic network server port is invalid")
    if transport == "unix" and server_port is not None:
        raise DeepProfileError("semantic Unix connection cannot contain a server port")
    tls_requested = _optional_boolean(
        item.get("tls_requested"),
        "semantic network TLS marker",
    )
    parent_pid = _integer(item.get("parent_pid"), "semantic network parent process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic network parent process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic network connection start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic network connection start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic network connection duration",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["connected", "connect_error", "unknown"]
    if raw_outcome == "connected":
        outcome = "connected"
    elif raw_outcome == "connect_error":
        outcome = "connect_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic network connection outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic network connection error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic network error type exceeds its character limit")
    if outcome == "connected" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("successful semantic network evidence is inconsistent")
    if outcome == "connect_error" and (duration_ns is None or error_type is None):
        raise DeepProfileError("failed semantic network evidence is inconsistent")
    if outcome == "unknown" and (duration_ns is not None or error_type is not None):
        raise DeepProfileError("unfinished semantic network evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic network connection finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _NetworkConnectionRecord(
        identifier,
        adapter,
        transport,
        address_family,
        server_port,
        tls_requested,
        parent_pid,
        started_at_ns,
        duration_ns,
        outcome,
        error_type,
        caller,
        caller_status,
    )


def _network_connection_event_id(record: _NetworkConnectionRecord) -> str:
    return f"semantic:network:{record.parent_pid}:{record.identifier}"


def _network_connection_event(
    record: _NetworkConnectionRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-network-connection-wrapper",
        "adapter": record.adapter,
        "transport": record.transport,
        "address_family": record.address_family,
        "server_address": None,
        "server_port": record.server_port,
        "server_identity_policy": "redact",
        "tls_requested": record.tls_requested,
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "path_captured": False,
        "credentials_captured": False,
        "duration_boundary": "connection_ready",
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _caller_event_id(
            _NETWORK_CALLER_CONTRACT, record.caller.identity
        )
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_network_connection_event_id(record),
        kind="network.connect",
        name=f"{record.transport.upper()} connect",
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _network_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _NetworkCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _NetworkConnectionRecord]] = []
    process_count = 0
    dropped_connection_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_connection_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if semantic.get("format_version") == 1 or "network_connections" not in semantic:
                saw_missing = True
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic network capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_network_connections"),
                    "semantic network connection limit",
                )
                != MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic network connection limit is unsupported")
            if semantic.get("server_identity_policy") != "redact":
                raise DeepProfileError("semantic network server identity policy is unsupported")
            raw_adapters = _list(
                semantic.get("network_adapters", ["stdlib.socket.connect"]),
                "semantic network adapters",
            )
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic network adapter")
                if adapter not in SEMANTIC_NETWORK_ADAPTERS or adapter in document_adapters:
                    raise DeepProfileError("semantic network adapters are unsupported")
                document_adapters.add(adapter)
            if "stdlib.socket.connect" not in document_adapters:
                raise DeepProfileError("semantic network standard-library adapter is missing")
            pid = _integer(document.get("pid"), "semantic network capture process id")
            raw_records = _list(
                semantic.get("network_connections"),
                "semantic network connections",
            )
            declared_count = _integer(
                semantic.get("network_connection_count"),
                "semantic network connection count",
            )
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_NETWORK_CONNECTIONS_PER_PROCESS
            ):
                raise DeepProfileError("semantic network connection count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_network_connection_count"),
                "semantic dropped network connection count",
            )
            document_callback_errors = _integer(
                semantic.get("network_callback_error_count"),
                "semantic network callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("network_caller_callback_error_count"),
                "semantic network caller callback error count",
            )
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic network registration contains evidence")
            records = tuple(
                _network_connection_record(raw_record, document_pid=pid)
                for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic network connection adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic network ids must be unique per process")
                identifiers.add(record.identifier)
            process_count += 1
            dropped_connection_count = _bounded_sum(
                dropped_connection_count,
                document_dropped_count,
                "semantic dropped network connection count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic network callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic network caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_connection_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = saw_incomplete or record.outcome == "unknown"
                rank = (
                    int(record.outcome != "connected"),
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_NETWORK_CONNECTION_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _NetworkCaptureEvidence(
            (), (), (), "invalid", process_count, 0, 0, "invalid", 0, 0, 0, 0, ()
        )
    if process_count == 0:
        return _NetworkCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_connection_count = _bounded_sum(
        dropped_connection_count,
        valid_connection_count - len(selected_records),
        "semantic dropped network connection count",
    )
    status = _retained_capture_status(
        partial=saw_missing or saw_checkpoint or saw_incomplete,
        dropped_count=dropped_connection_count,
        callback_error_count=callback_error_count,
    )
    (
        caller_events,
        caller_edges,
        attributed_connection_count,
        unattributed_connection_count,
    ) = _caller_evidence(
        selected_records,
        contract=_NETWORK_CALLER_CONTRACT,
        entity_id=entity_id,
        target_event_id=_network_connection_event_id,
    )
    caller_status = _caller_capture_status(
        invalid_count=invalid_caller_count,
        partial=saw_missing or saw_checkpoint,
        attributed_count=attributed_connection_count,
        unattributed_count=unattributed_connection_count,
        callback_error_count=caller_callback_error_count,
        dropped_count=dropped_connection_count,
    )
    return _NetworkCaptureEvidence(
        tuple(
            _network_connection_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_connection_count,
        callback_error_count,
        caller_status,
        attributed_connection_count,
        unattributed_connection_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )


def _network_setup_record(
    value: object,
    *,
    document_pid: int,
) -> _NetworkSetupRecord:
    item = _object(value, "semantic network setup record")
    identifier = _integer(item.get("id"), "semantic network setup id")
    raw_phase = item.get("phase")
    phase: Literal["dns", "tls"]
    if raw_phase == "dns":
        phase = "dns"
    elif raw_phase == "tls":
        phase = "tls"
    else:
        raise DeepProfileError("semantic network setup phase is unsupported")
    adapter = _text(item.get("adapter"), "semantic network setup adapter")
    if adapter not in SEMANTIC_NETWORK_SETUP_ADAPTERS:
        raise DeepProfileError("semantic network setup adapter is unsupported")
    if (phase == "dns") != (adapter == "stdlib.socket.getaddrinfo"):
        raise DeepProfileError("semantic network setup phase and adapter are inconsistent")
    parent_pid = _integer(item.get("parent_pid"), "semantic network setup process id")
    if parent_pid <= 0 or parent_pid != document_pid:
        raise DeepProfileError("semantic network setup process id is inconsistent")
    started_at_ns = _integer(item.get("started_at_ns"), "semantic network setup start")
    if started_at_ns <= 0:
        raise DeepProfileError("semantic network setup start must be positive")
    duration_ns = _optional_nonnegative_integer(
        item.get("duration_ns"),
        "semantic network setup duration",
    )
    raw_outcome = item.get("outcome")
    outcome: Literal["completed", "setup_error", "unknown"]
    if raw_outcome == "completed":
        outcome = "completed"
    elif raw_outcome == "setup_error":
        outcome = "setup_error"
    elif raw_outcome == "unknown":
        outcome = "unknown"
    else:
        raise DeepProfileError("semantic network setup outcome is unsupported")
    raw_error_type = item.get("error_type")
    error_type = (
        None
        if raw_error_type is None
        else _text(raw_error_type, "semantic network setup error type")
    )
    if error_type is not None and len(error_type) > MAX_SEMANTIC_EXECUTABLE_CHARACTERS:
        raise DeepProfileError("semantic network setup error type exceeds its character limit")
    if outcome == "completed" and (duration_ns is None or error_type is not None):
        raise DeepProfileError("successful semantic network setup evidence is inconsistent")
    if outcome == "setup_error" and (duration_ns is None or error_type is None):
        raise DeepProfileError("failed semantic network setup evidence is inconsistent")
    if outcome == "unknown" and (duration_ns is not None or error_type is not None):
        raise DeepProfileError("unfinished semantic network setup evidence is inconsistent")
    if duration_ns is not None:
        _bounded_sum(started_at_ns, duration_ns, "semantic network setup finish")
    raw_caller = item.get("caller")
    caller: _SubprocessCaller | None = None
    caller_status: Literal["complete", "unavailable", "invalid"] = "unavailable"
    if raw_caller is not None:
        try:
            caller = _subprocess_caller(raw_caller)
            caller_status = "complete"
        except DeepProfileError:
            caller_status = "invalid"
    return _NetworkSetupRecord(
        identifier,
        phase,
        adapter,
        parent_pid,
        started_at_ns,
        duration_ns,
        outcome,
        error_type,
        caller,
        caller_status,
    )


def _network_setup_event_id(record: _NetworkSetupRecord) -> str:
    return f"semantic:network-setup:{record.parent_pid}:{record.identifier}"


def _network_setup_event(
    record: _NetworkSetupRecord,
    *,
    entity_id: str,
    root_process_id: int | None,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-network-setup-wrapper",
        "phase": record.phase,
        "adapter": record.adapter,
        "parent_pid": record.parent_pid,
        "role": _process_role(record.parent_pid, root_process_id),
        "outcome": record.outcome,
        "hostname_captured": False,
        "server_address_captured": False,
        "sni_captured": False,
        "certificate_captured": False,
        "credentials_captured": False,
        "duration_boundary": record.phase,
    }
    if record.caller is not None:
        attributes["caller_event_id"] = _caller_event_id(
            _NETWORK_SETUP_CALLER_CONTRACT, record.caller.identity
        )
    if record.duration_ns is not None:
        attributes["duration_seconds"] = record.duration_ns / 1_000_000_000
    if record.error_type is not None:
        attributes["error"] = True
        attributes["error.type"] = record.error_type
    return Event(
        id=_network_setup_event_id(record),
        kind="network.resolve" if record.phase == "dns" else "network.tls_handshake",
        name="DNS resolution" if record.phase == "dns" else "TLS handshake",
        entity_id=entity_id,
        started_at_ns=record.started_at_ns,
        finished_at_ns=(
            None if record.duration_ns is None else record.started_at_ns + record.duration_ns
        ),
        clock_domain="host.wall",
        uncertainty_ns=None,
        sequence=None,
        attributes=attributes,
    )


def _network_setup_capture_evidence(
    payloads: tuple[bytes, ...],
    *,
    entity_id: str,
    root_process_id: int | None,
) -> _NetworkSetupCaptureEvidence:
    selected: list[tuple[int, int, int, int, int, int, _NetworkSetupRecord]] = []
    process_count = 0
    dropped_phase_count = 0
    callback_error_count = 0
    caller_callback_error_count = 0
    invalid_caller_count = 0
    valid_phase_count = 0
    ordinal = 0
    saw_missing = False
    saw_checkpoint = False
    saw_incomplete = False
    invalid = False
    adapters: set[str] = set()
    for payload in payloads:
        document = _parse_document(payload)
        raw_semantic = document.get("semantic_capture")
        if raw_semantic is None:
            saw_missing = True
            continue
        try:
            semantic = _object(raw_semantic, "semantic capture")
            if semantic.get("format_version") == 1 or "network_setup_phases" not in semantic:
                saw_missing = True
                continue
            if (
                semantic.get("format_version") != 2
                or semantic.get("observer") != "python-runtime-boundary-wrapper"
            ):
                raise DeepProfileError("semantic network setup capture format is unsupported")
            limits = _object(semantic.get("limits"), "semantic capture limits")
            if (
                _integer(
                    limits.get("max_network_setup_phases"),
                    "semantic network setup limit",
                )
                != MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS
            ):
                raise DeepProfileError("semantic network setup limit is unsupported")
            raw_adapters = _list(
                semantic.get("network_setup_adapters"),
                "semantic network setup adapters",
            )
            document_adapters: set[str] = set()
            for raw_adapter in raw_adapters:
                adapter = _text(raw_adapter, "semantic network setup adapter")
                if adapter not in SEMANTIC_NETWORK_SETUP_ADAPTERS or adapter in document_adapters:
                    raise DeepProfileError("semantic network setup adapters are unsupported")
                document_adapters.add(adapter)
            if "stdlib.socket.getaddrinfo" not in document_adapters:
                raise DeepProfileError("semantic DNS adapter is missing")
            pid = _integer(document.get("pid"), "semantic network setup process id")
            raw_records = _list(
                semantic.get("network_setup_phases"),
                "semantic network setup phases",
            )
            declared_count = _integer(
                semantic.get("network_setup_count"),
                "semantic network setup count",
            )
            if (
                declared_count != len(raw_records)
                or len(raw_records) > MAX_SEMANTIC_NETWORK_SETUP_PER_PROCESS
            ):
                raise DeepProfileError("semantic network setup count is inconsistent")
            document_dropped_count = _integer(
                semantic.get("dropped_network_setup_count"),
                "semantic dropped network setup count",
            )
            document_callback_errors = _integer(
                semantic.get("network_setup_callback_error_count"),
                "semantic network setup callback error count",
            )
            document_caller_callback_errors = _integer(
                semantic.get("network_setup_caller_callback_error_count"),
                "semantic network setup caller callback error count",
            )
            registration_only = _boolean(
                document.get("registration_only", False),
                "semantic capture registration marker",
            )
            if registration_only and (
                raw_records
                or document_dropped_count
                or document_callback_errors
                or document_caller_callback_errors
            ):
                raise DeepProfileError("semantic network setup registration contains evidence")
            records = tuple(
                _network_setup_record(raw_record, document_pid=pid) for raw_record in raw_records
            )
            if any(record.adapter not in document_adapters for record in records):
                raise DeepProfileError("semantic network setup adapter was not active")
            adapters.update(document_adapters)
            identifiers: set[int] = set()
            for record in records:
                if record.identifier in identifiers:
                    raise DeepProfileError("semantic network setup ids must be unique per process")
                identifiers.add(record.identifier)
            process_count += 1
            dropped_phase_count = _bounded_sum(
                dropped_phase_count,
                document_dropped_count,
                "semantic dropped network setup count",
            )
            callback_error_count = _bounded_sum(
                callback_error_count,
                document_callback_errors,
                "semantic network setup callback error count",
            )
            caller_callback_error_count = _bounded_sum(
                caller_callback_error_count,
                document_caller_callback_errors,
                "semantic network setup caller callback error count",
            )
            saw_checkpoint = (
                saw_checkpoint or document.get("snapshot_kind", "final") == "checkpoint"
            )
            for record in records:
                ordinal += 1
                valid_phase_count += 1
                invalid_caller_count += record.caller_status == "invalid"
                saw_incomplete = saw_incomplete or record.outcome == "unknown"
                rank = (
                    int(record.outcome != "completed"),
                    record.duration_ns or 0,
                    -record.started_at_ns,
                    -record.parent_pid,
                    -record.identifier,
                    ordinal,
                    record,
                )
                if len(selected) < MAX_SEMANTIC_NETWORK_SETUP_EVENTS:
                    heapq.heappush(selected, rank)
                elif rank[:6] > selected[0][:6]:
                    heapq.heapreplace(selected, rank)
        except DeepProfileError:
            invalid = True
    if invalid:
        return _NetworkSetupCaptureEvidence(
            (), (), (), "invalid", process_count, 0, 0, "invalid", 0, 0, 0, 0, ()
        )
    if process_count == 0:
        return _NetworkSetupCaptureEvidence(
            (), (), (), "unavailable", 0, 0, 0, "unavailable", 0, 0, 0, 0, ()
        )
    selected_records = tuple(
        sorted(
            (item[-1] for item in selected),
            key=lambda record: (record.started_at_ns, record.parent_pid, record.identifier),
        )
    )
    dropped_phase_count = _bounded_sum(
        dropped_phase_count,
        valid_phase_count - len(selected_records),
        "semantic dropped network setup count",
    )
    status = _retained_capture_status(
        partial=saw_missing or saw_checkpoint or saw_incomplete,
        dropped_count=dropped_phase_count,
        callback_error_count=callback_error_count,
    )
    caller_events, caller_edges, attributed_phase_count, unattributed_phase_count = (
        _caller_evidence(
            selected_records,
            contract=_NETWORK_SETUP_CALLER_CONTRACT,
            entity_id=entity_id,
            target_event_id=_network_setup_event_id,
            relation_for=lambda record: "resolves" if record.phase == "dns" else "handshakes",
        )
    )
    caller_status = _caller_capture_status(
        invalid_count=invalid_caller_count,
        partial=saw_missing or saw_checkpoint,
        attributed_count=attributed_phase_count,
        unattributed_count=unattributed_phase_count,
        callback_error_count=caller_callback_error_count,
        dropped_count=dropped_phase_count,
    )
    return _NetworkSetupCaptureEvidence(
        tuple(
            _network_setup_event(
                record,
                entity_id=entity_id,
                root_process_id=root_process_id,
            )
            for record in selected_records
        ),
        caller_events,
        caller_edges,
        status,
        process_count,
        dropped_phase_count,
        callback_error_count,
        caller_status,
        attributed_phase_count,
        unattributed_phase_count,
        invalid_caller_count,
        caller_callback_error_count,
        tuple(sorted(adapters)),
    )
