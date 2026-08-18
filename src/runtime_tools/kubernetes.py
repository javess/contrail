"""Normalize a bounded Kubernetes API snapshot into an existing execution."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Never

from runtime_tools.enrichment import EnrichmentError, enrich_copy
from runtime_tools.json_support import reject_duplicate_object
from runtime_tools.model import CausalEdge, Entity, Event, JsonValue
from runtime_tools.storage import RunpackReader, RunpackWriter


class KubernetesImportError(EnrichmentError):
    """Raised when Kubernetes snapshot evidence is malformed."""


@dataclass(frozen=True, slots=True)
class KubernetesImportResult:
    entity_count: int
    event_count: int
    edge_count: int
    correlation_count: int


_RFC3339_TIMESTAMP = re.compile(
    r"^(?P<whole>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?(?P<zone>Z|[+-]\d{2}:\d{2})$"
)
_RFC3339_WITHOUT_ZONE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MIN_RUNPACK_TIMESTAMP_NS = -(1 << 63)
_MAX_RUNPACK_TIMESTAMP_NS = (1 << 63) - 1
_WORKLOAD_KINDS = {"Node", "Deployment", "ReplicaSet", "Job", "Pod"}
MAX_KUBERNETES_SNAPSHOT_BYTES = 64 * 1024 * 1024
MAX_KUBERNETES_ITEMS = 200_000
MAX_KUBERNETES_CONTAINERS = 1_000_000


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise KubernetesImportError(f"{label} must be an object")
    return value


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise KubernetesImportError(f"{label} must be a list")
    return value


def _metadata(item: dict[str, object]) -> dict[str, object]:
    return _object(item.get("metadata", {}), "Kubernetes metadata")


def _validate_utf8(value: str, label: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise KubernetesImportError(f"{label} must be valid UTF-8") from exc
    return value


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise KubernetesImportError(f"{label} must be a non-empty string")
    return _validate_utf8(value, label)


def _optional_string(value: object, label: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise KubernetesImportError(f"{label} must be a string")
    return _validate_utf8(value, label)


def _kind(item: dict[str, object]) -> str:
    return _required_string(item.get("kind"), "Kubernetes kind")


def _name(item: dict[str, object]) -> str:
    return _required_string(_metadata(item).get("name"), "metadata.name")


def _namespace(metadata: dict[str, object]) -> str:
    value = metadata.get("namespace")
    if value is None or value == "":
        return "default"
    if not isinstance(value, str):
        raise KubernetesImportError("metadata.namespace must be a string")
    return _validate_utf8(value, "metadata.namespace")


def _uid(item: dict[str, object]) -> str:
    metadata = _metadata(item)
    value = metadata.get("uid")
    if value is None or value == "":
        return f"name:{_kind(item)}:{_namespace(metadata)}:{_name(item)}"
    if not isinstance(value, str):
        raise KubernetesImportError("metadata.uid must be a string")
    return _validate_utf8(value, "metadata.uid")


def _timestamp(value: object) -> int | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise KubernetesImportError("Kubernetes timestamp must be an RFC 3339 string")
    match = _RFC3339_TIMESTAMP.fullmatch(value)
    if match is None:
        if _RFC3339_WITHOUT_ZONE.fullmatch(value):
            raise KubernetesImportError(f"Kubernetes timestamp requires a timezone: {value}")
        raise KubernetesImportError(f"invalid Kubernetes timestamp: {value}")
    zone = "+00:00" if match.group("zone") == "Z" else match.group("zone")
    try:
        parsed = datetime.fromisoformat(f"{match.group('whole')}{zone}")
    except ValueError as exc:
        raise KubernetesImportError(f"invalid Kubernetes timestamp: {value}") from exc
    delta = parsed.astimezone(UTC) - _EPOCH
    whole_seconds = delta.days * 86_400 + delta.seconds
    fraction = match.group("fraction") or ""
    timestamp_ns = whole_seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")
    if not _MIN_RUNPACK_TIMESTAMP_NS <= timestamp_ns <= _MAX_RUNPACK_TIMESTAMP_NS:
        raise KubernetesImportError(f"Kubernetes timestamp exceeds runpack range: {value}")
    return timestamp_ns


def _integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise KubernetesImportError(f"{label} must be a non-negative integer")
    return value


def _metadata_string_map(value: object, label: str) -> dict[str, JsonValue]:
    if value is None:
        return {}
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise KubernetesImportError(f"{label} must map strings to strings")
    return {
        _validate_utf8(key, label): _validate_utf8(item, label)
        for key, item in value.items()
        if isinstance(key, str) and isinstance(item, str)
    }


def _attributes(item: dict[str, object]) -> dict[str, JsonValue]:
    metadata = _metadata(item)
    return {
        "k8s.uid": _uid(item),
        "k8s.namespace": _namespace(metadata),
        "k8s.labels": _metadata_string_map(metadata.get("labels"), "metadata.labels"),
        "k8s.annotations": _metadata_string_map(
            metadata.get("annotations"), "metadata.annotations"
        ),
    }


def _reject_json_constant(value: str) -> Never:
    raise ValueError(f"non-finite JSON constant: {value}")


def _load(source: Path) -> list[dict[str, object]]:
    try:
        with source.open("rb") as stream:
            raw = stream.read(MAX_KUBERNETES_SNAPSHOT_BYTES + 1)
    except OSError as exc:
        raise KubernetesImportError(f"could not read Kubernetes snapshot: {source}") from exc
    if len(raw) > MAX_KUBERNETES_SNAPSHOT_BYTES:
        raise KubernetesImportError(
            f"Kubernetes snapshot exceeds the {MAX_KUBERNETES_SNAPSHOT_BYTES}-byte input limit"
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise KubernetesImportError("Kubernetes snapshot must be UTF-8") from exc
    try:
        document = json.loads(
            text,
            parse_constant=_reject_json_constant,
            object_pairs_hook=reject_duplicate_object,
        )
    except json.JSONDecodeError as exc:
        raise KubernetesImportError(f"invalid Kubernetes JSON at line {exc.lineno}") from exc
    except RecursionError as exc:
        raise KubernetesImportError("Kubernetes JSON nesting is too deep") from exc
    except ValueError as exc:
        raise KubernetesImportError(f"invalid Kubernetes JSON: {exc}") from exc
    root = _object(document, "Kubernetes snapshot")
    items = _list(root.get("items"), "items")
    if len(items) > MAX_KUBERNETES_ITEMS:
        raise KubernetesImportError(
            f"Kubernetes snapshot exceeds the {MAX_KUBERNETES_ITEMS}-item input limit"
        )
    return [_object(item, "Kubernetes item") for item in items]


def _owner_uid(item: dict[str, object]) -> str | None:
    owners = _metadata(item).get("ownerReferences")
    if owners is None:
        return None
    if not isinstance(owners, list):
        raise KubernetesImportError("ownerReferences must be a list")
    owner_uids: list[str] = []
    controller_uids: list[str] = []
    for raw_owner in owners:
        owner = _object(raw_owner, "ownerReferences entry")
        uid = _required_string(owner.get("uid"), "ownerReferences uid")
        if uid in owner_uids:
            raise KubernetesImportError(f"duplicate ownerReferences uid: {uid}")
        owner_uids.append(uid)
        controller = owner.get("controller")
        if controller is not None and not isinstance(controller, bool):
            raise KubernetesImportError("ownerReferences controller must be a boolean")
        if controller is True:
            controller_uids.append(uid)
    if len(controller_uids) > 1:
        raise KubernetesImportError("ownerReferences contains multiple controllers")
    if controller_uids:
        return controller_uids[0]
    return owner_uids[0] if len(owner_uids) == 1 else None


def _replica_attributes(item: dict[str, object], status: dict[str, object]) -> dict[str, JsonValue]:
    spec = _object(item.get("spec", {}), f"{_kind(item)} spec")
    fields = (
        ("k8s.replicas.desired", spec.get("replicas")),
        ("k8s.replicas.current", status.get("replicas")),
        ("k8s.replicas.ready", status.get("readyReplicas")),
        ("k8s.replicas.available", status.get("availableReplicas")),
    )
    return {name: _integer(value, name) for name, value in fields if value is not None}


def _pod_finish(status: dict[str, object]) -> int | None:
    container_statuses = _container_statuses(status)
    if not container_statuses:
        return None
    finishes = []
    for container_status in container_statuses.values():
        state = _object(container_status.get("state", {}), "container state")
        raw_terminated = state.get("terminated")
        if raw_terminated is None:
            return None
        terminated = _object(raw_terminated, "terminated container state")
        value = _timestamp(terminated.get("finishedAt"))
        if value is None:
            return None
        finishes.append(value)
    return max(finishes)


def _container_statuses(status: dict[str, object]) -> dict[str, dict[str, object]]:
    raw_statuses = status.get("containerStatuses")
    if raw_statuses is None:
        return {}
    if not isinstance(raw_statuses, list):
        raise KubernetesImportError("containerStatuses must be a list")
    result: dict[str, dict[str, object]] = {}
    for raw_status in raw_statuses:
        container_status = _object(raw_status, "container status")
        name = _required_string(container_status.get("name"), "container status name")
        if name in result:
            raise KubernetesImportError(f"duplicate container status name: {name}")
        result[name] = container_status
    return result


def _resource_map(value: object) -> dict[str, JsonValue]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise KubernetesImportError("container resource quantities must map strings to strings")
    return {
        _validate_utf8(key, "container resource quantity"): _validate_utf8(
            item, "container resource quantity"
        )
        for key, item in value.items()
        if isinstance(key, str) and isinstance(item, str)
    }


def _container_interval(status: dict[str, object]) -> tuple[int | None, int | None]:
    state = status.get("state", {})
    if not isinstance(state, dict):
        return None, None
    running = state.get("running", {})
    terminated = state.get("terminated", {})
    started = _timestamp(running.get("startedAt")) if isinstance(running, dict) else None
    if isinstance(terminated, dict):
        terminated_started = _timestamp(terminated.get("startedAt"))
        if terminated_started is not None:
            started = terminated_started
        return started, _timestamp(terminated.get("finishedAt"))
    return started, None


def _correlations(
    runpack: Path,
    pod_events: dict[str, str],
) -> tuple[CausalEdge, ...]:
    with RunpackReader(runpack) as reader:
        entities = reader.entities()
        events = reader.events()
        edges = reader.causal_edges()
    entity_by_event = {event.id: event.entity_id for event in events}
    incoming_within_entity = {
        edge.target_event_id
        for edge in edges
        if edge.kind == "parent"
        and entity_by_event.get(edge.source_event_id) == entity_by_event.get(edge.target_event_id)
    }
    roots_by_entity: dict[str, list[str]] = {}
    for event in events:
        if event.entity_id is not None and event.id not in incoming_within_entity:
            roots_by_entity.setdefault(event.entity_id, []).append(event.id)
    result: list[CausalEdge] = []
    for entity in entities:
        pod_uid = entity.attributes.get("k8s.pod.uid")
        pod_event = pod_events.get(pod_uid) if isinstance(pod_uid, str) else None
        if pod_event:
            result.extend(
                CausalEdge(
                    pod_event,
                    root_event,
                    "correlates",
                    1.0,
                    {"source": "kubernetes"},
                )
                for root_event in roots_by_entity.get(entity.id, ())
            )
    return tuple(result)


def import_kubernetes_snapshot(
    runpack: Path,
    source: Path,
    output: Path,
) -> KubernetesImportResult:
    items = _load(source)
    entities: list[Entity] = []
    events: list[Event] = []
    edges: list[CausalEdge] = []
    entity_by_uid: dict[str, str] = {}
    lifecycle_by_uid: dict[str, str] = {}
    node_uid_by_name: dict[str, str] = {}
    container_count = 0

    for item in items:
        kind = _kind(item)
        if kind in _WORKLOAD_KINDS:
            uid = _uid(item)
            if uid in entity_by_uid:
                raise KubernetesImportError(f"duplicate Kubernetes object uid: {uid}")
            entity_by_uid[uid] = f"k8s:{kind.lower()}:{uid}"
            if kind == "Node":
                node_name = _name(item)
                if node_name in node_uid_by_name:
                    raise KubernetesImportError(f"duplicate Kubernetes Node name: {node_name}")
                node_uid_by_name[node_name] = uid

    owner_by_uid = {
        _uid(item): _owner_uid(item) for item in items if _kind(item) in _WORKLOAD_KINDS
    }
    complete_owner_chains: set[str] = set()
    for uid in owner_by_uid:
        trail: set[str] = set()
        current = uid
        while current in owner_by_uid and current not in complete_owner_chains:
            if current in trail:
                if len(trail) == 1:
                    raise KubernetesImportError(f"Kubernetes object cannot own itself: {uid}")
                raise KubernetesImportError("Kubernetes owner relationships contain a cycle")
            trail.add(current)
            owner_uid = owner_by_uid[current]
            if owner_uid is None:
                break
            current = owner_uid
        complete_owner_chains.update(trail)

    for item in items:
        kind = _kind(item)
        if kind not in _WORKLOAD_KINDS:
            continue
        uid = _uid(item)
        entity_id = f"k8s:{kind.lower()}:{uid}"
        parent_uid = owner_by_uid[uid]
        attributes = _attributes(item)
        status = _object(item.get("status", {}), f"{kind} status")
        node_name = ""
        if kind == "Pod":
            restart_count = sum(
                _integer(container_status.get("restartCount", 0), "container restart count")
                for container_status in _container_statuses(status).values()
            )
            node_name = _optional_string(
                _object(item.get("spec", {}), "Pod spec").get("nodeName"),
                "Pod spec.nodeName",
            )
            phase = _optional_string(status.get("phase"), "Pod status.phase")
            attributes.update(
                {
                    "k8s.node.name": node_name,
                    "k8s.pod.phase": phase,
                    "k8s.pod.restart_count": restart_count,
                }
            )
        if kind in {"Deployment", "ReplicaSet"}:
            attributes.update(_replica_attributes(item, status))
        entities.append(
            Entity(
                entity_id,
                kind.lower(),
                _name(item),
                entity_by_uid.get(parent_uid) if parent_uid else None,
                attributes,
            )
        )
        metadata = _metadata(item)
        started = _timestamp(metadata.get("creationTimestamp"))
        finished = (
            _pod_finish(status) if kind == "Pod" else _timestamp(status.get("completionTime"))
        )
        event_id = f"k8s:{uid}:lifecycle"
        lifecycle_by_uid[uid] = event_id
        events.append(
            Event(
                event_id,
                f"workload.{kind.lower()}",
                _name(item),
                entity_id,
                started,
                finished,
                "kubernetes.apiserver",
                None,
                None,
                {"phase": _optional_string(status.get("phase"), f"{kind} status.phase")},
            )
        )
        node_uid = node_uid_by_name.get(node_name)
        if kind == "Pod" and node_uid is not None:
            edges.append(
                CausalEdge(
                    f"k8s:{node_uid}:lifecycle",
                    event_id,
                    "hosts",
                    1.0,
                    {"source": "kubernetes"},
                )
            )

    for item in items:
        kind = _kind(item)
        if kind not in _WORKLOAD_KINDS:
            continue
        uid = _uid(item)
        owner_uid = owner_by_uid[uid]
        if owner_uid in lifecycle_by_uid:
            edges.append(
                CausalEdge(
                    lifecycle_by_uid[owner_uid],
                    lifecycle_by_uid[uid],
                    "owns",
                    1.0,
                    {"source": "kubernetes"},
                )
            )

    for item in items:
        if _kind(item) != "Pod":
            continue
        pod_uid = _uid(item)
        pod_entity = entity_by_uid.get(pod_uid)
        spec = _object(item.get("spec", {}), "Pod spec")
        pod_status = _object(item.get("status", {}), "Pod status")
        statuses = _container_statuses(pod_status)
        containers = _list(spec.get("containers", []), "Pod containers")
        container_count += len(containers)
        if container_count > MAX_KUBERNETES_CONTAINERS:
            raise KubernetesImportError(
                f"Kubernetes snapshot exceeds the {MAX_KUBERNETES_CONTAINERS}-container input limit"
            )
        if pod_entity:
            container_names: set[str] = set()
            for raw_container in containers:
                container = _object(raw_container, "container")
                name = _required_string(container.get("name"), "container name")
                if name in container_names:
                    raise KubernetesImportError(f"duplicate container name: {name}")
                container_names.add(name)
                resources = _object(container.get("resources", {}), "container resources")
                container_entity_id = f"{pod_entity}:container:{name}"
                container_status = statuses.get(name, {})
                entities.append(
                    Entity(
                        container_entity_id,
                        "container",
                        name,
                        pod_entity,
                        {
                            "image": _optional_string(container.get("image"), "container image"),
                            "resources.requests": _resource_map(resources.get("requests", {})),
                            "resources.limits": _resource_map(resources.get("limits", {})),
                            "restart_count": _integer(
                                container_status.get("restartCount", 0),
                                "container restart count",
                            ),
                        },
                    )
                )
                container_started, container_finished = _container_interval(container_status)
                container_event_id = f"{container_entity_id}:lifecycle"
                events.append(
                    Event(
                        container_event_id,
                        "workload.container",
                        name,
                        container_entity_id,
                        container_started,
                        container_finished,
                        "kubernetes.apiserver",
                        None,
                        None,
                        {
                            "restart_count": _integer(
                                container_status.get("restartCount", 0),
                                "container restart count",
                            )
                        },
                    )
                )
                edges.append(
                    CausalEdge(
                        lifecycle_by_uid[pod_uid],
                        container_event_id,
                        "contains",
                        1.0,
                        {"source": "kubernetes"},
                    )
                )
            unknown_statuses = sorted(statuses.keys() - container_names)
            if unknown_statuses:
                raise KubernetesImportError(
                    "containerStatuses contains names absent from Pod containers: "
                    f"{', '.join(unknown_statuses)}"
                )
    event_uids: set[str] = set()
    for item in items:
        if _kind(item) != "Event":
            continue
        event_uid = _uid(item)
        if event_uid in event_uids:
            raise KubernetesImportError(f"duplicate Kubernetes Event uid: {event_uid}")
        event_uids.add(event_uid)
        involved = _object(item.get("involvedObject", {}), "Event involvedObject")
        involved_uid = _optional_string(involved.get("uid"), "Event involvedObject.uid")
        involved_entity_id = entity_by_uid.get(involved_uid)
        if involved_entity_id is None:
            continue
        event_id = f"k8s:event:{event_uid}"
        event_timestamp = _timestamp(item.get("eventTime"))
        if event_timestamp is None:
            event_timestamp = _timestamp(_metadata(item).get("creationTimestamp"))
        events.append(
            Event(
                event_id,
                "kubernetes.event",
                _optional_string(item.get("reason"), "Event reason") or _name(item),
                involved_entity_id,
                event_timestamp,
                None,
                "kubernetes.apiserver",
                None,
                None,
                {
                    "message": _optional_string(item.get("message"), "Event message"),
                    "type": _optional_string(item.get("type"), "Event type"),
                },
            )
        )
        lifecycle = lifecycle_by_uid.get(involved_uid)
        if lifecycle:
            edges.append(CausalEdge(lifecycle, event_id, "emits", 1.0, {"source": "kubernetes"}))

    correlations = _correlations(runpack, lifecycle_by_uid)
    edges.extend(correlations)
    entity_order = {
        "node": 0,
        "deployment": 0,
        "job": 0,
        "replicaset": 1,
        "pod": 2,
        "container": 3,
    }
    entities.sort(key=lambda entity: (entity_order.get(entity.kind, 3), entity.id))

    def append(writer: RunpackWriter) -> KubernetesImportResult:
        starts = [event.started_at_ns for event in events if event.started_at_ns is not None]
        finishes = [event.finished_at_ns for event in events if event.finished_at_ns is not None]
        timestamps = [*starts, *finishes]
        if timestamps:
            writer.expand_execution_bounds(min(timestamps), max(finishes) if finishes else None)
        writer.add_entities(entities)
        writer.add_events(events)
        writer.add_causal_edges(edges)
        return KubernetesImportResult(len(entities), len(events), len(edges), len(correlations))

    return enrich_copy(runpack, output, append)
