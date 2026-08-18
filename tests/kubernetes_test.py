from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from itertools import permutations
from pathlib import Path
from typing import cast

import pytest

from runtime_tools import kubernetes
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.enrichment import enrich_copy as real_enrich_copy
from runtime_tools.inspect import inspect_runpack
from runtime_tools.kubernetes import (
    KubernetesImportError,
    KubernetesImportResult,
    import_kubernetes_snapshot,
)
from runtime_tools.model import CausalEdge, Entity, Event, Execution
from runtime_tools.otel import import_otlp_json
from runtime_tools.storage import RunpackReader, RunpackWriter


def _trace_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-trace:{label}").hex


def _span_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-span:{label}").hex[:16]


def _metadata(name: str, uid: str, **extra: object) -> dict[str, object]:
    return {"name": name, "uid": uid, **extra}


def test_kubernetes_snapshot_enriches_and_correlates_otel_runpack(tmp_path: Path) -> None:
    trace = tmp_path / "trace.json"
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "kubernetes.json"
    enriched = tmp_path / "enriched.runpack"
    trace.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "resource": {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": "worker"}},
                                {"key": "k8s.pod.uid", "value": {"stringValue": "pod-uid"}},
                            ]
                        },
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": _trace_id("trace"),
                                        "spanId": _span_id("work"),
                                        "name": "work",
                                        "startTimeUnixNano": "2000000000",
                                        "endTimeUnixNano": "8000000000",
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    import_otlp_json(trace, base, name="distributed-job")
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata(
                            "worker-0",
                            "pod-uid",
                            namespace="demo",
                            creationTimestamp="1970-01-01T00:00:02Z",
                            ownerReferences=[{"uid": "job-uid", "kind": "Job"}],
                            labels={"component": "worker"},
                            annotations={"example.dev/owner": "runtime-team"},
                        ),
                        "spec": {
                            "nodeName": "node-a",
                            "containers": [
                                {
                                    "name": "worker",
                                    "image": "demo:v1",
                                    "resources": {
                                        "requests": {"cpu": "500m", "memory": "1Gi"},
                                        "limits": {"cpu": "1", "memory": "2Gi"},
                                    },
                                }
                            ],
                        },
                        "status": {
                            "phase": "Succeeded",
                            "containerStatuses": [
                                {
                                    "name": "worker",
                                    "restartCount": 2,
                                    "state": {
                                        "terminated": {
                                            "startedAt": "1970-01-01T00:00:04Z",
                                            "finishedAt": "1970-01-01T00:00:08Z",
                                        }
                                    },
                                }
                            ],
                        },
                    },
                    {
                        "kind": "Job",
                        "metadata": _metadata(
                            "demo-job",
                            "job-uid",
                            namespace="demo",
                            creationTimestamp="1970-01-01T00:00:01Z",
                        ),
                        "status": {"completionTime": "1970-01-01T00:00:09Z"},
                    },
                    {
                        "kind": "Node",
                        "metadata": _metadata("node-a", "node-uid"),
                        "status": {},
                    },
                    {
                        "kind": "Event",
                        "metadata": _metadata(
                            "scheduled", "event-uid", creationTimestamp="1970-01-01T00:00:03Z"
                        ),
                        "involvedObject": {"uid": "pod-uid"},
                        "reason": "Scheduled",
                        "message": "Assigned demo/worker-0 to node-a",
                        "type": "Normal",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_kubernetes_snapshot(base, snapshot, enriched)

    assert result.entity_count == 4
    assert result.event_count == 5
    assert result.edge_count == 5
    assert result.correlation_count == 1
    summary = inspect_runpack(enriched)
    analysis = analyze_runpack(enriched)
    assert summary.started_at_ns == 1_000_000_000
    assert summary.finished_at_ns == 9_000_000_000
    assert [(phase.name, phase.duration_seconds, phase.source) for phase in analysis.lifecycle] == [
        ("provisioning", 1.0, "derived"),
        ("starting", 2.0, "derived"),
        ("executing", 4.0, "derived"),
        ("cleanup", 1.0, "derived"),
    ]
    with RunpackReader(enriched) as reader:
        entities = {entity.name: entity for entity in reader.entities()}
        container = next(entity for entity in reader.entities() if entity.kind == "container")
        events = reader.events()
        edges = reader.causal_edges()
    assert entities["worker-0"].parent_entity_id == entities["demo-job"].id
    assert container.parent_entity_id == entities["worker-0"].id
    assert container.attributes["resources.requests"] == {
        "cpu": "500m",
        "memory": "1Gi",
    }
    assert container.attributes["restart_count"] == 2
    assert entities["worker-0"].attributes["k8s.node.name"] == "node-a"
    assert entities["worker-0"].attributes["k8s.labels"] == {"component": "worker"}
    assert entities["worker-0"].attributes["k8s.annotations"] == {
        "example.dev/owner": "runtime-team"
    }
    assert any(event.name == "Scheduled" and event.kind == "kubernetes.event" for event in events)
    assert any(
        event.name == "worker"
        and event.kind == "workload.container"
        and event.started_at_ns == 4_000_000_000
        and event.finished_at_ns == 8_000_000_000
        for event in events
    )
    assert any(edge.kind == "correlates" for edge in edges)
    assert any(
        edge.kind == "hosts"
        and edge.source_event_id == "k8s:node-uid:lifecycle"
        and edge.target_event_id == "k8s:pod-uid:lifecycle"
        for edge in edges
    )
    assert base.is_file()


def test_kubernetes_correlations_use_the_copied_runpack_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    snapshot = tmp_path / "kubernetes.json"
    output = tmp_path / "output.runpack"
    for path, execution_name, correlated_entity_id in (
        (source, "snapshot-a", "service-a"),
        (replacement, "snapshot-b", "service-b"),
    ):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(execution_name, execution_name, 0, 10, (), str(tmp_path), 0, None, {})
            )
            writer.add_entities(
                tuple(
                    Entity(
                        entity_id,
                        "service",
                        entity_id,
                        None,
                        {"k8s.pod.uid": "pod-race"} if entity_id == correlated_entity_id else {},
                    )
                    for entity_id in ("service-a", "service-b")
                )
            )
            writer.add_events(
                tuple(
                    Event(
                        f"root-{suffix}",
                        "operation",
                        f"root-{suffix}",
                        f"service-{suffix}",
                        1,
                        2,
                        "test",
                        None,
                        None,
                        {},
                    )
                    for suffix in ("a", "b")
                )
            )
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod-race"),
                        "spec": {"containers": []},
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    def replace_source_then_enrich(
        source_path: Path,
        output_path: Path,
        operation: Callable[[RunpackWriter], KubernetesImportResult],
    ) -> KubernetesImportResult:
        replacement.replace(source_path)
        return real_enrich_copy(source_path, output_path, operation)

    monkeypatch.setattr(kubernetes, "enrich_copy", replace_source_then_enrich)

    result = import_kubernetes_snapshot(source, snapshot, output)

    with RunpackReader(output) as reader:
        execution = reader.execution()
        correlations = tuple(edge for edge in reader.causal_edges() if edge.kind == "correlates")
    assert execution.id == "snapshot-b"
    assert result.correlation_count == 1
    assert {edge.target_event_id for edge in correlations} == {"root-b"}


def test_kubernetes_completion_only_evidence_expands_execution_bounds(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "completion.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Job",
                        "metadata": _metadata("job", "job-uid"),
                        "status": {"completionTime": "1970-01-01T00:00:00.000000020Z"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        execution = reader.execution()
        event = reader.events()[0]
    assert execution.started_at_ns == 0
    assert execution.finished_at_ns == 20
    assert event.started_at_ns is None
    assert event.finished_at_ns == 20


def test_kubernetes_start_only_evidence_expands_closed_execution_bounds(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "live-pod.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata(
                            "worker",
                            "pod-uid",
                            creationTimestamp="1970-01-01T00:00:00.000000020Z",
                        ),
                        "spec": {"containers": []},
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        execution = reader.execution()
        event = reader.events()[0]
    assert execution.finished_at_ns == 20
    assert event.started_at_ns == 20
    assert event.finished_at_ns is None


def test_kubernetes_snapshot_keeps_delimited_entity_identities_distinct(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "delimited-identities.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("first", "pod"),
                        "spec": {"containers": [{"name": "worker"}]},
                    },
                    {
                        "kind": "Pod",
                        "metadata": _metadata("second", "pod:container:worker"),
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        entity_ids = {entity.id for entity in reader.entities()}
    assert {
        "k8s:pod:pod",
        "k8s:pod:pod:container:worker",
        "k8s:pod:pod%3Acontainer%3Aworker",
    } <= entity_ids


def test_kubernetes_snapshot_preserves_deployment_owner_chains(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "deployment.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 4, 5, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata(
                            "api-pod",
                            "pod-uid",
                            namespace="demo",
                            creationTimestamp="1970-01-01T00:00:00.000000003Z",
                            ownerReferences=[
                                {"uid": "ignored-uid", "kind": "ConfigMap"},
                                {"uid": "replicaset-uid", "kind": "ReplicaSet", "controller": True},
                            ],
                        ),
                        "spec": {"containers": []},
                        "status": {"phase": "Running"},
                    },
                    {
                        "kind": "ReplicaSet",
                        "metadata": _metadata(
                            "api-7d9f",
                            "replicaset-uid",
                            namespace="demo",
                            creationTimestamp="1970-01-01T00:00:00.000000002Z",
                            ownerReferences=[
                                {"uid": "deployment-uid", "kind": "Deployment", "controller": True}
                            ],
                        ),
                        "spec": {"replicas": 2},
                        "status": {"replicas": 2, "readyReplicas": 1},
                    },
                    {
                        "kind": "Deployment",
                        "metadata": _metadata(
                            "api",
                            "deployment-uid",
                            namespace="demo",
                            creationTimestamp="1970-01-01T00:00:00.000000001Z",
                        ),
                        "spec": {"replicas": 2},
                        "status": {"replicas": 2, "readyReplicas": 1, "availableReplicas": 1},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_kubernetes_snapshot(base, snapshot, output)

    assert (result.entity_count, result.event_count, result.edge_count) == (3, 3, 2)
    with RunpackReader(output) as reader:
        entities = {entity.name: entity for entity in reader.entities()}
        events = {event.name: event for event in reader.events()}
        edges = reader.causal_edges()
    assert entities["api-7d9f"].parent_entity_id == entities["api"].id
    assert entities["api-pod"].parent_entity_id == entities["api-7d9f"].id
    assert entities["api"].attributes["k8s.replicas.desired"] == 2
    assert entities["api"].attributes["k8s.replicas.ready"] == 1
    assert entities["api-7d9f"].attributes["k8s.replicas.current"] == 2
    assert events["api"].kind == "workload.deployment"
    assert events["api-7d9f"].kind == "workload.replicaset"
    assert {(edge.source_event_id, edge.target_event_id, edge.kind) for edge in edges} == {
        ("k8s:deployment-uid:lifecycle", "k8s:replicaset-uid:lifecycle", "owns"),
        ("k8s:replicaset-uid:lifecycle", "k8s:pod-uid:lifecycle", "owns"),
    }


def test_kubernetes_does_not_guess_between_non_controller_owners() -> None:
    item: dict[str, object] = {
        "kind": "Pod",
        "metadata": {
            "name": "worker",
            "ownerReferences": [{"uid": "job-a"}, {"uid": "job-b"}],
        },
    }

    assert kubernetes._owner_uid(item) is None


def test_kubernetes_rejects_multiple_controller_owners() -> None:
    item: dict[str, object] = {
        "kind": "Pod",
        "metadata": {
            "name": "worker",
            "ownerReferences": [
                {"uid": "job-a", "controller": True},
                {"uid": "job-b", "controller": True},
            ],
        },
    }

    with pytest.raises(KubernetesImportError, match="multiple controllers"):
        kubernetes._owner_uid(item)


def test_kubernetes_rejects_duplicate_owner_references() -> None:
    item: dict[str, object] = {
        "kind": "Pod",
        "metadata": {
            "name": "worker",
            "ownerReferences": [{"uid": "job"}, {"uid": "job"}],
        },
    }

    with pytest.raises(KubernetesImportError, match="duplicate ownerReferences uid: job"):
        kubernetes._owner_uid(item)


def test_kubernetes_snapshot_rejects_self_ownership_before_enrichment(
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "self-owner.json"
    output = tmp_path / "output.runpack"
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata(
                            "worker",
                            "pod",
                            ownerReferences=[{"uid": "pod", "controller": True}],
                        ),
                        "spec": {"containers": []},
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="object cannot own itself: pod"):
        import_kubernetes_snapshot(tmp_path / "missing.runpack", snapshot, output)

    assert not output.exists()


def test_kubernetes_snapshot_rejects_owner_cycles_before_enrichment(
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "owner-cycle.json"
    output = tmp_path / "output.runpack"
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Job",
                        "metadata": _metadata(
                            "first",
                            "first",
                            ownerReferences=[{"uid": "second", "controller": True}],
                        ),
                        "status": {},
                    },
                    {
                        "kind": "Job",
                        "metadata": _metadata(
                            "second",
                            "second",
                            ownerReferences=[{"uid": "first", "controller": True}],
                        ),
                        "status": {},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="owner relationships contain a cycle"):
        import_kubernetes_snapshot(tmp_path / "missing.runpack", snapshot, output)

    assert not output.exists()


def test_kubernetes_correlates_cross_service_trace_roots_to_each_pod(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "pods.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
        writer.add_entities(
            (
                Entity("frontend", "service", "frontend", None, {"k8s.pod.uid": "pod-a"}),
                Entity("backend", "service", "backend", None, {"k8s.pod.uid": "pod-b"}),
            )
        )
        writer.add_events(
            (
                Event("request", "client.request", "call", "frontend", 1, 9, None, None, None, {}),
                Event("handler", "server.request", "handle", "backend", 2, 8, None, None, None, {}),
            )
        )
        writer.add_causal_edge(CausalEdge("request", "handler", "parent", 1.0, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("frontend", "pod-a"),
                        "spec": {"containers": []},
                        "status": {},
                    },
                    {
                        "kind": "Pod",
                        "metadata": _metadata("backend", "pod-b"),
                        "spec": {"containers": []},
                        "status": {},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_kubernetes_snapshot(base, snapshot, output)

    assert result.correlation_count == 2
    with RunpackReader(output) as reader:
        correlations = {
            edge.target_event_id for edge in reader.causal_edges() if edge.kind == "correlates"
        }
    assert correlations == {"request", "handler"}


def test_kubernetes_correlates_every_independent_root_in_a_pod(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "pod.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "service", "worker", None, {"k8s.pod.uid": "pod-uid"}))
        writer.add_events(
            (
                Event("root-a", "server.request", "a", "worker", 1, 4, None, None, None, {}),
                Event("root-b", "server.request", "b", "worker", 6, 9, None, None, None, {}),
            )
        )
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod-uid"),
                        "spec": {"containers": []},
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_kubernetes_snapshot(base, snapshot, output)

    assert result.correlation_count == 2
    with RunpackReader(output) as reader:
        correlations = {
            edge.target_event_id for edge in reader.causal_edges() if edge.kind == "correlates"
        }
    assert correlations == {"root-a", "root-b"}


def test_kubernetes_snapshot_rejects_timezone_ambiguous_timestamps(tmp_path: Path) -> None:
    trace = tmp_path / "trace.json"
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "kubernetes.json"
    output = tmp_path / "output.runpack"
    trace.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": _trace_id("trace"),
                                        "spanId": _span_id("span"),
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    import_otlp_json(trace, base, name="run")
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": {
                            "name": "pod",
                            "uid": "pod",
                            "creationTimestamp": "2026-08-17T12:00:00",
                        },
                        "spec": {"containers": []},
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="requires a timezone"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("metadata", "spec", "status", "message"),
    (
        (
            {"name": "pod", "uid": "pod", "creationTimestamp": 123},
            {"containers": []},
            {},
            "Kubernetes timestamp must be an RFC 3339 string",
        ),
        (
            {"name": "deployment", "uid": "deployment"},
            {"replicas": -1},
            {},
            "k8s.replicas.desired must be a non-negative integer",
        ),
        (
            {"name": "deployment", "uid": "deployment"},
            {"replicas": 1 << 63},
            {},
            "k8s.replicas.desired exceeds the runpack integer range",
        ),
    ),
)
def test_kubernetes_snapshot_rejects_malformed_lifecycle_scalars(
    tmp_path: Path,
    metadata: dict[str, object],
    spec: dict[str, object],
    status: dict[str, object],
    message: str,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "malformed-lifecycle.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    kind = "Deployment" if metadata["name"] == "deployment" else "Pod"
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": kind,
                        "metadata": metadata,
                        "spec": spec,
                        "status": status,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match=message):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


def test_kubernetes_enrichment_reports_identity_collisions_without_publishing(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "duplicate.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {"kind": "Node", "metadata": _metadata("first", "duplicate"), "status": {}},
                    {
                        "kind": "Node",
                        "metadata": _metadata("second", "duplicate"),
                        "status": {},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="duplicate Kubernetes object uid: duplicate"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


def test_kubernetes_enrichment_rejects_duplicate_event_uids(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "duplicate-events.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Event",
                        "metadata": _metadata("first", "duplicate"),
                        "involvedObject": {"uid": "missing"},
                    },
                    {
                        "kind": "Event",
                        "metadata": _metadata("second", "duplicate"),
                        "involvedObject": {"uid": "missing"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="duplicate Kubernetes Event uid: duplicate"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


@pytest.mark.parametrize("order", tuple(permutations(("first-event", "second-event", "pod"))))
def test_kubernetes_enrichment_rejects_uids_shared_by_workloads_and_events(
    tmp_path: Path,
    order: tuple[str, str, str],
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "cross-kind-duplicate.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    pod = {
        "kind": "Pod",
        "metadata": _metadata("worker", "shared"),
        "spec": {"containers": []},
        "status": {},
    }
    items_by_name = {
        "first-event": {
            "kind": "Event",
            "metadata": _metadata("first", "shared"),
            "involvedObject": {"uid": "shared"},
        },
        "second-event": {
            "kind": "Event",
            "metadata": _metadata("second", "shared"),
            "involvedObject": {"uid": "shared"},
        },
        "pod": pod,
    }
    items = [items_by_name[name] for name in order]
    snapshot.write_text(json.dumps({"items": items}), encoding="utf-8")

    with pytest.raises(KubernetesImportError, match="duplicate Kubernetes object uid: shared"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


@pytest.mark.parametrize("reverse", (False, True))
def test_kubernetes_enrichment_reports_the_first_duplicate_uid_deterministically(
    tmp_path: Path,
    reverse: bool,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "multiple-duplicates.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    items = [
        {
            "kind": "Event",
            "metadata": _metadata("event-one", "z-duplicate"),
            "involvedObject": {"uid": "missing"},
        },
        {
            "kind": "Event",
            "metadata": _metadata("event-two", "z-duplicate"),
            "involvedObject": {"uid": "missing"},
        },
        {"kind": "Node", "metadata": _metadata("node-one", "a-duplicate"), "status": {}},
        {"kind": "Node", "metadata": _metadata("node-two", "a-duplicate"), "status": {}},
    ]
    snapshot.write_text(
        json.dumps({"items": list(reversed(items)) if reverse else items}), encoding="utf-8"
    )

    with pytest.raises(KubernetesImportError, match="duplicate Kubernetes object uid: a-duplicate"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("items", "message"),
    (
        (
            [
                {"kind": "Node", "metadata": _metadata("node-a", "node-1"), "status": {}},
                {"kind": "Node", "metadata": _metadata("node-a", "node-2"), "status": {}},
            ],
            "duplicate Kubernetes Node name: node-a",
        ),
        (
            [
                {
                    "kind": "Pod",
                    "metadata": _metadata("worker", "pod"),
                    "spec": {
                        "containers": [
                            {"name": "task", "resources": {}},
                            {"name": "task", "resources": {}},
                        ]
                    },
                    "status": {},
                }
            ],
            "duplicate container name: task",
        ),
        (
            [
                {
                    "kind": "Pod",
                    "metadata": _metadata("worker", "pod"),
                    "spec": {"containers": [{"name": "task", "resources": {}}]},
                    "status": {
                        "containerStatuses": [{"name": "other", "restartCount": 3, "state": {}}]
                    },
                }
            ],
            "containerStatuses contains names absent from Pod containers: other",
        ),
    ),
)
def test_kubernetes_snapshot_rejects_ambiguous_workload_names(
    tmp_path: Path,
    items: list[dict[str, object]],
    message: str,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "ambiguous.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(json.dumps({"items": items}), encoding="utf-8")

    with pytest.raises(KubernetesImportError, match=message):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


def test_kubernetes_snapshot_preserves_rfc3339_nanoseconds(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "nanoseconds.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata(
                            "worker",
                            "pod",
                            creationTimestamp="1970-01-01T00:00:00.123456789Z",
                        ),
                        "spec": {"containers": []},
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        pod = next(event for event in reader.events() if event.kind == "workload.pod")
    assert pod.started_at_ns == 123_456_789


def test_kubernetes_snapshot_keeps_partially_terminated_pods_open(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "running-pod.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod"),
                        "spec": {
                            "containers": [
                                {"name": "finished", "resources": {}},
                                {"name": "running", "resources": {}},
                            ]
                        },
                        "status": {
                            "containerStatuses": [
                                {
                                    "name": "finished",
                                    "state": {"terminated": {"finishedAt": "1970-01-01T00:00:01Z"}},
                                },
                                {
                                    "name": "running",
                                    "state": {"running": {"startedAt": "1970-01-01T00:00:00Z"}},
                                },
                            ]
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        pod = next(event for event in reader.events() if event.kind == "workload.pod")
    assert pod.finished_at_ns is None


def test_kubernetes_snapshot_keeps_pods_with_missing_container_statuses_open(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "incomplete-statuses.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod"),
                        "spec": {
                            "containers": [
                                {"name": "reported", "resources": {}},
                                {"name": "omitted", "resources": {}},
                            ]
                        },
                        "status": {
                            "containerStatuses": [
                                {
                                    "name": "reported",
                                    "state": {"terminated": {"finishedAt": "1970-01-01T00:00:01Z"}},
                                }
                            ]
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        pod = next(event for event in reader.events() if event.kind == "workload.pod")
    assert pod.finished_at_ns is None


def test_kubernetes_snapshot_rejects_malformed_running_container_state(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "malformed-running.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod"),
                        "spec": {"containers": [{"name": "worker", "resources": {}}]},
                        "status": {
                            "containerStatuses": [
                                {"name": "worker", "state": {"running": "invalid"}}
                            ]
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="running container state must be an object"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("state", "message"),
    (
        ({"waiting": "invalid"}, "waiting container state must be an object"),
        (
            {"running": {}, "terminated": {}},
            "container state cannot contain more than one",
        ),
    ),
)
def test_kubernetes_snapshot_rejects_invalid_container_state_unions(
    tmp_path: Path,
    state: dict[str, object],
    message: str,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "invalid-state.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod"),
                        "spec": {"containers": [{"name": "worker", "resources": {}}]},
                        "status": {"containerStatuses": [{"name": "worker", "state": state}]},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match=message):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


def test_kubernetes_snapshot_bounds_aggregate_restart_counts(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "restart-overflow.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod"),
                        "spec": {
                            "containers": [
                                {"name": "first", "resources": {}},
                                {"name": "second", "resources": {}},
                            ]
                        },
                        "status": {
                            "containerStatuses": [
                                {"name": "first", "restartCount": (1 << 63) - 1},
                                {"name": "second", "restartCount": 1},
                            ]
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="total container restart count exceeds"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


def test_kubernetes_fallback_identity_includes_namespace(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "namespaces.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": {"name": "worker", "namespace": namespace},
                        "spec": {"containers": []},
                        "status": {},
                    }
                    for namespace in ("first", "second")
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        pods = tuple(entity for entity in reader.entities() if entity.kind == "pod")
    assert {pod.attributes["k8s.uid"] for pod in pods} == {
        "name:Pod:first:worker",
        "name:Pod:second:worker",
    }


def test_kubernetes_event_preserves_epoch_timestamp_over_later_fallback(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "epoch-event.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod"),
                        "spec": {"containers": []},
                        "status": {},
                    },
                    {
                        "kind": "Event",
                        "metadata": _metadata(
                            "started", "event", creationTimestamp="1970-01-01T00:00:01Z"
                        ),
                        "involvedObject": {"uid": "pod"},
                        "eventTime": "1970-01-01T00:00:00Z",
                        "reason": "Started",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(base, snapshot, output)

    with RunpackReader(output) as reader:
        event = next(item for item in reader.events() if item.kind == "kubernetes.event")
    assert event.started_at_ns == 0
    assert event.finished_at_ns == 0


def test_kubernetes_snapshot_rejects_non_standard_json_constants(tmp_path: Path) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "non-standard.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base):
        pass
    snapshot.write_text('{"items":[],"invalid":Infinity}', encoding="utf-8")

    with pytest.raises(KubernetesImportError, match="non-finite JSON constant: Infinity"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


def test_kubernetes_snapshot_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    snapshot = tmp_path / "duplicate-keys.json"
    snapshot.write_text('{"items":[],"items":[]}', encoding="utf-8")

    with pytest.raises(KubernetesImportError, match="duplicate JSON key: items"):
        import_kubernetes_snapshot(
            tmp_path / "missing.runpack", snapshot, tmp_path / "output.runpack"
        )


@pytest.mark.parametrize(
    ("metadata", "message"),
    (
        ({"name": "pod", "uid": {"unexpected": "object"}}, "metadata.uid must be a string"),
        ({"name": "pod", "namespace": 7}, "metadata.namespace must be a string"),
        ({"name": "pod", "labels": {"team": 7}}, "metadata.labels must map strings"),
        (
            {"name": "pod", "annotations": {"example.dev/owner": 7}},
            "metadata.annotations must map strings",
        ),
    ),
)
def test_kubernetes_snapshot_rejects_malformed_identity_metadata(
    tmp_path: Path,
    metadata: dict[str, object],
    message: str,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "malformed-metadata.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": metadata,
                        "spec": {"containers": []},
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match=message):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("kind", "Kubernetes kind must be a non-empty string"),
        ("owner", "ownerReferences uid must be a non-empty string"),
        ("container", "container name must be a non-empty string"),
        ("resources", "container resource quantities must map strings to strings"),
    ),
)
def test_kubernetes_snapshot_rejects_malformed_relationship_text(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    base = tmp_path / "base.runpack"
    snapshot = tmp_path / "malformed-relationships.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(base) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    item: dict[str, object] = {
        "kind": "Pod",
        "metadata": _metadata("pod", "pod"),
        "spec": {
            "containers": [
                {
                    "name": "worker",
                    "resources": {"requests": {"cpu": "1"}},
                }
            ]
        },
        "status": {},
    }
    if case == "kind":
        item["kind"] = {"unexpected": "object"}
    elif case == "owner":
        metadata = item["metadata"]
        assert isinstance(metadata, dict)
        metadata["ownerReferences"] = [{"uid": 7}]
    else:
        spec = item["spec"]
        assert isinstance(spec, dict)
        containers = spec["containers"]
        assert isinstance(containers, list)
        container = cast(dict[str, object], containers[0])
        if case == "container":
            container["name"] = {"unexpected": "object"}
        else:
            container["resources"] = {"requests": {"cpu": 1}}
    snapshot.write_text(json.dumps({"items": [item]}), encoding="utf-8")

    with pytest.raises(KubernetesImportError, match=message):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()


def test_kubernetes_snapshot_rejects_oversized_sources_before_decoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "oversized.json"
    snapshot.write_bytes(b"{" + b" " * 32 + b"}")
    monkeypatch.setattr(kubernetes, "MAX_KUBERNETES_SNAPSHOT_BYTES", 32)

    with pytest.raises(
        KubernetesImportError,
        match="Kubernetes snapshot exceeds the 32-byte input limit",
    ):
        import_kubernetes_snapshot(
            tmp_path / "missing.runpack", snapshot, tmp_path / "output.runpack"
        )


def test_kubernetes_snapshot_rejects_too_many_items_before_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "too-many-items.json"
    snapshot.write_text(json.dumps({"items": [{}, {}]}), encoding="utf-8")
    monkeypatch.setattr(kubernetes, "MAX_KUBERNETES_ITEMS", 1)

    with pytest.raises(
        KubernetesImportError,
        match="Kubernetes snapshot exceeds the 1-item input limit",
    ):
        import_kubernetes_snapshot(
            tmp_path / "missing.runpack", snapshot, tmp_path / "output.runpack"
        )


def test_kubernetes_snapshot_rejects_too_many_containers_before_enrichment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "too-many-containers.json"
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": _metadata("worker", "pod"),
                        "spec": {
                            "containers": [
                                {"name": "one", "resources": {}},
                                {"name": "two", "resources": {}},
                            ]
                        },
                        "status": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(kubernetes, "MAX_KUBERNETES_CONTAINERS", 1)

    with pytest.raises(
        KubernetesImportError,
        match="Kubernetes snapshot exceeds the 1-container input limit",
    ):
        import_kubernetes_snapshot(
            tmp_path / "missing.runpack", snapshot, tmp_path / "output.runpack"
        )


def test_kubernetes_snapshot_normalizes_invalid_utf8(tmp_path: Path) -> None:
    snapshot = tmp_path / "invalid-utf8.json"
    snapshot.write_bytes(b"\xff")

    with pytest.raises(KubernetesImportError, match="Kubernetes snapshot must be UTF-8"):
        import_kubernetes_snapshot(
            tmp_path / "missing.runpack", snapshot, tmp_path / "output.runpack"
        )


def test_kubernetes_snapshot_rejects_invalid_identity_unicode_at_the_adapter_boundary(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "base.runpack"
    snapshot = tmp_path / "surrogate.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Pod",
                        "metadata": {"name": "bad-\ud800", "uid": "pod"},
                        "spec": {"containers": []},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(KubernetesImportError, match="metadata.name must be valid UTF-8"):
        import_kubernetes_snapshot(runpack, snapshot, output)

    assert not output.exists()


def test_kubernetes_snapshot_normalizes_excessive_document_nesting(tmp_path: Path) -> None:
    snapshot = tmp_path / "nested.json"
    snapshot.write_text("[" * 10_000 + "0" + "]" * 10_000, encoding="utf-8")

    with pytest.raises(KubernetesImportError, match="Kubernetes JSON nesting is too deep"):
        import_kubernetes_snapshot(
            tmp_path / "missing.runpack", snapshot, tmp_path / "output.runpack"
        )
