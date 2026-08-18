from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.inspect import inspect_runpack
from runtime_tools.kubernetes import KubernetesImportError, import_kubernetes_snapshot
from runtime_tools.model import CausalEdge, Entity, Event, Execution
from runtime_tools.otel import import_otlp_json
from runtime_tools.storage import RunpackReader, RunpackWriter


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
                                        "traceId": "trace",
                                        "spanId": "work",
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
                                        "traceId": "trace",
                                        "spanId": "span",
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


@pytest.mark.parametrize(
    ("metadata", "message"),
    (
        ({"name": "pod", "uid": {"unexpected": "object"}}, "metadata.uid must be a string"),
        ({"name": "pod", "namespace": 7}, "metadata.namespace must be a string"),
        ({"name": "pod", "labels": {"team": 7}}, "metadata.labels must map strings"),
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
        container = containers[0]
        assert isinstance(container, dict)
        if case == "container":
            container["name"] = {"unexpected": "object"}
        else:
            container["resources"] = {"requests": {"cpu": 1}}
    snapshot.write_text(json.dumps({"items": [item]}), encoding="utf-8")

    with pytest.raises(KubernetesImportError, match=message):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()
