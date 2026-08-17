from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.inspect import inspect_runpack
from runtime_tools.kubernetes import KubernetesImportError, import_kubernetes_snapshot
from runtime_tools.model import Execution
from runtime_tools.otel import import_otlp_json
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter


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

    with pytest.raises(RunpackError, match="UNIQUE constraint failed"):
        import_kubernetes_snapshot(base, snapshot, output)

    assert not output.exists()
