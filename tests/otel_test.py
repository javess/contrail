from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime_tools.inspect import inspect_runpack, render_causal_tree
from runtime_tools.otel import OtelImportError, import_otlp_json
from runtime_tools.storage import RunpackReader


def _attribute(key: str, value: str) -> dict[str, object]:
    return {"key": key, "value": {"stringValue": value}}


def test_otlp_json_import_normalizes_services_spans_and_parent_edges(tmp_path: Path) -> None:
    source = tmp_path / "trace.json"
    output = tmp_path / "trace.runpack"
    source.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "resource": {"attributes": [_attribute("service.name", "gateway")]},
                        "scopeSpans": [
                            {
                                "scope": {"name": "demo.http"},
                                "spans": [
                                    {
                                        "traceId": "trace-1",
                                        "spanId": "root",
                                        "name": "GET /items",
                                        "kind": "SPAN_KIND_SERVER",
                                        "startTimeUnixNano": "1000000",
                                        "endTimeUnixNano": "9000000",
                                        "attributes": [_attribute("http.request.method", "GET")],
                                    },
                                    {
                                        "traceId": "trace-1",
                                        "spanId": "lookup",
                                        "parentSpanId": "root",
                                        "name": "SELECT items",
                                        "kind": "SPAN_KIND_CLIENT",
                                        "startTimeUnixNano": "3000000",
                                        "endTimeUnixNano": "7000000",
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "resource": {"attributes": [_attribute("service.name", "worker")]},
                        "scopeSpans": [
                            {
                                "scope": {"name": "demo.worker"},
                                "spans": [
                                    {
                                        "traceId": "trace-1",
                                        "spanId": "work",
                                        "parentSpanId": "root",
                                        "name": "process item",
                                        "kind": 1,
                                        "startTimeUnixNano": "8000000",
                                        "endTimeUnixNano": "10000000",
                                    }
                                ],
                            }
                        ],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_otlp_json(source, output, name="trace-demo")

    summary = inspect_runpack(output)
    assert result.entity_count == 2
    assert result.event_count == 3
    assert result.edge_count == 2
    assert result.missing_parent_count == 0
    assert summary.started_at_ns == 1_000_000
    assert summary.finished_at_ns == 10_000_000
    assert summary.wall_time_seconds == 0.009
    assert summary.record_counts["entities"] == 2
    assert summary.record_counts["events"] == 3
    assert summary.record_counts["causal_edges"] == 2
    with RunpackReader(output) as reader:
        events = {event.name: event for event in reader.events()}
        assert events["GET /items"].kind == "server.request"
        assert events["SELECT items"].kind == "client.request"
        assert events["GET /items"].attributes["http.request.method"] == "GET"
        assert reader.clock_inconsistency_count() == 1

    tree = render_causal_tree(output)
    assert "gateway :: GET /items [server.request] 8.000ms" in tree
    assert "  gateway :: SELECT items [client.request] 4.000ms" in tree
    assert "  worker :: process item [operation] 2.000ms" in tree
    assert "clock inconsistencies: 1" in tree


def test_otlp_json_import_rejects_an_impossible_local_interval(tmp_path: Path) -> None:
    source = tmp_path / "invalid.json"
    output = tmp_path / "invalid.runpack"
    source.write_text(
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
                                        "startTimeUnixNano": "2000",
                                        "endTimeUnixNano": "1000",
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

    with pytest.raises(OtelImportError, match="ends before it starts"):
        import_otlp_json(source, output, name="invalid")

    assert not output.exists()
