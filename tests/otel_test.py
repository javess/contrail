from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime_tools import otel
from runtime_tools.inspect import inspect_runpack, render_causal_tree, render_summary
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
                                "scope": {
                                    "name": "demo.http",
                                    "version": "1.2.3",
                                    "attributes": [_attribute("schema", "stable")],
                                },
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
        assert events["GET /items"].attributes["otel.scope.version"] == "1.2.3"
        assert events["GET /items"].attributes["otel.scope.attributes"] == {"schema": "stable"}
        assert reader.clock_inconsistency_count() == 1

    tree = render_causal_tree(output)
    assert "gateway :: GET /items [server.request] 8.000ms" in tree
    assert "  gateway :: SELECT items [client.request] 4.000ms" in tree
    assert "  worker :: process item [operation] 2.000ms" in tree
    assert "clock inconsistencies: 1" in tree


def test_otlp_json_import_normalizes_numeric_enum_strings_and_error_status(
    tmp_path: Path,
) -> None:
    source = tmp_path / "status.json"
    output = tmp_path / "status.runpack"
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
                                        "name": "write",
                                        "kind": "3",
                                        "status": {"code": "STATUS_CODE_ERROR"},
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

    import_otlp_json(source, output, name="status")

    with RunpackReader(output) as reader:
        event = reader.events()[0]
        errors = reader.operation_error_counts()
    assert event.kind == "client.request"
    assert event.attributes["otel.status.code"] == "STATUS_CODE_ERROR"
    assert sum(errors.values()) == 1


@pytest.mark.parametrize(("field", "value"), (("kind", True), ("status", "invalid")))
def test_otlp_json_import_rejects_malformed_span_semantics(
    tmp_path: Path, field: str, value: object
) -> None:
    source = tmp_path / f"invalid-{field}.json"
    output = tmp_path / f"invalid-{field}.runpack"
    span: dict[str, object] = {
        "traceId": "trace",
        "spanId": "span",
        "startTimeUnixNano": "1",
        "endTimeUnixNano": "2",
        field: value,
    }
    source.write_text(
        json.dumps({"resourceSpans": [{"scopeSpans": [{"spans": [span]}]}]}),
        encoding="utf-8",
    )

    with pytest.raises(OtelImportError):
        import_otlp_json(source, output, name="invalid")

    assert not output.exists()


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


def test_otlp_json_import_rejects_timestamps_outside_runpack_range(tmp_path: Path) -> None:
    source = tmp_path / "out-of-range.json"
    output = tmp_path / "out-of-range.runpack"
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
                                        "startTimeUnixNano": str(2**63),
                                        "endTimeUnixNano": str(2**63),
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

    with pytest.raises(OtelImportError, match="exceeds the runpack timestamp range"):
        import_otlp_json(source, output, name="out-of-range")

    assert not output.exists()


def test_otlp_json_bulk_import_handles_ten_thousand_spans(tmp_path: Path) -> None:
    source = tmp_path / "large-trace.json"
    output = tmp_path / "large-trace.runpack"
    span_count = 10_000
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
                                        "spanId": str(index),
                                        "name": "work",
                                        "startTimeUnixNano": str(index + 1),
                                        "endTimeUnixNano": str(index + 2),
                                    }
                                    for index in range(span_count)
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_otlp_json(source, output, name="large-trace")

    assert result.event_count == span_count
    with RunpackReader(output) as reader:
        assert reader.counts()["events"] == span_count


def test_otlp_json_import_normalizes_asynchronous_span_links(tmp_path: Path) -> None:
    source = tmp_path / "links.json"
    output = tmp_path / "links.runpack"
    source.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": "producer-trace",
                                        "spanId": "publish",
                                        "name": "publish",
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                    },
                                    {
                                        "traceId": "consumer-trace",
                                        "spanId": "consume",
                                        "name": "consume",
                                        "startTimeUnixNano": "3",
                                        "endTimeUnixNano": "4",
                                        "links": [
                                            {
                                                "traceId": "producer-trace",
                                                "spanId": "publish",
                                                "attributes": [
                                                    {
                                                        "key": "messaging.message.id",
                                                        "value": {"stringValue": "message-1"},
                                                    }
                                                ],
                                            }
                                        ],
                                    },
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_otlp_json(source, output, name="async")

    assert result.edge_count == 1
    assert result.missing_link_count == 0
    with RunpackReader(output) as reader:
        edge = reader.causal_edges()[0]
    assert edge.kind == "link"
    assert edge.source_event_id == "otel:producer-trace:publish"
    assert edge.target_event_id == "otel:consumer-trace:consume"
    assert edge.attributes["otel.link.attributes"] == {"messaging.message.id": "message-1"}
    tree = render_causal_tree(output)
    assert "CAUSAL LINKS" in tree
    assert "publish → consume [link, confidence 1.00]" in tree


def test_otlp_json_import_reports_exporter_dropped_links_as_incomplete(
    tmp_path: Path,
) -> None:
    source = tmp_path / "dropped-links.json"
    output = tmp_path / "dropped-links.runpack"
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "droppedLinksCount": "2",
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

    result = import_otlp_json(source, output, name="dropped-links")

    assert result.missing_link_count == 2
    assert inspect_runpack(output).missing_causal_references == 2


def test_otlp_json_import_surfaces_exporter_dropped_attributes(tmp_path: Path) -> None:
    source = tmp_path / "dropped-attributes.json"
    output = tmp_path / "dropped-attributes.runpack"
    source.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "resource": {"droppedAttributesCount": 1},
                        "scopeSpans": [
                            {
                                "scope": {"droppedAttributesCount": 4},
                                "spans": [
                                    {
                                        "traceId": "trace",
                                        "spanId": "span",
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "droppedAttributesCount": "2",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_otlp_json(source, output, name="dropped-attributes")

    summary = inspect_runpack(output)
    assert summary.dropped_attribute_count == 7
    assert "semantics: 7 exporter-dropped OTLP attributes" in render_summary(summary, "text")


@pytest.mark.parametrize("code", (True, -1, 3, "INVALID", ""))
def test_otlp_json_import_rejects_invalid_span_status_codes(
    tmp_path: Path,
    code: object,
) -> None:
    source = tmp_path / "invalid-status.json"
    output = tmp_path / "invalid-status.runpack"
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "status": {"code": code},
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

    with pytest.raises(OtelImportError, match="unsupported OTLP span status code"):
        import_otlp_json(source, output, name="invalid")

    assert not output.exists()


@pytest.mark.parametrize(
    ("code", "normalized"),
    ((0, "STATUS_CODE_UNSET"), ("1", "STATUS_CODE_OK"), (2, "STATUS_CODE_ERROR")),
)
def test_otlp_json_import_normalizes_numeric_span_status_codes(
    tmp_path: Path,
    code: object,
    normalized: str,
) -> None:
    source = tmp_path / "status.json"
    output = tmp_path / "status.runpack"
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "status": {"code": code},
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

    import_otlp_json(source, output, name="status")

    with RunpackReader(output) as reader:
        event = reader.events()[0]
    assert event.attributes["otel.status.code"] == normalized


def test_otlp_json_import_rejects_invalid_dropped_link_counts(tmp_path: Path) -> None:
    source = tmp_path / "invalid-dropped-links.json"
    output = tmp_path / "invalid-dropped-links.runpack"
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "droppedLinksCount": -1,
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

    with pytest.raises(OtelImportError, match="droppedLinksCount must be a non-negative"):
        import_otlp_json(source, output, name="invalid")

    assert not output.exists()


def test_otlp_json_import_rejects_cyclic_parent_relationships(tmp_path: Path) -> None:
    source = tmp_path / "cycle.json"
    output = tmp_path / "cycle.runpack"
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
                                        "spanId": "a",
                                        "parentSpanId": "b",
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "4",
                                    },
                                    {
                                        "traceId": "trace",
                                        "spanId": "b",
                                        "parentSpanId": "a",
                                        "startTimeUnixNano": "2",
                                        "endTimeUnixNano": "3",
                                    },
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(OtelImportError, match="OTLP parent relationships contain a cycle"):
        import_otlp_json(source, output, name="cycle")

    assert not output.exists()


def test_otlp_json_import_can_preserve_raw_source_explicitly(tmp_path: Path) -> None:
    source = tmp_path / "raw.json"
    output = tmp_path / "raw.runpack"
    raw = (
        b'{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"trace",'
        b'"spanId":"span","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}'
    )
    source.write_bytes(raw)

    import_otlp_json(source, output, name="raw", include_raw=True)

    with RunpackReader(output) as reader:
        attachments = reader.attachments()
    assert len(attachments) == 1
    assert attachments[0].kind == "raw"
    assert attachments[0].media_type == "application/json"
    assert attachments[0].content == raw


def test_otlp_json_import_keeps_execution_open_when_any_span_is_incomplete(
    tmp_path: Path,
) -> None:
    source = tmp_path / "partial.json"
    output = tmp_path / "partial.runpack"
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
                                        "spanId": "complete",
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                    },
                                    {
                                        "traceId": "trace",
                                        "spanId": "open",
                                        "startTimeUnixNano": "3",
                                    },
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_otlp_json(source, output, name="partial")

    summary = inspect_runpack(output)
    assert summary.finished_at_ns is None
    assert summary.wall_time_seconds is None


def test_otlp_json_import_keeps_execution_open_when_a_span_start_is_missing(
    tmp_path: Path,
) -> None:
    source = tmp_path / "partial-start.json"
    output = tmp_path / "partial-start.runpack"
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
                                        "spanId": "complete",
                                        "startTimeUnixNano": "2",
                                        "endTimeUnixNano": "3",
                                    },
                                    {
                                        "traceId": "trace",
                                        "spanId": "partial",
                                        "endTimeUnixNano": "4",
                                    },
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_otlp_json(source, output, name="partial-start")

    summary = inspect_runpack(output)
    assert summary.finished_at_ns is None
    assert summary.wall_time_seconds is None


def test_otlp_json_import_rejects_non_string_string_attributes(tmp_path: Path) -> None:
    source = tmp_path / "malformed-attribute.json"
    output = tmp_path / "malformed-attribute.runpack"
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "attributes": [
                                            {
                                                "key": "malformed",
                                                "value": {"stringValue": {"not": "a string"}},
                                            }
                                        ],
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

    with pytest.raises(OtelImportError, match="OTLP stringValue is invalid"):
        import_otlp_json(source, output, name="malformed")

    assert not output.exists()


def test_otlp_json_import_rejects_ambiguous_attribute_values(tmp_path: Path) -> None:
    source = tmp_path / "ambiguous-attribute.json"
    output = tmp_path / "ambiguous-attribute.runpack"
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "attributes": [
                                            {
                                                "key": "ambiguous",
                                                "value": {
                                                    "stringValue": "one",
                                                    "intValue": "1",
                                                },
                                            }
                                        ],
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

    with pytest.raises(OtelImportError, match="exactly one value variant"):
        import_otlp_json(source, output, name="ambiguous-attribute")

    assert not output.exists()


def test_otlp_json_import_rejects_duplicate_attribute_keys(tmp_path: Path) -> None:
    source = tmp_path / "duplicate-attribute.json"
    output = tmp_path / "duplicate-attribute.runpack"
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "attributes": [
                                            _attribute("peer.service", "database-a"),
                                            _attribute("peer.service", "database-b"),
                                        ],
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

    with pytest.raises(OtelImportError, match="duplicate OTLP attribute key: peer.service"):
        import_otlp_json(source, output, name="duplicate-attribute")

    assert not output.exists()


def test_otlp_json_import_rejects_excessively_nested_attributes(tmp_path: Path) -> None:
    source = tmp_path / "nested-attribute.json"
    output = tmp_path / "nested-attribute.runpack"
    value: dict[str, object] = {"stringValue": "leaf"}
    for _ in range(65):
        value = {"arrayValue": {"values": [value]}}
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
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
                                        "attributes": [{"key": "nested", "value": value}],
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

    with pytest.raises(OtelImportError, match="OTLP attribute nesting exceeds 64 levels"):
        import_otlp_json(source, output, name="nested-attribute")

    assert not output.exists()


@pytest.mark.parametrize(
    ("resource_attributes", "span_name", "message"),
    (
        (
            [{"key": "service.name", "value": {"intValue": "7"}}],
            "work",
            "service.name must be a string",
        ),
        ([], {"unexpected": "object"}, "span name must be a string"),
    ),
)
def test_otlp_json_import_rejects_malformed_semantic_names(
    tmp_path: Path,
    resource_attributes: list[dict[str, object]],
    span_name: object,
    message: str,
) -> None:
    source = tmp_path / "malformed-name.json"
    output = tmp_path / "malformed-name.runpack"
    source.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "resource": {"attributes": resource_attributes},
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": "trace",
                                        "spanId": "span",
                                        "name": span_name,
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "2",
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

    with pytest.raises(OtelImportError, match=message):
        import_otlp_json(source, output, name="malformed-name")

    assert not output.exists()


def test_otlp_json_import_rejects_non_string_span_identifiers(tmp_path: Path) -> None:
    source = tmp_path / "malformed-id.json"
    output = tmp_path / "malformed-id.runpack"
    source.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": 123,
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

    with pytest.raises(OtelImportError, match="span traceId must be a non-empty string"):
        import_otlp_json(source, output, name="malformed")

    assert not output.exists()


def test_otlp_json_import_rejects_non_standard_json_constants(tmp_path: Path) -> None:
    source = tmp_path / "non-standard.json"
    output = tmp_path / "non-standard.runpack"
    source.write_text('{"resourceSpans":[],"invalid":NaN}', encoding="utf-8")

    with pytest.raises(OtelImportError, match="non-finite JSON constant: NaN"):
        import_otlp_json(source, output, name="non-standard")

    assert not output.exists()


def test_otlp_json_import_rejects_oversized_sources_before_decoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "oversized.json"
    output = tmp_path / "oversized.runpack"
    source.write_bytes(b"{" + b" " * 32 + b"}")
    monkeypatch.setattr(otel, "MAX_OTLP_DOCUMENT_BYTES", 32)

    with pytest.raises(OtelImportError, match="OTLP JSON exceeds the 32-byte input limit"):
        import_otlp_json(source, output, name="oversized")

    assert not output.exists()


def test_otlp_json_import_normalizes_excessive_document_nesting(tmp_path: Path) -> None:
    source = tmp_path / "nested.json"
    output = tmp_path / "nested.runpack"
    source.write_text("[" * 10_000 + "0" + "]" * 10_000, encoding="utf-8")

    with pytest.raises(OtelImportError, match="OTLP JSON nesting is too deep"):
        import_otlp_json(source, output, name="nested")

    assert not output.exists()
