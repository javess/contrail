from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from runtime_tools import otel
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.inspect import inspect_runpack
from runtime_tools.model import Entity, Event, Execution
from runtime_tools.otel import OtelImportError, import_otlp_logs
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.storage import RunpackReader, RunpackWriter


def _base_runpack(path: Path, working_directory: Path) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution("run", "run", 1, 10, (), str(working_directory), 0, None, {})
        )
        writer.add_entity(Entity("api", "service", "api", None, {"service.name": "api"}))
        writer.add_event(
            Event(
                "otel:trace:span",
                "server.request",
                "request",
                "api",
                2,
                9,
                "otel-resource-0",
                None,
                None,
                {"otel.trace_id": "trace", "otel.span_id": "span"},
            )
        )


def test_otlp_logs_enrich_known_spans_and_bound_timestamped_records(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    document = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]
                },
                "scopeLogs": [
                    {
                        "scope": {
                            "name": "example.logger",
                            "version": "2.0",
                            "attributes": [{"key": "schema", "value": {"stringValue": "stable"}}],
                        },
                        "logRecords": [
                            {
                                "timeUnixNano": "5",
                                "severityText": "INFO",
                                "body": {"stringValue": "accepted"},
                                "attributes": [
                                    {"key": "request.id", "value": {"stringValue": "42"}}
                                ],
                                "traceId": "trace",
                                "spanId": "span",
                            },
                            {
                                "timeUnixNano": "20",
                                "severityText": "INFO",
                                "body": {"stringValue": "outside"},
                            },
                        ],
                    }
                ],
            }
        ]
    }
    logs.write_text(json.dumps(document), encoding="utf-8")

    result = import_otlp_logs(source, logs, output, include_raw=True)

    assert result.event_count == 1
    assert result.edge_count == 1
    assert result.new_entity_count == 0
    assert result.dropped_outside_window == 1
    assert result.missing_span_count == 0
    with RunpackReader(output) as reader:
        log = next(event for event in reader.events() if event.kind == "log.record")
        edge = next(edge for edge in reader.causal_edges() if edge.target_event_id == log.id)
        attachments = reader.attachments()
    assert log.name == "accepted"
    assert log.entity_id == "api"
    assert log.started_at_ns == 5
    assert log.finished_at_ns is None
    assert log.attributes == {
        "log.body": "accepted",
        "log.severity_text": "INFO",
        "otel.scope.attributes": {"schema": "stable"},
        "otel.scope.name": "example.logger",
        "otel.scope.version": "2.0",
        "otel.span_id": "span",
        "otel.trace_id": "trace",
        "request.id": "42",
    }
    assert (edge.source_event_id, edge.kind) == ("otel:trace:span", "emits")
    assert len(attachments) == 1
    assert attachments[0].content == logs.read_bytes()
    with RunpackReader(source) as reader:
        assert all(event.kind != "log.record" for event in reader.events())


def test_otlp_logs_reject_records_over_the_input_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "too-many-logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {"timeUnixNano": "5", "body": {"stringValue": "one"}},
                                    {"timeUnixNano": "6", "body": {"stringValue": "two"}},
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(otel, "MAX_OTLP_LOG_RECORDS", 1)

    with pytest.raises(OtelImportError, match="exceeds the 1-log-record input limit"):
        import_otlp_logs(source, logs, output)

    assert not output.exists()
    with RunpackReader(source) as reader:
        assert all(event.kind != "log.record" for event in reader.events())


def test_otlp_logs_normalize_named_severity_numbers(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "severityNumber": "SEVERITY_NUMBER_ERROR3",
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

    import_otlp_logs(source, logs, output)

    with RunpackReader(output) as reader:
        log = next(event for event in reader.events() if event.kind == "log.record")
    assert log.attributes["log.severity_number"] == 19


def test_otlp_logs_accumulate_exporter_dropped_attributes(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    with sqlite3.connect(source) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["otel"] = {"dropped_attribute_count": 2}
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "resource": {"droppedAttributesCount": 3},
                        "scopeLogs": [
                            {
                                "scope": {"droppedAttributesCount": 5},
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "droppedAttributesCount": "4",
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

    result = import_otlp_logs(source, logs, output)

    assert result.dropped_attribute_count == 12
    assert inspect_runpack(output).dropped_attribute_count == 14


@pytest.mark.parametrize("severity", (True, "INVALID", -1, 25))
def test_otlp_logs_reject_invalid_severity_numbers(
    tmp_path: Path,
    severity: object,
) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {"logRecords": [{"timeUnixNano": "5", "severityNumber": severity}]}
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(OtelImportError, match="log severityNumber must be"):
        import_otlp_logs(source, logs, output)

    assert not output.exists()


def test_otlp_logs_create_service_entities_for_unmatched_resources(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": "worker"}}
                            ]
                        },
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "observedTimeUnixNano": "6",
                                        "body": {"intValue": "3"},
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

    result = import_otlp_logs(source, logs, output)

    assert result.new_entity_count == 1
    with RunpackReader(output) as reader:
        worker = next(entity for entity in reader.entities() if entity.name == "worker")
        log = next(event for event in reader.events() if event.kind == "log.record")
    assert worker.attributes["service.name"] == "worker"
    assert log.entity_id == worker.id
    assert log.name == "log"
    assert log.attributes["log.body"] == 3


def test_otlp_logs_can_add_distinct_documents_with_the_same_record_indexes(
    tmp_path: Path,
) -> None:
    source = tmp_path / "base.runpack"
    first_logs = tmp_path / "first.json"
    second_logs = tmp_path / "second.json"
    first_output = tmp_path / "first.runpack"
    second_output = tmp_path / "second.runpack"
    _base_runpack(source, tmp_path)
    for path, timestamp, body in (
        (first_logs, "4", "first"),
        (second_logs, "6", "second"),
    ):
        path.write_text(
            json.dumps(
                {
                    "resourceLogs": [
                        {
                            "scopeLogs": [
                                {
                                    "logRecords": [
                                        {
                                            "timeUnixNano": timestamp,
                                            "body": {"stringValue": body},
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

    import_otlp_logs(source, first_logs, first_output)
    import_otlp_logs(first_output, second_logs, second_output)

    with RunpackReader(second_output) as reader:
        logs = tuple(event for event in reader.events() if event.kind == "log.record")
    assert {event.name for event in logs} == {"first", "second"}
    assert len({event.id for event in logs}) == 2


def test_otlp_logs_preserve_ambiguous_service_resources_without_guessing(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 1, 10, (), str(tmp_path), 0, None, {}))
        writer.add_entities(
            (
                Entity("api-a", "service", "api", None, {"service.instance.id": "a"}),
                Entity("api-b", "service", "api", None, {"service.instance.id": "b"}),
            )
        )
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": "api"}},
                                {
                                    "key": "service.instance.id",
                                    "value": {"stringValue": "c"},
                                },
                            ]
                        },
                        "scopeLogs": [
                            {"logRecords": [{"timeUnixNano": "5", "body": {"stringValue": "ok"}}]}
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_otlp_logs(source, logs, output)

    assert result.ambiguous_service_count == 1
    assert result.new_entity_count == 1
    with RunpackReader(output) as reader:
        log = next(event for event in reader.events() if event.kind == "log.record")
        owner = next(entity for entity in reader.entities() if entity.id == log.entity_id)
    assert owner.id not in {"api-a", "api-b"}
    assert owner.attributes["service.instance.id"] == "c"


def test_otlp_logs_use_instance_identity_to_disambiguate_services(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 1, 10, (), str(tmp_path), 0, None, {}))
        writer.add_entities(
            (
                Entity("api-a", "service", "api", None, {"service.instance.id": "a"}),
                Entity("api-b", "service", "api", None, {"service.instance.id": "b"}),
            )
        )
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": "api"}},
                                {
                                    "key": "service.instance.id",
                                    "value": {"stringValue": "b"},
                                },
                            ]
                        },
                        "scopeLogs": [
                            {"logRecords": [{"timeUnixNano": "5", "body": {"stringValue": "ok"}}]}
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = import_otlp_logs(source, logs, output)

    assert result.new_entity_count == 0
    assert result.ambiguous_service_count == 0
    with RunpackReader(output) as reader:
        log = next(event for event in reader.events() if event.kind == "log.record")
    assert log.entity_id == "api-b"


def test_otlp_logs_reject_partial_trace_correlation_without_publishing(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "body": {"stringValue": "invalid"},
                                        "traceId": "trace",
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

    with pytest.raises(OtelImportError, match="traceId and spanId together"):
        import_otlp_logs(source, logs, output)

    assert not output.exists()


def test_otlp_logs_do_not_masquerade_as_operations_or_critical_work(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]
                        },
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "body": {"stringValue": "diagnostic message"},
                                        "traceId": "trace",
                                        "spanId": "span",
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
    import_otlp_logs(source, logs, output)

    diff = compare_runpacks(source, output)
    baseline_analysis = analyze_runpack(source)
    enriched_analysis = analyze_runpack(output)

    assert diff.operation_count_changes == ()
    assert diff.operation_duration_changes == ()
    assert diff.edge_count_changes == ()
    assert baseline_analysis.critical_path is not None
    assert enriched_analysis.critical_path is not None
    assert enriched_analysis.critical_path.event_ids == baseline_analysis.critical_path.event_ids
