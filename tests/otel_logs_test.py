from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

from runtime_tools import otel
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.enrichment import enrich_copy as real_enrich_copy
from runtime_tools.inspect import inspect_runpack
from runtime_tools.model import Entity, Event, Execution
from runtime_tools.otel import OtelImportError, OtelLogImportResult, import_otlp_logs
from runtime_tools.rundiff import compare_runpacks
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter


def _trace_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-trace:{label}").hex


def _span_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-span:{label}").hex[:16]


def _base_runpack(path: Path, working_directory: Path) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution("run", "run", 1, 10, (), str(working_directory), 0, None, {})
        )
        writer.add_entity(Entity("api", "service", "api", None, {"service.name": "api"}))
        writer.add_event(
            Event(
                f"otel:{_trace_id('trace')}:{_span_id('span')}",
                "server.request",
                "request",
                "api",
                2,
                9,
                "otel-resource-0",
                None,
                None,
                {"otel.trace_id": _trace_id("trace"), "otel.span_id": _span_id("span")},
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
                                "traceId": _trace_id("trace").upper(),
                                "spanId": _span_id("span").upper(),
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
    assert log.finished_at_ns == 5
    assert log.attributes == {
        "log.body": "accepted",
        "log.severity_text": "INFO",
        "otel.scope.attributes": {"schema": "stable"},
        "otel.scope.name": "example.logger",
        "otel.scope.version": "2.0",
        "otel.span_id": _span_id("span"),
        "otel.trace_id": _trace_id("trace"),
        "request.id": "42",
    }
    assert (edge.source_event_id, edge.kind) == (
        f"otel:{_trace_id('trace')}:{_span_id('span')}",
        "emits",
    )
    assert len(attachments) == 1
    assert attachments[0].content == logs.read_bytes()
    with RunpackReader(source) as reader:
        assert all(event.kind != "log.record" for event in reader.events())


def test_otlp_logs_preserve_exponent_timestamp_nanoseconds(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    timestamp_ns = 1_725_000_000_000_000_001
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                timestamp_ns - 1,
                timestamp_ns + 1,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
    logs.write_text(
        '{"resourceLogs":[{"scopeLogs":[{"logRecords":[{'
        '"timeUnixNano":1.725000000000000001e18,'
        '"severityNumber":9.0'
        "}]}]}]}",
        encoding="utf-8",
    )

    import_otlp_logs(source, logs, output)

    with RunpackReader(output) as reader:
        log = next(event for event in reader.events() if event.kind == "log.record")
    assert log.started_at_ns == timestamp_ns
    assert log.finished_at_ns == timestamp_ns
    assert log.attributes["log.severity_number"] == 9


def test_otlp_logs_reject_malformed_records_outside_the_run_window(tmp_path: Path) -> None:
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
                                        "timeUnixNano": "20",
                                        "severityNumber": "INVALID",
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

    with pytest.raises(OtelImportError, match="log severityNumber must be"):
        import_otlp_logs(source, logs, output)

    assert not output.exists()


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


@pytest.mark.parametrize(
    ("severity", "normalized"),
    (("SEVERITY_NUMBER_ERROR3", 19), (19.0, 19)),
)
def test_otlp_logs_normalize_severity_numbers(
    tmp_path: Path, severity: object, normalized: int
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
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "severityNumber": severity,
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
    assert log.attributes["log.severity_number"] == normalized


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


def test_otlp_logs_accumulate_missing_span_references(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    with sqlite3.connect(source) as connection:
        row = connection.execute("SELECT metadata_json FROM executions").fetchone()
        metadata = json.loads(row[0])
        metadata["otel"] = {"missing_parent_count": 2}
        connection.execute("UPDATE executions SET metadata_json = ?", (json.dumps(metadata),))
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
                                        "traceId": _trace_id("missing-trace"),
                                        "spanId": _span_id("missing-span"),
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

    result = import_otlp_logs(source, logs, output)

    assert result.missing_span_count == 1
    assert inspect_runpack(output).missing_causal_references == 3


@pytest.mark.parametrize("severity", (True, "INVALID", -1, 1.5, 25))
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


@pytest.mark.parametrize(
    ("incoming_namespace", "expected_new_entity_count"),
    (("production", 0), ("staging", 1)),
)
def test_otlp_logs_match_services_by_explicit_namespace(
    tmp_path: Path,
    incoming_namespace: str,
    expected_new_entity_count: int,
) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 1, 10, (), str(tmp_path), 0, None, {}))
        writer.add_entity(
            Entity(
                "production-api",
                "service",
                "api",
                None,
                {"service.name": "api", "service.namespace": "production"},
            )
        )
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": {"stringValue": "api"},
                                },
                                {
                                    "key": "service.namespace",
                                    "value": {"stringValue": incoming_namespace},
                                },
                            ]
                        },
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "body": {"stringValue": "namespaced log"},
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

    with RunpackReader(output) as reader:
        log = next(event for event in reader.events() if event.kind == "log.record")
        owner = next(entity for entity in reader.entities() if entity.id == log.entity_id)
    assert result.new_entity_count == expected_new_entity_count
    assert owner.attributes["service.namespace"] == incoming_namespace
    assert (owner.id == "production-api") is (incoming_namespace == "production")


def test_otlp_logs_use_exact_span_ownership_when_resource_identity_is_missing(
    tmp_path: Path,
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
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "body": {"stringValue": "correlated"},
                                        "traceId": _trace_id("trace"),
                                        "spanId": _span_id("span"),
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

    result = import_otlp_logs(source, logs, output)

    assert result.new_entity_count == 0
    with RunpackReader(output) as reader:
        log = next(event for event in reader.events() if event.kind == "log.record")
        services = tuple(entity for entity in reader.entities() if entity.kind == "service")
    assert log.entity_id == "api"
    assert [service.name for service in services] == ["api"]


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

    first_result = import_otlp_logs(source, first_logs, first_output)
    second_result = import_otlp_logs(first_output, second_logs, second_output)

    with RunpackReader(second_output) as reader:
        logs = tuple(event for event in reader.events() if event.kind == "log.record")
        services = {entity.id: entity for entity in reader.entities() if entity.kind == "service"}
    assert (first_result.new_entity_count, second_result.new_entity_count) == (1, 1)
    assert {event.name for event in logs} == {"first", "second"}
    assert len({event.id for event in logs}) == 2
    assert len({event.entity_id for event in logs}) == 2
    assert {services[event.entity_id].name for event in logs if event.entity_id is not None} == {
        "unknown-service"
    }


def test_otlp_logs_reuse_nameless_service_identity_for_the_same_source(
    tmp_path: Path,
) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    first_output = tmp_path / "first.runpack"
    independent_output = tmp_path / "independent.runpack"
    replay_output = tmp_path / "replay.runpack"
    _base_runpack(source, tmp_path)
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {"logRecords": [{"timeUnixNano": "4", "body": {"stringValue": "same"}}]}
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import_otlp_logs(source, logs, first_output)
    import_otlp_logs(source, logs, independent_output)

    with RunpackReader(first_output) as reader:
        first_service = next(
            entity for entity in reader.entities() if entity.name == "unknown-service"
        )
    with RunpackReader(independent_output) as reader:
        independent_service = next(
            entity for entity in reader.entities() if entity.name == "unknown-service"
        )
    assert independent_service.id == first_service.id

    with pytest.raises(RunpackError, match="UNIQUE constraint failed: events.id"):
        import_otlp_logs(first_output, logs, replay_output)
    assert not replay_output.exists()


def test_otlp_logs_keep_same_named_services_in_distinct_namespaces_across_enrichments(
    tmp_path: Path,
) -> None:
    source = tmp_path / "base.runpack"
    first_logs = tmp_path / "blue.json"
    second_logs = tmp_path / "green.json"
    first_output = tmp_path / "blue.runpack"
    second_output = tmp_path / "green.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 1, 10, (), str(tmp_path), 0, None, {}))
    for path, timestamp, namespace in (
        (first_logs, "4", "blue"),
        (second_logs, "6", "green"),
    ):
        path.write_text(
            json.dumps(
                {
                    "resourceLogs": [
                        {
                            "resource": {
                                "attributes": [
                                    {
                                        "key": "service.name",
                                        "value": {"stringValue": "api"},
                                    },
                                    {
                                        "key": "service.namespace",
                                        "value": {"stringValue": namespace},
                                    },
                                ]
                            },
                            "scopeLogs": [
                                {
                                    "logRecords": [
                                        {
                                            "timeUnixNano": timestamp,
                                            "body": {"stringValue": namespace},
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

    first_result = import_otlp_logs(source, first_logs, first_output)
    second_result = import_otlp_logs(first_output, second_logs, second_output)

    assert first_result.new_entity_count == 1
    assert second_result.new_entity_count == 1
    with RunpackReader(second_output) as reader:
        logs = tuple(event for event in reader.events() if event.kind == "log.record")
        services = {entity.id: entity for entity in reader.entities() if entity.kind == "service"}
    assert len({event.entity_id for event in logs}) == 2
    assert {
        (services[event.entity_id].name, services[event.entity_id].attributes["service.namespace"])
        for event in logs
        if event.entity_id is not None
    } == {("api", "blue"), ("api", "green")}


def test_otlp_logs_keep_correlating_to_spans_across_sequential_enrichments(
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
                                            "traceId": _trace_id("trace"),
                                            "spanId": _span_id("span"),
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

    first_result = import_otlp_logs(source, first_logs, first_output)
    second_result = import_otlp_logs(first_output, second_logs, second_output)

    assert (first_result.edge_count, first_result.missing_span_count) == (1, 0)
    assert (second_result.edge_count, second_result.missing_span_count) == (1, 0)
    assert second_result.new_entity_count == 0
    span_event_id = f"otel:{_trace_id('trace')}:{_span_id('span')}"
    with RunpackReader(second_output) as reader:
        logs = tuple(event for event in reader.events() if event.kind == "log.record")
        correlations = tuple(edge for edge in reader.causal_edges() if edge.kind == "emits")
        services = tuple(entity for entity in reader.entities() if entity.kind == "service")
    assert {edge.source_event_id for edge in correlations} == {span_event_id}
    assert {edge.target_event_id for edge in correlations} == {event.id for event in logs}
    assert [service.name for service in services] == ["api"]


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
                                        "traceId": _trace_id("trace"),
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


@pytest.mark.parametrize("field", ("traceId", "spanId"))
def test_otlp_logs_reject_invalid_identifier_unicode_at_the_adapter_boundary(
    tmp_path: Path, field: str
) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    record = {
        "timeUnixNano": "5",
        "body": {"stringValue": "invalid"},
        "traceId": _trace_id("trace"),
        "spanId": _span_id("span"),
        field: "bad-\ud800",
    }
    logs.write_text(
        json.dumps({"resourceLogs": [{"scopeLogs": [{"logRecords": [record]}]}]}),
        encoding="utf-8",
    )

    with pytest.raises(OtelImportError, match=f"log {field} must be valid UTF-8"):
        import_otlp_logs(source, logs, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("traceId", "0" * 32, "log traceId must not be all zero"),
        ("spanId", "0" * 16, "log spanId must not be all zero"),
    ),
)
def test_otlp_logs_reject_zero_correlation_identifiers_without_publishing(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)
    record = {
        "timeUnixNano": "5",
        "traceId": _trace_id("trace"),
        "spanId": _span_id("span"),
        field: value,
    }
    logs.write_text(
        json.dumps({"resourceLogs": [{"scopeLogs": [{"logRecords": [record]}]}]}),
        encoding="utf-8",
    )

    with pytest.raises(OtelImportError, match=message):
        import_otlp_logs(source, logs, output)

    assert not output.exists()


def test_otlp_logs_reject_non_utf8_source_filenames(tmp_path: Path) -> None:
    source = tmp_path / "base.runpack"
    logs = tmp_path / "logs-\udcff.json"
    output = tmp_path / "enriched.runpack"
    _base_runpack(source, tmp_path)

    with pytest.raises(OtelImportError, match="OTLP source filename must be valid UTF-8"):
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
                                        "traceId": _trace_id("trace"),
                                        "spanId": _span_id("span"),
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


def test_otlp_logs_plan_against_the_copied_source_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    logs = tmp_path / "logs.json"
    output = tmp_path / "enriched.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("a", "A", 0, 2_000_000_000, (), str(tmp_path), 0, None, {}))
    with RunpackWriter(replacement) as writer:
        writer.add_execution(Execution("b", "B", 0, 500_000_000, (), str(tmp_path), 0, None, {}))
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "1000000000",
                                        "body": {"stringValue": "only valid for A"},
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

    def replace_then_enrich(
        source_path: Path,
        output_path: Path,
        operation: Callable[[RunpackWriter], OtelLogImportResult],
    ) -> OtelLogImportResult:
        replacement.replace(source_path)
        return real_enrich_copy(source_path, output_path, operation)

    monkeypatch.setattr(otel, "enrich_copy", replace_then_enrich)

    result = import_otlp_logs(source, logs, output)

    assert result.event_count == 0
    assert result.dropped_outside_window == 1
    with RunpackReader(output) as reader:
        execution = reader.execution()
        log_records = tuple(event for event in reader.events() if event.kind == "log.record")
    assert (execution.id, execution.name, execution.finished_at_ns) == (
        "b",
        "B",
        500_000_000,
    )
    assert log_records == ()
