from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from runtime_tools.model import Entity, Execution
from runtime_tools.providers.builtins.prometheus import (
    PrometheusImportError,
    PrometheusImportResult,
    import_prometheus_response,
)
from runtime_tools.providers.builtins.prometheus import enrichment as prometheus
from runtime_tools.providers.enrichment import enrich_copy as real_enrich_copy
from runtime_tools.storage import RunpackReader, RunpackWriter


def test_prometheus_response_imports_only_windowed_samples_and_matches_pod(
    tmp_path: Path,
) -> None:
    source = tmp_path / "enriched.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "full.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                2_000_000_000,
                8_000_000_000,
                (),
                str(tmp_path),
                None,
                None,
                {},
            )
        )
        writer.add_entity(
            Entity(
                "pod",
                "pod",
                "worker-0",
                None,
                {"k8s.uid": "pod-uid", "k8s.namespace": "demo"},
            )
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "__name__": "queue_depth",
                                "pod": "worker-0",
                                "namespace": "demo",
                            },
                            "values": [[3, "12"], [4, "8"], [9, "1"]],
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    result = import_prometheus_response(source, response, output)

    assert result.sample_count == 2
    assert result.dropped_outside_window == 1
    assert result.matched_entity_count == 1
    with RunpackReader(output) as reader:
        measurements = [item for item in reader.measurements() if item.name == "queue_depth"]
        entities = {entity.id: entity for entity in reader.entities()}
    assert [item.value for item in measurements] == [12.0, 8.0]
    assert all(item.entity_id is not None for item in measurements)
    assert {entities[item.entity_id].name for item in measurements if item.entity_id} == {
        "worker-0"
    }
    assert source.is_file()


def test_prometheus_admission_and_matching_use_the_copied_runpack_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    for path, execution_name, bounds, matched_entity_id in (
        (source, "snapshot-a", (0, 10_000_000_000), "pod-a"),
        (
            replacement,
            "snapshot-b",
            (100_000_000_000, 200_000_000_000),
            "pod-b",
        ),
    ):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(
                    execution_name,
                    execution_name,
                    bounds[0],
                    bounds[1],
                    (),
                    str(tmp_path),
                    0,
                    None,
                    {},
                )
            )
            writer.add_entities(
                tuple(
                    Entity(
                        entity_id,
                        "pod",
                        entity_id,
                        None,
                        {"k8s.uid": "pod-race"}
                        if entity_id == matched_entity_id
                        else {"k8s.uid": "other"},
                    )
                    for entity_id in ("pod-a", "pod-b")
                )
            )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth", "pod_uid": "pod-race"},
                            "values": [[5, "1"], [150, "2"]],
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    def replace_source_then_enrich(
        source_path: Path,
        output_path: Path,
        operation: Callable[[RunpackWriter], PrometheusImportResult],
    ) -> PrometheusImportResult:
        replacement.replace(source_path)
        return real_enrich_copy(source_path, output_path, operation)

    monkeypatch.setattr(prometheus, "enrich_copy", replace_source_then_enrich)

    result = import_prometheus_response(source, response, output)

    with RunpackReader(output) as reader:
        execution = reader.execution()
        measurements = reader.measurements()
    assert execution.id == "snapshot-b"
    assert result == PrometheusImportResult(1, 1, 1)
    assert [(item.timestamp_ns, item.entity_id) for item in measurements] == [
        (150_000_000_000, "pod-b")
    ]


def test_prometheus_response_normalizes_an_empty_unit_as_dimensionless(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "ratio", "unit": ""},
                            "value": [1, "0.5"],
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    import_prometheus_response(source, response, output)

    with RunpackReader(output) as reader:
        measurement = reader.measurements()[0]
    assert measurement.unit == "1"
    assert measurement.attributes["unit"] == ""


@pytest.mark.parametrize("metric", ({"job": "api"}, {"__name__": "", "job": "api"}))
def test_prometheus_response_normalizes_nameless_expression_results(
    tmp_path: Path,
    metric: dict[str, str],
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": metric, "value": [1, "3"]}],
                },
            }
        ),
        encoding="utf-8",
    )

    result = import_prometheus_response(source, response, output)

    assert result.sample_count == 1
    with RunpackReader(output) as reader:
        measurement = reader.measurements()[0]
    assert measurement.name == "prometheus.result"
    assert measurement.value == 3
    assert measurement.attributes == {"job": "api"}


@pytest.mark.parametrize(
    ("bounds", "sample", "message"),
    (
        (
            (0, 1),
            {"metric": {"__name__": "queue_depth"}, "value": ["Infinity", "1"]},
            "invalid Prometheus sample timestamp",
        ),
        (
            (0, 1),
            {"metric": {"__name__": "queue_depth"}, "value": ["1e1000000000", "1"]},
            "timestamp exceeds the runpack range",
        ),
        (
            (-1, 1),
            {"metric": {"__name__": "queue_depth"}, "value": ["-0.0000000001", "1"]},
            "has sub-nanosecond precision",
        ),
        (
            (0, 2_000_000_000),
            {"metric": {"__name__": "queue_depth"}, "value": [3, "NaN"]},
            "sample values must be finite",
        ),
        (
            (0, 2),
            {
                "metric": {"__name__": "queue_depth", "pod": ["worker"]},
                "value": [1, "3"],
            },
            "labels must be strings",
        ),
        (
            (0, 2),
            {"metric": {"__name__": "bad-\ud800"}, "value": [1, "3"]},
            "labels must be valid UTF-8",
        ),
    ),
    ids=(
        "non-finite-timestamp",
        "out-of-range-timestamp",
        "subnanosecond-timestamp",
        "non-finite-value",
        "non-string-label",
        "invalid-label-unicode",
    ),
)
def test_prometheus_response_rejects_invalid_samples(
    tmp_path: Path,
    bounds: tuple[int, int],
    sample: dict[str, object],
    message: str,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", bounds[0], bounds[1], (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {"result": [sample]},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match=message):
        import_prometheus_response(source, response, output)

    assert not output.exists()


def test_prometheus_response_preserves_numeric_nanosecond_timestamps(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    timestamp_ns = 1_700_000_000_123_456_789
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                timestamp_ns,
                timestamp_ns,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
    response.write_text(
        """{
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {"__name__": "queue_depth"},
                    "value": [1700000000.123456789, "1"]
                }]
            }
        }""",
        encoding="utf-8",
    )

    result = import_prometheus_response(source, response, output)

    assert result.sample_count == 1
    with RunpackReader(output) as reader:
        assert reader.measurements()[0].timestamp_ns == timestamp_ns


@pytest.mark.parametrize("second_value", ("3", "4"))
def test_prometheus_response_rejects_duplicate_sample_identities(
    tmp_path: Path,
    second_value: str,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth", "service": "api"},
                            "value": [1, "3"],
                        },
                        {
                            "metric": {"service": "api", "__name__": "queue_depth"},
                            "value": [1, second_value],
                        },
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="duplicate Prometheus sample identity"):
        import_prometheus_response(source, response, output)

    assert not output.exists()
    with RunpackReader(source) as reader:
        assert reader.measurements() == ()


@pytest.mark.parametrize(
    ("first_metric", "second_metric"),
    (
        ({"job": "api"}, {"__name__": "", "job": "api"}),
        ({"job": "api"}, {"__name__": "prometheus.result", "job": "api"}),
    ),
)
def test_prometheus_response_rejects_normalized_sample_identity_collisions(
    tmp_path: Path,
    first_metric: dict[str, str],
    second_metric: dict[str, str],
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {"metric": first_metric, "value": [1, "3"]},
                        {"metric": second_metric, "value": [1, "4"]},
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="duplicate Prometheus sample identity"):
        import_prometheus_response(source, response, output)

    assert not output.exists()
    with RunpackReader(source) as reader:
        assert reader.measurements() == ()


def test_prometheus_response_keeps_same_timestamp_samples_with_distinct_labels(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth", "service": "api"},
                            "value": [1, "3"],
                        },
                        {
                            "metric": {"__name__": "queue_depth", "service": "worker"},
                            "value": [1, "4"],
                        },
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    result = import_prometheus_response(source, response, output)

    assert result.sample_count == 2
    with RunpackReader(output) as reader:
        measurements = reader.measurements()
    assert {(sample.attributes["service"], sample.value) for sample in measurements} == {
        ("api", 3.0),
        ("worker", 4.0),
    }


def test_prometheus_response_rejects_numeric_subnanosecond_timestamps(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                1_700_000_000_000_000_000,
                1_700_000_001_000_000_000,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
    response.write_text(
        """{
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {"__name__": "queue_depth"},
                    "value": [1700000000.1234567891, "1"]
                }]
            }
        }""",
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="has sub-nanosecond precision"):
        import_prometheus_response(source, response, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("result_type", "series", "message"),
    (
        ("scalar", [1, "3"], "resultType must be matrix or vector"),
        ([], {}, "resultType must be matrix or vector"),
        ({}, {}, "resultType must be matrix or vector"),
        (
            "matrix",
            {"metric": {"__name__": "depth"}, "value": [1, "3"]},
            "matrix series requires values",
        ),
        (
            "vector",
            {"metric": {"__name__": "depth"}, "values": [[1, "3"]]},
            "vector series requires value",
        ),
        (
            "matrix",
            {
                "metric": {"__name__": "depth"},
                "value": [1, "3"],
                "values": [[1, "4"]],
            },
            "matrix series cannot contain value",
        ),
        (
            "vector",
            {
                "metric": {"__name__": "depth"},
                "value": [1, "3"],
                "values": [[1, "4"]],
            },
            "vector series cannot contain values",
        ),
    ),
)
def test_prometheus_response_rejects_inconsistent_result_types(
    tmp_path: Path,
    result_type: object,
    series: object,
    message: str,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {"resultType": result_type, "result": [series]},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match=message):
        import_prometheus_response(source, response, output)

    assert not output.exists()


def test_prometheus_response_rejects_ambiguous_series_without_result_type(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "depth"},
                            "value": [1, "3"],
                            "values": [[1, "4"]],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        PrometheusImportError,
        match="series cannot contain both value and values",
    ):
        import_prometheus_response(source, response, output)

    assert not output.exists()


def test_prometheus_response_rolls_back_samples_before_a_malformed_value(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth"},
                            "values": [[1, "3"], [2, "NaN"]],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="sample values must be finite"):
        import_prometheus_response(source, response, output)

    assert not output.exists()
    with RunpackReader(source) as reader:
        assert reader.measurements() == ()


def test_prometheus_response_rolls_back_samples_over_the_input_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth"},
                            "values": [[1, "3"], [2, "4"]],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(prometheus, "MAX_PROMETHEUS_SAMPLES", 1)

    with pytest.raises(PrometheusImportError, match="exceeds the 1-sample input limit"):
        import_prometheus_response(source, response, output)

    assert not output.exists()
    with RunpackReader(source) as reader:
        assert reader.measurements() == ()


def test_prometheus_response_does_not_guess_between_ambiguous_pod_names(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entities(
            (
                Entity("pod-a", "pod", "worker", None, {"k8s.namespace": "a"}),
                Entity("pod-b", "pod", "worker", None, {"k8s.namespace": "b"}),
            )
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth", "pod": "worker"},
                            "value": [1, "3"],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    result = import_prometheus_response(source, response, output)

    assert result.sample_count == 1
    assert result.matched_entity_count == 0
    with RunpackReader(output) as reader:
        measurement = next(item for item in reader.measurements() if item.name == "queue_depth")
    assert measurement.entity_id is None


def test_prometheus_response_rejects_conflicting_pod_identity_labels(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 2_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(
            Entity(
                "pod",
                "pod",
                "worker-a",
                None,
                {"k8s.uid": "pod-uid", "k8s.namespace": "demo"},
            )
        )
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {
                                "__name__": "queue_depth",
                                "pod_uid": "pod-uid",
                                "pod": "worker-b",
                                "namespace": "demo",
                            },
                            "value": [1, "3"],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    result = import_prometheus_response(source, response, output)

    assert result.matched_entity_count == 0
    with RunpackReader(output) as reader:
        measurement = next(item for item in reader.measurements() if item.name == "queue_depth")
    assert measurement.entity_id is None


def test_prometheus_response_requires_a_closed_execution_window(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, None, (), str(tmp_path), None, None, {}))
    response.write_text(
        '{"status":"success","data":{"result":[]}}',
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="requires a finished execution window"):
        import_prometheus_response(source, response, output)

    assert not output.exists()


def test_prometheus_response_rejects_non_standard_json_constants(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source):
        pass
    response.write_text(
        '{"status":"success","data":{"result":[]},"invalid":-Infinity}',
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="non-finite JSON constant: -Infinity"):
        import_prometheus_response(source, response, output)

    assert not output.exists()


def test_prometheus_response_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    response = tmp_path / "duplicate-keys.json"
    output = tmp_path / "output.runpack"
    response.write_text(
        '{"status":"success","status":"error","data":{"result":[]}}',
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="duplicate JSON key: status"):
        import_prometheus_response(tmp_path / "missing.runpack", response, output)

    assert not output.exists()


def test_prometheus_response_rejects_oversized_sources_before_decoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = tmp_path / "oversized.json"
    response.write_bytes(b"{" + b" " * 32 + b"}")
    monkeypatch.setattr(prometheus, "MAX_PROMETHEUS_RESPONSE_BYTES", 32)

    with pytest.raises(
        PrometheusImportError,
        match="Prometheus response exceeds the 32-byte input limit",
    ):
        import_prometheus_response(
            tmp_path / "missing.runpack", response, tmp_path / "output.runpack"
        )


def test_prometheus_response_normalizes_invalid_utf8(tmp_path: Path) -> None:
    response = tmp_path / "invalid-utf8.json"
    response.write_bytes(b"\xff")

    with pytest.raises(PrometheusImportError, match="Prometheus response must be UTF-8"):
        import_prometheus_response(
            tmp_path / "missing.runpack", response, tmp_path / "output.runpack"
        )


def test_prometheus_response_normalizes_excessive_document_nesting(tmp_path: Path) -> None:
    response = tmp_path / "nested.json"
    response.write_text("[" * 10_000 + "0" + "]" * 10_000, encoding="utf-8")

    with pytest.raises(PrometheusImportError, match="Prometheus JSON nesting is too deep"):
        import_prometheus_response(
            tmp_path / "missing.runpack", response, tmp_path / "output.runpack"
        )
