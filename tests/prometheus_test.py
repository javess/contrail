from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime_tools import prometheus
from runtime_tools.model import Entity, Execution
from runtime_tools.prometheus import PrometheusImportError, import_prometheus_response
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


def test_prometheus_response_rejects_non_finite_timestamps(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth"},
                            "value": ["Infinity", "1"],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="invalid Prometheus sample timestamp"):
        import_prometheus_response(source, response, output)

    assert not output.exists()


def test_prometheus_response_rejects_timestamps_outside_the_runpack_range(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth"},
                            "value": ["1e1000000000", "1"],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="timestamp exceeds the runpack range"):
        import_prometheus_response(source, response, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("result_type", "series", "message"),
    (
        ("scalar", [1, "3"], "resultType must be matrix or vector"),
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
    result_type: str,
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


def test_prometheus_response_rejects_non_string_label_values(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 2, (), str(tmp_path), 0, None, {}))
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "queue_depth", "pod": ["worker"]},
                            "value": [1, "3"],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="labels must be strings"):
        import_prometheus_response(source, response, output)

    assert not output.exists()


def test_prometheus_response_rejects_invalid_label_unicode_at_the_adapter_boundary(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    response = tmp_path / "metrics.json"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 2, (), str(tmp_path), 0, None, {}))
    response.write_text(
        json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "bad-\ud800"},
                            "value": [1, "3"],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PrometheusImportError, match="labels must be valid UTF-8"):
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
