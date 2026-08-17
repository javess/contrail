from __future__ import annotations

import json
from pathlib import Path

from runtime_tools.model import Entity, Execution
from runtime_tools.prometheus import import_prometheus_response
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
