"""Import bounded Prometheus HTTP API samples as normalized measurements."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from runtime_tools.enrichment import EnrichmentError, enrich_copy
from runtime_tools.model import Entity, JsonValue, Measurement
from runtime_tools.storage import RunpackReader, RunpackWriter


class PrometheusImportError(EnrichmentError):
    """Raised when Prometheus response evidence is malformed."""


@dataclass(frozen=True, slots=True)
class PrometheusImportResult:
    sample_count: int
    dropped_outside_window: int
    matched_entity_count: int


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise PrometheusImportError(f"{label} must be an object")
    return value


def _timestamp_ns(value: object) -> int:
    try:
        return int(Decimal(str(value)) * 1_000_000_000)
    except (InvalidOperation, OverflowError, ValueError) as exc:
        raise PrometheusImportError(f"invalid Prometheus sample timestamp: {value}") from exc


def _sample_value(value: object) -> float:
    try:
        result = float(str(value))
    except ValueError as exc:
        raise PrometheusImportError(f"invalid Prometheus sample value: {value}") from exc
    if not math.isfinite(result):
        raise PrometheusImportError("Prometheus sample values must be finite")
    return result


def _labels(value: object) -> dict[str, str]:
    raw = _object(value, "Prometheus metric labels")
    return {str(key): str(item) for key, item in raw.items()}


def _entity_for(labels: dict[str, str], entities: tuple[Entity, ...]) -> str | None:
    pod_uid = labels.get("pod_uid")
    pod_name = labels.get("pod")
    namespace = labels.get("namespace")
    service = labels.get("service") or labels.get("service_name")
    if pod_uid:
        matches = tuple(
            entity.id
            for entity in entities
            if entity.kind == "pod" and entity.attributes.get("k8s.uid") == pod_uid
        )
        return matches[0] if len(matches) == 1 else None
    if pod_name:
        matches = tuple(
            entity.id
            for entity in entities
            if entity.kind == "pod"
            and entity.name == pod_name
            and (namespace is None or entity.attributes.get("k8s.namespace") == namespace)
        )
        return matches[0] if len(matches) == 1 else None
    if service:
        matches = tuple(
            entity.id for entity in entities if entity.kind == "service" and entity.name == service
        )
        return matches[0] if len(matches) == 1 else None
    return None


def _load(source: Path) -> list[tuple[dict[str, str], object, object]]:
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PrometheusImportError(f"could not read Prometheus response: {source}") from exc
    except json.JSONDecodeError as exc:
        raise PrometheusImportError(f"invalid Prometheus JSON at line {exc.lineno}") from exc
    root = _object(document, "Prometheus response")
    if root.get("status") != "success":
        raise PrometheusImportError("Prometheus response status is not success")
    data = _object(root.get("data"), "Prometheus data")
    result = data.get("result")
    if not isinstance(result, list):
        raise PrometheusImportError("Prometheus result must be a list")
    samples = []
    for raw_series in result:
        series = _object(raw_series, "Prometheus series")
        labels = _labels(series.get("metric", {}))
        raw_values = series.get("values")
        if raw_values is None and "value" in series:
            raw_values = [series["value"]]
        if not isinstance(raw_values, list):
            raise PrometheusImportError("Prometheus series requires value or values")
        for raw_sample in raw_values:
            if not isinstance(raw_sample, list) or len(raw_sample) != 2:
                raise PrometheusImportError("Prometheus sample must be [timestamp, value]")
            samples.append((labels, raw_sample[0], raw_sample[1]))
    return samples


def import_prometheus_response(
    runpack: Path,
    source: Path,
    output: Path,
) -> PrometheusImportResult:
    raw_samples = _load(source)
    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        entities = reader.entities()
    measurements: list[Measurement] = []
    dropped = 0
    matched_entities: set[str] = set()
    for labels, raw_timestamp, raw_value in raw_samples:
        name = labels.get("__name__")
        if not name:
            raise PrometheusImportError("Prometheus series requires a __name__ label")
        timestamp_ns = _timestamp_ns(raw_timestamp)
        if timestamp_ns < execution.started_at_ns or (
            execution.finished_at_ns is not None and timestamp_ns > execution.finished_at_ns
        ):
            dropped += 1
            continue
        entity_id = _entity_for(labels, entities)
        if entity_id is not None:
            matched_entities.add(entity_id)
        attributes: dict[str, JsonValue] = {
            key: value for key, value in labels.items() if key != "__name__"
        }
        measurements.append(
            Measurement(
                name,
                _sample_value(raw_value),
                labels.get("unit", "1"),
                timestamp_ns,
                entity_id,
                attributes,
            )
        )

    def append(writer: RunpackWriter) -> PrometheusImportResult:
        writer.add_measurements(measurements)
        return PrometheusImportResult(len(measurements), dropped, len(matched_entities))

    return enrich_copy(runpack, output, append)
