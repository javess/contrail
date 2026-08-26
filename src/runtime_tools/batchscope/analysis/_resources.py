"""Deterministic lifecycle, critical-path, throughput, and bottleneck facts."""

from __future__ import annotations

from runtime_tools.batchscope.analysis._boundary_models import (
    _ObservedProcessIdentity,
    _ProcessResourceAggregate,
)
from runtime_tools.batchscope.analysis._profile_models import ProcessResourceHotspot
from runtime_tools.storage import RunpackReader


def _process_resource_evidence(
    reader: RunpackReader,
) -> tuple[tuple[ProcessResourceHotspot, ...], dict[int, _ObservedProcessIdentity]]:
    entities = {entity.id: entity for entity in reader.entities()}
    aggregates: dict[str, _ProcessResourceAggregate] = {}
    for measurement in reader.measurements():
        if measurement.name not in {"process.memory.rss", "process.cpu.total"}:
            continue
        if measurement.attributes.get("source") != "process-observer":
            continue
        entity_id = measurement.entity_id
        pid = measurement.attributes.get("pid")
        process_name = measurement.attributes.get("process_name")
        if (
            entity_id is None
            or not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not isinstance(process_name, str)
            or not process_name
        ):
            continue
        aggregate = aggregates.get(entity_id)
        if aggregate is None:
            entity = entities.get(entity_id)
            parent = (
                entities.get(entity.parent_entity_id)
                if entity is not None and entity.parent_entity_id is not None
                else None
            )
            aggregate = _ProcessResourceAggregate(
                process_name,
                pid,
                parent.name if parent is not None else None,
            )
            aggregates[entity_id] = aggregate
        if measurement.name == "process.memory.rss":
            aggregate.sample_count += 1
            aggregate.peak_rss_bytes = max(aggregate.peak_rss_bytes, measurement.value)
        else:
            aggregate.cpu_seconds = max(aggregate.cpu_seconds, measurement.value)
    hotspots = tuple(
        ProcessResourceHotspot(
            aggregate.name,
            aggregate.pid,
            aggregate.parent_name,
            aggregate.sample_count,
            aggregate.peak_rss_bytes,
            aggregate.cpu_seconds,
        )
        for aggregate in aggregates.values()
    )
    identities: dict[int, _ObservedProcessIdentity] = {}
    ambiguous_pids: set[int] = set()
    for hotspot in hotspots:
        if hotspot.pid in identities:
            identities.pop(hotspot.pid)
            ambiguous_pids.add(hotspot.pid)
        elif hotspot.pid not in ambiguous_pids:
            identities[hotspot.pid] = _ObservedProcessIdentity(
                hotspot.name,
                hotspot.parent_name,
            )
    return (
        tuple(
            sorted(
                hotspots,
                key=lambda item: (-item.cpu_seconds, -item.peak_rss_bytes, item.name, item.pid),
            )[:100]
        ),
        identities,
    )
