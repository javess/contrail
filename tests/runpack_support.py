from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from runtime_tools.model import CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.storage import RunpackWriter


def write_runpack(
    path: Path,
    execution: Execution,
    *,
    entities: Iterable[Entity] = (),
    events: Iterable[Event] = (),
    causal_edges: Iterable[CausalEdge] = (),
    measurements: Iterable[Measurement] = (),
) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(execution)
        writer.add_entities(entities)
        writer.add_events(events)
        writer.add_causal_edges(causal_edges)
        writer.add_measurements(measurements)
