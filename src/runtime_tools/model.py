"""Normalized records shared by capture and analysis packages."""

from __future__ import annotations

from dataclasses import dataclass

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True, slots=True)
class Execution:
    id: str
    name: str
    started_at_ns: int
    finished_at_ns: int | None
    command: tuple[str, ...]
    working_directory: str
    exit_code: int | None
    revision: str | None
    metadata: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class Entity:
    id: str
    kind: str
    name: str
    parent_entity_id: str | None
    attributes: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class Event:
    id: str
    kind: str
    name: str
    entity_id: str | None
    started_at_ns: int | None
    finished_at_ns: int | None
    clock_domain: str | None
    uncertainty_ns: int | None
    sequence: int | None
    attributes: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class CausalEdge:
    source_event_id: str
    target_event_id: str
    kind: str
    confidence: float
    attributes: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class Measurement:
    name: str
    value: float
    unit: str
    timestamp_ns: int | None
    entity_id: str | None
    attributes: dict[str, JsonValue]
