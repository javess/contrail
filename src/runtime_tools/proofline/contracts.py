"""Strict parsing for explicit Proofline YAML contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import yaml

from runtime_tools.model import JsonValue

SUPPORTED_ASSERTIONS = {
    "output_equivalent",
    "result_equivalence",
    "max_runtime_regression",
    "max_peak_memory_regression",
    "forbid_new_dependency",
    "max_operation_count",
}


class ContractError(ValueError):
    """Raised when a contract is malformed or unsupported."""


@dataclass(frozen=True, slots=True)
class Assertion:
    type: str
    name: str
    config: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class Contract:
    name: str
    description: str | None
    assertions: tuple[Assertion, ...]


def _json_value(value: object, label: str, active_containers: set[int] | None = None) -> JsonValue:
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractError(f"{label} cannot contain non-finite numbers")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, dict)):
        active = active_containers if active_containers is not None else set()
        identity = id(value)
        if identity in active:
            raise ContractError(f"{label} cannot contain recursive values")
        active.add(identity)
        try:
            if isinstance(value, list):
                return [_json_value(item, label, active) for item in value]
            if all(isinstance(key, str) for key in value):
                return {
                    str(key): _json_value(item, label, active) for key, item in value.items()
                }
        finally:
            active.remove(identity)
    raise ContractError(f"{label} must contain only JSON-compatible values")


def _object(value: object, label: str) -> dict[str, JsonValue]:
    normalized = _json_value(value, label)
    if not isinstance(normalized, dict):
        raise ContractError(f"{label} must be an object")
    return normalized


def _required_string(value: JsonValue, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContractError(f"{label} must be a non-empty string")
    return value


def _parse_contract(value: object, default_name: str) -> Contract:
    raw = _object(value, "contract")
    name_value = raw.get("name", default_name)
    name = _required_string(name_value, "contract name")
    description_value = raw.get("description")
    if description_value is not None and not isinstance(description_value, str):
        raise ContractError("contract description must be a string")
    raw_assertions = raw.get("assertions")
    if not isinstance(raw_assertions, list) or not raw_assertions:
        raise ContractError(f"contract {name!r} requires a non-empty assertions list")
    assertions = []
    for index, raw_assertion in enumerate(raw_assertions, 1):
        config = _object(raw_assertion, f"assertion {index}")
        assertion_type = _required_string(config.get("type"), f"assertion {index} type")
        if assertion_type not in SUPPORTED_ASSERTIONS:
            raise ContractError(f"unsupported assertion type: {assertion_type}")
        assertion_name_value = config.get("name", assertion_type.replace("_", "-"))
        assertion_name = _required_string(assertion_name_value, f"assertion {index} name")
        assertions.append(Assertion(assertion_type, assertion_name, config))
    return Contract(name, description_value, tuple(assertions))


def load_contracts(path: Path) -> tuple[Contract, ...]:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ContractError(f"could not read contract file: {path}") from exc
    except yaml.YAMLError as exc:
        raise ContractError(f"invalid contract YAML: {exc}") from exc
    try:
        root = _object(document, "contract document")
    except RecursionError as exc:
        raise ContractError("contract document nesting is too deep") from exc
    raw_contracts = root.get("contracts")
    if raw_contracts is None:
        return (_parse_contract(root, path.stem),)
    if not isinstance(raw_contracts, list) or not raw_contracts:
        raise ContractError("contracts must be a non-empty list")
    return tuple(
        _parse_contract(item, f"{path.stem}-{index}") for index, item in enumerate(raw_contracts, 1)
    )
