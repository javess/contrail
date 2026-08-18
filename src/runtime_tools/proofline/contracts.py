"""Strict parsing for explicit Proofline YAML contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import yaml

from runtime_tools.model import JsonValue
from runtime_tools.yaml_support import YamlInputError, load_yaml_file

_ASSERTION_FIELDS = {
    "candidate_exit_success": set(),
    "exit_code_equivalent": set(),
    "output_equivalent": set(),
    "result_equivalence": set(),
    "max_runtime_regression": {"percent"},
    "max_cpu_time_regression": {"percent"},
    "max_peak_memory_regression": {"percent"},
    "forbid_new_dependency": {"from", "to"},
    "max_operation_count": {"operation", "relative_to", "factor"},
    "max_operation_error_count": {"operation", "relative_to", "factor"},
}
SUPPORTED_ASSERTIONS = set(_ASSERTION_FIELDS)
MAX_CONTRACT_BYTES = 1024 * 1024
MAX_CONTRACT_ASSERTIONS = 1_000


class ContractError(ValueError):
    """Raised when a contract is malformed or unsupported."""


@dataclass(frozen=True, slots=True)
class Assertion:
    type: str
    name: str
    config: dict[str, JsonValue]

    def as_json_value(self) -> dict[str, JsonValue]:
        """Return the resolved assertion policy in its canonical report shape."""
        return {
            "type": self.type,
            "name": self.name,
            **{key: self.config[key] for key in sorted(self.config.keys() - {"type", "name"})},
        }


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
                return {str(key): _json_value(item, label, active) for key, item in value.items()}
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


def _reject_unknown_fields(value: dict[str, JsonValue], allowed: set[str], label: str) -> None:
    unknown = sorted(value.keys() - allowed)
    if unknown:
        raise ContractError(f"{label} contains unsupported fields: {', '.join(unknown)}")


def _validate_assertion_fields(assertion_type: str, config: dict[str, JsonValue]) -> None:
    required = _ASSERTION_FIELDS[assertion_type]
    missing = sorted(required - config.keys())
    if missing:
        raise ContractError(f"{assertion_type} requires fields: {', '.join(missing)}")
    for field in required:
        value = config[field]
        if field in {"percent", "factor"}:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ContractError(f"{assertion_type} requires numeric {field}")
            try:
                number = float(value)
            except OverflowError as exc:
                raise ContractError(f"{assertion_type} {field} exceeds the numeric range") from exc
            if not math.isfinite(number):
                raise ContractError(f"{assertion_type} {field} must be finite")
            if number < 0:
                raise ContractError(f"{assertion_type} {field} cannot be negative")
        elif not isinstance(value, str) or not value:
            raise ContractError(f"{assertion_type} requires string {field}")
    if config.get("relative_to") not in (None, "baseline"):
        raise ContractError(f"{assertion_type} relative_to must be baseline")


def parse_assertion(value: object, *, label: str = "assertion") -> Assertion:
    """Parse one in-memory assertion using the contract input rules."""
    config = _object(value, label)
    assertion_type = _required_string(config.get("type"), f"{label} type")
    if assertion_type not in SUPPORTED_ASSERTIONS:
        raise ContractError(f"unsupported assertion type: {assertion_type}")
    _reject_unknown_fields(
        config,
        {"type", "name", *_ASSERTION_FIELDS[assertion_type]},
        f"{assertion_type} assertion",
    )
    _validate_assertion_fields(assertion_type, config)
    assertion_name_value = config.get("name", assertion_type.replace("_", "-"))
    assertion_name = _required_string(assertion_name_value, f"{label} name")
    return Assertion(assertion_type, assertion_name, config)


def _parse_contract(value: object, default_name: str) -> Contract:
    raw = _object(value, "contract")
    _reject_unknown_fields(raw, {"name", "description", "assertions"}, "contract")
    name_value = raw.get("name", default_name)
    name = _required_string(name_value, "contract name")
    description_value = raw.get("description")
    if description_value is not None and not isinstance(description_value, str):
        raise ContractError("contract description must be a string")
    raw_assertions = raw.get("assertions")
    if not isinstance(raw_assertions, list) or not raw_assertions:
        raise ContractError(f"contract {name!r} requires a non-empty assertions list")
    assertions = tuple(
        parse_assertion(raw_assertion, label=f"assertion {index}")
        for index, raw_assertion in enumerate(raw_assertions, 1)
    )
    return Contract(name, description_value, assertions)


def _enforce_assertion_limit(raw_contracts: tuple[JsonValue, ...]) -> None:
    assertion_count = 0
    for raw_contract in raw_contracts:
        if not isinstance(raw_contract, dict):
            continue
        raw_assertions = raw_contract.get("assertions")
        if not isinstance(raw_assertions, list):
            continue
        assertion_count += len(raw_assertions)
        if assertion_count > MAX_CONTRACT_ASSERTIONS:
            raise ContractError(
                f"contract document exceeds the {MAX_CONTRACT_ASSERTIONS}-assertion input limit"
            )


def load_contracts(path: Path) -> tuple[Contract, ...]:
    try:
        document = load_yaml_file(path, label="contract file", max_bytes=MAX_CONTRACT_BYTES)
    except YamlInputError as exc:
        raise ContractError(str(exc)) from exc
    except yaml.YAMLError as exc:
        raise ContractError(f"invalid contract YAML: {exc}") from exc
    try:
        root = _object(document, "contract document")
    except RecursionError as exc:
        raise ContractError("contract document nesting is too deep") from exc
    raw_contracts = root.get("contracts")
    contracts: tuple[Contract, ...]
    if raw_contracts is None:
        _enforce_assertion_limit((root,))
        contracts = (_parse_contract(root, path.stem),)
    else:
        _reject_unknown_fields(root, {"contracts"}, "contract document")
        if not isinstance(raw_contracts, list) or not raw_contracts:
            raise ContractError("contracts must be a non-empty list")
        _enforce_assertion_limit(tuple(raw_contracts))
        contracts = tuple(
            _parse_contract(item, f"{path.stem}-{index}")
            for index, item in enumerate(raw_contracts, 1)
        )
    assertion_count = sum(len(contract.assertions) for contract in contracts)
    if assertion_count > MAX_CONTRACT_ASSERTIONS:
        raise ContractError(
            f"contract document exceeds the {MAX_CONTRACT_ASSERTIONS}-assertion input limit"
        )
    return contracts
