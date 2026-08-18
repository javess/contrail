"""Search and shrink bounded parameterized workload counterexamples."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml
from hypothesis import find, settings
from hypothesis.errors import NoSuchExample
from hypothesis.strategies import SearchStrategy, fixed_dictionaries, integers

from runtime_tools.artifacts import artifact_exists
from runtime_tools.model import JsonValue
from runtime_tools.proofline.contracts import ContractError, load_contracts
from runtime_tools.proofline.experiments import (
    ExperimentError,
    ExperimentResult,
    _run_experiment,
    _validate_python_executable,
)
from runtime_tools.yaml_support import YamlInputError, load_yaml_file

MAX_COUNTEREXAMPLE_EXAMPLES = 1_000
MAX_COUNTEREXAMPLE_PARAMETERS = 64
MAX_PARAMETER_FILE_BYTES = 1024 * 1024
_MIN_PARAMETER_INTEGER = -(1 << 63)
_MAX_PARAMETER_INTEGER = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class IntegerParameter:
    name: str
    flag: str
    minimum: int
    maximum: int


@dataclass(frozen=True, slots=True)
class CounterexampleResult:
    parameters: dict[str, int]
    experiment: ExperimentResult
    shrink_budget_exhausted: bool = False

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "parameters": dict(sorted(self.parameters.items())),
            "experiment": self.experiment.as_json_value(),
            "shrink_budget_exhausted": self.shrink_budget_exhausted,
        }


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ContractError(f"{label} must be an object with string keys")
    return cast(dict[str, object], value)


def _integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ContractError(f"{label} must be an integer")
    if not _MIN_PARAMETER_INTEGER <= value <= _MAX_PARAMETER_INTEGER:
        raise ContractError(f"{label} exceeds the supported integer range")
    return value


def _reject_unknown_fields(value: dict[str, object], allowed: set[str], label: str) -> None:
    unknown = sorted(value.keys() - allowed)
    if unknown:
        raise ContractError(f"{label} contains unsupported fields: {', '.join(unknown)}")


def load_parameters(path: Path) -> tuple[IntegerParameter, ...]:
    try:
        document = load_yaml_file(
            path,
            label="parameter file",
            max_bytes=MAX_PARAMETER_FILE_BYTES,
        )
    except YamlInputError as exc:
        raise ContractError(str(exc)) from exc
    except yaml.YAMLError as exc:
        raise ContractError(f"invalid parameter YAML: {exc}") from exc
    root = _object(document, "parameter document")
    _reject_unknown_fields(root, {"parameters"}, "parameter document")
    raw_parameters = _object(root.get("parameters"), "parameters")
    if not raw_parameters:
        raise ContractError("parameters cannot be empty")
    if len(raw_parameters) > MAX_COUNTEREXAMPLE_PARAMETERS:
        raise ContractError(
            f"parameters cannot contain more than {MAX_COUNTEREXAMPLE_PARAMETERS} entries"
        )
    result = []
    flags: set[str] = set()
    for name, raw_spec in raw_parameters.items():
        if not isinstance(name, str) or not name or "\0" in name:
            raise ContractError("parameter names must be non-empty strings")
        spec = _object(raw_spec, f"parameter {name}")
        _reject_unknown_fields(spec, {"type", "min", "max", "flag"}, f"parameter {name}")
        if spec.get("type") != "integer":
            raise ContractError(f"parameter {name} supports only type: integer")
        minimum = _integer(spec.get("min"), f"parameter {name} min")
        maximum = _integer(spec.get("max"), f"parameter {name} max")
        if maximum < minimum:
            raise ContractError(f"parameter {name} max must be >= min")
        flag = spec.get("flag", f"--{name.replace('_', '-')}")
        if (
            not isinstance(flag, str)
            or not flag.startswith("-")
            or flag in {"-", "--"}
            or "=" in flag
            or "\0" in flag
            or any(character.isspace() for character in flag)
        ):
            raise ContractError(
                f"parameter {name} flag must be an option name without whitespace or '='"
            )
        if flag in flags:
            raise ContractError(f"parameter flags must be unique: {flag}")
        flags.add(flag)
        result.append(IntegerParameter(name, flag, minimum, maximum))
    return tuple(result)


def _strategy(parameters: tuple[IntegerParameter, ...]) -> SearchStrategy[dict[str, int]]:
    return fixed_dictionaries(
        {
            parameter.name: integers(min_value=parameter.minimum, max_value=parameter.maximum)
            for parameter in parameters
        }
    )


def _arguments(parameters: tuple[IntegerParameter, ...], values: dict[str, int]) -> tuple[str, ...]:
    return tuple(f"{parameter.flag}={values[parameter.name]}" for parameter in parameters)


def _has_violation(experiment: ExperimentResult) -> bool:
    return any(result.status == "fail" for result in experiment.verification.results)


def _temporary_search_directory() -> tempfile.TemporaryDirectory[str]:
    try:
        return tempfile.TemporaryDirectory(prefix="proofline-search-")
    except OSError as exc:
        raise ExperimentError(f"could not create temporary search directory: {exc}") from exc


def _search_repository(cwd: Path | None) -> Path:
    if cwd is not None and not isinstance(cwd, Path):
        raise ExperimentError("working directory must be a path")
    try:
        return (cwd if cwd is not None else Path.cwd()).resolve()
    except (OSError, RuntimeError) as exc:
        raise ExperimentError("could not resolve working directory") from exc


def search_counterexample(
    contract: Path,
    parameters_path: Path,
    *,
    baseline_ref: str,
    candidate_ref: str,
    workload: Path,
    output_dir: Path,
    max_examples: int = 25,
    cwd: Path | None = None,
    python_executable: Path | None = None,
) -> CounterexampleResult | None:
    if not isinstance(max_examples, int) or isinstance(max_examples, bool):
        raise ContractError("max_examples must be an integer")
    if max_examples <= 0:
        raise ContractError("max_examples must be positive")
    if max_examples > MAX_COUNTEREXAMPLE_EXAMPLES:
        raise ContractError(f"max_examples cannot exceed {MAX_COUNTEREXAMPLE_EXAMPLES}")
    if artifact_exists(output_dir):
        raise ExperimentError(f"refusing to reuse output directory: {output_dir}")
    python_executable = _validate_python_executable(python_executable)
    if not output_dir.parent.is_dir():
        raise ExperimentError(f"output parent directory does not exist: {output_dir.parent}")
    parameters = load_parameters(parameters_path)
    repo = _search_repository(cwd)
    contracts = load_contracts(contract)
    cache: dict[tuple[tuple[str, int], ...], bool] = {}
    shrink_budget_exhausted = False

    def violates(values: dict[str, int]) -> bool:
        nonlocal shrink_budget_exhausted
        key = tuple(sorted(values.items()))
        if key in cache:
            return cache[key]
        if len(cache) >= max_examples:
            shrink_budget_exhausted = True
            return False
        with _temporary_search_directory() as directory:
            experiment = _run_experiment(
                contracts,
                baseline_ref=baseline_ref,
                candidate_ref=candidate_ref,
                workload=workload,
                workload_args=_arguments(parameters, values),
                output_dir=Path(directory) / "artifacts",
                cwd=repo,
                python_executable=python_executable,
            )
            result = _has_violation(experiment)
            cache[key] = result
            return result

    try:
        values = find(
            _strategy(parameters),
            violates,
            settings=settings(
                max_examples=max_examples,
                deadline=None,
                database=None,
                derandomize=True,
            ),
        )
    except NoSuchExample:
        return None
    experiment = _run_experiment(
        contracts,
        baseline_ref=baseline_ref,
        candidate_ref=candidate_ref,
        workload=workload,
        workload_args=_arguments(parameters, values),
        output_dir=output_dir,
        cwd=repo,
        python_executable=python_executable,
    )
    if not _has_violation(experiment):
        raise ExperimentError("selected counterexample did not reproduce on the preserved run")
    return CounterexampleResult(values, experiment, shrink_budget_exhausted)
