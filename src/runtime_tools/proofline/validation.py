"""Parse and summarize Proofline inputs without executing a workload."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.json_support import JsonDocumentModel
from runtime_tools.proofline.contracts import load_contracts
from runtime_tools.proofline.counterexamples import load_parameters


@dataclass(frozen=True, slots=True)
class ValidationReport(JsonDocumentModel):
    document_type = "proofline.validation"

    contract_count: int
    assertion_count: int
    assertion_types: tuple[str, ...]
    parameter_count: int | None


def validate_inputs(contract_path: Path, parameters_path: Path | None) -> ValidationReport:
    contracts = load_contracts(contract_path)
    assertion_types = tuple(
        sorted({assertion.type for contract in contracts for assertion in contract.assertions})
    )
    parameter_count = len(load_parameters(parameters_path)) if parameters_path is not None else None
    return ValidationReport(
        len(contracts),
        sum(len(contract.assertions) for contract in contracts),
        assertion_types,
        parameter_count,
    )


def render_validation(report: ValidationReport, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report.as_json_value(), allow_nan=False, indent=2, sort_keys=True)
    parameters = "not provided" if report.parameter_count is None else str(report.parameter_count)
    return "\n".join(
        (
            "PROOFLINE INPUTS VALID",
            "",
            f"contracts:  {report.contract_count}",
            f"assertions: {report.assertion_count}",
            f"parameters: {parameters}",
        )
    )
