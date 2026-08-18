from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import pytest

from runtime_tools.proofline import (
    ContractError,
    ExperimentError,
    counterexamples,
    search_counterexample,
)
from runtime_tools.proofline.experiments import ExperimentResult
from runtime_tools.proofline.verify import ClaimResult, ClaimStatus, VerificationReport


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(("git", *args), cwd=repo, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def test_counterexample_search_finds_and_shrinks_minimal_integer_input(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Proofline Test")
    _git(repo, "config", "user.email", "proofline@example.invalid")
    workload = repo / "workload.py"
    workload.write_text(
        """import argparse
p = argparse.ArgumentParser()
p.add_argument("--value", type=int)
a = p.parse_args()
print(a.value)
""",
        encoding="utf-8",
    )
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    workload.write_text(
        """import argparse
p = argparse.ArgumentParser()
p.add_argument("--value", type=int)
a = p.parse_args()
print(a.value + 1 if a.value >= 2 else a.value)
""",
        encoding="utf-8",
    )
    _git(repo, "commit", "-am", "candidate")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n", encoding="utf-8"
    )
    parameters = repo / "parameters.yaml"
    parameters.write_text(
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 5\n",
        encoding="utf-8",
    )
    output = tmp_path / "counterexample"

    result = search_counterexample(
        contract,
        parameters,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        output_dir=output,
        max_examples=10,
        cwd=repo,
    )

    assert result is not None
    assert result.parameters == {"value": 2}
    assert result.experiment.verification.passed is False
    assert result.experiment.baseline_runpack.is_file()
    assert result.experiment.candidate_runpack.is_file()
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_counterexample_search_rejects_duplicate_parameter_flags(tmp_path: Path) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        """
parameters:
  first:
    type: integer
    min: 0
    max: 1
    flag: --value
  second:
    type: integer
    min: 0
    max: 1
    flag: --value
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="parameter flags must be unique: --value"):
        search_counterexample(
            tmp_path / "contract.yaml",
            parameters,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=tmp_path / "output",
        )


def test_counterexample_search_normalizes_temporary_directory_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise PermissionError("temporary storage denied")

    monkeypatch.setattr(tempfile, "TemporaryDirectory", fail)

    with pytest.raises(ExperimentError, match="could not create temporary search directory"):
        counterexamples._temporary_search_directory()


def test_counterexample_search_rejects_duplicate_parameter_keys(tmp_path: Path) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        """
parameters:
  value:
    type: integer
    min: 0
    min: 2
    max: 3
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="found duplicate key 'min'"):
        search_counterexample(
            tmp_path / "contract.yaml",
            parameters,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=tmp_path / "output",
        )


def test_counterexample_search_rejects_non_string_mapping_keys(tmp_path: Path) -> None:
    parameters = tmp_path / "non-string-key.yaml"
    parameters.write_text(
        "parameters:\n  size:\n    type: integer\n    min: 0\n    max: 1\n    2: invalid\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="parameter size must be an object with string keys"):
        counterexamples.load_parameters(parameters)


@pytest.mark.parametrize(
    "document",
    (
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 1\n    minimum: 0\n",
        "version: 1\nparameters:\n  value:\n    type: integer\n    min: 0\n    max: 1\n",
    ),
)
def test_counterexample_parameters_reject_unknown_fields(tmp_path: Path, document: str) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(document, encoding="utf-8")

    with pytest.raises(ContractError, match="contains unsupported fields"):
        counterexamples.load_parameters(parameters)


@pytest.mark.parametrize("flag", ("--value=other", "--value other", "--"))
def test_counterexample_parameters_reject_ambiguous_flags(tmp_path: Path, flag: str) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        f"parameters:\n  value:\n    type: integer\n    min: 0\n    max: 1\n    flag: {flag!r}\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="option name without whitespace"):
        counterexamples.load_parameters(parameters)


def test_counterexample_parameters_reject_nul_in_flags(tmp_path: Path) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        """
parameters:
  value:
    type: integer
    min: 0
    max: 1
    flag: "--value\\0"
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="option name without whitespace"):
        counterexamples.load_parameters(parameters)


def test_counterexample_search_caps_parameter_count(tmp_path: Path) -> None:
    parameters = tmp_path / "parameters.yaml"
    entries = "\n".join(
        f"  value_{index}:\n    type: integer\n    min: 0\n    max: 1" for index in range(65)
    )
    parameters.write_text(f"parameters:\n{entries}\n", encoding="utf-8")

    with pytest.raises(ContractError, match="parameters cannot contain more than 64 entries"):
        search_counterexample(
            tmp_path / "contract.yaml",
            parameters,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=tmp_path / "output",
        )


def test_counterexample_parameters_reject_oversized_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = tmp_path / "oversized.yaml"
    parameters.write_bytes(b"{" + b" " * 32 + b"}")
    monkeypatch.setattr(counterexamples, "MAX_PARAMETER_FILE_BYTES", 32)

    with pytest.raises(ContractError, match="parameter file exceeds the 32-byte input limit"):
        counterexamples.load_parameters(parameters)


def test_counterexample_parameters_reject_non_utf8_yaml(tmp_path: Path) -> None:
    parameters = tmp_path / "non-utf8.yaml"
    parameters.write_bytes(b"\xff")

    with pytest.raises(ContractError, match="parameter file must be UTF-8"):
        counterexamples.load_parameters(parameters)


def test_counterexample_parameters_reject_non_utf8_yaml_strings(tmp_path: Path) -> None:
    parameters = tmp_path / "non-utf8-string.yaml"
    parameters.write_text(
        'parameters:\n  "bad-\\uD800":\n    type: integer\n    min: 0\n    max: 1\n',
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="string that is not valid UTF-8"):
        counterexamples.load_parameters(parameters)


def test_counterexample_parameters_normalize_excessive_yaml_nesting(tmp_path: Path) -> None:
    parameters = tmp_path / "nested.yaml"
    parameters.write_text("value: " + "[" * 2_000 + "0" + "]" * 2_000, encoding="utf-8")

    with pytest.raises(ContractError, match="parameter file nesting is too deep"):
        counterexamples.load_parameters(parameters)


def test_counterexample_search_caps_parameter_integer_bounds(tmp_path: Path) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        f"parameters:\n  value:\n    type: integer\n    min: 0\n    max: {1 << 63}\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="parameter value max exceeds the supported"):
        search_counterexample(
            tmp_path / "contract.yaml",
            parameters,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=tmp_path / "output",
        )


def test_counterexample_search_caps_example_count_before_loading_parameters(
    tmp_path: Path,
) -> None:
    with pytest.raises(ContractError, match="max_examples cannot exceed 1000"):
        search_counterexample(
            tmp_path / "contract.yaml",
            tmp_path / "missing-parameters.yaml",
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=tmp_path / "output",
            max_examples=1_001,
        )

    with pytest.raises(ContractError, match="max_examples must be an integer"):
        search_counterexample(
            tmp_path / "contract.yaml",
            tmp_path / "missing-parameters.yaml",
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=tmp_path / "output",
            max_examples=True,
        )


def test_counterexample_search_hard_limits_distinct_experiment_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 100\n",
        encoding="utf-8",
    )
    calls: list[int] = []

    def experiment(*args: object, **kwargs: object) -> ExperimentResult:
        workload_args = kwargs["workload_args"]
        assert isinstance(workload_args, tuple)
        value = int(str(workload_args[0]).split("=", 1)[1])
        calls.append(value)
        status: ClaimStatus = "fail" if value >= 50 else "pass"
        report = VerificationReport(
            "baseline",
            "candidate",
            (
                ClaimResult(
                    "contract",
                    "output",
                    "output_equivalent",
                    status,
                    "equivalent output",
                    "different" if status == "fail" else "equivalent",
                ),
            ),
        )
        output_dir = kwargs["output_dir"]
        assert isinstance(output_dir, Path)
        return ExperimentResult(
            output_dir / "baseline.runpack",
            output_dir / "candidate.runpack",
            0,
            0,
            report,
        )

    monkeypatch.setattr(counterexamples, "run_experiment", experiment)

    result = search_counterexample(
        tmp_path / "contract.yaml",
        parameters,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        output_dir=tmp_path / "output",
        max_examples=2,
    )

    assert result is not None
    assert result.parameters["value"] >= 50
    assert len(calls) <= 3  # two search executions plus the preserved reproduction


def test_counterexample_search_rejects_existing_output_before_loading_parameters(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()

    with pytest.raises(ExperimentError, match="refusing to reuse output directory"):
        search_counterexample(
            tmp_path / "contract.yaml",
            tmp_path / "missing-parameters.yaml",
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=output,
        )


def test_counterexample_search_does_not_treat_unverifiable_claims_as_violations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 1\n",
        encoding="utf-8",
    )
    calls = 0

    def unverifiable(*args: object, **kwargs: object) -> ExperimentResult:
        nonlocal calls
        calls += 1
        report = VerificationReport(
            "baseline",
            "candidate",
            (
                ClaimResult(
                    "contract",
                    "output_equivalent",
                    "output_equivalent",
                    "unverifiable",
                    "equivalent output",
                    "output identity unavailable",
                ),
            ),
        )
        return ExperimentResult(
            tmp_path / "baseline.runpack",
            tmp_path / "candidate.runpack",
            0,
            0,
            report,
        )

    monkeypatch.setattr(counterexamples, "run_experiment", unverifiable)

    result = search_counterexample(
        tmp_path / "contract.yaml",
        parameters,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        output_dir=tmp_path / "output",
        max_examples=4,
        cwd=tmp_path,
    )

    assert result is None
    assert calls > 0


def test_counterexample_search_rejects_a_nonreproducible_final_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 0\n",
        encoding="utf-8",
    )
    final_output = tmp_path / "output"

    def experiment(*args: object, **kwargs: object) -> ExperimentResult:
        status: ClaimStatus = "pass" if kwargs["output_dir"] == final_output else "fail"
        report = VerificationReport(
            "baseline",
            "candidate",
            (
                ClaimResult(
                    "contract",
                    "output-equivalent",
                    "output_equivalent",
                    status,
                    "equivalent output",
                    "equivalent" if status == "pass" else "different",
                ),
            ),
        )
        return ExperimentResult(
            tmp_path / "baseline.runpack",
            tmp_path / "candidate.runpack",
            0,
            0,
            report,
        )

    monkeypatch.setattr(counterexamples, "run_experiment", experiment)

    with pytest.raises(ExperimentError, match="did not reproduce"):
        search_counterexample(
            tmp_path / "contract.yaml",
            parameters,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=final_output,
            max_examples=1,
            cwd=tmp_path,
        )
