from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from runtime_tools.proofline import (
    ContractError,
    ExperimentError,
    counterexamples,
    search_counterexample,
)
from runtime_tools.proofline import cli as proofline_cli
from runtime_tools.proofline.counterexamples import CounterexampleResult
from runtime_tools.proofline.experiments import ExperimentResult
from runtime_tools.proofline.verify import ClaimResult, ClaimStatus, VerificationReport


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(("git", *args), cwd=repo, check=True, capture_output=True, text=True)
    return result.stdout.strip()


@pytest.mark.parametrize(
    ("threshold", "maximum", "max_examples", "shrink_budget_exhausted"),
    ((2, 5, 10, False), (50, 100, 2, True)),
)
def test_counterexample_search_reports_budget_limited_shrinking(
    tmp_path: Path,
    threshold: int,
    maximum: int,
    max_examples: int,
    shrink_budget_exhausted: bool,
) -> None:
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
        f"""import argparse
p = argparse.ArgumentParser()
p.add_argument("--value", type=int)
a = p.parse_args()
print(a.value + 1 if a.value >= {threshold} else a.value)
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
        f"parameters:\n  value:\n    type: integer\n    min: 0\n    max: {maximum}\n",
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
        max_examples=max_examples,
        cwd=repo,
    )

    assert result is not None
    assert result.parameters["value"] >= threshold
    assert result.shrink_budget_exhausted is shrink_budget_exhausted
    if shrink_budget_exhausted:
        assert result.parameters["value"] > threshold
    else:
        assert result.parameters == {"value": threshold}
    assert result.experiment.verification.passed is False
    assert result.experiment.baseline_runpack.is_file()
    assert result.experiment.candidate_runpack.is_file()
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_counterexample_search_reuses_the_contract_snapshot_across_runs(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Proofline Test")
    _git(repo, "config", "user.email", "proofline@example.invalid")
    workload = repo / "workload.py"
    contract = repo / "contract.yaml"
    mutated_contract = "name: changed\nassertions:\n  - type: exit_code_equivalent\n"
    workload.write_text(
        "import argparse\nfrom pathlib import Path\n"
        "p = argparse.ArgumentParser()\np.add_argument('--value', type=int)\na = p.parse_args()\n"
        f"Path({str(contract)!r}).write_text({mutated_contract!r}, encoding='utf-8')\n"
        "print(a.value)\n",
        encoding="utf-8",
    )
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    workload.write_text(
        "import argparse\nfrom pathlib import Path\n"
        "p = argparse.ArgumentParser()\np.add_argument('--value', type=int)\na = p.parse_args()\n"
        f"Path({str(contract)!r}).write_text({mutated_contract!r}, encoding='utf-8')\n"
        "print(a.value + 1 if a.value >= 2 else a.value)\n",
        encoding="utf-8",
    )
    _git(repo, "commit", "-am", "candidate")
    contract.write_text(
        "name: original\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    parameters = repo / "parameters.yaml"
    parameters.write_text(
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 5\n",
        encoding="utf-8",
    )

    result = search_counterexample(
        contract,
        parameters,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        output_dir=tmp_path / "counterexample",
        max_examples=10,
        cwd=repo,
    )

    assert result is not None
    assert result.parameters == {"value": 2}
    assert contract.read_text(encoding="utf-8") == mutated_contract
    assert [
        (claim.contract, claim.type, claim.status)
        for claim in result.experiment.verification.results
    ] == [("original", "output_equivalent", "fail")]


def _counterexample_result(tmp_path: Path) -> CounterexampleResult:
    report = VerificationReport(
        "baseline",
        "candidate",
        (
            ClaimResult(
                "contract",
                "output",
                "output_equivalent",
                "fail",
                "equivalent output",
                "different",
            ),
        ),
    )
    return CounterexampleResult(
        {"jobs": 2},
        ExperimentResult(
            tmp_path / "baseline.runpack",
            tmp_path / "candidate.runpack",
            0,
            1,
            report,
        ),
        False,
    )


def test_counterexample_result_has_stable_json_shape(tmp_path: Path) -> None:
    payload = _counterexample_result(tmp_path).as_json_value()

    assert payload["parameters"] == {"jobs": 2}
    assert payload["shrink_budget_exhausted"] is False
    experiment = payload["experiment"]
    assert isinstance(experiment, dict)
    assert experiment["document_type"] == "proofline.experiment"
    assert experiment["format_version"] == "2"
    assert experiment["candidate_exit_code"] == 1
    verification = experiment["verification"]
    assert isinstance(verification, dict)
    assert verification["passed"] is False


@pytest.mark.parametrize("result, expected_status", ((None, 0), ("found", 1)))
def test_counterexample_search_cli_supports_json_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    result: str | None,
    expected_status: int,
) -> None:
    counterexample = None if result is None else _counterexample_result(tmp_path)
    calls: list[dict[str, object]] = []

    def search(*args: object, **kwargs: object) -> CounterexampleResult | None:
        calls.append(kwargs)
        return counterexample

    monkeypatch.setattr(proofline_cli, "search_counterexample", search)

    status = proofline_cli.main(
        [
            "search",
            str(tmp_path / "contract.yaml"),
            "--parameters",
            str(tmp_path / "parameters.yaml"),
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "HEAD",
            "--workload",
            "workload.py",
            "--python",
            str(tmp_path / "workload-python"),
            "--capture-level",
            "passive",
            "--max-examples",
            "7",
            "--format",
            "json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert status == expected_status
    assert captured.err == ""
    assert set(payload) == {
        "counterexample",
        "document_type",
        "format_version",
        "max_examples",
    }
    assert payload["document_type"] == "proofline.search"
    assert payload["format_version"] == "2"
    assert payload["max_examples"] == 7
    assert len(calls) == 1
    output_dir = calls[0].pop("output_dir")
    assert isinstance(output_dir, Path)
    assert output_dir.name.startswith("proofline-results-")
    assert calls == [
        {
            "baseline_ref": "main",
            "candidate_ref": "HEAD",
            "workload": Path("workload.py"),
            "max_examples": 7,
            "python_executable": tmp_path / "workload-python",
            "capture_level": "passive",
        }
    ]
    if counterexample is None:
        assert payload["counterexample"] is None
    else:
        assert payload["counterexample"]["parameters"] == {"jobs": 2}
        assert payload["counterexample"]["shrink_budget_exhausted"] is False


def test_counterexample_search_cli_reports_exhausted_shrink_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = _counterexample_result(tmp_path)
    result = CounterexampleResult(result.parameters, result.experiment, True)
    monkeypatch.setattr(proofline_cli, "search_counterexample", lambda *args, **kwargs: result)

    status = proofline_cli.main(
        [
            "search",
            str(tmp_path / "contract.yaml"),
            "--parameters",
            str(tmp_path / "parameters.yaml"),
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "HEAD",
            "--workload",
            "workload.py",
        ]
    )

    report = capsys.readouterr().out
    assert status == 1
    assert "shrink_budget_exhausted: true" in report
    assert "minimiz" not in report.lower()


@pytest.mark.parametrize(
    ("document", "message"),
    (
        (
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
""",
            "parameter flags must be unique: --value",
        ),
        (
            """
parameters:
  value:
    type: integer
    min: 0
    min: 2
    max: 3
""",
            "found duplicate key 'min'",
        ),
    ),
    ids=("duplicate-flag", "duplicate-key"),
)
def test_counterexample_search_rejects_ambiguous_parameters(
    tmp_path: Path, document: str, message: str
) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(document.strip(), encoding="utf-8")

    with pytest.raises(ContractError, match=message):
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


def test_counterexample_search_normalizes_repository_resolution_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 1\n",
        encoding="utf-8",
    )
    repository = tmp_path / "repo"
    repository.mkdir()
    resolve = Path.resolve

    def fail_repository_resolution(path: Path, strict: bool = False) -> Path:
        if path == repository:
            raise OSError("simulated resolution failure")
        return resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", fail_repository_resolution)

    with pytest.raises(ExperimentError, match="could not resolve working directory"):
        search_counterexample(
            tmp_path / "contract.yaml",
            parameters,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=tmp_path / "output",
            cwd=repository,
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
    contract = tmp_path / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    calls: list[int] = []
    python_executables: list[Path] = []

    def experiment(*args: object, **kwargs: object) -> ExperimentResult:
        workload_args = kwargs["workload_args"]
        assert isinstance(workload_args, tuple)
        value = int(str(workload_args[0]).split("=", 1)[1])
        calls.append(value)
        selected_python = kwargs["python_executable"]
        assert isinstance(selected_python, Path)
        python_executables.append(selected_python)
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

    monkeypatch.setattr(counterexamples, "_run_experiment", experiment)

    result = search_counterexample(
        contract,
        parameters,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        output_dir=tmp_path / "output",
        max_examples=2,
        python_executable=Path(sys.executable),
    )

    assert result is not None
    assert result.parameters["value"] >= 50
    assert len(calls) <= 3  # two search executions plus the preserved reproduction
    assert len(python_executables) >= 2
    assert set(python_executables) == {Path(os.path.abspath(sys.executable))}


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


def test_counterexample_search_rejects_invalid_workload_python_before_loading_inputs(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"

    with pytest.raises(ExperimentError, match="workload Python does not exist"):
        search_counterexample(
            tmp_path / "missing-contract.yaml",
            tmp_path / "missing-parameters.yaml",
            baseline_ref="bad\0ref",
            candidate_ref="bad\0ref",
            workload=Path("bad\0workload.py"),
            output_dir=output,
            python_executable=tmp_path / "missing-python",
        )

    assert not output.exists()


def test_counterexample_search_rejects_dangling_output_before_loading_inputs(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.symlink_to(tmp_path / "missing-output-target")

    with pytest.raises(ExperimentError, match="refusing to reuse output directory"):
        search_counterexample(
            tmp_path / "missing-contract.yaml",
            tmp_path / "missing-parameters.yaml",
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("missing-workload.py"),
            output_dir=output,
        )

    assert output.is_symlink()


def test_counterexample_search_does_not_treat_unverifiable_claims_as_violations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters = tmp_path / "parameters.yaml"
    parameters.write_text(
        "parameters:\n  value:\n    type: integer\n    min: 0\n    max: 1\n",
        encoding="utf-8",
    )
    contract = tmp_path / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
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

    monkeypatch.setattr(counterexamples, "_run_experiment", unverifiable)

    result = search_counterexample(
        contract,
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
    contract = tmp_path / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
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

    monkeypatch.setattr(counterexamples, "_run_experiment", experiment)

    with pytest.raises(ExperimentError, match="did not reproduce"):
        search_counterexample(
            contract,
            parameters,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            output_dir=final_output,
            max_examples=1,
            cwd=tmp_path,
        )
