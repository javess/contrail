from __future__ import annotations

import subprocess
from pathlib import Path

from runtime_tools.proofline import search_counterexample


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
