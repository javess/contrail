from __future__ import annotations

import subprocess
from pathlib import Path

from runtime_tools.proofline import run_experiment


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", *args),
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_proofline_experiment_isolates_refs_and_preserves_runpacks(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Proofline Test")
    _git(repo, "config", "user.email", "proofline@example.invalid")
    workload = repo / "workload.py"
    workload.write_text("print('baseline')\n", encoding="utf-8")
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    workload.write_text("print('candidate')\n", encoding="utf-8")
    _git(repo, "commit", "-am", "candidate")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    output = tmp_path / "results"

    result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        workload_args=(),
        output_dir=output,
        cwd=repo,
    )

    assert result.baseline_exit_code == 0
    assert result.candidate_exit_code == 0
    assert result.baseline_runpack.is_file()
    assert result.candidate_runpack.is_file()
    assert result.verification.passed is False
    assert result.verification.results[0].observed == "different"
    worktrees = _git(repo, "worktree", "list", "--porcelain")
    assert worktrees.count("worktree ") == 1
    assert _git(repo, "status", "--short") == "?? contract.yaml"
