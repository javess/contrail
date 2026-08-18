from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from runtime_tools.proofline import ContractError, ExperimentError, experiments, run_experiment


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", *args),
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_proofline_bounds_git_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def time_out(*args: object, **kwargs: object) -> None:
        assert kwargs["timeout"] == experiments.GIT_COMMAND_TIMEOUT_SECONDS
        raise subprocess.TimeoutExpired(("git", "status"), kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", time_out)

    with pytest.raises(ExperimentError, match="Git command timed out after 120 seconds"):
        experiments._git(tmp_path, "status")


def test_proofline_normalizes_git_launch_failures(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise OSError("argument vector is too large")

    monkeypatch.setattr(subprocess, "run", fail)

    with pytest.raises(ExperimentError, match="could not execute Git: argument vector"):
        experiments._git(tmp_path, "status")


def test_proofline_preserves_trailing_whitespace_in_repository_paths(tmp_path: Path) -> None:
    repo = tmp_path / "repo "
    repo.mkdir()
    _git(repo, "init", "-b", "main")

    assert experiments._repo_root(repo) == repo


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


def test_proofline_preserves_primary_error_when_worktree_cleanup_also_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outside = tmp_path / "outside.py"
    outside.write_text("print('outside')\n", encoding="utf-8")
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Proofline Test")
    _git(repo, "config", "user.email", "proofline@example.invalid")
    (repo / "workload.py").symlink_to(outside)
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "symlink workload")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    git = experiments._git

    def fail_after_worktree_removal(git_repo: Path, *args: str, capture: bool = False) -> str:
        result = git(git_repo, *args, capture=capture)
        if args[:2] == ("worktree", "remove"):
            raise ExperimentError("simulated cleanup failure")
        return result

    monkeypatch.setattr(experiments, "_git", fail_after_worktree_removal)

    with pytest.raises(ExperimentError, match="must resolve inside the isolated worktree") as error:
        run_experiment(
            contract,
            baseline_ref="main",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=tmp_path / "results",
            cwd=repo,
        )

    assert error.value.__notes__ == [
        "Proofline worktree cleanup also failed: simulated cleanup failure"
    ]
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1
    assert not (tmp_path / "results").exists()


def test_proofline_rejects_option_like_git_refs_before_creating_outputs(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Proofline Test")
    _git(repo, "config", "user.email", "proofline@example.invalid")
    workload = repo / "workload.py"
    workload.write_text("print('workload')\n", encoding="utf-8")
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")

    output = tmp_path / "results"
    with pytest.raises(ExperimentError, match="cannot start with"):
        run_experiment(
            repo / "contract.yaml",
            baseline_ref="--help",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=output,
            cwd=repo,
        )

    assert not output.exists()
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


@pytest.mark.parametrize(
    ("ref", "message"),
    (("bad\0ref", "cannot contain NUL bytes"), ("bad-\udcff", "must be valid UTF-8")),
)
def test_proofline_rejects_unrepresentable_git_refs_before_loading_contracts(
    tmp_path: Path, ref: str, message: str
) -> None:
    output = tmp_path / "results"

    with pytest.raises(ExperimentError, match=message):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref=ref,
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=output,
            cwd=tmp_path,
        )

    assert not output.exists()


def test_proofline_bounds_git_refs_before_loading_contracts(tmp_path: Path) -> None:
    with pytest.raises(ExperimentError, match="Git refs cannot exceed 4096 UTF-8 bytes"):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="a" * 4_097,
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=tmp_path / "results",
            cwd=tmp_path,
        )


@pytest.mark.parametrize(
    ("workload", "workload_args", "message"),
    (
        (Path("bad\0workload.py"), (), "workload path cannot contain NUL bytes"),
        (Path("bad-\udcff.py"), (), "workload path must be valid UTF-8"),
        (Path("workload.py"), ("bad\0argument",), "workload arguments cannot contain NUL bytes"),
        (Path("workload.py"), ("bad-\udcff",), "workload arguments must be valid UTF-8"),
    ),
)
def test_proofline_rejects_unrepresentable_workloads_before_loading_contracts(
    tmp_path: Path,
    workload: Path,
    workload_args: tuple[str, ...],
    message: str,
) -> None:
    output = tmp_path / "results"

    with pytest.raises(ExperimentError, match=message):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="main",
            candidate_ref="main",
            workload=workload,
            workload_args=workload_args,
            output_dir=output,
            cwd=tmp_path,
        )

    assert not output.exists()


@pytest.mark.parametrize(
    ("workload_args", "message"),
    (
        (("value",) * 1_025, "cannot contain more than 1024 entries"),
        (("x" * (1024 * 1024),), "cannot exceed 1048576 UTF-8 bytes"),
    ),
)
def test_proofline_bounds_workload_invocations_before_loading_contracts(
    tmp_path: Path, workload_args: tuple[str, ...], message: str
) -> None:
    with pytest.raises(ExperimentError, match=message):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="main",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=workload_args,
            output_dir=tmp_path / "results",
            cwd=tmp_path,
        )


def test_proofline_validates_contracts_before_creating_worktrees_or_outputs(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Proofline Test")
    _git(repo, "config", "user.email", "proofline@example.invalid")
    workload = repo / "workload.py"
    workload.write_text("raise RuntimeError('must not execute')\n", encoding="utf-8")
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")
    contract = repo / "invalid.yaml"
    contract.write_text("name: invalid\nassertions:\n  - type: unknown\n", encoding="utf-8")
    output = tmp_path / "results"

    with pytest.raises(ContractError, match="unsupported assertion type: unknown"):
        run_experiment(
            contract,
            baseline_ref="main",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=output,
            cwd=repo,
        )

    assert not output.exists()
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1
