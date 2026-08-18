"""Execute comparable workloads in isolated Git worktrees."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.capture import CaptureError, record_process
from runtime_tools.model import JsonValue
from runtime_tools.proofline.contracts import load_contracts
from runtime_tools.proofline.verify import VerificationReport, verify_contracts


class ExperimentError(ValueError):
    """Raised when an isolated comparison cannot be executed safely."""


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    baseline_runpack: Path
    candidate_runpack: Path
    baseline_exit_code: int
    candidate_exit_code: int
    verification: VerificationReport

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "baseline_runpack": str(self.baseline_runpack),
            "candidate_runpack": str(self.candidate_runpack),
            "baseline_exit_code": self.baseline_exit_code,
            "candidate_exit_code": self.candidate_exit_code,
            "verification": self.verification.as_json_value(),
        }


def _git(repo: Path, *args: str, capture: bool = False) -> str:
    try:
        result = subprocess.run(
            ("git", *args),
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise ExperimentError("git is required for Proofline execution") from exc
    except subprocess.CalledProcessError as exc:
        message = exc.stderr.strip() or f"git {' '.join(args)} failed"
        raise ExperimentError(message) from exc
    return result.stdout.strip() if capture else ""


def _repo_root(cwd: Path) -> Path:
    value = _git(cwd, "rev-parse", "--show-toplevel", capture=True)
    root = Path(value)
    if not root.is_dir():
        raise ExperimentError("Git repository root does not exist")
    return root


def _validate_ref(ref: str) -> None:
    if not ref or ref.startswith("-"):
        raise ExperimentError("Git refs must be non-empty and cannot start with '-'")


def _resolve_commit(repo: Path, ref: str) -> str:
    _validate_ref(ref)
    return _git(repo, "rev-parse", "--verify", f"{ref}^{{commit}}", capture=True)


def _workload_path(worktree: Path, workload: Path) -> Path:
    candidate = worktree / workload
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ExperimentError(f"workload does not exist at ref: {workload}") from exc
    if not resolved.is_relative_to(worktree.resolve()):
        raise ExperimentError(f"workload must resolve inside the isolated worktree: {workload}")
    if not resolved.is_file():
        raise ExperimentError(f"workload is not a file at ref: {workload}")
    return resolved


def _create_output_directory(output_dir: Path) -> None:
    try:
        output_dir.mkdir()
    except FileExistsError as exc:
        raise ExperimentError(f"refusing to reuse output directory: {output_dir}") from exc
    except OSError as exc:
        raise ExperimentError(f"could not create output directory: {output_dir}") from exc


def run_experiment(
    contract: Path,
    *,
    baseline_ref: str,
    candidate_ref: str,
    workload: Path,
    workload_args: tuple[str, ...],
    output_dir: Path,
    cwd: Path | None = None,
) -> ExperimentResult:
    if workload.is_absolute() or ".." in workload.parts:
        raise ExperimentError("workload must be a repository-relative path")
    _validate_ref(baseline_ref)
    _validate_ref(candidate_ref)
    contract = contract.resolve()
    load_contracts(contract)
    repo = _repo_root((cwd or Path.cwd()).resolve())
    if output_dir.exists():
        raise ExperimentError(f"refusing to reuse output directory: {output_dir}")
    if not output_dir.parent.is_dir():
        raise ExperimentError(f"output parent directory does not exist: {output_dir.parent}")
    baseline_commit = _resolve_commit(repo, baseline_ref)
    candidate_commit = _resolve_commit(repo, candidate_ref)

    baseline_runpack = output_dir / "baseline.runpack"
    candidate_runpack = output_dir / "candidate.runpack"
    temporary_root = Path(tempfile.mkdtemp(prefix="proofline-worktrees-"))
    baseline_tree = temporary_root / "baseline"
    candidate_tree = temporary_root / "candidate"
    added: list[Path] = []
    try:
        _git(repo, "worktree", "add", "--detach", str(baseline_tree), baseline_commit)
        added.append(baseline_tree)
        _git(repo, "worktree", "add", "--detach", str(candidate_tree), candidate_commit)
        added.append(candidate_tree)
        baseline_workload = _workload_path(baseline_tree, workload)
        candidate_workload = _workload_path(candidate_tree, workload)
        _create_output_directory(output_dir)
        try:
            baseline_exit = record_process(
                (sys.executable, str(baseline_workload), *workload_args),
                baseline_runpack,
                name=f"baseline:{baseline_ref}",
                cwd=baseline_tree,
            )
            candidate_exit = record_process(
                (sys.executable, str(candidate_workload), *workload_args),
                candidate_runpack,
                name=f"candidate:{candidate_ref}",
                cwd=candidate_tree,
            )
        except CaptureError as exc:
            raise ExperimentError(str(exc)) from exc
        verification = verify_contracts(contract, baseline_runpack, candidate_runpack)
        return ExperimentResult(
            baseline_runpack,
            candidate_runpack,
            baseline_exit,
            candidate_exit,
            verification,
        )
    finally:
        cleanup_error: ExperimentError | None = None
        for worktree in reversed(added):
            try:
                _git(repo, "worktree", "remove", "--force", str(worktree))
            except ExperimentError as exc:
                cleanup_error = exc
        shutil.rmtree(temporary_root, ignore_errors=True)
        if cleanup_error is not None:
            active_error = sys.exception()
            if active_error is None:
                raise cleanup_error
            active_error.add_note(f"Proofline worktree cleanup also failed: {cleanup_error}")


def default_output_directory() -> Path:
    return Path(f"proofline-results-{uuid.uuid4().hex[:8]}")
