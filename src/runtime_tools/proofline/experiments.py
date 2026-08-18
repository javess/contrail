"""Execute comparable workloads in isolated Git worktrees."""

from __future__ import annotations

import fcntl
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.artifacts import artifact_exists
from runtime_tools.capture import CaptureError, record_process
from runtime_tools.json_support import output_document
from runtime_tools.model import JsonValue
from runtime_tools.proofline.contracts import Contract, load_contracts
from runtime_tools.proofline.verify import (
    VerificationArtifactBindings,
    VerificationReport,
    _verify_contracts,
    verify_loaded_contracts_with_artifact_bindings,
)
from runtime_tools.rundiff.compare import ExecutionDiff


class ExperimentError(ValueError):
    """Raised when an isolated comparison cannot be executed safely."""


GIT_COMMAND_TIMEOUT_SECONDS = 120
MAX_GIT_REF_BYTES = 4 * 1024
MAX_WORKLOAD_ARGUMENTS = 1_024
MAX_WORKLOAD_INVOCATION_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    baseline_runpack: Path
    candidate_runpack: Path
    baseline_exit_code: int
    candidate_exit_code: int
    verification: VerificationReport
    diff: ExecutionDiff | None = None
    artifact_bindings: VerificationArtifactBindings | None = None

    def as_json_value(self, *, include_evidence: bool = False) -> dict[str, JsonValue]:
        document = output_document(
            "proofline.experiment",
            {
                "baseline_runpack": str(self.baseline_runpack),
                "candidate_runpack": str(self.candidate_runpack),
                "baseline_exit_code": self.baseline_exit_code,
                "candidate_exit_code": self.candidate_exit_code,
                "verification": self.verification.as_json_value(include_evidence=include_evidence),
            },
        )
        if include_evidence and self.artifact_bindings is not None:
            document["artifact_bindings"] = self.artifact_bindings.as_json_value()
        return document


def _git(repo: Path, *args: str, capture: bool = False) -> str:
    try:
        result = subprocess.run(
            ("git", *args),
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="strict",
            timeout=GIT_COMMAND_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise ExperimentError("git is required for Proofline execution") from exc
    except OSError as exc:
        raise ExperimentError(f"could not execute Git: {exc}") from exc
    except UnicodeError as exc:
        raise ExperimentError("Git output must be valid UTF-8") from exc
    except subprocess.CalledProcessError as exc:
        message = exc.stderr.strip() or f"git {' '.join(args)} failed"
        raise ExperimentError(message) from exc
    except subprocess.TimeoutExpired as exc:
        raise ExperimentError(
            f"Git command timed out after {GIT_COMMAND_TIMEOUT_SECONDS} seconds"
        ) from exc
    return result.stdout.removesuffix("\n").removesuffix("\r") if capture else ""


def _repo_root(cwd: Path) -> Path:
    value = _git(cwd, "rev-parse", "--show-toplevel", capture=True)
    if not value:
        raise ExperimentError("Git repository root is empty")
    root = Path(value)
    if not root.is_dir():
        raise ExperimentError("Git repository root does not exist")
    return root


def _validate_ref(ref: str) -> None:
    if not isinstance(ref, str) or not ref or ref.startswith("-"):
        raise ExperimentError("Git refs must be non-empty and cannot start with '-'")
    if "\0" in ref:
        raise ExperimentError("Git refs cannot contain NUL bytes")
    try:
        encoded = ref.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ExperimentError("Git refs must be valid UTF-8") from exc
    if len(encoded) > MAX_GIT_REF_BYTES:
        raise ExperimentError(f"Git refs cannot exceed {MAX_GIT_REF_BYTES} UTF-8 bytes")


def _validate_workload_invocation(workload: Path, workload_args: tuple[str, ...]) -> None:
    if not isinstance(workload, Path):
        raise ExperimentError("workload must be a repository-relative path")
    workload_text = str(workload)
    if workload.is_absolute() or ".." in workload.parts:
        raise ExperimentError("workload must be a repository-relative path")
    if "\0" in workload_text:
        raise ExperimentError("workload path cannot contain NUL bytes")
    try:
        workload_text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ExperimentError("workload path must be valid UTF-8") from exc
    if not isinstance(workload_args, tuple) or not all(
        isinstance(argument, str) for argument in workload_args
    ):
        raise ExperimentError("workload arguments must be a tuple of strings")
    if len(workload_args) > MAX_WORKLOAD_ARGUMENTS:
        raise ExperimentError(
            f"workload arguments cannot contain more than {MAX_WORKLOAD_ARGUMENTS} entries"
        )
    if any("\0" in argument for argument in workload_args):
        raise ExperimentError("workload arguments cannot contain NUL bytes")
    try:
        encoded_arguments = [argument.encode("utf-8") for argument in workload_args]
    except UnicodeEncodeError as exc:
        raise ExperimentError("workload arguments must be valid UTF-8") from exc
    invocation_bytes = len(workload_text.encode("utf-8")) + sum(
        len(argument) for argument in encoded_arguments
    )
    if invocation_bytes > MAX_WORKLOAD_INVOCATION_BYTES:
        raise ExperimentError(
            f"workload path and arguments cannot exceed {MAX_WORKLOAD_INVOCATION_BYTES} UTF-8 bytes"
        )


def _validate_python_executable(python_executable: Path | None) -> Path:
    selected = Path(sys.executable) if python_executable is None else python_executable
    if not isinstance(selected, Path):
        raise ExperimentError("workload Python must be a path")
    raw = str(selected)
    if "\0" in raw:
        raise ExperimentError("workload Python path cannot contain NUL bytes")
    try:
        os.fsencode(raw)
    except UnicodeEncodeError as exc:
        raise ExperimentError("workload Python path is not representable") from exc
    try:
        absolute = Path(os.path.abspath(raw))
    except (OSError, RuntimeError) as exc:
        raise ExperimentError("could not make workload Python path absolute") from exc
    try:
        metadata = absolute.stat()
    except FileNotFoundError as exc:
        raise ExperimentError(f"workload Python does not exist: {absolute}") from exc
    except (OSError, RuntimeError) as exc:
        raise ExperimentError(f"could not inspect workload Python: {absolute}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ExperimentError(f"workload Python must be a regular file: {absolute}")
    if not os.access(absolute, os.X_OK):
        raise ExperimentError(f"workload Python is not executable: {absolute}")
    return absolute


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


def _require_clean_worktree(worktree: Path, arm: str) -> None:
    status = _git(
        worktree,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        capture=True,
    )
    if status:
        raise ExperimentError(f"{arm} workload modified the isolated Git worktree")


def _create_output_directory(output_dir: Path) -> None:
    try:
        output_dir.mkdir()
    except FileExistsError as exc:
        raise ExperimentError(f"refusing to reuse output directory: {output_dir}") from exc
    except OSError as exc:
        raise ExperimentError(f"could not create output directory: {output_dir}") from exc


def _resolve_path(path: Path, label: str) -> Path:
    if not isinstance(path, Path):
        raise ExperimentError(f"{label} must be a path")
    try:
        return path.resolve()
    except (OSError, RuntimeError) as exc:
        raise ExperimentError(f"could not resolve {label}: {path}") from exc


def _temporary_worktree_root() -> Path:
    try:
        return Path(tempfile.mkdtemp(prefix="proofline-worktrees-"))
    except OSError as exc:
        raise ExperimentError(f"could not create temporary worktree directory: {exc}") from exc


def _reserve_annotation_fd() -> int:
    if os.name != "posix":
        raise ExperimentError("Proofline annotation capture requires POSIX file descriptors")
    descriptor: int | None = None
    try:
        descriptor = os.open(os.devnull, os.O_RDONLY)
        if descriptor < 3:
            original = descriptor
            replacement = fcntl.fcntl(original, fcntl.F_DUPFD_CLOEXEC, 3)
            descriptor = replacement
            os.close(original)
        return descriptor
    except OSError as exc:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise ExperimentError(f"could not reserve annotation transport fd: {exc}") from exc


def _run_experiment(
    contracts: tuple[Contract, ...],
    *,
    baseline_ref: str,
    candidate_ref: str,
    workload: Path,
    workload_args: tuple[str, ...],
    output_dir: Path,
    cwd: Path | None = None,
    python_executable: Path | None = None,
    bind_artifacts: bool = False,
) -> ExperimentResult:
    python_executable = _validate_python_executable(python_executable)
    _validate_ref(baseline_ref)
    _validate_ref(candidate_ref)
    _validate_workload_invocation(workload, workload_args)
    repo = _repo_root(_resolve_path(cwd or Path.cwd(), "working directory"))
    if artifact_exists(output_dir):
        raise ExperimentError(f"refusing to reuse output directory: {output_dir}")
    if not output_dir.parent.is_dir():
        raise ExperimentError(f"output parent directory does not exist: {output_dir.parent}")
    baseline_commit = _resolve_commit(repo, baseline_ref)
    candidate_commit = _resolve_commit(repo, candidate_ref)
    workload_argument = f"./{workload}"

    baseline_runpack = output_dir / "baseline.runpack"
    candidate_runpack = output_dir / "candidate.runpack"
    temporary_roots: list[Path] = []
    added: list[Path] = []
    annotation_fd: int | None = None

    def allocate_worktree() -> tuple[Path, Path]:
        root = _temporary_worktree_root()
        temporary_roots.append(root)
        return root, root / "worktree"

    def add_worktree(worktree: Path, commit: str) -> None:
        _git(repo, "worktree", "add", "--detach", str(worktree), commit)
        added.append(worktree)
        _workload_path(worktree, workload)

    def remove_worktree(worktree: Path) -> None:
        _git(repo, "worktree", "remove", "--force", str(worktree))
        added.remove(worktree)

    try:
        annotation_fd = _reserve_annotation_fd()
        annotation_root = _temporary_worktree_root()
        temporary_roots.append(annotation_root)
        annotation_directory = annotation_root / "annotations"
        try:
            annotation_directory.mkdir(mode=0o700)
        except OSError as exc:
            raise ExperimentError(f"could not create private annotation directory: {exc}") from exc
        preflight_root, preflight_worktree = allocate_worktree()
        for commit in (baseline_commit, candidate_commit):
            add_worktree(preflight_worktree, commit)
            remove_worktree(preflight_worktree)
        shutil.rmtree(preflight_root, ignore_errors=True)
        _create_output_directory(output_dir)
        try:
            _, baseline_worktree = allocate_worktree()
            add_worktree(baseline_worktree, baseline_commit)
            baseline_exit = record_process(
                (str(python_executable), workload_argument, *workload_args),
                baseline_runpack,
                name=f"baseline:{baseline_ref}",
                cwd=baseline_worktree,
                _annotation_fd=annotation_fd,
                _annotation_directory=annotation_directory,
            )
            _require_clean_worktree(baseline_worktree, "baseline")
            remove_worktree(baseline_worktree)
            _, candidate_worktree = allocate_worktree()
            add_worktree(candidate_worktree, candidate_commit)
            candidate_exit = record_process(
                (str(python_executable), workload_argument, *workload_args),
                candidate_runpack,
                name=f"candidate:{candidate_ref}",
                cwd=candidate_worktree,
                _annotation_fd=annotation_fd,
                _annotation_directory=annotation_directory,
            )
            _require_clean_worktree(candidate_worktree, "candidate")
            remove_worktree(candidate_worktree)
        except CaptureError as exc:
            raise ExperimentError(str(exc)) from exc
        artifact_bindings: VerificationArtifactBindings | None = None
        if bind_artifacts:
            verification, diff, artifact_bindings = verify_loaded_contracts_with_artifact_bindings(
                contracts,
                baseline_runpack,
                candidate_runpack,
            )
        else:
            verification, diff = _verify_contracts(contracts, baseline_runpack, candidate_runpack)
        return ExperimentResult(
            baseline_runpack,
            candidate_runpack,
            baseline_exit,
            candidate_exit,
            verification,
            diff,
            artifact_bindings,
        )
    finally:
        cleanup_error: ExperimentError | None = None
        for worktree in reversed(added):
            try:
                _git(repo, "worktree", "remove", "--force", str(worktree))
            except ExperimentError as exc:
                cleanup_error = exc
        if annotation_fd is not None:
            try:
                os.close(annotation_fd)
            except OSError as exc:
                descriptor_error = ExperimentError(
                    f"could not release annotation transport fd: {exc}"
                )
                if cleanup_error is None:
                    cleanup_error = descriptor_error
                else:
                    cleanup_error.add_note(str(descriptor_error))
        for temporary_root in temporary_roots:
            shutil.rmtree(temporary_root, ignore_errors=True)
        if cleanup_error is not None:
            active_error = sys.exception()
            if active_error is None:
                raise cleanup_error
            active_error.add_note(f"Proofline worktree cleanup also failed: {cleanup_error}")


def run_experiment(
    contract: Path,
    *,
    baseline_ref: str,
    candidate_ref: str,
    workload: Path,
    workload_args: tuple[str, ...],
    output_dir: Path,
    cwd: Path | None = None,
    python_executable: Path | None = None,
    bind_artifacts: bool = False,
) -> ExperimentResult:
    if artifact_exists(output_dir):
        raise ExperimentError(f"refusing to reuse output directory: {output_dir}")
    python_executable = _validate_python_executable(python_executable)
    _validate_ref(baseline_ref)
    _validate_ref(candidate_ref)
    _validate_workload_invocation(workload, workload_args)
    contract = _resolve_path(contract, "contract path")
    contracts = load_contracts(contract)
    return _run_experiment(
        contracts,
        baseline_ref=baseline_ref,
        candidate_ref=candidate_ref,
        workload=workload,
        workload_args=workload_args,
        output_dir=output_dir,
        cwd=cwd,
        python_executable=python_executable,
        bind_artifacts=bind_artifacts,
    )


def default_output_directory() -> Path:
    return Path(f"proofline-results-{uuid.uuid4().hex[:8]}")
