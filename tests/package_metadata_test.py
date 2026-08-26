from __future__ import annotations

import configparser
import email
import json
import os
import re
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import cast

import pytest
import yaml

import runtime_tools

_ROOT = Path(__file__).parents[1]
_EXPECTED_SCRIPTS = {
    "contrail": "runtime_tools.contrail_cli:main",
}
_EXPECTED_PROJECT_URLS = {
    "Homepage": "https://github.com/javess/contrail",
    "Repository": "https://github.com/javess/contrail",
    "Issues": "https://github.com/javess/contrail/issues",
    "Changelog": "https://github.com/javess/contrail/blob/main/CHANGELOG.md",
    "Security": "https://github.com/javess/contrail/security/policy",
}


def _project() -> dict[str, object]:
    document = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = document["project"]
    assert isinstance(project, dict)
    return cast(dict[str, object], project)


def _safe_archive_names(names: list[str]) -> None:
    for name in names:
        path = PurePosixPath(name)
        assert not path.is_absolute()
        assert ".." not in path.parts
        assert ".agent" not in path.parts
        assert ".hypothesis" not in path.parts
        assert "__pycache__" not in path.parts


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    assert all(isinstance(key, str) for key in value)
    return cast(dict[str, object], value)


def _sequence(value: object) -> list[object]:
    assert isinstance(value, list)
    return cast(list[object], value)


def _workflow() -> dict[str, object]:
    document: object = yaml.load(
        (_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    return _mapping(document)


def _job_steps(job: dict[str, object]) -> tuple[dict[str, object], ...]:
    return tuple(_mapping(step) for step in _sequence(job["steps"]))


def _named_step(job: dict[str, object], name: str) -> dict[str, object]:
    return next(step for step in _job_steps(job) if step.get("name") == name)


def test_distribution_version_and_entrypoints_have_one_release_value() -> None:
    project = _project()
    assert project["version"] == runtime_tools.__version__ == "0.10.0"
    assert project["scripts"] == _EXPECTED_SCRIPTS
    assert project["urls"] == _EXPECTED_PROJECT_URLS


def test_source_install_contains_typed_marker_and_builtin_commands() -> None:
    package = files("runtime_tools")
    assert package.joinpath("py.typed").is_file()
    assert package.joinpath("providers", "contracts.py").is_file()
    assert package.joinpath("providers", "registry.py").is_file()
    assert package.joinpath("providers", "builtins", "otel", "commands.py").is_file()


def test_release_smoke_tool_has_a_standalone_help_surface() -> None:
    completed = subprocess.run(
        (sys.executable, str(_ROOT / "tools" / "release_smoke.py"), "--help"),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert "--constraints" in completed.stdout
    assert "--offline" in completed.stdout
    assert "--find-links" in completed.stdout


def test_oss_contributor_surface_requires_reproduction_and_evidence_review() -> None:
    document: object = yaml.load(
        (_ROOT / ".github" / "ISSUE_TEMPLATE" / "bug_report.yml").read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    form = _mapping(document)
    fields = tuple(_mapping(field) for field in _sequence(form["body"]))
    identified = {str(field["id"]): field for field in fields if isinstance(field.get("id"), str)}
    assert len(identified) == len([field for field in fields if "id" in field])
    required_inputs = {
        "contrail-version",
        "python-version",
        "operating-system",
        "installation-method",
        "reproduction",
        "expected-outcome",
        "actual-outcome",
    }
    assert required_inputs <= identified.keys()
    assert all(
        _mapping(identified[field_id]["validations"])["required"] == "true"
        for field_id in required_inputs
    )
    evidence = str(_mapping(identified["evidence-artifacts"])["attributes"])
    assert all(
        artifact in evidence
        for artifact in (
            "baseline.runpack",
            "candidate.runpack",
            "contract.yaml",
            "proofline-report.json",
        )
    )
    for acknowledgement in ("public-report", "redaction-review"):
        options = _sequence(_mapping(identified[acknowledgement]["attributes"])["options"])
        assert options
        assert all(_mapping(option)["required"] == "true" for option in options)

    contributing = (_ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    assert "uv run contrail demo --output-dir" in contributing
    assert "--report proofline-report.json" in contributing
    assert "> proofline-report.json" not in contributing
    assert "exits 0" in contributing
    assert "exits 0 when" in contributing
    assert "1 when" in contributing
    assert "2 when" in contributing
    assert "benchmarks/release.py --profile pr" in contributing
    assert "[the contribution guide](CONTRIBUTING.md)" in (_ROOT / "README.md").read_text(
        encoding="utf-8"
    )


def test_release_check_accepts_only_the_project_tag_and_current_changelog(
    tmp_path: Path,
) -> None:
    output = tmp_path / "github-output"
    completed = subprocess.run(
        (
            sys.executable,
            str(_ROOT / "tools" / "release_check.py"),
            "--ref-type",
            "tag",
            "--ref-name",
            "v0.10.0",
            "--github-output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert json.loads(completed.stdout) == {
        "name": "contrail-runtime-tools",
        "tag": "v0.10.0",
        "version": "0.10.0",
    }
    assert output.read_text(encoding="utf-8") == (
        "name=contrail-runtime-tools\nversion=0.10.0\ntag=v0.10.0\n"
    )

    wrong_tag = subprocess.run(
        (
            sys.executable,
            str(_ROOT / "tools" / "release_check.py"),
            "--ref-type",
            "tag",
            "--ref-name",
            "v0.10.1",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert wrong_tag.returncode == 2
    assert "expected 'v0.10.0'" in wrong_tag.stderr

    stale_changelog = tmp_path / "CHANGELOG.md"
    stale_changelog.write_text("# Changelog\n\n## 0.8.0\n", encoding="utf-8")
    wrong_changelog = subprocess.run(
        (
            sys.executable,
            str(_ROOT / "tools" / "release_check.py"),
            "--ref-type",
            "branch",
            "--ref-name",
            "main",
            "--changelog",
            str(stale_changelog),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert wrong_changelog.returncode == 2
    assert "first changelog release is '0.8.0', expected '0.10.0'" in wrong_changelog.stderr


def test_ci_workflow_pins_the_release_and_proofline_gates() -> None:
    workflow = _workflow()
    assert set(_mapping(workflow["on"])) == {"pull_request", "push", "workflow_dispatch"}
    assert _mapping(workflow["concurrency"]) == {
        "group": "release-${{ github.ref }}",
        "cancel-in-progress": "false",
    }
    jobs = _mapping(workflow["jobs"])
    assert set(jobs) == {
        "quality",
        "release-artifacts",
        "publish-pypi",
        "publish-github",
        "proofline",
    }

    quality = _mapping(jobs["quality"])
    matrix = _mapping(_mapping(quality["strategy"])["matrix"])
    assert matrix == {
        "os": ["ubuntu-latest", "macos-latest"],
        "python": ["3.12", "3.14"],
    }
    assert any(step.get("run") == "uv run pytest" for step in _job_steps(quality))

    release = _mapping(jobs["release-artifacts"])
    assert release["needs"] == "quality"
    assert _mapping(release["outputs"]) == {
        "name": "${{ steps.release_identity.outputs.name }}",
        "version": "${{ steps.release_identity.outputs.version }}",
        "tag": "${{ steps.release_identity.outputs.tag }}",
    }
    identity = _named_step(release, "Validate release identity")
    assert identity["id"] == "release_identity"
    assert _mapping(identity["env"]) == {
        "RELEASE_REF_TYPE": "${{ github.ref_type }}",
        "RELEASE_REF_NAME": "${{ github.ref_name }}",
    }
    identity_command = identity["run"]
    assert isinstance(identity_command, str)
    assert "uv run python tools/release_check.py" in identity_command
    assert '--ref-type "$RELEASE_REF_TYPE"' in identity_command
    assert '--ref-name "$RELEASE_REF_NAME"' in identity_command
    assert '--github-output "$GITHUB_OUTPUT"' in identity_command
    assert "${{ github." not in identity_command
    build = _named_step(release, "Build twice from the commit timestamp")
    build_command = build["run"]
    assert isinstance(build_command, str)
    assert 'export SOURCE_DATE_EPOCH="$source_date_epoch"' in build_command
    assert "uv build --out-dir dist-a" in build_command
    assert "uv build --out-dir dist-b" in build_command
    reproducible = _named_step(release, "Verify reproducible artifact bytes")
    reproducible_command = reproducible["run"]
    assert isinstance(reproducible_command, str)
    assert "cmp dist-a/*.whl dist-b/*.whl" in reproducible_command
    assert "cmp dist-a/*.tar.gz dist-b/*.tar.gz" in reproducible_command
    archive_test = _named_step(release, "Validate release archive contents")
    archive_command = archive_test["run"]
    assert isinstance(archive_command, str)
    assert "CONTRAIL_RELEASE_WHEEL=" in archive_command
    assert "CONTRAIL_RELEASE_SDIST=" in archive_command
    smoke_command = _named_step(release, "Install and smoke-test the wheel")["run"]
    assert isinstance(smoke_command, str)
    assert "uv export --locked --no-dev --no-emit-project --no-hashes" in smoke_command
    assert "--output-file release-constraints.txt" in smoke_command
    assert "--constraints release-constraints.txt dist-a/*.whl" in smoke_command
    release_steps = _job_steps(release)
    release_upload = next(
        step
        for step in release_steps
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    )
    assert release_steps.index(release_upload) > release_steps.index(archive_test)
    assert release_steps.index(release_upload) > release_steps.index(
        _named_step(release, "Install and smoke-test the wheel")
    )
    assert "if" not in release_upload
    release_upload_settings = _mapping(release_upload["with"])
    assert release_upload_settings == {
        "name": "contrail-release-artifacts",
        "if-no-files-found": "error",
        "path": "dist-a/*\nSHA256SUMS\n",
    }

    publish_gate = "github.ref_type == 'tag' && vars.PYPI_PUBLISH_REPOSITORY == github.repository"
    publish_pypi = _mapping(jobs["publish-pypi"])
    assert publish_pypi["if"] == publish_gate
    assert publish_pypi["needs"] == "release-artifacts"
    assert publish_pypi["environment"] == "pypi"
    assert _mapping(publish_pypi["permissions"]) == {
        "contents": "read",
        "id-token": "write",
    }
    pypi_download, pypi_verify, pypi_publish = _job_steps(publish_pypi)
    assert pypi_download["uses"] == (
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    )
    assert _mapping(pypi_download["with"]) == {
        "name": "contrail-release-artifacts",
        "path": "release-artifacts",
    }
    assert pypi_verify["name"] == "Verify qualified distribution checksums"
    assert pypi_verify["working-directory"] == "release-artifacts"
    assert pypi_verify["run"] == "sha256sum --check SHA256SUMS"
    assert pypi_publish["uses"] == (
        "pypa/gh-action-pypi-publish@dc37677b2e1c63e2034f94d8a5b11f265b73ba33"
    )
    assert _mapping(pypi_publish["with"]) == {
        "packages-dir": "release-artifacts/dist-a",
        "attestations": "true",
    }
    assert "run" not in pypi_publish

    publish_github = _mapping(jobs["publish-github"])
    assert publish_github["if"] == publish_gate
    assert publish_github["needs"] == ["release-artifacts", "publish-pypi"]
    assert _mapping(publish_github["permissions"]) == {"contents": "write"}
    github_download, github_publish = _job_steps(publish_github)
    assert github_download["uses"] == (
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    )
    assert _mapping(github_download["with"]) == {
        "name": "contrail-release-artifacts",
        "path": "release-artifacts",
    }
    assert _mapping(github_publish["env"]) == {"GH_TOKEN": "${{ github.token }}"}
    github_command = github_publish["run"]
    assert isinstance(github_command, str)
    assert "sha256sum --check SHA256SUMS" in github_command
    assert (
        'gh release create "$GITHUB_REF_NAME" dist-a/*.whl dist-a/*.tar.gz SHA256SUMS '
        "--verify-tag --generate-notes"
    ) in github_command
    assert "uv build" not in github_command

    workflow_text = (_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "secrets." not in workflow_text
    assert "UV_PUBLISH_TOKEN" not in workflow_text
    assert "PYPI_API_TOKEN" not in workflow_text
    assert "skip-existing" not in workflow_text

    proofline = _mapping(jobs["proofline"])
    assert proofline["if"] == "github.event_name == 'pull_request'"
    gate = _named_step(proofline, "Evaluate the pull request runtime contract")
    assert gate["id"] == "proofline_gate"
    gate_command = gate["run"]
    assert isinstance(gate_command, str)
    assert "${{ github.event.pull_request.base.sha }}" in gate_command
    assert "${{ github.event.pull_request.head.sha }}" in gate_command
    assert "--explain" in gate_command
    assert "--report proofline-report.json" in gate_command
    assert "> proofline-report.json" not in gate_command
    assert "tee " not in gate_command
    assert "|| true" not in gate_command
    summary = _named_step(proofline, "Publish Proofline failure summary")
    assert summary["if"] == "failure() && steps.proofline_gate.conclusion == 'failure'"
    assert summary["shell"] == "bash"
    summary_command = summary["run"]
    assert isinstance(summary_command, str)
    assert "proofline-results/baseline.runpack" in summary_command
    assert "proofline-results/candidate.runpack" in summary_command
    assert "uv run contrail verify examples/local/contracts.yaml" in summary_command
    assert "--explain" in summary_command
    assert "|| true" in summary_command
    assert '>> "$GITHUB_STEP_SUMMARY"' in summary_command
    upload = _named_step(proofline, "Upload Proofline evidence")
    assert upload["if"] == "always()"
    upload_settings = _mapping(upload["with"])
    assert upload_settings["retention-days"] == "30"
    upload_paths = upload_settings["path"]
    assert isinstance(upload_paths, str)
    assert "proofline-report.json" in upload_paths
    assert "proofline-results/*.runpack" in upload_paths

    for job_value in jobs.values():
        for step in _job_steps(_mapping(job_value)):
            action = step.get("uses")
            if action is not None:
                assert isinstance(action, str)
                assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action)
            if isinstance(action, str) and action.startswith("astral-sh/setup-uv@"):
                assert _mapping(step["with"])["version"] == "0.12.3"


def test_release_archive_environment_is_all_or_nothing() -> None:
    assert bool(os.environ.get("CONTRAIL_RELEASE_WHEEL")) == bool(
        os.environ.get("CONTRAIL_RELEASE_SDIST")
    )


def test_opt_in_wheel_manifest_and_metadata() -> None:
    configured = os.environ.get("CONTRAIL_RELEASE_WHEEL")
    if not configured:
        pytest.skip("set CONTRAIL_RELEASE_WHEEL to validate a built wheel")
    wheel = Path(configured).resolve(strict=True)
    checkout = str(_ROOT.resolve()).encode()
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        _safe_archive_names(names)
        expected_package_files = {
            path.relative_to(_ROOT / "src").as_posix()
            for path in (_ROOT / "src" / "runtime_tools").rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        }
        packaged_files = {name for name in names if name.startswith("runtime_tools/")}
        assert packaged_files == expected_package_files
        assert any(name.endswith("runtime_tools/py.typed") for name in names)
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = email.message_from_bytes(archive.read(metadata_name))
        assert metadata["Version"] == runtime_tools.__version__
        assert set(metadata.get_all("Project-URL", [])) == {
            f"{label}, {url}" for label, url in _EXPECTED_PROJECT_URLS.items()
        }
        entrypoints_name = next(
            name for name in names if name.endswith(".dist-info/entry_points.txt")
        )
        entrypoints = configparser.ConfigParser()
        entrypoints.read_string(archive.read(entrypoints_name).decode("utf-8"))
        assert dict(entrypoints["console_scripts"]) == _EXPECTED_SCRIPTS
        for name in names:
            assert checkout not in archive.read(name)


def test_opt_in_sdist_has_no_workspace_or_cache_content() -> None:
    configured = os.environ.get("CONTRAIL_RELEASE_SDIST")
    if not configured:
        pytest.skip("set CONTRAIL_RELEASE_SDIST to validate a built source distribution")
    sdist = Path(configured).resolve(strict=True)
    checkout = str(_ROOT.resolve()).encode()
    with tarfile.open(sdist, "r:*") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        _safe_archive_names(names)
        roots = {PurePosixPath(name).parts[0] for name in names}
        assert len(roots) == 1
        root = next(iter(roots))
        required = {
            "README.md",
            "LICENSE",
            "CHANGELOG.md",
            "SECURITY.md",
            "CONTRIBUTING.md",
            "pyproject.toml",
            "uv.lock",
            ".github/workflows/ci.yml",
            ".github/ISSUE_TEMPLATE/bug_report.yml",
            ".github/pull_request_template.md",
            "docs/ci.md",
            "examples/sorting/README.md",
            "examples/sorting/after.py",
            "examples/sorting/before.py",
            "examples/sorting/contract.yaml",
            "examples/sorting/workload.py",
            "tests/package_metadata_test.py",
            "tools/release_check.py",
            "tools/release_smoke.py",
        }
        assert {f"{root}/{name}" for name in required}.issubset(names)
        for member in members:
            stream = archive.extractfile(member) if member.isfile() else None
            if stream is not None:
                assert checkout not in stream.read()
