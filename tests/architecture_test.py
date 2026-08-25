from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).parents[1]
_CHECKER = _ROOT / "tools" / "check_architecture.py"


def _check(source_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, str(_CHECKER), "--source-root", str(source_root)),
        check=False,
        capture_output=True,
        text=True,
    )


def test_repository_modules_follow_the_declared_dependency_direction() -> None:
    result = _check(_ROOT / "src" / "runtime_tools")

    assert result.returncode == 0, result.stderr
    assert "satisfy the declared dependency direction" in result.stdout


def test_foundation_cannot_import_presentation(tmp_path: Path) -> None:
    package = tmp_path / "runtime_tools"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "model.py").write_text("import runtime_tools.cli\n", encoding="utf-8")
    (package / "cli.py").write_text("", encoding="utf-8")

    result = _check(package)

    assert result.returncode == 1
    assert (
        "runtime_tools.model (foundation) imports runtime_tools.cli (presentation)" in result.stderr
    )


def test_same_layer_import_cycle_is_rejected(tmp_path: Path) -> None:
    package = tmp_path / "runtime_tools"
    providers = package / "providers"
    package.mkdir()
    providers.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "capture.py").write_text("import runtime_tools.providers\n", encoding="utf-8")
    (providers / "__init__.py").write_text("import runtime_tools.capture\n", encoding="utf-8")

    result = _check(package)

    assert result.returncode == 1
    assert "internal import cycle:" in result.stderr
    assert "runtime_tools.capture" in result.stderr
    assert "runtime_tools.providers" in result.stderr


def test_relative_import_from_package_is_classified(tmp_path: Path) -> None:
    package = tmp_path / "runtime_tools"
    analysis = package / "batchscope"
    analysis.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "cli.py").write_text("", encoding="utf-8")
    (analysis / "__init__.py").write_text("from .. import cli\n", encoding="utf-8")

    result = _check(package)

    assert result.returncode == 1
    assert (
        "runtime_tools.batchscope (analysis) imports runtime_tools.cli (presentation)"
        in result.stderr
    )
