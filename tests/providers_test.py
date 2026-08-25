from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from runtime_tools.providers import (
    ProviderCommand,
    ProviderConfigurationError,
    ProviderResult,
    ProviderSpec,
    resolve_provider_registry,
)


def _plugin_environment(root: Path) -> dict[str, str]:
    module = root / "fixture_provider.py"
    imported = root / "provider-imported"
    module.write_text(
        "\n".join(
            (
                "from pathlib import Path",
                "from runtime_tools.providers import ProviderCommand, ProviderResult, ProviderSpec",
                f"Path({str(imported)!r}).write_text('loaded', encoding='utf-8')",
                "def configure(parser):",
                "    parser.add_argument('--output', type=Path, required=True)",
                "def execute(arguments):",
                "    arguments.output.write_text('fixture provider', encoding='utf-8')",
                "    return ProviderResult(summary=f'fixture wrote {arguments.output}')",
                "provider = ProviderSpec(",
                "    key='fixture',",
                "    display_name='Fixture provider',",
                "    commands=(ProviderCommand(",
                "        name='fixture-import',",
                "        help='import fixture evidence',",
                "        configure=configure,",
                "        execute=execute,",
                "    ),),",
                ")",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    metadata = root / "contrail_fixture-1.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: contrail-fixture\nVersion: 1.0\n",
        encoding="utf-8",
    )
    (metadata / "entry_points.txt").write_text(
        "[contrail.providers]\nfixture = fixture_provider:provider\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.pop("CONTRAIL_ENABLE_PROVIDERS", None)
    environment.pop("CONTRAIL_DISABLE_PROVIDERS", None)
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(root), environment.get("PYTHONPATH", ""))
    ).rstrip(os.pathsep)
    return environment


def _contrail(
    *arguments: str,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, "-m", "runtime_tools.contrail_cli", *arguments),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )


def test_built_in_providers_have_one_canonical_module_path() -> None:
    from runtime_tools.providers.builtins.kubernetes import import_kubernetes_snapshot
    from runtime_tools.providers.builtins.otel import import_otlp_json
    from runtime_tools.providers.builtins.prometheus import import_prometheus_response
    from runtime_tools.providers.builtins.temporal import import_temporal_history

    assert import_otlp_json.__module__.startswith("runtime_tools.providers.builtins.otel.")
    assert import_kubernetes_snapshot.__module__.startswith(
        "runtime_tools.providers.builtins.kubernetes."
    )
    assert import_prometheus_response.__module__.startswith(
        "runtime_tools.providers.builtins.prometheus."
    )
    assert import_temporal_history.__module__.startswith(
        "runtime_tools.providers.builtins.temporal."
    )


def test_installed_external_provider_is_not_loaded_until_explicitly_enabled(
    tmp_path: Path,
) -> None:
    environment = _plugin_environment(tmp_path)

    result = _contrail("fixture-import", environment=environment)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == ("contrail: unknown command: fixture-import\nTry 'contrail --help'.\n")
    assert not (tmp_path / "provider-imported").exists()


def test_enabled_external_provider_adds_a_command_without_core_changes(tmp_path: Path) -> None:
    environment = _plugin_environment(tmp_path)
    environment["CONTRAIL_ENABLE_PROVIDERS"] = "fixture"
    output = tmp_path / "provider-output.txt"

    result = _contrail(
        "fixture-import",
        "--output",
        str(output),
        environment=environment,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == f"fixture wrote {output}\n"
    assert output.read_text(encoding="utf-8") == "fixture provider"
    assert (tmp_path / "provider-imported").read_text(encoding="utf-8") == "loaded"


def test_root_help_is_composed_from_the_enabled_provider_registry(tmp_path: Path) -> None:
    environment = _plugin_environment(tmp_path)
    environment["CONTRAIL_ENABLE_PROVIDERS"] = "fixture"
    enabled = _contrail("--help", environment=environment)

    environment["CONTRAIL_DISABLE_PROVIDERS"] = "otel"
    disabled = _contrail("--help", environment=environment)

    assert enabled.returncode == 0
    assert "fixture-import" in enabled.stdout
    assert "import fixture evidence" in enabled.stdout
    assert disabled.returncode == 0
    assert "fixture-import" in disabled.stdout
    assert "import-otel" not in disabled.stdout


def test_external_provider_failure_is_bounded_and_omits_its_exception_message(
    tmp_path: Path,
) -> None:
    environment = _plugin_environment(tmp_path)
    environment["CONTRAIL_ENABLE_PROVIDERS"] = "fixture"
    module = tmp_path / "fixture_provider.py"
    source = module.read_text(encoding="utf-8")
    module.write_text(
        source.replace(
            "return ProviderResult(summary=f'fixture wrote {arguments.output}')",
            "raise RuntimeError('private-provider-error-sentinel')",
        ),
        encoding="utf-8",
    )

    result = _contrail(
        "fixture-import",
        "--output",
        str(tmp_path / "unused.txt"),
        environment=environment,
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "contrail: provider command fixture-import failed: RuntimeError\n"
    assert "private-provider-error-sentinel" not in result.stderr
    assert "Traceback" not in result.stderr


def test_provider_inventory_shows_builtins_and_unloaded_entry_points(tmp_path: Path) -> None:
    environment = _plugin_environment(tmp_path)

    result = _contrail("providers", environment=environment)

    assert result.returncode == 0
    assert result.stderr == ""
    assert "otel" in result.stdout
    assert "import-otel, enrich-otel-logs" in result.stdout
    assert "fixture" in result.stdout
    assert "disabled" in result.stdout
    assert "entry-point" in result.stdout
    assert not (tmp_path / "provider-imported").exists()


def test_builtin_provider_can_be_disabled_without_changing_other_commands() -> None:
    environment = os.environ.copy()
    environment["CONTRAIL_DISABLE_PROVIDERS"] = "otel"

    disabled = _contrail("import-otel", "--help", environment=environment)
    inspect = _contrail("inspect", "--help", environment=environment)

    assert disabled.returncode == 2
    assert disabled.stdout == ""
    assert disabled.stderr == ("contrail: unknown command: import-otel\nTry 'contrail --help'.\n")
    assert inspect.returncode == 0
    assert inspect.stdout.startswith("usage: contrail inspect ")


def test_provider_selection_rejects_unknown_and_conflicting_keys_without_traceback() -> None:
    unknown_environment = os.environ.copy()
    unknown_environment["CONTRAIL_ENABLE_PROVIDERS"] = "missing"
    conflict_environment = os.environ.copy()
    conflict_environment["CONTRAIL_ENABLE_PROVIDERS"] = "otel"
    conflict_environment["CONTRAIL_DISABLE_PROVIDERS"] = "otel"

    unknown = _contrail("providers", environment=unknown_environment)
    conflict = _contrail("providers", environment=conflict_environment)

    assert unknown.returncode == 2
    assert unknown.stdout == ""
    assert unknown.stderr == "contrail: unknown provider in CONTRAIL_ENABLE_PROVIDERS: missing\n"
    assert conflict.returncode == 2
    assert conflict.stdout == ""
    assert conflict.stderr == "contrail: provider cannot be both enabled and disabled: otel\n"
    assert "Traceback" not in unknown.stderr + conflict.stderr


def test_provider_registry_rejects_duplicate_command_names() -> None:
    def configure(_parser: object) -> None:
        return None

    def execute(_arguments: object) -> ProviderResult:
        return ProviderResult()

    command = ProviderCommand("duplicate", "duplicate command", configure, execute)
    first = ProviderSpec("first", "First", (command,))
    second = ProviderSpec("second", "Second", (command,))

    with pytest.raises(
        ProviderConfigurationError,
        match="provider command is registered more than once: duplicate",
    ):
        resolve_provider_registry(builtins=(first, second), entry_points=())
