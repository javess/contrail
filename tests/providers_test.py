from __future__ import annotations

import os
import subprocess
import sys

import pytest

from runtime_tools.providers import ProviderConfigurationError, resolve_provider_registry


def _contrail(
    *arguments: str, environment: dict[str, str] | None = None
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

    imports = (
        import_otlp_json,
        import_kubernetes_snapshot,
        import_prometheus_response,
        import_temporal_history,
    )
    assert all(item.__module__.startswith("runtime_tools.providers.builtins.") for item in imports)


def test_registry_contains_only_the_fixed_builtin_commands() -> None:
    registry = resolve_provider_registry()

    assert registry.command_names == {
        "import-otel",
        "enrich-otel-logs",
        "enrich-kubernetes",
        "enrich-prometheus",
        "enrich-temporal-history",
    }
    assert [(item.key, item.enabled, item.source) for item in registry.inventory] == [
        ("otel", True, "built-in"),
        ("kubernetes", True, "built-in"),
        ("prometheus", True, "built-in"),
        ("temporal", True, "built-in"),
    ]


def test_provider_inventory_is_stable_and_environment_independent() -> None:
    environment = os.environ.copy()
    environment["CONTRAIL_ENABLE_PROVIDERS"] = "third-party"
    environment["CONTRAIL_DISABLE_PROVIDERS"] = "otel"

    result = _contrail("providers", environment=environment)

    assert result.returncode == 0
    assert result.stderr == ""
    rows = {" ".join(line.split()) for line in result.stdout.splitlines()}
    assert "otel enabled built-in import-otel, enrich-otel-logs" in rows
    assert "third-party" not in result.stdout


def test_root_help_lists_bundled_integration_commands() -> None:
    result = _contrail("--help")

    assert result.returncode == 0
    assert "import-otel" in result.stdout
    assert "enrich-kubernetes" in result.stdout


def test_registry_rejects_a_command_collision_with_core() -> None:
    with pytest.raises(
        ProviderConfigurationError,
        match="provider command is registered more than once: import-otel",
    ):
        resolve_provider_registry(reserved_commands=("import-otel",))
