"""Lazy catalog of providers shipped in the Contrail distribution."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BuiltinProviderDeclaration:
    key: str
    display_name: str
    module: str
    commands: tuple[str, ...]


BUILTIN_PROVIDERS = (
    BuiltinProviderDeclaration(
        "otel",
        "OpenTelemetry",
        "runtime_tools.providers.builtins.otel.commands",
        ("import-otel", "enrich-otel-logs"),
    ),
    BuiltinProviderDeclaration(
        "kubernetes",
        "Kubernetes",
        "runtime_tools.providers.builtins.kubernetes.commands",
        ("enrich-kubernetes",),
    ),
    BuiltinProviderDeclaration(
        "prometheus",
        "Prometheus",
        "runtime_tools.providers.builtins.prometheus.commands",
        ("enrich-prometheus",),
    ),
    BuiltinProviderDeclaration(
        "temporal",
        "Temporal",
        "runtime_tools.providers.builtins.temporal.commands",
        ("enrich-temporal-history",),
    ),
)
