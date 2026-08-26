"""Fixed registry for the evidence integrations bundled with Contrail."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from runtime_tools.providers.builtins.kubernetes.commands import (
    COMMANDS as KUBERNETES_COMMANDS,
)
from runtime_tools.providers.builtins.otel.commands import COMMANDS as OTEL_COMMANDS
from runtime_tools.providers.builtins.prometheus.commands import (
    COMMANDS as PROMETHEUS_COMMANDS,
)
from runtime_tools.providers.builtins.temporal.commands import COMMANDS as TEMPORAL_COMMANDS
from runtime_tools.providers.contracts import (
    ProviderCommand,
    ProviderConfigurationError,
    ProviderInventory,
)

_BUILTINS = (
    ("otel", "OpenTelemetry", OTEL_COMMANDS),
    ("kubernetes", "Kubernetes", KUBERNETES_COMMANDS),
    ("prometheus", "Prometheus", PROMETHEUS_COMMANDS),
    ("temporal", "Temporal", TEMPORAL_COMMANDS),
)


@dataclass(frozen=True, slots=True)
class ProviderRegistry:
    """The commands and display metadata bundled with this Contrail build."""

    commands: tuple[ProviderCommand, ...]
    inventory: tuple[ProviderInventory, ...]

    @property
    def command_names(self) -> frozenset[str]:
        return frozenset(command.name for command in self.commands)

    def command(self, name: str) -> ProviderCommand | None:
        return next((command for command in self.commands if command.name == name), None)


def resolve_provider_registry(*, reserved_commands: Iterable[str] = ()) -> ProviderRegistry:
    """Return the deterministic registry compiled into Contrail."""

    commands = tuple(command for _, _, group in _BUILTINS for command in group)
    names = tuple(command.name for command in commands)
    conflicts = set(reserved_commands).intersection(names)
    duplicates = {name for name in names if names.count(name) > 1}
    if conflicts or duplicates:
        name = min(conflicts | duplicates)
        raise ProviderConfigurationError(f"provider command is registered more than once: {name}")
    inventory = tuple(
        ProviderInventory(key, display_name, tuple(command.name for command in group))
        for key, display_name, group in _BUILTINS
    )
    return ProviderRegistry(commands, inventory)
