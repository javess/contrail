"""Typed provider extension surface for Contrail evidence integrations."""

from runtime_tools.providers.contracts import (
    Provider,
    ProviderCommand,
    ProviderConfigurationError,
    ProviderError,
    ProviderExecutionError,
    ProviderInventory,
    ProviderLoadError,
    ProviderResult,
    ProviderSpec,
)
from runtime_tools.providers.registry import ProviderRegistry, resolve_provider_registry

__all__ = [
    "Provider",
    "ProviderCommand",
    "ProviderConfigurationError",
    "ProviderError",
    "ProviderExecutionError",
    "ProviderInventory",
    "ProviderLoadError",
    "ProviderRegistry",
    "ProviderResult",
    "ProviderSpec",
    "resolve_provider_registry",
]
