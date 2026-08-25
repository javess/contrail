"""Public, dependency-free contracts for evidence providers."""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

PROVIDER_ENTRY_POINT_GROUP = "contrail.providers"
PROVIDER_ENABLE_ENV = "CONTRAIL_ENABLE_PROVIDERS"
PROVIDER_DISABLE_ENV = "CONTRAIL_DISABLE_PROVIDERS"

_KEY_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$")
_COMMAND_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")

type ProviderSource = Literal["built-in", "entry-point"]
type ConfigureProviderCommand = Callable[[argparse.ArgumentParser], None]
type ExecuteProviderCommand = Callable[[argparse.Namespace], "ProviderResult"]


class ProviderError(ValueError):
    """Base class for safe provider configuration and execution failures."""


class ProviderConfigurationError(ProviderError):
    """Raised when provider discovery or selection is invalid."""


class ProviderLoadError(ProviderError):
    """Raised when an explicitly selected provider cannot be loaded."""


class ProviderExecutionError(ProviderError):
    """Raised when a provider command cannot safely complete."""


def validate_provider_key(value: str, *, label: str = "provider key") -> str:
    """Return a canonical provider key or raise a safe configuration error."""

    if not _KEY_PATTERN.fullmatch(value):
        raise ProviderConfigurationError(
            f"{label} must use 1-64 lowercase letters, digits, dots, hyphens, or underscores"
        )
    return value


def validate_command_name(value: str) -> str:
    """Return a canonical CLI command name or raise a safe configuration error."""

    if not _COMMAND_PATTERN.fullmatch(value):
        raise ProviderConfigurationError(
            "provider command must use 1-64 lowercase letters, digits, or hyphens"
        )
    return value


@dataclass(frozen=True, slots=True)
class ProviderResult:
    """Bounded presentation result returned by one provider command."""

    summary: str | None = None
    exit_status: int = 0
    artifacts: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        if self.summary is not None and (not self.summary or len(self.summary) > 4_096):
            raise ProviderConfigurationError(
                "provider result summary must contain 1-4096 characters"
            )
        if not 0 <= self.exit_status <= 255:
            raise ProviderConfigurationError(
                "provider result exit status must be between 0 and 255"
            )
        if len(self.artifacts) > 64:
            raise ProviderConfigurationError(
                "provider result cannot publish more than 64 artifacts"
            )


@dataclass(frozen=True, slots=True)
class ProviderCommand:
    """One provider-owned command registered with the shared CLI."""

    name: str
    help: str
    configure: ConfigureProviderCommand
    execute: ExecuteProviderCommand

    def __post_init__(self) -> None:
        validate_command_name(self.name)
        if not self.help or len(self.help) > 256:
            raise ProviderConfigurationError("provider command help must contain 1-256 characters")
        if not callable(self.configure) or not callable(self.execute):
            raise ProviderConfigurationError("provider command callbacks must be callable")


@runtime_checkable
class Provider(Protocol):
    """Structural contract implemented by built-in and third-party providers."""

    @property
    def key(self) -> str: ...

    @property
    def display_name(self) -> str: ...

    @property
    def commands(self) -> tuple[ProviderCommand, ...]: ...

    @property
    def enabled_by_default(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class ProviderSpec:
    """Default immutable implementation of the provider protocol."""

    key: str
    display_name: str
    commands: tuple[ProviderCommand, ...]
    enabled_by_default: bool = True

    def __post_init__(self) -> None:
        validate_provider_key(self.key)
        if not self.display_name or len(self.display_name) > 128:
            raise ProviderConfigurationError("provider display name must contain 1-128 characters")
        if not self.commands:
            raise ProviderConfigurationError("provider must expose at least one command")
        command_names = tuple(command.name for command in self.commands)
        if len(set(command_names)) != len(command_names):
            raise ProviderConfigurationError(
                f"provider command is registered more than once: {self.key}"
            )


@dataclass(frozen=True, slots=True)
class ProviderInventory:
    """Discovery metadata that can be rendered without loading disabled code."""

    key: str
    display_name: str
    source: ProviderSource
    enabled: bool
    commands: tuple[str, ...]
    distribution: str | None = None
