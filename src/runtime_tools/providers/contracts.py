"""Small command boundary shared by Contrail's built-in evidence integrations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class ProviderError(ValueError):
    """Base class for safe provider failures."""


class ProviderConfigurationError(ProviderError):
    """Raised when built-in command registration is inconsistent."""


class ProviderExecutionError(ProviderError):
    """Raised when a built-in provider command cannot complete safely."""


@dataclass(frozen=True, slots=True)
class ProviderResult:
    """Presentation result returned by one built-in command."""

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
    """Display metadata for one command bundled into the shared CLI."""

    name: str
    help: str

    def __post_init__(self) -> None:
        if (
            not self.name
            or not self.name.isascii()
            or not all(
                character.islower() or character.isdigit() or character == "-"
                for character in self.name
            )
        ):
            raise ProviderConfigurationError("provider command name is invalid")
        if not self.help or len(self.help) > 256:
            raise ProviderConfigurationError("provider command help must contain 1-256 characters")


@dataclass(frozen=True, slots=True)
class ProviderInventory:
    """Display metadata for one bundled integration."""

    key: str
    display_name: str
    commands: tuple[str, ...]
    source: str = "built-in"
    enabled: bool = True
    distribution: str | None = None
