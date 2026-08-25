"""Deterministic built-in catalog and opt-in third-party provider discovery."""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from runtime_tools.providers.builtins.catalog import (
    BUILTIN_PROVIDERS,
    BuiltinProviderDeclaration,
)
from runtime_tools.providers.contracts import (
    PROVIDER_DISABLE_ENV,
    PROVIDER_ENABLE_ENV,
    PROVIDER_ENTRY_POINT_GROUP,
    Provider,
    ProviderCommand,
    ProviderConfigurationError,
    ProviderError,
    ProviderExecutionError,
    ProviderInventory,
    ProviderLoadError,
    ProviderResult,
    ProviderSource,
    ProviderSpec,
    validate_provider_key,
)

type ProviderLoader = Callable[[], object]


@dataclass(frozen=True, slots=True)
class _ProviderCandidate:
    key: str
    display_name: str
    source: ProviderSource
    enabled_by_default: bool
    known_commands: tuple[str, ...]
    distribution: str | None
    load: ProviderLoader


@dataclass(frozen=True, slots=True)
class ProviderRegistry:
    """Resolved provider set and safe discovery inventory for one CLI process."""

    providers: tuple[ProviderSpec, ...]
    inventory: tuple[ProviderInventory, ...]

    @property
    def commands(self) -> tuple[ProviderCommand, ...]:
        return tuple(command for provider in self.providers for command in provider.commands)

    @property
    def command_names(self) -> frozenset[str]:
        return frozenset(command.name for command in self.commands)

    def command(self, name: str) -> ProviderCommand | None:
        return next((command for command in self.commands if command.name == name), None)

    def execute(self, name: str, arguments: argparse.Namespace) -> ProviderResult:
        """Execute one registered command behind the provider error boundary."""

        command = self.command(name)
        if command is None:
            raise ProviderConfigurationError(f"unknown provider command: {name}")
        try:
            result = command.execute(arguments)
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderExecutionError(
                f"provider command {name} failed: {type(exc).__name__}"
            ) from exc
        if not isinstance(result, ProviderResult):
            raise ProviderExecutionError(f"provider command {name} did not return ProviderResult")
        return result


def _selector(environment: Mapping[str, str], name: str) -> frozenset[str]:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        return frozenset[str]()
    parts = tuple(part.strip() for part in raw.split(","))
    if any(not part for part in parts):
        raise ProviderConfigurationError(f"{name} must be a comma-separated list of provider keys")
    return frozenset(validate_provider_key(part, label=name) for part in parts)


def _load_builtin(declaration: BuiltinProviderDeclaration) -> object:
    module = importlib.import_module(declaration.module)
    return getattr(module, "PROVIDER", None)


def _builtin_candidates(
    providers: tuple[ProviderSpec, ...] | None,
) -> tuple[_ProviderCandidate, ...]:
    if providers is not None:
        return tuple(
            _ProviderCandidate(
                provider.key,
                provider.display_name,
                "built-in",
                provider.enabled_by_default,
                tuple(command.name for command in provider.commands),
                None,
                lambda provider=provider: provider,
            )
            for provider in providers
        )
    return tuple(
        _ProviderCandidate(
            declaration.key,
            declaration.display_name,
            "built-in",
            True,
            declaration.commands,
            None,
            lambda declaration=declaration: _load_builtin(declaration),
        )
        for declaration in BUILTIN_PROVIDERS
    )


def _entry_point_candidates(
    entry_points: tuple[importlib.metadata.EntryPoint, ...] | None,
) -> tuple[_ProviderCandidate, ...]:
    discovered = (
        tuple(importlib.metadata.entry_points(group=PROVIDER_ENTRY_POINT_GROUP))
        if entry_points is None
        else entry_points
    )
    candidates: list[_ProviderCandidate] = []
    for entry_point in sorted(discovered, key=lambda item: (item.name, item.value)):
        key = validate_provider_key(entry_point.name, label="provider entry-point name")
        distribution = entry_point.dist.name if entry_point.dist is not None else None
        candidates.append(
            _ProviderCandidate(
                key,
                key,
                "entry-point",
                False,
                (),
                distribution,
                entry_point.load,
            )
        )
    return tuple(candidates)


def _validated_provider(value: object, *, expected_key: str) -> ProviderSpec:
    if not isinstance(value, Provider):
        raise ProviderConfigurationError(
            f"provider entry point must expose a Provider object: {expected_key}"
        )
    if value.key != expected_key:
        raise ProviderConfigurationError(
            f"provider entry-point name {expected_key} does not match provider key {value.key}"
        )
    if not isinstance(value.commands, tuple) or not all(
        isinstance(command, ProviderCommand) for command in value.commands
    ):
        raise ProviderConfigurationError(
            f"provider commands must be a tuple of ProviderCommand values: {expected_key}"
        )
    if not isinstance(value.display_name, str) or type(value.enabled_by_default) is not bool:
        raise ProviderConfigurationError(f"provider metadata is invalid: {expected_key}")
    return ProviderSpec(
        value.key,
        value.display_name,
        value.commands,
        value.enabled_by_default,
    )


def _load_candidate(candidate: _ProviderCandidate) -> ProviderSpec:
    try:
        value = candidate.load()
        return _validated_provider(value, expected_key=candidate.key)
    except ProviderConfigurationError:
        raise
    except Exception as exc:
        raise ProviderLoadError(
            f"could not load provider {candidate.key}: {type(exc).__name__}"
        ) from exc


def _unique_candidates(
    candidates: Iterable[_ProviderCandidate],
) -> tuple[_ProviderCandidate, ...]:
    resolved: list[_ProviderCandidate] = []
    keys: set[str] = set()
    for candidate in candidates:
        if candidate.key in keys:
            raise ProviderConfigurationError(
                f"provider key is registered more than once: {candidate.key}"
            )
        keys.add(candidate.key)
        resolved.append(candidate)
    return tuple(resolved)


def resolve_provider_registry(
    *,
    builtins: tuple[ProviderSpec, ...] | None = None,
    entry_points: tuple[importlib.metadata.EntryPoint, ...] | None = None,
    environment: Mapping[str, str] | None = None,
    reserved_commands: Iterable[str] = (),
) -> ProviderRegistry:
    """Discover, select, validate, and load providers for one process."""

    candidates = _unique_candidates(
        (*_builtin_candidates(builtins), *_entry_point_candidates(entry_points))
    )
    environment_values = os.environ if environment is None else environment
    explicitly_enabled = _selector(environment_values, PROVIDER_ENABLE_ENV)
    explicitly_disabled = _selector(environment_values, PROVIDER_DISABLE_ENV)
    conflict = sorted(explicitly_enabled & explicitly_disabled)
    if conflict:
        raise ProviderConfigurationError(
            f"provider cannot be both enabled and disabled: {conflict[0]}"
        )
    candidate_keys = frozenset(candidate.key for candidate in candidates)
    for variable, selected in (
        (PROVIDER_ENABLE_ENV, explicitly_enabled),
        (PROVIDER_DISABLE_ENV, explicitly_disabled),
    ):
        unknown = sorted(selected - candidate_keys)
        if unknown:
            raise ProviderConfigurationError(f"unknown provider in {variable}: {unknown[0]}")

    enabled_providers: list[ProviderSpec] = []
    inventory: list[ProviderInventory] = []
    command_owners: dict[str, str] = {name: "core" for name in reserved_commands}
    for candidate in candidates:
        enabled = (
            candidate.key in explicitly_enabled
            or candidate.enabled_by_default
            and candidate.key not in explicitly_disabled
        )
        if not enabled:
            inventory.append(
                ProviderInventory(
                    candidate.key,
                    candidate.display_name,
                    candidate.source,
                    False,
                    candidate.known_commands,
                    candidate.distribution,
                )
            )
            continue
        provider = _load_candidate(candidate)
        for command in provider.commands:
            owner = command_owners.get(command.name)
            if owner is not None:
                raise ProviderConfigurationError(
                    f"provider command is registered more than once: {command.name}"
                )
            command_owners[command.name] = provider.key
        enabled_providers.append(provider)
        inventory.append(
            ProviderInventory(
                provider.key,
                provider.display_name,
                candidate.source,
                True,
                tuple(command.name for command in provider.commands),
                candidate.distribution,
            )
        )
    return ProviderRegistry(tuple(enabled_providers), tuple(inventory))
