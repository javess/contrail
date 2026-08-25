"""Small external Contrail provider used as a contributor template."""

from __future__ import annotations

import argparse

from runtime_tools.providers import ProviderCommand, ProviderResult, ProviderSpec


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("name")


def _execute(arguments: argparse.Namespace) -> ProviderResult:
    return ProviderResult(summary=f"hello from a provider, {arguments.name}")


PROVIDER = ProviderSpec(
    key="hello",
    display_name="Hello example",
    commands=(
        ProviderCommand(
            name="hello-provider",
            help="run the external provider example",
            configure=_configure,
            execute=_execute,
        ),
    ),
)
