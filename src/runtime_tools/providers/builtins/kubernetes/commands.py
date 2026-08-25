"""CLI command supplied by the built-in Kubernetes provider."""

from __future__ import annotations

import argparse
from pathlib import Path

from runtime_tools.providers.contracts import (
    ProviderCommand,
    ProviderExecutionError,
    ProviderResult,
    ProviderSpec,
)
from runtime_tools.terminal import terminal_text


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("runpack", type=Path)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--output", type=Path, required=True)


def _execute(arguments: argparse.Namespace) -> ProviderResult:
    from runtime_tools.providers.builtins.kubernetes.enrichment import (
        import_kubernetes_snapshot,
    )
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    output: Path = arguments.output
    try:
        result = import_kubernetes_snapshot(arguments.runpack, arguments.snapshot, output)
    except (EnrichmentError, RunpackError) as exc:
        raise ProviderExecutionError(str(exc)) from exc
    return ProviderResult(
        summary=(
            f"added {result.entity_count} Kubernetes entities, "
            f"{result.event_count} events, and "
            f"{result.correlation_count} telemetry correlations to {terminal_text(output)}"
        ),
        artifacts=(output,),
    )


PROVIDER = ProviderSpec(
    key="kubernetes",
    display_name="Kubernetes",
    commands=(
        ProviderCommand(
            name="enrich-kubernetes",
            help="add a bounded Kubernetes API snapshot",
            configure=_configure,
            execute=_execute,
        ),
    ),
)
