"""CLI command supplied by the built-in Kubernetes provider."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.providers.contracts import (
    ProviderCommand,
    ProviderExecutionError,
    ProviderResult,
)
from runtime_tools.terminal import terminal_text


def enrich_kubernetes(runpack: Path, snapshot: Path, *, output: Path) -> ProviderResult:
    from runtime_tools.providers.builtins.kubernetes.enrichment import (
        import_kubernetes_snapshot,
    )
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    try:
        result = import_kubernetes_snapshot(runpack, snapshot, output)
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


COMMANDS = (
    ProviderCommand(
        name="enrich-kubernetes",
        help="add a bounded Kubernetes API snapshot",
    ),
)
