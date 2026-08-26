"""CLI command supplied by the built-in Temporal provider."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.providers.contracts import (
    ProviderCommand,
    ProviderExecutionError,
    ProviderResult,
)
from runtime_tools.terminal import terminal_text


def enrich_temporal(runpack: Path, history: Path, *, output: Path) -> ProviderResult:
    from runtime_tools.providers.builtins.temporal.enrichment import import_temporal_history
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    try:
        result = import_temporal_history(runpack, history, output)
    except (EnrichmentError, RunpackError) as exc:
        raise ProviderExecutionError(str(exc)) from exc
    return ProviderResult(
        summary=(
            f"added {result.activity_count} Temporal activities, "
            f"{result.queue_wait_count} queue waits, and "
            f"{result.correlation_count} OTLP correlations to {terminal_text(output)}"
        ),
        artifacts=(output,),
    )


COMMANDS = (
    ProviderCommand(
        name="enrich-temporal-history",
        help="add bounded Temporal workflow history",
    ),
)
