"""CLI command supplied by the built-in Prometheus provider."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.providers.contracts import (
    ProviderCommand,
    ProviderExecutionError,
    ProviderResult,
)
from runtime_tools.terminal import terminal_text


def enrich_prometheus(runpack: Path, response: Path, *, output: Path) -> ProviderResult:
    from runtime_tools.providers.builtins.prometheus.enrichment import (
        import_prometheus_response,
    )
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    try:
        result = import_prometheus_response(runpack, response, output)
    except (EnrichmentError, RunpackError) as exc:
        raise ProviderExecutionError(str(exc)) from exc
    return ProviderResult(
        summary=(
            f"added {result.sample_count} Prometheus samples "
            f"({result.dropped_outside_window} outside the run window) "
            f"to {terminal_text(output)}"
        ),
        artifacts=(output,),
    )


COMMANDS = (
    ProviderCommand(
        name="enrich-prometheus",
        help="add a bounded Prometheus HTTP API response",
    ),
)
