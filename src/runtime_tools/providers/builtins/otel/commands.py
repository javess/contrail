"""CLI commands supplied by the built-in OpenTelemetry provider."""

from __future__ import annotations

from pathlib import Path

from runtime_tools.providers.contracts import (
    ProviderCommand,
    ProviderExecutionError,
    ProviderResult,
)
from runtime_tools.terminal import terminal_text


def import_trace(
    source: Path,
    *,
    name: str | None = None,
    output: Path | None = None,
    include_raw: bool = False,
) -> ProviderResult:
    from runtime_tools.providers.builtins.otel.importer import (
        OtelImportError,
        import_otlp_json,
    )
    from runtime_tools.storage import RunpackError

    destination = output or source.with_suffix(".runpack")
    try:
        result = import_otlp_json(
            source,
            destination,
            name=name or source.stem,
            include_raw=include_raw,
        )
    except (OtelImportError, RunpackError) as exc:
        raise ProviderExecutionError(str(exc)) from exc
    return ProviderResult(
        summary=(
            f"imported {result.event_count} spans and {result.edge_count} causal edges into "
            f"{terminal_text(destination)}"
        ),
        artifacts=(destination,),
    )


def enrich_logs(
    runpack: Path,
    source: Path,
    *,
    output: Path,
    include_raw: bool = False,
) -> ProviderResult:
    from runtime_tools.providers.builtins.otel.importer import (
        OtelImportError,
        import_otlp_logs,
    )
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    try:
        result = import_otlp_logs(
            runpack,
            source,
            output,
            include_raw=include_raw,
        )
    except (EnrichmentError, OtelImportError, RunpackError) as exc:
        raise ProviderExecutionError(str(exc)) from exc
    return ProviderResult(
        summary=(
            f"added {result.event_count} OTLP log records and "
            f"{result.edge_count} span correlations "
            f"({result.dropped_outside_window} outside the run window) "
            f"with {result.dropped_attribute_count} exporter-dropped attributes "
            f"to {terminal_text(output)}"
        ),
        artifacts=(output,),
    )


COMMANDS = (
    ProviderCommand(
        name="import-otel",
        help="import an OTLP/JSON trace export",
    ),
    ProviderCommand(
        name="enrich-otel-logs",
        help="add bounded OTLP/JSON log records",
    ),
)
