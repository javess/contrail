"""CLI commands supplied by the built-in OpenTelemetry provider."""

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


def _configure_trace(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("source", type=Path)
    parser.add_argument("--name", help="logical execution name")
    parser.add_argument("--output", type=Path, help="output .runpack path")
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="store the source OTLP JSON in the runpack",
    )


def _execute_trace(arguments: argparse.Namespace) -> ProviderResult:
    from runtime_tools.providers.builtins.otel.importer import (
        OtelImportError,
        import_otlp_json,
    )
    from runtime_tools.storage import RunpackError

    source: Path = arguments.source
    output: Path = arguments.output or source.with_suffix(".runpack")
    try:
        result = import_otlp_json(
            source,
            output,
            name=arguments.name or source.stem,
            include_raw=arguments.include_raw,
        )
    except (OtelImportError, RunpackError) as exc:
        raise ProviderExecutionError(str(exc)) from exc
    return ProviderResult(
        summary=(
            f"imported {result.event_count} spans and {result.edge_count} causal edges into "
            f"{terminal_text(output)}"
        ),
        artifacts=(output,),
    )


def _configure_logs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("runpack", type=Path)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="store the source OTLP logs JSON in the runpack",
    )


def _execute_logs(arguments: argparse.Namespace) -> ProviderResult:
    from runtime_tools.providers.builtins.otel.importer import (
        OtelImportError,
        import_otlp_logs,
    )
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    output: Path = arguments.output
    try:
        result = import_otlp_logs(
            arguments.runpack,
            arguments.source,
            output,
            include_raw=arguments.include_raw,
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


PROVIDER = ProviderSpec(
    key="otel",
    display_name="OpenTelemetry",
    commands=(
        ProviderCommand(
            name="import-otel",
            help="import an OTLP/JSON trace export",
            configure=_configure_trace,
            execute=_execute_trace,
        ),
        ProviderCommand(
            name="enrich-otel-logs",
            help="add bounded OTLP/JSON log records",
            configure=_configure_logs,
            execute=_execute_logs,
        ),
    ),
)
