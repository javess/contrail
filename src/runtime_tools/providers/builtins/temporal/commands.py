"""CLI command supplied by the built-in Temporal provider."""

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
    parser.add_argument("history", type=Path)
    parser.add_argument("--output", type=Path, required=True)


def _execute(arguments: argparse.Namespace) -> ProviderResult:
    from runtime_tools.providers.builtins.temporal.enrichment import import_temporal_history
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    output: Path = arguments.output
    try:
        result = import_temporal_history(arguments.runpack, arguments.history, output)
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


PROVIDER = ProviderSpec(
    key="temporal",
    display_name="Temporal",
    commands=(
        ProviderCommand(
            name="enrich-temporal-history",
            help="add bounded Temporal workflow history",
            configure=_configure,
            execute=_execute,
        ),
    ),
)
