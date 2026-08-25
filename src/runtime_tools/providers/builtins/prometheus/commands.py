"""CLI command supplied by the built-in Prometheus provider."""

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
    parser.add_argument("response", type=Path)
    parser.add_argument("--output", type=Path, required=True)


def _execute(arguments: argparse.Namespace) -> ProviderResult:
    from runtime_tools.providers.builtins.prometheus.enrichment import (
        import_prometheus_response,
    )
    from runtime_tools.providers.enrichment import EnrichmentError
    from runtime_tools.storage import RunpackError

    output: Path = arguments.output
    try:
        result = import_prometheus_response(arguments.runpack, arguments.response, output)
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


PROVIDER = ProviderSpec(
    key="prometheus",
    display_name="Prometheus",
    commands=(
        ProviderCommand(
            name="enrich-prometheus",
            help="add a bounded Prometheus HTTP API response",
            configure=_configure,
            execute=_execute,
        ),
    ),
)
