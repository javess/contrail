"""Typed CLI commands for Contrail's bundled evidence providers."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer

from runtime_tools._cli_support import command_context, fail, finish
from runtime_tools.console import print_text
from runtime_tools.providers.builtins.kubernetes.commands import enrich_kubernetes
from runtime_tools.providers.builtins.otel.commands import enrich_logs, import_trace
from runtime_tools.providers.builtins.prometheus.commands import enrich_prometheus
from runtime_tools.providers.builtins.temporal.commands import enrich_temporal
from runtime_tools.providers.contracts import ProviderError, ProviderExecutionError, ProviderResult

app = typer.Typer(
    help="Import and enrich evidence from bundled integrations.",
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)


def _execute(
    context: typer.Context,
    name: str,
    action: Callable[[], ProviderResult],
) -> None:
    application = command_context(context)
    try:
        result = action()
    except ProviderError as exc:
        fail(exc, label=application.error_label)
        finish(2)
        return
    except Exception as exc:
        fail(
            ProviderExecutionError(f"provider command {name} failed: {type(exc).__name__}"),
            label=application.error_label,
        )
        finish(2)
        return
    if result.summary is not None:
        print_text(result.summary, stderr=True, style="green")
    finish(result.exit_status)


@app.command("import-otel", help="Import an OTLP/JSON trace export.")
def import_otel_command(
    context: typer.Context,
    source: Annotated[Path, typer.Argument()],
    name: Annotated[str | None, typer.Option("--name")] = None,
    output: Annotated[Path | None, typer.Option("--output")] = None,
    include_raw: Annotated[
        bool, typer.Option("--include-raw", help="Store the source JSON in the runpack")
    ] = False,
) -> None:
    _execute(
        context,
        "import-otel",
        lambda: import_trace(source, name=name, output=output, include_raw=include_raw),
    )


@app.command("enrich-otel-logs", help="Add bounded OTLP/JSON log records.")
def enrich_otel_logs_command(
    context: typer.Context,
    runpack: Annotated[Path, typer.Argument()],
    source: Annotated[Path, typer.Argument()],
    output: Annotated[Path, typer.Option("--output")],
    include_raw: Annotated[
        bool, typer.Option("--include-raw", help="Store the source JSON in the runpack")
    ] = False,
) -> None:
    _execute(
        context,
        "enrich-otel-logs",
        lambda: enrich_logs(runpack, source, output=output, include_raw=include_raw),
    )


@app.command("enrich-kubernetes", help="Add a bounded Kubernetes API snapshot.")
def enrich_kubernetes_command(
    context: typer.Context,
    runpack: Annotated[Path, typer.Argument()],
    snapshot: Annotated[Path, typer.Argument()],
    output: Annotated[Path, typer.Option("--output")],
) -> None:
    _execute(
        context,
        "enrich-kubernetes",
        lambda: enrich_kubernetes(runpack, snapshot, output=output),
    )


@app.command("enrich-prometheus", help="Add a bounded Prometheus HTTP API response.")
def enrich_prometheus_command(
    context: typer.Context,
    runpack: Annotated[Path, typer.Argument()],
    response: Annotated[Path, typer.Argument()],
    output: Annotated[Path, typer.Option("--output")],
) -> None:
    _execute(
        context,
        "enrich-prometheus",
        lambda: enrich_prometheus(runpack, response, output=output),
    )


@app.command("enrich-temporal-history", help="Add bounded Temporal workflow history.")
def enrich_temporal_command(
    context: typer.Context,
    runpack: Annotated[Path, typer.Argument()],
    history: Annotated[Path, typer.Argument()],
    output: Annotated[Path, typer.Option("--output")],
) -> None:
    _execute(
        context,
        "enrich-temporal-history",
        lambda: enrich_temporal(runpack, history, output=output),
    )
