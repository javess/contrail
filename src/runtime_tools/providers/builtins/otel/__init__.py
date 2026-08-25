"""OpenTelemetry trace and log evidence provider."""

from runtime_tools.providers.builtins.otel.importer import (
    OtelImportError,
    OtelImportResult,
    OtelLogImportResult,
    import_otlp_json,
    import_otlp_logs,
)

__all__ = [
    "OtelImportError",
    "OtelImportResult",
    "OtelLogImportResult",
    "import_otlp_json",
    "import_otlp_logs",
]
