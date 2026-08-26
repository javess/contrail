"""Normalize bounded OTLP/JSON trace and log evidence."""

from runtime_tools.providers.builtins.otel.importer._common import (
    MAX_OTLP_ATTRIBUTE_DEPTH,
    MAX_OTLP_DOCUMENT_BYTES,
    MAX_OTLP_LINKS,
    MAX_OTLP_LOG_RECORDS,
    MAX_OTLP_SPANS,
    OtelImportError,
    OtelImportResult,
    OtelLogImportResult,
)
from runtime_tools.providers.builtins.otel.importer._logs import import_otlp_logs
from runtime_tools.providers.builtins.otel.importer._trace import import_otlp_json

__all__ = [
    "MAX_OTLP_ATTRIBUTE_DEPTH",
    "MAX_OTLP_DOCUMENT_BYTES",
    "MAX_OTLP_LINKS",
    "MAX_OTLP_LOG_RECORDS",
    "MAX_OTLP_SPANS",
    "OtelImportError",
    "OtelImportResult",
    "OtelLogImportResult",
    "import_otlp_json",
    "import_otlp_logs",
]
