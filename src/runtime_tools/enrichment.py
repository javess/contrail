"""Atomic helpers shared by bounded evidence adapters."""

from __future__ import annotations

import shutil
import uuid
from collections.abc import Callable
from pathlib import Path

from runtime_tools.artifacts import publish_without_overwrite
from runtime_tools.storage import RunpackReader, RunpackWriter


class EnrichmentError(ValueError):
    """Raised when a runpack cannot be enriched safely."""


def enrich_copy[T](
    source: Path,
    output: Path,
    operation: Callable[[RunpackWriter], T],
) -> T:
    if output.exists():
        raise EnrichmentError(f"refusing to overwrite existing runpack: {output}")
    if not output.parent.is_dir():
        raise EnrichmentError(f"output directory does not exist: {output.parent}")
    with RunpackReader(source) as reader:
        reader.execution()
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")
    temporary_created = False
    try:
        try:
            destination = temporary.open("xb")
        except FileExistsError as exc:
            raise EnrichmentError(f"temporary runpack already exists: {temporary}") from exc
        except OSError as exc:
            raise EnrichmentError(f"could not create temporary runpack: {exc}") from exc
        temporary_created = True
        try:
            with destination, source.open("rb") as source_file:
                shutil.copyfileobj(source_file, destination)
        except OSError as exc:
            raise EnrichmentError(f"could not copy runpack for enrichment: {exc}") from exc
        with RunpackWriter.open_existing(temporary) as writer:
            result = operation(writer)
        try:
            shutil.copymode(source, temporary)
        except OSError as exc:
            raise EnrichmentError(f"could not preserve runpack permissions: {exc}") from exc
        try:
            publish_without_overwrite(temporary, output)
        except FileExistsError as exc:
            raise EnrichmentError(f"refusing to overwrite existing runpack: {output}") from exc
        except OSError as exc:
            raise EnrichmentError(f"could not publish runpack {output}: {exc}") from exc
        return result
    except BaseException:
        if temporary_created:
            temporary.unlink(missing_ok=True)
        raise
