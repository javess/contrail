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
    with RunpackReader(source):
        pass
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")
    try:
        shutil.copyfile(source, temporary)
        shutil.copymode(source, temporary)
        with RunpackWriter.open_existing(temporary) as writer:
            result = operation(writer)
        try:
            publish_without_overwrite(temporary, output)
        except FileExistsError as exc:
            raise EnrichmentError(f"refusing to overwrite existing runpack: {output}") from exc
        except OSError as exc:
            raise EnrichmentError(f"could not publish runpack {output}: {exc}") from exc
        return result
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
