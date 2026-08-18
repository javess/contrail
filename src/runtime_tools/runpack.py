"""Supported read-only access to normalized Contrail runpack records."""

from __future__ import annotations

import os
from pathlib import Path
from types import TracebackType

from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    JsonScalar,
    JsonValue,
    Measurement,
)
from runtime_tools.storage import RunpackError
from runtime_tools.storage import RunpackReader as _RunpackReader

__all__ = [
    "Attachment",
    "CausalEdge",
    "Entity",
    "Event",
    "Execution",
    "JsonScalar",
    "JsonValue",
    "Measurement",
    "Runpack",
    "RunpackError",
    "open_runpack",
]


class Runpack:
    """A validated, read-only snapshot of one runpack."""

    __slots__ = ("__closed", "__reader")

    def __init__(self, path: str | os.PathLike[str]) -> None:
        try:
            self.__reader = _RunpackReader(Path(path))
        except RunpackError:
            raise
        except ValueError as error:
            raise RunpackError("invalid runpack path") from error
        self.__closed = False

    def _active_reader(self) -> _RunpackReader:
        if self.__closed:
            raise RunpackError("runpack is closed")
        return self.__reader

    def execution(self) -> Execution:
        """Return the run's normalized execution record."""
        return self._active_reader().execution()

    def manifest(self) -> dict[str, str]:
        """Return the runpack manifest."""
        return self._active_reader().manifest()

    def entities(self) -> tuple[Entity, ...]:
        """Return normalized entity records in stable order."""
        return self._active_reader().entities()

    def events(self) -> tuple[Event, ...]:
        """Return normalized event records in stable order."""
        return self._active_reader().events()

    def causal_edges(self) -> tuple[CausalEdge, ...]:
        """Return normalized causal edges in stable order."""
        return self._active_reader().causal_edges()

    def measurements(self) -> tuple[Measurement, ...]:
        """Return normalized measurement records in stable order."""
        return self._active_reader().measurements()

    def attachments(self) -> tuple[Attachment, ...]:
        """Return normalized attachment records in stable order."""
        return self._active_reader().attachments()

    def close(self) -> None:
        """Close the snapshot; repeated calls have no effect."""
        if self.__closed:
            return
        self.__closed = True
        self.__reader.close()

    def __enter__(self) -> Runpack:
        self._active_reader()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def open_runpack(path: str | os.PathLike[str]) -> Runpack:
    """Open and validate a read-only snapshot of one runpack."""
    return Runpack(path)
