"""Atomic publication for completed portable artifacts."""

from __future__ import annotations

import os
from pathlib import Path


def remove_best_effort(path: Path) -> None:
    """Remove a temporary artifact without replacing the primary outcome."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def publish_without_overwrite(temporary: Path, destination: Path) -> None:
    """Publish by hard link, atomically failing if ``destination`` exists."""
    os.link(temporary, destination)
    # The destination is already a complete hard link. Cleanup failure must not
    # turn successful publication into a contradictory error result.
    remove_best_effort(temporary)
