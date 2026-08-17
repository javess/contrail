"""Atomic publication for completed portable artifacts."""

from __future__ import annotations

import os
from pathlib import Path


def publish_without_overwrite(temporary: Path, destination: Path) -> None:
    """Publish by hard link, atomically failing if ``destination`` exists."""
    os.link(temporary, destination)
    temporary.unlink()
