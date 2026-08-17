from __future__ import annotations

from pathlib import Path

import pytest

from runtime_tools.artifacts import publish_without_overwrite


def test_artifact_publication_never_overwrites_a_concurrent_destination(tmp_path: Path) -> None:
    temporary = tmp_path / "temporary.runpack"
    destination = tmp_path / "result.runpack"
    temporary.write_bytes(b"new artifact")
    destination.write_bytes(b"concurrent artifact")

    with pytest.raises(FileExistsError):
        publish_without_overwrite(temporary, destination)

    assert destination.read_bytes() == b"concurrent artifact"
    assert temporary.read_bytes() == b"new artifact"


def test_artifact_publication_moves_completed_content_into_place(tmp_path: Path) -> None:
    temporary = tmp_path / "temporary.runpack"
    destination = tmp_path / "result.runpack"
    temporary.write_bytes(b"completed artifact")

    publish_without_overwrite(temporary, destination)

    assert destination.read_bytes() == b"completed artifact"
    assert not temporary.exists()
