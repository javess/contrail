from __future__ import annotations

from pathlib import Path

import pytest

from runtime_tools.artifacts import publish_without_overwrite
from runtime_tools.enrichment import enrich_copy
from runtime_tools.model import Execution
from runtime_tools.storage import RunpackError, RunpackWriter


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


def test_enrichment_preserves_source_artifact_permissions(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    source.chmod(0o640)

    enrich_copy(source, output, lambda writer: None)

    assert source.stat().st_mode & 0o777 == 0o640
    assert output.stat().st_mode & 0o777 == 0o640


def test_enrichment_rejects_a_runpack_without_an_execution(tmp_path: Path) -> None:
    source = tmp_path / "empty.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source):
        pass

    with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
        enrich_copy(source, output, lambda writer: None)

    assert not output.exists()
