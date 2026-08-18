from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from runtime_tools.artifacts import publish_without_overwrite
from runtime_tools.enrichment import EnrichmentError, enrich_copy
from runtime_tools.model import Entity, Execution, Measurement
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter


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


def test_artifact_publication_remains_successful_when_temporary_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary = tmp_path / "temporary.runpack"
    destination = tmp_path / "result.runpack"
    temporary.write_bytes(b"completed artifact")
    unlink = Path.unlink

    def fail_temporary_cleanup(path: Path, missing_ok: bool = False) -> None:
        if path == temporary:
            raise OSError("simulated cleanup failure")
        unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_temporary_cleanup)

    publish_without_overwrite(temporary, destination)

    assert destination.read_bytes() == b"completed artifact"
    assert temporary.exists()


def test_enrichment_preserves_source_artifact_permissions(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    source.chmod(0o640)

    enrich_copy(source, output, lambda writer: None)

    assert source.stat().st_mode & 0o777 == 0o640
    assert output.stat().st_mode & 0o777 == 0o640


def test_enrichment_can_extend_a_read_only_source_artifact(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
    source.chmod(0o440)

    def append(writer: RunpackWriter) -> None:
        writer.add_measurement(Measurement("work", 1, "1", 1, "worker", {}))

    enrich_copy(source, output, append)

    assert output.stat().st_mode & 0o777 == 0o440
    with RunpackReader(output) as reader:
        assert [measurement.name for measurement in reader.measurements()] == ["work"]


def test_enrichment_rejects_a_runpack_without_an_execution(tmp_path: Path) -> None:
    source = tmp_path / "empty.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source):
        pass

    with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
        enrich_copy(source, output, lambda writer: None)

    assert not output.exists()


def test_enrichment_does_not_follow_a_colliding_temporary_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    victim = tmp_path / "victim.txt"
    victim.write_text("preserve me", encoding="utf-8")
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))

    class FixedUuid:
        hex = "fixed"

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    temporary = tmp_path / ".output.runpack.tmp-fixed"
    temporary.symlink_to(victim)

    with pytest.raises(EnrichmentError, match="temporary runpack already exists"):
        enrich_copy(source, output, lambda writer: None)

    assert victim.read_text(encoding="utf-8") == "preserve me"
    assert temporary.is_symlink()
    assert not output.exists()
