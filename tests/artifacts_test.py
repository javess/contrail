from __future__ import annotations

import hashlib
import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

import runtime_tools.providers.enrichment as enrichment_module
from runtime_tools.artifacts import (
    ArtifactError,
    prepare_atomic_artifact,
    publish_without_overwrite,
)
from runtime_tools.model import Entity, Event, Execution, Measurement
from runtime_tools.proofline.verify import verify_contracts_with_artifact_bindings
from runtime_tools.providers.enrichment import EnrichmentError, enrich_copy
from runtime_tools.storage import (
    RunpackError,
    RunpackReader,
    RunpackWriter,
    UnsupportedSchemaError,
    open_runpack_snapshot,
)
from runtime_tools.storage import (
    validated_runpack_snapshot as real_validated_snapshot,
)


def _enrichment_temporary(tmp_path: Path) -> Path:
    return tmp_path / ".contrail-tmp" / ".output.runpack.tmp-fixed"


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


def test_atomic_artifact_cleanup_is_idempotent_after_descriptor_reuse(tmp_path: Path) -> None:
    publication = prepare_atomic_artifact(tmp_path / "report.json", label="report")
    sentinel = tmp_path / "sentinel"
    sentinel.write_bytes(b"sentinel")

    publication.close()
    sentinel_descriptor = os.open(sentinel, os.O_RDONLY)
    try:
        publication.close()
        assert os.read(sentinel_descriptor, 8) == b"sentinel"
    finally:
        os.close(sentinel_descriptor)


def test_atomic_artifact_prepare_never_unlinks_a_replacement_temporary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_fchmod = os.fchmod
    replacement: Path | None = None

    def replace_temporary_before_fchmod(descriptor: int, mode: int) -> None:
        nonlocal replacement
        temporary_files = tuple(tmp_path.glob(".contrail-report.tmp-*"))
        assert len(temporary_files) == 1
        replacement = temporary_files[0]
        replacement.unlink()
        replacement.write_bytes(b"concurrent replacement")
        real_fchmod(descriptor, mode)

    monkeypatch.setattr(os, "fchmod", replace_temporary_before_fchmod)

    with pytest.raises(ArtifactError, match="temporary report was replaced"):
        prepare_atomic_artifact(tmp_path / "report.json", label="report")

    assert replacement is not None
    assert replacement.read_bytes() == b"concurrent replacement"


@pytest.mark.parametrize("link_replacement", ("symlink", "followed-target"))
def test_atomic_artifact_publish_removes_its_replaced_link_without_deleting_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    link_replacement: str,
) -> None:
    destination = tmp_path / "report.json"
    publication = prepare_atomic_artifact(destination, label="report")
    temporary = next(tmp_path.glob(".contrail-report.tmp-*"))
    victim = tmp_path / "victim"
    victim.write_bytes(b"do not delete")
    real_link = os.link

    def replace_temporary_inside_link(
        source: str,
        target: str,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        assert source == temporary.name
        temporary.unlink()
        temporary.symlink_to(victim)
        linked_source = source if link_replacement == "symlink" else victim.name
        real_link(
            linked_source,
            target,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(os, "link", replace_temporary_inside_link)

    try:
        with pytest.raises(ArtifactError, match="published report changed"):
            publication.publish(b"complete report")
    finally:
        publication.close()

    assert not destination.exists()
    assert not destination.is_symlink()
    assert temporary.is_symlink()
    assert victim.read_bytes() == b"do not delete"


def test_atomic_artifact_publish_never_unlinks_a_replaced_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "report.json"
    publication = prepare_atomic_artifact(destination, label="report")
    real_link = os.link

    def replace_destination_inside_link(
        source: str,
        target: str,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        real_link(
            source,
            target,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        destination.unlink()
        destination.write_bytes(b"concurrent destination")

    monkeypatch.setattr(os, "link", replace_destination_inside_link)

    try:
        with pytest.raises(ArtifactError, match="published report changed"):
            publication.publish(b"complete report")
    finally:
        publication.close()

    assert destination.read_bytes() == b"concurrent destination"
    assert not tuple(tmp_path.glob(".contrail-report.tmp-*"))


def test_enrichment_preserves_source_artifact_permissions(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    source.chmod(0o640)

    enrich_copy(source, output, lambda writer: None)

    assert source.stat().st_mode & 0o777 == 0o640
    assert output.stat().st_mode & 0o777 == 0o640


def test_enrichment_keeps_temporary_evidence_private_until_publication(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    source.chmod(0o644)
    observed_modes: list[int] = []

    enrich_copy(
        source,
        output,
        lambda writer: observed_modes.append(writer.path.stat().st_mode & 0o777),
    )

    assert observed_modes == [0o600]
    assert (tmp_path / ".contrail-tmp").stat().st_mode & 0o777 == 0o700
    assert output.stat().st_mode & 0o777 == 0o644


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


def test_enrichment_copies_the_same_resolved_artifact_it_validates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validated = tmp_path / "validated.runpack"
    replacement = tmp_path / "replacement.runpack"
    requested = tmp_path / "requested.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(validated) as writer:
        writer.add_execution(
            Execution("validated", "validated", 0, 1, (), str(tmp_path), 0, None, {})
        )
    with RunpackWriter(replacement) as writer:
        writer.add_execution(
            Execution("replacement", "replacement", 0, 1, (), str(tmp_path), 0, None, {})
        )
    requested.symlink_to(replacement)
    real_resolve = Path.resolve

    def retarget_before_copy(path: Path, strict: bool = False) -> Path:
        if path == requested:
            return validated
        return real_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", retarget_before_copy)

    enrich_copy(requested, output, lambda writer: None)

    with RunpackReader(output) as reader:
        assert reader.execution().name == "validated"


def test_validated_snapshot_copy_stays_bound_to_the_open_descriptor(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    output = tmp_path / "output.runpack"
    for path, name in ((source, "original"), (replacement, "replacement")):
        with RunpackWriter(path) as writer:
            writer.add_execution(Execution(name, name, 0, 1, (), str(tmp_path), 0, None, {}))

    descriptor = os.open(source, os.O_RDONLY)
    try:
        with real_validated_snapshot(descriptor) as snapshot:
            os.replace(replacement, source)
            with RunpackWriter(output) as writer:
                snapshot.copy_snapshot_to(writer)
    finally:
        os.close(descriptor)

    with RunpackReader(output) as reader:
        assert reader.execution().name == "original"
    with RunpackReader(source) as reader:
        assert reader.execution().name == "replacement"


def test_read_snapshot_identity_stays_bound_to_the_open_generation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    for path, name in ((source, "original"), (replacement, "replacement")):
        with RunpackWriter(path) as writer:
            writer.add_execution(Execution(name, name, 0, 1, (), str(tmp_path), 0, None, {}))
    original_bytes = source.read_bytes()

    with open_runpack_snapshot(source) as (reader, identity):
        os.replace(replacement, source)
        assert reader.execution().name == "original"
        assert identity.size_bytes == len(original_bytes)
        assert identity.sha256 == hashlib.sha256(original_bytes).hexdigest()

    with RunpackReader(source) as reader:
        assert reader.execution().name == "replacement"


def test_read_snapshot_path_replacement_during_hash_cannot_mix_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    for path, name in ((source, "original"), (replacement, "replacement")):
        with RunpackWriter(path) as writer:
            writer.add_execution(Execution(name, name, 0, 1, (), str(tmp_path), 0, None, {}))
    original_bytes = source.read_bytes()
    real_pread = os.pread
    replaced = False

    def replace_on_first_read(descriptor: int, byte_count: int, offset: int) -> bytes:
        nonlocal replaced
        if not replaced:
            replaced = True
            os.replace(replacement, source)
        return real_pread(descriptor, byte_count, offset)

    monkeypatch.setattr("runtime_tools.storage._snapshot.os.pread", replace_on_first_read)

    with open_runpack_snapshot(source) as (reader, identity):
        assert reader.execution().name == "original"
        assert identity.sha256 == hashlib.sha256(original_bytes).hexdigest()

    assert replaced is True
    with RunpackReader(source) as reader:
        assert reader.execution().name == "replacement"


def test_read_snapshot_rejects_an_in_place_change_during_use(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))

    with pytest.raises(RunpackError, match="changed while its read snapshot was open"):
        with open_runpack_snapshot(source):
            status = source.stat()
            os.utime(
                source,
                ns=(status.st_atime_ns, status.st_mtime_ns + 1_000_000_000),
            )


def test_read_snapshot_rechecks_the_descriptor_after_sqlite_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    real_close = RunpackReader.close

    def mutate_when_snapshot_closes(reader: RunpackReader) -> None:
        real_close(reader)
        status = source.stat()
        os.utime(
            source,
            ns=(status.st_atime_ns, status.st_mtime_ns + 1_000_000_000),
        )

    monkeypatch.setattr(RunpackReader, "close", mutate_when_snapshot_closes)

    with pytest.raises(RunpackError, match="changed while its read snapshot was open"):
        with open_runpack_snapshot(source):
            pass


def test_read_snapshot_rejects_a_different_schema(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    with sqlite3.connect(source) as connection:
        connection.execute("UPDATE manifest SET value = '1.7' WHERE key = 'schema_version'")
        connection.execute("ALTER TABLE events ADD COLUMN future_optional TEXT")

    with pytest.raises(UnsupportedSchemaError, match="supported schema: 1.1"):
        with open_runpack_snapshot(source):
            pass


def test_artifact_bound_verification_uses_the_same_reader_snapshots(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    for path, execution_id in ((baseline, "baseline"), (candidate, "candidate")):
        with RunpackWriter(path) as writer:
            writer.add_execution(
                Execution(execution_id, execution_id, 0, 1, (), str(tmp_path), 0, None, {})
            )
    contract.write_text(
        "name: exact\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )

    report, diff, bindings = verify_contracts_with_artifact_bindings(
        contract,
        baseline,
        candidate,
    )

    assert report.baseline_id == diff.baseline.id == "baseline"
    assert report.candidate_id == diff.candidate.id == "candidate"
    assert bindings.baseline.sha256 == hashlib.sha256(baseline.read_bytes()).hexdigest()
    assert bindings.candidate.sha256 == hashlib.sha256(candidate.read_bytes()).hexdigest()
    assert (
        report.as_json_value(
            include_evidence=True,
            artifact_bindings=bindings,
        )["artifact_bindings"]
        == bindings.as_json_value()
    )


def test_snapshot_copy_restores_destination_foreign_key_enforcement(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))

    descriptor = os.open(source, os.O_RDONLY)
    try:
        with real_validated_snapshot(descriptor) as snapshot, RunpackWriter(output) as writer:
            snapshot.copy_snapshot_to(writer)
            with pytest.raises(RunpackError, match="FOREIGN KEY constraint failed"):
                writer.add_event(
                    Event("event", "operation", "event", "missing", 0, 1, "test", 0, 0, {})
                )
    finally:
        os.close(descriptor)


def test_snapshot_copy_normalizes_sqlite_failures(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))

    descriptor = os.open(source, os.O_RDONLY)
    destination = RunpackWriter(output)
    destination.close()
    try:
        with real_validated_snapshot(descriptor) as snapshot:
            with pytest.raises(RunpackError, match="could not copy runpack snapshot"):
                snapshot.copy_snapshot_to(destination)
    finally:
        os.close(descriptor)


def test_snapshot_copy_requires_exactly_one_execution(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source):
        pass

    with RunpackReader(source) as snapshot, RunpackWriter(output) as destination:
        with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
            snapshot.copy_snapshot_to(destination)


def test_enrichment_rejects_source_replacement_while_opening_the_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    output = tmp_path / "output.runpack"
    for path, name in ((source, "validated"), (replacement, "replacement")):
        with RunpackWriter(path) as writer:
            writer.add_execution(Execution(name, name, 0, 1, (), str(tmp_path), 0, None, {}))
    source.chmod(0o640)

    @contextmanager
    def replace_after_snapshot(descriptor: int) -> Iterator[RunpackReader]:
        with real_validated_snapshot(descriptor) as reader:
            os.replace(replacement, source)
            yield reader

    monkeypatch.setattr(
        enrichment_module,
        "validated_runpack_snapshot",
        replace_after_snapshot,
    )

    operations: list[RunpackWriter] = []

    with pytest.raises(EnrichmentError, match="source runpack was replaced while opening"):
        enrich_copy(source, output, operations.append)

    with RunpackReader(source) as reader:
        assert reader.execution().name == "replacement"
    assert source.stat().st_mode & 0o777 == 0o600
    assert operations == []
    assert not output.exists()


def test_enrichment_source_snapshot_resists_path_replacement_aba(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    moved_source = tmp_path / "moved-source.runpack"
    output = tmp_path / "output.runpack"
    for path, name in ((source, "original"), (replacement, "replacement")):
        with RunpackWriter(path) as writer:
            writer.add_execution(Execution(name, name, 0, 1, (), str(tmp_path), 0, None, {}))
    source.chmod(0o640)

    @contextmanager
    def replace_during_connection_open(descriptor: int) -> Iterator[RunpackReader]:
        os.replace(source, moved_source)
        os.replace(replacement, source)
        try:
            with real_validated_snapshot(descriptor) as reader:
                os.replace(source, replacement)
                os.replace(moved_source, source)
                yield reader
        finally:
            if moved_source.exists():
                if source.exists():
                    os.replace(source, replacement)
                os.replace(moved_source, source)

    monkeypatch.setattr(
        enrichment_module,
        "validated_runpack_snapshot",
        replace_during_connection_open,
    )

    enrich_copy(source, output, lambda writer: None)

    with RunpackReader(output) as reader:
        assert reader.execution().name == "original"
    assert output.stat().st_mode & 0o777 == 0o640


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
    temporary = _enrichment_temporary(tmp_path)
    temporary.parent.mkdir(mode=0o700)
    temporary.symlink_to(victim)

    with pytest.raises(EnrichmentError, match="temporary runpack already exists"):
        enrich_copy(source, output, lambda writer: None)

    assert victim.read_text(encoding="utf-8") == "preserve me"
    assert temporary.is_symlink()
    assert not output.exists()


def test_enrichment_removes_temporary_artifact_when_snapshot_copy_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))

    class FailingSnapshot:
        def copy_snapshot_to(self, writer: RunpackWriter) -> None:
            raise RunpackError("could not copy runpack snapshot: simulated snapshot failure")

    @contextmanager
    def fail_snapshot_copy(descriptor: int) -> Iterator[FailingSnapshot]:
        with real_validated_snapshot(descriptor):
            yield FailingSnapshot()

    monkeypatch.setattr(
        enrichment_module,
        "validated_runpack_snapshot",
        fail_snapshot_copy,
    )

    with pytest.raises(EnrichmentError, match="could not copy runpack for enrichment"):
        enrich_copy(source, output, lambda writer: None)

    assert not output.exists()
    assert not list((tmp_path / ".contrail-tmp").iterdir())


def test_enrichment_does_not_publish_a_replaced_temporary_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("source", "source", 0, 1, (), str(tmp_path), 0, None, {}))
    with RunpackWriter(replacement) as writer:
        writer.add_execution(
            Execution("replacement", "replacement", 0, 1, (), str(tmp_path), 0, None, {})
        )

    class FixedUuid:
        hex = "fixed"

    temporary = _enrichment_temporary(tmp_path)

    @contextmanager
    def replace_temporary_after_copy(descriptor: int) -> Iterator[RunpackReader]:
        with real_validated_snapshot(descriptor) as reader:
            yield reader
        os.replace(replacement, temporary)

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    monkeypatch.setattr(
        enrichment_module,
        "validated_runpack_snapshot",
        replace_temporary_after_copy,
    )
    operations: list[RunpackWriter] = []

    with pytest.raises(EnrichmentError, match="temporary runpack was replaced"):
        enrich_copy(source, output, operations.append)

    assert operations == []
    assert not output.exists()
    with RunpackReader(temporary) as reader:
        assert reader.execution().name == "replacement"


def test_enrichment_chmods_only_the_owned_temporary_inode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("source", "source", 0, 1, (), str(tmp_path), 0, None, {}))
    source.chmod(0o640)
    with RunpackWriter(replacement) as writer:
        writer.add_execution(
            Execution("replacement", "replacement", 0, 1, (), str(tmp_path), 0, None, {})
        )

    class FixedUuid:
        hex = "fixed"

    temporary = _enrichment_temporary(tmp_path)
    real_fchmod = os.fchmod

    def replace_before_fchmod(descriptor: int, mode: int) -> None:
        os.replace(replacement, temporary)
        real_fchmod(descriptor, mode)

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    monkeypatch.setattr("runtime_tools.providers.enrichment.os.fchmod", replace_before_fchmod)

    with pytest.raises(EnrichmentError, match="temporary runpack was replaced"):
        enrich_copy(source, output, lambda writer: None)

    assert not output.exists()
    assert temporary.stat().st_mode & 0o777 == 0o600
    with RunpackReader(temporary) as reader:
        assert reader.execution().name == "replacement"


def test_enrichment_verifies_the_published_inode_before_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("source", "source", 0, 1, (), str(tmp_path), 0, None, {}))
    with RunpackWriter(replacement) as writer:
        writer.add_execution(
            Execution("replacement", "replacement", 0, 1, (), str(tmp_path), 0, None, {})
        )

    class FixedUuid:
        hex = "fixed"

    temporary = _enrichment_temporary(tmp_path)
    real_link = os.link

    def replace_output_after_link(
        source_path: str | bytes,
        output_path: str | bytes,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        real_link(
            source_path,
            output_path,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        os.replace(replacement, output)

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    monkeypatch.setattr("runtime_tools.providers.enrichment.os.link", replace_output_after_link)

    with pytest.raises(EnrichmentError, match="published runpack was replaced"):
        enrich_copy(source, output, lambda writer: None)

    assert not temporary.exists()
    with RunpackReader(output) as reader:
        assert reader.execution().name == "replacement"


def test_enrichment_rolls_back_a_replaced_staged_entry_linked_as_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("source", "source", 0, 1, (), str(tmp_path), 0, None, {}))
    with RunpackWriter(replacement) as writer:
        writer.add_execution(
            Execution("replacement", "replacement", 0, 1, (), str(tmp_path), 0, None, {})
        )

    class FixedUuid:
        hex = "fixed"

    temporary = _enrichment_temporary(tmp_path)
    real_link = os.link

    def replace_staged_entry_inside_link(
        source_path: str | bytes,
        output_path: str | bytes,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        os.replace(replacement, temporary)
        real_link(
            source_path,
            output_path,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    monkeypatch.setattr(
        "runtime_tools.providers.enrichment.os.link", replace_staged_entry_inside_link
    )

    with pytest.raises(EnrichmentError, match="published runpack was replaced"):
        enrich_copy(source, output, lambda writer: None)

    assert not output.exists()
    with RunpackReader(temporary) as reader:
        assert reader.execution().name == "replacement"


def test_enrichment_rejects_output_parent_replacement_during_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    requested_parent = tmp_path / "requested"
    moved_parent = tmp_path / "moved"
    output = requested_parent / "output.runpack"
    requested_parent.mkdir()
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("source", "source", 0, 1, (), str(tmp_path), 0, None, {}))

    real_link = os.link

    def replace_output_parent_inside_link(
        source_path: str | bytes,
        output_path: str | bytes,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        requested_parent.rename(moved_parent)
        requested_parent.mkdir()
        real_link(
            source_path,
            output_path,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(
        "runtime_tools.providers.enrichment.os.link", replace_output_parent_inside_link
    )

    with pytest.raises(EnrichmentError, match="output directory was replaced"):
        enrich_copy(source, output, lambda writer: None)

    assert not output.exists()
    assert not (moved_parent / output.name).exists()


def test_enrichment_publication_uses_the_open_private_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    replacement = tmp_path / "replacement.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("source", "source", 0, 1, (), str(tmp_path), 0, None, {}))
    with RunpackWriter(replacement) as writer:
        writer.add_execution(
            Execution("replacement", "replacement", 0, 1, (), str(tmp_path), 0, None, {})
        )

    class FixedUuid:
        hex = "fixed"

    staging = tmp_path / ".contrail-tmp"
    moved_staging = tmp_path / "moved-staging"
    temporary_name = ".output.runpack.tmp-fixed"
    real_link = os.link

    def replace_staging_before_link(
        source_path: str | bytes,
        output_path: str | bytes,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        staging.rename(moved_staging)
        staging.mkdir(mode=0o700)
        os.replace(replacement, staging / temporary_name)
        real_link(
            source_path,
            output_path,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    monkeypatch.setattr("runtime_tools.providers.enrichment.os.link", replace_staging_before_link)

    enrich_copy(source, output, lambda writer: None)

    with RunpackReader(output) as reader:
        assert reader.execution().name == "source"
    with RunpackReader(staging / temporary_name) as reader:
        assert reader.execution().name == "replacement"
    assert not (moved_staging / temporary_name).exists()


def test_enrichment_preserves_primary_failure_when_temporary_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
    unlink = os.unlink

    def fail_temporary_cleanup(
        path: str | bytes,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if path == ".output.runpack.tmp-fixed" and dir_fd is not None:
            raise OSError("simulated cleanup failure")
        unlink(path, dir_fd=dir_fd)

    def fail_enrichment(writer: RunpackWriter) -> None:
        raise RuntimeError("primary enrichment failure")

    class FixedUuid:
        hex = "fixed"

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    monkeypatch.setattr(os, "unlink", fail_temporary_cleanup)

    with pytest.raises(RuntimeError, match="primary enrichment failure"):
        enrich_copy(source, output, fail_enrichment)

    assert not output.exists()


def test_enrichment_cleanup_stays_inside_the_open_private_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.runpack"
    output = tmp_path / "output.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(Execution("source", "source", 0, 1, (), str(tmp_path), 0, None, {}))

    class FixedUuid:
        hex = "fixed"

    staging = tmp_path / ".contrail-tmp"
    moved_staging = tmp_path / "moved-staging"
    temporary_name = ".output.runpack.tmp-fixed"
    unlink = os.unlink

    def replace_staging_before_cleanup(
        path: str | bytes,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if path == temporary_name and dir_fd is not None:
            staging.rename(moved_staging)
            staging.mkdir(mode=0o700)
            (staging / temporary_name).write_text("preserve me", encoding="utf-8")
        unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(uuid, "uuid4", FixedUuid)
    monkeypatch.setattr(os, "unlink", replace_staging_before_cleanup)

    enrich_copy(source, output, lambda writer: None)

    with RunpackReader(output) as reader:
        assert reader.execution().name == "source"
    assert (staging / temporary_name).read_text(encoding="utf-8") == "preserve me"
    assert not (moved_staging / temporary_name).exists()
