from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from runtime_tools.enrichment import enrich_copy
from runtime_tools.model import Attachment, Entity
from runtime_tools.storage import (
    RunpackError,
    RunpackReader,
    RunpackWriter,
    UnsupportedSchemaError,
)

_FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "compat"
_FIXTURE_SHA256 = {
    "schema-1.sql": "bf30b574629d4d3081b690d209475457a6a6613099a27155480dbc394c073a3f",
    "schema-1.1.sql": "2d46fd9f8be122aa0c8d4c2a0e6106d4e84bec2cf66ec020d2b6d541d89f2d9c",
}


def _materialize_fixture(tmp_path: Path, name: str) -> Path:
    source = _FIXTURE_DIRECTORY / name
    encoded = source.read_bytes()
    assert hashlib.sha256(encoded).hexdigest() == _FIXTURE_SHA256[name]
    output = tmp_path / name.replace(".sql", ".runpack")
    with sqlite3.connect(output) as connection:
        connection.executescript(encoded.decode("utf-8"))
    return output


@pytest.mark.parametrize(
    ("fixture", "schema_version", "attachment_count"),
    (("schema-1.sql", "1", 0), ("schema-1.1.sql", "1.1", 1)),
)
def test_reader_opens_immutable_released_schema_fixtures(
    tmp_path: Path,
    fixture: str,
    schema_version: str,
    attachment_count: int,
) -> None:
    runpack = _materialize_fixture(tmp_path, fixture)

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        manifest = reader.manifest()
        attachments = reader.attachments()

    assert manifest == {"producer_version": "0.1.0", "schema_version": schema_version}
    assert execution.id == "fixture-run"
    assert execution.name == "compatibility-fixture"
    assert len(attachments) == attachment_count


def test_reader_accepts_structurally_valid_additive_future_minor_schema(tmp_path: Path) -> None:
    runpack = _materialize_fixture(tmp_path, "schema-1.1.sql")
    with sqlite3.connect(runpack) as connection:
        connection.execute("UPDATE manifest SET value = '1.7' WHERE key = 'schema_version'")
        connection.execute("ALTER TABLE events ADD COLUMN future_optional TEXT")
        connection.execute("CREATE TABLE future_evidence(id TEXT PRIMARY KEY, payload TEXT)")

    with RunpackReader(runpack) as reader:
        assert reader.manifest()["schema_version"] == "1.7"
        assert reader.execution().id == "fixture-run"


def test_writer_rejects_future_minor_schema_without_mutating_artifact(tmp_path: Path) -> None:
    runpack = _materialize_fixture(tmp_path, "schema-1.1.sql")
    with sqlite3.connect(runpack) as connection:
        connection.execute("UPDATE manifest SET value = '1.7' WHERE key = 'schema_version'")
        connection.execute("ALTER TABLE events ADD COLUMN future_optional TEXT")
    before = runpack.read_bytes()

    with pytest.raises(
        UnsupportedSchemaError,
        match=r"cannot modify runpack schema '1\.7'; writable schemas: 1, 1\.0, 1\.1",
    ):
        RunpackWriter.open_existing(runpack)

    assert runpack.read_bytes() == before


def test_snapshot_copy_rejects_future_minor_schema_before_backup(tmp_path: Path) -> None:
    source = _materialize_fixture(tmp_path, "schema-1.1.sql")
    output = tmp_path / "output.runpack"
    with sqlite3.connect(source) as connection:
        connection.execute("UPDATE manifest SET value = '1.7' WHERE key = 'schema_version'")

    with RunpackReader(source) as snapshot, RunpackWriter(output) as destination:
        with pytest.raises(
            UnsupportedSchemaError,
            match=r"cannot modify runpack schema '1\.7'; writable schemas: 1, 1\.0, 1\.1",
        ):
            snapshot.copy_snapshot_to(destination)

        with RunpackReader(output) as unchanged:
            with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
                unchanged.execution()


def test_enrichment_rejects_future_minor_schema_before_copying(tmp_path: Path) -> None:
    source = _materialize_fixture(tmp_path, "schema-1.1.sql")
    output = tmp_path / "enriched.runpack"
    with sqlite3.connect(source) as connection:
        connection.execute("UPDATE manifest SET value = '1.7' WHERE key = 'schema_version'")
        connection.execute("ALTER TABLE events ADD COLUMN future_optional TEXT")
    before = source.read_bytes()
    operation_called = False

    def add_evidence(writer: RunpackWriter) -> None:
        nonlocal operation_called
        operation_called = True
        writer.add_entity(Entity("new-entity", "worker", "new", None, {}))

    with pytest.raises(
        UnsupportedSchemaError,
        match=r"cannot modify runpack schema '1\.7'; writable schemas: 1, 1\.0, 1\.1",
    ):
        enrich_copy(source, output, add_evidence)

    assert operation_called is False
    assert source.read_bytes() == before
    assert not output.exists()


def test_reader_rejects_unknown_schema_major_from_released_fixture(tmp_path: Path) -> None:
    runpack = _materialize_fixture(tmp_path, "schema-1.1.sql")
    with sqlite3.connect(runpack) as connection:
        connection.execute("UPDATE manifest SET value = '2.0' WHERE key = 'schema_version'")

    with pytest.raises(UnsupportedSchemaError, match="unsupported runpack schema '2.0'"):
        RunpackReader(runpack)


@pytest.mark.parametrize("legacy_version", ("1", "1.0"))
def test_adding_attachments_atomically_upgrades_legacy_schema(
    tmp_path: Path, legacy_version: str
) -> None:
    runpack = _materialize_fixture(tmp_path, "schema-1.sql")
    if legacy_version != "1":
        with sqlite3.connect(runpack) as connection:
            connection.execute(
                "UPDATE manifest SET value = ? WHERE key = 'schema_version'",
                (legacy_version,),
            )

    with RunpackWriter.open_existing(runpack) as writer:
        writer.add_attachments(
            (Attachment("new", "raw", "evidence", "text/plain", b"evidence", {}),)
        )

    with RunpackReader(runpack) as reader:
        assert reader.manifest()["schema_version"] == "1.1"
        assert [attachment.id for attachment in reader.attachments()] == ["new"]


def test_failed_legacy_attachment_write_rolls_back_schema_upgrade(tmp_path: Path) -> None:
    runpack = _materialize_fixture(tmp_path, "schema-1.sql")
    duplicate = Attachment("duplicate", "raw", "evidence", "text/plain", b"evidence", {})

    with RunpackWriter.open_existing(runpack) as writer:
        with pytest.raises(RunpackError, match="could not write runpack"):
            writer.add_attachments((duplicate, duplicate))

    with sqlite3.connect(runpack) as connection:
        schema_version = connection.execute(
            "SELECT value FROM manifest WHERE key = 'schema_version'"
        ).fetchone()[0]
        attachments_table = connection.execute(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'attachments'"
        ).fetchone()[0]
    assert schema_version == "1"
    assert attachments_table == 0


def test_known_legacy_schema_remains_writable_without_attachments(tmp_path: Path) -> None:
    runpack = _materialize_fixture(tmp_path, "schema-1.sql")

    with RunpackWriter.open_existing(runpack) as writer:
        writer.add_entity(Entity("new-entity", "worker", "new", None, {}))

    with RunpackReader(runpack) as reader:
        assert reader.manifest()["schema_version"] == "1"
        assert [entity.id for entity in reader.entities()] == ["fixture-process", "new-entity"]
