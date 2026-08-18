"""Atomic helpers shared by bounded evidence adapters."""

from __future__ import annotations

import os
import sqlite3
import stat
import uuid
from collections.abc import Callable
from pathlib import Path

from runtime_tools.artifacts import artifact_exists
from runtime_tools.storage import (
    RunpackError,
    RunpackWriter,
    resolve_runpack_path,
    validated_runpack_snapshot,
)


class EnrichmentError(ValueError):
    """Raised when a runpack cannot be enriched safely."""


_STAGING_DIRECTORY = ".contrail-tmp"


def validate_enrichment_destination(output: Path) -> None:
    """Reject an unsafe destination before reading or normalizing evidence."""
    if artifact_exists(output):
        raise EnrichmentError(f"refusing to overwrite existing runpack: {output}")
    if not output.parent.is_dir():
        raise EnrichmentError(f"output directory does not exist: {output.parent}")


def _same_regular_file(path: Path, identity: tuple[int, int]) -> bool:
    try:
        current = path.stat(follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISREG(current.st_mode) and (current.st_dev, current.st_ino) == identity


def _source_snapshot_matches(
    path: Path,
    descriptor: int,
    expected: os.stat_result,
) -> bool:
    try:
        current_path = path.stat(follow_symlinks=False)
        current_descriptor = os.fstat(descriptor)
    except OSError:
        return False
    expected_key = expected.st_dev, expected.st_ino, stat.S_IMODE(expected.st_mode)
    return stat.S_ISREG(current_path.st_mode) and all(
        (current.st_dev, current.st_ino, stat.S_IMODE(current.st_mode)) == expected_key
        for current in (current_path, current_descriptor)
    )


def _same_directory(path: Path, identity: tuple[int, int]) -> bool:
    try:
        current = path.stat()
    except OSError:
        return False
    return stat.S_ISDIR(current.st_mode) and (current.st_dev, current.st_ino) == identity


def _regular_file_identity(descriptor: int, name: str) -> tuple[int, int] | None:
    try:
        current = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
    except OSError:
        return None
    if not stat.S_ISREG(current.st_mode):
        return None
    return current.st_dev, current.st_ino


def _open_staging_directory(parent: Path) -> tuple[Path, int]:
    staging = parent / _STAGING_DIRECTORY
    try:
        staging.mkdir(mode=0o700)
    except FileExistsError:
        pass
    except OSError as exc:
        raise EnrichmentError(f"could not create private staging directory: {exc}") from exc
    descriptor: int | None = None
    try:
        descriptor = os.open(
            staging,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        status = os.fstat(descriptor)
    except OSError as exc:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise EnrichmentError(f"could not open private staging directory: {exc}") from exc
    private_mode = stat.S_IMODE(status.st_mode) & 0o077 == 0
    owned = not hasattr(os, "geteuid") or status.st_uid == os.geteuid()
    if not stat.S_ISDIR(status.st_mode) or not private_mode or not owned:
        os.close(descriptor)
        raise EnrichmentError("private staging directory has unsafe ownership or permissions")
    return staging, descriptor


def _remove_matching_regular_file(
    descriptor: int,
    name: str,
    identity: tuple[int, int],
) -> None:
    # A directory descriptor prevents pathname retargeting, and the mode-0700
    # staging directory excludes mutation by other credentials. The destination
    # remains subject to its parent directory's permissions. A fully hostile
    # process with the same effective UID can still race this check and unlink;
    # Python has no portable link-by-fd or unlink-if-inode primitive that can make
    # the identity condition atomic.
    if _regular_file_identity(descriptor, name) != identity:
        return
    try:
        os.unlink(name, dir_fd=descriptor)
    except OSError:
        pass


def enrich_copy[T](
    source: Path,
    output: Path,
    operation: Callable[[RunpackWriter], T],
) -> T:
    validate_enrichment_destination(output)
    resolved_source = resolve_runpack_path(source)
    temporary_name = f".{output.name}.tmp-{uuid.uuid4().hex}"
    source_descriptor: int | None = None
    try:
        source_descriptor = os.open(
            resolved_source,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        source_status = os.fstat(source_descriptor)
    except OSError as exc:
        if source_descriptor is not None:
            try:
                os.close(source_descriptor)
            except OSError:
                pass
        raise EnrichmentError(f"could not read runpack permissions: {exc}") from exc
    if not stat.S_ISREG(source_status.st_mode):
        os.close(source_descriptor)
        raise EnrichmentError("source runpack is not a regular file")
    source_mode = stat.S_IMODE(source_status.st_mode)
    temporary: Path | None = None
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    temporary_descriptor: int | None = None
    staging_descriptor: int | None = None
    output_directory_descriptor: int | None = None
    output_directory_identity: tuple[int, int] | None = None
    destination_writer: RunpackWriter | None = None
    try:
        with validated_runpack_snapshot(source_descriptor) as source_snapshot:
            if not _source_snapshot_matches(
                resolved_source,
                source_descriptor,
                source_status,
            ):
                raise EnrichmentError("source runpack was replaced while opening it")
            try:
                output_directory_descriptor = os.open(
                    output.parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                output_directory_status = os.fstat(output_directory_descriptor)
            except OSError as exc:
                raise EnrichmentError(f"could not open output directory: {exc}") from exc
            if not stat.S_ISDIR(output_directory_status.st_mode):
                raise EnrichmentError("output directory is not a directory")
            output_directory_identity = (
                output_directory_status.st_dev,
                output_directory_status.st_ino,
            )
            if not _same_directory(output.parent, output_directory_identity):
                raise EnrichmentError("output directory was replaced while opening it")
            staging, staging_descriptor = _open_staging_directory(output.parent)
            temporary = staging / temporary_name
            try:
                destination_writer = RunpackWriter(temporary)
            except RunpackError as exc:
                if artifact_exists(temporary):
                    raise EnrichmentError(f"temporary runpack already exists: {temporary}") from exc
                raise EnrichmentError(f"could not create temporary runpack: {exc}") from exc
            temporary_owned = True
            try:
                temporary_descriptor = os.open(
                    temporary_name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=staging_descriptor,
                )
                temporary_stat = os.fstat(temporary_descriptor)
                if not stat.S_ISREG(temporary_stat.st_mode):
                    raise EnrichmentError("temporary runpack is not a regular file")
                temporary_identity = (temporary_stat.st_dev, temporary_stat.st_ino)
                if not _same_regular_file(temporary, temporary_identity):
                    raise EnrichmentError("temporary runpack was replaced before publication")
            except OSError as exc:
                raise EnrichmentError(f"could not copy runpack for enrichment: {exc}") from exc
            try:
                source_snapshot.copy_snapshot_to(destination_writer)
            except RunpackError as exc:
                raise EnrichmentError(f"could not copy runpack for enrichment: {exc}") from exc
        if not _same_regular_file(temporary, temporary_identity):
            temporary_owned = False
            raise EnrichmentError("temporary runpack was replaced before publication")
        writer = destination_writer
        destination_writer = None
        with writer:
            result = operation(writer)
        if not _same_regular_file(temporary, temporary_identity):
            temporary_owned = False
            raise EnrichmentError("temporary runpack was replaced before publication")
        try:
            os.fchmod(temporary_descriptor, source_mode)
        except OSError as exc:
            raise EnrichmentError(f"could not preserve runpack permissions: {exc}") from exc
        if not _same_regular_file(temporary, temporary_identity):
            temporary_owned = False
            raise EnrichmentError("temporary runpack was replaced before publication")
        assert output_directory_identity is not None
        if not _same_directory(output.parent, output_directory_identity):
            raise EnrichmentError("output directory was replaced before publication")
        try:
            assert staging_descriptor is not None
            assert output_directory_descriptor is not None
            os.link(
                temporary_name,
                output.name,
                src_dir_fd=staging_descriptor,
                dst_dir_fd=output_directory_descriptor,
            )
        except FileExistsError as exc:
            raise EnrichmentError(f"refusing to overwrite existing runpack: {output}") from exc
        except OSError as exc:
            raise EnrichmentError(f"could not publish runpack {output}: {exc}") from exc
        staged_identity = _regular_file_identity(staging_descriptor, temporary_name)
        published_identity = _regular_file_identity(output_directory_descriptor, output.name)
        output_directory_matches = _same_directory(output.parent, output_directory_identity)
        if (
            staged_identity != temporary_identity
            or published_identity != temporary_identity
            or not output_directory_matches
        ):
            if published_identity is not None and (
                published_identity == temporary_identity or published_identity == staged_identity
            ):
                _remove_matching_regular_file(
                    output_directory_descriptor,
                    output.name,
                    published_identity,
                )
            if staged_identity != temporary_identity:
                temporary_owned = False
            if not output_directory_matches:
                raise EnrichmentError("output directory was replaced during publication")
            raise EnrichmentError("published runpack was replaced before verification")
        _remove_matching_regular_file(staging_descriptor, temporary_name, temporary_identity)
        # The private directory is intentionally retained for reuse. Python has
        # no portable rmdir-by-handle primitive, so removing its pathname would
        # reintroduce the replacement race avoided by descriptor-relative cleanup.
        if not _same_directory(output.parent, output_directory_identity) or not _same_regular_file(
            output,
            temporary_identity,
        ):
            _remove_matching_regular_file(
                output_directory_descriptor,
                output.name,
                temporary_identity,
            )
            raise EnrichmentError("published runpack was replaced before return")
        return result
    except BaseException:
        if destination_writer is not None:
            try:
                destination_writer.close()
            except sqlite3.Error:
                pass
        if (
            temporary_owned
            and staging_descriptor is not None
            and temporary_identity is not None
            and _regular_file_identity(staging_descriptor, temporary_name) != temporary_identity
        ):
            temporary_owned = False
        if temporary_owned and staging_descriptor is not None and temporary_identity is not None:
            _remove_matching_regular_file(
                staging_descriptor,
                temporary_name,
                temporary_identity,
            )
        raise
    finally:
        if source_descriptor is not None:
            try:
                os.close(source_descriptor)
            except OSError:
                pass
        if temporary_descriptor is not None:
            try:
                os.close(temporary_descriptor)
            except OSError:
                pass
        if staging_descriptor is not None:
            try:
                os.close(staging_descriptor)
            except OSError:
                pass
        if output_directory_descriptor is not None:
            try:
                os.close(output_directory_descriptor)
            except OSError:
                pass
