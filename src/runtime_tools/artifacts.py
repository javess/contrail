"""Atomic publication for completed portable artifacts."""

from __future__ import annotations

import os
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path


class ArtifactError(ValueError):
    """Raised when a completed artifact cannot be published safely."""


def artifact_exists(path: Path) -> bool:
    """Return whether a filesystem entry, including a dangling symlink, exists."""
    return path.exists() or path.is_symlink()


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


def _entry_identity(
    descriptor: int,
    name: str,
    *,
    follow_symlinks: bool = False,
) -> tuple[int, int] | None:
    try:
        status = os.stat(name, dir_fd=descriptor, follow_symlinks=follow_symlinks)
    except FileNotFoundError:
        return None
    except OSError:
        return None
    return status.st_dev, status.st_ino


def _regular_entry_identity(descriptor: int, name: str) -> tuple[int, int] | None:
    try:
        status = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
    except OSError:
        return None
    if not stat.S_ISREG(status.st_mode):
        return None
    return status.st_dev, status.st_ino


def _same_directory(path: Path, identity: tuple[int, int]) -> bool:
    try:
        status = path.stat()
    except OSError:
        return False
    return stat.S_ISDIR(status.st_mode) and (status.st_dev, status.st_ino) == identity


def _remove_owned_entry(descriptor: int, name: str, identity: tuple[int, int]) -> None:
    if _entry_identity(descriptor, name) != identity:
        return
    try:
        os.unlink(name, dir_fd=descriptor)
    except OSError:
        pass


@dataclass(slots=True)
class AtomicArtifactPublication:
    """A private sibling file reserved before producing an artifact."""

    destination: Path
    label: str
    _directory_descriptor: int | None
    _directory_identity: tuple[int, int]
    _temporary_name: str
    _temporary_descriptor: int | None
    _temporary_identity: tuple[int, int]
    _published: bool = False

    def publish(self, content: bytes) -> None:
        """Write all content and atomically link it into place without replacement."""
        descriptor = self._temporary_descriptor
        directory_descriptor = self._directory_descriptor
        if descriptor is None or directory_descriptor is None:
            raise ArtifactError(f"{self.label} publication is already closed")
        if self._published:
            raise ArtifactError(f"{self.label} publication is already complete")
        if not isinstance(content, bytes):
            raise ArtifactError(f"{self.label} content must be bytes")
        try:
            view = memoryview(content)
            while view:
                try:
                    written = os.write(descriptor, view)
                except InterruptedError:
                    continue
                if written <= 0:
                    raise OSError("temporary artifact write made no progress")
                view = view[written:]
            os.fsync(descriptor)
            status = os.fstat(descriptor)
        except OSError as exc:
            raise ArtifactError(f"could not write {self.label}: {exc}") from exc
        if (
            not stat.S_ISREG(status.st_mode)
            or (status.st_dev, status.st_ino) != self._temporary_identity
            or _regular_entry_identity(directory_descriptor, self._temporary_name)
            != self._temporary_identity
        ):
            raise ArtifactError(f"temporary {self.label} was replaced before publication")
        if not _same_directory(self.destination.parent, self._directory_identity):
            raise ArtifactError(f"{self.label} parent directory was replaced before publication")

        linked_destination_identity: tuple[int, int] | None = None
        try:
            os.link(
                self._temporary_name,
                self.destination.name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            source_identity = _entry_identity(directory_descriptor, self._temporary_name)
            followed_source_identity = _entry_identity(
                directory_descriptor,
                self._temporary_name,
                follow_symlinks=True,
            )
            destination_identity = _entry_identity(
                directory_descriptor,
                self.destination.name,
            )
            # Python has no portable link-by-fd operation. Record the exact
            # generation linked through the name so rollback can remove that
            # generation, including a symlink, without removing a later replacement.
            if destination_identity is not None and destination_identity in (
                source_identity,
                followed_source_identity,
                self._temporary_identity,
            ):
                linked_destination_identity = destination_identity
            if (
                _regular_entry_identity(directory_descriptor, self._temporary_name)
                != self._temporary_identity
                or _regular_entry_identity(directory_descriptor, self.destination.name)
                != self._temporary_identity
                or not _same_directory(self.destination.parent, self._directory_identity)
            ):
                raise ArtifactError(f"published {self.label} changed before verification")
        except FileExistsError as exc:
            raise ArtifactError(
                f"refusing to overwrite existing {self.label}: {self.destination}"
            ) from exc
        except ArtifactError:
            if linked_destination_identity is not None:
                _remove_owned_entry(
                    directory_descriptor,
                    self.destination.name,
                    linked_destination_identity,
                )
            raise
        except OSError as exc:
            raise ArtifactError(
                f"could not publish {self.label} {self.destination}: {exc}"
            ) from exc

        self._published = True
        _remove_owned_entry(
            directory_descriptor,
            self._temporary_name,
            self._temporary_identity,
        )

    def close(self) -> None:
        """Close descriptors and remove an unpublished owned temporary."""
        directory_descriptor = self._directory_descriptor
        if directory_descriptor is None:
            return
        if self._temporary_descriptor is not None:
            try:
                os.close(self._temporary_descriptor)
            except OSError:
                pass
            self._temporary_descriptor = None
        _remove_owned_entry(
            directory_descriptor,
            self._temporary_name,
            self._temporary_identity,
        )
        try:
            os.close(directory_descriptor)
        except OSError:
            pass
        self._directory_descriptor = None


def prepare_atomic_artifact(
    destination: Path,
    *,
    label: str = "artifact",
) -> AtomicArtifactPublication:
    """Validate a destination and reserve a mode-0600 sibling temporary."""
    if not isinstance(destination, Path):
        raise ArtifactError(f"{label} destination must be a path")
    raw = str(destination)
    if "\0" in raw:
        raise ArtifactError(f"{label} destination cannot contain NUL bytes")
    if not destination.name:
        raise ArtifactError(f"{label} destination must name a file")

    directory_descriptor: int | None = None
    temporary_descriptor: int | None = None
    temporary_identity: tuple[int, int] | None = None
    temporary_name = f".contrail-{label}.tmp-{uuid.uuid4().hex}"
    publication_ready = False
    try:
        directory_descriptor = os.open(
            destination.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        directory_status = os.fstat(directory_descriptor)
        if not stat.S_ISDIR(directory_status.st_mode):
            raise ArtifactError(f"{label} parent is not a directory: {destination.parent}")
        directory_identity = directory_status.st_dev, directory_status.st_ino
        if not _same_directory(destination.parent, directory_identity):
            raise ArtifactError(f"{label} parent directory was replaced while opening it")
        try:
            os.stat(destination.name, dir_fd=directory_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise ArtifactError(f"refusing to overwrite existing {label}: {destination}")
        temporary_descriptor = os.open(
            temporary_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_descriptor,
        )
        temporary_status = os.fstat(temporary_descriptor)
        temporary_identity = temporary_status.st_dev, temporary_status.st_ino
        if not stat.S_ISREG(temporary_status.st_mode):
            raise ArtifactError(f"temporary {label} is not a regular file")
        os.fchmod(temporary_descriptor, 0o600)
        if _regular_entry_identity(directory_descriptor, temporary_name) != temporary_identity:
            raise ArtifactError(f"temporary {label} was replaced while opening it")
        publication = AtomicArtifactPublication(
            destination,
            label,
            directory_descriptor,
            directory_identity,
            temporary_name,
            temporary_descriptor,
            temporary_identity,
        )
        publication_ready = True
        return publication
    except ArtifactError:
        raise
    except FileNotFoundError as exc:
        raise ArtifactError(
            f"{label} parent directory does not exist: {destination.parent}"
        ) from exc
    except NotADirectoryError as exc:
        raise ArtifactError(f"{label} parent is not a directory: {destination.parent}") from exc
    except OSError as exc:
        raise ArtifactError(f"could not prepare {label} {destination}: {exc}") from exc
    finally:
        if temporary_descriptor is not None and not publication_ready:
            try:
                os.close(temporary_descriptor)
            except OSError:
                pass
        if directory_descriptor is not None and not publication_ready:
            if temporary_identity is not None:
                _remove_owned_entry(
                    directory_descriptor,
                    temporary_name,
                    temporary_identity,
                )
            try:
                os.close(directory_descriptor)
            except OSError:
                pass
