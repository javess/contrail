from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import TextIO

_ROOT = Path(__file__).parents[1]
_DISTRIBUTION_NAME = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")
_CHANGELOG_RELEASE = re.compile(r"^## ([^\r\n]+)$", re.MULTILINE)


class ReleaseCheckError(ValueError):
    pass


def _release_identity(pyproject: Path, changelog: Path) -> dict[str, str]:
    try:
        document = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        project = document["project"]
        name = project["name"]
        version = project["version"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise ReleaseCheckError(
            f"cannot read project identity from {pyproject}: {error}"
        ) from error

    if not isinstance(name, str) or not _DISTRIBUTION_NAME.fullmatch(name):
        raise ReleaseCheckError("project.name must be a non-empty distribution name")
    if (
        not isinstance(version, str)
        or not version
        or any(character.isspace() for character in version)
    ):
        raise ReleaseCheckError("project.version must be a non-empty string without whitespace")

    try:
        changelog_text = changelog.read_text(encoding="utf-8")
    except OSError as error:
        raise ReleaseCheckError(f"cannot read changelog from {changelog}: {error}") from error
    release = _CHANGELOG_RELEASE.search(changelog_text)
    if release is None:
        raise ReleaseCheckError(f"{changelog} has no release heading")
    if release.group(1) != version:
        raise ReleaseCheckError(
            f"first changelog release is {release.group(1)!r}, expected {version!r}"
        )

    return {"name": name, "version": version, "tag": f"v{version}"}


def check_release(
    *, pyproject: Path, changelog: Path, ref_type: str, ref_name: str
) -> dict[str, str]:
    identity = _release_identity(pyproject, changelog)
    if ref_type == "tag" and ref_name != identity["tag"]:
        raise ReleaseCheckError(
            f"release tag is {ref_name!r}, expected {identity['tag']!r} for project version "
            f"{identity['version']}"
        )
    return identity


def _write_github_output(stream: TextIO, identity: dict[str, str]) -> None:
    for key in ("name", "version", "tag"):
        stream.write(f"{key}={identity[key]}\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the project, changelog, and optional release tag identity."
    )
    parser.add_argument("--pyproject", type=Path, default=_ROOT / "pyproject.toml")
    parser.add_argument("--changelog", type=Path, default=_ROOT / "CHANGELOG.md")
    parser.add_argument("--ref-type", choices=("branch", "tag"), required=True)
    parser.add_argument("--ref-name", required=True)
    parser.add_argument(
        "--github-output",
        type=Path,
        help="append validated name, version, and tag values to a GitHub Actions output file",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        identity = check_release(
            pyproject=arguments.pyproject,
            changelog=arguments.changelog,
            ref_type=arguments.ref_type,
            ref_name=arguments.ref_name,
        )
        if arguments.github_output is not None:
            with arguments.github_output.open("a", encoding="utf-8") as output:
                _write_github_output(output, identity)
    except (OSError, ReleaseCheckError) as error:
        print(f"release-check: {error}", file=sys.stderr)
        return 2
    print(json.dumps(identity, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
