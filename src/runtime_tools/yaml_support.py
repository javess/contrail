"""Strict YAML loading for user-authored contracts and parameter specs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode
from yaml.resolver import BaseResolver
from yaml.tokens import AliasToken


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


class YamlInputError(ValueError):
    """Raised when a YAML source cannot be read within its input contract."""


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: MappingNode, deep: bool = False
) -> dict[object, object]:
    loader.flatten_mapping(node)
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_yaml(value: str) -> Any:
    """Load safe YAML while rejecting aliases and duplicate mapping keys."""
    for token in yaml.scan(value):
        if isinstance(token, AliasToken):
            raise ConstructorError(
                None,
                None,
                "YAML aliases are not supported",
                token.start_mark,
            )
    return yaml.load(value, Loader=_UniqueKeyLoader)


def _validate_utf8_strings(value: object, label: str) -> None:
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise YamlInputError(f"{label} contains a string that is not valid UTF-8") from exc
        return
    if isinstance(value, list):
        for item in value:
            _validate_utf8_strings(item, label)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _validate_utf8_strings(key, label)
            _validate_utf8_strings(item, label)


def load_yaml_file(path: Path, *, label: str, max_bytes: int) -> Any:
    """Read and decode a bounded UTF-8 YAML document."""
    try:
        with path.open("rb") as stream:
            raw = stream.read(max_bytes + 1)
    except OSError as exc:
        raise YamlInputError(f"could not read {label}: {path}") from exc
    if len(raw) > max_bytes:
        raise YamlInputError(f"{label} exceeds the {max_bytes}-byte input limit")
    try:
        value = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise YamlInputError(f"{label} must be UTF-8") from exc
    try:
        document = load_yaml(value)
        _validate_utf8_strings(document, label)
    except RecursionError as exc:
        raise YamlInputError(f"{label} nesting is too deep") from exc
    return document
