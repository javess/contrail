#!/usr/bin/env python3
"""Enforce Contrail's internal import direction without third-party tooling."""

from __future__ import annotations

import argparse
import ast
import sys
from collections.abc import Iterable
from pathlib import Path

_PACKAGE = "runtime_tools"
_LAYER_NAMES = ("foundation", "adapter", "analysis", "presentation")
_FOUNDATION = frozenset(
    {
        "_version",
        "artifacts",
        "json_support",
        "model",
        "runpack",
        "semantics",
        "storage",
        "terminal",
        "yaml_support",
    }
)
_ADAPTERS = frozenset(
    {
        "annotations",
        "_deep_profile_bootstrap",
        "_sampling_profile_bootstrap",
        "_semantic_capture_bootstrap",
        "capture",
        "deep_profile",
        "enrichment",
        "kubernetes",
        "otel",
        "prometheus",
        "process_observer",
        "runtime",
        "temporal",
    }
)
_ANALYSES = frozenset({"inspect", "query", "batchscope", "rundiff", "proofline"})
_PRESENTATION = frozenset(
    {"capture_jobs", "capture_worker", "cli", "contrail_cli", "demo", "suite_report", "ui"}
)
_PRESENTATION_MODULES = frozenset(
    {
        "runtime_tools.batchscope.cli",
        "runtime_tools.batchscope.report",
        "runtime_tools.proofline.cli",
        "runtime_tools.proofline.report",
        "runtime_tools.rundiff.cli",
        "runtime_tools.rundiff.report",
    }
)


def _module_name(path: Path, source_root: Path) -> str:
    relative = path.relative_to(source_root).with_suffix("")
    parts = relative.parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join((_PACKAGE, *parts)) if parts else _PACKAGE


def _layer(module: str) -> int:
    if module == _PACKAGE or module in _PRESENTATION_MODULES:
        return 3
    relative = module.removeprefix(f"{_PACKAGE}.")
    top_level = relative.partition(".")[0]
    if top_level in _FOUNDATION:
        return 0
    if top_level in _ADAPTERS:
        return 1
    if top_level in _ANALYSES:
        return 2
    if top_level in _PRESENTATION:
        return 3
    raise ValueError(f"unclassified module: {module}")


def _known_target(candidate: str, modules: set[str]) -> str | None:
    parts = candidate.split(".")
    while parts:
        target = ".".join(parts)
        if target in modules:
            return target
        parts.pop()
    return None


def _absolute_from_module(
    source: str,
    node: ast.ImportFrom,
    package_modules: set[str],
) -> str | None:
    if node.level == 0:
        return node.module
    package = source if source in package_modules else source.rpartition(".")[0]
    parts = package.split(".")
    if node.level > len(parts):
        return None
    base = parts[: len(parts) - node.level + 1]
    if node.module:
        base.extend(node.module.split("."))
    return ".".join(base)


def _imports(
    source: str,
    tree: ast.AST,
    modules: set[str],
    package_modules: set[str],
) -> set[str]:
    targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            candidates: Iterable[str] = (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = _absolute_from_module(source, node, package_modules)
            if base is None:
                continue
            candidates = tuple(
                f"{base}.{alias.name}" if f"{base}.{alias.name}" in modules else base
                for alias in node.names
            )
        else:
            continue
        for candidate in candidates:
            if candidate != _PACKAGE and not candidate.startswith(f"{_PACKAGE}."):
                continue
            target = _known_target(candidate, modules)
            if target is not None and target != source:
                targets.add(target)
    return targets


def _cycle(graph: dict[str, set[str]]) -> tuple[str, ...] | None:
    active: list[str] = []
    active_set: set[str] = set()
    complete: set[str] = set()

    def visit(module: str) -> tuple[str, ...] | None:
        if module in complete:
            return None
        if module in active_set:
            start = active.index(module)
            return (*active[start:], module)
        active.append(module)
        active_set.add(module)
        for dependency in sorted(graph[module]):
            found = visit(dependency)
            if found is not None:
                return found
        active.pop()
        active_set.remove(module)
        complete.add(module)
        return None

    for module in sorted(graph):
        found = visit(module)
        if found is not None:
            return found
    return None


def check_architecture(source_root: Path) -> tuple[str, ...]:
    paths = tuple(sorted(source_root.rglob("*.py")))
    if not paths:
        return (f"no Python modules found under {source_root}",)
    modules = {_module_name(path, source_root) for path in paths}
    package_modules = {
        _module_name(path, source_root) for path in paths if path.name == "__init__.py"
    }
    graph: dict[str, set[str]] = {module: set() for module in modules}
    errors: list[str] = []

    for module in sorted(modules):
        try:
            _layer(module)
        except ValueError as error:
            errors.append(str(error))

    for path in paths:
        module = _module_name(path, source_root)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeError) as error:
            errors.append(f"cannot inspect {path}: {error}")
            continue
        graph[module] = _imports(module, tree, modules, package_modules)

    for source, dependencies in sorted(graph.items()):
        try:
            source_layer = _layer(source)
        except ValueError:
            continue
        for dependency in sorted(dependencies):
            try:
                dependency_layer = _layer(dependency)
            except ValueError:
                continue
            if dependency_layer > source_layer:
                errors.append(
                    f"{source} ({_LAYER_NAMES[source_layer]}) imports "
                    f"{dependency} ({_LAYER_NAMES[dependency_layer]})"
                )

    found_cycle = _cycle(graph)
    if found_cycle is not None:
        errors.append(f"internal import cycle: {' -> '.join(found_cycle)}")
    return tuple(errors)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path(__file__).parents[1] / "src" / _PACKAGE,
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    errors = check_architecture(arguments.source_root)
    if errors:
        for error in errors:
            print(f"architecture: {error}", file=sys.stderr)
        return 1
    modules = tuple(arguments.source_root.rglob("*.py"))
    print(f"architecture: {len(modules)} modules satisfy the declared dependency direction")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
