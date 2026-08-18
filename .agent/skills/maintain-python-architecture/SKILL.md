---
name: maintain-python-architecture
description: Maintain Contrail's Python module boundaries, packaging, strict typing, tests, and portability. Use for changes under src/runtime_tools or tests, pyproject.toml edits, dependency changes, module extraction, import-cycle removal, public Python API work, or repository-wide refactors.
---

# Maintain Python Architecture

Keep structural changes aligned with the repository's enforced dependency
direction and installed-package behavior.

## Read first

1. Read `AGENTS.md`, `.agent/rules.md`, `.agent/runtime.md`, and
   `docs/development.md`.
2. Read `docs/architecture.md` for changes that cross modules or layers.
3. Read `pyproject.toml` and the closest behavior test.
4. Run `uv run python tools/check_architecture.py` before changing imports so
   the starting state is explicit.

## Make the change

1. Classify each touched module as foundation, adapter, analysis, or
   presentation using `.agent/rules.md`.
2. Keep dependencies pointed left in that sequence. Import a concrete internal
   module; never import the package facade from package internals.
3. Extract a module only when it creates one cohesive responsibility or removes
   a real dependency problem. Preserve the existing public import path with a
   small facade when compatibility requires it.
4. Represent stable domain facts with frozen, slotted dataclasses. Use a
   protocol only for a real replaceable boundary. Do not add abstraction solely
   to reduce file length.
5. Accept `object` at untrusted serialization boundaries, validate it, and
   narrow to precise internal types. Keep `Any` and casts at those boundaries.
6. Preserve CPython 3.12-3.14 behavior and POSIX-only behavior documented in
   `docs/support.md`. Avoid ambient working-directory, locale, timezone,
   network, or environment assumptions.
7. Use `uv add` or `uv remove` for an authorized dependency change. Keep
   runtime and development dependencies separate and the lockfile synchronized.

## Prove the result

Run the narrowest applicable checks after the final edit:

```bash
uv run python tools/check_architecture.py
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest <focused targets>
```

Run `uv build` for packaging or packaged-resource changes. Run the full test
suite for cross-layer refactors. Inspect the final diff for compatibility,
unnecessary abstractions, and unrelated churn.

Stop before adding a dependency without explicit approval. Invoke
`protect-public-contracts` before changing runpack, JSON, CLI, exit-status, or
public Python compatibility.
