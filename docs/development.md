# Python development

Contrail uses one installable Python distribution with explicit internal
layers. The goal is to make a checkout, wheel, and CI job exercise the same
imports and behavior.

## Set up the locked environment

Install the project and default development group from the committed universal
lockfile:

```bash
uv sync --locked
```

Run project commands with `uv run`. Do not install packages directly into
`.venv`; add an authorized runtime dependency with `uv add` or a development
tool with `uv add --dev`. `pyproject.toml` expresses supported dependency
ranges, while `uv.lock` records the exact cross-platform resolution.

This follows the PyPA
[`pyproject.toml` specification and guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
and uv's
[locking and syncing model](https://docs.astral.sh/uv/concepts/projects/sync/).

## Keep imports directional

Importable code lives under `src/runtime_tools`. The
[PyPA src-layout guidance](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
explains why this prevents repository-root files from becoming accidental
imports and makes tests exercise an installed package.

Internal modules follow four layers:

| Layer | Responsibilities | May import |
|---|---|---|
| Foundation | model, storage, artifact safety, serialization, shared support | foundation |
| Providers/adapters | capture, annotations, profiling, provider contracts and integrations | foundation, providers/adapters |
| Analyses | inspection, queries, RunDiff, BatchScope, Proofline | foundation, adapters, analyses |
| Presentation | public facade, CLIs, reports, demo, local UI | every layer |

Import concrete internal modules. The `runtime_tools` package root is the
external facade, so package internals must not import it. Run the executable
boundary and cycle check after changing imports:

```bash
uv run python tools/check_architecture.py
```

Provider implementations live under `runtime_tools/providers`; add host-side
integrations through the [provider contract](providers.md), not a new CLI
branch. The exact layer classification is in `.agent/rules.md` and the design
rationale is in [the architecture](architecture.md).

## Preserve strict types and runtime validation

All Python modules are checked with ty, while Ruff's `ANN` rules require typed
function boundaries. Use Python 3.12 native type syntax, precise unions, and
immutable value objects. Introduce a protocol only when callers genuinely need
structural substitution.

Static types do not validate data at runtime. Accept `object` at JSON, YAML,
SQLite, subprocess, and filesystem trust boundaries; validate shape and bounds;
then return a precise internal type. Keep `Any` and casts confined to
third-party or serialization boundaries. The
[Python typing specification](https://typing.python.org/en/latest/spec/type-system.html)
defines static analysis as typing's primary goal. The
[ty configuration reference](https://docs.astral.sh/ty/reference/configuration/)
documents the project-wide source and rule settings enforced locally and in CI.

Run:

```bash
uv run ty check
```

The distributed `py.typed` marker makes the public annotations available to
downstream type checkers.

## Test public decisions

Tests live outside the package and import supported package surfaces. Prefer an
observable outcome over private call order. Add cases for distinct decisions,
boundaries, and error behavior; parameterize equivalent input/output cases.
Keep tests offline and deterministic by isolating files, processes, clocks,
randomness, and services.

Pytest's
[good practices](https://docs.pytest.org/en/stable/explanation/goodpractices.html)
recommend a `src` layout and tests against the installed package. This
repository also enables strict configuration and marker checking.

Run the smallest relevant target while iterating, then the full suite for
cross-layer or public-contract changes:

```bash
uv run pytest tests/capture_test.py -x
uv run pytest
```

## Treat compatibility as architecture

Runpack schemas, versioned JSON, CLI exit statuses, public Python exports,
entry points, and packaged files are explicit contracts. Read
[runpack compatibility](compatibility.md) or
[machine-readable output](machine-output.md) before changing those surfaces.
Keep structured facts independent from human rendering and retain legacy
fixtures for reader compatibility.

For the complete local gate:

```bash
uv run python tools/check_architecture.py
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
uv build
uv run python benchmarks/release.py --profile pr
```

Repository-local agent rules and skills live under `.agent`; they encode the
same boundaries so future automated changes use the human and executable
contracts together.
