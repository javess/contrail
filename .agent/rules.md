# Repository rules

## Architecture

- Keep importable code under `src/runtime_tools` and tests under `tests`.
- Import concrete internal modules. Do not import the `runtime_tools` package
  facade from another `runtime_tools` module.
- Preserve this dependency direction:
  `foundation -> adapters -> analyses -> presentation`.
- Foundation modules are `_version`, `artifacts`, `json_support`, `model`,
  `runpack`, `semantics`, `storage`, `terminal`, and `yaml_support`.
- Adapters are `annotations`, `capture`, `deep_profile`, `enrichment`,
  `kubernetes`, `otel`, `process_observer`, `prometheus`, `runtime`, `temporal`,
  and the private Python profiling and semantic-capture bootstraps.
- Analyses are `inspect`, `query`, `batchscope`, `rundiff`, and `proofline`.
- Presentation is the package facade, command modules, `capture_jobs`,
  `capture_worker`, `demo`, and `ui`.
- A layer may depend on itself or a layer to its left. The architecture check
  must remain cycle-free. Split modules around cohesive responsibilities, not
  an arbitrary line limit.

## Python and types

- Support CPython 3.12 through 3.14. Do not use syntax or APIs outside that
  range without a guarded compatibility path and a test.
- Keep ty strict across `src`, `tests`, `tools`, and `benchmarks`. Prefer precise value objects,
  immutable dataclasses, protocols at genuine injection boundaries, and
  `object` plus explicit validation for untrusted input.
- Keep `Any`, casts, and ignores at serialization or third-party boundaries.
  Never use them to silence an internal design mismatch.
- Treat type hints as static evidence, not runtime validation. Validate JSON,
  YAML, SQLite, subprocess, filesystem, and network-derived values before use.
- Use `Path` or `os.PathLike` at public filesystem boundaries. Use low-level
  `os` APIs when descriptor identity or race resistance requires them.

## Packaging and dependencies

- Keep project metadata, dependencies, and tool configuration in
  `pyproject.toml`; keep `uv.lock` committed and synchronized.
- Use `uv add` or `uv remove` for dependency changes. Runtime dependencies go
  in `project.dependencies`; development tools go in `dependency-groups.dev`.
- Prefer the standard library, then an existing dependency. A new dependency
  requires explicit user approval and focused compatibility evidence.
- Preserve the `src` layout, wheel type marker, packaged schemas, UI assets,
  entry points, and release-archive checks.

## Behavior and tests

- Preserve public runpack, JSON, CLI, exit-status, and Python API contracts.
  Read the compatibility skill before changing one.
- Test observable behavior through the supported surface. Add separate cases
  only for distinct decisions, boundaries, or failure modes.
- Keep tests deterministic and offline. Inject or isolate time, randomness,
  processes, filesystems, and external services.
- For a regression, demonstrate that the focused test fails without the fix
  when that proof is safe and practical.

## Documentation and evidence

- Update the closest canonical document when architecture, compatibility,
  security, packaging, or contributor workflow changes.
- Keep `README.md` task-oriented. Put design rationale in `docs/architecture.md`,
  compatibility in the versioned contract documents, and development policy in
  `docs/development.md`.
- Run `uv run python tools/check_architecture.py` for import-boundary changes,
  plus the narrowest applicable lint, type, test, build, and benchmark roles in
  `.agent/runtime.md`.
