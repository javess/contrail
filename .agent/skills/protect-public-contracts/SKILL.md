---
name: protect-public-contracts
description: Evolve Contrail's versioned public behavior without accidental compatibility breaks. Use for runpack schemas, JSON schemas or output, CLI arguments and exit statuses, public Python exports, Proofline reports, packaged resources, release metadata, and compatibility-policy changes.
---

# Protect Public Contracts

Turn every public-boundary change into an explicit compatibility decision with
executable evidence.

## Read first

1. Read `AGENTS.md`, `.agent/rules.md`, and `.agent/runtime.md`.
2. Read the boundary's canonical policy:
   - `docs/compatibility.md` for runpacks;
   - `docs/machine-output.md` and packaged schemas for JSON;
   - `README.md` and CLI tests for commands and exit statuses;
   - `src/runtime_tools/__init__.py`, `src/runtime_tools/runpack.py`, and
     `py.typed` for the public Python surface;
   - `docs/ci.md`, `CHANGELOG.md`, and package metadata tests for releases.
3. Read a fixture from `tests/fixtures` when changing a persisted format.

## Decide compatibility

1. Name the observable contract and its current version before editing code.
2. Prefer additive readers and stable writers. Preserve accepted older input
   while preventing new writers from silently emitting an older or unknown
   format.
3. Reject unknown major versions, ambiguous input, duplicate keys, non-finite
   numbers, unsafe archive paths, and unsupported executable database features
   at the boundary.
4. Keep human rendering downstream of structured facts. Do not parse rendered
   prose to recover machine behavior.
5. Treat exit codes, JSON field names, ordering guarantees, error classes,
   package exports, entry points, and packaged files as compatibility surfaces.
6. Document a deliberate break before implementing it. Require an explicit
   version transition, migration story, changelog entry, and user approval.

## Prove the result

Add focused tests for the new behavior and retained compatibility. Use legacy
fixtures for reader changes and build/install tests for distribution changes.
Then run:

```bash
uv run python tools/check_architecture.py
uv run pytest tests/compatibility_test.py tests/json_contract_test.py
uv run pytest tests/package_metadata_test.py
uv build
```

Add the closest CLI or feature test when the contract is not covered by those
targets. Run the full suite for a format or public-facade change. Verify docs
describe the behavior proven by tests and inspect built archives when packaged
content changes.
