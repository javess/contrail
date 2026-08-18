# Support policy

## Release qualification matrix

Contrail 0.9 is a POSIX developer tool. CI runs the full source test and static
analysis gates on the minimum supported Python and the newest stable Python in
this matrix. The installed-wheel gate is separately qualified on Linux with
the minimum supported Python.

| Host or runner | Python | Qualification |
| --- | --- | --- |
| Linux (`ubuntu-latest`) | CPython 3.12 and 3.14 | Full source suite and static gates; installed-wheel smoke on 3.12 |
| macOS (`macos-latest`) | CPython 3.12 and 3.14 | Full source suite and static gates |
| Windows | any | Unsupported |
| Other POSIX systems | 3.12+ | Best effort, not release-qualified |

The package metadata accepts CPython 3.12 through 3.14. Python 3.13 is covered
by that compatibility range but is not a release-gating matrix entry. PyPy is
not qualified.

The installed artifact gate is independent of the checkout:

```bash
python tools/release_smoke.py dist/contrail_runtime_tools-0.9.0-py3-none-any.whl
```

That gate installs the wheel into a fresh tool environment and first captures a
plain process from a separate workload environment where Contrail is not
installed. It then installs the exact qualified wheel into the workload
environment using the same constraints and index policy, captures a
`client.request`/`db.write` annotation with the CLI from the tool environment,
and verifies that normalized event through both the supported Python runpack
reader and the versioned `runtime.query` JSON surface. It also runs `contrail
demo`, the failed contract, all branded analysis surfaces, and retained-report
event selection without reading examples from the source tree.

For an offline gate, provide a directory containing wheels for the locked
runtime dependencies with `--offline --find-links WHEELHOUSE`. Set
`CONTRAIL_RELEASE_WHEEL` and `CONTRAIL_RELEASE_SDIST` while running
`tests/package_metadata_test.py` to validate archive paths, metadata, entry
points, typed markers, UI assets, and absence of checkout-path leakage.

Local capture and annotations use POSIX process groups, file descriptors,
`fcntl`, `resource`, and non-blocking pipes. The top-level package imports the
capture surface, so Windows is not an adapters-only supported configuration.

## Host requirements

- A local filesystem that supports atomic hard links is required for no-clobber
  artifact publication. Network, FUSE, synchronized, and object-backed mounts
  are not release-qualified even when they appear to implement hard links.
- `/dev/fd` or `/proc/self/fd` must expose inherited descriptors for snapshot
  copying. Annotation transport uses `/dev/fd` on qualified hosts.
- Git is optional for capture, inspection, adapters, RunDiff, BatchScope, and
  verification of existing artifacts. It is required for `proofline run` and
  `proofline search`.
- Node.js is a test-only requirement for the packaged UI semantic regression.
  It is not required to run the local server. The UI targets current evergreen
  Chrome, Firefox, and Safari releases; no legacy-browser compatibility is
  promised.
- Contrail uses the SQLite library bundled with the selected CPython. Optional
  JSON query functions depend on that SQLite build; core reading and analysis do
  not require the JSON extension.

## Compatibility

Runpack schema major 1 is the portable artifact boundary. Readers reject other
major versions and accept additive 1.x minor versions. Compatibility releases
must continue to read the immutable 1 and 1.1 corpus; a current writer cannot
be used to regenerate those fixtures.

Machine-readable CLI documents carry their own format identity. Within the 0.9
series, fields may be added but existing meanings are not silently changed or
removed. Human-readable terminal layout is not a stable parsing interface.

The supported read-only Python runpack surface is `open_runpack` and
`RunpackError` from `runtime_tools`, the `Runpack` reader, and the normalized
record and JSON types exported from `runtime_tools.runpack`. Use `Runpack` as a
context manager or call `close()`; close is idempotent, and reads after close
raise `RunpackError`. The returned records are frozen snapshots, but nested JSON
lists and dictionaries remain ordinary mutable Python containers. Mutating
those in-memory containers does not change the runpack. The documented
annotation API is also supported; internal storage, writer, and analysis
helpers and private names beginning with `_` are not.

Security fixes are provided for the current 0.9 minor series. Older development
snapshots receive no backports. See the repository `SECURITY.md` for reporting.

## Resource envelope

The release benchmark is qualified on a host with at least two CPU cores and
4 GiB of available memory. Adapter byte/count constants are safety rejection
bounds, not a promise that every maximum-shape document is practical on the
minimum host. The deterministic supported shapes and hard budgets are documented
in `docs/performance.md` and `benchmarks/budgets.json`.
