# Release benchmarks

The harness generates deterministic evidence in a private temporary directory,
runs each operation in a fresh subprocess, and enforces elapsed-time and peak-RSS
ceilings from `budgets.json`. It uses only the Python standard library and the
installed Contrail package.

```bash
uv run python benchmarks/release.py --profile pr
uv run python benchmarks/release.py --profile release --json
```

The PR profile catches accidental algorithmic regressions quickly. The release
profile covers the largest interactive UI payload and representative six-figure
adapter inputs. These are release qualification ceilings, not end-user latency
promises. Input safety limits remain rejection bounds and do not promise that
every maximum-size input fits every machine.
