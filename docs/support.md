# Support and current limits

Contrail `0.10` is a POSIX developer tool.

## Qualified platforms

| Host | CPython | Qualification |
|---|---|---|
| Linux (`ubuntu-latest`) | 3.12, 3.14 | full source/static suite; wheel smoke on 3.12 |
| macOS (`macos-latest`) | 3.12, 3.14 | full source/static suite |
| CPython 3.13 | accepted and locally verified | inside package range; not a CI matrix endpoint |
| other POSIX | 3.12–3.14 | best effort |
| Windows or PyPy | — | unsupported |

Local capture relies on POSIX process groups, file descriptors, `fcntl`,
`resource`, non-blocking pipes, named pipes, and `/bin/ps`. Artifact publication
requires a local filesystem with reliable atomic hard links. Network, FUSE,
synchronized, and object-backed mounts are not release-qualified.

The installed-wheel gate builds a fresh tool environment and exercises capture
against a separate workload environment without reading the checkout. See
[CI and release qualification](ci.md).

## Capture support

| Boundary | Level | Qualified shape |
|---|---|---|
| process tree/resources | process+ | POSIX `/bin/ps` fields used by Contrail |
| Python stack samples | sample | supported CPython interpreters with site startup |
| Python/native calls and exceptions | deep | CPython profile/trace events |
| subprocess | sample/deep | standard-library `subprocess.Popen` |
| outbound HTTP | sample/deep | `http.client`; lazy httpcore and aiohttp shapes |
| network connect/DNS/TLS | sample/deep | blocking sockets plus standard asyncio/ssl shapes |
| database/cache/queue/broker | deep | listed standard and lazy client adapters |
| executors/asyncio tasks | deep | standard executors and supported top-level asyncio scheduling |
| inbound WSGI | deep | `wsgiref.handlers.BaseHandler.run` |
| inbound ASGI HTTP | deep | Uvicorn h11 and httptools `RequestResponseCycle.run_asgi` |

Contrail does not install or import optional clients or Uvicorn. Lazy adapters
activate only after the workload imports a recognized module.

Deep database/cache/queue/broker coverage includes standard `sqlite3`,
`queue.Queue`, `asyncio.Queue`, and recognized SQLAlchemy, Redis, Pika, and
aiokafka public call shapes. Direct `_sqlite3`, custom factories, native
drivers, `SimpleQueue`, custom importers, replaced methods, and unsupported
client versions may remain generic native evidence or make that family partial.

Executor timing runs from submission to Future completion and includes queueing
and result relay. Async task timing runs from creation to completion and
includes suspended awaits; it is not CPU time. Existing Futures passed through
supported scheduling helpers are excluded to prevent double counting.

WSGI and Uvicorn server timing runs through response completion. Uvicorn h11
and httptools HTTP are qualified; WebSockets are deliberately ignored. Gunicorn,
uWSGI, Waitress, other ASGI servers, custom gateways, replaced request-cycle
methods, and unknown Uvicorn layouts are outside semantic classification and
remain ordinary Deep profiling evidence.

## Known observer limits

- Interpreters that disable site startup are not automatically observed.
- Direct `fork`/`exec`, native launches, datagrams, native transports/resolvers,
  alternate event loops, and native-only worker threads can escape semantic
  capture.
- HTTP response-body download time is outside outbound request timing.
- aiohttp redirects appear as one logical outbound request.
- TLS direction is not inferred; local client and server handshakes can both
  appear.
- Deep native aggregates classify functions, calls, timing, and safe exception
  counts, not database/cache/filesystem semantics.
- Python exception counts are per propagated frame, not unique failure.
- Workloads, debuggers, profilers, or coverage tools that replace trace/profile
  hooks can conflict with Deep. Public setter calls are detected and evidence
  is downgraded, but C API or interpreter-state mutation is not.
- Sample and Deep wrapping is unsupported when application code replaces a
  recognized method after Contrail has adapted it.

All capture families are bounded. Unsupported or incomplete evidence is marked
partial/unavailable rather than inferred. See [capture and evidence](capture.md)
and [performance](performance.md).

## Providers

Built-in OpenTelemetry, Kubernetes, Prometheus, and Temporal integrations import
exported files; they are not live collectors. They are always available through
the fixed command catalog and have canonical paths below
`runtime_tools.providers.builtins`.

See [built-in evidence integrations](providers.md).

## Jobs and recovery

The capture worker is local and per command; there is no daemon. Job discovery,
wait, cancel, and detached output use private same-user files, locks, and pipes.
At most 100 terminal jobs are retained for seven days. Detached output is
limited to 1 MiB each for stdout and stderr.

Recovery requires the retained temporary runpack and profile session on the
same host. It is not a distributed handoff protocol. Attached output uses
inherited descriptors; detached capture has no stdin.

## Other requirements

- Git is optional for capture, import, inspection, analysis, and verification
  of existing artifacts. It is required for `contrail run` and `contrail search`.
- Optional JSON queries depend on JSON support in CPython's bundled SQLite;
  core reading and analysis do not.

## Compatibility and fixes

Runpack schema `1.1`, structured document format `2`, and the read-only
`open_runpack` API are documented compatibility
surfaces. Human output and internal modules are not. See
[compatibility](compatibility.md).

Security fixes target the current `0.10` line; development snapshots receive no
backports. Report issues through [SECURITY.md](../SECURITY.md).

Release benchmarks assume at least two CPU cores and 4 GiB available memory.
Safety limits are rejection bounds, not a promise that every combined maximum
shape is practical on that minimum host.
