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
sessions, `fcntl`, `resource`, and non-blocking pipes. CLI capture also requires
anonymous-pipe descriptor inheritance and process-group signaling for its
separate worker. The top-level package imports the capture surface, so Windows
is not an adapters-only supported configuration.

## Host requirements

- A local filesystem that supports atomic hard links is required for no-clobber
  artifact publication. Network, FUSE, synchronized, and object-backed mounts
  are not release-qualified even when they appear to implement hard links.
- Post-exit `runtime recover` and `contrail recover` require the retained
  temporary runpack and any referenced private profile directory to remain on
  the same qualified host. Recovery is not a distributed handoff protocol.
- The capture worker is per-command and local to the invoking host. There is no
  persistent service. `runtime job` and `contrail job` discover, wait for, and
  cancel workers through private local files, named pipes, and advisory locks;
  this is not a distributed handoff protocol. Attached completion output still
  uses inherited descriptors. Explicit detached launches instead retain
  bounded output in the registry and make stdin unavailable.
- Capture-job discovery uses `$XDG_RUNTIME_DIR` when configured and otherwise a
  same-user subdirectory of the platform temporary directory. The selected
  local filesystem must support atomic replacement, named pipes, and `fcntl`
  advisory locks. Up to 100 terminal jobs are retained for seven days, including
  at most 1 MiB each of stdout and stderr for every explicitly detached job,
  split evenly between an append-only head and rolling tail for new jobs.
  Live output following polls those local files and job state at 100-millisecond
  intervals; it is not a remote log stream or persistent subscription.
- `/dev/fd` or `/proc/self/fd` must expose inherited descriptors for snapshot
  copying. Annotation transport uses `/dev/fd` on qualified hosts.
- Optional `--capture-level process|sample|deep` or
  `--observe-process-tree` capture requires the qualified host's
  `/bin/ps` with POSIX `pid`, `ppid`, `pgid`, `rss`, `time`, and `comm` fields.
  Missing support is recorded as unavailable observation rather than failing the
  workload.
- Sample and deep capture additionally wrap CPython's standard-library
  `subprocess.Popen` methods to record bounded semantic boundaries. This is
  qualified only on the supported CPython and POSIX matrix. Alternative
  subprocess implementations, direct `fork`/`exec`, native launches, and
  interpreters that disable site startup are not automatically observed.
- Deep capture consumes CPython `c_call`, `c_return`, and `c_exception`
  profiling events for built-ins and extension functions invoked through the
  interpreter. This supplies generic aggregate timing, native exception counts,
  and Python/native call edges for direct `_sqlite3`, file objects, compression,
  locks, and similar boundaries without a client dependency. Calls performed
  entirely inside native code, extension-owned worker threads that do not enter
  profiled Python, alternative interpreters, and native programs remain outside
  this coverage. Generic native evidence has no database, cache, broker, or
  filesystem semantic classification.
- Deep capture also consumes CPython `exception` trace events while explicitly
  disabling line and opcode events. Counts are per propagated Python frame, not
  unique exception instances: one exception crossing three frames can add three
  events. The raw aggregate is preserved. A second aggregate excludes only
  exact built-in `StopIteration`, `StopAsyncIteration`, and `GeneratorExit`
  type identities so normal iterator and coroutine completion cannot trigger
  exception-churn diagnosis. The type object is inspected transiently by
  identity but is not retained; its name, value, message, traceback, arguments,
  and locals are not read or retained. Subclasses are not filtered.
  A debugger, coverage tool, profiler, or workload that replaces `sys.settrace`
  can disable or conflict with this observation; alternative interpreters are
  outside the qualified boundary.
- Deep recognizes calls to the public `sys` and `threading` trace/profile hook
  setters and reports affected processes without inspecting the supplied hook.
  Any profile setter call conservatively makes exact call and caller evidence
  truncated; any trace setter call makes Python-exception evidence partial,
  even if the caller attempted to reinstall the same hook.
  Calls through CPython's C tracing API, direct interpreter-state mutation,
  temporary displacement followed by restoration before capture starts, and
  alternative interpreter mechanisms remain outside this integrity detector.
- Sample and deep capture wrap CPython's standard-library `http.client`
  boundary and lazily adapt supported `httpcore` sync/async and aiohttp request
  methods when those packages are installed by the workload. This covers
  `urllib.request`, HTTPX's default HTTP/1.1 and HTTP/2 transports, and aiohttp
  without making them Contrail dependencies. aiohttp redirects are
  represented as one logical request, and response-body download time is
  outside the measured interval.
- Sample and deep capture also wrap blocking CPython stream-socket connects and
  lazily wrap the standard asyncio TCP and Unix transport methods. This gives
  custom transports and Python-based database, queue, cache, and RPC clients a
  redacted connection fallback. Native transports, datagrams, raw nonblocking
  state machines, custom importers that bypass `PathFinder`, alternative event
  loops that replace the supported methods, and isolated interpreters are not
  automatically observed.
- Sample and deep capture wrap CPython `socket.getaddrinfo`,
  `ssl.SSLSocket.do_handshake`, and `ssl.SSLObject.do_handshake` for bounded DNS
  and blocking/asyncio TLS setup evidence. Native resolvers or TLS stacks,
  alternate SSL object implementations, and code that replaces these methods
  after Contrail starts are not automatically observed. TLS direction is not
  inferred, so local server and client handshakes can both appear.
- Deep capture lazily wraps standard `sqlite3.Connection`/`Cursor`,
  `queue.Queue`, and `asyncio.Queue` operation methods. SQLite's default module
  factory returns an observed subclass so C-defined methods can be timed;
  `type(connection) is sqlite3.Connection` remains true through the patched
  module export, but custom factories and direct `_sqlite3` imports are not
  semantically adapted. Their CPython-visible native calls can still appear as
  generic Deep hotspots. It also recognizes documented SQLAlchemy sync/async
  Connection and Session execution/transaction methods, Redis sync/async
  commands and pipelines, Pika `BlockingChannel` publish/get, and aiokafka
  producer send-and-wait plus consumer get-one/get-many when those packages are
  installed. Contrail neither imports nor depends on them. The repository's
  dependency-free conformance matrix exercises their public call shapes; exact
  upstream package/service combinations remain a compatibility qualification
  surface. `queue.SimpleQueue`, native database drivers, unsupported clients,
  custom finders that resolve a module before `PathFinder`, and code that
  replaces a wrapped method after import remain outside the semantic adapter
  set. An unsupported recognized module shape makes logical evidence incomplete.
  Standard `ThreadPoolExecutor` and `ProcessPoolExecutor` submissions are also
  observed until their returned Future completes. Timing includes executor
  queueing, process serialization, worker execution, and result relay rather
  than isolated worker runtime. Callable identity, arguments, successful
  results, and exception messages are excluded. Cancelled and rejected work is
  represented by safe exception class. Queue operations whose available exact
  caller is library/runtime code are excluded, preventing normal executor
  polling from appearing as application queue failures. Custom Executor/Future
  implementations and replaced `submit` methods remain outside this adapter.
  Explicit CPython `asyncio.create_task` and `TaskGroup.create_task` calls,
  top-level `asyncio.ensure_future` calls, and coroutines scheduled by
  top-level `asyncio.gather` are observed until completion with exact
  application callers. Existing Futures passed to `ensure_future` or `gather`
  are excluded rather than double-counted. The observer does
  not read coroutine/awaitable contents, task names, context values, arguments,
  successful results, or exception messages, and it does not consume task
  exceptions. Direct loop scheduling, direct use of the `asyncio.tasks`
  submodule, custom task factories, alternate event loops, and replaced
  methods remain outside this semantic adapter.
  Standard-library `wsgiref` requests are observed through Deep's existing
  call hook from `BaseHandler.run` entry through return. The numeric status and
  first application function are retained; method, route, URL, headers, bodies,
  and client address are not. Uvicorn h11 and httptools HTTP requests are
  observed through the same call hook from `RequestResponseCycle.run_asgi`
  entry through the final response-body send. Uvicorn is neither imported nor
  installed by Contrail. ASGI scope and messages are not retained, and
  WebSocket lifecycles are deliberately unclassified. Gunicorn, uWSGI,
  Waitress, other ASGI servers, custom gateways, replaced request-cycle
  methods, and Uvicorn protocol layouts outside the qualified shape remain
  generic profiler evidence.
  This broad method wrapping is intentionally unavailable in Sample mode.
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
