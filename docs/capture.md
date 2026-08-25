# Capture and evidence

Use this guide to choose a capture level, understand what Contrail retains, and
recover or control long-running local captures.

## Choose the lowest useful level

```bash
contrail record --capture-level LEVEL --name NAME -- COMMAND
contrail analyze NAME.runpack
```

| Level | Evidence | Workload effect |
|---|---|---|
| `passive` | exit outcome, wall/CPU time, peak RSS, output identities | no injected observer |
| `process` | passive evidence plus bounded process-tree RSS and CPU samples | controller polls the process group |
| `sample` | process evidence, Python stack samples, subprocess/HTTP/network boundaries | 10 ms statistical sampler and boundary observer |
| `deep` | every Python/native call, Python exception propagation, and supported logical boundaries | exact and intrusive; highest overhead |

The default is `passive`. Use `sample` to find Python hotspots and `deep` when
exact callers, call counts, or semantic operation boundaries justify timing
distortion. Compare baseline and candidate with the same level.

The lower-level `--instrument sample|deep` and `--observe-process-tree` flags
remain available for expert combinations. Do not combine them with
`--capture-level`.

## Zero-touch Python capture

Sample and Deep prepend a private, standard-library-only `sitecustomize` to the
child process. The application does not import Contrail. Ordinary Python
children inherit the observer; isolated interpreters, disabled site startup,
detached processes, and alternate launchers may not. Coverage gaps appear in
the runpack and analysis.

Sample reports statistical stack evidence. Deep reports exact aggregate Python
and CPython-visible native call timing. Neither reads call arguments, return
values, locals, source contents, tracebacks, or exception messages.

Deep’s Python exception count is a propagation count: one exception may cross
several frames. The diagnostic count filters exact built-in iterator-control
types, but never records exception type names or values. Calls that replace
Python profile or trace hooks make the relevant evidence partial.

## Automatic boundaries

Sample and Deep retain bounded subprocess, outbound HTTP, connection, DNS, and
TLS evidence. Deep also enables logical-operation and inbound-server adapters.

| Family | Supported boundaries | Retained facts |
|---|---|---|
| subprocess | `subprocess.Popen` lifecycle | executable basename, PID relationship, timing, exit/launch classification, caller |
| HTTP client | `http.client`, HTTPX’s default `httpcore`, aiohttp | safe method, scheme, numeric port, timing to response headers, status/error class, caller |
| connection | blocking sockets, asyncio TCP/Unix transports | transport/family, numeric port, TLS marker, timing, safe outcome, caller |
| setup | `getaddrinfo`, blocking and asyncio TLS handshake | DNS/TLS phase, timing, safe outcome, caller |
| database/queue | sqlite3, SQLAlchemy, blocking/asyncio queues | operation class, timing, safe outcome, exact caller |
| cache/broker | Redis, Pika, aiokafka | operation class, timing, safe outcome, exact caller |
| executor/task | thread/process executors, `create_task`, `TaskGroup`, `ensure_future`, `gather` | submission-to-completion timing, safe outcome, exact caller |
| inbound HTTP | wsgiref, Uvicorn h11 and httptools cycles | adapter, numeric status, request-to-response completion timing, safe 5xx class, PID/role, exact application caller |

Optional client and Uvicorn packages are never imported or installed by
Contrail. Their adapters activate only when the workload imports a supported
module shape. Nested adapters are suppressed so one logical operation does not
also count lower client layers.

Inbound request duration ends after WSGI response iteration/transmission or the
final ASGI response-body send. WebSockets are ignored. Other gateways remain
ordinary Deep call evidence until explicitly supported.

## Privacy boundary

Automatic semantic capture does not retain:

- server identity, URL, route, path, query, ASGI scope, headers, bodies, client
  address, SNI, certificate, hostname, resolved address, or Unix path;
- SQL or parameters, cache keys, broker destinations/payloads, queue items,
  executor callables/arguments/results, awaitables, task names, or context;
- arguments, locals, successful return values, exception values, or exception
  messages.

It does retain low-entropy metadata such as adapter name, operation class,
numeric port/status, timing, safe exception class, PID/role, and source
filename/function for callers. Treat runpacks as potentially sensitive.

Stdout/stderr content and selected environment values are opt-in. Imported
provider files and raw attachments have their own privacy profile; see the
[threat model](threat-model.md).

## Bounds and partial evidence

Each process retains at most 256 records in each semantic family. The controller
normalizes at most 2,000 records per family. Profile functions and relationships
have separate documented bounds. Selection favors failures and longer retained
operations where applicable.

Truncation, unfinished records, malformed snapshots, missing status, hook
replacement, process-report gaps, and crash checkpoints downgrade completeness.
Proofline refuses exact count/error claims when their required evidence is not
complete.

Each interpreter registers synchronously, checkpoints after 50 ms, then every
500 ms. A crash or signal can therefore leave bounded partial evidence. A
process ending before the first checkpoint is registration-only: Contrail can
prove the observer loaded but makes no hotspot claim.

See [support](support.md) for exact platform, server, client, and process-model
limits and [performance](performance.md) for measured overhead and budgets.

## Long-running captures

Detach a capture when the launching terminal should not own it:

```bash
contrail record --detach --output nightly.runpack -- python nightly.py
```

Detached capture returns a job ID and privately retains bounded stdout/stderr:
the first and most recent 512 KiB of each stream. Output can contain secrets.

```bash
contrail job list
contrail job status JOB_ID
contrail job wait JOB_ID
contrail job output JOB_ID --follow
contrail job cancel JOB_ID
```

`wait` returns the captured command’s exit status. Ctrl-C on `output --follow`
stops only that observer; use `job cancel` to stop the capture.

If the controller is lost after the workload exits, recover the private
checkpoint without rerunning the workload:

```bash
contrail recover .nightly.runpack.tmp-ID --output nightly.runpack
```

Recovery validates the checkpoint, completes profile normalization, and never
overwrites an existing output.

## Runnable examples

All examples are local and self-contained:

```bash
contrail record --capture-level sample --name workers -- \
  python examples/local/worker_pool.py

contrail record --capture-level deep --name logical -- \
  python examples/local/logical_operations.py

uv run --with 'uvicorn[standard]' contrail record \
  --capture-level deep --name asgi -- \
  python examples/local/asgi_server.py --http h11
```
