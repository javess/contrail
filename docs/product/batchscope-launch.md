# BatchScope launch brief

## Positioning

- **Primary user:** Platform, SRE, backend, and distributed-systems engineers
  debugging finite asynchronous jobs.
- **Primary pain:** The job took much longer than its compute, and explaining the
  remainder requires manually correlating several tools.
- **Primary promise:** Show where one logical run spent its wall-clock time.
- **Primary differentiation:** The unit of analysis is a job/run and its causal
  execution, not a service, pod, span, or log stream.
- **Main reason to believe:** BatchScope derives lifecycle, critical path,
  progress, post-compute drain, and evidence-labelled bottlenecks from a portable
  runpack; the built-in demo exercises that path locally.

Candidate lines:

1. Find where distributed jobs actually spend their time.
2. Debug the run, not five dashboards.
3. See what happened after the workers finished.

Use **“Find where distributed jobs actually spend their time.”** It states the
object, question, and value without implying unsupported automation.

## Landing page copy

### Metadata

`<title>`: `BatchScope — find where distributed jobs spend their time`

Meta description: `Reconstruct one asynchronous run from execution evidence and
see its lifecycle, critical path, post-compute drain, and bottlenecks. Local-first
and open source.`

### Header

Wordmark: `BatchScope by Contrail`

Links: `How it works` · `Limitations` · `GitHub`

### Hero

Headline: **Find where distributed jobs actually spend their time.**

Subheading: `A job can run for 45 minutes after its workers finish in eight.
BatchScope reconstructs one logical execution and shows what consumed the rest.`

Primary CTA: `Try the demo`

Secondary CTA: `View on GitHub`

Hero proof:

```text
BATCHSCOPE
run:   demo-candidate

Bottleneck
  serialized_stage (90%)
    result-aggregation ran at concurrency 1

Observation
  compute completed with 20 / 100 work items remaining
  post-compute wall time followed
```

Caption: `Output from the offline Contrail demo. Exact timings vary by machine.`

### Problem

Heading: **All the data exists. You still have to reconstruct the job.**

Body: `The trace shows services. The queue dashboard shows depth. The workflow
UI shows history. Worker logs show attempts. Kubernetes shows placement. None of
them necessarily explains why this logical run remained incomplete after its
main computation finished.`

Proof line: `Trace + queue + workflow + logs + Kubernetes ≠ one job timeline.`

### Product

Heading: **One finite run, accounted for.**

Body: `BatchScope reads normalized execution evidence, follows causal edges
across asynchronous boundaries, preserves missing evidence as uncertainty, and
derives lifecycle, critical path, throughput, remaining work, and bottlenecks.`

Callout: `It complements OpenTelemetry. It does not replace it.`

### Diagnosis examples

**Serialized result aggregation**

`Compute completed with 20% of work outstanding. The concurrency-one aggregation
stage dominated the remaining wall time.`

**Worker straggler**

`One operation took more than 2.5× the cohort median and added a material tail to
the run.`

**External dependency**

`Client operations occupied most of the observed critical path.`

**Capacity starvation**

`Correlated Kubernetes FailedScheduling evidence shows the run waited for
placement.`

### How it works

Heading: **Evidence in. Explanation out.**

```text
Capture or import bounded execution evidence
                    ↓
Normalize it into one portable .runpack
                    ↓
Follow causal work and explicit uncertainty
                    ↓
Explain lifecycle, drain, and bottlenecks
```

Body: `Use local process capture, optional Python work annotations, exported
OTLP/JSON traces and logs, Kubernetes snapshots, Prometheus JSON, or Temporal
history. No LLM is required for a diagnosis.`

### Open source and local

Heading: **Keep the evidence local.**

Body: `Run the core workflow on your machine. No account is required. Contrail
does not upload runpacks or telemetry, and stdout/stderr content is excluded by
default.`

### Limitations

Heading: **Useful beta, bounded scope.**

Body: `Today BatchScope analyzes finite runpacks. Telemetry integrations,
including Temporal history, consume exported JSON rather than live streams.
There is no hosted run search or generic APM dashboard. Reliable reconstruction
still depends on causal links, correlation identifiers, or small explicit
annotations for application phases and progress.`

### Final CTA

Heading: **Stop at the first unexplained minute.**

Body: `Run the offline demo, inspect the same evidence with all three Contrail
products, and decide whether the execution model fits your jobs.`

Primary CTA: `Run contrail demo`

Secondary CTA: `Read the source`

## Page structure

```text
Header
Hero
ExecutionProof
Problem
LogicalRunExplanation
DiagnosisExamples
HowItWorks
OpenSource
Limitations
FinalCTA
Footer
```

## Visual direction

Use a restrained near-black or warm-white surface, one monospace family for
execution evidence, and one neutral sans-serif for navigation and prose. The
terminal output is the hero artwork. Highlight only the bottleneck and the
post-compute interval with one amber accent. Avoid illustrations, fake dashboard
chrome, stock imagery, gradients, and decorative telemetry graphs.

## Demo asset

Record a 20–25 second terminal session:

1. Run `contrail demo`; pause on the retained artifact list.
2. Run the printed `contrail compare` command; highlight `3 → 30` writes and the
   new `metadata-db` dependency.
3. Run the printed `contrail analyze` command; stop on `result-aggregation` and
   `20 / 100 work items remaining`.
4. End on the local `contrail report` and `contrail inspect --tree` commands.

Record at a fixed terminal size with no shell theme noise. Use the same asset in
the README and hero; provide an accessible static terminal transcript beside it.

## Launch titles

1. Show HN: Contrail — debug finite jobs from one portable execution artifact
2. Show HN: BatchScope — find where background jobs spend their time
3. Why a finished worker does not mean a finished job
4. The missing 37 minutes in a 45-minute asynchronous job
5. Debugging post-compute latency without another tracing backend
6. One runpack, three questions: time, change, and runtime contracts
7. Reconstructing logical jobs across asynchronous boundaries
8. When result aggregation is slower than the computation
9. What distributed traces do not tell you about one logical run
10. Building an evidence-first debugger for background jobs

## First technical article

1. Open with a 45-minute job whose workers finished after eight minutes.
2. Walk through the five evidence surfaces an engineer normally checks.
3. Show why trace/service topology and logical job execution are related but not
   identical across queues, retries, fan-out, and post-processing.
4. Reconstruct the finite run as events plus explicit causal edges and clock
   uncertainty.
5. Account for compute completion, outstanding work, and serialized aggregation.
6. Show the BatchScope evidence before naming the product.
7. Explain the shared runpack pattern: RunDiff asks what changed; Proofline asks
   whether the change violated a contract.
8. State current limits: exported JSON, no live Temporal client, and annotation
   cost for application phases and progress.
9. End with the exact offline demo and the hypotheses that still need real-user
   evidence.

## Quality check

- **5-second:** The headline names distributed jobs and wall-clock time.
- **30-second:** The problem and product sections distinguish a logical run from
  service-centric tracing.
- **Credibility:** Every capability is present today; live-collector and
  automatic phase/progress claims are explicitly excluded.
- **Desire:** The compute-finished/result-aggregation example is concrete.
- **Action:** `contrail demo` is the primary action throughout.
- **Anti-marketing:** Claims are phrased as observed evidence or current limits.
