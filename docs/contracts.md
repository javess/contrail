# Proofline contracts

Proofline evaluates explicit YAML claims over existing baseline and candidate
runpacks. It does not generate claims or use an LLM as an oracle.

Contract files are limited to 1 MiB and 1,000 assertions across all contracts
in the file. Duplicate keys, YAML aliases, unknown fields, unsupported
assertions, and non-finite thresholds are rejected before runpacks are loaded.

```yaml
name: persistence-regression
assertions:
  - type: candidate_exit_success
  - type: exit_code_equivalent
  - type: output_equivalent
  - type: max_runtime_regression
    percent: 10
  - type: max_cpu_time_regression
    percent: 15
  - type: max_peak_memory_regression
    percent: 20
  - type: forbid_new_dependency
    from: worker
    to: metadata-db
  - type: max_operation_count
    operation: db.write
    relative_to: baseline
    factor: 1.2
```

Supported assertions are intentionally mechanical:

- `candidate_exit_success` requires the candidate to have a captured exit
  status of zero. It is unverifiable when that status is unavailable. Use it
  in CI contracts because `exit_code_equivalent` deliberately permits two
  equal nonzero statuses;
- `exit_code_equivalent` compares the captured baseline and candidate exit
  statuses. Negative exit codes are reported as signals. The assertion is
  unverifiable when either status is unavailable; matching nonzero statuses
  pass because equivalence does not require a successful exit;
- `output_equivalent` (and the `result_equivalence` alias) compares captured
  stdout identities without storing output content;
- `max_runtime_regression`, `max_cpu_time_regression`, and
  `max_peak_memory_regression` compare numeric evidence with a percentage
  allowance;
- `forbid_new_dependency` fails only when the named edge was absent from the
  baseline and present in the candidate. It remains unverifiable when either
  OTLP import reports unresolved parent, link, or log-to-span references, or
  exporter-dropped attributes make semantic dependency matching incomplete;
- `max_operation_count` aggregates a semantic operation name across entities
  and constrains it relative to the baseline. If neither run contains the named
  operation, the claim is unverifiable rather than an automatic zero-count pass.
- `max_operation_error_count` applies the same baseline-relative limit to
  explicit operation failures while allowing an observed zero-failure baseline.

Dependency and operation-error assertions are also unverifiable when an OTLP
exporter reports dropped attributes. A missing semantic attribute can otherwise
hide a dependency or explicit failure.

An assertion with missing required evidence is `UNVERIFIABLE` and makes the
verification fail. Invalid or unsupported contracts are errors rather than
silently skipped claims. Unknown contract or assertion fields are also errors,
so misspelled thresholds cannot be ignored. `proofline verify` exits 0 for a
full pass, 1 for a failed or unverifiable claim, and 2 for invalid input.

## Trace a claim to runtime evidence

`proofline verify --format json --explain` and
`proofline run --format json --explain` attach portable structured policy and
evidence to every result. Each item records the canonical resolved assertion,
the exact values used for the verdict, and related detail in the same-snapshot
RunDiff included in the document:

| Assertion | RunDiff path | Selector |
| --- | --- | --- |
| `candidate_exit_success` | `/candidate/exit_code` | none |
| `exit_code_equivalent` | `/exit_code_equivalent` | none |
| `output_equivalent`, `result_equivalence` | `/output_equivalent` | none |
| `max_runtime_regression` | `/wall_time` | none |
| `max_cpu_time_regression` | `/cpu_time` | none |
| `max_peak_memory_regression` | `/peak_memory` | none |
| `forbid_new_dependency` | `/edge_count_changes` | `source_name`, `target_name` |
| `max_operation_count` | `/operation_count_changes` | `operation_name` |
| `max_operation_error_count` | `/operation_error_count_changes` | `operation_name` |

Text explanations show the exact evaluated values and the same RunDiff context
below each failed or unverifiable claim before rendering the complete diff and
deeper inspection commands. Change arrays are grouped by semantic identity, so
an aggregate claim's selector can match zero or multiple detail rows; the
item’s `fact` object is the authoritative aggregate used for the verdict.

Use `--report PATH` to retain this explained JSON without shell redirection.
The option implies explanation, publishes a complete private file for both pass
and contract-failure outcomes, refuses to overwrite any existing entry, and
leaves no report when input validation or execution exits 2.

Open the same contract beside its two artifacts to move directly from a failed
claim to candidate evidence:

```bash
runtime serve baseline.runpack --compare candidate.runpack \
  --contract proofline.yaml
```

Operation and dependency findings focus bounded event selections resolved by
the Python evaluator. Resource, outcome, and output findings focus the candidate
summary because attributing them to an arbitrary interval would overstate the
evidence. Use `--proofline-report REPORT.json` to inspect the exact explained
report retained by CI instead of re-evaluating a possibly changed contract.
For current reports, that mode parses the embedded assertions, evaluates them
against the supplied runpacks, and requires the complete result to match before
labelling it replay-verified. Assertion-less version-1 reports stay on the
explicitly degraded report-authored-policy/runtime-consistent path, including
pass results and selector-based claims.
