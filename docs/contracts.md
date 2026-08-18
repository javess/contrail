# Proofline contracts

Proofline evaluates explicit YAML claims over existing baseline and candidate
runpacks. It does not generate claims or use an LLM as an oracle.

Contract files are limited to 1 MiB and 1,000 assertions across all contracts
in the file. Duplicate keys, YAML aliases, unknown fields, unsupported
assertions, and non-finite thresholds are rejected before runpacks are loaded.

```yaml
name: persistence-regression
assertions:
  - type: output_equivalent
  - type: max_runtime_regression
    percent: 10
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

- `output_equivalent` (and the `result_equivalence` alias) compares captured
  stdout identities without storing output content;
- `max_runtime_regression` and `max_peak_memory_regression` compare numeric
  evidence with a percentage allowance;
- `forbid_new_dependency` fails only when the named edge was absent from the
  baseline and present in the candidate. It remains unverifiable when either
  OTLP import reports unresolved parent or link references, or exporter-dropped
  attributes make semantic dependency matching incomplete;
- `max_operation_count` aggregates a semantic operation name across entities
  and constrains it relative to the baseline. If neither run contains the named
  operation, the claim is unverifiable rather than an automatic zero-count pass.
- `max_operation_error_count` applies the same baseline-relative limit to
  explicit operation failures while allowing an observed zero-failure baseline.

Output equivalence and operation assertions are also unverifiable when an OTLP
exporter reports dropped attributes. A missing semantic attribute can otherwise
turn distinct results into apparent matches or hide an operation or failure.

An assertion with missing required evidence is `UNVERIFIABLE` and makes the
verification fail. Invalid or unsupported contracts are errors rather than
silently skipped claims. Unknown contract or assertion fields are also errors,
so misspelled thresholds cannot be ignored. `proofline verify` exits 0 for a
full pass, 1 for a failed or unverifiable claim, and 2 for invalid input.
