# Complex regression examples

These deterministic examples model application-shaped workflows while keeping the business result
equivalent. Each candidate introduces multiple runtime regressions that Contrail can trace from a
failed contract to exact events.

| Scenario | Preserved result | Candidate regressions |
| --- | --- | --- |
| Checkout | confirmed order | payment retries, gateway errors, legacy risk database |
| Analytics | published dataset | warehouse write amplification, raw profile API fan-out |
| Inference | same recommendations | feature lookup amplification, model-registry fallback |

Capture and inspect any scenario from the repository root. Replace `checkout` with `analytics` or
`inference` and use the matching script and contract names:

```bash
uv run contrail record --name checkout-baseline \
  --output checkout-baseline.runpack -- \
  python examples/complex/checkout_service.py
uv run contrail record --name checkout-candidate \
  --output checkout-candidate.runpack -- \
  python examples/complex/checkout_service.py --candidate
uv run contrail verify examples/complex/checkout_contract.yaml \
  --baseline checkout-baseline.runpack --candidate checkout-candidate.runpack \
  --report checkout-report.json
uv run contrail serve checkout-baseline.runpack \
  --compare checkout-candidate.runpack --proofline-report checkout-report.json
```

The verification command intentionally exits 1 because the candidate violates the example
contract. The report and runpacks remain valid inputs for the local comparison UI.
