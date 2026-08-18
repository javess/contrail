"""Inference request with a candidate cache miss and model-registry fallback."""

from __future__ import annotations

import argparse
import json
import time

from runtime_tools import runtime


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", action="store_true")
    args = parser.parse_args()
    feature_count = 24

    with runtime.run("recommendation-request", total_work=4, feature_count=feature_count):
        with runtime.stage("decode-request", phase="request", concurrency=1):
            runtime.progress(completed=1, total=4, phase="decoded")

        with runtime.stage("load-features", phase="fetch", concurrency=8):
            lookups = feature_count if args.candidate else 4
            for _ in range(lookups):
                runtime.event(
                    "feature.lookup",
                    kind="client.request",
                    **{"peer.service": "feature-store"},
                )
            runtime.progress(completed=2, total=4, phase="features-loaded")

        with runtime.stage("model-inference", phase="compute", concurrency=1):
            if args.candidate:
                runtime.event(
                    "model.fetch",
                    kind="client.request",
                    **{"peer.service": "fallback-model-registry"},
                )
                time.sleep(0.002)
            else:
                runtime.event("model.cache-hit", kind="cache")
            runtime.progress(completed=3, total=4, phase="scored")

        with runtime.stage("encode-response", phase="response", concurrency=1):
            runtime.event("response.serialize", kind="cpu")
            runtime.progress(completed=4, total=4, phase="encoded")

    print(json.dumps({"items": ["sku-7", "sku-11", "sku-19"], "model": "v4"}))


if __name__ == "__main__":
    main()
