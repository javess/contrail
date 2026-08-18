"""Analytics pipeline whose candidate amplifies writes and adds profile fan-out."""

from __future__ import annotations

import argparse
import json
import time

from runtime_tools import runtime


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", action="store_true")
    args = parser.parse_args()
    batches = 8

    with runtime.run("daily-analytics-pipeline", total_work=batches):
        runtime.progress(completed=0, total=batches, phase="starting")
        with runtime.stage("extract", phase="read", concurrency=8):
            for _ in range(batches):
                runtime.event(
                    "object.read",
                    kind="client.request",
                    **{"peer.service": "events-bucket"},
                )
            runtime.progress(completed=2, total=batches, phase="extracted")

        with runtime.stage("normalize", phase="compute", concurrency=8):
            time.sleep(0.001)
            runtime.progress(completed=4, total=batches, phase="normalized")

        with runtime.stage("enrich", phase="compute", concurrency=4):
            if args.candidate:
                for _ in range(batches):
                    runtime.event(
                        "profile.lookup",
                        kind="client.request",
                        **{"peer.service": "raw-profile-api"},
                    )
            runtime.progress(completed=6, total=batches, phase="enriched")

        with runtime.stage("warehouse-load", phase="persist", concurrency=1):
            writes_per_batch = 4 if args.candidate else 1
            for _ in range(batches * writes_per_batch):
                runtime.event(
                    "warehouse.upsert",
                    kind="client.request",
                    **{"peer.service": "analytics-warehouse"},
                )
                time.sleep(0.0001)
            runtime.progress(completed=batches, total=batches, phase="loaded")

    print(json.dumps({"batches": batches, "rows": 24000, "status": "published"}, sort_keys=True))


if __name__ == "__main__":
    main()
