"""Small deterministic workload for the first RunDiff demonstration."""

from __future__ import annotations

import argparse
import json
import time

from runtime_tools import runtime


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--regression", action="store_true")
    args = parser.parse_args()
    write_count = 30 if args.regression else 3

    with runtime.run("local-pipeline", total_work=100):
        with runtime.stage("read"):
            rows = list(range(100))
        with runtime.stage("transform"):
            result = sum(rows)
        with runtime.stage("persist", concurrency=1):
            for _ in range(write_count):
                runtime.event(
                    "db.write",
                    kind="client.request",
                    **{"peer.service": "results-db"},
                )
                time.sleep(0.0005)
            if args.regression:
                runtime.event(
                    "metadata.lookup",
                    kind="client.request",
                    **{"peer.service": "metadata-db"},
                )
        runtime.progress(completed=100, total=100)

    print(json.dumps({"rows": len(rows), "checksum": result}, sort_keys=True))


if __name__ == "__main__":
    main()
