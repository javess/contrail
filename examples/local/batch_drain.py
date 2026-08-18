"""Finite workload whose serialized result drain outlives parallel compute."""

from __future__ import annotations

import json
import time

from runtime_tools import runtime


def main() -> None:
    total = 100
    with runtime.run("batch-drain", total_work=total):
        runtime.progress(completed=0, total=total, series="items")
        with runtime.stage("compute", phase="compute", concurrency=4):
            time.sleep(0.01)
            runtime.progress(completed=80, total=total, series="items")
        with runtime.stage("result-drain", phase="draining", concurrency=1):
            for completed in (85, 90, 95, 100):
                time.sleep(0.01)
                runtime.progress(completed=completed, total=total, series="items")

    print(json.dumps({"completed": total}, sort_keys=True))


if __name__ == "__main__":
    main()
