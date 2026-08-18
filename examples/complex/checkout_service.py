"""Checkout workflow with retry amplification and a new risk dependency."""

from __future__ import annotations

import argparse
import json
import time

from runtime_tools import runtime


class RetryablePaymentError(RuntimeError):
    """Simulated gateway failure retained as operation evidence."""


def _authorize_payment(*, fail: bool, attempt: int) -> None:
    try:
        with runtime.stage("payment.authorize", attempt=attempt, concurrency=1):
            time.sleep(0.0004)
            if fail:
                raise RetryablePaymentError("gateway timeout")
    except RetryablePaymentError:
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", action="store_true")
    args = parser.parse_args()

    with runtime.run("checkout-request", total_work=5, order_items=4):
        with runtime.stage("validate-cart", concurrency=4):
            for _ in range(4):
                runtime.event(
                    "inventory.read",
                    kind="client.request",
                    **{"peer.service": "inventory-db"},
                )
            runtime.progress(completed=1, total=5, phase="validated")

        with runtime.stage("price-order", concurrency=2):
            runtime.event(
                "pricing.quote",
                kind="client.request",
                **{"peer.service": "pricing-service"},
            )
            runtime.progress(completed=2, total=5, phase="priced")

        if args.candidate:
            runtime.event(
                "risk.lookup",
                kind="client.request",
                **{"peer.service": "legacy-risk-db"},
            )
        attempts = 3 if args.candidate else 1
        for attempt in range(1, attempts + 1):
            _authorize_payment(fail=args.candidate and attempt < attempts, attempt=attempt)
        runtime.progress(completed=3, total=5, phase="authorized")

        with runtime.stage("reserve-inventory", concurrency=4):
            for _ in range(4):
                runtime.event(
                    "inventory.write",
                    kind="client.request",
                    **{"peer.service": "inventory-db"},
                )
            runtime.progress(completed=4, total=5, phase="reserved")

        with runtime.stage("confirm-order", concurrency=1):
            runtime.event(
                "order.write",
                kind="client.request",
                **{"peer.service": "orders-db"},
            )
            runtime.progress(completed=5, total=5, phase="confirmed")

    print(json.dumps({"order_id": "order-1042", "status": "confirmed"}, sort_keys=True))


if __name__ == "__main__":
    main()
