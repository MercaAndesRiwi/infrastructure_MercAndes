"""Publish marked alerts for the durability and DLQ exercises."""

from __future__ import annotations

import argparse
from uuid import uuid4

from operational_messaging.config import ALERT_QUEUES, STOCKOUT_QUEUE
from operational_messaging.rabbitmq import publish_messages


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--queue", choices=ALERT_QUEUES, default=STOCKOUT_QUEUE)
    parser.add_argument("--fail", action="store_true", help="Publish an invalid alert to exercise the DLQ")
    args = parser.parse_args()

    run_id = str(uuid4())
    messages = [
        {
            "message_id": f"durability-{run_id}-{index}",
            "message_type": "DURABILITY_TEST",
            "payload": "invalid-payload" if args.fail else {"run_id": run_id, "sequence": index},
        }
        for index in range(args.count)
    ]
    publish_messages(args.queue, messages)
    print(f"Published {args.count} test alert(s); run_id={run_id}")


if __name__ == "__main__":
    main()
