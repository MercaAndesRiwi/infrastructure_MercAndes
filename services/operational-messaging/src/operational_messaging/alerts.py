"""Consumer entrypoint for data-quality and stockout alerts."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

from operational_messaging.config import ALERT_QUEUES
from operational_messaging.rabbitmq import consume_queue


def process_alert(message: dict) -> None:
    """Validate an alert and append it to the durable alert log."""
    if not message.get("message_type") or not isinstance(message.get("payload"), dict):
        raise ValueError("Alert must include message_type and an object payload")

    log_path = Path(os.getenv("ALERT_LOG_PATH", "/app/alerts/processed.jsonl"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as alert_log:
        alert_log.write(json.dumps(message, default=str) + "\n")
        alert_log.flush()
        os.fsync(alert_log.fileno())
    print(f"[alert] {message['message_type']}: {message['payload']}")


def run_consumer(queue: str) -> None:
    """Keep one queue consumer alive even after an unexpected error."""
    while True:
        try:
            consume_queue(queue, process_alert)
        except Exception as exc:
            print(f"[alert] {queue} consumer stopped: {exc}; restarting in 5 seconds")
            time.sleep(5)


def main() -> None:
    for queue in ALERT_QUEUES:
        threading.Thread(
            target=run_consumer,
            args=(queue,),
            name=f"consumer-{queue}",
            daemon=True,
        ).start()
    threading.Event().wait()


if __name__ == "__main__":
    main()
