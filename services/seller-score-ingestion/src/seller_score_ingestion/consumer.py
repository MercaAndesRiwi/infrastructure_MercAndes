"""Consume Marketplace lifecycle events and materialize seller-score Silver rows."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

def normalize_event(payload: bytes) -> dict:
    """Validate and normalize the event fields used by the scoring tables."""
    event = json.loads(payload.decode("utf-8"))
    required = ("event_id", "order_id", "event_type", "seller_id", "event_ts")
    missing = [field for field in required if not event.get(field)]
    if missing:
        raise ValueError("Marketplace event is missing: " + ", ".join(missing))

    event_ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=timezone.utc)

    return {
        "event_id": str(event["event_id"]),
        "order_id": str(event["order_id"]),
        "event_type": str(event["event_type"]).lower(),
        "source_seller_id": str(event["seller_id"]),
        "source_country": str(event.get("country", "CO")).upper(),
        "source_seller_type": (
            "own" if any(marker in str(event["seller_id"]).upper() for marker in ("-FIS-", "-ECOM", "-MKT")) else "third_party"
        ),
        "event_ts": event_ts,
    }


def persist_events(connection, events: list[dict]) -> None:
    """Store events idempotently and update the two Marketplace Silver tables."""
    if not events:
        return

    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO public.marketplace_seller_score_events
                    (event_id, order_id, event_type, source_seller_id,
                     source_country, source_seller_type, event_ts)
                VALUES (%(event_id)s, %(order_id)s, %(event_type)s,
                        %(source_seller_id)s, %(source_country)s,
                        %(source_seller_type)s, %(event_ts)s)
                ON CONFLICT (event_id) DO UPDATE SET
                    source_country = EXCLUDED.source_country,
                    source_seller_type = EXCLUDED.source_seller_type
                """,
                events,
            )
            cursor.execute(
                """
                WITH order_events AS (
                    SELECT order_id, source_seller_id,
                           MIN(event_ts) FILTER (WHERE event_type = 'paid') AS paid_at,
                           MIN(event_ts) FILTER (WHERE event_type = 'shipped') AS shipped_at
                    FROM public.marketplace_seller_score_events
                    GROUP BY order_id, source_seller_id
                )
                INSERT INTO public.silver_seller_score_dispatch
                    (dispatch_id, source_system, source_seller_id, paid_at, shipped_at)
                SELECT order_id, 'marketplace', source_seller_id, paid_at, shipped_at
                FROM order_events
                WHERE paid_at IS NOT NULL AND shipped_at IS NOT NULL
                ON CONFLICT (dispatch_id) DO UPDATE SET
                    source_seller_id = EXCLUDED.source_seller_id,
                    paid_at = EXCLUDED.paid_at,
                    shipped_at = EXCLUDED.shipped_at
                """
            )
            cursor.execute(
                """
                WITH order_events AS (
                    SELECT order_id, source_seller_id,
                           BOOL_OR(event_type = 'cancelled') AS cancelled,
                           MIN(event_ts) FILTER (WHERE event_type = 'cancelled') AS cancelled_at,
                           BOOL_OR(event_type IN ('delivered', 'cancelled', 'returned')) AS is_final
                    FROM public.marketplace_seller_score_events
                    GROUP BY order_id, source_seller_id
                )
                INSERT INTO public.silver_seller_score_cancellations
                    (order_id, source_system, source_seller_id, cancelled, cancellation_reason, cancelled_at)
                SELECT order_id, 'marketplace', source_seller_id, cancelled, NULL, cancelled_at
                FROM order_events
                WHERE is_final
                ON CONFLICT (order_id) DO UPDATE SET
                    source_seller_id = EXCLUDED.source_seller_id,
                    cancelled = EXCLUDED.cancelled,
                    cancellation_reason = EXCLUDED.cancellation_reason,
                    cancelled_at = EXCLUDED.cancelled_at
                """
            )


def run_consumer() -> int:
    """Consume until the topic has been idle for the configured interval."""
    import psycopg
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC_ORDERS", "mercaandes.orders.events"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        group_id=os.getenv("KAFKA_SELLER_SCORE_GROUP", "seller-score-silver"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
        value_deserializer=lambda value: value,
    )
    connection = psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    idle_timeout = int(os.getenv("KAFKA_IDLE_TIMEOUT_SECONDS", "10"))
    idle_since = time.monotonic()
    consumed = 0
    try:
        while time.monotonic() - idle_since < idle_timeout:
            records = consumer.poll(timeout_ms=1000, max_records=500)
            events = [
                normalize_event(record.value)
                for partition_records in records.values()
                for record in partition_records
            ]
            if not events:
                continue

            persist_events(connection, events)
            consumer.commit()
            consumed += len(events)
            idle_since = time.monotonic()
    finally:
        connection.close()
        consumer.close()

    print(f"[SELLER SCORE INGESTION] Consumed {consumed} Marketplace events.")
    return consumed


if __name__ == "__main__":
    run_consumer()
