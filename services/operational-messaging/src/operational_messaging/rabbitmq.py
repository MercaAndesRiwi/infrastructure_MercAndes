"""RabbitMQ connection, topology, publisher and manual-ACK consumer."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from typing import Any

import pika

from operational_messaging.config import (
    ALL_QUEUES,
    DEAD_LETTER_EXCHANGE,
    MAX_RETRIES,
    OPERATIONAL_EXCHANGE,
    RETRY_DELAY_MS,
    dead_letter_queue_name,
    retry_queue_name,
)


def connection_parameters() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest_user"),
        os.getenv("RABBITMQ_PASSWORD", ""),
    )
    return pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    """Create the durable exchanges, queues, retry queues and DLQs."""
    channel.exchange_declare(OPERATIONAL_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(DEAD_LETTER_EXCHANGE, exchange_type="direct", durable=True)

    for queue in ALL_QUEUES:
        channel.queue_declare(
            queue=queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key": dead_letter_queue_name(queue),
            },
        )
        channel.queue_bind(queue=queue, exchange=OPERATIONAL_EXCHANGE, routing_key=queue)

        retry_queue = retry_queue_name(queue)
        channel.queue_declare(
            queue=retry_queue,
            durable=True,
            arguments={
                "x-message-ttl": RETRY_DELAY_MS,
                "x-dead-letter-exchange": OPERATIONAL_EXCHANGE,
                "x-dead-letter-routing-key": queue,
            },
        )
        channel.queue_bind(queue=retry_queue, exchange=OPERATIONAL_EXCHANGE, routing_key=retry_queue)

        dead_queue = dead_letter_queue_name(queue)
        channel.queue_declare(queue=dead_queue, durable=True)
        channel.queue_bind(queue=dead_queue, exchange=DEAD_LETTER_EXCHANGE, routing_key=dead_queue)


def publish_message(
    queue: str,
    message: dict[str, Any],
    *,
    retry_count: int = 0,
    retry: bool = False,
) -> None:
    """Publish one persistent message and wait for the broker confirmation."""
    publish_messages(queue, [message], retry_count=retry_count, retry=retry)


def publish_messages(
    queue: str,
    messages: list[dict[str, Any]],
    *,
    retry_count: int = 0,
    retry: bool = False,
) -> None:
    """Publish a batch over one confirmed connection."""
    if queue not in ALL_QUEUES:
        raise ValueError(f"Unsupported operational queue: {queue}")
    if not all(isinstance(message, dict) for message in messages):
        raise ValueError("Operational messages must be JSON objects")

    connection = pika.BlockingConnection(connection_parameters())
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        routing_key = retry_queue_name(queue) if retry else queue
        for message in messages:
            confirmed = channel.basic_publish(
                exchange=OPERATIONAL_EXCHANGE,
                routing_key=routing_key,
                body=json.dumps(message, default=str).encode("utf-8"),
                mandatory=True,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=message.get("message_id"),
                    headers={"x-retry-count": retry_count},
                ),
            )
            if confirmed is False:
                raise RuntimeError("RabbitMQ did not confirm the message")
    finally:
        connection.close()


def publish_dead_letter(queue: str, body: bytes, headers: dict[str, Any] | None) -> None:
    """Move a failed message to its durable dead-letter queue."""
    connection = pika.BlockingConnection(connection_parameters())
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        confirmed = channel.basic_publish(
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key=dead_letter_queue_name(queue),
            body=body,
            mandatory=True,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                headers=headers or {},
            ),
        )
        if confirmed is False:
            raise RuntimeError("RabbitMQ did not confirm the dead-letter message")
    finally:
        connection.close()


def next_action(retry_count: int) -> str:
    """Return ``retry`` while attempts remain, otherwise ``dead_letter``."""
    return "retry" if retry_count < MAX_RETRIES else "dead_letter"


def consume_queue(queue: str, process: Callable[[dict], None]) -> None:
    """Consume one queue, ACK only after success, and retry failures three times."""
    while True:
        connection = None
        try:
            connection = pika.BlockingConnection(connection_parameters())
            channel = connection.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=1)

            def handle_message(ch, method, properties, body):
                headers = properties.headers or {}
                try:
                    retry_count = int(headers.get("x-retry-count", 0))
                except (TypeError, ValueError):
                    retry_count = 0

                try:
                    message = json.loads(body.decode("utf-8"))
                    if not isinstance(message, dict):
                        raise ValueError("Operational messages must be JSON objects")
                    process(message)
                except Exception as exc:
                    print(f"[operational-worker] {queue} failed: {exc}")
                    handle_failure(ch, method, queue, body, headers, retry_count)
                    return

                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queue, on_message_callback=handle_message, auto_ack=False)
            print(f"[operational-worker] consuming {queue} with manual ACK")
            channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as exc:
            print(f"[operational-worker] RabbitMQ unavailable: {exc}; retrying in 5 seconds")
            time.sleep(5)
        finally:
            if connection is not None and connection.is_open:
                connection.close()


def handle_failure(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    method: Any,
    queue: str,
    body: bytes,
    headers: dict[str, Any],
    retry_count: int,
) -> None:
    """Retry a failed delivery or move it to the dead-letter queue."""
    try:
        if next_action(retry_count) == "retry":
            try:
                retry_message = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                retry_message = None

            if not isinstance(retry_message, dict):
                publish_dead_letter(queue, body, headers)
                print(f"[operational-worker] invalid payload moved to {queue}.dlq")
            else:
                publish_message(queue, retry_message, retry_count=retry_count + 1, retry=True)
                print(f"[operational-worker] retry {retry_count + 1}/{MAX_RETRIES} for {queue}")
        else:
            publish_dead_letter(queue, body, headers)
            print(f"[operational-worker] moved message to {queue}.dlq")

        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as publish_error:
        print(f"[operational-worker] retry publish failed: {publish_error}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
