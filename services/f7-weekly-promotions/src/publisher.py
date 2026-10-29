"""Publishes F7 promotion events to Kafka and RabbitMQ with specific resource names.

Connects to:
  - Kafka topic: mercaandes.promotions.weekly
  - RabbitMQ queue: mercaandes.promotions.weekly.ingest
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import pika
from kafka import KafkaProducer


def _kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def _rabbitmq_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", ""),
        os.getenv("RABBITMQ_PASSWORD", ""),
    )
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
            credentials=credentials,
        )
    )


def publish_promotion_event(
    source: str,
    s3_key: str,
    filename: str,
    seed: int,
) -> str:
    """
    Publish a promotion ingestion event to:
      - Kafka topic: mercaandes.promotions.weekly
      - RabbitMQ queue: mercaandes.promotions.weekly.ingest

    Returns the generated event_id.
    """
    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "source": source,
        "s3_key": s3_key,
        "filename": filename,
        "seed": seed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # --- Kafka: specific topic ---
    producer = _kafka_producer()
    try:
        producer.send(os.getenv("KAFKA_TOPIC_PROMOTIONS", "mercaandes.promotions.weekly"), value=event)
        producer.flush()
    finally:
        producer.close()

    # --- RabbitMQ: specific queue ---
    connection = _rabbitmq_connection()
    try:
        channel = connection.channel()
        channel.queue_declare(
            queue=os.getenv("RABBITMQ_QUEUE_PROMO_INGEST", "mercaandes.promotions.weekly.ingest"),
            durable=True,
        )
        channel.basic_publish(
            exchange="",
            routing_key=os.getenv("RABBITMQ_QUEUE_PROMO_INGEST", "mercaandes.promotions.weekly.ingest"),
            body=json.dumps(event),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        connection.close()

    return event_id


def publish_to_rabbitmq(queue: str, message: dict) -> None:
    """
    Publish a generic message to a RabbitMQ queue.
    Used by EC2 (AWS) workers to report status.
    """
    connection = _rabbitmq_connection()
    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        connection.close()
