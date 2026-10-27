#!/bin/bash
# Kafka topic initialization for MercaAndes speed layer
# Creates topics with explicit partitions and retention for auditability

set -e

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"
TOPIC_ORDERS="${KAFKA_TOPIC_ORDERS:-mercaandes.orders.events}"
PARTITIONS="${KAFKA_ORDERS_PARTITIONS:-3}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
RETENTION_HOURS="${KAFKA_RETENTION_HOURS:-168}"
RETENTION_MS=$((RETENTION_HOURS * 3600 * 1000))

echo "Waiting for Kafka to be ready at ${BOOTSTRAP_SERVER}..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP_SERVER}" > /dev/null 2>&1; do
  echo "Kafka not ready yet, retrying in 5s..."
  sleep 5
done
echo "Kafka is ready."

echo "Creating topic ${TOPIC_ORDERS} if not exists (partitions=${PARTITIONS}, rf=${REPLICATION_FACTOR}, retention.ms=${RETENTION_MS})..."
kafka-topics --create \
  --if-not-exists \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${TOPIC_ORDERS}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}" \
  --config cleanup.policy=delete \
  --config retention.ms="${RETENTION_MS}"

echo "Describing topic ${TOPIC_ORDERS}:"
kafka-topics --describe --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC_ORDERS}"
