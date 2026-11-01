#!/bin/bash
# Kafka topic initialization for MercaAndes speed layer
# Creates topics with explicit partitions and retention for auditability

set -e

# Strictly read Kafka configuration from environment variables without hardcoded defaults
BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-${KAFKA_BOOTSTRAP_SERVER}}"
TOPIC_ORDERS="${KAFKA_TOPIC_ORDERS}"
TOPIC_PROMOTIONS="${KAFKA_TOPIC_PROMOTIONS_WEEKLY:-${KAFKA_TOPIC_PROMOTIONS}}"
PARTITIONS="${KAFKA_ORDERS_PARTITIONS}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR}"
RETENTION_HOURS="${KAFKA_RETENTION_HOURS}"

if [ -z "${BOOTSTRAP_SERVER}" ] || [ -z "${TOPIC_ORDERS}" ] || [ -z "${TOPIC_PROMOTIONS}" ] || [ -z "${PARTITIONS}" ] || [ -z "${REPLICATION_FACTOR}" ] || [ -z "${RETENTION_HOURS}" ]; then
  echo "Error: Required Kafka environment variables are missing."
  echo "Expected: KAFKA_BOOTSTRAP_SERVERS (or KAFKA_BOOTSTRAP_SERVER), KAFKA_TOPIC_ORDERS, KAFKA_TOPIC_PROMOTIONS_WEEKLY, KAFKA_ORDERS_PARTITIONS, KAFKA_REPLICATION_FACTOR, KAFKA_RETENTION_HOURS."
  exit 1
fi
RETENTION_MS=$((RETENTION_HOURS * 3600 * 1000))

echo "Waiting for Kafka to be ready at ${BOOTSTRAP_SERVER}..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP_SERVER}" > /dev/null 2>&1; do
  echo "Kafka not ready yet, retrying in 5s..."
  sleep 5
done
echo "Kafka is ready."

# 1. Orders topic (Speed layer - US-02)
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

# 2. Weekly promotions topic (Commercial promotions stream - F7)
echo "Creating topic ${TOPIC_PROMOTIONS} if not exists (partitions=${PARTITIONS}, rf=${REPLICATION_FACTOR}, retention.ms=${RETENTION_MS})..."
kafka-topics --create \
  --if-not-exists \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${TOPIC_PROMOTIONS}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}" \
  --config cleanup.policy=delete \
  --config retention.ms="${RETENTION_MS}"

echo "Describing topic ${TOPIC_PROMOTIONS}:"
kafka-topics --describe --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC_PROMOTIONS}"
