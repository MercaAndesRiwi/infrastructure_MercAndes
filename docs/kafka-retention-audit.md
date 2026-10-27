# SCRUM-115 - Kafka topic retention configuration (Audit)

## Objective
Explicitly configure and document topic retention for `mercaandes.orders.events` so that the platform supports historical reprocessing ("time travel") and passes the US-02 audit requirement.

## Configured values

| Parameter | Value | Notes |
| --- | --- | --- |
| **Topic** | `mercaandes.orders.events` | Order events from Marketplace API simulator |
| **cleanup.policy** | `delete` | Time-based retention (log segments deleted when older than retention) |
| **Retention (hours)** | `168` | 7 days |
| **Retention (ms)** | `604800000` | `168 * 3600 * 1000` |
| **Partitions** | `3` | See SCRUM-113 |
| **Replication factor** | `1` | Local/dev |

These values are controlled via environment variables in `infrastructure_MercAndes/.env` / `.env.example`:
- `KAFKA_RETENTION_HOURS`
- `KAFKA_TOPIC_ORDERS`
- `KAFKA_ORDERS_PARTITIONS`
- `KAFKA_REPLICATION_FACTOR`

## How to verify (audit)

### Option 1: Kafka UI
1. Open `http://localhost:9000` (Kafdrop)
2. Select topic `mercaandes.orders.events`
3. Check partitions and configuration (look for `retention.ms` and `cleanup.policy`)

### Option 2: CLI (inside kafka container)
```bash
# Describe topic
docker exec -it platform-kafka kafka-topics --describe --bootstrap-server kafka:29092 --topic mercaandes.orders.events

# Inspect topic configs
docker exec -it platform-kafka kafka-configs --describe --bootstrap-server kafka:29092 --entity-type topics --entity-name mercaandes.orders.events
```

### Option 3: From host with kafka tools
```bash
kafka-topics --describe --bootstrap-server localhost:9092 --topic mercaandes.orders.events
kafka-configs --describe --bootstrap-server localhost:9092 --entity-type topics --entity-name mercaandes.orders.events
```

## Reprocessing / "time travel"
To reprocess historical data (e.g. after a cleanup/error):
1. Ensure messages are still within retention window (older than `retention.ms` may be purged).
2. For Spark Structured Streaming consumers, reset by removing/renaming the checkpoint directory used by the streaming job (or use a new checkpointLocation).
3. Restart the consumer with `startingOffsets = "earliest"`. It will read from the oldest available offset in retained segments.
4. Validate with Kafka UI/offsets.

**Note:** Once segments age past retention, data is deleted and cannot be reprocessed - hence the audit of retention values is required.
