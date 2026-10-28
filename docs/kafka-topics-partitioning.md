# SCRUM-113 - Design topics and partitioning strategy

## Objective
Define the Kafka topic(s), partitioning strategy, keying, and configuration needed for the MercaAndes order events to flow through Kafka as the backbone of the speed layer (US-02). This enables historical reprocessing ("time travel") and decouples producers from consumers at scale.

## Topic design

| Field | Value | Notes |
| --- | --- | --- |
| **Topic name** | `mercaandes.orders.events` | Descriptive, domain-aligned to orders. Used by the order event simulator (Marketplace API). |
| **Cleanup policy** | `delete` | Event stream with a time-based retention window (see SCRUM-115). |
| **Replication factor** | `1` | Local/dev environment (Docker). For higher environments, recommend >= 2. |
| **Number of partitions** | `3` | Balanced for local parallelism with Spark Structured Streaming. Adjustable based on throughput. |
| **Message format** | JSON | Compatible with current `JsonlEventProducerRepository` schema to avoid unnecessary breaking changes. |

## Keying / partitioning strategy

- **Key:** `store_id` (encoded as UTF-8 string)
- **Rationale:** Partitioning by store keeps ordering semantics per store/point of sale, distributes load across partitions, and aligns with the acceptance criteria ("partitioned Kafka topic (e.g., by store or event type)").
- **Value:** JSON object with the event fields produced by the simulator (see below).

### Event schema (value)

```json
{
  "event_id": "...",
  "order_id": "...",
  "event_type": "...",
  "sku": "...",
  "store_id": "...",
  "seller_id": "...",
  "country": "...",
  "quantity": 0,
  "unit_price": 0.0,
  "event_ts": "ISO-8601 timestamp",
  "arrival_ts": "ISO-8601 timestamp"
}
```

## Justification for US-02
- **Partitioned topic:** Satisfied by explicit partitions and keyed messages by `store_id`.
- **Decoupling:** Producers (Marketplace API simulator) and consumers (Spark Streaming) communicate through Kafka.
- **Reprocessing/time travel ready:** With configurable retention (SCRUM-115) and consumers able to read from `earliest`, events can be replayed on demand.
- **Scalability:** Multiple consumers/partitions allow parallel processing.

## Configuration references
Environment variables (defined in `.env.example`): `KAFKA_TOPIC_ORDERS`, `KAFKA_ORDERS_PARTITIONS`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_RETENTION_HOURS`.
