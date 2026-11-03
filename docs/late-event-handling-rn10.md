# Late Event Handling and Incremental Recalculation (RN-10)

- **Requirement:** RN-10 (Event deduplication and late event incorporation)
- **User Story:** *"As an analyst, I want a late event to automatically recalculate the aggregates for the affected day, so that the figures in Gold always reflect reality."*
- **Status:** Implemented / Ready for AWS Production
- **Scope:** Bronze -> Silver -> Gold Medallion Pipeline (MercaAndes Platform)

---

## 1. Business Context and the 72-Hour Limit

In omnichannel retail and marketplace platforms like MercaAndes, events from physical stores or online orders can arrive late due to network disconnections or offline queue retries.

### Why 72 hours?

1. **Financial and Operational Close:** Daily sales, commissions, and store metrics are considered finalized within 72 hours.
2. **Gold Layer Stability:** Allowing events older than 72 hours to automatically rewrite historical tables would distort closed financial reports and executive dashboards in Power BI.
3. **Audit and Exceptions:** Events arriving later than 72 hours require manual review and auditing, rather than silent automatic inclusion.

---

## 2. Event Latency Classification

Latency is the difference between arrival time (`arrival_ts` or ingestion time) and the original event time (`event_ts`):

$$
\text{delay} = \text{arrival\_ts} - \text{event\_ts}

$$


| Latency Range                                              | Classification       | Action in Silver                                                         | Action in Gold                                                                     | Alerts & Logs                                                           |
| :----------------------------------------------------------- | :--------------------- | :------------------------------------------------------------------------- | :----------------------------------------------------------------------------------- | :------------------------------------------------------------------------ |
| $\text{delay} \le 24\text{ hours}$                         | **On-Time**          | Ingested in the normal daily batch.                                      | Included in normal daily aggregates.                                               | Normal log.                                                             |
| $24\text{h} < \text{delay} \le 72\text{ hours}$ (e.g. 48h) | **Valid Late Event** | Ingested and marked as `processed_incorporated`.                         | **Triggers automatic incremental recalculation for the affected day's partition.** | CloudWatch log: `[RN-10][LATE_EVENT_DETECTED]`.                         |
| $\text{delay} > 72\text{ hours}$                           | **Late Exception**   | Marked as `exception_exceeded_72h`. **Excluded from normal aggregates.** | Gold aggregates are not altered.                                                   | **Persistent RabbitMQ alert to `alertas.calidad_datos`** + warning log. |

---

## 3. Exception Handling Protocol (> 72 hours)

When an event arrives more than 72 hours late:

1. **Database Isolation:**
   The event is updated with `status = 'exception_exceeded_72h'`, preventing it from being counted in regular sales figures.
2. **RabbitMQ Quality Alert:**
   The DAG publishes a persistent alert to exchange `mercaandes.operational` routed to queue `alertas.calidad_datos`:

   ```json
   {
     "message_id": "c1f72e90-...",
     "message_type": "DATA_QUALITY_LATE_EVENT_EXCEEDED",
     "created_at": "2026-11-03T08:00:00Z",
     "payload": {
       "source": "marketplace_events",
       "issue": "late_event_exceeded_72h",
       "event_id": "evt-75h-sample",
       "order_id": "ORD-2026-0099",
       "event_ts": "2026-10-31T05:00:00Z",
       "arrival_ts": "2026-11-03T08:00:00Z",
       "delay_hours": 75.0,
       "max_tolerance_hours": 72.0,
       "reason": "Late event exceeded 72-hour operational window; excluded from Gold aggregates.",
       "dag_run_id": "scheduled__2026-11-03T08:00:00+00:00"
     }
   }
   ```
3. **Storage & Audit:**
   The `operational-alerts-worker` appends the message to `/app/alerts/processed.jsonl`. If workers are restarting, RabbitMQ holds the alert safely in durable queues with a Dead-Letter Queue (DLQ).

---

## 4. Incremental Recalculation by Partition

To make sure **recalculation only touches the affected day**:

1. **Extract Affected Dates:**
   Airflow checks the incoming batch and finds the dates affected by late events ($\le 72$h):

   $$
   \text{affected\_partitions} = \{ \text{DATE}(event\_ts) \mid 24\text{h} \le \text{delay} \le 72\text{h} \}

   $$

   This list is passed through **Airflow XCom**.
2. **Partition Isolation on Amazon S3:**
   When running the recalculation job in PySpark:

   - Spark uses dynamic partition overwrite or writes to the specific date folder:
     `s3a://${S3_DATA_LAKE_BUCKET_NAME}/gold/daily_sales/order_date=YYYY-MM-DD/`
   - Previous and next days (`order_date = D-1` and `order_date = D+1`) remain untouched.
3. **Partition Isolation on RDS PostgreSQL:**
   The update statement strictly scopes the update by `order_date`:

   ```sql
   INSERT INTO public.gold_daily_sales (order_date, total_orders, total_net_sales_usd, recalculated_at)
   SELECT %s::date, COUNT(DISTINCT order_id), SUM(...), NOW()
   FROM marketplace_events
   WHERE event_timestamp::date = %s::date
   AND status <> 'exception_exceeded_72h'
   ON CONFLICT (order_date) DO UPDATE
   SET total_orders = EXCLUDED.total_orders,
       total_net_sales_usd = EXCLUDED.total_net_sales_usd,
       recalculated_at = NOW();
   ```

---

## 5. Verification Matrix


| Acceptance Criteria                                                   | Component                                                              | Evidence                                                                         |
| :---------------------------------------------------------------------- | :----------------------------------------------------------------------- | :--------------------------------------------------------------------------------- |
| **AC-1:** Event 48h late recalculates affected day in Silver -> Gold. | `bronze_to_silver_cleaning.py` + `calculate_daily_sales_aggregates.py` | Detects $\text{delay} = 48\text{h} \le 72\text{h}$, recalculates date partition. |
| **AC-2:** Event > 72h late is logged as an exception.                 | `late_event_detector.py` + `operational_messaging`                     | Marks `exception_exceeded_72h` and sends alert to RabbitMQ.                      |
| **AC-3:** Recalculation only touches the affected day's partition.    | `calculate_daily_sales_aggregates.py`                                  | Filters by `order_date == target_date`; other partitions stay unchanged.         |
| **AC-4:** Implement late event detection.                             | `late_event_detector.py`                                               | `classify_event_latency` function with 24h and 72h thresholds.                   |
| **AC-5:** Trigger incremental recalculation of affected partition.    | Task `rn_10_recalculate_gold_partitions` in Airflow                    | Takes dates from XCom and triggers recalculation.                                |
| **AC-6:** Document 72-hour limit and exception handling.              | This document (`late-event-handling-rn10.md`)                          | Clear business and technical explanation.                                        |
| **AC-7:** Automated test with simulated 48h and >72h event.           | `tests/test_late_event_recalculation.py`                               | Tested and passing with `pytest`.                                                |
