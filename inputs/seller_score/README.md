# Seller score relational Silver contract

The seller scoring job reads these PostgreSQL Silver tables through JDBC. No
Parquet files need to be created or copied manually.

## Seller identity

`public.dim_seller_source_key` maps each source's natural ID to a surrogate UUID
in `public.dim_seller`:

| Column | Meaning |
| --- | --- |
| `source_system` | Source name: `olist`, `marketplace`, or `inventory_api`. |
| `source_seller_id` | Seller ID assigned by that source. |
| `seller_id` | Surrogate UUID consumed by Gold and the sellers API. |

Because the generated catalogs do not share a reliable seller identity, the
demo groups sellers by `(country, seller_type)` and maps source IDs to the same
group UUID. Several API seller IDs therefore return the same score. This is a
demo rule, not production identity resolution. Olist is mapped to the Brazilian
third-party group; its state abbreviation is never treated as a country code.

## Silver input tables

| Table | Required columns |
| --- | --- |
| `silver_seller_score_dispatch` | `dispatch_id`, `source_system`, `source_seller_id`, `paid_at`, `shipped_at` |
| `silver_seller_score_reviews` | `review_id`, `source_system`, `source_seller_id`, `review_score`, `reviewed_at` |
| `silver_seller_score_inventory` | `observation_id`, `source_system`, `source_seller_id`, `inventory_accurate`, `observed_at` |
| `silver_seller_score_cancellations` | `order_id`, `source_system`, `source_seller_id`, `cancelled`, `cancellation_reason`, `cancelled_at` |

Dispatch is on time when shipment occurs no more than 24 hours after payment.
Reviews use a 1–5 scale. The Inventory API generates quantities but has no
historical counted quantity or stockout label. To keep the demo runnable,
Airflow creates one stable simulated accuracy observation per API seller ID;
nine out of ten are marked accurate using a repeatable hash rule. This is not a
measurement of RN-09 against real order outcomes.

## How rows are prepared

Infrastructure creates the tables idempotently. The weekly DAG consumes
Marketplace events from Kafka, loads Olist reviews from the existing historical
CSV files, maps source IDs to country/type groups, and prepares simulated
inventory accuracy. Olist reviews are attributed only when an order has one
seller. If Marketplace has emitted no events, Airflow creates repeatable sample
dispatch and cancellation rows so the demo can calculate all four components.
