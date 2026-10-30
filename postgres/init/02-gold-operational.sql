-- Gold table consumed by the US-03 stockout-risk report.
-- Idempotent: it is safe to run on every database initialization.
--
-- To apply it to an existing volume without recreating the database:
--   docker compose exec -T postgres psql -U pos_user -d merca_pos < postgres/init/02-gold-operational.sql

CREATE TABLE IF NOT EXISTS gold_stockout_risk (
    store_id            VARCHAR(64) NOT NULL,
    product_id          VARCHAR(64) NOT NULL,
    available_stock     NUMERIC(14, 2),
    avg_daily_sales     NUMERIC(14, 2),
    days_until_stockout NUMERIC(10, 1),
    threshold_days      INTEGER NOT NULL DEFAULT 3,
    at_risk             BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_gold_stockout_risk_at_risk
    ON gold_stockout_risk (at_risk, days_until_stockout);
