-- Gold seller scores produced by the PySpark batch job.
-- Keep this file idempotent so it can initialize new and existing databases.

-- Seed the score groups' geography (including Brazil for Olist) without
-- interpreting Brazilian state abbreviations as Andean country codes.
INSERT INTO public.dim_currency (currency_id, currency_code, currency_name, is_active)
VALUES (gen_random_uuid(), 'COP', 'Colombian Peso', TRUE),
       (gen_random_uuid(), 'PEN', 'Peruvian Sol', TRUE),
       (gen_random_uuid(), 'USD', 'US Dollar', TRUE),
       (gen_random_uuid(), 'BOB', 'Bolivian Boliviano', TRUE),
       (gen_random_uuid(), 'CLP', 'Chilean Peso', TRUE),
       (gen_random_uuid(), 'BRL', 'Brazilian Real', TRUE)
ON CONFLICT (currency_code) DO NOTHING;

INSERT INTO public.dim_country (country_id, country_code, country_name, currency_id)
SELECT gen_random_uuid(), country.country_code, country.country_name, currency.currency_id
FROM (VALUES ('CO', 'Colombia', 'COP'), ('PE', 'Peru', 'PEN'),
             ('EC', 'Ecuador', 'USD'), ('BO', 'Bolivia', 'BOB'),
             ('CL', 'Chile', 'CLP'), ('BR', 'Brazil', 'BRL'))
     AS country(country_code, country_name, currency_code)
JOIN public.dim_currency currency USING (currency_code)
ON CONFLICT (country_code) DO NOTHING;

-- One canonical simulated seller per country and seller type.
INSERT INTO public.dim_seller (seller_id, country_id, seller_name, external, is_active)
SELECT gen_random_uuid(),
       country.country_id,
       'Seller score group ' || country.country_code || ' ' || seller_type,
       seller_type = 'third_party',
       TRUE
FROM public.dim_country country
CROSS JOIN (VALUES ('own'), ('third_party')) AS types(seller_type)
WHERE country.country_code IN ('CO', 'PE', 'EC', 'BO', 'CL', 'BR')
  AND NOT EXISTS (
      SELECT 1 FROM public.dim_seller seller
      WHERE seller.country_id = country.country_id
        AND seller.seller_name = 'Seller score group ' || country.country_code || ' ' || seller_type
  );

-- Natural source keys are kept here and linked to the POS surrogate UUID.
CREATE TABLE IF NOT EXISTS public.dim_seller_source_key (
    source_system       VARCHAR(64) NOT NULL,
    source_seller_id    VARCHAR(256) NOT NULL,
    seller_id           UUID NOT NULL REFERENCES public.dim_seller(seller_id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, source_seller_id)
);

-- Idempotent Bronze landing table for Marketplace lifecycle events.
CREATE TABLE IF NOT EXISTS public.marketplace_seller_score_events (
    event_id            VARCHAR(256) PRIMARY KEY,
    order_id            VARCHAR(256) NOT NULL,
    event_type          VARCHAR(64) NOT NULL,
    source_seller_id    VARCHAR(256) NOT NULL,
    source_country      CHAR(2) NOT NULL,
    source_seller_type  VARCHAR(32) NOT NULL,
    event_ts            TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.marketplace_seller_score_events
    ADD COLUMN IF NOT EXISTS source_country CHAR(2) NOT NULL DEFAULT 'CO',
    ADD COLUMN IF NOT EXISTS source_seller_type VARCHAR(32) NOT NULL DEFAULT 'third_party';

CREATE INDEX IF NOT EXISTS ix_marketplace_seller_score_events_order
    ON public.marketplace_seller_score_events (order_id, source_seller_id);

-- Silver inputs are relational so Spark can read them through JDBC.
CREATE TABLE IF NOT EXISTS public.silver_seller_score_dispatch (
    dispatch_id         VARCHAR(256) PRIMARY KEY,
    source_system       VARCHAR(64) NOT NULL,
    source_seller_id    VARCHAR(256) NOT NULL,
    paid_at             TIMESTAMPTZ NOT NULL,
    shipped_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.silver_seller_score_reviews (
    review_id           VARCHAR(256) PRIMARY KEY,
    source_system       VARCHAR(64) NOT NULL,
    source_seller_id    VARCHAR(256) NOT NULL,
    review_score        INTEGER NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    reviewed_at         TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.silver_seller_score_inventory (
    observation_id      VARCHAR(256) PRIMARY KEY,
    source_system       VARCHAR(64) NOT NULL,
    source_seller_id    VARCHAR(256) NOT NULL,
    inventory_accurate  BOOLEAN NOT NULL,
    observed_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.silver_seller_score_cancellations (
    order_id            VARCHAR(256) PRIMARY KEY,
    source_system       VARCHAR(64) NOT NULL,
    source_seller_id    VARCHAR(256) NOT NULL,
    cancelled           BOOLEAN NOT NULL,
    cancellation_reason VARCHAR(128),
    cancelled_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_silver_seller_score_dispatch_source
    ON public.silver_seller_score_dispatch (source_system, source_seller_id);
CREATE INDEX IF NOT EXISTS ix_silver_seller_score_reviews_source
    ON public.silver_seller_score_reviews (source_system, source_seller_id);
CREATE INDEX IF NOT EXISTS ix_silver_seller_score_inventory_source
    ON public.silver_seller_score_inventory (source_system, source_seller_id);
CREATE INDEX IF NOT EXISTS ix_silver_seller_score_cancellations_source
    ON public.silver_seller_score_cancellations (source_system, source_seller_id);

CREATE TABLE IF NOT EXISTS public.gold_seller_score (
    seller_id           VARCHAR(128) PRIMARY KEY,
    score               NUMERIC(5, 2) NOT NULL,
    dispatch_pct        NUMERIC(5, 2) NOT NULL,
    review_rating       NUMERIC(5, 2) NOT NULL,
    inventory_accuracy  NUMERIC(5, 2) NOT NULL,
    cancellation_rate   NUMERIC(5, 2) NOT NULL,
    calculated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.gold_seller_score_history (
    seller_id           VARCHAR(128) NOT NULL,
    week_start          DATE NOT NULL,
    score               NUMERIC(5, 2) NOT NULL,
    dispatch_pct        NUMERIC(5, 2) NOT NULL,
    review_rating       NUMERIC(5, 2) NOT NULL,
    inventory_accuracy NUMERIC(5, 2) NOT NULL,
    cancellation_rate   NUMERIC(5, 2) NOT NULL,
    calculated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (seller_id, week_start)
);

CREATE TABLE IF NOT EXISTS public.gold_seller_status (
    seller_id   VARCHAR(128) PRIMARY KEY,
    status      VARCHAR(32) NOT NULL DEFAULT 'active',
    reason      TEXT NOT NULL DEFAULT 'active',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
