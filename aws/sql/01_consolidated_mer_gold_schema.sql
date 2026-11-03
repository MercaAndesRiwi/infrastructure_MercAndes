-- ==============================================================================
-- MercaAndes Data Platform - Consolidated MER DDL Script (SCRUM-97 / US-05)
-- Database: PostgreSQL 16 (RDS Gold / Serving Layer)
-- Consolidated Star/Snowflake Schema:
--   - Dimensions: dim_currency, dim_country, dim_geolocation, dim_customer,
--                 dim_branch, dim_seller, dim_product_category, dim_product,
--                 dim_source, dim_exchange_rate, dim_holiday_type, dim_holiday,
--                 dim_holiday_type_bridge
--   - Facts:      fact_sale, fact_sale_item, fact_order, fact_order_item
--   - Gold Serv:  gold_stockout_risk, hot_inventory_risk
--   - Schemas:    Operational country POS schemas (colombia, peru, ecuador, bolivia, chile)
-- ==============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ==============================================================================
-- 1. CENTRALIZED SHARED DIMENSIONS (public schema)
-- ==============================================================================

-- 1.1 Currencies
CREATE TABLE IF NOT EXISTS public.dim_currency (
    currency_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    currency_code CHAR(3) NOT NULL UNIQUE,
    currency_name VARCHAR(50) NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE
);

-- 1.2 Countries
CREATE TABLE IF NOT EXISTS public.dim_country (
    country_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    country_code   CHAR(2) NOT NULL UNIQUE,
    country_name   VARCHAR(80) NOT NULL,
    currency_id    UUID NOT NULL REFERENCES public.dim_currency(currency_id),
    is_operational BOOLEAN NOT NULL DEFAULT TRUE
);

-- 1.3 Geolocation
CREATE TABLE IF NOT EXISTS public.dim_geolocation (
    geolocation_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    zip_code_prefix VARCHAR(15) NOT NULL,
    latitude        NUMERIC(10, 8),
    longitude       NUMERIC(11, 8),
    city            VARCHAR(100) NOT NULL,
    state           VARCHAR(80) NOT NULL,
    country_id      UUID NOT NULL REFERENCES public.dim_country(country_id)
);
CREATE INDEX IF NOT EXISTS idx_dim_geo_zip ON public.dim_geolocation(zip_code_prefix);

-- 1.4 Customers
CREATE TABLE IF NOT EXISTS public.dim_customer (
    customer_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_token  VARCHAR(128) NOT NULL UNIQUE,
    country_id      UUID NOT NULL REFERENCES public.dim_country(country_id),
    geolocation_id  UUID REFERENCES public.dim_geolocation(geolocation_id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_dim_customer_token ON public.dim_customer(customer_token);

-- 1.5 Physical Branches
CREATE TABLE IF NOT EXISTS public.dim_branch (
    branch_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    country_id     UUID NOT NULL REFERENCES public.dim_country(country_id),
    branch_name    VARCHAR(120) NOT NULL,
    address        VARCHAR(200) NOT NULL,
    geolocation_id UUID REFERENCES public.dim_geolocation(geolocation_id),
    is_active      BOOLEAN NOT NULL DEFAULT TRUE
);

-- 1.6 Sellers (Internal & External Marketplace Sellers)
CREATE TABLE IF NOT EXISTS public.dim_seller (
    seller_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    country_id  UUID NOT NULL REFERENCES public.dim_country(country_id),
    branch_id   UUID REFERENCES public.dim_branch(branch_id),
    seller_name VARCHAR(150) NOT NULL,
    external    BOOLEAN NOT NULL DEFAULT FALSE,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE
);

-- 1.7 Product Categories
CREATE TABLE IF NOT EXISTS public.dim_product_category (
    category_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_name VARCHAR(100) NOT NULL UNIQUE
);

-- 1.8 Products (SKU Master)
CREATE TABLE IF NOT EXISTS public.dim_product (
    product_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku          VARCHAR(100) NOT NULL UNIQUE,
    category_id  UUID REFERENCES public.dim_product_category(category_id),
    product_name VARCHAR(150) NOT NULL,
    brand        VARCHAR(100),
    is_active    BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_dim_product_sku ON public.dim_product(sku);

-- 1.9 Data Sources
CREATE TABLE IF NOT EXISTS public.dim_source (
    source_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name VARCHAR(80) NOT NULL UNIQUE,
    description TEXT
);

-- 1.10 Exchange Rates
CREATE TABLE IF NOT EXISTS public.dim_exchange_rate (
    exchange_rate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    currency_id      UUID NOT NULL REFERENCES public.dim_currency(currency_id),
    source_id        UUID NOT NULL REFERENCES public.dim_source(source_id),
    rate_date        DATE NOT NULL,
    rate             NUMERIC(18, 8) NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_exchange_rate_currency_date UNIQUE (currency_id, source_id, rate_date)
);

-- 1.11 Holiday Types
CREATE TABLE IF NOT EXISTS public.dim_holiday_type (
    holiday_type_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type_name       VARCHAR(80) NOT NULL UNIQUE
);

-- 1.12 Holidays
CREATE TABLE IF NOT EXISTS public.dim_holiday (
    holiday_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    country_id      UUID NOT NULL REFERENCES public.dim_country(country_id),
    holiday_type_id UUID NOT NULL REFERENCES public.dim_holiday_type(holiday_type_id),
    holiday_date    DATE NOT NULL,
    holiday_name    VARCHAR(150) NOT NULL,
    CONSTRAINT uq_dim_holiday_country_date UNIQUE (country_id, holiday_date, holiday_name)
);

-- 1.13 Holiday Type Bridge (Supporting multi-classification)
CREATE TABLE IF NOT EXISTS public.dim_holiday_type_bridge (
    holiday_id      UUID NOT NULL REFERENCES public.dim_holiday(holiday_id) ON DELETE CASCADE,
    holiday_type_id UUID NOT NULL REFERENCES public.dim_holiday_type(holiday_type_id) ON DELETE CASCADE,
    description     VARCHAR(120),
    PRIMARY KEY (holiday_id, holiday_type_id)
);

-- ==============================================================================
-- 2. FACT TABLES (CENTRALIZED SALES & ORDER ITEMS)
-- ==============================================================================

-- 2.1 Fact Sale (POS Transactions)
CREATE TABLE IF NOT EXISTS public.fact_sale (
    sale_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    branch_id       UUID NOT NULL REFERENCES public.dim_branch(branch_id),
    seller_id       UUID NOT NULL REFERENCES public.dim_seller(seller_id),
    customer_id     UUID REFERENCES public.dim_customer(customer_id),
    sale_datetime   TIMESTAMPTZ NOT NULL,
    currency_code   CHAR(3) NOT NULL,
    gross_amount    NUMERIC(14, 2) NOT NULL,
    discount_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    tax_amount      NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    net_amount      NUMERIC(14, 2) GENERATED ALWAYS AS (gross_amount - discount_amount) STORED,
    status          VARCHAR(30) NOT NULL DEFAULT 'COMPLETED'
);
CREATE INDEX IF NOT EXISTS idx_fact_sale_date ON public.fact_sale(sale_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_sale_branch ON public.fact_sale(branch_id);

-- 2.2 Fact Sale Items
CREATE TABLE IF NOT EXISTS public.fact_sale_item (
    sale_item_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sale_id         UUID NOT NULL REFERENCES public.fact_sale(sale_id) ON DELETE CASCADE,
    product_id      UUID NOT NULL REFERENCES public.dim_product(product_id),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(14, 2) NOT NULL,
    discount_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    total_amount    NUMERIC(14, 2) GENERATED ALWAYS AS ((quantity * unit_price) - discount_amount) STORED
);
CREATE INDEX IF NOT EXISTS idx_fact_sale_item_sale ON public.fact_sale_item(sale_id);
CREATE INDEX IF NOT EXISTS idx_fact_sale_item_prod ON public.fact_sale_item(product_id);

-- ==============================================================================
-- 3. GOLD OPERATIONAL & SERVING LAYER TABLES
-- ==============================================================================

-- 3.1 Stockout Risk (Daily Batch Rule: Silver -> Gold)
CREATE TABLE IF NOT EXISTS public.gold_stockout_risk (
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
    ON public.gold_stockout_risk (at_risk, days_until_stockout);

-- 3.2 Hot Inventory Risk (Speed Layer: Real-time micro-batches)
CREATE TABLE IF NOT EXISTS public.hot_inventory_risk (
    store_id            VARCHAR(64) NOT NULL,
    sku                 VARCHAR(64) NOT NULL,
    approx_qty_on_hand  INTEGER DEFAULT 0,
    approx_reserved     INTEGER DEFAULT 0,
    approx_in_transit   INTEGER DEFAULT 0,
    approx_available    INTEGER DEFAULT 0,
    approx_risk_score   NUMERIC(10, 4) DEFAULT 0,
    last_event_id       VARCHAR(128),
    last_event_type     VARCHAR(32),
    last_event_ts       TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reconciled_at       TIMESTAMPTZ,
    is_reconciled       BOOLEAN DEFAULT FALSE,
    source_batch_id     VARCHAR(64),
    PRIMARY KEY (store_id, sku)
);
CREATE INDEX IF NOT EXISTS idx_hot_inventory_risk_updated ON public.hot_inventory_risk(updated_at);

-- ==============================================================================
-- 4. OPERATIONAL COUNTRY SCHEMAS (Multi-Tenancy POS)
-- ==============================================================================

DO $$
DECLARE
    schema_name TEXT;
    schemas TEXT[] := ARRAY['colombia', 'peru', 'ecuador', 'bolivia', 'chile'];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);

        -- Country-specific POS Sales table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.pos_sale (
                sale_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                branch_code     VARCHAR(50) NOT NULL,
                seller_code     VARCHAR(50) NOT NULL,
                sale_datetime   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                currency_code   CHAR(3) NOT NULL,
                gross_amount    NUMERIC(14, 2) NOT NULL,
                tax_amount      NUMERIC(14, 2) DEFAULT 0.00,
                status          VARCHAR(30) NOT NULL DEFAULT ''COMPLETED'',
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )', schema_name);

        -- Country-specific POS Sale Item table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.pos_sale_item (
                sale_item_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                sale_id         UUID NOT NULL REFERENCES %I.pos_sale(sale_id) ON DELETE CASCADE,
                sku             VARCHAR(100) NOT NULL,
                quantity        INTEGER NOT NULL CHECK (quantity > 0),
                unit_price      NUMERIC(14, 2) NOT NULL,
                discount_amount NUMERIC(14, 2) DEFAULT 0.00
            )', schema_name, schema_name);

        -- Indexes per country schema
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_sale_dt ON %I.pos_sale(sale_datetime)', schema_name, schema_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_item_sale ON %I.pos_sale_item(sale_id)', schema_name, schema_name);
    END LOOP;
END $$;
