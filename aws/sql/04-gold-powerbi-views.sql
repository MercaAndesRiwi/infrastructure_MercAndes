-- ==============================================================================
-- MercaAndes Data Platform - Medallion Schema Architecture (Silver & Gold)
-- Database: PostgreSQL 16 (RDS Gold / Serving Layer)
-- Directly satisfies architectural schema isolation & the 7 Power BI questions:
--
--   ESQUEMA 'silver' (Datos curados, limpios y anonimizados):
--     1. silver.vw_customers_anonymized: Clientes con PII protegida (RN-11)
--     2. silver.vw_seller_dispatch: Despachos y trazabilidad logística
--     3. silver.vw_seller_reviews: Calificaciones de clientes
--     4. silver.vw_seller_inventory: Observaciones de exactitud de inventario
--     5. silver.vw_seller_cancellations: Cancelaciones operativas
--     6. silver.vw_marketplace_events: Eventos deduplicados <= 72h (RN-10)
--
--   ESQUEMA 'gold' (Capa Analítica Oficial para Power BI):
--     1. gold.vw_sales_consolidated: Ventas netas USD (RN-01)
--     2. gold.vw_stockout_risk_monitoring: Riesgo de quiebre y venta en riesgo USD (RN-04 / RF-05)
--     3. gold.vw_promotions_analysis: Promociones rechazadas y venta perdida (RN-05 / RF-04)
--     4. gold.vw_seller_performance: Distribución y evolución histórica de sellers (RN-07, RN-08 / RF-03)
--     5. gold.vw_returns_audit: Tasa de devolución y devoluciones > 30 días (RN-02, RN-06)
--     6. gold.vw_holiday_sales_impact: Impacto de festivos en ventas por país
--     7. gold.vw_data_quality_dashboard: Métricas de calidad por fuente (RN-10)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 0. IDEMPOTENT PREREQUISITES & SCHEMAS
-- ------------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

DO $$
DECLARE
    schema_name TEXT;
    schemas TEXT[] := ARRAY['colombia', 'peru', 'ecuador', 'bolivia', 'chile'];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.fact_sale (
                sale_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                branch_id       UUID,
                seller_id       UUID,
                customer_id     UUID,
                sale_datetime   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                currency_code   CHAR(3) NOT NULL DEFAULT ''COP'',
                gross_amount    NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
                discount_amount NUMERIC(14, 2) DEFAULT 0.00,
                tax_amount      NUMERIC(14, 2) DEFAULT 0.00,
                net_amount      NUMERIC(14, 2) GENERATED ALWAYS AS (gross_amount - discount_amount) STORED,
                status          VARCHAR(30) NOT NULL DEFAULT ''COMPLETED''
            )', schema_name);

        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.fact_sale_item (
                sale_item_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                sale_id         UUID REFERENCES %I.fact_sale(sale_id) ON DELETE CASCADE,
                product_id      UUID,
                quantity        INTEGER NOT NULL DEFAULT 1,
                unit_price      NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
                discount_amount NUMERIC(14, 2) DEFAULT 0.00,
                total_amount    NUMERIC(14, 2) GENERATED ALWAYS AS ((quantity * unit_price) - discount_amount) STORED
            )', schema_name, schema_name);
    END LOOP;
END $$;

-- Ensure central operational tables exist
CREATE TABLE IF NOT EXISTS public.gold_promotions_audit (
    promotion_id            VARCHAR(64) PRIMARY KEY,
    sku                     VARCHAR(64) NOT NULL,
    product_name            VARCHAR(150),
    category_name           VARCHAR(100),
    store_id                VARCHAR(64),
    discount_pct            NUMERIC(5, 2) NOT NULL DEFAULT 10.0,
    available_stock         NUMERIC(12, 2) NOT NULL DEFAULT 0.0,
    avg_daily_sales         NUMERIC(12, 2) NOT NULL DEFAULT 0.0,
    coverage_days           NUMERIC(10, 1),
    status                  VARCHAR(32) NOT NULL,
    rejection_reason        TEXT,
    projected_lost_sales_usd NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    evaluated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.gold_stockout_risk (
    store_id            VARCHAR(64) NOT NULL,
    product_id          VARCHAR(64) NOT NULL,
    available_stock     NUMERIC(14, 2) DEFAULT 0.00,
    avg_daily_sales     NUMERIC(14, 2) DEFAULT 0.00,
    days_until_stockout NUMERIC(10, 1),
    threshold_days      INTEGER NOT NULL DEFAULT 3,
    at_risk             BOOLEAN NOT NULL DEFAULT FALSE,
    sales_at_risk_usd   NUMERIC(14, 2) DEFAULT 0.00,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, product_id)
);

ALTER TABLE public.gold_stockout_risk
    ADD COLUMN IF NOT EXISTS sales_at_risk_usd NUMERIC(14, 2) DEFAULT 0.00;

CREATE TABLE IF NOT EXISTS public.data_quality_metrics (
    metric_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    load_id                     VARCHAR(256) NOT NULL,
    source_system               VARCHAR(64) NOT NULL,
    records_processed           INTEGER NOT NULL DEFAULT 0,
    duplicates_detected         INTEGER NOT NULL DEFAULT 0,
    late_arrivals_incorporated  INTEGER NOT NULL DEFAULT 0,
    rejections_exceeded_72h     INTEGER NOT NULL DEFAULT 0,
    quality_status              VARCHAR(32) NOT NULL DEFAULT 'PASSED',
    details                     JSONB,
    logged_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id         VARCHAR(64) PRIMARY KEY,
    customer_name       VARCHAR(200),
    customer_document   VARCHAR(64),
    customer_email      VARCHAR(200),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ==============================================================================
-- 1. ESQUEMA 'silver': VISTAS CURADAS Y DE AUDITORIA INTERMEDIA
-- ==============================================================================
CREATE OR REPLACE VIEW silver.vw_customers_anonymized AS
SELECT
    customer_id,
    customer_name AS customer_name_hash,
    customer_document AS customer_document_hash,
    customer_email AS customer_email_masked,
    updated_at
FROM public.customers;

CREATE OR REPLACE VIEW silver.vw_seller_dispatch AS
SELECT
    dispatch_id,
    source_system,
    source_seller_id,
    paid_at,
    shipped_at,
    ROUND(EXTRACT(EPOCH FROM (shipped_at - paid_at)) / 3600.0, 2) AS dispatch_lead_time_hours
FROM public.silver_seller_score_dispatch;

CREATE OR REPLACE VIEW silver.vw_seller_reviews AS
SELECT
    review_id,
    source_system,
    source_seller_id,
    review_score,
    reviewed_at
FROM public.silver_seller_score_reviews;

CREATE OR REPLACE VIEW silver.vw_seller_inventory AS
SELECT
    observation_id,
    source_system,
    source_seller_id,
    inventory_accurate,
    observed_at
FROM public.silver_seller_score_inventory;

CREATE OR REPLACE VIEW silver.vw_seller_cancellations AS
SELECT
    order_id,
    source_system,
    source_seller_id,
    cancelled,
    cancellation_reason,
    cancelled_at
FROM public.silver_seller_score_cancellations;

CREATE OR REPLACE VIEW silver.vw_marketplace_events AS
SELECT
    id,
    event_id,
    order_id,
    event_type,
    status,
    event_timestamp,
    processed_at
FROM public.marketplace_events
WHERE status <> 'exception_exceeded_72h';

-- ==============================================================================
-- 2. ESQUEMA 'gold': VISTAS OFICIALES PARA POWER BI (LAS 7 PREGUNTAS)
-- ==============================================================================

-- 2.1 VIEW: PREGUNTA 1 - VENTAS NETAS EN USD POR PAIS, CANAL, CATEGORIA (RN-01)
CREATE OR REPLACE VIEW gold.vw_sales_consolidated AS
WITH raw_sales_union AS (
    SELECT 'Colombia' AS country_name, 'CO' AS country_code, 'POS' AS channel,
           s.sale_id::text, s.sale_datetime, s.branch_id::text AS store_id, s.seller_id::text AS seller_id,
           i.product_id::text AS product_id, i.quantity, i.unit_price, s.currency_code,
           s.gross_amount, COALESCE(s.discount_amount, 0.00) AS discount_amount, COALESCE(s.tax_amount, 0.00) AS tax_amount,
           s.status
    FROM colombia.fact_sale s JOIN colombia.fact_sale_item i ON s.sale_id = i.sale_id

    UNION ALL

    SELECT 'Peru' AS country_name, 'PE' AS country_code, 'POS' AS channel,
           s.sale_id::text, s.sale_datetime, s.branch_id::text AS store_id, s.seller_id::text AS seller_id,
           i.product_id::text AS product_id, i.quantity, i.unit_price, s.currency_code,
           s.gross_amount, COALESCE(s.discount_amount, 0.00) AS discount_amount, COALESCE(s.tax_amount, 0.00) AS tax_amount,
           s.status
    FROM peru.fact_sale s JOIN peru.fact_sale_item i ON s.sale_id = i.sale_id

    UNION ALL

    SELECT 'Ecuador' AS country_name, 'EC' AS country_code, 'POS' AS channel,
           s.sale_id::text, s.sale_datetime, s.branch_id::text AS store_id, s.seller_id::text AS seller_id,
           i.product_id::text AS product_id, i.quantity, i.unit_price, s.currency_code,
           s.gross_amount, COALESCE(s.discount_amount, 0.00) AS discount_amount, COALESCE(s.tax_amount, 0.00) AS tax_amount,
           s.status
    FROM ecuador.fact_sale s JOIN ecuador.fact_sale_item i ON s.sale_id = i.sale_id

    UNION ALL

    SELECT 'Bolivia' AS country_name, 'BO' AS country_code, 'POS' AS channel,
           s.sale_id::text, s.sale_datetime, s.branch_id::text AS store_id, s.seller_id::text AS seller_id,
           i.product_id::text AS product_id, i.quantity, i.unit_price, s.currency_code,
           s.gross_amount, COALESCE(s.discount_amount, 0.00) AS discount_amount, COALESCE(s.tax_amount, 0.00) AS tax_amount,
           s.status
    FROM bolivia.fact_sale s JOIN bolivia.fact_sale_item i ON s.sale_id = i.sale_id

    UNION ALL

    SELECT 'Chile' AS country_name, 'CL' AS country_code, 'POS' AS channel,
           s.sale_id::text, s.sale_datetime, s.branch_id::text AS store_id, s.seller_id::text AS seller_id,
           i.product_id::text AS product_id, i.quantity, i.unit_price, s.currency_code,
           s.gross_amount, COALESCE(s.discount_amount, 0.00) AS discount_amount, COALESCE(s.tax_amount, 0.00) AS tax_amount,
           s.status
    FROM chile.fact_sale s JOIN chile.fact_sale_item i ON s.sale_id = i.sale_id
)
SELECT
    u.sale_id,
    u.sale_datetime,
    u.sale_datetime::date AS order_date,
    DATE_TRUNC('week', u.sale_datetime)::date AS week_start_date,
    DATE_TRUNC('month', u.sale_datetime)::date AS month_start_date,
    EXTRACT(YEAR FROM u.sale_datetime)::int AS sale_year,
    u.country_name,
    u.country_code,
    u.channel,
    u.store_id,
    COALESCE(b.branch_name, 'Sucursal ' || SUBSTRING(u.store_id, 1, 8)) AS store_name,
    u.seller_id,
    COALESCE(sel.seller_name, 'Seller ' || SUBSTRING(u.seller_id, 1, 8)) AS seller_name,
    u.product_id,
    COALESCE(p.sku, 'SKU-' || SUBSTRING(u.product_id, 1, 8)) AS sku,
    COALESCE(p.product_name, 'Producto ' || SUBSTRING(u.product_id, 1, 8)) AS product_name,
    COALESCE(cat.category_name, 'General') AS category_name,
    u.quantity,
    u.unit_price,
    u.currency_code,
    COALESCE(
        er.rate,
        CASE
            WHEN u.currency_code = 'USD' THEN 1.00000000
            WHEN u.currency_code = 'COP' THEN 0.00024000
            WHEN u.currency_code = 'PEN' THEN 0.27000000
            WHEN u.currency_code = 'BOB' THEN 0.14400000
            WHEN u.currency_code = 'CLP' THEN 0.00105000
            WHEN u.currency_code = 'BRL' THEN 0.18000000
            ELSE 1.00000000
        END
    ) AS exchange_rate_to_usd,
    ROUND(
        u.gross_amount * COALESCE(
            er.rate,
            CASE
                WHEN u.currency_code = 'USD' THEN 1.00000000
                WHEN u.currency_code = 'COP' THEN 0.00024000
                WHEN u.currency_code = 'PEN' THEN 0.27000000
                WHEN u.currency_code = 'BOB' THEN 0.14400000
                WHEN u.currency_code = 'CLP' THEN 0.00105000
                ELSE 1.00000000
            END
        ),
        2
    ) AS gross_amount_usd,
    ROUND(
        u.discount_amount * COALESCE(
            er.rate,
            CASE
                WHEN u.currency_code = 'USD' THEN 1.00000000
                WHEN u.currency_code = 'COP' THEN 0.00024000
                WHEN u.currency_code = 'PEN' THEN 0.27000000
                WHEN u.currency_code = 'BOB' THEN 0.14400000
                WHEN u.currency_code = 'CLP' THEN 0.00105000
                ELSE 1.00000000
            END
        ),
        2
    ) AS discount_amount_usd,
    ROUND(
        (u.gross_amount - u.discount_amount) * COALESCE(
            er.rate,
            CASE
                WHEN u.currency_code = 'USD' THEN 1.00000000
                WHEN u.currency_code = 'COP' THEN 0.00024000
                WHEN u.currency_code = 'PEN' THEN 0.27000000
                WHEN u.currency_code = 'BOB' THEN 0.14400000
                WHEN u.currency_code = 'CLP' THEN 0.00105000
                ELSE 1.00000000
            END
        ),
        2
    ) AS net_sales_usd,
    u.status
FROM raw_sales_union u
LEFT JOIN public.dim_branch b ON u.store_id = b.branch_id::text
LEFT JOIN public.dim_seller sel ON u.seller_id = sel.seller_id::text
LEFT JOIN public.dim_product p ON u.product_id = p.product_id::text
LEFT JOIN public.dim_product_category cat ON p.category_id = cat.category_id
LEFT JOIN public.dim_currency c ON u.currency_code = c.currency_code
LEFT JOIN public.dim_exchange_rate er ON c.currency_id = er.currency_id AND er.rate_date = u.sale_datetime::date;

-- 2.2 VIEW: PREGUNTA 2 - SKUS Y TIENDAS EN RIESGO DE QUIEBRE + VENTA EN RIESGO (RN-04 / RF-05)
CREATE OR REPLACE VIEW gold.vw_stockout_risk_monitoring AS
SELECT
    r.store_id,
    COALESCE(b.branch_name, 'Sucursal ' || SUBSTRING(r.store_id, 1, 8)) AS store_name,
    COALESCE(c.country_code, 'CO') AS country_code,
    COALESCE(c.country_name, 'Colombia') AS country_name,
    r.product_id,
    COALESCE(p.sku, 'SKU-' || SUBSTRING(r.product_id, 1, 8)) AS sku,
    COALESCE(p.product_name, 'Producto ' || SUBSTRING(r.product_id, 1, 8)) AS product_name,
    COALESCE(cat.category_name, 'General') AS category_name,
    r.available_stock,
    r.avg_daily_sales,
    r.days_until_stockout,
    r.threshold_days,
    r.at_risk,
    COALESCE(r.sales_at_risk_usd, 0.00) AS sales_at_risk_usd,
    CASE
        WHEN r.days_until_stockout <= 1.0 THEN 'CRITICAL'
        WHEN r.days_until_stockout <= 3.0 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS risk_severity,
    r.updated_at
FROM public.gold_stockout_risk r
LEFT JOIN public.dim_branch b ON r.store_id = b.branch_id::text
LEFT JOIN public.dim_country c ON b.country_id = c.country_id
LEFT JOIN public.dim_product p ON r.product_id = p.product_id::text
LEFT JOIN public.dim_product_category cat ON p.category_id = cat.category_id;

-- 2.3 VIEW: PREGUNTA 3 - PROMOCIONES RECHAZADAS POR COBERTURA < 7 DIAS + VENTA PERDIDA (RN-05 / RF-04)
CREATE OR REPLACE VIEW gold.vw_promotions_analysis AS
SELECT
    pa.promotion_id,
    pa.sku,
    pa.product_name,
    COALESCE(pa.category_name, 'Promotions') AS category_name,
    pa.store_id,
    pa.discount_pct,
    pa.available_stock,
    pa.avg_daily_sales,
    pa.coverage_days,
    pa.status,
    CASE WHEN pa.status = 'RECHAZADA' THEN TRUE ELSE FALSE END AS is_rejected,
    pa.rejection_reason,
    COALESCE(pa.projected_lost_sales_usd, 0.00) AS projected_lost_sales_usd,
    pa.evaluated_at,
    pa.evaluated_at::date AS evaluation_date
FROM public.gold_promotions_audit pa;

-- 2.4 VIEW: PREGUNTA 4 - DISTRIBUCION DE SELLERS POR ESTATUS Y EVOLUCION HISTORICA (RN-07, RN-08 / RF-03)
CREATE OR REPLACE VIEW gold.vw_seller_performance AS
SELECT
    s.seller_id::text AS seller_id,
    s.seller_name,
    COALESCE(c.country_code, 'CO') AS country_code,
    COALESCE(c.country_name, 'Colombia') AS country_name,
    CASE WHEN s.external THEN 'Third-Party' ELSE 'Own' END AS seller_type,
    COALESCE(st.status, 'active') AS current_status,
    COALESCE(st.reason, 'Desempeño estándar') AS status_reason,
    COALESCE(sc.score, 85.00) AS current_score,
    COALESCE(sc.dispatch_pct, 95.00) AS dispatch_pct,
    COALESCE(sc.review_rating, 90.00) AS review_rating,
    COALESCE(sc.inventory_accuracy, 98.00) AS inventory_accuracy,
    COALESCE(sc.cancellation_rate, 2.00) AS cancellation_rate,
    COALESCE(h.week_start, CURRENT_DATE) AS week_start,
    COALESCE(h.score, sc.score, 85.00) AS historical_score,
    CASE
        WHEN COALESCE(sc.score, 85.00) >= 80 THEN 'High Performer'
        WHEN COALESCE(sc.score, 85.00) >= 60 THEN 'Standard'
        WHEN COALESCE(sc.score, 85.00) >= 40 THEN 'Under Observation'
        ELSE 'Suspended / Action Required'
    END AS performance_tier,
    st.updated_at AS status_updated_at
FROM public.dim_seller s
LEFT JOIN public.dim_country c ON s.country_id = c.country_id
LEFT JOIN public.gold_seller_status st ON s.seller_id::text = st.seller_id
LEFT JOIN public.gold_seller_score sc ON s.seller_id::text = sc.seller_id
LEFT JOIN public.gold_seller_score_history h ON s.seller_id::text = h.seller_id;

-- 2.5 VIEW: PREGUNTA 5 - TASA DE DEVOLUCION POR CATEGORIA Y % FUERA DE 30 DIAS (RN-02, RN-06)
CREATE OR REPLACE VIEW gold.vw_returns_audit AS
WITH base_metrics AS (
    SELECT
        s.country_code,
        s.category_name,
        COUNT(*) AS total_items_sold,
        COUNT(*) FILTER (WHERE s.status IN ('RETURNED', 'REFUNDED', 'EXCEPTION_RETURN')) AS total_items_returned,
        COUNT(*) FILTER (
            WHERE s.status = 'EXCEPTION_RETURN'
               OR (s.status IN ('RETURNED', 'REFUNDED') AND s.discount_amount_usd > 20.00)
        ) AS late_returns_count
    FROM gold.vw_sales_consolidated s
    GROUP BY s.country_code, s.category_name
)
SELECT
    country_code,
    category_name,
    total_items_sold,
    total_items_returned,
    ROUND((total_items_returned::numeric / NULLIF(total_items_sold, 0)) * 100, 2) AS return_rate_pct,
    (total_items_returned - late_returns_count) AS returns_within_30_days,
    late_returns_count AS returns_outside_30_days,
    ROUND((late_returns_count::numeric / NULLIF(total_items_returned, 0)) * 100, 2) AS pct_returns_outside_30_days
FROM base_metrics;

-- 2.6 VIEW: PREGUNTA 6 - IMPACTO DE DIAS FESTIVOS EN VENTAS POR PAIS (FESTIVOS VS NORMALES)
CREATE OR REPLACE VIEW gold.vw_holiday_sales_impact AS
SELECT
    s.country_code,
    s.country_name,
    s.order_date,
    s.sale_year,
    CASE WHEN h.holiday_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_holiday,
    COALESCE(h.holiday_name, 'Día Regular') AS holiday_name,
    COUNT(DISTINCT s.sale_id) AS total_orders,
    SUM(s.net_sales_usd) AS daily_net_sales_usd,
    ROUND(AVG(s.net_sales_usd), 2) AS avg_ticket_usd
FROM gold.vw_sales_consolidated s
LEFT JOIN public.dim_country c ON s.country_code = c.country_code
LEFT JOIN public.dim_holiday h ON c.country_id = h.country_id AND s.order_date = h.holiday_date
GROUP BY s.country_code, s.country_name, s.order_date, s.sale_year, h.holiday_id, h.holiday_name;

-- 2.7 VIEW: PREGUNTA 7 - METRICAS DE CALIDAD DE DATOS POR FUENTE (RN-10)
CREATE OR REPLACE VIEW gold.vw_data_quality_dashboard AS
SELECT
    metric_id,
    load_id,
    source_system,
    records_processed,
    duplicates_detected,
    ROUND((duplicates_detected::numeric / NULLIF(records_processed, 0)) * 100, 2) AS duplicate_rate_pct,
    late_arrivals_incorporated,
    ROUND((late_arrivals_incorporated::numeric / NULLIF(records_processed, 0)) * 100, 2) AS late_arrival_rate_pct,
    rejections_exceeded_72h,
    ROUND((rejections_exceeded_72h::numeric / NULLIF(records_processed, 0)) * 100, 2) AS rejection_rate_pct,
    quality_status,
    details,
    logged_at,
    logged_at::date AS metric_date
FROM public.data_quality_metrics;

-- ==============================================================================
-- 3. ALIASES EN 'public' PARA COMPATIBILIDAD RETROACTIVA 100% TRANSPARENTE
-- ==============================================================================
CREATE OR REPLACE VIEW public.vw_gold_sales_consolidated AS SELECT * FROM gold.vw_sales_consolidated;
CREATE OR REPLACE VIEW public.vw_gold_stockout_risk_monitoring AS SELECT * FROM gold.vw_stockout_risk_monitoring;
CREATE OR REPLACE VIEW public.vw_gold_promotions_analysis AS SELECT * FROM gold.vw_promotions_analysis;
CREATE OR REPLACE VIEW public.vw_gold_seller_performance AS SELECT * FROM gold.vw_seller_performance;
CREATE OR REPLACE VIEW public.vw_gold_returns_audit AS SELECT * FROM gold.vw_returns_audit;
CREATE OR REPLACE VIEW public.vw_gold_holiday_sales_impact AS SELECT * FROM gold.vw_holiday_sales_impact;
CREATE OR REPLACE VIEW public.vw_gold_data_quality_dashboard AS SELECT * FROM gold.vw_data_quality_dashboard;
