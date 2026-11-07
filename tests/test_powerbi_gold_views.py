"""
Test: test_powerbi_gold_views.py
Purpose: Validates that the Medallion schemas (Silver & Gold) and Power BI views
directly satisfy architectural isolation and the 7 executive business questions:
  1. Net sales in USD by country, channel, category (daily, weekly, monthly) (RN-01)
  2. SKUs and stores at stockout risk today + monetary sales at risk ($ USD) (RN-04 / RF-05)
  3. Rejected promotions due to inventory coverage < 7 days + potential lost sales (RN-05 / RF-04)
  4. Distribution of sellers by status and historical score evolution (RN-07, RN-08 / RF-03)
  5. Return rate by category and % of returns outside 30-day window (RN-02, RN-06)
  6. Public holidays impact on sales by country (holiday vs normal days)
  7. Data quality metrics by source (duplicates, late arrivals <= 72h, rejected > 72h) (RN-10)

Also validates:
  - Isolation of dedicated schemas 'silver' and 'gold'
  - The Airflow metadata purge script for merca_gold.public
"""

from __future__ import annotations

from pathlib import Path
import pytest

POSTGRES_DIR = Path(__file__).resolve().parent.parent / "postgres"
DDL_FILE = POSTGRES_DIR / "ddl" / "04-gold-powerbi-views.sql"
INIT_FILE = POSTGRES_DIR / "init" / "04-gold-powerbi-views.sql"
AWS_SQL_FILE = Path(__file__).resolve().parent.parent / "aws" / "sql" / "04-gold-powerbi-views.sql"

PURGE_DDL = POSTGRES_DIR / "ddl" / "05-purge-airflow-from-gold.sql"
PURGE_INIT = POSTGRES_DIR / "init" / "05-purge-airflow-from-gold.sql"
PURGE_AWS = Path(__file__).resolve().parent.parent / "aws" / "sql" / "05-purge-airflow-from-gold.sql"


def test_ddl_and_init_scripts_exist():
    assert DDL_FILE.exists(), f"DDL file missing: {DDL_FILE}"
    assert INIT_FILE.exists(), f"Init file missing: {INIT_FILE}"
    assert AWS_SQL_FILE.exists(), f"AWS SQL file missing: {AWS_SQL_FILE}"


def test_ddl_and_init_are_synchronized():
    ddl_content = DDL_FILE.read_text(encoding="utf-8")
    init_content = INIT_FILE.read_text(encoding="utf-8")
    aws_content = AWS_SQL_FILE.read_text(encoding="utf-8")
    assert ddl_content == init_content, "DDL and Init scripts must be identical"
    assert ddl_content == aws_content, "DDL and AWS SQL scripts must be identical"


@pytest.fixture(scope="module")
def sql_content() -> str:
    return DDL_FILE.read_text(encoding="utf-8")


def test_schema_isolation_silver_and_gold(sql_content: str):
    """Verifies that dedicated 'silver' and 'gold' schemas are created and populated."""
    assert "CREATE SCHEMA IF NOT EXISTS silver;" in sql_content
    assert "CREATE SCHEMA IF NOT EXISTS gold;" in sql_content

    # Silver curated views
    assert "CREATE OR REPLACE VIEW silver.vw_customers_anonymized" in sql_content
    assert "CREATE OR REPLACE VIEW silver.vw_seller_dispatch" in sql_content
    assert "CREATE OR REPLACE VIEW silver.vw_seller_reviews" in sql_content
    assert "CREATE OR REPLACE VIEW silver.vw_seller_inventory" in sql_content
    assert "CREATE OR REPLACE VIEW silver.vw_seller_cancellations" in sql_content
    assert "CREATE OR REPLACE VIEW silver.vw_marketplace_events" in sql_content

    # Gold analytical views
    assert "CREATE OR REPLACE VIEW gold.vw_sales_consolidated" in sql_content
    assert "CREATE OR REPLACE VIEW gold.vw_stockout_risk_monitoring" in sql_content
    assert "CREATE OR REPLACE VIEW gold.vw_promotions_analysis" in sql_content
    assert "CREATE OR REPLACE VIEW gold.vw_seller_performance" in sql_content
    assert "CREATE OR REPLACE VIEW gold.vw_returns_audit" in sql_content
    assert "CREATE OR REPLACE VIEW gold.vw_holiday_sales_impact" in sql_content
    assert "CREATE OR REPLACE VIEW gold.vw_data_quality_dashboard" in sql_content

    # Backward compatibility aliases in public
    assert "CREATE OR REPLACE VIEW public.vw_gold_sales_consolidated" in sql_content
    assert "CREATE OR REPLACE VIEW public.vw_gold_stockout_risk_monitoring" in sql_content
    assert "CREATE OR REPLACE VIEW public.vw_gold_promotions_analysis" in sql_content
    assert "CREATE OR REPLACE VIEW public.vw_gold_seller_performance" in sql_content
    assert "CREATE OR REPLACE VIEW public.vw_gold_returns_audit" in sql_content
    assert "CREATE OR REPLACE VIEW public.vw_gold_holiday_sales_impact" in sql_content
    assert "CREATE OR REPLACE VIEW public.vw_gold_data_quality_dashboard" in sql_content


def test_view_1_sales_consolidated_question_1(sql_content: str):
    """Q1: Net sales in USD by country, channel, category (RN-01)."""
    assert "gold.vw_sales_consolidated" in sql_content
    # Must unify country schemas
    assert "colombia.fact_sale" in sql_content
    assert "peru.fact_sale" in sql_content
    assert "ecuador.fact_sale" in sql_content
    assert "bolivia.fact_sale" in sql_content
    assert "chile.fact_sale" in sql_content
    # Must have required dimensional and measure columns
    assert "net_sales_usd" in sql_content
    assert "country_code" in sql_content
    assert "channel" in sql_content
    assert "category_name" in sql_content
    assert "week_start_date" in sql_content
    assert "month_start_date" in sql_content


def test_view_2_stockout_risk_question_2(sql_content: str):
    """Q2: SKUs and stores at stockout risk + monetary sales at risk ($ USD) (RN-04 / RF-05)."""
    assert "gold.vw_stockout_risk_monitoring" in sql_content
    assert "sales_at_risk_usd" in sql_content
    assert "days_until_stockout" in sql_content
    assert "threshold_days" in sql_content
    assert "at_risk" in sql_content
    assert "risk_severity" in sql_content


def test_view_3_promotions_analysis_question_3(sql_content: str):
    """Q3: Rejected promotions due to coverage < 7 days + potential lost sales (RN-05 / RF-04)."""
    assert "gold.vw_promotions_analysis" in sql_content
    assert "gold_promotions_audit" in sql_content
    assert "coverage_days" in sql_content
    assert "is_rejected" in sql_content
    assert "projected_lost_sales_usd" in sql_content


def test_view_4_seller_performance_question_4(sql_content: str):
    """Q4: Distribution of sellers by status and historical score evolution (RN-07, RN-08 / RF-03)."""
    assert "gold.vw_seller_performance" in sql_content
    assert "current_status" in sql_content
    assert "current_score" in sql_content
    assert "historical_score" in sql_content
    assert "dispatch_pct" in sql_content
    assert "inventory_accuracy" in sql_content
    assert "performance_tier" in sql_content


def test_view_5_returns_audit_question_5(sql_content: str):
    """Q5: Return rate by category and % of returns outside 30-day window (RN-02, RN-06)."""
    assert "gold.vw_returns_audit" in sql_content
    assert "return_rate_pct" in sql_content
    assert "returns_within_30_days" in sql_content
    assert "returns_outside_30_days" in sql_content
    assert "pct_returns_outside_30_days" in sql_content


def test_view_6_holiday_sales_impact_question_6(sql_content: str):
    """Q6: Public holidays impact on sales by country (holiday vs normal days)."""
    assert "gold.vw_holiday_sales_impact" in sql_content
    assert "is_holiday" in sql_content
    assert "holiday_name" in sql_content
    assert "daily_net_sales_usd" in sql_content


def test_view_7_data_quality_dashboard_question_7(sql_content: str):
    """Q7: Data quality metrics by source (duplicates, late arrivals, rejections) (RN-10)."""
    assert "gold.vw_data_quality_dashboard" in sql_content
    assert "data_quality_metrics" in sql_content
    assert "duplicate_rate_pct" in sql_content
    assert "late_arrival_rate_pct" in sql_content
    assert "rejection_rate_pct" in sql_content
    assert "quality_status" in sql_content


def test_purge_airflow_script():
    """Validates that 05-purge-airflow-from-gold.sql drops airflow tables while protecting business tables."""
    assert PURGE_DDL.exists(), f"Purge DDL missing: {PURGE_DDL}"
    assert PURGE_INIT.exists(), f"Purge Init missing: {PURGE_INIT}"
    assert PURGE_AWS.exists(), f"Purge AWS missing: {PURGE_AWS}"

    purge_content = PURGE_DDL.read_text(encoding="utf-8")
    assert purge_content == PURGE_INIT.read_text(encoding="utf-8"), "Purge scripts must be identical"
    assert purge_content == PURGE_AWS.read_text(encoding="utf-8"), "Purge scripts must be identical"

    # Must target Airflow metadata tables
    assert "DROP TABLE IF EXISTS public.dag" in purge_content
    assert "DROP TABLE IF EXISTS public.dag_run" in purge_content
    assert "DROP TABLE IF EXISTS public.task_instance" in purge_content
    assert "DROP TABLE IF EXISTS public.ab_user" in purge_content
    assert "DROP TABLE IF EXISTS public.connection" in purge_content
    assert "DROP TABLE IF EXISTS public.xcom" in purge_content

    # MUST NOT drop any MercaAndes business tables
    assert "DROP TABLE IF EXISTS public.dim_country" not in purge_content
    assert "DROP TABLE IF EXISTS public.dim_branch" not in purge_content
    assert "DROP TABLE IF EXISTS public.dim_seller" not in purge_content
    assert "DROP TABLE IF EXISTS public.gold_stockout_risk" not in purge_content
    assert "DROP TABLE IF EXISTS public.gold_promotions_audit" not in purge_content
    assert "DROP TABLE IF EXISTS public.customers" not in purge_content
    assert "DROP TABLE IF EXISTS public.data_quality_metrics" not in purge_content
