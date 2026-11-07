"""
DAG: silver_to_gold_business_rules
Purpose: MercaAndes business rules from Silver to Gold layer.
Implements:
  - RN-04: Stockout risk evaluation with public holiday threshold (3 days vs 5 days).
  - RN-04 (Alerts): Publication of operational stockout alerts to RabbitMQ.
  - RN-09: Seller inventory accuracy (percentage of orders without out-of-stock cancellations).
  - RF-05: Daily calculation of stockout risk by SKU and store, monetary sales at risk ($ USD), and replenishment notification.

Schedule: 0 10 * * * (Daily at 10:00 AM)
Architecture: Optimized for AWS Multi-Worker Cluster (ECS / EC2 + RabbitMQ + RDS).
"""

from __future__ import annotations

import csv
import io
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from operational_messaging.config import STOCKOUT_QUEUE
from operational_messaging.messages import new_message
from operational_messaging.rabbitmq import publish_messages

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "MercaAndes Data Team",
    "depends_on_past": False,
    "email": ["yailauc_19@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}

BUCKET = os.getenv("S3_DATA_LAKE_BUCKET_NAME", "mercaandes-data-lake")
GOLD_FOLDER = os.getenv("S3_GOLD_FOLDER", "gold")


@dag(
    dag_id="silver_to_gold_business_rules",
    default_args=default_args,
    description="Business rules Silver -> Gold (RN-04, RN-09, RF-05). Distributes alerts to RabbitMQ queue.",
    schedule="0 10 * * *",
    start_date=datetime(2026, 11, 4, 14, 0),
    catchup=False,
    max_active_runs=1,
    tags=["layer:gold", "business-rules", "inventory", "alerts", "mercaandes", "rf-05"],
    doc_md="""
    # Silver to Gold Business Rules (MercaAndes)
    
    This DAG materializes Gold tables for Serving/Analytics and emits operational alerts:
    
    1. **TaskGroup: `stockout_risk_monitoring` (Worker 1)**:
       - `RN-04 / RF-05`: Evaluates stockout risk against the holiday calendar (3 or 5 days threshold)
         and computes monetary sales at risk ($ USD).
       - Publishes critical alerts to RabbitMQ (`alertas.riesgo_quiebre`) notifying the replenishment team.
    2. **TaskGroup: `seller_inventory_performance` (Worker 2 - In Parallel)**:
       - `RN-09`: Calculates seller inventory accuracy over a 30-day rolling window.
    3. **Task: `export_gold_metrics_to_s3`**:
       - Exports Gold metrics safely to AWS S3.
    """,
)
def silver_to_gold_business_rules():
    @task(task_id="rn_04_calculate_stockout_risk")
    def calculate_stockout_risk(**context):
        """
        RN-04 / RF-05: A SKU is at "stockout risk" when its available inventory covers less than
        3 days of average sales. If a holiday exists in the next 5 days (CO, PE, EC, BO, CL),
        the threshold rises to 5 days.
        Calculates daily stockout risk and monetary sales at risk ($ USD).
        Connects to Postgres (RDS): merca_pos.
        """
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        # Ensure prerequisite tables exist idempotently
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.gold_available_inventory (
                store_id VARCHAR(64),
                product_id VARCHAR(64),
                available_stock NUMERIC(12, 2) DEFAULT 0,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (store_id, product_id)
            );
            CREATE TABLE IF NOT EXISTS public.inventory (
                store_id VARCHAR(64),
                product_id VARCHAR(64),
                physical_stock NUMERIC(10, 2) DEFAULT 0,
                PRIMARY KEY (store_id, product_id)
            );
            CREATE TABLE IF NOT EXISTS public.orders (
                order_id VARCHAR(64) PRIMARY KEY,
                order_date DATE DEFAULT CURRENT_DATE,
                status VARCHAR(32) DEFAULT 'delivered'
            );
            CREATE TABLE IF NOT EXISTS public.order_items (
                order_item_id VARCHAR(64) PRIMARY KEY,
                order_id VARCHAR(64),
                seller_id VARCHAR(64),
                store_id VARCHAR(64),
                product_id VARCHAR(64),
                quantity NUMERIC(10, 2) DEFAULT 1,
                price NUMERIC(10, 2) DEFAULT 25.00,
                cancel_reason VARCHAR(64)
            );
            ALTER TABLE public.order_items ADD COLUMN IF NOT EXISTS seller_id VARCHAR(64);
            ALTER TABLE public.order_items ADD COLUMN IF NOT EXISTS price NUMERIC(10, 2) DEFAULT 25.00;

            CREATE TABLE IF NOT EXISTS public.gold_stockout_risk (
                store_id VARCHAR(64),
                product_id VARCHAR(64),
                available_stock NUMERIC(12, 2) DEFAULT 0,
                avg_daily_sales NUMERIC(12, 2) DEFAULT 0,
                days_until_stockout NUMERIC(10, 2),
                threshold_days INT DEFAULT 3,
                at_risk BOOLEAN DEFAULT FALSE,
                sales_at_risk_usd NUMERIC(14, 2) DEFAULT 0.00,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (store_id, product_id)
            );
            ALTER TABLE public.gold_stockout_risk ADD COLUMN IF NOT EXISTS sales_at_risk_usd NUMERIC(14, 2) DEFAULT 0.00;

            CREATE TABLE IF NOT EXISTS public.holidays (
                holiday_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                country_code VARCHAR(2) NOT NULL,
                holiday_date DATE NOT NULL,
                holiday_name VARCHAR(150) NOT NULL,
                source VARCHAR(100) DEFAULT 'system',
                CONSTRAINT uq_holiday_country_date_name UNIQUE (country_code, holiday_date, holiday_name)
            );
            """
        )

        today = date.today()
        five_days_later = today + timedelta(days=5)
        cur.execute(
            """
            SELECT COUNT(*) FROM holidays
            WHERE country_code IN ('CO', 'PE', 'EC', 'BO', 'CL')
            AND holiday_date BETWEEN %s AND %s
            """,
            (today, five_days_later),
        )
        holiday_count = cur.fetchone()[0]
        threshold_days = 5 if holiday_count > 0 else 3
        print(f"[RN-04/RF-05] Holiday in next 5 days: {holiday_count > 0}. Threshold: {threshold_days} days")

        cur.execute(
            """
            INSERT INTO gold_stockout_risk (
                store_id, product_id, available_stock, avg_daily_sales,
                days_until_stockout, threshold_days, at_risk, sales_at_risk_usd, created_at
            )
            SELECT
                ai.store_id,
                ai.product_id,
                ai.available_stock,
                COALESCE(s.avg_daily_sales, 0),
                CASE WHEN COALESCE(s.avg_daily_sales, 0) > 0
                    THEN ROUND(ai.available_stock::numeric / s.avg_daily_sales, 1)
                END,
                %s,
                (ai.available_stock <= COALESCE(s.avg_daily_sales, 0) * %s),
                CASE
                    WHEN (ai.available_stock <= COALESCE(s.avg_daily_sales, 0) * %s)
                    THEN ROUND(
                        GREATEST(0, (%s - COALESCE(ROUND(ai.available_stock::numeric / NULLIF(s.avg_daily_sales, 0), 1), 0)))
                        * COALESCE(s.avg_daily_sales, 0)
                        * COALESCE(s.avg_unit_price, 25.00),
                        2
                    )
                    ELSE 0.00
                END,
                NOW()
            FROM gold_available_inventory ai
            LEFT JOIN (
                SELECT
                    store_id,
                    product_id,
                    AVG(quantity) AS avg_daily_sales,
                    COALESCE(AVG(price), 25.00) AS avg_unit_price
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
                AND o.status NOT IN ('cancelled', 'exception_return')
                GROUP BY store_id, product_id
            ) s ON s.store_id = ai.store_id AND s.product_id = ai.product_id
            ON CONFLICT (store_id, product_id)
            DO UPDATE SET available_stock = EXCLUDED.available_stock,
                        avg_daily_sales = EXCLUDED.avg_daily_sales,
                        days_until_stockout = EXCLUDED.days_until_stockout,
                        threshold_days = EXCLUDED.threshold_days,
                        at_risk = EXCLUDED.at_risk,
                        sales_at_risk_usd = EXCLUDED.sales_at_risk_usd,
                        updated_at = NOW()
            """,
            (threshold_days, threshold_days, threshold_days, threshold_days),
        )
        inserted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"[RN-04/RF-05] Evaluated stockout risk for {inserted} products (threshold: {threshold_days} days)")
        return threshold_days

    @task(task_id="rn_04_publish_stockout_alerts")
    def publish_stockout_alerts(**context):
        """
        RF-05 / RN-04: Publishes stockout risk alerts to the durable operational RabbitMQ route
        `alertas.riesgo_quiebre` for notification to the replenishment team.
        """
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT store_id, product_id, available_stock, avg_daily_sales,
                days_until_stockout, threshold_days, sales_at_risk_usd
            FROM gold_stockout_risk
            WHERE at_risk = TRUE
            """
        )
        risks = cur.fetchall()
        cur.close()
        conn.close()

        if not risks:
            print("[RN-04/RF-05] No products at stockout risk.")
            return 0

        messages = [
            new_message(
                "STOCKOUT_RISK",
                {
                    "store_id": store_id,
                    "product_id": product_id,
                    "available_stock": float(stock) if stock else 0.0,
                    "average_daily_sales": float(avg_sales) if avg_sales else 0.0,
                    "days_until_stockout": float(days_until) if days_until is not None else 0.0,
                    "threshold_days": int(threshold),
                    "sales_at_risk_usd": float(sales_at_risk) if sales_at_risk else 0.0,
                    "urgency": "CRITICAL" if (days_until is not None and float(days_until) <= 1.0) else "HIGH",
                    "target_area": "reabastecimiento",
                },
            )
            for store_id, product_id, stock, avg_sales, days_until, threshold, sales_at_risk in risks
        ]
        publish_messages(STOCKOUT_QUEUE, messages)
        print(f"[RN-04/RF-05] Published {len(messages)} persistent alerts to {STOCKOUT_QUEUE} (Replenishment Area)")
        return len(risks)

    @task(task_id="rn_09_calculate_inventory_accuracy")
    def calculate_inventory_accuracy(**context):
        """
        RN-09: Seller inventory accuracy: percentage of orders that were not cancelled
        due to stock unavailability in the last 30 days.
        Connects to Postgres (RDS): merca_pos.
        """
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.gold_inventory_accuracy (
                seller_id VARCHAR(64) PRIMARY KEY,
                accuracy_pct NUMERIC(5, 2) DEFAULT 100.0,
                period_days INT DEFAULT 30,
                calculated_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS public.orders (
                order_id VARCHAR(64) PRIMARY KEY,
                order_date DATE DEFAULT CURRENT_DATE,
                status VARCHAR(32) DEFAULT 'delivered'
            );
            CREATE TABLE IF NOT EXISTS public.order_items (
                order_item_id VARCHAR(64) PRIMARY KEY,
                order_id VARCHAR(64),
                seller_id VARCHAR(64),
                store_id VARCHAR(64),
                product_id VARCHAR(64),
                quantity NUMERIC(10, 2) DEFAULT 1,
                price NUMERIC(10, 2) DEFAULT 25.00,
                cancel_reason VARCHAR(64)
            );
            ALTER TABLE public.order_items ADD COLUMN IF NOT EXISTS seller_id VARCHAR(64);
            ALTER TABLE public.order_items ADD COLUMN IF NOT EXISTS price NUMERIC(10, 2) DEFAULT 25.00;
            """
        )

        cur.execute(
            """
            INSERT INTO gold_inventory_accuracy (seller_id, accuracy_pct, period_days, calculated_at)
            SELECT
                oi.seller_id,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE oi.cancel_reason IS NULL OR oi.cancel_reason <> 'out_of_stock')
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS accuracy_pct,
                30,
                NOW()
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY oi.seller_id
            ON CONFLICT (seller_id)
            DO UPDATE SET accuracy_pct = EXCLUDED.accuracy_pct, updated_at = NOW()
            """
        )
        inserted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"[RN-09] Calculated inventory accuracy for {inserted} sellers")
        return inserted

    start_business_rules = EmptyOperator(
        task_id="start_business_rules",
        doc_md="Start point for Silver -> Gold business rules calculation.",
    )

    with TaskGroup("stockout_risk_monitoring", tooltip="RN-04 / RF-05: Stockout Risk Monitoring") as tg_stockout:
        t_stockout_risk = calculate_stockout_risk()
        t_stockout_alert = publish_stockout_alerts()
        t_stockout_risk >> t_stockout_alert

    with TaskGroup("seller_inventory_performance", tooltip="RN-09: Seller Inventory Accuracy") as tg_seller:
        t_inventory_accuracy = calculate_inventory_accuracy()

    @task(task_id="export_gold_metrics_to_s3")
    def export_gold_metrics_to_s3(**context):
        """Export computed Gold tables to AWS S3 using clean CSV formatting."""
        import boto3
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        try:
            s3 = boto3.client("s3")
            pg_hook = PostgresHook(postgres_conn_id="pos_db")
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            gold_tables = ["gold_stockout_risk", "gold_available_inventory", "gold_inventory_accuracy", "gold_daily_sales"]
            for table in gold_tables:
                try:
                    cur.execute(f"SELECT * FROM public.{table};")
                    cols = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    if rows:
                        output = io.StringIO()
                        writer = csv.writer(output)
                        writer.writerow(cols)
                        writer.writerows(rows)
                        key = f"{GOLD_FOLDER}/{table}.csv"
                        s3.put_object(Bucket=BUCKET, Key=key, Body=output.getvalue().encode("utf-8"))
                        print(f"[GOLD] Exported {len(rows)} records to s3://{BUCKET}/{key}")
                except Exception as exc:
                    print(f"[GOLD Warning] Could not export {table}: {exc}")
            cur.close()
            conn.close()
        except Exception as exc:
            print(f"[GOLD Warning] S3 export skipped: {exc}")
        return True

    t_export_gold = export_gold_metrics_to_s3()

    end_business_rules = EmptyOperator(
        task_id="end_business_rules",
        doc_md="Successful completion of Silver -> Gold business rules.",
    )

    # Parallel execution across workers
    start_business_rules >> [tg_stockout, tg_seller] >> t_export_gold >> end_business_rules


silver_to_gold_business_rules_dag = silver_to_gold_business_rules()
