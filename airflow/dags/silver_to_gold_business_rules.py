"""
DAG: silver_to_gold_business_rules
Purpose: MercaAndes business rules from Silver to Gold layer.
Implements:
  - RN-04: Stockout risk evaluation with public holiday threshold (3 days vs 5 days).
  - RN-04 (Alerts): Publication of operational stockout alerts to RabbitMQ.
  - RN-09: Seller inventory accuracy (percentage of orders without out-of-stock cancellations).

Schedule: 0 10 * * * (Daily at 10:00 AM)
Architecture: Optimized for AWS Multi-Worker Cluster (ECS / EC2 + RabbitMQ + RDS).
"""

from __future__ import annotations

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


@dag(
    dag_id="silver_to_gold_business_rules",
    default_args=default_args,
    description="Business rules Silver -> Gold (RN-04, RN-09). Distributes alerts to RabbitMQ queue.",
    schedule="0 10 * * *",
    start_date=datetime(2026, 11, 5, 10, 0),
    catchup=False,
    max_active_runs=1,
    tags=["layer:gold", "business-rules", "inventory", "alerts", "mercaandes"],
    doc_md="""
    # Silver to Gold Business Rules (MercaAndes)
    
    This DAG materializes Gold tables for Serving/Analytics and emits operational alerts:
    
    1. **TaskGroup: `stockout_risk_monitoring` (Worker 1)**:
       - `RN-04`: Evaluates stockout risk against the holiday calendar (3 or 5 days threshold).
       - Publishes critical alerts to RabbitMQ (`alertas.riesgo_quiebre`).
    2. **TaskGroup: `seller_inventory_performance` (Worker 2 - In Parallel)**:
       - `RN-09`: Calculates seller inventory accuracy over a 30-day rolling window.
    """,
)
def silver_to_gold_business_rules():
    @task(task_id="rn_04_calculate_stockout_risk")
    def calculate_stockout_risk(**context):
        """
        RN-04: A SKU is at "stockout risk" when its available inventory covers less than
        3 days of average sales. If a holiday exists in the next 5 days (CO, PE, EC, BO, CL),
        the threshold rises to 5 days.
        Connects to Postgres (RDS): merca_pos.
        """
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

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
        print(f"[RN-04] Holiday in next 5 days: {holiday_count > 0}. Threshold: {threshold_days} days")

        cur.execute(
            """
            INSERT INTO gold_stockout_risk (store_id, product_id, available_stock, avg_daily_sales,
                                            days_until_stockout, threshold_days, at_risk, created_at)
            SELECT
                i.store_id,
                i.product_id,
                ai.available_stock,
                COALESCE(s.avg_daily_sales, 0),
                CASE WHEN COALESCE(s.avg_daily_sales, 0) > 0
                    THEN ROUND(ai.available_stock::numeric / s.avg_daily_sales, 1)
                END,
                %s,
                (ai.available_stock <= COALESCE(s.avg_daily_sales, 0) * %s),
                NOW()
            FROM gold_available_inventory ai
            LEFT JOIN (
                SELECT store_id, product_id, AVG(quantity) AS avg_daily_sales
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
                AND o.status NOT IN ('cancelled', 'exception_return')
                GROUP BY store_id, product_id
            ) s ON s.store_id = ai.store_id AND s.product_id = ai.product_id
            JOIN inventory i ON i.store_id = ai.store_id AND i.product_id = ai.product_id
            ON CONFLICT (store_id, product_id)
            DO UPDATE SET available_stock = EXCLUDED.available_stock,
                        avg_daily_sales = EXCLUDED.avg_daily_sales,
                        days_until_stockout = EXCLUDED.days_until_stockout,
                        threshold_days = EXCLUDED.threshold_days,
                        at_risk = EXCLUDED.at_risk,
                        updated_at = NOW()
            """,
            (threshold_days, threshold_days),
        )
        inserted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"[RN-04] Evaluated stockout risk for {inserted} products (threshold: {threshold_days} days)")
        return threshold_days

    @task(task_id="rn_04_publish_stockout_alerts")
    def publish_stockout_alerts(**context):
        """
        Publishes stockout risk alerts to the durable operational RabbitMQ route.
        """
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT store_id, product_id, available_stock, avg_daily_sales,
                days_until_stockout, threshold_days
            FROM gold_stockout_risk
            WHERE at_risk = TRUE
            """
        )
        risks = cur.fetchall()
        cur.close()
        conn.close()

        if not risks:
            print("[RN-04] No products at stockout risk.")
            return 0

        messages = [
            new_message(
                "STOCKOUT_RISK",
                {
                    "store_id": store_id,
                    "product_id": product_id,
                    "available_stock": float(stock) if stock else 0,
                    "average_daily_sales": float(avg_sales) if avg_sales else 0,
                    "days_until_stockout": float(days_until) if days_until else None,
                    "threshold_days": threshold,
                },
            )
            for store_id, product_id, stock, avg_sales, days_until, threshold in risks
        ]
        publish_messages(STOCKOUT_QUEUE, messages)
        print(f"[RN-04] Published {len(messages)} persistent alerts to {STOCKOUT_QUEUE}")
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

    with TaskGroup("stockout_risk_monitoring", tooltip="RN-04: Stockout Risk Monitoring") as tg_stockout:
        t_stockout_risk = calculate_stockout_risk()
        t_stockout_alert = publish_stockout_alerts()
        t_stockout_risk >> t_stockout_alert

    with TaskGroup("seller_inventory_performance", tooltip="RN-09: Seller Inventory Accuracy") as tg_seller:
        t_inventory_accuracy = calculate_inventory_accuracy()

    end_business_rules = EmptyOperator(
        task_id="end_business_rules",
        doc_md="Successful completion of Silver -> Gold business rules.",
    )

    # Parallel execution across workers
    start_business_rules >> [tg_stockout, tg_seller] >> end_business_rules


silver_to_gold_business_rules_dag = silver_to_gold_business_rules()
