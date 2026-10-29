"""
DAG: silver_to_gold_business_rules
Purpose: MercaAndes business rules from Silver to Gold layer.
Implements:
  - RN-04: Stockout risk evaluation with public holiday threshold (3 days vs 5 days).
  - RN-04 (Alerts): Publication of operational stockout alerts to RabbitMQ.
  - RN-09: Seller inventory accuracy (percentage of orders without out-of-stock cancellations).

Schedule: @daily
Architecture: Optimized for AWS Multi-Worker Cluster (ECS / EC2 + RabbitMQ + RDS).
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone

import pika
from airflow import DAG
from airflow.utils.timezone import datetime as airflow_datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "MercaAndes Data Team",
    "depends_on_past": False,
    "email": ["data-team@mercaandes.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", os.getenv("RABBITMQ_USERNAME", "guest_user"))
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
QUEUE_STOCKOUT = os.getenv("RABBITMQ_QUEUE_STOCKOUT_ALERTS", "mercaandes.alerts.stockout")


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
        JOIN inventory i ON i.store_id = ai.store_id AND i.product_id = i.product_id
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


def publish_stockout_alerts(**context):
    """
    Publishes stockout risk alerts to RabbitMQ queue: mercaandes.alerts.stockout.
    Compatible with EC2 RabbitMQ in private compute subnet.
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

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
        )
    )
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_STOCKOUT, durable=True)

        for store_id, product_id, stock, avg_sales, days_until, threshold in risks:
            alert = {
                "alert_type": "STOCKOUT_RISK",
                "store_id": store_id,
                "product_id": product_id,
                "available_stock": float(stock) if stock else 0,
                "average_daily_sales": float(avg_sales) if avg_sales else 0,
                "days_until_stockout": float(days_until) if days_until else None,
                "threshold_days": threshold,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_STOCKOUT,
                body=json.dumps(alert),
                properties=pika.BasicProperties(delivery_mode=2),
            )
    finally:
        connection.close()

    print(f"[RN-04] Published {len(risks)} stockout alerts to RabbitMQ queue: {QUEUE_STOCKOUT}")
    return len(risks)


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


with DAG(
    dag_id="silver_to_gold_business_rules",
    default_args=default_args,
    description="Business rules Silver -> Gold (RN-04, RN-09). Distributes alerts to RabbitMQ queue.",
    schedule="@daily",
    start_date=airflow_datetime(2026, 1, 1, tzinfo="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    tags=["layer:gold", "business-rules", "inventory", "alerts", "mercaandes"],
    doc_md="""
    # Reglas de Negocio Silver a Gold (MercaAndes)
    
    Este DAG materializa las tablas Gold para Serving/Analytics y emite alertas operacionales:
    
    1. **TaskGroup: `stockout_risk_monitoring` (Worker 1)**:
       - `RN-04`: Evalúa el riesgo de quiebre de stock cruzando con el calendario de festivos (umbral de 3 o 5 días).
       - Publica las alertas críticas a RabbitMQ (`mercaandes.alerts.stockout`).
    2. **TaskGroup: `seller_inventory_performance` (Worker 2 - En Paralelo)**:
       - `RN-09`: Calcula la exactitud de inventario por vendedor en ventana móvil de 30 días.
    """,
) as dag:

    start_business_rules = EmptyOperator(
        task_id="start_business_rules",
        doc_md="Punto de inicio para cálculo de reglas de negocio Silver -> Gold.",
    )

    with TaskGroup("stockout_risk_monitoring", tooltip="RN-04: Monitoreo de Riesgo de Quiebre de Stock") as tg_stockout:
        t_stockout_risk = PythonOperator(
            task_id="rn_04_calculate_stockout_risk",
            python_callable=calculate_stockout_risk,
            doc_md="Calcula SKU en riesgo según ventas promedio e impacto de festivos.",
        )

        t_stockout_alert = PythonOperator(
            task_id="rn_04_publish_stockout_alerts",
            python_callable=publish_stockout_alerts,
            doc_md="Publica alertas operacionales a RabbitMQ en la subred privada.",
        )

        t_stockout_risk >> t_stockout_alert

    with TaskGroup("seller_inventory_performance", tooltip="RN-09: Evaluación de Exactitud de Vendedores") as tg_seller:
        t_inventory_accuracy = PythonOperator(
            task_id="rn_09_calculate_inventory_accuracy",
            python_callable=calculate_inventory_accuracy,
            doc_md="Calcula % de órdenes no canceladas por falta de stock (RN-09).",
        )

    end_business_rules = EmptyOperator(
        task_id="end_business_rules",
        doc_md="Punto de finalización exitosa de reglas de negocio Silver -> Gold.",
    )

    # Ejecución paralela en los 2 workers
    start_business_rules >> [tg_stockout, tg_seller] >> end_business_rules
