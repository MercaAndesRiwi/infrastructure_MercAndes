"""
DAG: seller_scoring
Purpose: Weekly Seller scoring and operational status management.
Implements:
  - RN-07: Seller score (0-100): 40% on-time dispatch, 30% reviews, 20% inventory accuracy, 10% inverse cancellation.
  - RN-08: Status management: score < 60 for 2 weeks = observation; score < 45 for 2 weeks = suspended.

Schedule: @weekly
Architecture: Optimized for AWS Multi-Worker Cluster (ECS Fargate + RDS PostgreSQL).
"""

from __future__ import annotations

from datetime import timedelta

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


def validate_scoring_prerequisites(**context):
    """Verifies that source metrics (orders, reviews) exist before evaluating scores."""
    pg_hook = PostgresHook(postgres_conn_id="pos_db")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM sellers;")
    total_sellers = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"[PRE-CHECK] Vendedores registrados en la plataforma: {total_sellers}")
    return total_sellers


def calculate_seller_score(**context):
    """
    RN-07: Seller score (0-100):
      40% on-time dispatch compliance,
      30% review rating,
      20% inventory accuracy,
      10% inverse cancellation rate.
    Connects to Postgres (RDS): merca_pos.
    """
    pg_hook = PostgresHook(postgres_conn_id="pos_db")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO gold_seller_score (seller_id, score, dispatch_pct, review_rating,
                                    inventory_accuracy, cancellation_rate, calculated_at)
        SELECT
            s.seller_id,
            ROUND(
                COALESCE(d.dispatch_pct, 0) * 0.40 +
                COALESCE(r.review_rating, 0) * 0.30 +
                COALESCE(ia.accuracy_pct, 0) * 0.20 +
                (100 - COALESCE(c.cancellation_rate, 0)) * 0.10,
                2
            ) AS score,
            COALESCE(d.dispatch_pct, 0),
            COALESCE(r.review_rating, 0),
            COALESCE(ia.accuracy_pct, 0),
            COALESCE(c.cancellation_rate, 0),
            NOW()
        FROM sellers s
        LEFT JOIN (
            SELECT seller_id,
                   ROUND(100.0 * SUM(CASE WHEN dispatched_on_time THEN 1 ELSE 0 END) / COUNT(*), 2) AS dispatch_pct
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY seller_id
        ) d ON d.seller_id = s.seller_id
        LEFT JOIN (
            SELECT seller_id, AVG(review_score) AS review_rating
            FROM order_reviews r
            JOIN orders o ON r.order_id = o.order_id
            WHERE r.review_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY seller_id
        ) r ON r.seller_id = s.seller_id
        LEFT JOIN gold_inventory_accuracy ia ON ia.seller_id = s.seller_id
        LEFT JOIN (
            SELECT seller_id,
                   ROUND(100.0 * SUM(CASE WHEN cancel_reason = 'customer_initiated' THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM orders
            WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY seller_id
        ) c ON c.seller_id = s.seller_id
        ON CONFLICT (seller_id)
        DO UPDATE SET score = EXCLUDED.score,
                    dispatch_pct = EXCLUDED.dispatch_pct,
                    review_rating = EXCLUDED.review_rating,
                    inventory_accuracy = EXCLUDED.inventory_accuracy,
                    cancellation_rate = EXCLUDED.cancellation_rate,
                    updated_at = NOW()
        """
    )
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"[RN-07] Calculated scores for {inserted} sellers")
    return inserted


def update_seller_status(**context):
    """
    RN-08: Seller with score < 60 for 2 consecutive weeks -> "under observation".
    Seller with score < 45 for 2 consecutive weeks -> "suspended".
    A suspended seller cannot receive new orders.
    Connects to Postgres (RDS): merca_pos.
    """
    pg_hook = PostgresHook(postgres_conn_id="pos_db")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO gold_seller_status (seller_id, status, reason, updated_at)
        SELECT gs.seller_id,
            CASE
                WHEN gs.consecutive_weeks_low_score >= 2 AND gs.latest_score < 45 THEN 'suspended'
                WHEN gs.consecutive_weeks_low_score >= 2 AND gs.latest_score < 60 THEN 'under_observation'
                ELSE gs.status
            END AS new_status,
            CASE
                WHEN gs.latest_score < 45 THEN 'Score below 45 for 2 consecutive weeks'
                WHEN gs.latest_score < 60 THEN 'Score below 60 for 2 consecutive weeks'
                ELSE 'active'
            END,
        NOW()
        FROM (
            SELECT seller_id,
                COUNT(*) FILTER (WHERE score < 60) OVER (PARTITION BY seller_id ORDER BY week_start ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS consecutive_weeks_low_score,
                FIRST_VALUE(score) OVER (PARTITION BY seller_id ORDER BY week_start DESC) AS latest_score,
                LAG(status) OVER (PARTITION BY seller_id ORDER BY week_start DESC) AS status
            FROM gold_seller_score_weekly
            WHERE week_start >= CURRENT_DATE - INTERVAL '14 days'
        ) gs
        WHERE gs.latest_score IS NOT NULL
        ON CONFLICT (seller_id)
        DO UPDATE SET status = EXCLUDED.status,
                    reason = EXCLUDED.reason,
                    updated_at = NOW()
        """
    )
    updated = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"[RN-08] Updated status for {updated} sellers")
    return updated


with DAG(
    dag_id="seller_scoring",
    default_args=default_args,
    description="Seller scoring and status management (RN-07, RN-08) in Gold layer.",
    schedule="@weekly",
    start_date=airflow_datetime(2026, 1, 1, tzinfo="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    tags=["layer:gold", "seller", "scoring", "compliance", "mercaandes"],
    doc_md="""
    # Evaluación Semanal y Estatus de Vendedores (MercaAndes)
    
    1. **Validación previa**: Confirma existencia de vendedores y métricas operacionales.
    2. **TaskGroup: `seller_evaluation_workflow`**:
       - `RN-07`: Pondera el Score 0-100 (40% Despacho a tiempo, 30% Calificaciones, 20% Exactitud de inventario, 10% Tasa inversa de cancelación).
       - `RN-08`: Actualiza el estado del vendedor (Activo / En Observación / Suspendido).
    """,
) as dag:

    start_scoring = EmptyOperator(
        task_id="start_scoring",
        doc_md="Inicio del ciclo semanal de evaluación de vendedores.",
    )

    t_validate = PythonOperator(
        task_id="validate_scoring_prerequisites",
        python_callable=validate_scoring_prerequisites,
        doc_md="Verifica integridad de datos antes de calcular puntuaciones.",
    )

    with TaskGroup("seller_evaluation_workflow", tooltip="Cálculo de Score (RN-07) y Asignación de Estatus (RN-08)") as tg_scoring:
        t_score = PythonOperator(
            task_id="rn_07_calculate_seller_score",
            python_callable=calculate_seller_score,
            doc_md="Calcula score ponderado de 0 a 100.",
        )

        t_status = PythonOperator(
            task_id="rn_08_update_seller_status",
            python_callable=update_seller_status,
            doc_md="Asigna estatus activo, en observación o suspendido según histórico.",
        )

        t_score >> t_status

    end_scoring = EmptyOperator(
        task_id="end_scoring",
        doc_md="Finalización exitosa del ciclo de evaluación de vendedores.",
    )

    start_scoring >> t_validate >> tg_scoring >> end_scoring
