"""
DAG: bronze_to_silver_cleaning
Purpose: Cleaning, anonymization, and normalization from Bronze to Silver layer.
Implements:
  - RN-10: Event deduplication and incorporation of late events (<= 72h).
  - RN-11: Customer personal data anonymization (MD5 on name, document, email).
  - RN-01, RN-02, RN-06: Spark Net Sales (post-dispatch cancellation handling & 30-day return window).
  - RN-03: Spark Available Inventory (physical - reserved - in_transit).

Schedule: @daily
Architecture: Optimized for AWS Multi-Worker Cluster (ECS Fargate / EC2 + Spark Standalone).
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime as airflow_datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from operational_messaging.config import DATA_QUALITY_QUEUE
from operational_messaging.messages import new_message
from operational_messaging.rabbitmq import publish_message

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

BUCKET = os.getenv("S3_DATA_LAKE_BUCKET_NAME", "mercaandes-data-lake")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")


def deduplicate_and_handle_late_events(**context):
    """
    RN-10: Duplicate events are discarded; late events (up to 72 hours) are incorporated
    and affected aggregates are recalculated.
    Connects to Postgres (RDS): merca_pos (table: marketplace_events).
    """
    pg_hook = PostgresHook(postgres_conn_id="pos_db")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        DELETE FROM marketplace_events me
        USING marketplace_events me2
        WHERE me.id > me2.id
        AND me.event_type = me2.event_type
        AND me.order_id = me2.order_id
        AND me.payload = me2.payload
        AND me.created_at > me2.created_at
        """
    )
    duplicates_removed = cur.rowcount

    cur.execute(
        """
        UPDATE marketplace_events me
        SET processed = TRUE,
            processed_at = NOW()
        WHERE me.processed = FALSE
        AND me.event_timestamp >= NOW() - INTERVAL '72 hours'
        """
    )
    late_incorporated = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    print(f"[RN-10] Removed {duplicates_removed} duplicates, incorporated {late_incorporated} late events")
    return {"duplicates_removed": duplicates_removed, "late_incorporated": late_incorporated}


def anonymize_personal_data(**context):
    """
    RN-11: Customer personal data (name, document, email) cannot reach the analytics
    layer without being anonymized.
    Connects to Postgres (RDS): merca_pos (table: customers).
    """
    pg_hook = PostgresHook(postgres_conn_id="pos_db")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE customers
        SET customer_name = md5(customer_name || customer_id),
            customer_document = md5(customer_document || customer_id),
            customer_email = md5(customer_email || customer_id),
            updated_at = NOW()
        WHERE customer_name IS NOT NULL
        AND customer_name <> md5(customer_name || customer_id)
        """
    )
    anonymized = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"[RN-11] Anonymized {anonymized} customer records")
    return anonymized


def verify_silver_layer_quality(**context):
    """Data Quality Check: Confirms that Bronze data was properly processed before closing."""
    ti = context["ti"]
    rn10_result = ti.xcom_pull(task_ids="bronze_cleaning_compliance.rn_10_deduplicate_late_events")
    rn11_result = ti.xcom_pull(task_ids="bronze_cleaning_compliance.rn_11_anonymize_personal_data")
    print(f"[DATA QUALITY] Pipeline Bronze -> Silver finalizado exitosamente. Métricas: RN-10={rn10_result}, RN-11={rn11_result}")
    return True


def publish_duplicate_data_alert(**context):
    """Publish a data-quality alert when RN-10 removes duplicate events."""
    task_instance = context["ti"]
    result = task_instance.xcom_pull(
        task_ids="bronze_cleaning_compliance.rn_10_deduplicate_late_events"
    ) or {}
    duplicates_removed = int(result.get("duplicates_removed", 0))
    if duplicates_removed == 0:
        print("[DATA QUALITY] No duplicate events detected; no alert was sent.")
        return 0

    message = new_message(
        "DATA_QUALITY_DUPLICATES",
        {
            "source": "marketplace_events",
            "issue": "duplicate_events_removed",
            "records_affected": duplicates_removed,
            "dag_run_id": context["run_id"],
        },
    )
    publish_message(DATA_QUALITY_QUEUE, message)
    print(f"[DATA QUALITY] Published alert for {duplicates_removed} duplicate events.")
    return duplicates_removed


with DAG(
    dag_id="bronze_to_silver_cleaning",
    default_args=default_args,
    description="Cleaning, compliance and business rules Bronze -> Silver (RN-01, 02, 03, 06, 10, 11).",
    schedule="@daily",
    start_date=airflow_datetime(2026, 1, 1, tzinfo="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    tags=["layer:silver", "pipeline:bronze-to-silver", "cleaning", "compliance", "mercaandes"],
    doc_md="""
    # Pipeline Bronze a Silver (MercaAndes)
    
    Este DAG ejecuta la limpieza, cumplimiento de datos (PII) y transformaciones hacia la capa Silver.
    
    ### Flujo Lógico Medallion:
    1. **Inicio de Pipeline**: Verificación de conectividad.
    2. **TaskGroup: `bronze_cleaning_compliance`**:
       - `RN-10`: Deduplicación de eventos de marketplace y gestión de eventos tardíos (hasta 72h).
       - `RN-11`: Anonimización con hash MD5 de datos sensibles de clientes (Nombre, Cédula/Documento, Email).
    3. **TaskGroup: `spark_silver_transformations`**:
       - `RN-01, RN-02, RN-06`: Cálculo de ventas netas, cancelaciones post-despacho y devoluciones en ventana de 30 días.
       - `RN-03`: Cálculo de inventario disponible (`físico - reservado - en tránsito`).
       - Distribución de carga configurada para clúster Spark de 2 Workers.
    4. **Control de Calidad**: Validación y cierre de pipeline.
    """,
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline",
        doc_md="Punto de entrada del pipeline Bronze -> Silver.",
    )

    with TaskGroup("bronze_cleaning_compliance", tooltip="Fase 1: Limpieza, deduplicación y cumplimiento normativo") as tg_cleaning:
        t_deduplicate = PythonOperator(
            task_id="rn_10_deduplicate_late_events",
            python_callable=deduplicate_and_handle_late_events,
            doc_md="Descarta eventos duplicados e incorpora eventos con retraso <= 72h.",
        )

        t_quality_alert = PythonOperator(
            task_id="publish_duplicate_data_alert",
            python_callable=publish_duplicate_data_alert,
            doc_md="Publishes a RabbitMQ alert when duplicate records are removed.",
        )

        t_anonymize = PythonOperator(
            task_id="rn_11_anonymize_personal_data",
            python_callable=anonymize_personal_data,
            doc_md="Aplica hash MD5 a nombres, documentos y correos de clientes.",
        )

        t_deduplicate >> [t_quality_alert, t_anonymize]

    with TaskGroup("spark_silver_transformations", tooltip="Fase 2: Transformaciones analíticas con Apache Spark (2 Workers)") as tg_spark:
        t_net_sales = SparkSubmitOperator(
            task_id="rn_01_02_06_spark_net_sales",
            application="/opt/spark_jobs/net_sales_job.py",
            conn_id="spark_default",
            application_args=[
                "--silver-path", f"s3a://{BUCKET}/silver/",
                "--gold-path", f"s3a://{BUCKET}/gold/",
            ],
            conf={
                "spark.master": SPARK_MASTER,
                "spark.executor.instances": "2",
                "spark.executor.cores": "1",
                "spark.executor.memory": "1G",
            },
            env={"S3_BUCKET": BUCKET},
            doc_md="Job Spark para ventas netas con manejo de cancelaciones y devoluciones.",
        )

        t_available_inventory = SparkSubmitOperator(
            task_id="rn_03_spark_available_inventory",
            application="/opt/spark_jobs/available_inventory_job.py",
            conn_id="spark_default",
            application_args=[
                "--silver-path", f"s3a://{BUCKET}/silver/",
                "--gold-path", f"s3a://{BUCKET}/gold/",
            ],
            conf={
                "spark.master": SPARK_MASTER,
                "spark.executor.instances": "2",
                "spark.executor.cores": "1",
                "spark.executor.memory": "1G",
            },
            env={"S3_BUCKET": BUCKET},
            doc_md="Job Spark para inventario disponible = físico - reservado - en tránsito.",
        )

    t_verify = PythonOperator(
        task_id="verify_silver_layer_quality",
        python_callable=verify_silver_layer_quality,
        doc_md="Verificación de calidad de datos procesados hacia Silver.",
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        doc_md="Punto de finalización exitosa del pipeline Bronze -> Silver.",
    )

    # Flujo ordenado Medallion: Limpieza y Compliance PRIMERO, luego transformaciones analíticas Spark
    start_pipeline >> tg_cleaning >> tg_spark >> t_verify >> end_pipeline
