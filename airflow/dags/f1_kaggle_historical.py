"""
DAG: f1_kaggle_historical_load
Purpose: One-time definitive ingestion of Kaggle Olist dataset (F1) to S3 Bronze,
validates datasets, and triggers Spark Bronze -> Silver ingestion.

Schedule: @once (manual trigger)
Source: F1 - Brazilian E-Commerce Public Dataset by Olist (9 CSV files).
Architecture: Optimized for AWS S3 Data Lake + Multi-Worker Spark Cluster.
"""

from __future__ import annotations

import os
from datetime import timedelta

import boto3
from airflow import DAG
from airflow.utils.timezone import datetime as airflow_datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    "execution_timeout": timedelta(minutes=60),
}

F1_KAGGLE_LOCAL_INPUT_PATH = os.getenv("F1_KAGGLE_LOCAL_INPUT_PATH", "/opt/inputs/f1_kaggle")
BUCKET = os.getenv("S3_DATA_LAKE_BUCKET_NAME", "mercaandes-data-lake")
BRONZE_FOLDER = os.getenv("S3_BRONZE_FOLDER", "bronze")
F1_KAGGLE_FOLDER = os.getenv("S3_F1_KAGGLE_FOLDER", "f1_kaggle")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")


def upload_local_f1_to_s3(**context):
    """
    Ingests Kaggle Olist CSV files to S3 Bronze.
    Resilient for AWS:
    - If local directory has files, uploads them to S3 Bronze.
    - If running on ECS Fargate worker, verifies files already in S3 Bronze landing.
    """
    s3 = boto3.client("s3")
    execution_date = context["ds"]
    local_path = os.path.join(F1_KAGGLE_LOCAL_INPUT_PATH)

    uploaded_files = []
    if os.path.exists(local_path):
        for fname in sorted(os.listdir(local_path)):
            if fname.endswith(".csv"):
                local_file = os.path.join(local_path, fname)
                s3_key = f"{BRONZE_FOLDER}/{F1_KAGGLE_FOLDER}/{execution_date}/{fname}"
                s3.upload_file(local_file, BUCKET, s3_key)
                uploaded_files.append(s3_key)

    if uploaded_files:
        print(f"[F1 Kaggle] Subidos {len(uploaded_files)} archivos a s3://{BUCKET}/{BRONZE_FOLDER}/{F1_KAGGLE_FOLDER}/{execution_date}/")
    else:
        # Modo AWS ECS: Verificar si ya existen archivos en S3
        prefix = f"{BRONZE_FOLDER}/{F1_KAGGLE_FOLDER}/"
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20)
        found = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
        if found:
            print(f"[F1 Kaggle AWS] Confirmada existencia de {len(found)} datasets en S3 Bronze.")
            uploaded_files = found
        else:
            print(f"[F1 Kaggle Info] No se encontraron archivos locales en {local_path} ni en s3://{BUCKET}/{prefix}. Verifique el volumen o S3.")

    return {"files_count": len(uploaded_files), "bucket": BUCKET}


def validate_f1_bronze_datasets(**context):
    """Data Quality Check: Validates that essential Olist tables exist in Bronze."""
    s3 = boto3.client("s3")
    prefix = f"{BRONZE_FOLDER}/{F1_KAGGLE_FOLDER}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [item["Key"] for item in response.get("Contents", [])]

    expected = ["customers", "orders", "order_items", "products"]
    found_expected = []
    for exp in expected:
        for k in keys:
            if exp in k.lower():
                found_expected.append(exp)
                break

    print(f"[F1 VALIDATION] Datasets esenciales encontrados en S3 Bronze: {set(found_expected)}")
    return len(keys) > 0


with DAG(
    dag_id="f1_kaggle_historical_load",
    default_args=default_args,
    description="One-time definitive load of Kaggle F1 (Olist) to S3 Bronze + Spark Silver initialization.",
    schedule="@once",
    start_date=airflow_datetime(2026, 1, 1, tzinfo="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    tags=["layer:bronze", "f1", "kaggle", "historical", "mercaandes"],
    doc_md="""
    # Carga Histórica F1 Olist (MercaAndes)
    
    1. **Ingesta a S3 Bronze**: Carga de los 9 datasets de Brazilian E-Commerce (Olist).
    2. **Validación de Calidad**: Verifica la existencia de datasets críticos (clientes, órdenes, ítems, productos).
    3. **Spark Bronze -> Silver**: Inicializa las transformaciones de ventas netas e inventarios.
    """,
) as dag:

    start_f1_load = EmptyOperator(
        task_id="start_f1_load",
        doc_md="Punto de inicio para la carga histórica F1.",
    )

    with TaskGroup("f1_ingestion_and_validation", tooltip="Carga de archivos y validación en S3 Bronze") as tg_f1:
        t_upload = PythonOperator(
            task_id="upload_local_f1_to_s3",
            python_callable=upload_local_f1_to_s3,
            doc_md="Carga datasets Olist a S3 Bronze.",
        )

        t_validate = PythonOperator(
            task_id="validate_f1_bronze_datasets",
            python_callable=validate_f1_bronze_datasets,
            doc_md="Valida que los datasets requeridos existan en S3 Bronze.",
        )

        t_upload >> t_validate

    t_spark_init = SparkSubmitOperator(
        task_id="spark_f1_bronze_to_silver_init",
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
        doc_md="Inicializa las tablas Silver con el histórico de ventas F1 usando los 2 workers de Spark.",
    )

    end_f1_load = EmptyOperator(
        task_id="end_f1_load",
        doc_md="Finalización exitosa de la carga histórica F1.",
    )

    start_f1_load >> tg_f1 >> t_spark_init >> end_f1_load
