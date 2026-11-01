FROM apache/airflow:3.1.5

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    pika \
    boto3 \
    pandas \
    openpyxl
