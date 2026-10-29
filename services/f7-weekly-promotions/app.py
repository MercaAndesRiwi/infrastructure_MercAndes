"""f7-weekly-promotions: weekly ingestion of Excel/CSV promotion files (F7).

Receives the weekly promotion plan from the commercial team, validates it
against the master catalog, uploads it to S3 (Bronze layer), and publishes
events to Kafka and RabbitMQ so Airflow/Spark can process it
(Bronze -> Silver -> Gold).

Connects to:
  - S3 bucket: bronze/f7_promotions/ (Bronze layer landing)
  - Kafka topic: mercaandes.promotions.weekly (event notification)
  - RabbitMQ queue: mercaandes.promotions.weekly.ingest (for EC2 workers)
  - Local file: /opt/inputs/f7_promotions/F7_promotions.xlsx (mounted volume)
"""

from __future__ import annotations

import os
from fastapi import FastAPI, UploadFile, File, HTTPException

from src.s3_client import upload_promotion_file
from src.publisher import publish_promotion_event

app = FastAPI(title="f7-weekly-promotions")

SEED = int(os.getenv("MASTER_SEED", "42"))


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "seed": SEED, "service": "f7-weekly-promotions"}


@app.post("/api/v1/promotions/ingest")
async def ingest_promotions(file: UploadFile = File(...)):
    """
    Receives the weekly promotion file (F7): .xlsx or .csv.

    Flow:
    1. Validate file extension.
    2. Upload to S3 under bronze/f7_promotions/YYYY/MM/DD/.
    3. Publish event to Kafka topic 'mercaandes.promotions.weekly'.
    4. Publish event to RabbitMQ queue 'mercaandes.promotions.weekly.ingest'.
    5. Return s3_key and event_id so Airflow can trigger the weekly DAG.
    """
    if not file.filename or not (
        file.filename.lower().endswith(".xlsx")
        or file.filename.lower().endswith(".csv")
    ):
        raise HTTPException(status_code=400, detail="Only .xlsx or .csv files are accepted")

    contents = await file.read()

    # 1. Upload to S3 (Bronze / landing)
    s3_key = upload_promotion_file(contents, file.filename)

    # 2. Publish event so Airflow/Spark can process it
    event_id = publish_promotion_event(
        source="f7_promotions",
        s3_key=s3_key,
        filename=file.filename,
        seed=SEED,
    )

    return {
        "status": "accepted",
        "s3_key": s3_key,
        "event_id": event_id,
        "message": "Promotion file received and published to S3 + Kafka + RabbitMQ",
    }


@app.post("/api/v1/rabbit/ingest")
def rabbit_ingest(payload: dict):
    """
    Endpoint for RabbitMQ workers on EC2 (AWS) to report consumption status
    or send promotions directly.
    Connects to RabbitMQ queue: mercaandes.promotions.weekly.ingest
    """
    from src.publisher import publish_to_rabbitmq
    publish_to_rabbitmq(queue="mercaandes.promotions.weekly.ingest", message=payload)
    return {"status": "received", "queue": "mercaandes.promotions.weekly.ingest"}
