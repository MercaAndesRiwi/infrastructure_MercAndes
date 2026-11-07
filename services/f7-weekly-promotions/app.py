"""f7-weekly-promotions: weekly ingestion of Excel/CSV promotion files (F7) and real-time promotion request evaluation (RF-04 / RN-05).

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
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, status
from pydantic import BaseModel, Field

from src.s3_client import upload_promotion_file
from src.publisher import publish_promotion_event

app = FastAPI(title="f7-weekly-promotions")

SEED = int(os.getenv("MASTER_SEED", "42"))

# In-memory audit log of promotion requests for RF-04 / RN-05 auditing
PROMOTION_REQUESTS_AUDIT: list[dict] = []


class PromotionRequest(BaseModel):
    sku: str = Field(..., description="Product SKU (e.g. SKU-HEA-CO-00001)")
    store_id: str = Field(..., description="Store/Branch ID (e.g. STO-CO-FIS-001)")
    seller_id: Optional[str] = Field(default=None, description="Seller ID")
    discount_pct: float = Field(..., ge=0.0, le=100.0, description="Discount percentage")
    start_date: str = Field(..., description="Promotion start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="Promotion end date (YYYY-MM-DD)")
    regular_price: float = Field(default=50.0, ge=0.0, description="Regular unit price in USD")
    available_stock_override: Optional[float] = Field(default=None, description="Override for testing inventory stock")
    avg_daily_sales_override: Optional[float] = Field(default=None, description="Override for testing avg daily sales")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "seed": SEED, "service": "f7-weekly-promotions"}


@app.post(
    "/api/v1/promotions/request",
    status_code=status.HTTP_200_OK,
    summary="Register promotion request and evaluate inventory coverage (RF-04 / RN-05)",
    description="Evaluates SKU inventory coverage against the mandatory 7-day threshold (RN-05). Responds APROBADA or RECHAZADA with potential sales lost.",
)
def request_promotion(req: PromotionRequest):
    """
    RF-04: Expose an API to register promotion requests and respond
    APROBADA / RECHAZADA according to RN-05 (minimum 7 days inventory coverage).
    """
    request_id = f"PRM-REQ-{uuid.uuid4().hex[:8].upper()}"

    # Determine available stock and avg daily sales
    if req.available_stock_override is not None and req.avg_daily_sales_override is not None:
        available_stock = float(req.available_stock_override)
        avg_daily_sales = float(req.avg_daily_sales_override)
    else:
        # Query PostgreSQL if available, otherwise deterministic simulation based on SKU hash
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv("POS_DB_HOST", "postgres"),
                port=int(os.getenv("POS_DB_PORT", "5432")),
                dbname=os.getenv("POS_DB_NAME", "merca_pos"),
                user=os.getenv("POS_DB_USER", "pos_user"),
                password=os.getenv("POS_DB_PASSWORD", ""),
                connect_timeout=2,
            )
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COALESCE(ai.available_stock, 150), COALESCE(sr.avg_daily_sales, 10)
                FROM public.gold_available_inventory ai
                LEFT JOIN public.gold_stockout_risk sr ON ai.store_id = sr.store_id AND ai.product_id = sr.product_id
                WHERE ai.store_id = %s
                LIMIT 1;
                """,
                (req.store_id,),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            available_stock = float(row[0]) if row else 150.0
            avg_daily_sales = float(row[1]) if row and row[1] else 10.0
        except Exception:
            # Deterministic calculation for offline / test scenarios
            sku_hash = sum(ord(c) for c in req.sku)
            available_stock = float((sku_hash % 200) + 20)
            avg_daily_sales = float((sku_hash % 25) + 2)

    coverage_days = round((available_stock / avg_daily_sales), 2) if avg_daily_sales > 0 else (999.0 if available_stock > 0 else 0.0)

    # RN-05 Business Rule: Minimum 7 days inventory coverage required
    is_approved = coverage_days >= 7.0

    promotional_price = round(req.regular_price * (1.0 - (req.discount_pct / 100.0)), 2)
    projected_sales_units = round(avg_daily_sales * 7.0, 1)

    if is_approved:
        decision_status = "APROBADA"
        reason = f"Inventory coverage ({coverage_days} days) satisfies RN-05 threshold (>= 7 days)."
        projected_lost_sales_usd = 0.0
    else:
        decision_status = "RECHAZADA"
        reason = f"Inventory coverage ({coverage_days} days) is below mandatory 7 days threshold (RN-05)."
        # Projected revenue that would have been achieved in 7 days of promo
        projected_lost_sales_usd = round(projected_sales_units * promotional_price, 2)

    audit_entry = {
        "request_id": request_id,
        "sku": req.sku,
        "store_id": req.store_id,
        "seller_id": req.seller_id,
        "discount_pct": req.discount_pct,
        "status": decision_status,
        "approved": is_approved,
        "coverage_days": coverage_days,
        "threshold_required_days": 7.0,
        "available_stock": available_stock,
        "avg_daily_sales": avg_daily_sales,
        "regular_price_usd": req.regular_price,
        "promotional_price_usd": promotional_price,
        "projected_lost_sales_usd": projected_lost_sales_usd,
        "reason": reason,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }

    PROMOTION_REQUESTS_AUDIT.append(audit_entry)

    # Persist to database if available
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POS_DB_HOST", "postgres"),
            port=int(os.getenv("POS_DB_PORT", "5432")),
            dbname=os.getenv("POS_DB_NAME", "merca_pos"),
            user=os.getenv("POS_DB_USER", "pos_user"),
            password=os.getenv("POS_DB_PASSWORD", ""),
            connect_timeout=2,
        )
        cur = conn.cursor()
        cur.execute(
            """
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
            INSERT INTO public.gold_promotions_audit (
                promotion_id, sku, product_name, category_name, store_id,
                discount_pct, available_stock, avg_daily_sales, coverage_days,
                status, rejection_reason, projected_lost_sales_usd, evaluated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (promotion_id) DO NOTHING;
            """,
            (
                request_id,
                req.sku,
                f"Product {req.sku}",
                "Promotions",
                req.store_id,
                req.discount_pct,
                available_stock,
                avg_daily_sales,
                coverage_days,
                decision_status,
                reason,
                projected_lost_sales_usd,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

    return audit_entry


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
