"""Stockout-risk report: PostgreSQL query, CSV output and queue consumer."""

from __future__ import annotations

import csv
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

from operational_messaging.config import REPORT_QUEUE
from operational_messaging.rabbitmq import consume_queue

REPORT_COLUMNS = [
    "store_id",
    "product_id",
    "available_stock",
    "avg_daily_sales",
    "days_until_stockout",
    "threshold_days",
    "at_risk",
    "updated_at",
]


def get_stockout_rows() -> list[dict]:
    connection = psycopg2.connect(
        host=os.getenv("POS_DB_HOST", "postgres"),
        port=os.getenv("POS_DB_PORT", "5432"),
        dbname=os.getenv("POS_DB_NAME", "merca_pos"),
        user=os.getenv("POS_DB_USER", "pos_user"),
        password=os.getenv("POS_DB_PASSWORD", ""),
    )
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT store_id, product_id, available_stock, avg_daily_sales,
                       days_until_stockout, threshold_days, at_risk, updated_at
                FROM gold_stockout_risk
                ORDER BY at_risk DESC, days_until_stockout ASC NULLS LAST
                """
            )
            return [dict(row) for row in cursor.fetchall()]
    finally:
        connection.close()


def write_stockout_csv(job_id: str, rows: list[dict]) -> str:
    output_dir = Path(os.getenv("REPORTS_OUTPUT_DIR", "/app/reports"))
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{job_id}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as report_file:
        writer = csv.DictWriter(report_file, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return str(output_path)


def generate_stockout_risk_report(message: dict) -> str:
    report_type = message.get("payload", {}).get("report_type")
    if report_type != "stockout_risk":
        raise ValueError(f"Unsupported report type: {report_type}")

    rows = get_stockout_rows()
    return write_stockout_csv(message["message_id"], rows)


def process_report(message: dict) -> None:
    output_path = generate_stockout_risk_report(message)
    print(f"[report] generated {message['message_id']} at {output_path}")


def main() -> None:
    consume_queue(REPORT_QUEUE, process_report)


if __name__ == "__main__":
    main()
