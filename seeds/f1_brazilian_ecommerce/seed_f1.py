"""seed_f1.py - One-time definitive load of Kaggle Olist (F1) to PostgreSQL.

Section 5.2 requirements:
  1. Fixed seed (42) for reproducibility.
  2. Idempotent: INSERT ... ON CONFLICT DO NOTHING.
  3. Parametrizable volume: --mode dev (sample) / --mode full (all).
  4. Referential integrity: load order respects foreign keys.
  5. Quality defects (5.3): configurable defect rate F1_DEFECT_RATE.
  6. Local and AWS: same script with DB_HOST=localhost or AWS RDS endpoint.

Usage:
  python seed_f1.py --mode full
  python seed_f1.py --mode dev
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
import kagglehub
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SEED = int(os.getenv("MASTER_SEED", "42"))
DEFECT_RATE = float(os.getenv("F1_DEFECT_RATE", "0.05"))  # 5% default
DB_HOST = os.getenv("POS_DB_HOST", "localhost")
DB_PORT = os.getenv("POS_DB_PORT", "5432")
DB_NAME = os.getenv("POS_DB_NAME", "merca_pos")
DB_USER = os.getenv("POS_DB_USER", "merca")
DB_PASSWORD = os.getenv("POS_DB_PASSWORD", "change_me")

random.seed(SEED)
np.random.seed(SEED)

# Load order respecting referential integrity (FKs)
LOAD_ORDER: List[str] = [
    "customers",
    "sellers",
    "products",
    "geolocation",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
]

# CSV file → MercaAndes table name mapping
FILE_TO_TABLE = {
    "customers.csv": "customers",
    "sellers.csv": "sellers",
    "products.csv": "products",
    "geolocation.csv": "geolocation",
    "orders.csv": "orders",
    "order_items.csv": "order_items",
    "order_payments.csv": "order_payments",
    "order_reviews.csv": "order_reviews",
    "product_category_name_translation.csv": "product_category_translation",
}

# Olist column → MercaAndes column mapping
COLUMN_MAPPING = {
    "customers": {
        "customer_id": "customer_id",
        "customer_unique_id": "customer_unique_id",
        "customer_city": "city",
        "customer_state": "state",
    },
    "sellers": {
        "seller_id": "seller_id",
        "seller_city": "city",
        "seller_state": "state",
    },
    "products": {
        "product_id": "product_id",
        "product_category_name": "category",
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "zip_prefix",
        "geolocation_city": "city",
        "geolocation_state": "state",
    },
    "orders": {
        "order_id": "order_id",
        "customer_id": "customer_id",
        "order_status": "status",
        "order_purchase_timestamp": "order_date",
    },
    "order_items": {
        "order_id": "order_id",
        "product_id": "product_id",
        "seller_id": "seller_id",
        "price": "unit_price",
        "freight_value": "freight_value",
    },
    "order_payments": {
        "order_id": "order_id",
        "payment_type": "method",
        "payment_installments": "installments",
        "payment_value": "amount",
    },
    "order_reviews": {
        "review_id": "review_id",
        "order_id": "order_id",
        "review_score": "rating",
    },
    "product_category_translation": {
        "product_category_name": "category_name_pt",
        "product_category_name_english": "category_name_en",
    },
}

# Status value mapping
STATUS_MAP = {
    "delivered": "completed",
    "canceled": "cancelled",
    "processing": "in_process",
    "shipped": "shipped",
    "created": "created",
    "approved": "approved",
    "unavailable": "unavailable",
    "invoiced": "invoiced",
}

# Payment method mapping
PAYMENT_METHOD_MAP = {
    "credit_card": "credit_card",
    "boleto": "boleto",
    "voucher": "voucher",
    "debit_card": "debit_card",
    "not_defined": "not_defined",
}


def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(url)


def inject_defects(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Inject quality defects with configurable rate (Section 5.3)."""
    if DEFECT_RATE <= 0:
        return df

    df = df.copy()
    n_rows = len(df)
    n_defects = max(1, int(n_rows * DEFECT_RATE))
    defect_idx = np.random.choice(n_rows, size=n_defects, replace=False)

    for idx in defect_idx:
        defect_type = np.random.choice(["null_date", "negative_price", "invalid_sku"])

        if defect_type == "null_date":
            date_cols = [c for c in df.columns if "date" in c or "timestamp" in c]
            if date_cols:
                df.at[idx, np.random.choice(date_cols)] = None

        elif defect_type == "negative_price":
            price_cols = [c for c in df.columns if "price" in c or "value" in c or "freight" in c]
            if price_cols:
                col = np.random.choice(price_cols)
                val = pd.to_numeric(df.at[idx, col], errors="coerce")
                if pd.notna(val) and val > 0:
                    df.at[idx, col] = round(-val, 2)

        elif defect_type == "invalid_sku":
            id_cols = [c for c in df.columns if c.endswith("_id") or c == "product_id"]
            if id_cols:
                col = np.random.choice(id_cols)
                df.at[idx, col] = f"INVALID_{df.at[idx, col]}"

    return df


def normalize_dataframe(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Normalize column names and values according to MercaAndes mapping."""
    df = df.copy()

    # Rename columns according to mapping
    if table in COLUMN_MAPPING:
        df = df.rename(columns=COLUMN_MAPPING[table])

    # Normalize status in orders
    if table == "orders" and "status" in df.columns:
        df["status"] = df["status"].map(STATUS_MAP).fillna(df["status"])

    # Normalize payment_type in order_payments
    if table == "order_payments" and "method" in df.columns:
        df["method"] = df["method"].map(PAYMENT_METHOD_MAP).fillna(df["method"])

    # Convert dates to ISO string if present
    for col in df.columns:
        if "date" in col or "timestamp" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    return df


def load_table(engine, table: str, df: pd.DataFrame) -> int:
    """Load a DataFrame to PostgreSQL idempotently (ON CONFLICT DO NOTHING)."""
    if df.empty:
        return 0

    df = normalize_dataframe(df, table)
    df = inject_defects(df, table)

    # Determine PK column for ON CONFLICT (assume first column as id)
    pk_column = df.columns[0]

    # Build INSERT ... ON CONFLICT DO NOTHING
    columns = ", ".join(df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])
    conflict_col = pk_column

    sql = text(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_col}) DO NOTHING"
    )

    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            params = {c: (None if pd.isna(v) else v) for c, v in row.items()}
            result = conn.execute(sql, params)
            inserted += result.rowcount

    return inserted


def main(mode: str = "full"):
    print(f"[F1 Seed] Starting Kaggle Olist load (mode={mode}, seed={SEED})")

    # 1. Download dataset with kagglehub
    print("[F1 Seed] Downloading dataset from Kaggle...")
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    print(f"[F1 Seed] Local path: {path}")

    # 2. List available CSV files
    csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
    print(f"[F1 Seed] Found CSV files: {csv_files}")

    engine = get_engine()

    # 3. Load in referential integrity order
    total_inserted = 0
    for logical_name in LOAD_ORDER:
        # Find corresponding CSV file
        csv_file = next(
            (f for f in csv_files if FILE_TO_TABLE.get(f) == logical_name),
            None,
        )
        if csv_file is None:
            print(f"[F1 Seed] Skipping {logical_name}: no matching CSV file found")
            continue

        file_path = os.path.join(path, csv_file)
        df = pd.read_csv(file_path)

        # Dev mode: small sample
        if mode == "dev":
            df = df.sample(n=min(len(df), 200), random_state=SEED)

        inserted = load_table(engine, logical_name, df)
        total_inserted += inserted
        print(f"[F1 Seed] {logical_name}: {inserted} rows inserted (idempotent)")

    print(f"[F1 Seed] Completed. Total inserted: {total_inserted} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed F1 - Kaggle Olist")
    parser.add_argument(
        "--mode",
        choices=["dev", "full"],
        default="full",
        help="Load volume: dev (small sample) or full (all data)",
    )
    args = parser.parse_args()
    main(args.mode)
