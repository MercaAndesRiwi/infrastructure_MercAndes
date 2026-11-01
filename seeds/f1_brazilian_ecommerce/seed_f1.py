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
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Load environment variables strictly from .env if present
try:
    from dotenv import load_dotenv
    cur = Path(__file__).resolve().parent
    for _ in range(4):
        candidate = cur / ".env"
        if candidate.exists():
            load_dotenv(dotenv_path=candidate)
            break
        if cur.parent == cur:
            break
        cur = cur.parent
except Exception:
    pass

# ---------------------------------------------------------------------------
# Configuration (Strictly read from environment variables without defaults)
# ---------------------------------------------------------------------------
DEFAULT_INPUT_DIR = os.environ.get(
    "F1_KAGGLE_LOCAL_INPUT_PATH",
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "inputs", "f1_kaggle")),
)
SEED = int(os.environ.get("MASTER_SEED", "42"))
DEFECT_RATE = float(os.environ.get("F1_DEFECT_RATE", "0.05"))
DB_HOST = os.environ.get("POS_DB_HOST")
DB_PORT = os.environ.get("POS_DB_PORT")
DB_NAME = os.environ.get("POS_DB_NAME")
DB_USER = os.environ.get("POS_DB_USER")
DB_PASSWORD = os.environ.get("POS_DB_PASSWORD")

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
    "olist_customers_dataset.csv": "customers",
    "sellers.csv": "sellers",
    "olist_sellers_dataset.csv": "sellers",
    "products.csv": "products",
    "olist_products_dataset.csv": "products",
    "geolocation.csv": "geolocation",
    "olist_geolocation_dataset.csv": "geolocation",
    "orders.csv": "orders",
    "olist_orders_dataset.csv": "orders",
    "order_items.csv": "order_items",
    "olist_order_items_dataset.csv": "order_items",
    "order_payments.csv": "order_payments",
    "olist_order_payments_dataset.csv": "order_payments",
    "order_reviews.csv": "order_reviews",
    "olist_order_reviews_dataset.csv": "order_reviews",
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
    """Create SQLAlchemy engine strictly using database environment variables without defaults."""
    missing = [name for name, val in [
        ("POS_DB_HOST", DB_HOST),
        ("POS_DB_PORT", DB_PORT),
        ("POS_DB_NAME", DB_NAME),
        ("POS_DB_USER", DB_USER),
        ("POS_DB_PASSWORD", DB_PASSWORD),
    ] if not val]

    if missing:
        raise EnvironmentError(
            f"Missing required database environment variables: {', '.join(missing)}. "
            "Variables must be defined strictly in the environment without defaults."
        )

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
    """Load a DataFrame to PostgreSQL using a staging anti-join pattern.

    1. Normalizes columns and applies controlled quality defects in staging memory.
    2. Inspects existing primary keys in destination PostgreSQL table.
    3. Performs left anti-join to isolate strictly new records (zero overwrite).
    4. Batch inserts only new delta records.
    """
    if df.empty:
        return 0

    df = normalize_dataframe(df, table)
    df = inject_defects(df, table)

    # Primary key column (first column in normalized dataframe)
    pk_column = df.columns[0]

    # Step 1: Query existing keys from target table
    existing_keys = set()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT {pk_column} FROM {table}"))
            existing_keys = {row[0] for row in result.fetchall() if row[0] is not None}
    except Exception:
        # Table might be uninitialized or empty
        existing_keys = set()

    # Step 2: Left anti-join: filter out records that already exist in database
    new_df = df[~df[pk_column].isin(existing_keys)]
    if new_df.empty:
        print(f"[F1 Seed Staging] {table}: all {len(df)} records already exist in database. 0 rows inserted.")
        return 0

    # Step 3: Batch insert strictly new records without overwriting
    columns = ", ".join(new_df.columns)
    placeholders = ", ".join([f":{c}" for c in new_df.columns])
    sql = text(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk_column}) DO NOTHING"
    )

    inserted = 0
    with engine.begin() as conn:
        records = [
            {c: (None if pd.isna(v) else v) for c, v in row.items()}
            for _, row in new_df.iterrows()
        ]
        result = conn.execute(sql, records)
        inserted = result.rowcount if result.rowcount > 0 else len(records)

    print(
        f"[F1 Seed Staging] {table}: {len(df)} total in source, "
        f"{len(existing_keys)} already in DB, {inserted} new records inserted."
    )
    return inserted


def main(mode: str = "full", input_dir: str | None = None):
    print(f"[F1 Seed] Starting Kaggle Olist load (mode={mode}, seed={SEED})")

    # 1. Resolve input directory (prioritize local inputs/f1_kaggle)
    path = input_dir or DEFAULT_INPUT_DIR
    if os.path.exists(path) and any(f.endswith(".csv") for f in os.listdir(path)):
        print(f"[F1 Seed] Using local input directory: {path}")
    else:
        # Fallback to kagglehub if local directory is missing
        print(f"[F1 Seed] Local path '{path}' not found. Attempting download from Kaggle...")
        try:
            import kagglehub
            path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
            print(f"[F1 Seed] Downloaded to: {path}")
        except Exception as exc:
            raise FileNotFoundError(
                f"Input directory '{path}' not found and Kaggle download failed: {exc}"
            ) from exc

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
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help="Path to directory containing Olist CSV files (defaults to inputs/f1_kaggle)",
    )
    args = parser.parse_args()
    main(mode=args.mode, input_dir=args.input_dir)
