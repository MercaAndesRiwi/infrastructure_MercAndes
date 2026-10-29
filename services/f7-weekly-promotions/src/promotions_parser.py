"""Parser for F7 promotion files (Excel / CSV)."""

from __future__ import annotations

import io
from typing import List, Dict

import pandas as pd


def parse_promotions(file_contents: bytes, filename: str) -> List[Dict]:
    """
    Parse an .xlsx or .csv promotion file and return a list of dictionaries
    with normalized columns.

    Expected columns (case-insensitive):
      sku, seller_id, store_id, promotion_id, start_date, end_date,
      discount_pct, promo_price, channel
    """
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_contents))
    elif filename.lower().endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(file_contents), engine="openpyxl")
    else:
        raise ValueError("Unsupported format. Use .csv or .xlsx")

    # Normalize column names to lowercase
    df.columns = [c.strip().lower() for c in df.columns]

    required = {"sku", "seller_id", "store_id", "promotion_id", "start_date", "end_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Convert dates to ISO string
    for col in ("start_date", "end_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")

    return df.to_dict(orient="records")
