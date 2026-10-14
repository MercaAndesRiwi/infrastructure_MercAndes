"""inventory-api: deterministic stock from the shared seeded catalog."""

from __future__ import annotations

import os

from fastapi import FastAPI, Query

from src.simulators.inventory_api.stock import stock_level

app = FastAPI(title="inventory-api")
SEED = int(os.getenv("MASTER_SEED", "42"))


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "seed": SEED}


@app.get("/stock")
def stock(sku: str = Query(...), store_id: str = Query(...)) -> dict:
    return {
        "sku": sku,
        "store_id": store_id,
        "stock": stock_level(SEED, sku, store_id),
    }