"""marketplace-api: seeded F4 events with lifecycle + fault injection."""

from __future__ import annotations

import json
import os

from fastapi import FastAPI, Query

from src.common.master_catalog import build_master_catalog
from src.simulators.marketplace_events.producer import (
    ProducerConfig,
    generate_events,
)

app = FastAPI(title="marketplace-api")
SEED = int(os.getenv("MASTER_SEED", "42"))
_CATALOG = build_master_catalog(seed=SEED)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "seed": SEED,
        "products": len(_CATALOG.products),
        "stores": len(_CATALOG.stores),
    }


@app.get("/events/next")
def next_events(
    n: int = Query(10, ge=1, le=500),
    duplicate_rate: float = Query(0.0, ge=0.0, le=1.0),
    ooo: bool = Query(False),
) -> dict:
    orders = max(1, (n + 3) // 4)
    config = ProducerConfig(
        seed=SEED,
        n_orders=orders,
        duplicate_rate=duplicate_rate,
        ooo_enabled=ooo,
    )
    events = generate_events(config, catalog=_CATALOG)[:n]
    return {"events": [json.loads(e.to_json()) for e in events]}