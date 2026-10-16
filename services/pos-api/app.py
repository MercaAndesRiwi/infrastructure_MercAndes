"""pos-api: catalog reads (no DB) + POS generation trigger (needs Postgres)."""

from __future__ import annotations

import os
import random
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query

from src.common.config import COUNTRIES
from src.common.master_catalog import build_master_catalog
from src.simulators.pos_seed.config import (
    COUNTRIES as POS_COUNTRIES,
)
from src.simulators.pos_seed.config import (
    CURRENCIES,
    POS_END_DATE,
    POS_START_DATE,
)
from src.simulators.pos_seed.connection import get_engine
from src.simulators.pos_seed.exchange_rates import load_rate_cache
from src.simulators.pos_seed.main import (
    generate_country_sales,
    load_seasonality_data,
)

app = FastAPI(title="pos-api")
SEED = int(os.getenv("MASTER_SEED", "42"))
_CATALOG = build_master_catalog(seed=SEED)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "seed": SEED}


@app.get("/catalog/product/random")
def random_product() -> dict:
    product = random.Random().choice(_CATALOG.active_products)
    return {
        "sku": product.sku,
        "name": product.name,
        "category": product.category,
        "country": product.country,
        "seller_id": product.seller_id,
        "base_price_usd": product.base_price_usd,
    }


@app.post("/sales/generate")
def generate_sales(
    country: str = Query(..., pattern="^(CO|PE|EC|BO|CL)$"),
    n: int = Query(10, ge=1, le=5000),
) -> dict:
    engine = None
    try:
        schema = POS_COUNTRIES[country]
        engine = get_engine()
        start = datetime.fromisoformat(POS_START_DATE)
        end = datetime.fromisoformat(POS_END_DATE)
        with engine.begin() as connection:
            rate_cache = load_rate_cache(connection, CURRENCIES)
            holidays, promotions, promo_dates = load_seasonality_data(
                connection, start, end
            )
            generate_country_sales(
                connection, country, schema, n, rate_cache,
                promotions, holidays, promo_dates,
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        if engine is not None:
            engine.dispose()
    return {"country": country, "requested": n, "schema": COUNTRIES[country]}