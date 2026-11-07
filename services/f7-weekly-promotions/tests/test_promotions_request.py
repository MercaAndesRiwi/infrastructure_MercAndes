"""Unit tests for RF-04: Promotion registration and RN-05 inventory coverage evaluation."""

import pytest
from fastapi.testclient import TestClient

from app import app


@pytest.fixture
def client():
    return TestClient(app)


def test_promotion_request_approved_rn05(client):
    """TC-RF04-01: Promotion is approved when coverage is >= 7 days."""
    payload = {
        "sku": "SKU-TEST-001",
        "store_id": "STORE-01",
        "seller_id": "SEL-001",
        "discount_pct": 20.0,
        "start_date": "2026-11-10",
        "end_date": "2026-11-17",
        "regular_price": 100.0,
        "available_stock_override": 100.0,
        "avg_daily_sales_override": 10.0,  # Coverage = 10 days (>= 7)
    }

    response = client.post("/api/v1/promotions/request", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "APROBADA"
    assert data["approved"] is True
    assert data["coverage_days"] == 10.0
    assert data["projected_lost_sales_usd"] == 0.0
    assert "satisfies RN-05" in data["reason"]


def test_promotion_request_rejected_rn05(client):
    """TC-RF04-02: Promotion is rejected when coverage is < 7 days and computes lost sales."""
    payload = {
        "sku": "SKU-TEST-002",
        "store_id": "STORE-01",
        "seller_id": "SEL-002",
        "discount_pct": 15.0,
        "start_date": "2026-11-10",
        "end_date": "2026-11-17",
        "regular_price": 50.0,
        "available_stock_override": 25.0,
        "avg_daily_sales_override": 5.0,  # Coverage = 5 days (< 7)
    }

    response = client.post("/api/v1/promotions/request", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "RECHAZADA"
    assert data["approved"] is False
    assert data["coverage_days"] == 5.0
    # Projected lost sales: 5 sales/day * 7 days * (50 * 0.85 = 42.5) = 35 * 42.5 = 1487.5
    assert data["projected_lost_sales_usd"] == 1487.5
    assert "below mandatory 7 days" in data["reason"]
