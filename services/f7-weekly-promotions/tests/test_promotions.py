import io
import pytest
from fastapi.testclient import TestClient

from app import app
from src.promotions_parser import parse_promotions


@pytest.fixture
def client():
    return TestClient(app)


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "f7-weekly-promotions"


def test_ingest_rejects_invalid_extension(client):
    file_content = b"dummy content"
    files = {"file": ("promotions.txt", io.BytesIO(file_content), "text/plain")}
    response = client.post("/api/v1/promotions/ingest", files=files)
    assert response.status_code == 400
    assert "Only .xlsx or .csv files are accepted" in response.json()["detail"]


def test_parse_promotions_csv():
    csv_data = (
        b"sku,seller_id,store_id,promotion_id,start_date,end_date,discount_pct\n"
        b"SKU001,SEL01,STR01,PRM01,2025-01-01,2025-01-07,15.0\n"
    )
    records = parse_promotions(csv_data, "promo.csv")
    assert len(records) == 1
    assert records[0]["sku"] == "SKU001"
    assert records[0]["promotion_id"] == "PRM01"
