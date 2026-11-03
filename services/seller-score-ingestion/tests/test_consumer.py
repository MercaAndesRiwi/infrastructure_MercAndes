import json
from datetime import timezone

import pytest

from seller_score_ingestion.consumer import normalize_event


def test_normalize_marketplace_event():
    event = {
        "event_id": "event-1",
        "order_id": "order-1",
        "event_type": "PAID",
        "seller_id": "SEL-CO-PROD-001",
        "event_ts": "2026-10-20T12:00:00Z",
    }

    result = normalize_event(json.dumps(event).encode("utf-8"))

    assert result["event_type"] == "paid"
    assert result["source_seller_id"] == "SEL-CO-PROD-001"
    assert result["event_ts"].tzinfo == timezone.utc


def test_reject_marketplace_event_without_seller():
    event = {
        "event_id": "event-1",
        "order_id": "order-1",
        "event_type": "paid",
        "event_ts": "2026-10-20T12:00:00Z",
    }

    with pytest.raises(ValueError, match="seller_id"):
        normalize_event(json.dumps(event).encode("utf-8"))
