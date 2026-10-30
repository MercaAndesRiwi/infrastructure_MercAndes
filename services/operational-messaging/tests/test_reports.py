from pathlib import Path

import pytest

from operational_messaging.reports import (
    generate_stockout_risk_report,
    write_stockout_csv,
)


def test_report_generation_rejects_an_unknown_report_type():
    message = {"message_id": "job-1", "payload": {"report_type": "unknown"}}

    with pytest.raises(ValueError, match="Unsupported report type"):
        generate_stockout_risk_report(message)


def test_write_stockout_csv_creates_the_job_file(tmp_path, monkeypatch):
    monkeypatch.setenv("REPORTS_OUTPUT_DIR", str(tmp_path))
    row = {
        "store_id": "S-1",
        "product_id": "P-1",
        "available_stock": 10,
        "avg_daily_sales": 4,
        "days_until_stockout": 2.5,
        "threshold_days": 3,
        "at_risk": True,
        "updated_at": "2026-10-29T00:00:00+00:00",
    }

    output_path = write_stockout_csv("job-1", [row])

    path = Path(output_path)
    assert path.name == "job-1.csv"
    content = path.read_text(encoding="utf-8")
    assert "store_id" in content
    assert "S-1" in content
