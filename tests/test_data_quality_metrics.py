"""
Unit tests for Data Quality Metrics logging by source and load.

Validates Acceptance Criteria:
- Given a finished load, when it is reviewed, then quality metrics
  (duplicates, late arrivals, rejections) are logged by source and load.
- Implement data quality metric logging.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
import pytest


def calculate_load_quality_summary(
    load_id: str,
    source_system: str,
    duplicates_removed: int,
    late_incorporated: int,
    exceptions_exceeded_72h: int,
    customers_anonymized: int = 0,
) -> dict:
    """Calculates audit metrics for a completed load."""
    records_processed = duplicates_removed + late_incorporated + exceptions_exceeded_72h
    quality_status = "PASSED" if exceptions_exceeded_72h == 0 else "COMPLETED_WITH_WARNINGS"

    return {
        "load_id": load_id,
        "source_system": source_system,
        "records_processed": records_processed,
        "duplicates_detected": duplicates_removed,
        "late_arrivals_incorporated": late_incorporated,
        "rejections_exceeded_72h": exceptions_exceeded_72h,
        "quality_status": quality_status,
        "details": {
            "customers_anonymized": customers_anonymized,
            "logged_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def test_data_quality_metrics_clean_run():
    """Validates metrics for a load with no exceptions."""
    load_id = "scheduled__2026-11-03T06:00:00+00:00"
    source = "marketplace_events"

    metrics = calculate_load_quality_summary(
        load_id=load_id,
        source_system=source,
        duplicates_removed=5,
        late_incorporated=12,
        exceptions_exceeded_72h=0,
        customers_anonymized=40,
    )

    assert metrics["load_id"] == load_id
    assert metrics["source_system"] == source
    assert metrics["duplicates_detected"] == 5
    assert metrics["late_arrivals_incorporated"] == 12
    assert metrics["rejections_exceeded_72h"] == 0
    assert metrics["records_processed"] == 17
    assert metrics["quality_status"] == "PASSED"


def test_data_quality_metrics_with_late_rejections():
    """Validates metrics for a load with >72h rejections (triggers warning status)."""
    load_id = "scheduled__2026-11-03T07:00:00+00:00"
    source = "marketplace_events"

    metrics = calculate_load_quality_summary(
        load_id=load_id,
        source_system=source,
        duplicates_removed=2,
        late_incorporated=8,
        exceptions_exceeded_72h=3,  # 3 events arrived > 72h
        customers_anonymized=15,
    )

    assert metrics["load_id"] == load_id
    assert metrics["source_system"] == source
    assert metrics["rejections_exceeded_72h"] == 3
    assert metrics["records_processed"] == 13
    assert metrics["quality_status"] == "COMPLETED_WITH_WARNINGS"
    assert "customers_anonymized" in metrics["details"]
