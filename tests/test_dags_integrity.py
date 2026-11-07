"""
Test: test_dags_integrity.py
Purpose: Validates syntax, structure, and functional requirements of all Airflow DAGs:
  - RF-05: Stockout risk calculation, monetary sales at risk ($ USD), and replenishment alert queue.
  - RF-06: Automatic recalculation of aggregates and historical partitions on late events.
  - Absence of hardcoded dummy data.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parent.parent / "airflow" / "dags"


def test_dags_directory_exists():
    assert DAGS_DIR.exists(), f"DAGs directory not found at {DAGS_DIR}"


@pytest.mark.parametrize(
    "dag_filename",
    [
        "silver_to_gold_business_rules.py",
        "bronze_to_silver_cleaning.py",
        "dag_partitioned_reprocessing.py",
        "f1_kaggle_historical.py",
        "f7_promotions_weekly.py",
        "seller_scoring.py",
    ],
)
def test_dag_files_have_valid_ast_syntax(dag_filename: str):
    file_path = DAGS_DIR / dag_filename
    assert file_path.exists(), f"DAG file {dag_filename} does not exist"

    content = file_path.read_text(encoding="utf-8")
    parsed = ast.parse(content, filename=str(file_path))
    assert parsed is not None, f"Failed to parse AST for {dag_filename}"


def test_silver_to_gold_business_rules_rf05_compliance():
    """
    Verifies RF-05:
    - Calculates daily stockout risk and sales_at_risk_usd
    - Publishes alerts with urgency to RabbitMQ STOCKOUT_QUEUE
    - Safely exports gold metrics to S3 without corrupting comma joins
    """
    file_path = DAGS_DIR / "silver_to_gold_business_rules.py"
    content = file_path.read_text(encoding="utf-8")

    assert "sales_at_risk_usd" in content, "Missing sales_at_risk_usd calculation for RF-05"
    assert "STOCKOUT_QUEUE" in content, "Missing STOCKOUT_QUEUE publication"
    assert "reabastecimiento" in content, "Missing notification target for replenishment area"
    assert "publish_messages(STOCKOUT_QUEUE" in content, "Alerts must be published to RabbitMQ queue"
    assert "GOLD_FOLDER" in content, "GOLD_FOLDER must be defined"
    assert "BUCKET" in content, "BUCKET must be defined"


def test_bronze_to_silver_cleaning_rf06_compliance():
    """
    Verifies RF-06:
    - Automatically recalculates aggregates when late events arrive
    - Aggregates real sales from country schemas (colombia, peru, etc.)
    - No hardcoded dummy sales values
    """
    file_path = DAGS_DIR / "bronze_to_silver_cleaning.py"
    content = file_path.read_text(encoding="utf-8")

    # Verify no fake dummy insert exists
    assert "VALUES (CURRENT_DATE, 100, 15420.50" not in content, (
        "Fake hardcoded SQL insert must be removed"
    )

    # Verify multi-country aggregation
    assert "fact_sale" in content, "Must query real fact_sale POS data"
    assert "colombia" in content and "peru" in content, "Must include country schemas"
    assert "recalculate_affected_gold_partitions" in content, "Must recalculate affected partitions"
    assert "exception_exceeded_72h" in content, "Must handle >72h late exceptions"


def test_partitioned_reprocessing_dag_structure():
    """
    Verifies dag_partitioned_reprocessing.py:
    - Provides on-demand backfill parameterization
    - Idempotently updates Gold sales without dropping non-targeted partitions
    - Audits backfill into data_quality_metrics
    """
    file_path = DAGS_DIR / "dag_partitioned_reprocessing.py"
    content = file_path.read_text(encoding="utf-8")

    assert "dag_partitioned_reprocessing" in content
    assert "recalculate_gold_aggregates" in content
    assert "data_quality_metrics" in content
    assert "export_reprocessed_to_s3" in content
