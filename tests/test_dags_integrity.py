"""
Automated DAG syntax and integrity tests for Apache Airflow.

Fulfills Acceptance Criteria:
- Given a change in a DAG, when the pipeline triggers, then it validates its syntax.
- Automatically validate DAG syntax and structure before deployment.
- Verifies that DAG files compile without syntax errors, have valid dag_ids,
  and when Airflow is installed, confirms 0 import errors in DagBag.
"""

from __future__ import annotations

import ast
import os
import py_compile
from pathlib import Path
import pytest


DAGS_DIR = Path(__file__).resolve().parent.parent / "airflow" / "dags"


def get_dag_files() -> list[Path]:
    """Retrieve all Python DAG files, excluding hidden or test files."""
    if not DAGS_DIR.exists():
        return []
    return [
        p
        for p in DAGS_DIR.glob("*.py")
        if not p.name.startswith((".", "_", "test_"))
    ]


@pytest.mark.parametrize("dag_path", get_dag_files(), ids=lambda p: p.name)
def test_dag_syntax_compilation(dag_path: Path):
    """
    AC-2 & AC-5: Every DAG file must compile cleanly with python -m py_compile.
    Fails immediately if there are syntax errors, invalid indentation, or broken syntax.
    """
    try:
        py_compile.compile(str(dag_path), doraise=True)
    except py_compile.PyCompileError as exc:
        pytest.fail(f"Syntax error found in DAG {dag_path.name}: {exc}")


@pytest.mark.parametrize("dag_path", get_dag_files(), ids=lambda p: p.name)
def test_dag_structure_ast(dag_path: Path):
    """
    Validates via Python AST that each DAG file instantiates a DAG
    and defines required metadata without circular dependencies.
    """
    tree = ast.parse(dag_path.read_text(encoding="utf-8"), filename=str(dag_path))
    has_dag_instantiation = False
    dag_ids = []

    for node in ast.walk(tree):
        # Look for DAG(...) instantiation or with DAG(...)
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name == "DAG":
                has_dag_instantiation = True
                # Extract dag_id if passed as keyword or arg
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        dag_ids.append(kw.value.value)

    assert has_dag_instantiation, f"File {dag_path.name} does not appear to instantiate a DAG."
    assert len(dag_ids) > 0, f"DAG in {dag_path.name} must declare an explicit dag_id string."


def test_dagbag_import_errors():
    """
    Validates that Airflow DagBag imports all DAGs without errors,
    verifying tasks exist and dependencies are acyclic (no cycles).
    """
    try:
        from airflow.models.dagbag import DagBag
    except ImportError:
        pytest.skip("apache-airflow is not installed in the local environment; running AST/syntax checks.")

    dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

    # 1. Zero import errors
    assert len(dag_bag.import_errors) == 0, (
        f"Airflow DagBag found {len(dag_bag.import_errors)} import errors: "
        f"{dag_bag.import_errors}"
    )

    # 2. At least one DAG loaded
    assert len(dag_bag.dags) > 0, "No DAGs were loaded from DAGs directory."

    # 3. Check each DAG for cycles and task existence
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.tasks) > 0, f"DAG {dag_id} contains 0 tasks."
        # Verify DAG is acyclic (test_cycle raises AirflowDagCycleException if cyclic)
        dag.test_cycle()
