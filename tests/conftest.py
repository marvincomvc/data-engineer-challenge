"""Shared pytest fixtures for unit and integration tests.

Tests never read from docs/sample-data/ directly — that directory is
read-only in the container and would make tests environment-dependent.
Use `synthetic_data_path` for integration tests that need real files.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "generate_sample_data.py"


@pytest.fixture()
def tmp_duckdb(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """In-process DuckDB with all schemas bootstrapped. Isolated per test."""
    from pipeline.db import get_connection

    db_path = tmp_path / "test.duckdb"
    conn = get_connection(db_path)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def synthetic_data_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Small deterministic dataset generated from the project's script.

    1 000 rows, 10 days, 20 merchants, seed 42 — fast enough for CI,
    large enough to exercise all reconciliation categories.
    """
    out = tmp_path_factory.mktemp("synthetic")
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--rows", "1000",
            "--days", "10",
            "--merchants", "20",
            "--seed", "42",
            "--out", str(out),
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    return out
