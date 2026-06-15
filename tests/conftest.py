"""Shared pytest fixtures."""
import pathlib

import pytest

from pipeline import db

REPO = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR = REPO / "docs" / "sample-data"


@pytest.fixture
def con():
    """A fresh in-memory DuckDB connection per test."""
    c = db.connect()
    yield c
    c.close()


@pytest.fixture
def data_dir() -> str:
    """Path to the real sample-data fixtures (forward slashes for DuckDB)."""
    return str(DATA_DIR).replace("\\", "/")


@pytest.fixture
def prepared(con, data_dir):
    """A connection through bronze -> silver -> quarantine -> dims -> init_facts,
    mirroring the runner's order, ready for gold fact/mart tests."""
    from pipeline import bronze, gold, quality, silver
    from pipeline.adapters.paysettler import PaySettlerAdapter

    bronze.load_bronze(con, data_dir)
    silver.build_silver(con, PaySettlerAdapter())
    quality.init_quarantine(con)
    quality.quarantine_settlements(con)
    quality.quarantine_results(con)
    silver.build_settlement_current(con)
    gold.build_dimensions(con)
    gold.init_facts(con)
    return con
