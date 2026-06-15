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
