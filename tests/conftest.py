"""Fixtures de pytest compartilhadas."""
import pathlib

import pytest

from pipeline import db

REPO = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR = REPO / "docs" / "sample-data"


@pytest.fixture
def con():
    """Uma conexao DuckDB em memoria nova a cada teste."""
    c = db.connect()
    yield c
    c.close()


@pytest.fixture
def data_dir() -> str:
    """Caminho dos fixtures reais de sample-data (com barras normais pro DuckDB)."""
    return str(DATA_DIR).replace("\\", "/")


@pytest.fixture
def prepared(con, data_dir):
    """Conexao que ja passou por bronze -> silver -> quarantine -> dims -> init_facts,
    seguindo a mesma ordem do runner, pronta pros testes de fact/mart da camada gold."""
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
