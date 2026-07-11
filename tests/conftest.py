"""Shared fixtures: build a *controlled* source dataset and run the pipeline.

Correctness is validated against hand-authored inputs with known expected
categories (not the messy sample fixture, whose synthetic PaySettler file does
not reconcile 1:1 with the historical results). Each source is written as a
DuckDB-produced parquet/CSV so the real ingest path is exercised end to end.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
VENV_DIR = os.environ.get("PIPELINE_VENV")  # optional: dir with bin/dbt + bin/python

REFERENCE_DATE = "2025-01-10"  # window [2025-01-03, 2025-01-10]

# (transaction_id, merchant, internal_amount, created_at, csv_amount_str,
#  internal_ccy, processor_ccy, expected_category)
# csv_amount_str / processor_ccy are None when the txn is NOT in the processor file;
# internal_ccy is None when there is no internal row.
SCENARIO = [
    ("t1-match",      "merch_a", 100.00, "2025-01-08", "100.00",   "BRL", "BRL", "MATCHED"),
    ("t2-tolerance",  "merch_a", 100.00, "2025-01-09", "100.01",   "BRL", "BRL", "MATCHED"),        # diff==tol
    ("t3-mismatch",   "merch_b", 100.00, "2025-01-07", "100,02",   "BRL", "BRL", "MISMATCHED"),     # comma decimal
    ("t4-internal",   "merch_b",  50.00, "2025-01-05", None,       "BRL", None,  "UNRECONCILED_INTERNAL"),
    ("t5-processor",  "merch_c",  75.00, None,         "R$ 75,00", None,  "BRL", "UNRECONCILED_PROCESSOR"),  # pt-BR
    ("t6-outofwindow","merch_c", 200.00, "2025-01-01", "200.00",   "BRL", "BRL", "UNRECONCILED_PROCESSOR"),  # pre-window
    ("t8-usd",        "merch_a", 200.00, "2025-01-08", "200.00",   "USD", "USD", "MATCHED"),        # USD grain
    ("t9-crossccy",   "merch_b", 100.00, "2025-01-07", "100.00",   "BRL", "USD", "MISMATCHED"),     # cross-currency
]


def _pybin() -> str:
    return f"{VENV_DIR}/bin/python" if VENV_DIR else sys.executable


def _dbtbin() -> str:
    return f"{VENV_DIR}/bin/dbt" if VENV_DIR else "dbt"


def _build_sources(data_dir: Path) -> None:
    con = duckdb.connect()
    # --- internal transactions, split across two CDC batches with drift -----
    # batch_1 (no payment_method); t1 appears TWICE to exercise CDC collapse
    # (stale row first, correct row wins by _timestamp).
    rows_b1 = []
    idc = 1
    for tid, m, amt, created, _csv, int_ccy, _proc_ccy, _cat in SCENARIO:
        if created is None:
            continue
        if tid == "t1-match":
            rows_b1.append((idc, tid, m, 999.99, int_ccy, "COMPLETED", created, "c", "2025-01-08T00:00:00"))
            idc += 1
        rows_b1.append((idc, tid, m, amt, int_ccy, "COMPLETED", created, "c", "2025-01-09T00:00:00"))
        idc += 1
    values_b1 = ",".join(
        f"({i},'{t}','{m}',{a},'{ccy}','{s}','desc','{c}','{c}','{op}','{ts}')"
        for (i, t, m, a, ccy, s, c, op, ts) in rows_b1
    )
    con.execute(
        f"""COPY (SELECT * FROM (VALUES {values_b1})
            t(id,transaction_id,merchant_id,amount,currency,status,description,created_at,updated_at,Op,_timestamp))
            TO '{(data_dir / 'transactions_batch_1.parquet').as_posix()}' (FORMAT parquet)"""
    )
    # batch_2 carries the drift column payment_method; keep it minimal (one row).
    con.execute(
        f"""COPY (SELECT * FROM (VALUES
             (999,'t7-drift','merch_a',10.00,'BRL','COMPLETED','desc','2025-01-09','2025-01-09','c','2025-01-09T00:00:00','pix'))
             t(id,transaction_id,merchant_id,amount,currency,status,description,created_at,updated_at,Op,_timestamp,payment_method))
            TO '{(data_dir / 'transactions_batch_2.parquet').as_posix()}' (FORMAT parquet)"""
    )
    # --- PaySettler CSV -----------------------------------------------------
    csv_path = data_dir / "settlement_paysettler.csv"
    lines = ["transaction_id,merchant_id,amount,currency,settled_at,processor_reference,status"]
    for tid, m, _amt, _created, csv_amt, _int_ccy, proc_ccy, _cat in SCENARIO:
        if csv_amt is None:
            continue
        # quote amounts containing commas (pt-BR)
        amt_field = f'"{csv_amt}"' if "," in csv_amt else csv_amt
        lines.append(f"{tid},{m},{amt_field},{proc_ccy},{REFERENCE_DATE}T12:00:00Z,PS-1,SETTLED")
    csv_path.write_text("\n".join(lines) + "\n")
    # --- other CDC sources (minimal but schema-complete) --------------------
    con.execute(
        f"""COPY (SELECT * FROM (VALUES
             (1,'{REFERENCE_DATE}','settlement.csv','COMPLETED',6,'2025-01-10T02:00:00','2025-01-10T02:05:00','2025-01-10T02:00:00','c','2025-01-10T02:00:00'))
             t(id,reference_date,file_name,status,total_transactions,started_at,completed_at,created_at,Op,_timestamp))
            TO '{(data_dir / 'reconciliation_runs.parquet').as_posix()}' (FORMAT parquet)"""
    )
    con.execute(
        f"""COPY (SELECT * FROM (VALUES
             (1,1,'t1-match','merch_a','MATCHED',100.00,100.00,0.00,'2025-01-10','c','2025-01-10T02:00:00'))
             t(id,run_id,transaction_id,merchant_id,category,internal_amount,processor_amount,difference,created_at,Op,_timestamp))
            TO '{(data_dir / 'reconciliation_results.parquet').as_posix()}' (FORMAT parquet)"""
    )
    merch_values = ",".join(
        f"({i},'{m}','Legal {m}','Trade {m}','00.000.000/0001-{i:02d}','4712100','2025-01-01','2025-01-01','c','2025-01-01T00:00:00')"
        for i, m in enumerate(["merch_a", "merch_b", "merch_c"], start=1)
    )
    con.execute(
        f"""COPY (SELECT * FROM (VALUES {merch_values})
             t(id,merchant_id,legal_name,trade_name,document,primary_cnae,created_at,updated_at,Op,_timestamp))
            TO '{(data_dir / 'enterprise_company.parquet').as_posix()}' (FORMAT parquet)"""
    )
    con.close()


def run_pipeline(warehouse: Path, data_dir: Path, reference_date: str, command: str = "run") -> None:
    """Run the pipeline against an isolated warehouse + sources.

    command="run" exercises the real end-to-end CLI (ingest -> dbt build ->
    publish). Any other value runs ingest + `dbt <command>` for granular tests.
    """
    env = {
        **os.environ,
        "DATA_PATH": str(data_dir),
        "DUCKDB_PATH": str(warehouse),
        "GOLD_PATH": str(warehouse.parent / "gold"),
        "PYTHONPATH": str(REPO_ROOT / "src"),
        "PATH": (f"{VENV_DIR}/bin:" if VENV_DIR else "") + os.environ.get("PATH", ""),
    }
    if command == "run":
        subprocess.run(
            [_pybin(), "-m", "pipeline", "run", "--reference-date", reference_date],
            cwd=REPO_ROOT, env=env, check=True, capture_output=True, text=True,
        )
        return
    subprocess.run(
        [_pybin(), "-m", "pipeline", "ingest", "--reference-date", reference_date],
        cwd=REPO_ROOT, env=env, check=True, capture_output=True, text=True,
    )
    subprocess.run(
        [_dbtbin(), command, "--project-dir", "dbt", "--profiles-dir", "dbt",
         "--vars", f'{{"reference_date": "{reference_date}", "run_id": "test"}}'],
        cwd=REPO_ROOT, env=env, check=True, capture_output=True, text=True,
    )


@pytest.fixture
def controlled_warehouse(tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _build_sources(data_dir)
    warehouse = tmp_path / "wh.duckdb"
    yield warehouse, data_dir
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.fixture
def expected_categories() -> dict[str, str]:
    return {row[0]: row[-1] for row in SCENARIO}
