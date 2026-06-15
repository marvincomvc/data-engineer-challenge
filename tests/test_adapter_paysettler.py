import datetime

import duckdb
import pytest

from pipeline.adapters.paysettler import PaySettlerAdapter


@pytest.fixture
def raw(tmp_path):
    # Note: the comma-decimal amount is QUOTED, exactly as the real PaySettler
    # CSV writes it (otherwise the decimal comma would split the column).
    csv = tmp_path / "ps.csv"
    csv.write_text(
        'transaction_id,merchant_id,amount,currency,settled_at,processor_reference,status\n'
        't1,MERCH_001,1234.56,BRL,2025-03-16T10:00:00Z,PS-1,SETTLED\n'
        't2,merch_002,"1000,50",BRL,2025-03-16T11:00:00,PS-2,SETTLED\n'
        't3,MERCH_003,99.90,BRL,2025-03-16T12:00:00+00:00,PS-3,REVERSED\n'
        't4,MERCH_004,"R$ 47.401,20",BRL,2025-03-16T09:00:00Z,PS-4,SETTLED\n',
        encoding="utf-8",
    )
    con = duckdb.connect()
    path = str(csv).replace("\\", "/")
    con.execute(
        f"create view raw_ps as select * from read_csv('{path}', all_varchar=true, header=true)"
    )
    return con


def test_canonical_fixes_dirty_rows(raw):
    sql = PaySettlerAdapter().canonical_sql("raw_ps")
    rel = raw.sql(sql)
    cols = [d[0] for d in rel.description]
    rows = {r[cols.index("transaction_id")]: r for r in rel.fetchall()}
    amt, mid, src, st = (cols.index(c) for c in ("settled_amount", "merchant_id", "source_name", "status"))
    assert float(rows["t2"][amt]) == 1000.50      # comma decimal fixed
    assert float(rows["t4"][amt]) == 47401.20     # R$ + thousands dot + decimal comma
    assert rows["t2"][mid] == "MERCH_002"          # uppercased
    assert rows["t1"][src] == "paysettler"         # source tagged
    assert rows["t3"][st] == "REVERSED"            # status preserved


def test_settled_at_normalized_to_timestamp(raw):
    sql = PaySettlerAdapter().canonical_sql("raw_ps")
    rows = raw.sql(f"select transaction_id, settled_at from ({sql}) order by transaction_id").fetchall()
    assert all(isinstance(r[1], datetime.datetime) for r in rows)
    by = {r[0]: r[1] for r in rows}
    assert by["t1"].hour == 10 and by["t2"].hour == 11 and by["t3"].hour == 12
