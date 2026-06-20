#!/usr/bin/env python3
"""
Utilitário de consulta ao banco analítico (data/analytics.db).

Exibe resumos das tabelas fato, dimensões, agregações e arquivos bronze.
Uso:
    python scripts/query_analytics.py [--db data/analytics.db] [--bronze bronze]
"""

import argparse
from pathlib import Path

import duckdb


def consultar_banco(db_path: str, bronze_path: str) -> None:
    """Conecta no DuckDB e imprime resumos formatados."""

    path = Path(db_path)
    if not path.exists():
        print(f"ERRO: Banco não encontrado em '{db_path}'.")
        print("Execute o pipeline primeiro: python -m src.cli --reference-date 2025-03-15")
        return

    con = duckdb.connect(str(path))

    separador = "=" * 55

    # ============================================================
    # TABELAS EXISTENTES
    # ============================================================
    print(separador)
    print("TABELAS NO BANCO ANALÍTICO")
    print(separador)
    tabelas = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    for (nome,) in tabelas:
        print(f"  {nome}")
    print()

    # ============================================================
    # FCT_RECONCILIATION
    # ============================================================
    print(separador)
    print("FCT_RECONCILIATION (Fato)")
    print(separador)

    total = con.execute("SELECT count(*) FROM fct_reconciliation").fetchone()[0]
    print(f"Total de registros: {total}")

    if total > 0:
        print("\nPor categoria:")
        categorias = con.execute("""
            SELECT category, count(*) AS total
            FROM fct_reconciliation
            GROUP BY category
            ORDER BY total DESC
        """).fetchall()
        for cat, cnt in categorias:
            print(f"  {cat:<28} {cnt:>6}")

        print("\nPor reference_date:")
        datas = con.execute("""
            SELECT reference_date, count(*) AS total
            FROM fct_reconciliation
            GROUP BY reference_date
            ORDER BY reference_date DESC
            LIMIT 10
        """).fetchall()
        for data, cnt in datas:
            print(f"  {data}  {cnt:>6} registros")

        print("\nTop 5 registros (amostra):")
        amostra = con.execute("""
            SELECT reconciliation_result_id, transaction_id, merchant_id,
                   category, internal_amount, processor_amount,
                   difference_abs, reference_date
            FROM fct_reconciliation
            LIMIT 5
        """).fetchall()
        print(f"  {'ID':>6}  {'transaction_id':<38}  {'merchant':<12}  {'category':<26}  {'internal':>10}  {'processor':>10}  {'diff':>8}  {'ref_date':>10}")
        print(f"  {'-'*6}  {'-'*38}  {'-'*12}  {'-'*26}  {'-'*10}  {'-'*10}  {'-'*8}  {'-'*10}")
        for row in amostra:
            print(f"  {row[0]:>6}  {row[1]:<38}  {row[2] or 'NULL':<12}  {row[3]:<26}  {row[4] or 'NULL':>10}  {row[5] or 'NULL':>10}  {row[6] or 'NULL':>8}  {row[7]}")
    print()

    # ============================================================
    # DIMENSOES
    # ============================================================
    print(separador)
    print("DIMENSÕES")
    print(separador)

    # dim_category
    n_cat = con.execute("SELECT count(*) FROM dim_category").fetchone()[0]
    print(f"\ndim_category: {n_cat} categorias")
    cats = con.execute(
        "SELECT category_code, description FROM dim_category"
    ).fetchall()
    for cod, desc in cats:
        print(f"  {cod:<28} {desc}")

    # dim_merchant
    n_merch = con.execute("SELECT count(*) FROM dim_merchant").fetchone()[0]
    print(f"\ndim_merchant: {n_merch} merchants")
    merchs = con.execute("""
        SELECT merchant_id, trade_name, document, primary_cnae
        FROM dim_merchant
        LIMIT 5
    """).fetchall()
    for mid, trade, doc, cnae in merchs:
        print(f"  {mid:<12} {trade:<14} {doc:<16} {cnae}")

    # dim_reconciliation_run
    n_runs = con.execute(
        "SELECT count(*) FROM dim_reconciliation_run"
    ).fetchone()[0]
    print(f"\ndim_reconciliation_run: {n_runs} runs")
    runs = con.execute("""
        SELECT run_id, reference_date, status, total_transactions
        FROM dim_reconciliation_run
        ORDER BY reference_date DESC
        LIMIT 5
    """).fetchall()
    for rid, ref, st, total_txn in runs:
        print(f"  run_id={rid:<4} ref={ref}  status={st:<12}  transactions={total_txn}")

    # dim_date
    n_date = con.execute("SELECT count(*) FROM dim_date").fetchone()[0]
    print(f"\ndim_date: {n_date} datas")
    dates = con.execute("""
        SELECT date_id, year, month, day, day_of_week, is_weekend
        FROM dim_date
        ORDER BY date_id
        LIMIT 5
    """).fetchall()
    for d, y, m, day, dow, we in dates:
        print(f"  {d}  ano={y} mês={m:>2} dia={day:>2}  dow={dow}  weekend={we}")
    print()

    # ============================================================
    # AGREGACOES
    # ============================================================
    print(separador)
    print("AGREGAÇÕES")
    print(separador)

    # agg_daily
    n_daily = con.execute("SELECT count(*) FROM agg_daily_reconciliation").fetchone()[0]
    print(f"\nagg_daily_reconciliation: {n_daily} grupos")
    if n_daily > 0:
        daily = con.execute("""
            SELECT reference_date, merchant_id, category,
                   total_transactions, sum_internal, sum_processor, sum_diff_abs
            FROM agg_daily_reconciliation
            ORDER BY reference_date DESC, total_transactions DESC
            LIMIT 5
        """).fetchall()
        for ref, mid, cat, tt, si, sp, sd in daily:
            print(f"  {ref}  {mid:<12}  {cat:<26}  txn={tt:<4}  int={si or 0:>10.2f}  proc={sp or 0:>10.2f}  diff={sd or 0:>8.2f}")
    else:
        print("  (vazia — o filtro da agregação pode não ter encontrado a reference_date)")

    # agg_monthly
    n_monthly = con.execute(
        "SELECT count(*) FROM agg_monthly_financial"
    ).fetchone()[0]
    print(f"\nagg_monthly_financial: {n_monthly} meses")
    monthly = con.execute("""
        SELECT year, month, total_processor_volume, total_internal_volume, total_diff
        FROM agg_monthly_financial
        ORDER BY year DESC, month DESC
    """).fetchall()
    print(f"  {'Ano':<6} {'Mês':<5} {'Vol. Processador':>16} {'Vol. Interno':>16} {'Diferença':>14}")
    print(f"  {'-'*6} {'-'*5} {'-'*16} {'-'*16} {'-'*14}")
    for y, m, tvp, tvi, td in monthly:
        print(f"  {y:<6} {m:<5} {tvp or 0:>16.2f} {tvi or 0:>16.2f} {td or 0:>14.2f}")
    print()

    # ============================================================
    # ARQUIVOS BRONZE
    # ============================================================
    print(separador)
    print("ARQUIVOS BRONZE")
    print(separador)

    bronze = Path(bronze_path)
    if bronze.exists():
        parquets = sorted(bronze.glob("*.parquet"))
        if parquets:
            print(f"\n{len(parquets)} arquivos encontrados em '{bronze_path}/':")
            for p in parquets:
                tamanho_kb = p.stat().st_size / 1024
                print(f"  {p.name:<60} {tamanho_kb:>8.1f} KB")
        else:
            print(f"\nNenhum arquivo .parquet em '{bronze_path}/'")
            print("(normal se não houve problemas de qualidade)")
    else:
        print(f"\nPasta bronze '{bronze_path}/' não encontrada.")
    print()

    con.close()
    print("Fim da consulta.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consulta o banco analítico do pipeline de reconciliação"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="data/analytics.db",
        help="Caminho do DuckDB de saída (default: data/analytics.db)",
    )
    parser.add_argument(
        "--bronze",
        type=str,
        default="bronze",
        help="Pasta bronze (default: bronze)",
    )
    args = parser.parse_args()
    consultar_banco(args.db, args.bronze)


if __name__ == "__main__":
    main()