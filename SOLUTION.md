# Solution — Settlement Reconciliation Data Products

My submission for the challenge (original brief in [`docs/CHALLENGE.md`](docs/CHALLENGE.md);
navigation in [`README.md`](README.md)). It turns the
transactional reconciliation data into consumable data products for Ops, CFO, and
Compliance, using a medallion pipeline on **DuckDB + dbt + Airflow**.

> **PR deliverable sections** (Como rodar, Premissas e decisões, Arquitetura,
> Extensibilidade, Limites do desenho, Produção, Ferramentas de IA) →
> [`docs/PR.md`](docs/PR.md).

## TL;DR

- **Recomputes** reconciliation from raw sources (7-day window, R$0,01 tolerance,
  match on `transaction_id`) — it does not trust the pre-baked results.
- **Medallion + star schema**: Bronze (raw+lineage) → Silver (CDC-collapsed,
  repaired) → Gold (conformed dims + immutable fact + 3 consumer marts).
- **Idempotent**: re-running a `reference_date` never duplicates or corrupts
  (delete+insert on the natural key). Proven by tests.
- **Data quality is gated**: integrity → `error` (blocks), anomaly → `warn`
  (alerts), all observable in `meta.dq_check_result`.
- **Consumable**: `run` publishes Gold to a partitioned **Parquet product zone**
  (`gold/`) + a `serving.duckdb` catalog; an optional **Metabase** BI layer reads it.
- **Observable**: `meta.pipeline_run` + `meta.dq_check_result` + structured JSON logs.

## Quickstart

```bash
docker compose up -d --build          # start the pipeline container
make run                              # ingest + dbt build + publish for 2025-03-16
make test                             # pytest suite (correctness + idempotency + publish)

# consume the published data products (Parquet, no warehouse access needed)
docker compose exec pipeline duckdb -c \
  "select * from read_parquet('gold/mart_ops_health/**/*.parquet', hive_partitioning=true)"
# or the developer view via the marts directly:
docker compose exec pipeline duckdb warehouse/settlement.duckdb < sql/examples/ops_health.sql

# optional: interactive BI dashboard on the products (http://localhost:3000)
make bi-up

# optional: Airflow orchestration (http://localhost:8080)
make airflow-up

# validate at scale (~1M rows)
make scale-test
```

**How consumers read it** → [`docs/consumption.md`](docs/consumption.md).

`make run REFERENCE_DATE=2025-03-13` processes a different day (accumulates, no clobber).

## Repository layout

```
src/pipeline/          Python ingest CLI (Bronze load, dirty-data repair, run registry)
  ingest/              bronze.py, csv_parser.py
  publish.py           export Gold → partitioned Parquet products + serving catalog
  cli.py               `python -m pipeline {ingest,run,dbt-run,dbt-test,publish}`
dbt/                   dbt-duckdb project
  models/staging/      CDC collapse + repair (Silver); stg_settlements = canonical
                       processor union (the seam for adding a 2nd processor)
  models/intermediate/ int_reconciliation.sql  ← the reconciliation engine
  models/marts/        core (dims+facts) + ops/finance/compliance marts (Gold)
  macros/              clean_amount (pt-BR/comma money repair)
  tests/               singular DQ tests (grain, anomaly, cross-check)
airflow/dags/          reconciliation_dag.py (ingest ▶ dbt run ▶ dbt test)
sql/examples/          per-consumer example queries
tests/                 pytest: reconciliation correctness, idempotency, ingest repair
docs/                  data-products (Part 1), architecture/troubleshooting/scalability (Part 3)
```

## How the requirements are met

| Requirement | Where |
|---|---|
| Part 1 — modeling & analytics | [`docs/data-products.md`](docs/data-products.md), `dbt/models/marts/`, `sql/examples/` |
| Consumption / serving | [`docs/consumption.md`](docs/consumption.md), `src/pipeline/publish.py`, `gold/`, Metabase (`make bi-up`) |
| Part 2 — pipeline | `src/pipeline/`, `dbt/`, `airflow/` |
| Correctness | `dbt/models/intermediate/int_reconciliation.sql`, `tests/test_reconciliation.py` |
| Idempotency | delete+insert on natural key; `tests/test_idempotency.py` |
| Observability | `meta.pipeline_run`, `meta.dq_check_result`, structured logs |
| Data quality | dbt tests (`error`/`warn`); `dbt/models/**/*.yml`, `dbt/tests/` |
| Testability | `tests/` (pytest) + `dbt test` |
| Part 3.1 architecture | [`docs/architecture.md`](docs/architecture.md) |
| Part 3.2 troubleshooting | [`docs/troubleshooting.md`](docs/troubleshooting.md) |
| Part 3.3 scalability | [`docs/scalability.md`](docs/scalability.md) |

## Key data caveat (read this)

The sample `settlement_paysettler.csv` is a **synthetic standalone fixture** and
does not reconcile 1:1 with the historical `reconciliation_results` (different file,
seeded amount differences). A recompute against it yields ~0 MATCHED — expected, not
a bug. Correctness is therefore proven with **controlled unit fixtures**
(`tests/test_reconciliation.py`), and the source cross-check is a `warn` signal.
Full reasoning in [`docs/data-products.md`](docs/data-products.md).

## Design trail

The full reasoning (brainstorm → requirements → design, with ADRs and the
36-file manifest) is in `.claude/sdd/features/` —
`BRAINSTORM_`, `DEFINE_`, `DESIGN_SETTLEMENT_RECONCILIATION.md`.
