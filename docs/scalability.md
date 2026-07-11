# Part 3.3 — Scalability

Target: **~5M transactions/day** in 18 months (≈1.8B/year in `reconciliation_results`).
Today's fixture is ~1k rows. This documents where the current design breaks first
and how I'd stage the evolution.

## Where it breaks first (in order)

1. **DuckDB single-writer + single-file** — the hard ceiling. One process writes at
   a time, so no horizontal write parallelism. A single-node DuckDB handles tens of
   millions of rows comfortably, but 1.8B rows in one file and concurrent BI reads
   during writes will not hold. **This breaks first.**
2. **Full-replace Bronze ingest** — re-landing every source each run is O(total
   data), not O(new data). At 5M/day this wastes hours re-reading history.
3. **`fct_transaction` current-state refresh** without partition pruning becomes
   expensive as the internal book grows.
4. **Sequential DAG** — fine while stages are minutes; at scale the ingest→transform
   critical path lengthens and you want intra-stage parallelism.

## Evolution plan

### Stage 1 — same design, tightened (up to ~5M/day on a big box)
- **Incremental Bronze**: land only the new `reference_date` partition (partitioned
  parquet in object storage), not a full reload. Ingest becomes append-by-partition.
- **Partition & prune**: the fact is already keyed by `reference_date`; enforce
  partition pruning in incremental predicates so re-runs touch one day.
- **Bound memory**: `memory_limit` + spill are set; size the box so a day's data
  fits working memory.

### Stage 2 — swap the warehouse substrate (the real fix)
The dbt project is **portable** — models are ANSI-ish SQL. Point `profiles.yml` at
**Snowflake or BigQuery** (or Databricks) and the same Bronze→Silver→Gold DAG runs
distributed, with separated storage/compute and concurrent reader isolation. This
directly removes ceilings #1 and #3:
- Facts become **clustered/partitioned by `reference_date`** (BigQuery partitioning
  / Snowflake clustering) — reconciliation only ever scans the relevant window.
- `delete+insert` idempotency becomes a native **MERGE** on `(reference_date,
  transaction_id)`.
- Writers and BI readers no longer contend for one file.

### Stage 3 — ingestion & orchestration at scale
- **CDC in earnest**: Debezium → Kafka → partitioned landing, replacing batch
  parquet extracts; enables near-real-time if the business ever needs it.
- **Parallelize** the orchestrator: per-partition/per-merchant task mapping
  (Airflow dynamic task mapping) once the warehouse supports concurrent writes.
- **Data-quality at volume**: keep dbt tests but sample expensive anomaly checks,
  and add source **freshness** SLAs + alerting on `dq_check_result`.

## What deliberately does *not* change

The parts that encode the business remain identical across every stage: the
medallion layering, the reconciliation rules (7-day window, R$0,01 tolerance,
match on `transaction_id`), the idempotency contract (natural-key upsert scoped by
`reference_date`), and the error-vs-warn DQ split. That portability is the point of
choosing dbt + a medallion model over a bespoke script — **the logic outlives the
substrate.**

## Rough sizing

| Metric | Today | 5M/day target |
|---|---|---|
| `reconciliation_results` rows | ~800 | ~1.8B/year |
| Storage (Parquet, compressed) | KBs | ~single-digit TB/year |
| Engine | DuckDB single file | Distributed warehouse, partitioned |
| Reconciliation scan per run | whole table | one 7-day window (partition-pruned) |
