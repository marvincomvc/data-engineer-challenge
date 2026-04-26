# Scalability — Evolving to 5 M Transactions / Day

## Current Scale

The present design targets the sample-data volume:

| Metric | Sample data | Production target |
|--------|------------|-------------------|
| Transactions / day | ~1 000 | 5 000 000 |
| Settlement rows / day | ~500 | ~4 000 000 |
| Reconciliation results / day | ~100 | ~4 000 000 |
| `reconciliation_results` / year | ~1 000 | **~1.46 billion** |
| `mart_compliance` / year | ~1 000 | **~1.46 billion** |
| Parquet files size / day | < 1 MB | ~2–4 GB |
| DuckDB file size / year | < 100 MB | **> 500 GB** |

---

## Where the Current Design Breaks

### 1. Full-replace bronze ingest

```python
conn.execute("DELETE FROM bronze.transactions")
conn.execute(f"INSERT INTO bronze.transactions SELECT ... FROM read_parquet('{glob}')")
```

Every pipeline run reads **all historical CDC parquets** from disk. At 5 M txn/day with 90-day retention, the glob expands to hundreds of files and tens of GB. Ingest time grows linearly with history size.

**Breaks at:** ~30 days of history (> 100 M rows in bronze, > 20 GB of parquets).

### 2. Settlement CSV size

A single daily CSV at 5 M rows × 200 bytes ≈ **1 GB uncompressed** per day. `read_csv` with `all_varchar=True` loads it fully into DuckDB's buffer. On a container with 2–4 GB RAM, this leaves little headroom for transformations.

**Breaks at:** ~1–2 M rows / day on a 4 GB container.

### 3. DuckDB single file

DuckDB is an in-process OLAP engine. At 1.46 B rows in `gold.mart_compliance`, a single `.duckdb` file will reach 200–400 GB. Full-table scans on that file will exceed typical instance memory (32 GB) and spill to disk, making queries slow for BI users.

**Breaks at:** ~200 M rows / year in the largest mart.

### 4. No parallelism

The pipeline is single-threaded Python. At 5 M rows, each DuckDB analytical step (dedup window function, silver JOIN) takes 30–120 seconds. Total pipeline duration: 10–30 minutes per reference_date. If daily settlement files must be processed within a 1-hour SLA, this barely fits — with no margin for failures or reruns.

**Breaks at:** pipeline duration > SLA window (typically 1–2 hours after midnight).

### 5. No incremental reads for CDC

The CDC parquets contain the full history of every table. Once the pipeline reads 90 days × 5 M rows = 450 M CDC events per run, the `dedup_latest` window function will be the bottleneck.

**Breaks at:** ~30 days accumulated history (> 100 M bronze rows).

---

## Evolution Path

### Phase 1 — Vertical scaling + date-partitioned parquets (5 M txn/month)

*Stays with DuckDB. Buys 6–12 months.*

**Change the parquet layout to Hive partitioning:**
```
transactions/
  date=2025-03-04/
    batch_0001.parquet   # ≤ 500 MB each
    batch_0002.parquet
  date=2025-03-05/
    ...
```

**Ingest only today's partition:**
```python
glob = str(data_path / "transactions" / f"date={reference_date}" / "*.parquet")
conn.execute("DELETE FROM bronze.transactions WHERE created_at::DATE = ?", [reference_date])
conn.execute(f"INSERT INTO bronze.transactions SELECT ... FROM read_parquet('{glob}')")
```

This reduces ingest from "all history" to "one day" — a 30×–90× speedup.

**Increase container memory** to 8–16 GB. DuckDB uses memory-mapped files and benefits directly.

**Split settlement CSV into chunks** (100 K rows each) upstream, or compress with gzip (`read_csv` supports `.csv.gz` natively in DuckDB).

Impact: pipeline duration drops from 30 min to < 5 min. DuckDB file stays < 50 GB for two years.

---

### Phase 2 — Object storage + DuckDB HTTPFS (50 M txn/month)

*Parquets move to S3. DuckDB reads directly without downloading.*

```python
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_region='us-east-1';")
glob = "s3://payments-datalake/transactions/date=2025-03-04/*.parquet"
conn.execute(f"INSERT INTO bronze.transactions SELECT ... FROM read_parquet('{glob}')")
```

DuckDB's HTTPFS extension streams Parquet column groups directly from S3, reading only the columns/row-groups needed for the query. At 5 M txn/day, this eliminates the local disk bottleneck entirely.

**Gold marts stay in a local DuckDB file** (or move to MotherDuck for a managed option). Compliance audit mart is now queried via DuckDB attached to S3 for the raw data.

**Add date-range partitioning to gold.mart_compliance:**
```sql
-- Instead of one giant table, partition by year-month
CREATE TABLE gold.mart_compliance_2025_03 AS ...
```

Impact: handles 50× growth without re-architecting. S3 cost is ~$0.023/GB/month — 2 years of data ≈ $250/month.

---

### Phase 3 — Distributed processing (500 M txn/month, 1.8 B/year)

*At this scale, a single-node DuckDB is no longer appropriate for transformation.*

**Move transformation layer to Apache Spark or dbt + BigQuery / Snowflake / Redshift:**

```
CDC events → Kafka → Spark Structured Streaming → S3 (Iceberg/Delta Lake)
                                                    ↓
                                             dbt transformations
                                                    ↓
                                          BigQuery / Redshift mart tables
```

| Component | Replaces | Why |
|-----------|----------|-----|
| Apache Kafka | File polling | Sub-second CDC delivery; no polling lag |
| Iceberg / Delta Lake | Raw Parquet files | ACID transactions on S3; `DELETE` by key for CDC dedup without rewriting files |
| dbt | silver.py / marts/*.py | SQL-first, version-controlled transformations; lineage graph built-in |
| BigQuery / Snowflake | DuckDB | Serverless, MPP execution; handles 1.8 B rows in mart_compliance without ops overhead |

**What stays the same across all phases:**
- Business logic (STOP/WARN checks, reconciliation categories, idempotency pattern)
- Gold mart schemas (same columns, same aggregations)
- Audit trail approach (`pipeline_runs` equivalent in the orchestrator)

**What changes:**
- The *engine* executing the SQL, not the SQL itself — dbt models are straightforward translations of the current Python/SQL code
- The *storage layer* for the largest tables (Parquet → Iceberg on S3)
- The *orchestrator* (cron/Make → Airflow / Dagster)

---

## Summary Table

| Scale | Architecture | Max daily rows | DuckDB file | Pipeline duration |
|-------|-------------|---------------|-------------|-------------------|
| **Now** | Single DuckDB, local Parquet | ~10 K | < 1 GB | < 5 s |
| **Phase 1** | DuckDB + date-partitioned Parquet, 16 GB RAM | ~5 M | < 50 GB / 2 yr | < 10 min |
| **Phase 2** | DuckDB + S3 HTTPFS, Parquet on S3 | ~50 M | < 200 GB / 2 yr | < 30 min |
| **Phase 3** | Spark + Iceberg + dbt + BigQuery | Unlimited | N/A (managed) | Streaming / < 1 hr batch |

The critical insight is that **DuckDB is far more capable than it appears**: benchmarks show single-node DuckDB handling 500 M–1 B rows in analytical queries with enough RAM. Phase 1 and Phase 2 can realistically carry the business to 10–50× current volume with modest infrastructure investment. Phase 3 becomes necessary only when multi-tenant access, sub-minute latency, or > 100 M rows/day sustained throughput is required.
