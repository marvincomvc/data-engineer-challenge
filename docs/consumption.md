# Consumption — how the business reads the data products

The pipeline's output is not "a table in a database file" — it publishes
**consumable data products**. This is the serving layer.

## Why publish, instead of querying the warehouse directly?

DuckDB is **single-writer**: a BI tool or analyst holding `settlement.duckdb`
open would block the pipeline from writing. So the `publish` stage exports Gold to
a partitioned **Parquet zone** (one partition per `reference_date`, re-published on
re-run) that consumers read instead — fully decoupled from the writer, and the same
pattern you'd use in production (Gold → object storage → warehouse external tables / BI).

```
gold/
  mart_ops_health/reference_date=2025-03-16/*.parquet
  mart_cfo_volume/reference_date=2025-03-16/*.parquet   + mart_cfo_volume.csv
  mart_compliance_audit/reference_date=2025-03-16/*.parquet
  fct_reconciliation_result/reference_date=2025-03-16/*.parquet
  dim_merchant/  dim_category/  dim_date/
  serving.duckdb          ← thin catalog: VIEWS over the Parquet globs (for BI)
```

Re-publishing a `reference_date` overwrites only that partition → idempotent.
`serving.duckdb` holds only *views over the Parquet globs*, so new partitions are
picked up automatically and the file never needs re-writing while BI holds it open.

## Three ways to consume

### 1. Data-product files (portable, no dependencies)
Any tool that reads Parquet/CSV. Examples:
```python
import duckdb
duckdb.sql("select * from read_parquet('gold/mart_ops_health/**/*.parquet', hive_partitioning=true)")
# or pandas: pd.read_parquet('gold/mart_ops_health'); or the CSV extract for spreadsheets
```
This is the contract consumers (downstream pipelines, a warehouse `COPY`, a data
share) build against.

### 2. BI dashboard — Metabase (interactive, for humans)
```bash
make run          # produce gold/ first
make bi-up        # start Metabase  ->  http://localhost:3000
```
Then in Metabase:
1. Finish the admin sign-up.
2. **Settings → Admin → Databases → Add database → DuckDB.**
3. Database file: `/gold/serving.duckdb`  (mounted read-only into the container).
4. Browse `mart_ops_health` / `mart_cfo_volume` / `mart_compliance_audit`, or paste
   the queries from [`sql/examples/`](../sql/examples) into the SQL editor to build
   the Ops health, CFO volume, and Compliance audit dashboards.

> **Driver note:** Metabase's DuckDB driver is a community plugin, fetched by the
> `metabase-init` service from a pinned release. Community-driver ↔ Metabase ↔
> DuckDB-format versions drift; if the pinned download 404s or the connection
> errors on version, override the URL:
> ```bash
> DUCKDB_DRIVER_URL=https://github.com/AlexR2D2/metabase_duckdb_driver/releases/download/<ver>/duckdb.metabase-driver.jar make bi-up
> ```
> The Parquet products (option 1) stand on their own regardless — BI is additive.

### 3. Direct SQL / power users
Analysts can attach `gold/serving.duckdb` read-only from any DuckDB client, or
point Superset/Looker at it (locally) or at the warehouse (in production).

## Per-consumer mapping

| Consumer | Product | Surface |
|---|---|---|
| **Ops** | `mart_ops_health` | Metabase dashboard + threshold alerts on mismatch/unreconciled rate |
| **CFO** | `mart_cfo_volume` (+ CSV) | Metabase report or scheduled spreadsheet extract |
| **Compliance** | `mart_compliance_audit` | Queryable audit table + Parquet evidence export |

## Production shape

`gold/` becomes an object-storage prefix (S3/GCS), partitioned by `reference_date`.
BI (Looker/Metabase) and analysts hit a warehouse (Snowflake/BigQuery) whose Gold
tables are these same models — or read the Parquet directly as external tables.
The publish contract (partition layout, schema) is unchanged; only the substrate
scales. See [`scalability.md`](scalability.md).
