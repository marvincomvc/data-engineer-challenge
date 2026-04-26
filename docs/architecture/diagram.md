# Architecture Diagram — Settlement Reconciliation Analytics

## End-to-End Overview

```mermaid
flowchart TD
    subgraph Sources["Source Systems"]
        PG["PostgreSQL\n(internal DB)\n─────────────\ntransactions\nreconciliation_runs\nreconciliation_results\nenterprise_company"]
        PS["PaySettler\n(external processor)\n─────────────\nsettlement_YYYY-MM-DD.csv\n• SETTLED / REVERSED rows\n• one file per day"]
    end

    subgraph Extraction["Extraction / Delivery"]
        CDC["Debezium CDC\n(Kafka Connect)\n─────────────\nOp: I / U / D\n_timestamp per event"]
        PGSYNC["Periodic export\nor direct COPY"]
        S3["Object Storage\n(S3 / local disk)\n─────────────\ntransactions_batch_*.parquet\nreconciliation_runs.parquet\nreconciliation_results.parquet\nenterprise_company.parquet\npaysettler/settlement_YYYY-MM-DD.csv"]
    end

    subgraph Pipeline["Analytical Pipeline  (Python + DuckDB)"]
        direction TB
        INGEST["① Ingest → bronze\n────────────────────\nread_parquet union_by_name\nall_varchar CSV\nschema validation\nfull-replace idempotency"]
        STOPS["② STOP quality checks\n────────────────────\nnull transaction_id\nunparseable amounts\ninvalid CSV status\nnull run_id in results"]
        SILVER["③ Transform → silver\n────────────────────\nCDC dedup (latest per key)\nBRL amount normalisation\nTIMESTAMPTZ coercion\nOp=D → is_deleted flag"]
        WARNS["④ WARN quality checks\n────────────────────\nnon-BRL currency\nnull merchant_id\nlow match rate < 50 %\norphan results"]
        GOLD["⑤ Build gold marts\n────────────────────\nDELETE + INSERT per date\natomic, idempotent"]
    end

    subgraph Warehouse["DuckDB Warehouse  (warehouse.duckdb)"]
        direction TB
        B["bronze\n─────────────\nraw CDC rows\nraw CSV amounts\nno deduplication"]
        SL["silver\n─────────────\ndeduped per key\nnormalised types\nis_deleted flag"]
        G["gold\n─────────────\npipeline_runs (audit)\nmart_operations\nmart_cfo\nmart_compliance"]
    end

    subgraph Consumers["Business Consumers"]
        OPS["Operations\n─────────────\nDaily reconciliation\nhealth dashboard\nmatch rate trend\ndiscrepancy volume"]
        CFO["CFO / Finance\n─────────────\nSettled volume by\nmerchant & day\nfinancial exposure\nfrom mismatches"]
        COMP["Compliance\n─────────────\nFull audit trail\nper transaction\nlineage from source\nto settlement"]
    end

    PG -->|"CDC events\n(Debezium)"| CDC
    PG -->|"enterprise_company\nsnapshot"| PGSYNC
    CDC --> S3
    PGSYNC --> S3
    PS -->|"daily file drop"| S3

    S3 --> INGEST
    INGEST --> STOPS
    STOPS -->|"abort on failure\n(QualityError)"| STOPS
    STOPS --> SILVER
    SILVER --> WARNS
    WARNS -->|"log warning\nnever abort"| WARNS
    WARNS --> GOLD

    INGEST -->|"raw rows"| B
    SILVER -->|"clean rows"| SL
    GOLD -->|"aggregated rows"| G

    G --> OPS
    G --> CFO
    G --> COMP
```

---

## Layer Responsibilities

| Layer | Location | What it contains | Idempotency |
|-------|----------|-----------------|-------------|
| **bronze** | `bronze.*` tables | Raw CDC rows — every Op (I/U/D) preserved, amounts as VARCHAR, no deduplication | Full `DELETE + INSERT` per table |
| **silver** | `silver.*` tables | One row per business key (latest CDC state), normalised types, `is_deleted` flag | `DELETE WHERE reference_date = ?` then `INSERT` |
| **gold** | `gold.*` tables | Aggregated marts for each consumer, full lineage columns | `DELETE WHERE reference_date = ?` then `INSERT` |

---

## Pipeline Execution Sequence

```mermaid
sequenceDiagram
    participant Scheduler
    participant Pipeline
    participant DuckDB

    Scheduler->>Pipeline: trigger(reference_date=2025-03-04)
    Pipeline->>DuckDB: INSERT pipeline_runs (status=RUNNING)

    Pipeline->>DuckDB: ingest transactions (union_by_name parquet)
    Pipeline->>DuckDB: ingest reconciliation_runs
    Pipeline->>DuckDB: ingest reconciliation_results
    Pipeline->>DuckDB: ingest merchants
    Pipeline->>DuckDB: ingest settlement CSV

    Pipeline->>DuckDB: STOP quality checks
    alt check fails
        DuckDB-->>Pipeline: violation count > 0
        Pipeline->>DuckDB: UPDATE pipeline_runs (status=FAILED)
        Pipeline-->>Scheduler: exit code 1
    end

    Pipeline->>DuckDB: build silver (dedup + normalise)
    Pipeline->>DuckDB: WARN quality checks (log only)
    Pipeline->>DuckDB: build mart_operations
    Pipeline->>DuckDB: build mart_cfo
    Pipeline->>DuckDB: build mart_compliance

    Pipeline->>DuckDB: UPDATE pipeline_runs (status=COMPLETED, counts=...)
    Pipeline-->>Scheduler: exit code 0
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage engine | Single DuckDB file | No external dependencies; ACID transactions; columnar scans are fast for analytical queries |
| Bronze amounts | `VARCHAR` | Preserves dirty source formats (`"R$ 23.696,77"`) for auditable normalisation in silver |
| CDC dedup | `ROW_NUMBER() OVER (PARTITION BY key ORDER BY _timestamp DESC)` | Handles both updates and replays deterministically |
| Idempotency | `DELETE WHERE date = ? + INSERT` | DuckDB 1.x lacks reliable `MERGE`; this pattern is atomic within a transaction |
| Schema drift | `read_parquet(..., union_by_name=True)` | `payment_method` was added in batch_2; `NULL` for older rows is semantically correct |
| STOP vs WARN | Two-tier quality gates | STOP protects data integrity (pipeline aborts); WARN signals business anomalies (pipeline continues) |
| Settlement lookup | Per-day file → consolidated fallback | Allows both generated (per-day) and sample-data (consolidated) layouts without code changes |
