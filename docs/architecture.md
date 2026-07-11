# Part 3.1 — Architecture

> Diagrams are [Mermaid](https://mermaid.js.org/) and render automatically on GitHub
> (in the PR / file view). ASCII fallbacks live in the git history of this file.

## Local pipeline (this repository)

```mermaid
flowchart TB
    classDef src fill:#e8eef7,stroke:#5b7aa8,color:#1a2c42;
    classDef bronze fill:#f4e6cf,stroke:#b08637,color:#3a2c10;
    classDef silver fill:#e6e9ec,stroke:#8a97a3,color:#26303a;
    classDef gold fill:#f7efc9,stroke:#c9a634,color:#3d3208;
    classDef pub fill:#dcefe4,stroke:#4f9d76,color:#123425;
    classDef obs fill:#efe3f2,stroke:#9a63a8,color:#341b3d;

    subgraph SRC["Sources · filesystem"]
        CDC["CDC parquet<br/>transactions · runs · results · merchants<br/>(Op / _timestamp)"]
        CSV["PaySettler CSV"]
    end

    BRONZE["Bronze · bronze.raw_*<br/>raw landing + lineage<br/>_source_file · _ingested_at · _run_id"]

    subgraph SILVER["Silver · dbt staging"]
        STG["stg_transactions · stg_*<br/>CDC collapse + dirty-data repair"]
        SET["stg_settlements<br/>canonical processor union"]
    end

    INT["int_reconciliation<br/>window 7d · tolerance R$0,01<br/>FULL OUTER JOIN on transaction_id"]

    subgraph GOLD["Gold · dbt marts"]
        DIM["dims<br/>merchant · date · category"]
        FCT["fct_reconciliation_result · fct_transaction<br/>incremental · delete+insert"]
        MART["marts<br/>ops_health · cfo_volume · compliance_audit"]
    end

    DQ{{"dbt test<br/>error = block · warn = alert"}}
    PUB["Publish<br/>gold/**.parquet (partition = reference_date)<br/>+ serving.duckdb views"]

    subgraph CONS["Consume"]
        MB["Metabase"]
        FILES["Parquet · pandas · Spark"]
        SQLC["SQL / warehouse"]
    end

    OBS[("meta.pipeline_run<br/>meta.dq_check_result")]

    CDC --> BRONZE
    CSV --> BRONZE
    BRONZE --> STG --> SET --> INT
    STG --> INT
    INT --> FCT
    DIM --> MART
    FCT --> MART
    MART --> DQ --> PUB
    PUB --> MB
    PUB --> FILES
    PUB --> SQLC
    BRONZE -. observes .-> OBS
    DQ -. writes .-> OBS

    class CDC,CSV src;
    class BRONZE bronze;
    class STG,SET silver;
    class INT,DIM,FCT,MART gold;
    class PUB pub;
    class OBS obs;
```

<sub>📎 If Mermaid doesn't render in your viewer: [static PNG](img/pipeline.png).</sub>

- **Ingest (Python + DuckDB):** lands each source into `bronze` with lineage columns,
  streaming parquet under a `memory_limit` so a 5M-row file never needs to fit in RAM.
  Transactional full-replace → Bronze is idempotent.
- **Transform (dbt-duckdb):** staging collapses CDC to current state and repairs dirty
  data; `stg_settlements` is the canonical processor seam; `int_reconciliation`
  recomputes categories; marts are incremental (`delete+insert` on the natural key).
- **Quality gates (dbt test):** integrity (`error`) blocks, anomaly (`warn`) alerts;
  every result lands in `meta.dq_check_result`.
- **Publish:** Gold → partitioned Parquet products + a `serving.duckdb` view catalog,
  so consumers never touch the single-writer warehouse.

## Orchestration (Airflow DAG)

```mermaid
flowchart LR
    classDef t fill:#eef2f7,stroke:#5b7aa8,color:#1a2c42;
    A["ingest<br/>python -m pipeline ingest"] --> B["dbt run<br/>Silver + Gold"]
    B --> C["dbt test<br/>error → task fails"]
    C --> D["publish<br/>Parquet products"]
    class A,B,C,D t;
```

<sub>📎 Static PNG fallback: [dag.png](img/dag.png).</sub>

Linear and **sequential** because DuckDB is single-writer — one task holds the warehouse
file at a time. Parametrized by `reference_date` (the run's logical date), `retries=2`;
idempotency makes every retry/backfill safe.

## Production topology (how this maps to the cloud)

```mermaid
flowchart TB
    classDef store fill:#e8eef7,stroke:#5b7aa8,color:#1a2c42;
    classDef proc fill:#f7efc9,stroke:#c9a634,color:#3d3208;
    classDef out fill:#dcefe4,stroke:#4f9d76,color:#123425;

    PG[("Postgres<br/>settlement_db")] -->|Debezium CDC| K["Kafka / Kinesis"]
    PS["PaySettler"] -->|SFTP / S3 drop| L
    K --> L["Landing<br/>S3 / GCS parquet<br/>partitioned by date"]

    subgraph WH["Warehouse — same dbt project, swap the adapter"]
        direction LR
        BRZ["Bronze"] --> SLV["Silver"] --> GLD["Gold"]
    end
    L --> BRZ

    GLD --> BI["BI · Looker / Metabase"]
    GLD --> RE["reverse-ETL"]
    GLD --> AU["audit exports"]

    ORCH["Airflow / MWAA / Composer"] -. orchestrates .-> L
    ORCH -. orchestrates .-> WH
    WH -. dbt artifacts + DQ .-> MON["Datadog / Grafana"]

    class PG,PS,K,L store;
    class BRZ,SLV,GLD proc;
    class BI,RE,AU,MON out;
```

<sub>📎 Static PNG fallback: [production.png](img/production.png).</sub>

The **dbt project is portable**: the same models run on Snowflake/BigQuery by changing the
adapter in `profiles.yml`. Bronze becomes a partitioned object-store landing zone; ingest
becomes CDC (Debezium→Kafka) instead of a batch parquet extract. The medallion layering,
the reconciliation logic, the idempotency strategy, and the DQ gates are unchanged — only
the substrate scales (see [`scalability.md`](scalability.md)).

## Key design decisions (full ADRs in the DESIGN doc)

| Decision | Why |
|---|---|
| Recompute reconciliation (don't ingest results) | Owns the domain logic; exercises the dirty-data traps; testable |
| Idempotency = delete+insert on natural key, per `reference_date` | Re-runs / backfills / retries never duplicate or corrupt |
| DQ = dbt `error` (integrity) vs `warn` (anomaly) | Block on structural breakage, alert on distribution drift |
| CDC collapse in staging (latest-per-key, drop deletes) | Deterministic current state without a stateful CDC engine |
| Canonical `stg_settlements` seam | Adding a processor = one adapter + one union line, engine unchanged |
| Publish to Parquet + serving views | Consumers decoupled from the single-writer warehouse |
| Sequential Airflow DAG | Respects DuckDB single-writer |
