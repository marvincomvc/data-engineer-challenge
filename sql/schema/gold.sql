-- Gold layer: analytical marts and pipeline observability.

CREATE SCHEMA IF NOT EXISTS gold;

-- Pipeline execution log — the first thing Ops queries on Monday morning.
CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
    run_id           VARCHAR NOT NULL PRIMARY KEY,
    reference_date   DATE NOT NULL,
    status           VARCHAR NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ,
    duration_seconds INTEGER,
    counts           JSON,
    error_message    VARCHAR
);

-- Consumer: Operations — daily reconciliation health per run.
CREATE TABLE IF NOT EXISTS gold.mart_operations (
    reference_date                  DATE NOT NULL,
    run_id                          BIGINT NOT NULL,
    file_name                       VARCHAR,
    run_status                      VARCHAR,
    run_duration_seconds            INTEGER,
    total_transactions              BIGINT,
    matched_count                   BIGINT,
    mismatched_count                BIGINT,
    unreconciled_processor_count    BIGINT,
    unreconciled_internal_count     BIGINT,
    match_rate_pct                  DECIMAL(5, 2),
    total_discrepancy_amount        DECIMAL(18, 2),
    _pipeline_run_id                VARCHAR,
    _loaded_at                      TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (reference_date, run_id)
);

-- Consumer: CFO — consolidated financial volume by day and merchant.
CREATE TABLE IF NOT EXISTS gold.mart_cfo (
    reference_date               DATE NOT NULL,
    merchant_id                  VARCHAR NOT NULL,
    merchant_legal_name          VARCHAR,
    merchant_trade_name          VARCHAR,
    document                     VARCHAR,
    primary_cnae                 VARCHAR,
    total_transactions           BIGINT,
    total_settled_amount         DECIMAL(18, 2),
    total_discrepancy_amount     DECIMAL(18, 2),
    matched_count                BIGINT,
    mismatched_count             BIGINT,
    unreconciled_internal_count  BIGINT,
    unreconciled_processor_count BIGINT,
    _pipeline_run_id             VARCHAR,
    _loaded_at                   TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (reference_date, merchant_id)
);

-- Consumer: Compliance — full audit trail per transaction per run.
CREATE TABLE IF NOT EXISTS gold.mart_compliance (
    transaction_id         VARCHAR NOT NULL,
    run_id                 BIGINT NOT NULL,
    reference_date         DATE NOT NULL,
    file_name              VARCHAR,
    merchant_id            VARCHAR,
    category               VARCHAR NOT NULL,
    internal_amount        DECIMAL(18, 2),
    processor_amount       DECIMAL(18, 2),
    difference             DECIMAL(18, 2),
    internal_status        VARCHAR,
    transaction_created_at TIMESTAMPTZ,
    result_created_at      TIMESTAMPTZ,
    run_started_at         TIMESTAMPTZ,
    run_completed_at       TIMESTAMPTZ,
    _pipeline_run_id       VARCHAR,
    _ingested_at           TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (transaction_id, run_id)
);
