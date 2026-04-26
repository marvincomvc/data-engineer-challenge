-- Bronze layer: raw ingested data with CDC columns and _ingested_at lineage field.
-- No deduplication or type normalization at this layer.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.transactions (
    id                BIGINT,
    transaction_id    VARCHAR,
    merchant_id       VARCHAR,
    amount            DOUBLE,
    currency          VARCHAR,
    status            VARCHAR,
    description       VARCHAR,
    payment_method    VARCHAR,   -- present only in batch_2; NULL for batch_1 rows
    created_at        VARCHAR,
    updated_at        VARCHAR,
    op                VARCHAR,
    _timestamp        VARCHAR,
    _ingested_at      TIMESTAMPTZ DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS bronze.reconciliation_runs (
    id                   BIGINT,
    reference_date       VARCHAR,
    file_name            VARCHAR,
    status               VARCHAR,
    total_transactions   BIGINT,
    started_at           VARCHAR,
    completed_at         VARCHAR,
    created_at           VARCHAR,
    op                   VARCHAR,
    _timestamp           VARCHAR,
    _ingested_at         TIMESTAMPTZ DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS bronze.reconciliation_results (
    id                BIGINT,
    run_id            BIGINT,
    transaction_id    VARCHAR,
    merchant_id       VARCHAR,
    category          VARCHAR,
    internal_amount   DOUBLE,
    processor_amount  DOUBLE,
    difference        DOUBLE,
    created_at        VARCHAR,
    op                VARCHAR,
    _timestamp        VARCHAR,
    _ingested_at      TIMESTAMPTZ DEFAULT current_timestamp
);

-- amount is VARCHAR in the source CSV; may be "R$ 9.087,52" format.
CREATE TABLE IF NOT EXISTS bronze.settlement (
    transaction_id       VARCHAR,
    merchant_id          VARCHAR,
    amount               VARCHAR,
    currency             VARCHAR,
    settled_at           VARCHAR,
    processor_reference  VARCHAR,
    status               VARCHAR,
    _source_file         VARCHAR,
    _ingested_at         TIMESTAMPTZ DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS bronze.merchants (
    id             BIGINT,
    merchant_id    VARCHAR,
    legal_name     VARCHAR,
    trade_name     VARCHAR,
    document       VARCHAR,
    primary_cnae   VARCHAR,
    created_at     VARCHAR,
    updated_at     VARCHAR,
    op             VARCHAR,
    _timestamp     VARCHAR,
    _ingested_at   TIMESTAMPTZ DEFAULT current_timestamp
);
