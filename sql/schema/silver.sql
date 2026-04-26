-- Silver layer: CDC-deduplicated, type-normalized data.
-- Timestamps as TIMESTAMPTZ, amounts as DECIMAL(18,2).
-- Scoped by reference_date for idempotent DELETE + INSERT.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.transactions (
    transaction_id    VARCHAR NOT NULL,
    merchant_id       VARCHAR,
    amount            DECIMAL(18, 2),
    currency          VARCHAR,
    status            VARCHAR,
    payment_method    VARCHAR,
    created_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ,
    is_deleted        BOOLEAN DEFAULT false,
    _ingested_at      TIMESTAMPTZ DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS silver.reconciliation_runs (
    run_id               BIGINT NOT NULL,
    reference_date       DATE NOT NULL,
    file_name            VARCHAR,
    status               VARCHAR,
    total_transactions   BIGINT,
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ,
    _ingested_at         TIMESTAMPTZ DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS silver.reconciliation_results (
    result_id         BIGINT NOT NULL,
    run_id            BIGINT NOT NULL,
    transaction_id    VARCHAR NOT NULL,
    merchant_id       VARCHAR,
    category          VARCHAR NOT NULL,
    internal_amount   DECIMAL(18, 2),
    processor_amount  DECIMAL(18, 2),
    difference        DECIMAL(18, 2),
    reference_date    DATE,
    created_at        TIMESTAMPTZ,
    is_orphan_result  BOOLEAN DEFAULT false,
    _ingested_at      TIMESTAMPTZ DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS silver.settlement (
    transaction_id       VARCHAR NOT NULL,
    merchant_id          VARCHAR,
    amount               DECIMAL(18, 2),
    currency             VARCHAR,
    settled_at           TIMESTAMPTZ,
    processor_reference  VARCHAR,
    status               VARCHAR,
    _source_file         VARCHAR,
    _ingested_at         TIMESTAMPTZ DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS silver.merchants (
    merchant_id    VARCHAR NOT NULL,
    legal_name     VARCHAR,
    trade_name     VARCHAR,
    document       VARCHAR,
    primary_cnae   VARCHAR,
    created_at     TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ,
    _ingested_at   TIMESTAMPTZ DEFAULT current_timestamp
);
