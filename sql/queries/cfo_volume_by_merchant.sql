-- cfo_volume_by_merchant.sql
-- Consumer: CFO / Finance team
-- Questions: What is the total settled volume per merchant today?
--            Which merchants have the highest financial exposure from mismatches?
--            How does settled volume compare across merchants?
--
-- Usage (inside container):
--   python -c "import duckdb; c = duckdb.connect('/data/warehouse.duckdb'); c.sql(open('/app/sql/queries/cfo_volume_by_merchant.sql').read()).show()"

-- Settled volume and discrepancy by merchant (latest reference_date)
SELECT
    merchant_legal_name,
    merchant_trade_name,
    document,
    total_transactions,
    total_settled_amount,
    total_discrepancy_amount,
    matched_count,
    mismatched_count,
    ROUND(
        matched_count * 100.0 / NULLIF(total_transactions, 0),
    2) AS match_rate_pct
FROM gold.mart_cfo
WHERE reference_date = (SELECT MAX(reference_date) FROM gold.mart_cfo)
ORDER BY total_settled_amount DESC;


-- Merchants with highest financial exposure from mismatches
SELECT
    merchant_legal_name,
    SUM(total_discrepancy_amount) AS total_exposure,
    SUM(mismatched_count)         AS total_mismatches,
    COUNT(DISTINCT reference_date) AS days_with_mismatches
FROM gold.mart_cfo
WHERE mismatched_count > 0
GROUP BY merchant_legal_name
ORDER BY total_exposure DESC
LIMIT 10;


-- Consolidated volume across all dates
SELECT
    reference_date,
    SUM(total_settled_amount)    AS daily_settled_volume,
    SUM(total_discrepancy_amount) AS daily_discrepancy,
    COUNT(DISTINCT merchant_id)  AS active_merchants
FROM gold.mart_cfo
GROUP BY reference_date
ORDER BY reference_date DESC;
