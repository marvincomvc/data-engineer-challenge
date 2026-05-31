-- Gold: daily reconciliation health overview
SELECT reference_date,
       total_results,
       matched_count,
       mismatched_count,
       unreconciled_processor_count,
       unreconciled_internal_count,
       ROUND(discrepancy_rate * 100, 2) AS discrepancy_pct
  FROM gold.daily_reconciliation_health
 ORDER BY reference_date;
