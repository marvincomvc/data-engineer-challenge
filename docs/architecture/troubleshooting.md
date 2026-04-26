# Troubleshooting Runbook — "Dashboards Without Data Since Friday"

**Scenario:** Monday morning. The Operations dashboard shows no reconciliation data for Friday (and possibly Saturday/Sunday). This runbook guides the on-call engineer from symptom to resolution in under 30 minutes.

---

## Step 1 — Establish what is missing

Connect to the DuckDB warehouse and query the audit log:

```sql
-- Which reference_dates are missing from gold, relative to the last 7 days?
SELECT
    gs.reference_date::DATE AS reference_date,
    pr.status,
    pr.error_message,
    pr.started_at,
    pr.completed_at
FROM generate_series(current_date - 7, current_date, INTERVAL 1 DAY) gs(reference_date)
LEFT JOIN gold.pipeline_runs pr
       ON pr.reference_date = gs.reference_date::DATE
ORDER BY gs.reference_date DESC;
```

| Result | What it means |
|--------|--------------|
| Row with `status = 'COMPLETED'` | Pipeline ran successfully — data IS there, issue is in the BI layer |
| Row with `status = 'FAILED'` | Pipeline ran but aborted — check `error_message` column |
| Row with `status = 'RUNNING'` | Pipeline started but never finished — likely a crash or OOM |
| **No row at all** | Pipeline was never triggered for that date |

---

## Step 2 — Decision tree

```
Missing data since Friday
│
├─► status = COMPLETED?
│       └─► Data is in DuckDB. Check mart row counts:
│               SELECT COUNT(*) FROM gold.mart_operations
│               WHERE reference_date = '2025-03-07'::DATE;
│           If 0 rows → silver was empty → go to Step 3B (source files)
│           If rows exist → BI tool / connection issue, not a pipeline problem
│
├─► status = FAILED?
│       └─► Read error_message from pipeline_runs.
│           ├─► "Quality STOP failures [unparseable_settlement_amount]"
│           │       → Settlement CSV has dirty amounts → go to Step 3A
│           ├─► "Quality STOP failures [invalid_settlement_status]"
│           │       → PaySettler sent unknown status values → notify PaySettler team
│           ├─► "Quality STOP failures [null_transaction_id]"
│           │       → CDC export has corrupted rows → check Debezium/export job
│           ├─► "FileNotFoundError: No settlement CSV"
│           │       → CSV was not delivered → go to Step 3B
│           └─► Any other exception
│               → Read structured logs (Step 4) for the full stack trace
│
├─► status = RUNNING (stale)?
│       └─► Check when it started:
│               SELECT started_at FROM gold.pipeline_runs
│               WHERE status = 'RUNNING' ORDER BY started_at;
│           If started > 2 hours ago → process crashed mid-run (OOM, killed)
│           Safe to re-run: idempotent pipeline will resume cleanly
│
└─► No row?
        └─► Scheduler / orchestrator did not trigger the pipeline
            Check cron logs, Airflow DAG, or manual trigger history
```

---

## Step 3A — Source file investigation

### Settlement CSV not delivered or malformed

```bash
# Check if the per-day file exists
ls -lh /app/docs/sample-data/paysettler/settlement_2025-03-07.csv

# Check file size (a zero-byte file or truncated file can cause silent issues)
wc -l /app/docs/sample-data/paysettler/settlement_2025-03-07.csv

# Spot-check header and first rows
head -3 /app/docs/sample-data/paysettler/settlement_2025-03-07.csv
```

Expected header:
```
transaction_id,merchant_id,amount,currency,settled_at,processor_reference,status
```

Common file issues and fixes:

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| File missing entirely | PaySettler delivery failed | Request re-delivery; re-run with `--reference-date` once file arrives |
| File has 0 rows (header only) | PaySettler had no settlements that day | Legitimate — re-run pipeline; mart will have 0 settlement rows |
| `amount` column has `null` or `""` values | Upstream encoding bug | Work with PaySettler to fix; manually patch known-good values if critical |
| `status` not in `SETTLED / REVERSED` | New status introduced by PaySettler | Update `invalid_settlement_status` check; deploy and re-run |
| File delivered in wrong path | Directory layout changed | Verify `DATA_PATH` env var; update `find_settlement_file()` if layout changed |

---

## Step 3B — Source parquet investigation

```bash
# Check if parquet files are present and non-empty
docker compose run --rm pipeline python -c "
import duckdb
c = duckdb.connect(':memory:')
print(c.sql(\"SELECT COUNT(*) FROM read_parquet('/app/docs/sample-data/transactions_batch_*.parquet', union_by_name=True)\").fetchone())
print(c.sql(\"SELECT COUNT(*) FROM read_parquet('/app/docs/sample-data/reconciliation_results.parquet')\").fetchone())
"
```

If parquets are missing or empty, the CDC export job (Debezium → S3) failed upstream. Escalate to the platform/infrastructure team.

---

## Step 4 — Read structured logs

The pipeline emits JSON-structured logs. Each entry has `step`, `reference_date`, `level`, and `message` fields.

```bash
# In Docker Compose environment
docker compose logs pipeline 2>&1 | python -c "
import sys, json
for line in sys.stdin:
    try:
        entry = json.loads(line)
        if entry.get('level') in ('ERROR', 'WARNING'):
            print(entry)
    except:
        print(line, end='')
"
```

Key log entries to look for:

| `step` | `level` | What happened |
|--------|---------|--------------|
| `ingest.settlement` | WARNING | `"per-day CSV not found, using fallback"` — per-day file missing, used consolidated |
| `quality.stop_checks` | ERROR | `"pipeline aborted: STOP quality check failed"` — data integrity violation |
| `quality.*` | WARNING | `"check failed"` — WARN-level anomaly (pipeline continued) |
| `error` | ERROR | `"pipeline failed with unexpected error"` — unhandled exception, see `exc_info` |

---

## Step 5 — Re-run the pipeline

Once the root cause is fixed (file re-delivered, CDC export restored, etc.):

```bash
# Re-run for the specific missing date
make pipeline-date DATE=2025-03-07

# Or for multiple missing dates (run sequentially)
for date in 2025-03-07 2025-03-08 2025-03-09; do
    make pipeline-date DATE=$date
done
```

The pipeline is **fully idempotent**: re-running for a date that already has gold data will replace it with fresh data without duplicating rows. The `pipeline_runs` audit log will accumulate an additional COMPLETED entry each time.

After re-run, verify:

```sql
-- Confirm all expected dates now have COMPLETED status
SELECT reference_date, status, counts->>'mart_compliance' AS compliance_rows
FROM gold.pipeline_runs
WHERE reference_date >= '2025-03-07'::DATE
ORDER BY reference_date;
```

---

## Quick-reference checklist

- [ ] Query `gold.pipeline_runs` for missing dates — identify status (COMPLETED / FAILED / RUNNING / missing)
- [ ] If FAILED: read `error_message` — categorise as source file, quality check, or unexpected error
- [ ] If RUNNING (stale): confirm process is dead, then re-run
- [ ] If missing row: check scheduler / cron logs
- [ ] Verify source files exist and are non-empty
- [ ] Fix root cause (request re-delivery, patch bad values, etc.)
- [ ] Re-run with `make pipeline-date DATE=YYYY-MM-DD`
- [ ] Confirm COMPLETED status and non-zero mart row counts
- [ ] Notify stakeholders that dashboards are back
