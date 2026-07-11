# Part 3.2 — Troubleshooting Runbook

> Monday morning. Ops reports the reconciliation dashboards have had no data since
> Friday night. I'm first in. Here's how I'd work it.

## 0. Frame it (30 seconds)

"No data since Friday night" = either the **pipeline didn't run**, **ran and
failed**, or **ran and produced empty/blocked output**. The pipeline is built to
answer which one *from its own artifacts* before I touch any code.

## 1. Did it run, and how did it end? — `meta.pipeline_run`

```sql
select reference_date, stage, status, started_at, finished_at, error
from meta.pipeline_run
where reference_date >= current_date - 4
order by started_at desc;
```

- **No rows since Friday** → the scheduler never fired. Go to Airflow (step 2).
- **Last row `stage='ingest' status='FAILED'`** → source problem (step 3).
- **`stage='dbt_test' status='FAILED'`** → an integrity gate blocked publish (step 4).
- **All `COMPLETED` but dashboards empty** → data is there but thin/filtered (step 5).

## 2. Scheduler / orchestration — Airflow

- Is the `settlement_reconciliation` DAG **unpaused** and are Fri/Sat/Sun runs
  present? Check for runs stuck in `queued` (worker down) or `up_for_retry`.
- Read the failed task log. Because the DAG is `ingest >> dbt_run >> dbt_test`,
  the first red task localizes the stage immediately.
- Retries are configured (2×); if all retries burned, the underlying cause is
  persistent (not transient) — proceed to step 3/4.

## 3. Ingest failed — the source

Most likely a **missing or malformed PaySettler file** (the classic "Friday night"
cause: upstream didn't drop the file, or dropped a truncated one).

```sql
-- what did we last land, and from which file?
select _source_file, count(*), max(_ingested_at)
from bronze.raw_paysettler group by 1;
```

- File absent → escalate to the PaySettler integration owner; this is upstream.
- File present but ingest raised → check the structured logs (`event="bronze.load_failed"`,
  filter by `run_id`) for the parse error.

## 4. `dbt test` blocked publish — an integrity gate fired

```sql
select check_name, severity, failure_count, checked_at
from meta.dq_check_result
where reference_date >= current_date - 4 and passed = false
order by checked_at desc;
```

- A `severity='error'` failure (e.g. `not_null_stg_paysettler_amount`,
  `assert_fct_grain_unique`) is **intentional** — it stopped bad data from reaching
  the dashboards. Inspect the offending rows in `bronze.raw_paysettler` /
  staging, decide fix-forward (repair rule) or reject-and-reprocess.
- `severity='warn'` rows did **not** block; they're signal, not the outage cause.

## 5. Everything COMPLETED but dashboards empty

- Confirm the marts actually have Friday's slice:
  `select max(reference_date) from mart_ops_health;`
- If the fact has the date but marts don't → a mart model or the BI query filter.
- If `reference_date` inference picked the wrong day (generic filename), the data
  landed under an unexpected date — check `mart_ops_health` for the real date.

## 6. How far I'd go before escalating

I'd resolve it myself if it's: a DQ gate I can fix-forward, a stale/paused DAG, a
mart bug, or a re-runnable transient failure (the pipeline is **idempotent** — I can
safely `make run REFERENCE_DATE=<fri>` to reprocess without duplicates). I'd
**escalate** when the root cause is outside the pipeline: PaySettler didn't deliver
the file, source Postgres/CDC is down, or the warehouse/infra is unavailable —
those need the upstream/platform owners, and I'd hand off with the exact
`pipeline_run` / `dq_check_result` evidence already in hand.
