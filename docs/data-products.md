# Part 1 — Data Products: Modeling & Analytics

This document explains the analytical model: what each consumer can ask, what it
deliberately **cannot**, and why. Example queries live in [`sql/examples/`](../sql/examples).

## Modeling approach — medallion + dimensional marts

```
Bronze (raw CDC/CSV + lineage)
   └─ Silver (staging: CDC-collapsed current state, dirty-data repaired)
        └─ Gold
             ├─ core: dim_merchant, dim_date, dim_category,
             │        fct_reconciliation_result (grain: reference_date × transaction_id),
             │        fct_transaction (grain: transaction_id)
             └─ consumer marts: mart_ops_health · mart_cfo_volume · mart_compliance_audit
```

`fct_reconciliation_result` is the **immutable spine**: one row per transaction per
business day, categorized by our own reconciliation engine. Every consumer mart is
a projection of that fact joined to conformed dimensions. Why this shape:

- **A star schema is the least-surprising, most defensible choice** for three
  heterogeneous consumers — each mart reuses the same conformed `dim_merchant` /
  `dim_date` / `dim_category`, so "mismatch rate" means the same thing to Ops and
  the CFO.
- **The fact is additive and date-partitioned**, which is exactly what the
  idempotency (delete+insert by `reference_date`) and the scale story (partition
  pruning) need.

---

## Operations — `mart_ops_health`

**Pain:** "Is the discrepancy rate rising, and what's my backlog today?"

| Can answer | Cannot answer (by design) |
|---|---|
| Daily matched / mismatch / unreconciled **rates** and day-over-day trend | Root cause of an individual mismatch (that's a drill-down into the fact) |
| Investigation backlog (`needs_investigation`) and financial exposure per day | Sub-daily / intraday health (source is a daily file) |
| Which `reference_date` had a **FAILED** run (missing/stale data signal) | Per-merchant health (available in the fact/compliance mart, not this daily rollup) |

**Why these:** ops needs a *trend and a triage number*, not a row dump. One row per
`reference_date` keeps the dashboard query O(days). Per-merchant slicing is
deliberately pushed to the compliance mart to keep this one focused.

## CFO — `mart_cfo_volume`

**Pain:** "What did we move per day, how much is trustworthy, what's my exposure?"

| Can answer | Cannot answer (by design) |
|---|---|
| Settled vs. **confirmed (matched)** volume per day & currency | Per-merchant revenue breakdown (a future `mart_cfo_by_merchant`) |
| Financial **exposure** under investigation (Σ mismatch difference) | Fees / net revenue (not in source data) |
| **Reversal** (chargeback) volume and rate — a risk signal | Forecasts / projections (BI/planning concern) |

**Why these:** the CFO's core question is "how much money, and can I trust it?" —
so we separate *settled* (what the processor claims) from *confirmed* (matched
within tolerance) and quantify exposure and reversals. Fees/net are excluded
because the source contains gross settlement only — a scope boundary, stated
rather than faked.

## Compliance — `mart_compliance_audit`

**Pain:** "For any transaction, prove what was decided, for whom, by which run."

| Can answer | Cannot answer (by design) |
|---|---|
| Full per-transaction audit record (category, amounts, merchant legal identity, run, source file, `audited_at`) | Point-in-time merchant *history* (name/CNAE as-of a past date) — needs SCD2 |
| Per-merchant discrepancy summary (who to audit) | Who/what *changed* a record (no user-action log in source) |
| Completeness/immutability checks (no audit gaps) | |

**Why current-state merchant, not SCD2:** the *audit* need — reproducing a past
day's categorization — is already met by the immutable, date-keyed fact. Merchant
attributes rarely change and full SCD2 (via a dbt snapshot on `_timestamp`) is a
documented, low-risk extension rather than build-now scope. This is the main YAGNI
call in Part 1; the upgrade path is one `snapshots/` file + a join swap.

---

## Correctness & the cross-check caveat

The pipeline **recomputes** categorization from raw sources (see
[`int_reconciliation.sql`](../dbt/models/intermediate/int_reconciliation.sql)); it
does not ingest the provided `reconciliation_results`. Rules implemented exactly
per the glossary: match on `transaction_id`, internal window
`[reference_date − 7d, reference_date]`, tolerance R$0,01.

**Important data caveat:** the sample `settlement_paysettler.csv` is a *synthetic
standalone fixture* — 510 rows all dated 2025-03-16 spanning the whole month's
transactions, with seeded amount differences on every overlapping txn. It is **not**
the file that produced the historical `reconciliation_results` (run 5 processed 98
txns; the CSV has 510). So a recompute against the sample yields ~0 MATCHED and
mostly MISMATCHED/UNRECONCILED — expected, not a bug. Consequences:

- **Correctness is proven with controlled unit fixtures** (`tests/test_reconciliation.py`)
  where inputs → categories are known, covering all four categories plus the
  tolerance and window boundaries and CDC collapse.
- The **cross-check test** (`assert_recompute_vs_source_distribution`) is a
  `severity: warn` signal that intentionally fires on the synthetic sample —
  demonstrating the anomaly gate working. Fed the true daily file in production,
  it should stay green within tolerance.

## Example queries

```bash
duckdb warehouse/settlement.duckdb < sql/examples/ops_health.sql
duckdb warehouse/settlement.duckdb < sql/examples/cfo_volume.sql
duckdb warehouse/settlement.duckdb < sql/examples/compliance_audit.sql
```
