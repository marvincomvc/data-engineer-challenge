# Review Decisions — what we fixed, deferred, and why

Two independent AI reviews were run against this solution: **CodeRabbit** (live agent
over the working tree) and **Codex** (see [`codex_review.md`](codex_review.md)). This
document records what we **deliberately did not implement now** and the reasoning —
so the boundary between "done" and "later" is an explicit decision, not an oversight.

## Fixed (for reference)

Applied across two commits on this branch. Both reviews independently flagged the same
top three, which we treated as the real issues and closed:

- **Processor records scoped to `reference_date`** (was: unscoped → cross-date double-count).
- **CFO per-currency grain**: currency carried through the fact; `(reference_date, currency)`;
  cross-currency pair → `MISMATCHED`; two-currency fixture + uniqueness test.
- **`meta.dq_check_result` populated** from dbt `run_results.json`.
- **Airflow parity**: DAG routes dbt through the CLI so stages + DQ are recorded under one `run_id`.
- **Run-header dedup** in ops/compliance marts + compliance grain test.
- **Audit reframed** as current-state (not "immutable"); `run_status` → `source_run_status`
  + `pipeline_health.sql`. Plus Airflow `retry_delay`, merchant dedup ordering, `nulls last`,
  config forwarding, Dockerfile cache layer.

---

## Deferred — with rationale and revisit trigger

### A. Production hardening the take-home explicitly invites deferring

The brief asks us to state what a production version would need rather than build it. These
are already discussed in [`scalability.md`](scalability.md) / [`data-products.md`](data-products.md);
we chose not to implement them in a local, single-day, filesystem exercise.

| Item (source) | Why deferred | Revisit when |
|---|---|---|
| **Late-arriving corrections / `fct_transaction` staleness outside the 7-day window** (Codex #6) | Requires a persisted watermark + CDC-`_timestamp`-driven incrementality + a late-arrival policy — a real ingestion-architecture change. The daily-batch, fixed-window model is the documented domain behavior. | Corrections/reversals to transactions older than the window must reflect in CFO history before a full refresh. |
| **Source-freshness checks** (expected business date present, empty file, file received twice, duplicate `processor_reference`) (Codex) | These guard the *real* SFTP/S3 feed; the local fixture is a single static file. Belongs with `dbt source freshness` + an ingestion contract in production. | Onboarding the real PaySettler delivery. |
| **Execution timeout, SLA/freshness alerting, concurrency controls** (Codex) | Operational concerns of a scheduled production DAG; the local DAG is a demonstrator (SequentialExecutor). | Running on MWAA/Composer with an SLA. |
| **Tighter dependency pinning / lock file** (Codex) | Minimum-version ranges keep the take-home easy to build; a lock file is a reproducibility hardening step. | CI/CD + reproducible production builds. |

### B. Design/scope decisions with a documented upgrade path

Deliberate current-state simplifications; each has a stated path in [`data-products.md`](data-products.md).

| Item (source) | Why deferred | Revisit when |
|---|---|---|
| **Append-only audit fact** keyed by `(run_id, transaction_id)` (Codex #5) | The current-state fact + `run_id`/`_loaded_at` provenance meets "what is the current decision, by which run"; full decision *lineage* across re-runs is a second fact, not a rewrite. Reframed the claim so we don't over-promise. | Compliance must reconstruct what an *earlier* run decided after a correction. |
| **SCD2 merchant dimension** (both) | Reproducing a past day's categorization is met by the date-keyed fact; merchant attributes rarely change. Upgrade = one dbt snapshot + a join swap. | Point-in-time merchant identity (name/CNAE as-of) is required. |
| **Cross-processor collision policy** — quarantine vs. "latest settlement wins" (Codex) | With a single processor today there is no collision. Latest-wins is a defensible default and the seam (`stg_settlements`) is where a quarantine rule would live. | A second processor is onboarded and the same `transaction_id` can arrive twice. |
| **Per-merchant CFO mart** (`mart_cfo_by_merchant`) (Codex #2) | The CFO product answers day/currency-level questions; per-merchant is a distinct, additive mart. | CFO asks for merchant-level revenue breakdowns. |

### C. Latent issues — correct on current data, guarded by a note

Real sharp edges that do not fire on the current feed; we left a code comment rather than
add machinery, to keep the surface small.

| Item (source) | Why deferred | Revisit when |
|---|---|---|
| **`delete+insert` removes only matching keys** — a `transaction_id` that disappears from a `reference_date` on re-run leaves a stale row (CodeRabbit #3) | Requires whole-partition replacement (`insert_overwrite`) semantics. In this domain a settled transaction does not vanish from its business day; grain-uniqueness still holds. | A corrected re-run can legitimately *drop* rows for a date (e.g. a smaller replacement file). |
| **`clean_amount` treats `1,234.56` (US thousands) as pt-BR** (CodeRabbit #12) | The PaySettler feed is pt-BR; the macro is deterministic for it. | A US-format processor adapter is added (would get its own parsing in its `stg_` adapter). |
| **`publish` `OVERWRITE_OR_IGNORE` doesn't clear a partition dir** — fewer part-files on re-run could leave stale ones (CodeRabbit #14) | At current single-file-per-partition scale a re-publish overwrites the one file. | Partitions emit a variable number of part-files at scale (add a clean-then-write). |
| **Residual SQL string interpolation** for `reference_date`/`run_id` in models (CodeRabbit #10) | Inputs are validated (`date.fromisoformat`) or generated (`uuid4`); CLI vars now go through `json.dumps`. Not attacker-reachable here. | Any untrusted value could ever reach these vars → switch to parameter binding. |
| **CFO CSV extract dumps all dates** while the Parquet is per-partition (CodeRabbit #16) | Convenience spreadsheet export; the Parquet is the contract. | Consumers treat the CSV as the authoritative grain. |

### D. Reviewed — no change (push-back)

| Item (source) | Decision |
|---|---|
| **"8-day window"** — `[reference_date − 7, reference_date]` spans 8 dates (CodeRabbit #15) | **Correct as-is.** The glossary specifies the interval *inclusive on both ends*, which is 8 calendar dates. Documented and tested; intentional. |

---

## Principle

The recurring theme across both reviews was *documentation claiming more than the code
delivered*. Our response was to **close that gap in both directions**: implement the items
that change real outputs (currency, scoping, DQ persistence, orchestration parity), and
**honestly reframe or explicitly defer** the rest — never leave a claim standing that the
code doesn't back. Each deferral above names the concrete trigger that would move it into
scope.
