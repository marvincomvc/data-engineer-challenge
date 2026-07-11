# Review Decisions — what we fixed, deferred, and why

Multiple review rounds were run against this solution across two independent tools:
**CodeRabbit** (two rounds, live agent
over the working tree) and **Codex** (see [`codex_review.md`](codex_review.md)). This
document records what we **deliberately did not implement now** and the reasoning —
so the boundary between "done" and "later" is an explicit decision, not an oversight.

## Fixed (for reference)

Applied across several commits on this branch. Both reviews independently flagged the same
top items, which we treated as the real issues and closed:

- **Processor records scoped to `reference_date`** (was: unscoped → cross-date double-count).
- **CFO per-currency grain**: currency carried through the fact; `(reference_date, currency)`;
  cross-currency pair → `MISMATCHED`; two-currency fixture + uniqueness test.
- **`meta.dq_check_result` populated** from dbt `run_results.json` (even on a failed gate).
- **Airflow parity**: DAG routes dbt through the CLI so stages + DQ are recorded under one `run_id`.
- **Run-header dedup** in ops/compliance marts + compliance grain test.
- **Audit reframed** as current-state (not "immutable"); `run_status` → `source_run_status`
  + `pipeline_health.sql`.
- **Full-partition replacement on re-run** (delete+insert by `reference_date`): a corrected
  re-run that *drops* a transaction no longer leaves a stale row.
- **Determinism & correctness hardening** (Cubic round): equal-time collision tie-break,
  categorize by join-presence not amount-nullability, corrupt-amount guard, cross-currency
  exposure fix, `run_id` injection guard, `meta` schema auto-create, DQ dedup on retry,
  readable check names, and added tests (compliance/`stg_settlements` schema tests,
  date-isolation, two-currency). Plus Airflow `retry_delay`, merchant dedup ordering,
  `nulls last`, config forwarding, Dockerfile cache layer.

---

## Deferred — with rationale and revisit trigger

### A. Production hardening the take-home explicitly invites deferring

The brief asks us to state what a production version would need rather than build it. These
are already discussed in [`scalability.md`](scalability.md) / [`data-products.md`](data-products.md);
we chose not to implement them in a local, single-day, filesystem exercise.

| Item (source) | Why deferred | Revisit when |
|---|---|---|
| **Full CDC-watermark incrementality** for `fct_transaction` (Codex) | Partially addressed: the incremental filter now refreshes rows created OR *updated* within the window, so in-window late status changes are picked up. A true persisted watermark for arbitrarily-old updates is the remaining ingestion-architecture change. | Corrections to transactions whose `updated_at` is also outside the window must reflect before a full refresh. |
| **Source-freshness checks** (expected business date present, empty file, file received twice, duplicate `processor_reference`) (Codex) | These guard the *real* SFTP/S3 feed; the local fixture is a single static file. Belongs with `dbt source freshness` + an ingestion contract in production. | Onboarding the real PaySettler delivery. |
| **Execution timeout, SLA/freshness alerting, concurrency controls** (Codex) | Operational concerns of a scheduled production DAG; the local DAG is a demonstrator (SequentialExecutor). | Running on MWAA/Composer with an SLA. |
| **Tighter dependency pinning / lock file** (Codex) | Acknowledged this is more than production hardening — a lock file also improves *evaluator* reproducibility. Deferred only because the pinned Docker image already gives a consistent build for review; a lock file (`uv.lock`/`pip-tools`) is the clean next step. | Any environment where the Docker image isn't the build boundary. |

### B. Design/scope decisions with a documented upgrade path

Deliberate current-state simplifications; each has a stated path in [`data-products.md`](data-products.md).

| Item (source) | Why deferred | Revisit when |
|---|---|---|
| **Append-only audit fact** keyed by `(run_id, transaction_id)` (Codex) | The current-state fact + `run_id`/`_loaded_at` provenance meets "what is the current decision, by which run"; full decision *lineage* across re-runs is a second fact, not a rewrite. Reframed the claim so we don't over-promise. | Compliance must reconstruct what an *earlier* run decided after a correction. |
| **SCD2 merchant dimension** (both) | Reproducing a past day's categorization is met by the date-keyed fact; merchant attributes rarely change. Upgrade = one dbt snapshot + a join swap. | Point-in-time merchant identity (name/CNAE as-of) is required. |
| **Cross-processor collision policy** — quarantine vs. "latest settlement wins" (Codex) | With a single processor today there is no collision. Latest-wins is a defensible default and the seam (`stg_settlements`) is where a quarantine rule would live. | A second processor is onboarded and the same `transaction_id` can arrive twice. |
| **Per-merchant CFO mart** (`mart_cfo_by_merchant`) (Codex) | The CFO product answers day/currency-level questions; per-merchant is a distinct, additive mart. | CFO asks for merchant-level revenue breakdowns. |

### C. Latent issues — correct on current data, guarded by a note

Real sharp edges that do not fire on the current feed; we left a code comment rather than
add machinery, to keep the surface small.

| Item (source) | Why deferred | Revisit when |
|---|---|---|
| **`clean_amount` treats `1,234.56` (US thousands) as pt-BR** (CodeRabbit) | The PaySettler feed is pt-BR; the macro is deterministic for it (and now rejects non-numeric junk). | A US-format processor adapter is added (would get its own parsing in its `stg_` adapter). |
| **`publish` `OVERWRITE_OR_IGNORE` doesn't clear a partition dir** — fewer part-files on re-run could leave stale ones (CodeRabbit) | At current single-file-per-partition scale a re-publish overwrites the one file. | Partitions emit a variable number of part-files at scale (add a clean-then-write). |
| **Residual SQL string interpolation** for `reference_date`/`run_id` in models (CodeRabbit) | `reference_date` is validated (`date.fromisoformat`), `run_id` is now charset-validated at the CLI boundary, `DATA_PATH` is quote-escaped, and CLI vars go through `json.dumps`. Full parameter binding across dbt-rendered SQL remains the belt-and-suspenders step. | dbt-rendered SQL must accept fully untrusted variables. |
| **CFO CSV extract dumps all dates** while the Parquet is per-partition (CodeRabbit) | Convenience spreadsheet export; the Parquet is the contract. | Consumers treat the CSV as the authoritative grain. |

### D. Reviewed — no change (push-back)

| Item (source) | Decision |
|---|---|
| **"8-day window"** — `[reference_date − 7, reference_date]` spans 8 dates (CodeRabbit) | **Correct as-is.** The glossary specifies the interval *inclusive on both ends*, which is 8 calendar dates. Documented and tested; intentional. |

---

## Principle

The recurring theme across both reviews was *documentation claiming more than the code
delivered*. Our response was to **close that gap in both directions**: implement the items
that change real outputs (currency, scoping, DQ persistence, orchestration parity), and
**honestly reframe or explicitly defer** the rest — never leave a claim standing that the
code doesn't back. Each deferral above names the concrete trigger that would move it into
scope.
