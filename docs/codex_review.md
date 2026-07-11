# Codex Review

Overall, this is a strong, polished take-home with good architecture, documentation, tests, and developer experience. However, several production-facing guarantees in the documentation are not actually implemented. I would rate it “promising senior-level submission, but changes requested” rather than approve as-is.

## Major findings

### 1. High — Reconciliation does not scope processor records to the requested date

[`int_reconciliation.sql`](../dbt/models/intermediate/int_reconciliation.sql#L18) reads every row from `stg_settlements`, while only internal transactions are filtered by `reference_date`.

Consequences:

- `make run REFERENCE_DATE=2025-03-13` reconciles the March 16 PaySettler file against March 13 internal data.
- Backfills reuse the currently mounted CSV instead of the file belonging to that business date.
- With multiple daily files or processors, every run could include historical/future settlements.

Filter settlements by `cast(settled_at as date) = reference_date`, and ideally persist an explicit `reference_date`/file identity during ingestion. Add a test containing processor rows from at least two dates.

### 2. High — CFO output does not actually have a per-currency grain

[`mart_cfo_volume.sql`](../dbt/models/marts/finance/mart_cfo_volume.sql#L8) claims a grain of `reference_date × currency`, but groups only by `reference_date` and selects `max(currency_hint)`.

A day containing BRL and USD is reported as one arbitrary currency with both amounts combined. Processor-only rows are also silently assumed to be BRL.

Group by a row-level currency value, propagate processor currency into the reconciliation fact, and explicitly handle cross-currency matches. Add a two-currency fixture and a uniqueness test on `(reference_date, currency)`.

### 3. High — `meta.dq_check_result` is never populated

The table is created in [`run_registry.py`](../src/pipeline/run_registry.py#L53), but there is no insertion into it anywhere in the repository. Nevertheless, the README, architecture, troubleshooting guide, and solution claim every DQ result is recorded there.

This makes the primary documented troubleshooting query return no evidence.

Either:

- Parse dbt’s `run_results.json` after tests and write normalized results into the table, or
- Remove the claim and make dbt artifacts/logs the documented source of truth.

This behavior needs an integration test.

### 4. High — The Airflow observability path does not match the advertised design

The DAG invokes dbt directly in [`reconciliation_dag.py`](../airflow/dags/reconciliation_dag.py#L56), bypassing `cmd_dbt` and its stage registry. Ingest and publish independently invoke the CLI, which generates a new UUID each time in [`cli.py`](../src/pipeline/cli.py#L89).

Therefore, one DAG execution does not share one pipeline `run_id`, and `meta.pipeline_run` lacks the Airflow dbt stages.

Pass Airflow’s run ID into every CLI stage and add a CLI `--run-id` option. Have the DAG call the same registered stage commands used locally.

### 5. Medium-high — The “immutable audit spine” is mutable

[`fct_reconciliation_result.sql`](../dbt/models/marts/core/fct_reconciliation_result.sql#L7) uses `delete+insert` keyed by `(reference_date, transaction_id)`. A rerun overwrites the prior category, amounts, `run_id`, and `_loaded_at`.

That is idempotent current-state processing, but not immutable audit history. Compliance cannot reconstruct what the first run decided after a corrected rerun.

Choose and document one of these designs:

- Current result fact plus an append-only run-versioned audit fact keyed by `(run_id, transaction_id)`, or
- A versioned fact with an `is_current` projection for Ops/CFO.

### 6. Medium-high — Historical transactions can become stale

On incremental runs, [`fct_transaction.sql`](../dbt/models/marts/core/fct_transaction.sql#L25) refreshes only transactions whose `created_at` is inside the current seven-day window.

If an older transaction is updated, corrected, reversed, or arrives late, staging sees the change but `fct_transaction` never applies it. CFO history can therefore remain incorrect until a full refresh.

Drive incrementality from `updated_at` or CDC `_timestamp`, with a persisted watermark and late-arrival policy.

### 7. Medium — Joins to reconciliation runs can duplicate marts

Both [`mart_ops_health.sql`](../dbt/models/marts/ops/mart_ops_health.sql#L37) and [`mart_compliance_audit.sql`](../dbt/models/marts/compliance/mart_compliance_audit.sql#L26) join run metadata only on `reference_date`.

Multiple attempts for one date—normal in production—will duplicate the daily Ops row and every Compliance record. The current sample happens not to expose it.

Select a canonical run per date or carry the precise source `run_id` through the fact and join on it.

### 8. Medium — Ops cannot report pipeline failures as documented

`mart_ops_health.run_status` comes from the source service’s `reconciliation_runs`, not `meta.pipeline_run`. In addition, a failed dbt run cannot publish an updated mart.

So the documented question “which run failed?” is not answered for this pipeline. Provide a separate operational run-health product sourced from `meta.pipeline_run`, or have the BI layer query operational metadata directly.

## Other recommendations

- Add source freshness checks for the expected PaySettler business date, empty files, duplicate processor references, and files received twice.
- Test behavior for `REVERSED`, conflicting merchant IDs, conflicting currencies, duplicate processor rows with equal timestamps, malformed timestamps, negative amounts, and future settlements.
- Define how two processors claiming the same `transaction_id` should be handled. “Latest settlement wins” may hide a serious collision; quarantining or flagging it is safer.
- Replace `retry_delay=None` in the Airflow DAG with an explicit delay and add execution timeout, SLA/freshness alerting, and concurrency controls.
- Pin dependency versions more tightly and produce a lock file. The open-ended minimum versions reduce reproducibility.
- Remove `pip install ... || true` from the [`Dockerfile`](../Dockerfile#L12); silently ignoring installation failure hides broken builds.
- The documentation says “14 tests verdes,” but the current suite contains 23 tests. Small issue, but it weakens the otherwise careful documentation.

## What was done particularly well

- Excellent repository navigation and requirement traceability.
- Clear separation of Bronze, staging, reconciliation, facts, marts, and publication.
- Good controlled fixtures for category, tolerance, CDC, idempotency, and publication behavior.
- Sensible use of blocking versus warning dbt tests.
- The raw-string CSV landing and deterministic monetary cleanup are good decisions.
- Strong discussion of scaling limits, late arrivals, SCD2, PII, and DuckDB’s single-writer constraint.
- Publishing Parquet products instead of exposing the live DuckDB warehouse is a thoughtful local serving model.
- The required Docker environment works: I ran the suite inside Compose and got **23 passed in 106.44s**. A host-side run failed because the host has an unrelated dbt Cloud CLI shadowing dbt Core; that is an environment issue, not a repository test failure.

The highest-value fixes before submission would be processor date isolation, CFO currency correctness, real DQ metadata persistence, and consistent Airflow run correlation. Those are the items most likely to come up in the technical interview.
