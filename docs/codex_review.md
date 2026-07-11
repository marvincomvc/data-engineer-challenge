# Codex Review — Post-fix Reassessment

The second review is much improved. The primary correctness issues—date scoping, currency grain, orchestration correlation, and run-header fan-out—are fixed, and the deferrals are generally well reasoned for a take-home.

I would still request a small follow-up because the new DQ implementation has an important failure-path bug.

## Findings

### 1. High — Failed dbt tests are not written to `meta.dq_check_result`

In [`cli.py`](../src/pipeline/cli.py#L93), the code raises immediately when dbt returns non-zero:

```python
if result.returncode != 0:
    raise RuntimeError(...)
```

`load_dq_results()` is called afterward, so it only runs when dbt succeeds. A blocking `severity: error` test causes `dbt test` or `dbt build` to return non-zero; precisely those failed checks are therefore never persisted.

This contradicts the troubleshooting instructions in [`troubleshooting.md`](troubleshooting.md#L50), which tell the operator to query `meta.dq_check_result` to identify the gate that blocked publication.

Load `run_results.json` before raising, preferably in a `try/finally`, while preserving the non-zero pipeline result. Add an integration test that deliberately violates a blocking dbt test and confirms:

- The pipeline exits non-zero.
- The dbt stage is recorded as `FAILED`.
- The failed check appears in `meta.dq_check_result`.

### 2. Medium — DQ metrics do not contain the advertised observed value or threshold

[`run_registry.py`](../src/pipeline/run_registry.py#L113) records dbt’s `failures` field as `observed_value` and always writes `threshold = NULL`.

For a mismatch-rate test, `failures` is normally the number of rows returned by the test—often `1`—rather than the actual mismatch rate. Consequently, the table cannot answer “by how much did the rate exceed 10%?” despite its schema and documentation suggesting that it can.

Options:

- Rename the field to `failure_count` and stop presenting it as the measured business metric, or
- Make anomaly tests emit/store their actual observed metric and configured threshold.

The first option is perfectly adequate for the challenge.

### 3. Low — Some “immutable audit” claims remain

The decision log says the audit was reframed as current-state, and [`data-products.md`](data-products.md#L18) now explains this well. However, stale claims remain:

- [`_marts.yml`](../dbt/models/marts/_marts.yml#L5): “Immutable audit spine”
- [`mart_compliance_audit.sql`](../dbt/models/marts/compliance/mart_compliance_audit.sql#L1): “immutable audit trail”
- [`dim_merchant.sql`](../dbt/models/marts/core/dim_merchant.sql#L6): says the audit requirement is met by an immutable fact and incorrectly describes its key as including `run_id`

These should say “current-state reconciliation spine/audit view.” This is especially worth correcting because [`review-decisions.md`](review-decisions.md#L72) explicitly promises that unsupported claims were removed.

### 4. Low — The date-scoping fix lacks its proposed regression test

The implementation in [`int_reconciliation.sql`](../dbt/models/intermediate/int_reconciliation.sql#L38) is correct, but the controlled CSV fixture still appears to contain only the target business date.

Add one processor record from the previous or following day and assert it is absent from the selected reference date. Date isolation was the most consequential original output bug, so it deserves a direct regression test.

## Assessment of the deferrals

The deferrals in [`review-decisions.md`](review-decisions.md) are mostly defensible:

- Append-only audit history: reasonable if the current-state limitation remains explicit.
- SCD2 merchant history: reasonable and clearly scoped.
- Late-arrival handling: reasonable for a local exercise, though it remains a real CFO correctness limitation.
- Source freshness and production DAG controls: reasonable production concerns.
- Cross-processor collision handling: reasonable until a second processor is introduced.
- Whole-partition replacement and publish cleanup: acceptable given the fixture assumptions, with appropriate revisit triggers.

One wording adjustment: calling dependency locking merely “production hardening” understates its value for a take-home. A lock file improves evaluator reproducibility too, although I would not block the submission over it.

## Verification

The updated Dockerized test suite passes:

```text
25 passed in 124.34s
```

The fixed output logic looks sound, and the decision document materially improves the submission by showing deliberate engineering judgment rather than silently ignoring review feedback.

Final verdict: nearly ready. Fix the failed-DQ persistence path and remove the remaining immutable-audit claims; the other items can remain recommendations.
