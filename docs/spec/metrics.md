# Metrics

Purpose: Prometheus names and labels exposed on `GET /metrics`.

Read when: Writing a dashboard query or a cost check.

Does not cover: How to open the Grafana JSON (`docs/runbook/operations.md`).

Authority: `crates/provider/src/metrics.rs`, `crates/engine/src/metrics.rs`, `deploy/grafana/datalens.json`.

| Name | Labels | Meaning |
| --- | --- | --- |
| `datalens_provider_calls_total` | `chain`, `method`, `purpose`, `application`, `outcome` | JSON-RPC calls that reached the provider. `purpose` is `head`, `reorg`, `follow`, `query_fill`, or `backfill` |
| `datalens_provider_call_duration_seconds` | `chain`, `method`, `purpose` | HTTP duration of those calls |
| `datalens_provider_budget_tokens` | `chain` | Remaining token-bucket balance |
| `datalens_provider_budget_rejections_total` | `chain`, `purpose` | Calls that never left because the budget refused a token |
| `datalens_queries_total` | `application`, `chain`, `dataset`, `outcome` | `outcome` is `complete`, `partial`, or an error kind |
| `datalens_query_duration_seconds` | `chain`, `dataset`, `finality`, `outcome` | Handler time, including fills waited on |
| `datalens_fill_jobs` | `status` | Current jobs by status |
| `datalens_fill_job_duration_seconds` | `chain`, `dataset`, `outcome` | Claim to commit |
| `datalens_follow_lag_blocks` | `chain` | Latest head minus the lowest followed head |
| `datalens_reorgs_total` | `chain` | Unsafe rollbacks |
| `datalens_finality_violations_total` | `chain` | Forks at or below safe |
| `datalens_subscriptions` | `chain`, `dataset` | Unexpired watches |
| `datalens_follow_halted` | `chain` | 1 when follow is halted |

A covered query does not increment `datalens_provider_calls_total` for `eth_getLogs`. Fill and follow usage split `LogsPage.calls` across the applications that share the batch. That count includes retries that reached the provider.

Dashboard file: `deploy/grafana/datalens.json`.
