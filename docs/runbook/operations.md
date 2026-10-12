# Operations

Purpose: What to do when follow, budget, or finality looks wrong.

Read when: Responding to a dashboard panel or a halted chain.

Does not cover: Metric label lists (`docs/spec/metrics.md`).

## Dashboard

Import `deploy/grafana/datalens.json`. Select a Prometheus datasource. Provider requests per second are `sum by (chain, purpose, outcome) (rate(datalens_provider_calls_total[1m]))`. Arbitrum steady state at a 2 second tick is expected at or below 5 requests per second.

## Follow halted

`datalens_follow_halted{chain="..."}` is 1 after a fork at or below safe, or a fork deeper than the reorg window. The chain stays halted across restarts.

Resume with an admin application:

```sh
curl -sS -X POST "$BASE/v1/admin/follow/resume" \
  -H "x-datalens-application: $APP" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"chain":"arbitrum"}'
```

## Finality violation

`datalens_finality_violations_total` increments when a fork reaches a durable height. Do not delete durable rows by hand. Resume follow only after the canonical chain is understood.

## Budget exhausted

Queries return `incomplete_reason = budget_exhausted` or HTTP 429 `quota_exceeded` with `Retry-After`. `datalens_provider_budget_tokens` near 0 means the chain token bucket is empty. Raise `provider.rps` or lower client `wait_ms`. Fill backs off. Head, reorg, and follow wait for a token.

## Backfill

An admin `POST /v1/admin/backfill` enqueues aligned chunks. Watch `GET /v1/admin/jobs?status=done` and `datalens_fill_jobs`. A covered range does not call `eth_getLogs` again.
