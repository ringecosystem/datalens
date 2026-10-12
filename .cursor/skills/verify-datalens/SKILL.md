---
name: verify-datalens
description: Launch an isolated datalens server against live Ethereum RPC through a counting proxy, drive the PostgreSQL HTTP API as an application, and capture responses plus every provider call as evidence. Use when verifying a datalens change end to end, proving a covered-range cache hit, or measuring how many RPC calls a query costs.
---

# verify-datalens

The user surface is `datalens serve`: `POST /v1/query`, `POST /graphql`,
`GET /v1/heads` and `/v1/heads/{chain}/wait`, `POST /v1/watch`, admin routes, `/health`,
`/ready`, and `/metrics`. Callers send `x-datalens-application` and
`Authorization: Bearer`. A counting JSON-RPC proxy sits between datalens and the upstream.

Paths below are relative to this skill directory. The contract is `docs/spec/http-api.md`.

## Launch

```sh
RUN=$(scripts/launch.sh)          # release binary; --no-build reuses $CARGO_TARGET_DIR/release/datalens
# optional: --upstream https://other-ethereum-rpc
```

Each run creates a database `verify_*` on `127.0.0.1:55901`, a free API port, and a free
proxy port. `PIN_START`..`PIN_END` is eight blocks ending 70 below the upstream head, which
is under the configured safe lag of 32. Tokens stay in `.data/verify/$RUN/state.env` and are
omitted from `run.env`.

## Doctor

```sh
scripts/doctor.sh "$RUN"
```

Confirms the pids, `/health`, `/ready`, unauthenticated heads is 401, authenticated heads
return `latest`, and the proxy reaches the upstream.

## Drive

```sh
scripts/query.sh "$RUN" requests/logs-usdc-transfer-durable.json logs-fill
scripts/e2e.sh
```

`query.sh` replaces `"@START@"` and `"@END@"` and prints HTTP status, `served`,
`next_start`, `incomplete_reason`, row count, and RPC calls by method.

`scripts/e2e.sh` runs the feature table: doctor, bad token, durable logs, a second query
with zero new `eth_getLogs`, `transaction_from`, pagination, durable blocks, watch,
heads wait, GraphQL, over-quota 400, and metrics. It cleans up on exit.

## Evidence

`.data/verify-evidence/$RUN/` (git-ignored):

- `queries/NN-<label>.{request,response,rpc-calls,summary}.json`
- `rpc-calls.jsonl` — every JSON-RPC call
- `features.tsv` — the e2e table, written by `scripts/e2e.sh`
- `server.log`, `proxy.log`, `run.env` (no tokens)

## Cleanup

```sh
scripts/cleanup.sh "$RUN"
```

Kills the server and proxy, drops the run database, deletes `.data/verify/$RUN/`, keeps evidence.

## Gotchas

- publicnode history is short. Stay inside `PIN_START`..`PIN_END`.
- `transaction_from` is requested only on `requests/logs-with-sender.json`, and only for one block, because each transaction is an extra `eth_getTransactionByHash`.
- A range longer than `max_range_blocks` (64 in the verify config) is HTTP 400 `invalid_input`, not 429.
- `max_subscriptions` is HTTP 403 `forbidden`.
- Durable coverage is written by the fill worker before the query returns when `wait_ms` is large enough. The default query waits up to 20 seconds. The e2e cache check runs after that fill has completed.
