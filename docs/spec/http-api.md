# HTTP API

Purpose: Contract for the datalens HTTP API.

Read when: Calling or implementing `POST /v1/query`, heads, watch, admin, GraphQL, or `/metrics`.

Does not cover: Why the API is shaped this way (`docs/decisions/postgres-redesign.md`), or how to operate a process (`docs/runbook/operations.md`).

Authority: `crates/api/src/lib.rs`, `crates/api/src/error.rs`, `crates/core/src/wire.rs`, `schemas/native.graphql`.

## Auth

Every `/v1/*` and `/graphql` request requires both headers:

- `x-datalens-application`: application id from config
- `Authorization: Bearer <application token>`

`/health` and `/ready` are open. `/metrics` uses `Authorization: Bearer <metrics.bearer_token>`.

## Errors

Body: `{"error":{"kind":"...","message":"..."}}`.

| kind | HTTP | When |
| --- | --- | --- |
| `unauthorized` | 401 | Missing or unknown application or token |
| `forbidden` | 403 | Chain, dataset, or unfiltered logs not allowed. Also `max_subscriptions` (retrying does not free a slot) |
| `invalid_input` | 400 | Bad JSON, unknown field, range longer than `max_range_blocks` |
| `quota_exceeded` | 429 | Requests per minute or concurrency. Response includes `Retry-After: 1` |
| `unsupported` | 422 | Operation the server refuses as unsupported |
| `internal` | 500 | Store or programmer failure |

## `POST /v1/query`

Request JSON:

| Field | Meaning |
| --- | --- |
| `chain` | Chain name, for example `ethereum` |
| `dataset` | `evm.logs` or `evm.blocks` |
| `selector` | Logs: `{ "addresses": [...], "topics": [...] }`. Omit for blocks. A topic position `null` matches any value |
| `range` | `{ "start": N, "end": M }` inclusive |
| `finality` | `durable_only` (default), `safe_to_latest`, or `latest_only` |
| `fields` | Omit for base fields. `block_timestamp` and `transaction_from` only when listed |
| `page` | `{ "limit": N, "cursor": "..." }`. Default limit is `query.page_limit` (10000) |
| `wait_ms` | How long a cold range may wait for fill. Server cap is `query.max_wait_ms` |

`evm.logs` without an address is forbidden unless the application sets `allow_unfiltered_logs`.

Response JSON:

| Field | Meaning |
| --- | --- |
| `requested` | The range asked for |
| `served` | Contiguous prefix actually returned. Null when nothing was served |
| `next_start` | First block not served. Null when `requested` is fully served |
| `incomplete_reason` | Null, or `pending`, `provider_error`, `budget_exhausted`, `not_followed_yet`, `below_safe` |
| `segments` | `source` is `durable` or `hot`. Hot segments may include `anchor` `{ "number", "hash" }` |
| `rows` | Log or block objects |
| `page.next_cursor` | Present when more rows exist in `served` |

`durable_only` never returns blocks above the safe head. `latest_only` does not serve blocks at or below safe. `safe_to_latest` may include both.

## Other routes

| Method and path | Role |
| --- | --- |
| `GET /v1/heads` | Heads for every chain the application may read |
| `GET /v1/heads/{chain}` | One chain: `latest`, `latest_hash`, `safe`, `finalized` |
| `GET /v1/heads/{chain}/wait?after=&kind=&timeout_ms=` | Long-poll until `kind` (`latest`, `safe`, `finalized`) passes `after`. Timeout returns the current heads. Server cap 60s |
| `POST /v1/watch` | Subscribe a selector. Body: `chain`, `dataset`, optional `selector`. Renews an existing subscription |
| `DELETE /v1/watch` | Drop that subscription |
| `POST /graphql` | `query(input: JSON)` where `input` is the REST query body. Block numbers in the result are GraphQL `BigInt` strings |
| `POST /v1/admin/backfill` | Admin. Body: `chain`, `dataset`, optional `selector`, `range`. Returns fill job ids |
| `GET /v1/admin/coverage` | Admin. Query `chain`, `dataset`, optional selector and range |
| `GET /v1/admin/jobs?status=` | Admin. Fill jobs |
| `POST /v1/admin/follow/resume` | Admin. Body `{ "chain" }`. Clears a persisted follow halt |
| `GET /health` | `{ "status": "ok" }` |
| `GET /ready` | `{ "status": "ready" }` or 503 `{ "status": "unavailable" }` |
| `GET /metrics` | Prometheus text |

## Datasets

The datasets are `evm.logs` and `evm.blocks`.
