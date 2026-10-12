# Configuration

Purpose: Every key in the datalens TOML file.

Read when: Writing or loading a config file.

Does not cover: The sequence to start a process (`docs/runbook/local-development.md`).

Authority: `crates/server/src/config.rs`. Unknown keys are rejected. `${NAME}` placeholders are read from the environment.

## `[server]`

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `bind` | yes | | Socket the API listens on |
| `roles` | yes | | `api`, `worker`, or both |

## `[database]`

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `url` | yes | | PostgreSQL URL. Production uses `${DATALENS_DATABASE_URL}` |
| `max_connections` | no | 32 | Pool size |

## `[query]`

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `max_wait_ms` | no | 15000 | Cap on request `wait_ms` |
| `page_limit` | no | 10000 | Default and maximum page size |
| `follow_attach_blocks` | no | 1000 | How far a new subscription attaches behind the head |

## `[follow]`

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `subscription_ttl_s` | no | 900 | Watch lifetime. `POST /v1/watch` renews it |
| `unsafe_retention_s` | no | 86400 | How long unsafe rows are kept |

## `[fill]`

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `workers` | no | 4 | Fill workers when the `worker` role is on |
| `inline_workers` | no | 2 | Fill workers on an api-only process |
| `lease_s` | no | 120 | Job lease |
| `job_retention_s` | no | 86400 | Age before done and failed jobs are deleted |

## `[metrics]`

| Key | Required | Meaning |
| --- | --- | --- |
| `bearer_token` | yes | Bearer token for `GET /metrics` |

## `[chains.<name>]`

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `chain_id` | yes | | EVM chain id |
| `rpc_url` | yes | | JSON-RPC URL |
| `finality` | yes | | `{ mode = "lag", safe_lag_blocks, finalized_lag_blocks }` or `{ mode = "rpc_tags" }` |
| `tick_ms` | no | 2000 | Leader tick. Head, follow, reorg check, retention |
| `reorg_window_blocks` | no | 512 | How far a fork search looks before widening |
| `chunk_blocks` | yes | | Aligned `evm.logs` fill width |
| `header_chunk_blocks` | no | 100 | `evm.blocks` fill width |
| `max_get_logs_range_blocks` | yes | | Largest `eth_getLogs` span |
| `max_addresses_per_query` | yes | | Addresses per `eth_getLogs` |
| `provider.rps` | yes | | Token bucket rate |
| `provider.burst` | yes | | Token bucket burst |
| `provider.daily_calls` | no | | Optional UTC-day cap |
| `provider.max_in_flight` | yes | | Concurrent calls for the chain |
| `provider.min_range_blocks` | no | 16 | Floor of the adaptive range |

## `[[applications]]`

| Key | Required | Default | Meaning |
| --- | --- | --- | --- |
| `id` | yes | | `x-datalens-application` value |
| `token` | yes | | Bearer token |
| `chains` | yes | | Allowed chain names |
| `datasets` | yes | | Allowed datasets |
| `quota.max_range_blocks` | yes | | Largest query range. Longer is HTTP 400 |
| `quota.max_requests_per_minute` | yes | | HTTP 429 when exceeded |
| `quota.max_concurrent_requests` | yes | | HTTP 429 when exceeded |
| `quota.max_subscriptions` | yes | | HTTP 403 when a new selector would exceed it |
| `quota.max_fill_blocks_per_minute` | no | | Optional fill budget |
| `quota.allow_unfiltered_logs` | no | false | Allow `evm.logs` with no address |
| `admin` | no | false | Allows `/v1/admin/*` |

Example file: `config/datalens.example.toml`.
