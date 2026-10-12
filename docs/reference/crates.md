# Crates

Purpose: Which crate owns which behavior.

Read when: Choosing where a change belongs.

Does not cover: The HTTP contract (`docs/spec/http-api.md`).

| Path | Owns |
| --- | --- |
| `crates/core` | Chains, ranges, selectors, finality, wire JSON, error kinds |
| `crates/evm` | JSON-RPC encode/decode, `get_logs`, block headers, mock RPC |
| `crates/provider` | Token bucket, error classes, adaptive range, provider metrics |
| `crates/store` | Migrations and SQL repositories |
| `crates/engine` | Query, fill, follow, reorg, retention, usage, engine metrics |
| `crates/api` | REST, GraphQL, auth, quota, `/metrics` |
| `crates/server` | Config load, `datalens serve` / `datalens migrate`, `/health`, `/ready` |
| `crates/client` | In-repo REST client used by server tests |
| `sdks/rust` | `datalens-sdk` for applications: query, follow, cursors |
| `sdks/typescript`, `sdks/go` | The same HTTP client without the cursors' Rust types |
| `schemas/native.graphql` | Committed GraphQL schema. `just schema` regenerates it |
| `deploy/grafana/datalens.json` | Dashboard |
| `loadtest/` | Release load test runner |

A request enters `crates/api`, which calls `QueryEngine::query`. Covered spans read `crates/store`. Missing spans become `fill_jobs`. The leader in `crates/engine` polls heads and follow on each chain tick.
