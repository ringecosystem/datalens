# datalens

Query-driven EVM data cache backed by PostgreSQL.

Callers ask for `evm.logs` or `evm.blocks` over a block range. datalens answers from
PostgreSQL when the range is covered, fills missing aligned chunks from RPC once per
cluster, and serves the near-head tail from a shared per-chain follower.

## Layout

| Crate | Role |
| --- | --- |
| `crates/core` | Domain vocabulary: chains, block ranges, selectors, finality, rows, errors |
| `crates/evm` | EVM JSON-RPC client and row normalization |
| `crates/provider` | Provider gateway: budget, error classification, adaptive ranges |
| `crates/store` | PostgreSQL migrations and repositories |
| `crates/engine` | Query planner, fill worker, head tracker, follower, finalizer |
| `crates/api` | REST and GraphQL API, auth, quota |
| `crates/server` | `datalens` binary, configuration, role startup |
| `crates/client` | Internal REST client for tests |

## Local development

```sh
just pg-up
export DATABASE_URL=postgres://datalens:datalens@127.0.0.1:5432/datalens
just check
just serve
```

## Documentation

Start with [`docs/index.md`](docs/index.md). The HTTP contract is
[`docs/spec/http-api.md`](docs/spec/http-api.md). SDKs are documented in
[`sdks/README.md`](sdks/README.md). The load test is [`loadtest/README.md`](loadtest/README.md).
