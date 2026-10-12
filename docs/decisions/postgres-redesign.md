# PostgreSQL redesign

Purpose: Why datalens stores and serves chain data this way.

Read when: A change would add a compatibility layer, another dataset, or a second store.

Does not cover: The current contract (`docs/spec/http-api.md`, `docs/spec/data-model.md`).

## Decision

PostgreSQL is the only store. Coverage ranges and aligned fill jobs replace object-storage files. The cluster shares one follower per chain for the hot tail. Callers see `served`, `next_start`, and `incomplete_reason` instead of a cache object.

## Consequences

- No compatibility with the previous config, HTTP body, SDK, or storage layout. Old code is deleted, not adapted.
- EVM only. `evm.logs` and `evm.blocks` are the only datasets. Solana, Tron, `evm.transactions`, and `evm.receipts` are not implemented.
- No historical import. A new database fills from RPC when queried or watched.
- `transaction_from` is off unless the request lists that field, because it costs an `eth_getTransactionByHash` per transaction.
- `degov-realtime` stays a separate application id and token from `degov-live` and `degov-staging`.
- Fill jobs dedupe identical chunks so two applications do not double-fetch one span.
- Follow promotion does not refetch RPC. Reorgs never delete durable rows. A fork through safe halts follow until an admin resumes it.
- The previous object store is not read and is not deleted by this repository change.

## Rejected

- Keeping a read path for the old API so existing binaries would keep working. Two contracts in one process hid which code was live.
- Importing old files into tables before cutover. The clients reindex the windows they still need.
