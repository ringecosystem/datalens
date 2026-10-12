# Provider cost accounting

## Sub-features

- Per-query RPC call count and method breakdown.
- `eth_getLogs` span distribution (detects collapse to 1-block ranges).
- JSON-RPC error count (provider limit / archive errors).

## How to get to it (user POV)

Operators see this as provider bills and rate-limit errors (the Arbitrum request spike).
It is the main success metric of the PostgreSQL redesign.

## Driving it with verify-datalens

```sh
scripts/rpc-calls.sh "$RUN"                                                   # whole run
scripts/rpc-calls.sh "$RUN" ../../../.data/verify-evidence/$RUN/queries/01-logs-miss.rpc-calls.jsonl
```

Compare `rpc_calls` in `queries/*.summary.json` before and after a change on the same
request file.

## Gotchas

- The doctor's `eth_blockNumber` probe and anything launched after a query (background
  promotion, compaction) are attributed to whichever query slice they fall into; let the
  instance settle a few seconds before a measured query.
- `launch.sh` fetches the head directly from upstream, so it is not counted.
