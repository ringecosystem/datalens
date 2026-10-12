# Durable logs query

## Sub-features

- `evm.logs` with address + topic selector, `finality: durable_only`.
- Cache miss → provider `eth_getLogs` → durable write (logs + aligned block headers).
- Later identical query → `FullHit`, served from storage, zero provider calls.

## How to get to it (user POV)

degov-live and ormp call `POST /v1/query` with `dataset_key: "evm.logs"` and a selector of
1–3 contract addresses plus event topics, walking finalized ranges.

## Driving it with verify-datalens

```sh
scripts/query.sh "$RUN" requests/logs-usdc-transfer-durable.json logs-miss
scripts/wait-durable.sh "$RUN" evm.logs
scripts/query.sh "$RUN" requests/logs-usdc-transfer-durable.json logs-hit
```

Expect: first summary has `missing_ranges` = requested range, `eth_getLogs: 1` and ~1000
`eth_getBlockByNumber`; second has `hit_ranges` = requested range and `rpc_calls: 0`.
Rows: USDC Transfer logs (~700 per 10 blocks), every row's `address` is the USDC
contract and `topics[0]` the Transfer topic.

## Gotchas

- First miss takes ~2 minutes on publicnode because of the 1000-block header fill.
- Querying again before `wait-durable.sh` succeeds misses and refetches `eth_getLogs`;
  the durable intent worker also refetches once on its own.
- Rows are under `rows.rows.rows` in the response.
