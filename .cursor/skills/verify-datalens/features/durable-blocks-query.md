# Durable blocks query

## Sub-features

- `evm.blocks` with `selector: {"kind": "all"}`, `finality: durable_only`.
- Block rows: `number`, `hash`, `parent_hash`, `timestamp`, ascending by number.

## How to get to it (user POV)

ormp reads `evm.blocks` as reorg anchors; degov reads them for timestamps.

## Driving it with verify-datalens

```sh
scripts/query.sh "$RUN" requests/blocks-durable.json blocks-miss
scripts/wait-durable.sh "$RUN" evm.blocks
scripts/query.sh "$RUN" requests/blocks-durable.json blocks-hit
```

Expect 10 rows, numbers `PIN_START..PIN_START+9`, contiguous, each `parent_hash` equal to
the previous row's `hash`. Miss costs ~10 `eth_getBlockByNumber` plus one
`eth_blockNumber`; a settled repeat costs 0.

## Gotchas

- A repeat 6 s after the miss still missed (19 calls): the intent worker had not yet
  written coverage (it refetches the range itself, ~17 s). Always `wait-durable.sh` first.
- Header rows filled by a prior logs query live in `evm.block_headers`, a different
  dataset; they do not make an `evm.blocks` query a hit.
