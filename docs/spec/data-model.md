# Data model

Purpose: Invariants the PostgreSQL schema and engine keep.

Read when: Changing migrations, coverage, follow, or reorg behavior.

Does not cover: HTTP field names (`docs/spec/http-api.md`).

Authority: `crates/store/migrations/0001_init.sql`, `0002_unsafe_rows.sql`, `0003_follow_finality_and_halt.sql`, `crates/engine/src/query.rs`, `crates/engine/src/follow.rs`, `crates/engine/src/reorg.rs`.

## Tables

- `chains` maps `chain_id` to a name. `evm_logs` and `evm_blocks` are partitioned by `chain_id`.
- `selectors` stores the canonical key of an `evm.logs` filter or the single `evm.blocks` selector.
- `coverage` is an `int8range` per selector at finality `safe` (2) or `finalized` (3). Ranges for one selector do not overlap (`EXCLUDE` gist).
- `evm_logs` primary key is `(chain_id, block_number, log_index)`. Address reads use `evm_logs_address_idx` on `(chain_id, address, block_number, log_index)`.
- `evm_blocks` primary key is `(chain_id, number)`.
- `fill_jobs` dedupe one chunk: the same selector and aligned block span share one job.
- `follow_progress` tracks `head_block` / `head_hash` and `promoted_to` per selector.
- `subscriptions` are the watch set. Expiry stops follow. Durable coverage remains.
- `chains.follow_halted_at` stays set across restarts until `POST /v1/admin/follow/resume`.

## Finality

- Rows at or below the safe head can be promoted. Promotion updates `finality` and merges coverage. It does not call RPC again.
- Rows above safe stay `unsafe` and are the hot tail. A hot query segment's anchor is the follow head hash.
- Unsafe rows at or below the retention horizon are deleted. Durable rows are not.

## Reorg

- Each follow tick checks the unpromoted head hash with one `eth_getBlockByNumber`.
- A fork deletes unsafe rows at and above the fork block and rewinds `follow_progress`. `promoted_to` does not move backward.
- A fork at or below safe is a finality violation. Follow for that chain halts and the halt is persisted.

## Fill

- A cold query enqueues aligned chunks of `chunk_blocks`. Workers fetch each chunk once and insert rows plus coverage.
- A second query of a fully covered range does not call `eth_getLogs`.
