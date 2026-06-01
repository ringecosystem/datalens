# Degov Client Example

Purpose: Show a governance application that owns its database schema,
checkpoint, and proposal projections while consuming decoded Datalens events.

`datalens serve` runs independently as the shared cache service. It exposes the
native Datalens REST and GraphQL surfaces, such as `/v1/query` and
`/native/graphql`; `datalens serve` does not expose `/index/graphql`.

This example is an external application indexer. It uses only `datalens-sdk` to
query `datalens serve` through `/native/graphql`, and it does not link Datalens
server, edge, storage, or indexer runtime crates.

Start the shared Datalens cache service separately:

```sh
cargo run -p datalens-cli -- serve --config config/datalens.dev.toml
```

For the local Datalens service setup, see `../../docs/runbook/local-rustfs.md`.
For the runtime ownership boundary, see `../../docs/spec/production-runtime.md`.

Run the Degov application indexer against `datalens serve`:

```sh
DATALENS_ENDPOINT=http://127.0.0.1:3000 \
DATALENS_APPLICATION=degov-client \
DEGOV_CHAIN_NAME=ethereum \
DEGOV_CHAIN_ID=1 \
DEGOV_CONTRACT_ADDRESS=0x0000000000000000000000000000000000000000 \
DEGOV_EVENT_TOPIC0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef \
DEGOV_START_BLOCK=100 \
DEGOV_END_BLOCK=200 \
DEGOV_DATABASE_URL=sqlite:.tmp/degov-client.sqlite \
  cargo run -p datalens-example-degov-client
```

Set `DATALENS_ENDPOINT` to the base service URL, not `/native/graphql`; the Rust
SDK client appends the native GraphQL path. Use `DATALENS_TOKEN` when the
service requires an authorization token.

## Configuration

The application reads:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATALENS_ENDPOINT` | `http://127.0.0.1:3000` | Datalens service base URL |
| `DATALENS_TOKEN` | unset | Optional bearer token |
| `DATALENS_APPLICATION` | `degov-client` | Application identity header |
| `DEGOV_DATABASE_URL` | `sqlite:.tmp/degov-client.sqlite` | Application-owned SQLite database |
| `DEGOV_CHAIN_NAME` / `DEGOV_CHAIN_ID` | `ethereum` / `1` | Native Datalens chain identity |
| `DEGOV_DATASET_FAMILY` / `DEGOV_DATASET_NAME` | `evm` / `logs` | Native dataset key |
| `DEGOV_CONTRACT_ADDRESS` | zero address placeholder | Governance contract address |
| `DEGOV_EVENT_TOPIC0` | configured placeholder | `VoteCast` topic selector |
| `DEGOV_EVENT_SIGNATURE` | `VoteCast(address,uint256,uint8,uint256,string)` | Business event signature stored with decoded rows |
| `DEGOV_START_BLOCK` / `DEGOV_END_BLOCK` | `0` / unset | Inclusive configured block range |
| `DEGOV_CHUNK_SIZE` | `100` | Maximum blocks queried per run |
| `DEGOV_RESET_CHECKPOINT` | unset | Set to `true` to replay from `DEGOV_START_BLOCK` |
| `DEGOV_CONSUMER_NAME` | `degov-vote-consumer` | Checkpoint identity |

By default the consumer resumes from its stored next-block checkpoint and does
not reprocess completed ranges unless `DEGOV_RESET_CHECKPOINT=true`.

## Application Schema

The SQLite migration in `migrations/0001_init.sql` creates application-owned
tables:

| Table | Responsibility |
| --- | --- |
| `consumer_checkpoints` | Stores the next block per consumer name. |
| `degov_votes` | Stores normalized vote data plus the raw decoded event JSON. `event_cursor` is unique and the vote key is derived from transaction hash plus log index when available. |
| `degov_proposals` | Stores proposal projection totals for for, against, and abstain votes. |

Vote rows, proposal totals, and checkpoint updates are written in one SQLite
transaction. Reprocessing the same page inserts no duplicate votes and does not
double-count proposal totals.

## Tests

Smoke and application tests use mock GraphQL responses, so CI does not require
live RPC or a running Datalens service:

```sh
cargo test -p datalens-example-degov-client
```
