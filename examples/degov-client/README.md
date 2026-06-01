# Degov Client Example

Purpose: Show a governance application that consumes raw native EVM logs from
Datalens, decodes `VoteCast` locally, and owns its database schema, checkpoint,
and proposal projections.

This is an external-business example. The repository includes a bounded live
fixture file with known `VoteCast` ranges for Ethereum, Arbitrum, Base, and
Darwinia so the example can be used for data-positive live E2E checks.

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
DEGOV_CONTRACT_ADDRESS="${DEGOV_CONTRACT_ADDRESS:?set a governance contract that emits VoteCast}" \
DEGOV_EVENT_TOPIC0=0xb8e138887d0aa13bab447e82de9d5c1777041ecd21ca36ba824ff1e6c07ddda4 \
DEGOV_START_BLOCK="${DEGOV_START_BLOCK:?set the first block in a VoteCast range}" \
DEGOV_END_BLOCK="${DEGOV_END_BLOCK:?set the last block in a VoteCast range}" \
DEGOV_DATABASE_URL=sqlite:.tmp/degov-client.sqlite \
  cargo run -p datalens-example-degov-client
```

Run the bundled multi-chain live fixtures:

```sh
DATALENS_ENDPOINT=http://127.0.0.1:3000 \
DATALENS_APPLICATION=degov-live \
DATALENS_TOKEN="$DATALENS_DEGOV_TOKEN" \
DATALENS_TIMEOUT_SECONDS=120 \
DEGOV_FIXTURES_PATH=examples/degov-client/fixtures/live-votecast.toml \
DEGOV_DATABASE_URL=sqlite:.tmp/degov-client-live.sqlite \
DEGOV_RESET_CHECKPOINT=true \
  cargo run -p datalens-example-degov-client
```

Set `DATALENS_ENDPOINT` to the base service URL, not `/native/graphql`; the Rust
SDK client appends the native GraphQL path. Use `DATALENS_TOKEN` when the
service requires an authorization token.

For live E2E reporting, `fetched=0 inserted=0` can still prove service/cache
correctness for a bounded native query, but it must not be counted as
business-row insertion positivity.

To validate duplicate handling after the first run, run the same command again
against the same SQLite database. The application resumes from its stored
next-block checkpoint and reports idempotent writes without depending on an
application index GraphQL service.

## Configuration

The application reads:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATALENS_ENDPOINT` | `http://127.0.0.1:3000` | Datalens service base URL |
| `DATALENS_TOKEN` | unset | Optional bearer token |
| `DATALENS_APPLICATION` | `degov-client` | Application identity header |
| `DATALENS_TIMEOUT_SECONDS` | `60` | SDK request timeout. Use a higher value for live historical ranges. |
| `DEGOV_DATABASE_URL` | `sqlite:.tmp/degov-client.sqlite` | Application-owned SQLite database |
| `DEGOV_CHAIN_NAME` / `DEGOV_CHAIN_ID` | `ethereum` / `1` | Native Datalens chain identity |
| `DEGOV_DATASET_FAMILY` / `DEGOV_DATASET_NAME` | `evm` / `logs` | Native dataset key |
| `DEGOV_CONTRACT_ADDRESS` | required | Governance contract address |
| `DEGOV_EVENT_TOPIC0` | required `0xb8e138887d0aa13bab447e82de9d5c1777041ecd21ca36ba824ff1e6c07ddda4` | `VoteCast(address,uint256,uint8,uint256,string)` topic selector |
| `DEGOV_EVENT_SIGNATURE` | `VoteCast(address,uint256,uint8,uint256,string)` | Business event signature stored with locally decoded rows |
| `DEGOV_START_BLOCK` / `DEGOV_END_BLOCK` | required | Inclusive configured block range |
| `DEGOV_CHUNK_SIZE` | `100` | Maximum blocks queried per run |
| `DEGOV_RESET_CHECKPOINT` | unset | Set to `true` to replay from `DEGOV_START_BLOCK` |
| `DEGOV_CONSUMER_NAME` | `degov-vote-consumer` | Checkpoint identity |
| `DEGOV_FIXTURES_PATH` | unset | Optional TOML file with `[[workloads]]` entries. When set, the executable runs every workload in the file. |

By default the consumer resumes from its stored next-block checkpoint and does
not reprocess completed ranges unless `DEGOV_RESET_CHECKPOINT=true`.

Fixture workload fields:

| Field | Purpose |
| --- | --- |
| `name` | Workload name; also scopes the default checkpoint consumer name. |
| `chain_name` / `chain_id` | Native Datalens chain identity. |
| `contract_address` | Governor contract that emits `VoteCast`. |
| `start_block` / `end_block` | Inclusive block range. |
| `chunk_size` | Optional per-workload override for `DEGOV_CHUNK_SIZE`. |
| `consumer_name` | Optional checkpoint consumer override. |

## Application Schema

The SQLite migration in `migrations/0001_init.sql` creates application-owned
tables:

| Table | Responsibility |
| --- | --- |
| `consumer_checkpoints` | Stores the next block per consumer name. |
| `degov_votes` | Stores normalized vote data plus the SDK event JSON, including locally decoded args and the original native log row in `payload`. `event_cursor` is unique and the vote key is derived from transaction hash plus log index when available. |
| `degov_proposals` | Stores proposal projection totals for for, against, and abstain votes. |

Vote rows, proposal totals, and checkpoint updates are written in one SQLite
transaction. Reprocessing the same page inserts no duplicate votes and does not
double-count proposal totals. Logs that cannot be decoded are marked with
`decodeStatus=failed` and skipped by the business handler.

## Tests

Smoke and application tests use mock GraphQL responses, so CI does not require
live RPC or a running Datalens service:

```sh
cargo test -p datalens-example-degov-client
```
