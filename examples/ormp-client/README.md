# ORMP Client Example

Purpose: Show an external ORMP application consuming decoded events from a
shared Datalens service through the Rust SDK while owning its own business
database and checkpoint.

## Runtime model

`datalens serve` runs independently as the shared cache service. It exposes the
native Datalens REST and GraphQL surfaces, such as `/v1/query` and
`/native/graphql`; `datalens serve` does not expose `/index/graphql`.

This example is a separate application process. It uses only `datalens-sdk` to
query `datalens serve` through `/native/graphql`; it does not link Datalens
server, edge, storage, indexer, or runtime crates.

Start the shared Datalens cache service separately:

```sh
cargo run -p datalens-cli -- serve --config config/datalens.dev.toml
```

For the local Datalens service setup, see `../../docs/runbook/local-rustfs.md`.
For the runtime ownership boundary, see `../../docs/spec/production-runtime.md`.

Run the ORMP application indexer against `datalens serve`:

```sh
DATALENS_ENDPOINT=http://127.0.0.1:3000 \
DATALENS_APPLICATION=ormp-client \
ORMP_CHAIN_NAME=ethereum \
ORMP_CHAIN_ID=1 \
ORMP_CONTRACT_ADDRESS=0x13b2211a7ca45db2808f6db05557ce5347e3634e \
ORMP_EVENT_TOPIC0=0x0aa912fb8d4a847fff42b3406d9581df47d263a5a2c1f91e586f3dc9d213808a \
ORMP_START_BLOCK=20009590 \
ORMP_END_BLOCK=20009690 \
ORMP_DATABASE_URL=sqlite:.tmp/ormp-client.sqlite \
  cargo run -p datalens-example-ormp-client
```

Set `DATALENS_ENDPOINT` to the base service URL, not `/native/graphql`; the
Rust SDK client appends the native GraphQL path. Use `DATALENS_TOKEN` when the
service requires a bearer token.

To validate duplicate handling after the first run, run the same command again
against the same SQLite database. The application resumes from its stored
next-block checkpoint and reports idempotent writes without depending on an
application index GraphQL service.

## Configuration

The executable reads:

- `DATALENS_ENDPOINT`: Datalens service base URL, defaulting to
  `http://127.0.0.1:3000`.
- `DATALENS_TOKEN`: optional bearer token.
- `DATALENS_APPLICATION`: application identity header, defaulting to
  `ormp-client`.
- `ORMP_DATABASE_URL`: SQLite database URL, defaulting to
  `sqlite:.tmp/ormp-client.sqlite`.
- `ORMP_CHAIN_NAME` and `ORMP_CHAIN_ID`: native Datalens chain identity.
- `ORMP_DATASET_FAMILY` and `ORMP_DATASET_NAME`: default to `evm` and `logs`.
- `ORMP_CONTRACT_ADDRESS`: ORMP contract address to query.
- `ORMP_EVENT_TOPIC0`: `MessageAccepted` topic selector.
- `ORMP_EVENT_SIGNATURE`: business event signature stored with decoded rows.
- `ORMP_START_BLOCK` and optional `ORMP_END_BLOCK`: inclusive configured block
  range.
- `ORMP_CHUNK_SIZE`: maximum blocks queried per run, defaulting to `100`.
- `ORMP_RESET_CHECKPOINT`: set to `true` to intentionally replay from
  `ORMP_START_BLOCK`.
- `ORMP_CONSUMER_NAME`: checkpoint owner, defaulting to
  `ormp-message-consumer`.

By default the application resumes from its stored next-block checkpoint. It
does not reprocess completed ranges unless `ORMP_RESET_CHECKPOINT=true`.

## Application database

Migrations live in `examples/ormp-client/migrations/`.

`consumer_checkpoints` stores the application-owned next block for each consumer
name. Datalens does not own this state.

`ormp_messages` stores ORMP business data derived from `MessageAccepted` events:
`message_hash`, source and target chain identifiers when present, sender and
receiver when present, transaction hash, block number, event cursor, and a JSON
snapshot of the decoded SDK event. `message_hash` is the primary key, and the
synthesized event cursor is unique, so replaying the same range does not
duplicate rows.

The handler writes ORMP rows and the checkpoint in one SQLite transaction. If a
business write fails, the checkpoint is not advanced. Events missing
`messageHash` or `msgHash` are skipped with a warning.

The executable prints a concise one-page summary:

```text
fetched=1 inserted=1 duplicates=0 invalid=0 checkpoint=20009691 has_next_page=false
```

## Tests

Tests use mock GraphQL responses and local SQLite databases, so CI does not need
live RPC or a running Datalens service:

```sh
cargo test -p datalens-example-ormp-client
```
