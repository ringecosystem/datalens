# ORMP Client Example

Purpose: Show an external ORMP application consuming raw native EVM logs from a
shared Datalens service, decoding `MessageAccepted` locally, and owning its own
business database and checkpoint.

This is an external-business example. The default command documents the
application structure and cache path, but the public range is not treated as a
data-positive live E2E fixture unless the selected contract/range returns at
least one `MessageAccepted` log during that run.

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

Run the ORMP application indexer against the checked-in live fixture:

```sh
DATALENS_ENDPOINT=http://127.0.0.1:3000 \
DATALENS_APPLICATION=ormp-client \
ORMP_DATABASE_URL=sqlite:.tmp/ormp-client.sqlite \
ORMP_FIXTURES_PATH=examples/ormp-client/fixtures/live-message-accepted.toml \
  cargo run -p datalens-example-ormp-client
```

The fixture includes a verified Base `MessageAccepted` workload for contract
`0x13b2211a7ca45db2808f6db05557ce5347e3634e`, blocks
`30519000-30520999`, with one known live event at block `30519957`.

Set `DATALENS_ENDPOINT` to the base service URL, not `/native/graphql`; the
Rust SDK client appends the native GraphQL path. Use `DATALENS_TOKEN` when the
service requires a bearer token.

To validate duplicate handling after the first run, run the fixture again
against the same SQLite database with checkpoint reset enabled:

```sh
DATALENS_ENDPOINT=http://127.0.0.1:3000 \
DATALENS_APPLICATION=ormp-client \
ORMP_DATABASE_URL=sqlite:.tmp/ormp-client.sqlite \
ORMP_FIXTURES_PATH=examples/ormp-client/fixtures/live-message-accepted.toml \
ORMP_RESET_CHECKPOINT=true \
  cargo run -p datalens-example-ormp-client
```

The first fixture run should report `fetched=1 inserted=1 invalid=0`. Replaying
the same application database with reset enabled should report
`inserted=0 duplicates=1 invalid=0`.

The env-only mode still supports one explicitly configured workload:

```sh
DATALENS_ENDPOINT=http://127.0.0.1:3000 \
DATALENS_APPLICATION=ormp-client \
ORMP_CHAIN_NAME=ethereum \
ORMP_CHAIN_ID=1 \
ORMP_CONTRACT_ADDRESS=0x13b2211a7ca45db2808f6db05557ce5347e3634e \
ORMP_EVENT_TOPIC0=0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18 \
ORMP_START_BLOCK=20009590 \
ORMP_END_BLOCK=20009690 \
ORMP_DATABASE_URL=sqlite:.tmp/ormp-client.sqlite \
  cargo run -p datalens-example-ormp-client
```

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
- `ORMP_EVENT_SIGNATURE`: business event signature stored with locally decoded
  rows.
- `ORMP_START_BLOCK` and optional `ORMP_END_BLOCK`: inclusive configured block
  range.
- `ORMP_CHUNK_SIZE`: maximum blocks queried per run, defaulting to `100`.
- `ORMP_RESET_CHECKPOINT`: set to `true` to intentionally replay from
  `ORMP_START_BLOCK`.
- `ORMP_CONSUMER_NAME`: checkpoint owner, defaulting to
  `ormp-message-consumer`.
- `ORMP_FIXTURES_PATH`: optional TOML fixture path. When set, the executable
  runs every `[[workloads]]` entry until its configured range is complete.

By default the application resumes from its stored next-block checkpoint. It
does not reprocess completed ranges unless `ORMP_RESET_CHECKPOINT=true`.

Fixture workload entries use this shape:

```toml
[[workloads]]
name = "base"
chain_name = "base"
chain_id = 8453
contract_address = "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
start_block = 30519000
end_block = 30520999
chunk_size = 2000
```

`chunk_size` and `consumer_name` are optional. If `consumer_name` is omitted,
the application scopes checkpoints as `ormp-message-consumer:<fixture-name>`.

## Application database

Migrations live in `examples/ormp-client/migrations/`.

`consumer_checkpoints` stores the application-owned next block for each consumer
name. Datalens does not own this state.

`ormp_messages` stores ORMP business data derived from locally decoded
`MessageAccepted` logs:
`message_hash`, source and target chain identifiers when present, sender and
receiver when present, transaction hash, block number, event cursor, and a JSON
snapshot of the SDK event, including both `decodedArgs` and the original native
log row in `payload`. `message_hash` is the primary key, and the synthesized
event cursor is unique, so replaying the same range does not duplicate rows.

The handler writes ORMP rows and the checkpoint in one SQLite transaction. If a
business write fails, the checkpoint is not advanced. Logs that cannot be
decoded are marked with `decodeStatus=failed` and skipped by the business
handler.

The executable prints a concise one-page summary:

```text
fixture=base chain=base range=30519000-30520999 elapsed_ms=1234 fetched=1 inserted=1 duplicates=0 invalid=0 checkpoint=30521000 has_next_page=false
```

For live E2E reporting, `fetched=0 inserted=0` can still prove service/cache
correctness for a bounded native query, but it must not be counted as
business-row insertion positivity.

## Tests

Tests use mock GraphQL responses and local SQLite databases, so CI does not need
live RPC or a running Datalens service:

```sh
cargo test -p datalens-example-ormp-client
```
