# ORMP Client Example

Purpose: Show an external ORMP application consuming decoded events from a
shared Datalens service through the Rust SDK while owning its own business
database and checkpoint.

## Runtime model

`datalens serve` runs independently as the shared cache service. It exposes the
native Datalens REST and GraphQL surfaces, such as `/v1/query` and
`/native/graphql`; `datalens serve` does not expose `/index/graphql`.

This example is a separate application process. It uses only `datalens-sdk` and
an application-owned index GraphQL endpoint; it does not link Datalens server,
edge, storage, indexer, or runtime crates.

Start the shared Datalens cache service separately:

```sh
cargo run -p datalens-cli -- serve --config config/datalens.dev.toml
```

Start an ORMP application index GraphQL service separately before running this
client. For local e2e, this example ships an app-owned fixture service that
exposes the decoded-event GraphQL contract consumed by the client:

```sh
ORMP_FIXTURE_ADDR=127.0.0.1:3100 \
  cargo run -p datalens-example-ormp-client --bin ormp-app-index-fixture
```

The fixture is not `datalens serve` and does not perform production indexing. It
serves deterministic `MessageAccepted` pages, cursors, and pagination for client
checkpoint and idempotency testing.

For the local Datalens service setup, see `../../docs/runbook/local-rustfs.md`.
For the runtime ownership boundary, see `../../docs/spec/production-runtime.md`.

Run the ORMP application consumer against the application-owned index
GraphQL endpoint:

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:3100/graphql \
ORMP_DATABASE_URL=sqlite:.tmp/ormp-client.sqlite \
  cargo run -p datalens-example-ormp-client
```

Set `DATALENS_INDEX_GRAPHQL_URL` to the exact endpoint exposed by the ORMP
application service. The example binary's fallback URL is a local convention
only; it is not provided by `datalens serve`. Use `DATALENS_TOKEN` when the
external application service requires an application token.

To validate duplicate handling after the first run, replay from the fixture's
start cursor against the same SQLite database:

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:3100/graphql \
ORMP_DATABASE_URL=sqlite:.tmp/ormp-client.sqlite \
ORMP_START_CURSOR=ormp-cursor-0 \
  cargo run -p datalens-example-ormp-client
```

## Configuration

The executable reads:

- `DATALENS_INDEX_GRAPHQL_URL`: external ORMP application-owned index GraphQL
  endpoint.
- `DATALENS_TOKEN`: optional bearer token.
- `ORMP_DATABASE_URL`: SQLite database URL, defaulting to
  `sqlite:.tmp/ormp-client.sqlite`.
- `ORMP_PAGE_SIZE`: page size for `decodedEventsConnection`, defaulting to `25`.
- `ORMP_START_CURSOR`: optional explicit cursor override for one run.
- `ORMP_CONSUMER_NAME`: checkpoint owner, defaulting to
  `ormp-message-consumer`.

By default the application resumes from its stored checkpoint. `ORMP_START_CURSOR`
is an explicit override for testing or controlled replay.

## Application database

Migrations live in `examples/ormp-client/migrations/`.

`consumer_checkpoints` stores the application-owned cursor for each consumer
name. Datalens does not own this state.

`ormp_messages` stores ORMP business data derived from `MessageAccepted` events:
`message_hash`, source and target chain identifiers when present, sender and
receiver when present, transaction hash, block number, event cursor, and a JSON
snapshot of the decoded SDK event. `message_hash` is the primary key, and
`event_cursor` is unique, so rerunning the same page does not duplicate rows.

The handler writes ORMP rows and the checkpoint in one SQLite transaction. If a
business write fails, the checkpoint is not advanced. Events missing
`messageHash` or `msgHash` are skipped with a warning.

The executable prints a concise one-page summary:

```text
fetched=1 inserted=1 duplicates=0 invalid=0 checkpoint=cursor-2 has_next_page=false
```

## Tests

Tests use mock GraphQL responses and local SQLite databases, so CI does not need
live RPC or a running Datalens service:

```sh
cargo test -p datalens-example-ormp-client
```
