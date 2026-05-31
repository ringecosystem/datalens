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

Start the ORMP application index GraphQL service separately before running this
client. This repository does not currently ship a stable ORMP service command or
checked-in ORMP index service config. The service must be an already-running
external application process, configured by that application, and it must expose
the index GraphQL schema at the endpoint you pass to
`DATALENS_INDEX_GRAPHQL_URL`.

For the local Datalens service setup, see `../../docs/runbook/local-rustfs.md`.
For the runtime ownership boundary, see `../../docs/spec/production-runtime.md`.

Run the ORMP application consumer against the external application-owned index
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
