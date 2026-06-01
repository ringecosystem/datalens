# Degov Client Example

Purpose: Show a governance application that owns its database schema,
checkpoint, and proposal projections while consuming decoded Datalens events.

`datalens serve` runs independently as the shared cache service. It exposes the
native Datalens REST and GraphQL surfaces, such as `/v1/query` and
`/native/graphql`; `datalens serve` does not expose `/index/graphql`.

This example is an external application consumer. It uses only `datalens-sdk`
and an application-owned index GraphQL endpoint, and it does not link Datalens
server, edge, storage, or indexer runtime crates.

Start the shared Datalens cache service separately:

```sh
cargo run -p datalens-cli -- serve --config config/datalens.dev.toml
```

Start a Degov application index GraphQL service separately before running this
client. For local e2e, this example ships an app-owned fixture service that
exposes the decoded-event GraphQL contract consumed by the client:

```sh
DEGOV_FIXTURE_ADDR=127.0.0.1:3101 \
  cargo run -p datalens-example-degov-client --bin degov-app-index-fixture
```

The fixture is not `datalens serve` and does not perform production indexing. It
serves deterministic `VoteCast` pages, cursors, and pagination for client
checkpoint and idempotency testing.

For the local Datalens service setup, see `../../docs/runbook/local-rustfs.md`.
For the runtime ownership boundary, see `../../docs/spec/production-runtime.md`.

Run the Degov consumer against the external application-owned index GraphQL
endpoint:

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:3101/graphql \
DEGOV_DATABASE_URL=sqlite:.tmp/degov-client.sqlite \
  cargo run -p datalens-example-degov-client
```

Set `DATALENS_INDEX_GRAPHQL_URL` to the exact endpoint exposed by the Degov
application service. The example binary's fallback URL is a local convention
only; it is not provided by `datalens serve`. Use `DATALENS_TOKEN` when the
external application service requires an authorization token.

To validate duplicate handling after the first run, replay from the fixture's
start cursor against the same SQLite database:

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:3101/graphql \
DEGOV_DATABASE_URL=sqlite:.tmp/degov-client.sqlite \
DEGOV_START_CURSOR=degov-cursor-0 \
  cargo run -p datalens-example-degov-client
```

## Configuration

The application reads:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATALENS_INDEX_GRAPHQL_URL` | `http://127.0.0.1:3100/graphql` | Degov application-owned index GraphQL endpoint |
| `DATALENS_TOKEN` | unset | Optional bearer token |
| `DEGOV_DATABASE_URL` | `sqlite:.tmp/degov-client.sqlite` | Application-owned SQLite database |
| `DEGOV_PAGE_SIZE` | `25` | VoteCast page size |
| `DEGOV_START_CURSOR` | unset | Explicit cursor override for one run |
| `DEGOV_CONSUMER_NAME` | `degov-vote-consumer` | Checkpoint identity |

By default the consumer resumes from its stored checkpoint. Set
`DEGOV_START_CURSOR` only when intentionally overriding that resume cursor.

## Application Schema

The SQLite migration in `migrations/0001_init.sql` creates application-owned
tables:

| Table | Responsibility |
| --- | --- |
| `consumer_checkpoints` | Stores the last durable Datalens cursor per consumer name. |
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
