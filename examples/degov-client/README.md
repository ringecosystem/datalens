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

Start the Degov application index GraphQL service separately before running this
client. This repository does not currently ship a stable Degov service command
or checked-in Degov index service config. The service must be an already-running
external application process, configured by that application, and it must expose
the index GraphQL schema at the endpoint you pass to
`DATALENS_INDEX_GRAPHQL_URL`.

For the local Datalens service setup, see `../../docs/runbook/local-rustfs.md`.
For the runtime ownership boundary, see `../../docs/spec/production-runtime.md`.

Run the Degov consumer against the external application-owned index GraphQL
endpoint:

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:3100/graphql \
DEGOV_DATABASE_URL=sqlite:.tmp/degov-client.sqlite \
  cargo run -p datalens-example-degov-client
```

Set `DATALENS_INDEX_GRAPHQL_URL` to the exact endpoint exposed by the Degov
application service. The example binary's fallback URL is a local convention
only; it is not provided by `datalens serve`. Use `DATALENS_TOKEN` when the
external application service requires an authorization token.

## Configuration

The application reads:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATALENS_INDEX_GRAPHQL_URL` | `http://127.0.0.1:3100/graphql` | External Degov application-owned index GraphQL endpoint |
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
