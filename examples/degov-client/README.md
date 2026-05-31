# Degov Client Example

Purpose: Show a governance application that owns its database schema,
checkpoint, and proposal projections while consuming decoded Datalens events.

`datalens serve` runs independently as the shared cache and query service. This
example is an external application consumer: it talks to Datalens only through
the public Rust SDK and `/index/graphql` protocol, and it does not link Datalens
server, edge, storage, or indexer runtime crates.

Start Datalens separately:

```sh
cargo run -p datalens-cli -- serve --config config/datalens.dev.toml
```

Run the Degov consumer:

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:3000/index/graphql \
DEGOV_DATABASE_URL=sqlite:.tmp/degov-client.sqlite \
  cargo run -p datalens-example-degov-client
```

The default endpoint is `http://127.0.0.1:3000/index/graphql`, matching the
local Datalens serve default. Use `DATALENS_TOKEN` when the service requires an
authorization token.

## Configuration

The application reads:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATALENS_INDEX_GRAPHQL_URL` | `http://127.0.0.1:3000/index/graphql` | Datalens index GraphQL endpoint |
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
