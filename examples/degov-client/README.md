# Degov Client Example

Purpose: Show a reduced governance application consuming indexed Datalens rows
and keeping application-owned state outside Datalens.

Run one shared Datalens service:

```sh
datalens serve --config config/datalens.dev.toml
```

The application consumes an external application index GraphQL service through
`sdks/rust`; it does not link server-side indexing crates.

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:8080/graphql \
  cargo run -p datalens-example-degov-client
```

The example reads `VoteCast` pages with `decodedEventsConnection`, stores the
cursor as the application checkpoint, and updates an application-owned proposal
projection.

Smoke tests use mock GraphQL responses:

```sh
cargo test -p datalens-example-degov-client
```
