# ORMP Client Example

Purpose: Show an application consuming decoded ORMP events from a shared
Datalens service through the Rust SDK.

Run Datalens as the public service owner:

```sh
datalens serve --config path/to/datalens.toml
```

Applications do not embed Datalens internals. They call the index GraphQL
endpoint served at `/index/graphql`:

```sh
DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:8080/index/graphql \
  cargo run -p datalens-example-ormp-client
```

The example uses `sdks/rust`, queries `decodedEventsConnection`, passes an
optional cursor with `DATALENS_AFTER_CURSOR`, and prints the returned next cursor
when another page is available.

Smoke tests use mock GraphQL responses:

```sh
cargo test -p datalens-example-ormp-client
```
