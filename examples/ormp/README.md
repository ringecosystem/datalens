# ORMP Declarative Index Example

This example shows ORMP/Msgport EVM log indexing with the declarative index
runner. Application authors provide `ormp.index.toml`; no custom Rust client code
is needed.

The config uses `DATALENS_ORMP_TOKEN` for the application token. Set it to a
local development token before running the index commands:

```sh
export DATALENS_ORMP_TOKEN=replace-with-local-token
```

The local workflow uses two processes:

1. `datalens serve` runs the shared cache server.
2. `datalens index daemon` runs the ORMP application indexer, writes indexed
   events to SQLite, and serves GraphQL for those indexed events.

For local development, start the cache server with the server configuration and
storage setup documented by the repo runbooks:

```sh
cargo run -p datalens-cli -- serve --config path/to/datalens.toml
```

In another shell, inspect and plan the ORMP index:

```sh
cargo run -p datalens-cli -- index doctor --config examples/ormp/ormp.index.toml
cargo run -p datalens-cli -- index plan --config examples/ormp/ormp.index.toml
```

With the cache server still running, start the application-side daemon:

```sh
cargo run -p datalens-cli -- index daemon --config examples/ormp/ormp.index.toml
```

The default config writes to `.data/indexes/ormp/index.db`, stores checkpoints
under `.data/indexes/ormp/checkpoint.json`, and serves GraphQL on
`http://127.0.0.1:9090/graphql`. If the playground is enabled, open
`http://127.0.0.1:9090/graphql/playground`.

After the daemon has indexed rows, query raw indexed events:

```graphql
query OrmpEvents {
  events(dataset: "evm.logs", indexName: "ormp", limit: 10) {
    chain
    chainId
    blockNumber
    transactionHash
    address
    topics
    data
  }
}
```

The example indexes ORMP and Msgport contract logs on Ethereum mainnet and
Polygon PoS using known smoke-test block ranges. It does not decode ORMP ABIs;
rows are served as raw EVM logs. JSONL output remains useful for debugging, but
database output is the primary application deployment path for this example.
