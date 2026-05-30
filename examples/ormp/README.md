# ORMP Declarative Index Example

This example shows ORMP/Msgport EVM log indexing with the declarative index
runner. Application authors provide `ormp.index.toml`; Datalens runs the
application indexer, writes database output, labels configured ABI events, and
serves queryable rows through GraphQL. No custom Rust client code is needed.

The config uses `DATALENS_ORMP_TOKEN` for the application token. Server RPC
URLs, database URLs, and auth tokens should stay in local environment-specific
configuration. Set the application token to a local development value before
running the index commands:

```sh
export DATALENS_ORMP_TOKEN=replace-with-local-token
```

The service workflow uses one long-running Datalens process:

1. `datalens serve --config datalens.toml` runs the shared cache server, ORMP
   application index worker, and separate native/index GraphQL surfaces when enabled.

For local development, start the cache server with the server configuration and
storage setup documented by the repo runbooks:

```sh
cargo run -p datalens-cli -- serve --config path/to/datalens.toml
```

Inspect and plan the standalone ORMP index helper config:

```sh
cargo run -p datalens-cli -- index doctor --config examples/ormp/ormp.index.toml
cargo run -p datalens-cli -- index plan --config examples/ormp/ormp.index.toml
```

For one-shot local helper runs, execute the ORMP index config directly:

```sh
cargo run -p datalens-cli -- index run --config examples/ormp/ormp.index.toml
```

The default config writes to `.data/indexes/ormp/index.db`, stores checkpoints
under `.data/indexes/ormp/checkpoint.json`. When embedded in service config, the index
GraphQL surface is served by `datalens serve` at `/index/graphql`; if the playground is
enabled, open `/index/graphiql`.

The example keeps only the ABI event fragments it needs in `abi/events.json`.
The same minimal event definitions are declared in `ormp.index.toml` under
`[decode]` so the indexer can attach stable event names and signatures to
matching EVM logs.

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

Query decoded ORMP-related events by chain, contract address, event signature,
and block range:

```graphql
query DecodedOrmpEvents {
  events(
    dataset: "evm.logs"
    indexName: "ormp"
    chain: "ethereum"
    chainId: 1
    address: "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
    eventName: "MessageAccepted"
    signature: "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))"
    fromBlock: 20009590
    toBlock: 20059589
    limit: 10
  ) {
    chain
    chainId
    blockNumber
    transactionHash
    address
    topic0
    signature
    eventName
    decoded
    payload
  }
}
```

Query Msgport send-side events with the same GraphQL workflow:

```graphql
query MsgportSentEvents {
  events(
    dataset: "evm.logs"
    indexName: "ormp"
    chain: "polygon"
    chainId: 137
    address: "0x2cd1867fb8016f93710b6386f7f9f1d540a60812"
    eventName: "MessageSent"
    signature: "MessageSent(bytes32,address,uint256,address,bytes,bytes)"
    fromBlock: 64142806
    toBlock: 64142806
    limit: 10
  ) {
    blockNumber
    transactionHash
    signature
    eventName
    decoded
  }
}
```

The example indexes ORMP and Msgport contract logs on Ethereum mainnet and
Polygon PoS using known smoke-test block ranges. JSONL output remains useful for
debugging, but database output is the primary application deployment path for
this example.
