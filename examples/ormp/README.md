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

The local workflow uses two processes:

1. `datalens serve` runs the shared cache server.
2. `datalens index daemon` runs the ORMP application indexer, writes indexed
   events to SQLite, labels configured ORMP/Msgport ABI events, and serves
   GraphQL for those indexed events.

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

The example keeps only the ABI event fragments it needs in `abi/events.json`.
The same minimal event definitions are declared in `ormp.index.toml` under
`[decode]` so the daemon can attach stable event names and signatures to
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
