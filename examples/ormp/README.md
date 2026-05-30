# ORMP Declarative Index Example

This example shows ORMP/Msgport EVM log indexing with the declarative index
runner. Application authors provide `ormp.index.toml`; no custom Rust client code
is needed.

The config uses `DATALENS_ORMP_TOKEN` for the application token. Set it to a
local development token before running the index commands:

```sh
export DATALENS_ORMP_TOKEN=replace-with-local-token
```

Start the datalens server separately. For local development, use the server
configuration and storage setup documented by the repo runbooks, then run:

```sh
cargo run -p datalens-cli -- serve --config path/to/datalens.toml
```

In another shell, inspect and plan the ORMP index:

```sh
cargo run -p datalens-cli -- index doctor --config examples/ormp/ormp.index.toml
cargo run -p datalens-cli -- index plan --config examples/ormp/ormp.index.toml
```

With the server still running, execute the index:

```sh
cargo run -p datalens-cli -- index run --config examples/ormp/ormp.index.toml
```

The example indexes ORMP and Msgport contract logs on Ethereum mainnet and
Polygon PoS using known smoke-test block ranges. It does not decode ORMP ABIs;
rows are emitted as raw EVM logs.
