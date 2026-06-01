# Token SDK TypeScript Example

Purpose: Show a TypeScript application querying token activity through an
independently running Datalens service.

`datalens serve` runs separately as the shared cache and query service. This
live example uses only `sdks/typescript` and the public `/native/graphql` API
exposed by `datalens serve`.

`datalens serve` does not expose `/index/graphql`. Application-specific index
GraphQL endpoints are owned by external application services. This example
therefore queries Ethereum USDC through the native `evm.logs` path.

## Token targets

| Chain | Token | Target |
| --- | --- | --- |
| Ethereum | USDC | `0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48` |
| Solana | USDC | `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v` |
| TRON | USDT | `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t` |

Ethereum and Solana USDC addresses come from Circle's USDC contract address
docs. TRON uses Tether USDT, not USDC, because Circle discontinued USDC support
on TRON. The TRON selector uses the normalized hex form
`41a614f803b6fd780986a42c78ec9c7f77e6ded13c`.

## Run

Start Datalens separately:

```sh
export DATALENS_LIVE_SMOKE_TOKEN=replace-with-live-smoke-token
export DATALENS_SERVER_BIND=127.0.0.1:3100
cargo run -p datalens-cli -- serve --config config/datalens.compose.toml --bind "$DATALENS_SERVER_BIND"
```

Run the example:

```sh
cd sdks/typescript && npm ci && npm run build
cd ../../examples/token-sdk-typescript
npm install
export DATALENS_ENDPOINT=http://$DATALENS_SERVER_BIND
export DATALENS_APPLICATION=live-smoke
export DATALENS_TOKEN=$DATALENS_LIVE_SMOKE_TOKEN
npm run build
npm start
```

The first run over an uncached bounded range should print `rows=<n>` plus sample
`row=...` lines and cache miss/fill metadata for Ethereum, Solana, and TRON.
Rerun the same command to confirm cache hit metadata for the same native
queries.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATALENS_ENDPOINT` | `http://127.0.0.1:3000` | Datalens service endpoint |
| `DATALENS_TOKEN` | unset | Bearer token; use `$DATALENS_LIVE_SMOKE_TOKEN` with `config/datalens.compose.toml` |
| `DATALENS_APPLICATION` | `token-sdk-typescript` | Application header value; use `live-smoke` with `config/datalens.compose.toml` |
| `DATALENS_ETHEREUM_FROM_BLOCK` | `19000000` | Ethereum native `evm.logs` bounded start block |
| `DATALENS_ETHEREUM_TO_BLOCK` | `19000010` | Ethereum native `evm.logs` bounded end block |
| `DATALENS_SOLANA_FROM_SLOT` | `250000000` | Solana bounded start slot |
| `DATALENS_SOLANA_TO_SLOT` | `250000003` | Solana bounded end slot |
| `DATALENS_TRON_FROM_BLOCK` | `83200000` | Public RPC smoke TRON bounded start block |
| `DATALENS_TRON_TO_BLOCK` | `83200002` | Public RPC smoke TRON bounded end block |

Public RPC smoke uses `evm.logs` on Ethereum chain id `1`,
`solana.transactions` on Solana chain id `101`, and `tron.events` on TRON chain
id `728126428`. It requires Datalens to be configured with RPC access for those
chains. Do not commit real RPC URLs, API keys, or tokens.

Archive/business TRON ranges, including older ranges around block 60000000,
require an archive-capable TRON provider or a TronGrid-backed path. Keep those
ranges as explicit environment overrides, not as public RPC smoke defaults.

## Tests

Tests lock that the executable live smoke path calls native Ethereum, Solana,
and TRON queries. They use mock data, so CI does not need live RPC,
`/index/graphql`, or a running Datalens service:

```sh
cd sdks/typescript && npm ci && npm run build
cd ../../examples/token-sdk-typescript
npm install
npm test
```
