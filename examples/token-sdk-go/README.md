# Token SDK Go Example

Purpose: Show a Go application querying token activity through an independently
running Datalens service.

`datalens serve` runs separately as the shared cache and query service. This
example uses only `sdks/go` and the public `/index/graphql` and
`/native/graphql` APIs.

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
cargo run -p datalens-cli -- serve --config config/datalens.dev.toml
```

Run the example:

```sh
cd examples/token-sdk-go
DATALENS_ENDPOINT=http://127.0.0.1:3000 go run .
```

The first run over an uncached bounded range should show cache miss/fill
metadata in the native cache summaries. Rerun the same command to confirm cache
hit metadata for the Solana and TRON native queries.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATALENS_ENDPOINT` | `http://127.0.0.1:3000` | Datalens service endpoint |
| `DATALENS_TOKEN` | unset | Optional bearer token |
| `DATALENS_APPLICATION` | `token-sdk-go` | Application header value |
| `DATALENS_ETHEREUM_FROM_BLOCK` | `19000000` | Ethereum bounded start block |
| `DATALENS_ETHEREUM_TO_BLOCK` | `19000010` | Ethereum bounded end block |
| `DATALENS_ETHEREUM_FIRST` | `10` | Ethereum result page size |
| `DATALENS_SOLANA_FROM_SLOT` | `250000000` | Solana bounded start slot |
| `DATALENS_SOLANA_TO_SLOT` | `250000003` | Solana bounded end slot |
| `DATALENS_TRON_FROM_BLOCK` | `60000000` | TRON bounded start block |
| `DATALENS_TRON_TO_BLOCK` | `60000002` | TRON bounded end block |

Live smoke requires Datalens to be configured with RPC access for Ethereum,
Solana, and TRON. Do not commit real RPC URLs, API keys, or tokens.

## Tests

Tests use mock data and query construction only, so CI does not need live RPC or
a running Datalens service:

```sh
cd examples/token-sdk-go
go test ./...
```
