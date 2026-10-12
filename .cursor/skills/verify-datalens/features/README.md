# Features

`scripts/e2e.sh` is the full live run. Each row is one behavior of the PostgreSQL API.

| Feature | What it drives |
| --- | --- |
| doctor | `/health`, `/ready`, 401 without a token, authenticated heads, proxy |
| auth-bad-token | Wrong bearer on `GET /v1/heads/ethereum` is 401 |
| logs-durable | `POST /v1/query` USDC Transfer, `durable_only`, `served` set, no `incomplete_reason` |
| logs-cache-hit | The same query adds zero `eth_getLogs` |
| transaction_from | One block with `fields` including `transaction_from` |
| pagination | `page.limit = 1` and `next_cursor` when the fill had more than one row |
| blocks-durable | `evm.blocks` over the pin window returns at least one header |
| watch | `POST /v1/watch` for the USDC address |
| heads-wait | `GET /v1/heads/ethereum/wait` returns 200 |
| graphql | `POST /graphql` `query(input:)` returns `evm.logs` |
| quota-range | A 200-block range is 400 `invalid_input` |
| metrics | `/metrics` contains `datalens_provider_calls_total` |
