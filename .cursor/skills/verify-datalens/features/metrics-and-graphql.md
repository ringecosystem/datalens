# Metrics and GraphQL

`scripts/e2e.sh` posts the same durable logs query to `POST /graphql` as `query(input:)` and checks the result names `evm.logs`.

It then fetches `/metrics` with the metrics bearer token and requires `datalens_provider_calls_total`.
