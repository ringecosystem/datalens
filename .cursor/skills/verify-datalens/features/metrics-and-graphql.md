# Metrics and GraphQL

## Sub-features

- `GET /metrics` Prometheus text, bearer `METRICS_TOKEN`; labels `application`, `chain`,
  `chain_kind`, `dataset`.
- `POST /native/graphql` native query surface.

## How to get to it (user POV)

Prometheus scrapes `/metrics`; GraphQL clients use `/native/graphql`.

## Driving it with verify-datalens

```sh
source ../../../.data/verify/$RUN/state.env
curl -s -H "Authorization: Bearer $METRICS_TOKEN" "$BASE_URL/metrics" \
  | grep -E '^datalens_query_total' > ../../../.data/verify-evidence/$RUN/metrics.txt
curl -s -X POST "$BASE_URL/native/graphql" -H 'content-type: application/json' \
  -H "x-datalens-application: $APP" -H "Authorization: Bearer $TOKEN" \
  -d '{"query":"{ __schema { queryType { fields { name } } } }"}'
```

Expect `datalens_query_total{application="verify",chain="ethereum",...}` after any query.

## Gotchas

- `/metrics` without the bearer returns 401.
- GraphQL field names come from introspection; check them before writing queries.
