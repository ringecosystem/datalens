# Edge Crate

Purpose: Describe the current `datalens-edge` crate layout and service boundary.

Read this when: You need to place edge code for REST, GraphQL, metrics, discovery,
warmup, authentication, or query routing.

Not this document: Normative API contracts live under `docs/spec/`; operational
procedures live under `docs/runbook/`.

Covers: Current module ownership in `crates/edge/src/`.

## Service Boundary

`datalens-edge` is the multi-transport service boundary for datalens.

- REST routes live under `http/`.
- GraphQL schema, resolvers, and scalar helpers live under `graphql/`.
- Prometheus metrics are exposed by the HTTP metrics handler and recorded by query
  services.
- Discovery DTOs live under `contract/discovery.rs`.
- Query DTOs live under `contract/query.rs`.
- Warmup DTOs live under `contract/warmup.rs`.
- Stable API error response mapping lives under `contract/error.rs`.
- Application authentication, allowlists, and quota checks live under
  `auth/application.rs`.
- Query services, the service registry, warmup service traits, and lifecycle shutdown
  live under `service/`.

REST and GraphQL expose equivalent query capability over the same native query contract.
Transport code may adapt request and response shape, but it must call the shared native
query service path instead of defining transport-specific query behavior.
