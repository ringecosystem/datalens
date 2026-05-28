# 06 - API, SDK, And Compatibility

This step decides how people use datalens without letting any single external protocol
control the internal architecture.

## Native Service API

The native API is the primary service contract. It should expose datalens concepts rather
than copying another gateway's schema:

- Chain kind and chain name.
- Dataset selection.
- Range selection.
- Filters and predicates.
- Field selection.
- Safe/finalized height policy.
- Optional metadata about cache hit, partial hit, or fill behavior.

The native API should map cleanly onto the planner. It should not require API handlers to
know object storage details or EVM RPC details.

## SDK Role

The SDK is a convenience layer. It can provide typed requests, pagination helpers, retry
helpers, authentication helpers, and integration utilities for indexer authors.

The SDK does not have to own a complete indexing runtime. datalens itself is already a
service capable of answering historical structured queries. A future SDK can choose
between two shapes:

- Service client only: the SDK talks to datalens and leaves direct RPC indexing to the
  user's own tools.
- Hybrid helper: the SDK can optionally fall back to direct RPC when datalens is not
  configured or when a request is outside datalens scope.

This decision should be made later based on real integration needs. The architecture only
requires that direct RPC indexing remains a legitimate workflow.

## Rust Client Contract

The first stable Rust client contract is the `datalens-client` crate.

Production dependencies:

- `datalens-core`
- `reqwest`
- `serde`
- `serde_json`

The client crate must not depend on executor, storage, writer, or API server runtime
construction crates.

The default client behavior is service-client only:

- `DatalensClient::query_blocks` sends a JSON request to `POST /v1/query` with
  `dataset: "blocks"`.
- `DatalensClient::query_logs` sends a JSON request to `POST /v1/query` with
  `dataset: "logs"`.
- `DatalensClient::discover` reads `GET /v1/discovery`.
- The client must not call executor, storage, writer, chain adapters, or RPC providers
  directly.

## HTTP Contract

`POST /v1/query` request fields:

- `chain`: `ChainIdentity`.
  - `family` is the chain family, such as `Evm`.
  - `configured_name` is the configured service chain name, such as `ethereum`.
  - `network_id` identifies the upstream network when available.
- `dataset`: `blocks` or `logs`.
- `range`: inclusive `from_block` and `to_block`.
- `filter`: required for `logs`, absent or `null` for `blocks`.
- `include_block`: retained as a compatibility field; first-version client requests use
  `false`.

Logs filter fields:

- `addresses`: empty means any address.
- `topics`: ordered topic positions.
- `null` topic position means wildcard.
- Empty topic value set at a position means match no topic at that position.
- Non-empty topic value set means match any listed topic at that position.

`POST /v1/query` response fields:

- `chain`: the resolved `ChainIdentity`.
- `range`: the inclusive requested block range.
- `cache.hit_ranges`: inclusive block ranges served from durable cache.
- `cache.missing_ranges`: inclusive block ranges not served from durable cache.
- `rows`: `QueryRows`, sorted by dataset order before response.
- Empty results are represented by an empty `rows` array for the selected dataset.

Client cache outcome helpers:

- `FullHit`: `hit_ranges` is non-empty and `missing_ranges` is empty.
- `PartialHit`: both `hit_ranges` and `missing_ranges` are non-empty.
- `Miss`: `hit_ranges` is empty and `missing_ranges` is non-empty.
- `Empty`: both range lists are empty.

`GET /v1/discovery` response fields:

- `chains`: ordered chain discovery entries.
- `chains[].identity`: `ChainIdentity` with family, configured name, and network id.
- `chains[].datasets`: enabled first-version datasets, currently `blocks` and `logs`.

Error responses have the stable shape:

- `error.kind`: stable snake-case error kind.
- `error.message`: diagnostic message for operators and logs.

SDK callers must branch on typed error kind, not on `error.message`.

## Application Identity

SDK requests must send application identity using the `x-datalens-application` header.

Rules:

- Empty or whitespace-only SDK application identity becomes `unknown`.
- Missing SDK application identity becomes `unknown`.
- The server maps a present `x-datalens-application` header to the metrics
  `application` label.
- If the header is absent, the server uses its configured default metrics application.
- Application identity is request metadata. It must not alter durable cache keys or
  object layout.

## Fallback Boundary

The first Rust client does not implement safe-to-latest hot query or real RPC fallback.

The fallback boundary is explicit:

- `FallbackMode::Rpc` returns `UnsupportedFallback`.
- Unsupported fallback must not send an HTTP query to datalens.
- Unsupported fallback must not write durable cache.
- Future hot query or RPC fallback must remain isolated from durable safe/finalized cache
  writes.

## Compatibility Adapters

Compatibility adapters should be edge adapters. SQD Gateway-compatible behavior can be
implemented later by translating incoming SQD-shaped requests into native datalens
requests, then reshaping native responses into the expected compatibility response.

Compatibility must not define:

- Core dataset names.
- Manifest coverage semantics.
- Chunk storage format.
- Chain adapter interfaces.
- Planner behavior.

This separation lets datalens serve multiple caller types without storing the same
historical data in many caller-specific formats.

## What To Implement In This Step

The first API implementation should include:

- A native query endpoint.
- A height or status endpoint.
- Response streaming for large results.
- Clear error responses for invalid input, unsupported dataset, provider failure, and
  storage failure.
- A small SDK or client example only after the native API has stabilized.

SQD compatibility should not be first. It should come after the native request, planner,
storage, and EVM fill path are proven.
