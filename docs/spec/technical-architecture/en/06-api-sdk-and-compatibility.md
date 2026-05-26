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
