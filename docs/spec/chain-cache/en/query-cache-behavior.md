# Query-Cache Behavior

Purpose: Define the normative query lifecycle, cache-hit behavior, cache-miss fill
behavior, and correctness expectations for datalens chain-cache query execution.

Status: normative

Read this when: You are designing or reviewing query planner, cache coverage, missing
range resolution, writer, or response streaming behavior.

Not this document: This document does not define API endpoint names, chain-specific fetch
algorithms, final object key layouts, final chunk schemas, or implementation code.

Defines:

- Query-driven request lifecycle.
- Cache hit, partial hit, and miss behavior.
- Field selection and selective dataset storage rules.
- Missing range materialization behavior.
- Correctness expectations for coverage, retries, limits, concurrency, streaming, safe
  heights, and reorg posture.

Paired translation: `../zh-CN/query-cache-behavior.md`

## Query-Driven Cache Model

datalens chain-cache MUST cache structured chain data derived from normalized dataset
requirements. It MUST NOT treat arbitrary raw SDK response blobs, raw RPC responses, or
adapter-native payloads as the durable cache authority.

The query path MUST be demand-driven. A query MAY cause missing historical data to be
fetched, normalized, written as structured chunks, and marked covered before the response
is complete.

The first query for a dataset and range MAY be slower than later equivalent queries
because the first query materializes missing history. Later equivalent queries SHOULD read
the durable structured chunks instead of fetching the same source range again.

The cache MUST be reusable across compatible callers when their planned dataset, range,
chain, finalized/safe height, and field requirements are satisfied by already covered
structured chunks.

## Request Lifecycle

The canonical query lifecycle is:

1. Accept query.
2. Plan required datasets and bounded ranges.
3. Inspect manifest coverage.
4. Read covered objects.
5. Resolve missing ranges.
6. Fetch missing ranges from the chain adapter.
7. Write fetched normalized data as structured chunks.
8. Update manifest coverage after durable chunk writes succeed.
9. Stream or return the planned response.

The implementation MAY pipeline reads, fetches, writes, and response streaming, but the
observable behavior MUST preserve this lifecycle ordering:

- A query MUST be planned before coverage is trusted.
- Covered objects MUST be read through the storage boundary.
- Missing ranges MUST be derived from manifest coverage and the planned requirements.
- Chain adapter fetches MUST be limited to missing ranges that are required for the
  planned response.
- A fetched chunk MUST NOT expand durable coverage until the structured chunk write
  succeeds.
- The response MUST expose the planned result shape, not adapter-native payloads.

## Cache Hit States

A cache hit occurs when manifest coverage proves that every required dataset and range
for the planned query is already durable at the required finalized/safe height and field
coverage level.

On a cache hit:

- The query MUST read covered structured chunks from storage.
- The query MUST NOT fetch equivalent data from the chain adapter.
- The response MAY be streamed from stored chunks.
- Response ordering, filtering, and projection MUST match the planned query semantics.

A partial hit occurs when manifest coverage satisfies only part of the planned query.

On a partial hit:

- Covered ranges MUST be read from storage.
- Missing ranges MUST be resolved and fetched through the chain adapter.
- Newly fetched data MUST be normalized and written as structured chunks before it is
  marked covered.
- The response MAY combine stored chunks and newly fetched chunks.
- The combined response MUST be indistinguishable from a response produced from a complete
  durable cache, except for latency and streaming timing.

A cache miss occurs when no required dataset/range segment for the planned query is
covered.

On a cache miss:

- The missing range resolver MUST derive the required fetch work from the planned query.
- The chain adapter MUST fetch only the bounded ranges and datasets required by the plan.
- The writer MUST persist normalized structured chunks and update manifest coverage after
  durable writes succeed.
- The response MAY begin streaming only when the streamed items have a defined source:
  durable storage or newly fetched normalized chunks that are part of the planned result.

## Field Selection And Dataset Requirements

The query planner MUST translate caller field selection into explicit dataset and field
coverage requirements.

Field selection MAY reduce response projection. Field selection MUST NOT cause the system
to store arbitrary caller-shaped response blobs as cache artifacts.

When a selected field belongs to a structured dataset that is already cached with
sufficient field coverage, the response SHOULD project from the cached structured chunk.

When a selected field requires a dataset or field group that is not covered, the missing
range resolver MUST include that dataset or field group in the fetch plan.

The implementation MAY store a superset of the selected fields when the superset is the
canonical structured chunk shape for that dataset. The implementation MUST NOT claim
coverage for fields or datasets that were not durably written.

Selective dataset requirements MUST avoid forcing unrelated datasets to be fetched. For
example, a query that requires logs MUST NOT fetch transaction receipts, traces, or state
items unless the planned response semantics require those datasets.

## Coverage And Manifest Expectations

Manifest coverage MUST be deterministic for the same chain, dataset, normalized range,
finalized/safe height policy, and field coverage policy.

Coverage entries MUST distinguish at least:

- Chain or chain family identity.
- Structured dataset identity.
- Covered range.
- Field coverage level or canonical chunk shape.
- Finalized/safe height basis.
- Durable chunk identity or storage reference.

Manifest updates MUST happen after structured chunks are durably written. A failed write
MUST NOT create coverage. A failed manifest update MAY leave a durable chunk unreferenced
by coverage, but retry behavior MUST be able to detect or overwrite the same logical
chunk without corrupting coverage.

Retries MUST be idempotent. Repeating the same fetch/write/manifest update for the same
logical chunk MUST converge on the same durable coverage state or fail without creating
contradictory coverage.

## Bounds, Concurrency, And Streaming

Every planned query MUST have bounded ranges before storage lookup or adapter fetch
begins. The planner MUST reject or split unbounded work before it reaches the chain
adapter.

Implementations MUST enforce concurrency limits for missing range fetches and writes.
Concurrency limits MAY be configured by deployment, chain family, provider capability, or
storage backend, but query execution MUST NOT allow unbounded adapter calls or unbounded
writer fan-out.

Large result sets SHOULD be streamed. Streaming MUST preserve the planned response order
and MUST NOT expose chunks that might later be rolled back by a failed required write or
manifest update unless the response explicitly treats those chunks as non-durable
in-flight results.

When a query cannot complete because required missing ranges cannot be fetched or written,
the response MUST report an error for the incomplete portion instead of silently returning
an apparently complete result.

## Finalized And Safe Height Handling

The planner MUST bind each query to an explicit finalized/safe height policy before
coverage is evaluated.

For finalized or safe historical queries, manifest coverage MUST be evaluated only against
chunks that satisfy the required finalized/safe height basis. Chunks above the accepted
safe height MUST NOT satisfy finalized/safe coverage.

The chain adapter MAY fetch data near the chain head only when the planned query and
finalized/safe height policy allow that range. The cache MUST distinguish head-adjacent or
provisional data from finalized/safe durable coverage.

## Reorg Posture

The specification-level reorg posture is conservative:

- Finalized/safe coverage MUST be preferred for durable reusable cache entries.
- Provisional head-adjacent data MUST NOT be promoted to finalized/safe coverage until the
  finalized/safe height policy permits promotion.
- If a reorg invalidates provisional cached data, later manifest coverage MUST exclude or
  supersede the invalidated chunks.
- Queries requiring finalized/safe data MUST NOT be answered from known-invalidated
  provisional chunks.

This document does not define chain-family-specific reorg detection, rollback, or
refetch algorithms.

## Non-Goals

This document MUST NOT define API endpoint names.

This document MUST NOT define chain-specific fetch algorithms.

This document MUST NOT define final storage object key layouts.

This document MUST NOT require complete chain-wide archival capture before a query can be
served.

This document MUST NOT be used as authority for implementation code.
