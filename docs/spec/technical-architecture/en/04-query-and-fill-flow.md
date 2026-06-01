# 04 - Query And Fill Flow

This step implements the behavior that makes datalens useful: a query can return cached
history when it exists and can materialize missing history when it does not.

## Query Planning

The API should not call storage or chain adapters directly. It first sends a native
request to the planner. The planner turns the request into an executable plan containing:

- Chain kind and chain identity.
- Dataset requirements.
- Range requirements.
- Filters and predicates.
- Field selection.
- Response ordering.
- Safe or finalized height policy.
- Capability requirements for the target adapter.

The plan should be bounded before any storage lookup or fetch begins. If a request would
require unbounded work, the planner should reject it or split it into configured maximum
ranges.

The plan must also separate durable work from latest-capable work before any storage
lookup, fetch, or durable write. Durable-only requests are bounded by the adapter
safe/finalized height and serve only safe/finalized historical ranges from the durable
cache path. If a durable-only requested range ends above `adapter.safe_height()`, the
query must fail with a clear invalid-input error. It must not silently truncate the
range, fetch only the safe part, or return a response that looks complete.

Latest-capable requests are explicit. `SafeToLatest` may split the request into durable
coverage at or below the safe/finalized boundary and a hot/live provider tail up to the
adapter latest height. `LatestOnly` may read through live provider data without reading
or writing durable cache coverage. These segments must be visible in response metadata.

## Cache States

A cache hit means manifest coverage satisfies the whole plan. The system reads stored
chunks and streams the response. It does not fetch equivalent data again.

A partial hit means some durable ranges are covered and some durable ranges are missing.
The system reads the covered ranges, fetches only the durable missing ranges, passes
safe/finalized fetched rows through the durable writer, updates manifest coverage when
the writer flushes or records empty coverage, and returns one coherent response.

A durable cache miss means no required durable range is covered. The system fetches the
planned durable ranges, passes safe/finalized rows through the durable writer, updates
manifest coverage when the writer flushes or records empty coverage, and returns the
response. A hot/latest live fetch is not a durable cache fill and must not write unsafe
data into durable storage.

The caller should not need to know which state happened except through optional metadata,
latency, or observability. The result should have the same meaning.

After the executor knows the query outcome, cache outcome, fill outcome, and row count, it
should append an application usage ledger event when an application identity is available.
The ledger event must use the registry-normalized application id supplied by the API or
service context, not raw credentials. Full hits record `hit` cache and query outcomes.
Partial hits record `partial_hit`. Misses that write rows record `filled` query outcome
and `written` fill outcome. Misses that record empty coverage use `empty` query outcome
and `empty_coverage_recorded` fill outcome. Provider and storage failures use the
corresponding error outcomes and must not make incomplete responses look successful.

The interval above safe/finalized height and up to latest height is outside durable cache
semantics. Callers that need that interval must opt into latest-capable behavior through
`QueryFinalityRequirement`, using `safe_to_latest` or `latest_only`. Hot or live provider
data must not update durable manifest coverage.

## Fill Execution

Missing ranges should be grouped into fetch tasks. The resolver should prefer fewer
larger fetches when the adapter and provider allow it, but must respect provider limits
and configured range caps.

Every fetch task that will write through the durable cache path must satisfy the same
safe/finalized height bound as the durable portion of the original plan. If a durable
write task is above that height, the executor must fail before calling storage. It must
not write a data object, manifest coverage, or empty coverage. Fetch tasks for hot/live
segments must be marked non-durable and must not call the durable writer.

For EVM logs, chain dataset config chooses either `provider_filter` (`eth_getLogs`) or
`block_range` (block plus receipt scan with local address/topic filtering). The durable
cache key remains the logical selector and range; operators must configure
`block_range` only for providers where receipt scans are complete for the requested
range. For block headers, it can fetch by block number batches. For transactions or
receipts, it can fetch only when the plan requires those datasets.

Fetched data must be normalized before it enters writer or response assembly code. Raw
adapter payloads are not the durable cache contract.

## Response Assembly

The response assembler combines stored chunks and newly fetched chunks. It should preserve
the planned ordering and field selection. Large responses should be streamed so callers do
not wait for every chunk to be loaded into memory.

If a missing range cannot be fetched or persisted, the response must not silently look
complete. The error should preserve whether the failure is retryable, provider-limited,
unsupported, or caused by invalid input.

## What To Implement In This Step

The first query/fill implementation should include:

- A native query request type.
- A planner that produces a bounded executable plan.
- A coverage check against manifest entries.
- A missing range resolver.
- A minimal EVM logs fill path.
- Response assembly that can merge cached and newly fetched data.
- Tests for hit, partial hit, miss, retry, and bounded range rejection.

After this step, datalens should be able to prove the core loop even if the dataset set is
small.
