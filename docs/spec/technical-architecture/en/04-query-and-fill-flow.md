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

The plan must also be bounded by the adapter safe/finalized height before any storage
lookup, fetch, or durable write. First-stage datalens serves only safe/finalized
historical ranges from the durable cache path. If the requested range ends above
`adapter.safe_height()`, the query must fail with a clear invalid-input error. It must not
silently truncate the range, fetch only the safe part, or return a response that looks
complete.

## Cache States

A cache hit means manifest coverage satisfies the whole plan. The system reads stored
chunks and streams the response. It does not fetch equivalent data again.

A partial hit means some ranges are covered and some are missing. The system reads the
covered ranges, fetches only the missing ranges, writes the fetched data as durable
chunks, updates manifest coverage, and returns one coherent response.

A cache miss means no required range is covered. The system fetches the planned ranges,
persists them, updates manifest coverage, and returns the response.

The caller should not need to know which state happened except through optional metadata,
latency, or observability. The result should have the same meaning.

The interval above safe/finalized height and up to latest height is outside first-stage
durable cache semantics. Callers that need that interval must query RPC directly or use a
future explicitly non-durable hot path. Hot data must not update durable manifest
coverage.

## Fill Execution

Missing ranges should be grouped into fetch tasks. The resolver should prefer fewer
larger fetches when the adapter and provider allow it, but must respect provider limits
and configured range caps.

Every fetch task that will write through the durable cache path must satisfy the same
safe/finalized height bound as the original plan. If a task is above that height, the
executor must fail before calling storage. It must not write a data object, manifest
coverage, or empty coverage.

For EVM logs, the first fill implementation can fetch by `eth_getLogs` using the planned
address/topic/range filters. For block headers, it can fetch by block number batches. For
transactions or receipts, it can fetch only when the plan requires those datasets.

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
