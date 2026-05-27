# 05 - Chain Adapters And EVM

This step keeps the project from becoming EVM-only while still giving the first release a
real chain family to implement.

## Adapter Model

A chain adapter converts chain-family-specific sources into datalens structured datasets.
The core system asks for ranges and datasets; the adapter decides how to fetch that data
from the chain family.

An adapter should expose:

- Chain family and chain identity.
- Latest and safe/finalized height information when available.
- Supported datasets and filters.
- Fetch operations for bounded ranges.
- Source health and provider capability information.
- Error classification that distinguishes retryable failures, permanent unsupported
  requests, provider limits, and invalid input.

The core should not know whether EVM uses `eth_getLogs`, whether Solana uses slots, or
whether Tron has a different event model.

For durable cache writes, the adapter's safe/finalized height is authoritative for the
chain-family range kind it exposes. First-stage datalens accepts only `Safe` or
`Finalized` adapter heights for durable coverage. `Latest` is observable source state, not
durable cache finality.

## EVM First

The first adapter should be `datalens-evm`. It should support the minimum datasets needed
to prove query-driven historical caching:

- Block headers.
- Logs filtered by address and topics.
- Transactions when requested.
- Receipts only when required by the planned response.

The first fill path can focus on logs because logs are a common indexing primitive and
are easy to make selective. Block headers can be added early because many indexers need
height, hash, parent hash, and timestamp context.

## EVM Source Strategy

The adapter should support configurable RPC endpoints first. IPC or node-local transports
can be added later as provider options, but datalens should not pretend that adapter
transport support means it replaces every RPC workflow.

Provider behavior should be isolated in `datalens-evm`: batching, rate-limit backoff,
range splitting, retry decisions, and EVM response decoding belong there. Once data leaves
the adapter boundary, it should be normalized into datalens dataset rows.

The EVM adapter determines safe/finalized height through a chain-level finality policy:

- `mode = "auto"` first tries `eth_getBlockByNumber("finalized", false)`, then
  `eth_getBlockByNumber("safe", false)`.
- If RPC finality tags are unsupported, `auto` may use an adapter-owned chain profile
  with conservative lag values.
- Unknown chains without RPC finality tag support must require an explicit override
  instead of treating `latest` as safe.
- `mode = "lag"` is an explicit operator override using positive `safe_lag_blocks` or
  `finalized_lag_blocks`.
- `mode = "rpc_tags"` is an advanced override for provider-specific tag names.

Lag-based values are fallback policy, not the default source of truth. A lag of zero must
not be used for durable cache writes because that would mark `latest` as safe/finalized.

## Future Tron And Solana Support

Future adapters should not be forced to imitate EVM. Solana may use slots and account
changes; Tron may have its own event and transaction surfaces. Those concepts should
become chain-family-specific datasets behind the same adapter boundary.

Adding a new chain family should require:

1. A new adapter crate such as `datalens-solana` or `datalens-tron`.
2. Dataset definitions for that family.
3. Fetch and normalization code for that family.
4. Capability metadata so the planner knows what the adapter can satisfy.
5. Storage chunks and coverage records that include the new `chain-kind`.

It should not require rewriting `datalens-core`, `datalens-storage`, or the native API.

## What To Implement In This Step

The first adapter implementation should include:

- Chain adapter traits or equivalent interfaces.
- EVM adapter configuration for chain name, chain id, and RPC URLs.
- Safe/latest height lookup.
- EVM logs fetch over bounded ranges.
- Normalization into datalens-owned log records.
- Provider error classification.
- Tests using a mock provider or recorded fixture.

After this step, the query/fill loop should be able to fetch real or fixture-backed EVM
logs without embedding EVM assumptions into the chain-neutral crates.
