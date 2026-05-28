# 07 - Reorg-Aware Hot Cache Layer

This step defines the hot cache boundary for queries above the durable safe/finalized
height. The durable cache remains the long-term historical archive. The hot cache is a
separate, reorg-aware layer for unsafe or latest data that has not yet reached the durable
boundary.

This document defines architecture and contracts only. It does not require a concrete hot
storage layout, complete rollback algorithm, promotion writer, durable manifest schema
change, or Tron/Solana adapter implementation.

## Durable And Hot Boundary

The durable cache may contain only safe or finalized coverage. Durable object chunks,
manifest coverage, and manifest empty coverage must continue to satisfy the durable write
invariant:

```text
range.end <= adapter.safe_height().value
adapter.safe_height().finality in { Safe, Finalized }
```

Hot cache data is non-durable query acceleration. It may contain unsafe, latest, or
otherwise not-yet-durable ranges, but it must never directly write durable manifest
coverage or durable empty coverage. Hot cache rows, indexes, block metadata, and rollback
state are separate from durable object layout and manifest truth.

Promotion from hot cache to durable cache is an explicit workflow. Promotion may read hot
data only after the relevant range is at or below the adapter safe/finalized boundary,
then it must pass through the normal durable writer and manifest update path. Promotion is
not permission for hot storage to mutate durable manifests directly.

Durable cache never rolls back for hot reorgs. If a reorg affects data that was previously
promoted, that is a durable finality violation and must be treated as an adapter or
operator incident, not as normal hot cache rollback.

## Query Flow

The planner must classify a requested range against three boundaries:

- Durable boundary: highest safe/finalized height that may be served or filled through
  the durable path.
- Hot boundary: configured latest/unsafe window that the hot layer is allowed to inspect.
- Provider boundary: latest canonical height reported by the adapter/provider.

When a request crosses boundaries, the first hot-capable planner should split it into
ordered segments:

- `durable_read`: range covered by durable manifest.
- `durable_fetch`: missing range at or below the durable boundary; fetched and written
  through the durable writer.
- `hot_read`: range covered by hot cache and still canonical according to hot metadata.
- `live_fetch`: range not covered by hot cache but inside the supported hot/latest window.

Mixed responses are allowed in the hot-capable API contract when the request asks for
latest-capable behavior. A response may combine durable cache, hot cache, and live
provider segments, but each segment must be represented in response metadata with source
and finality. Durable-only requests keep the current behavior: a range above the durable
boundary returns a stable invalid-input or unsupported error and must not silently become
a mixed hot response.

The shared response metadata vocabulary is:

- `source = durable_cache`: rows came from durable object storage and manifest coverage.
- `source = hot_cache`: rows came from the hot cache.
- `source = live_provider`: rows came directly from an adapter/provider fetch.
- `finality = finalized`: data is finalized by the adapter.
- `finality = safe`: data is safe by the adapter.
- `finality = unsafe`: data is below latest but not safe/finalized.
- `finality = latest`: data is at the provider latest boundary.

The Rust contract for this metadata is `QuerySegmentMetadata` with `QuerySegmentSource`
and `QueryDataFinality`.

## Reorg-Aware Requirements

Every hot cache entry must be tied to block metadata for the chain-family range model.
For block-height chains, the minimum metadata is:

- `chain`: full `ChainIdentity`, including family, configured name, and network id when
  available.
- `range_kind`: for EVM, `block`.
- `height`.
- `hash`.
- `parent_hash`.
- `finality`.
- Dataset key, selector fingerprint, and normalized range.

The chain adapter boundary must expose canonical block lookup for adapters that support
hot cache. The shared contract is `canonical_block(CanonicalBlockRequest) ->
CanonicalBlock`. An adapter that cannot answer canonical hash queries must return
`UnsupportedDataset` and the hot path must reject latest-capable queries instead of
pretending the hot cache missed.

The minimum reorg detection signals are:

- Parent mismatch: a block's `parent_hash` does not match the stored previous hot block
  hash for the same chain identity and range kind.
- Same height different hash: hot metadata already has height `H` with hash `A`, but the
  provider canonical block at `H` has hash `B`.
- Provider canonical hash change: a previously observed canonical hash changes when
  rechecked through `canonical_block`.

Hot rollback is limited to unsafe data. The hot layer may delete, replace, or mark stale
hot entries that are not safe/finalized. It must not delete durable chunks, remove
durable manifest entries, or rewrite durable object storage as part of normal reorg
handling.

After rollback, affected future queries must either refetch the canonical hot range or
return a stable reorg/unsupported error. They must not return stale rows without metadata
that identifies the reorg outcome.

## Multi-Chain And Application Context

Hot cache state must be isolated by `ChainIdentity`. Two chains with the same configured
name but different family or network id must not share hot entries, block metadata,
rollback cursors, promotion state, metrics, or usage ledger attribution.

The hot layer is inside the normal application boundary. It must not bypass API
authentication, chain/dataset allowlists, request range limits, quota validation, or
normalized application identity. Hot hits, hot misses, live fetches, reorg rollback, and
promotion outcomes are attributable usage for the requesting or promotion-owning
application context.

Usage ledger outcomes must distinguish durable and hot behavior. The first shared
vocabulary includes:

- Query outcomes: `hot_hit`, `hot_miss`, `mixed`, `reorg_rollback`,
  `promotion_completed`, `promotion_skipped`.
- Cache outcomes: `hot_hit`, `hot_miss`, `mixed`.
- Fill outcomes: `live_fetch`, `reorg_rollback`, `promotion_written`,
  `promotion_skipped`.

Metrics must use distinct outcome labels for durable cache, hot cache, live fetch, reorg
rollback, and promotion. Labels must continue to use normalized application id, chain,
chain kind, and dataset. Metrics labels must not include selector values, block hashes,
raw credentials, or untrusted headers.

## Unsupported First-Version Behavior

The first hot-capable implementation must return stable unsupported errors for these
cases:

- The adapter does not support `canonical_block`.
- The adapter cannot determine latest height.
- The adapter cannot determine the durable safe/finalized boundary.
- The request reaches deeper than the configured hot reorg window.
- Chain finality cannot be represented as `safe`, `finalized`, `unsafe`, or `latest`.
- Reorg metadata is incomplete for a hot hit.
- The chain identity is ambiguous or not registered.
- Tron or Solana latest/hot cache behavior is requested before those adapters define
  their canonicality contracts.

These errors must not be reported as durable cache misses. A durable cache miss means
safe/finalized durable coverage is absent and may be filled through the durable writer.
Unsupported hot behavior means datalens cannot safely serve the latest-capable request.

## First Implementation Scope

The first implementation based on this architecture should add only the smallest
hot-aware runtime path:

- Planner segmentation for durable, hot, and live provider ranges.
- Hot cache interface traits or equivalent module boundary.
- Adapter canonical block lookup for EVM providers that support it.
- Response segment metadata in API and SDK models.
- Metrics and usage ledger recording for hot outcomes.
- Unsupported errors for adapters or chains that do not meet hot requirements.

Storage layout, complete rollback replacement, promotion scheduling, and durable
promotion writes should remain follow-up work unless the implementing issue explicitly
includes them.
