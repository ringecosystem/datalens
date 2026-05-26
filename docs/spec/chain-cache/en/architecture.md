# Chain-Cache Architecture

Purpose: Define the chain-cache module boundary contract and canonical data flow for the
query-driven structured archive design.

Status: normative

Read this when: You are creating workspace crates, assigning module ownership, or checking
whether a chain-cache design keeps chain-neutral and chain-family-specific concerns in the
right layer.

Not this document: This document does not define Rust trait signatures, final Parquet
schemas, object key layouts, service deployment topology, or per-chain data schemas.

Defines:

- Canonical chain-cache request and fill data flow.
- Canonical module names and responsibilities.
- Chain-neutral and chain-family-specific module boundaries.
- Compatibility layer placement rules.
- Extension rules for future Tron and Solana support.

Paired translation: `../zh-CN/architecture.md`

## Canonical Data Flow

The canonical chain-cache data flow is:

1. SDK/API request
2. Query planner
3. Coverage/manifest lookup
4. Object storage read
5. Missing range resolver
6. Chain adapter fetch
7. Chunk writer
8. Object storage
9. Response stream

The request path MUST preserve this ordering even when an implementation pipelines reads,
fetches, writes, and streaming for latency.

The query planner MUST translate the SDK/API request into a chain-neutral plan before
storage or chain-family code is selected.

The coverage/manifest lookup MUST decide which requested ranges are already durable in
object storage and which ranges are missing.

Object storage reads MUST serve cached ranges through the storage boundary, not by reading
chain-family objects directly from planner or API code.

The missing range resolver MUST derive the minimum fetch work needed to complete the
planned request from coverage/manifest results.

Chain adapter fetches MUST be invoked only for missing ranges and MUST enter through the
chain adapter boundary.

The chunk writer MUST persist fetched normalized chunks before those chunks are treated as
covered durable data.

The response stream MAY combine cached chunks and newly fetched chunks, but it MUST expose
the planned response shape rather than adapter-native payloads.

## Module Boundary Table

| Module | Boundary type | Responsibilities | Must not own |
| --- | --- | --- | --- |
| `datalens-core` | Chain-neutral | Shared domain vocabulary, request range concepts, normalized result envelopes, error categories, and cross-module invariants. | Chain-family RPC clients, object storage drivers, query planning policy, write orchestration, API compatibility behavior. |
| `datalens-chain` | Chain-neutral adapter contract | Chain adapter interfaces at the conceptual boundary, chain family identifiers, adapter capability metadata, and normalized fetch result expectations. | EVM-specific RPC semantics, final schemas, storage layout, API request parsing. |
| `datalens-evm` | Chain-family-specific | EVM chain adapter implementation, EVM RPC/provider integration, EVM-specific normalization before returning through `datalens-chain`. | Chain-neutral planning policy, object storage persistence policy, SDK/API compatibility layers. |
| `datalens-storage` | Chain-neutral | Coverage/manifest lookup, object storage read/write primitives, durable chunk existence checks, storage error mapping, and manifest consistency requirements. | Chain-family RPC fetches, query request interpretation, final Parquet schema authority, API response compatibility. |
| `datalens-planner` | Chain-neutral | Query planning, request normalization into executable plans, requested range decomposition, and plan validation against chain-neutral capabilities. | Object storage drivers, chain-family RPC details, chunk serialization, API transport concerns. |
| `datalens-writer` | Chain-neutral | Chunk write orchestration, idempotent write behavior, manifest update sequencing, and marking fetched chunks durable after storage succeeds. | Chain adapter RPC logic, SDK/API request parsing, final object key layout authority. |
| `datalens-api` | Edge-facing | SDK/API request parsing, transport-level validation, authentication/authorization integration points, response streaming, and compatibility adapters for external callers. | Core architecture rules, chain-family normalization, object storage internals, planner internals. |

## Chain Neutrality Contract

`datalens-core`, `datalens-chain`, `datalens-storage`, `datalens-planner`, and
`datalens-writer` MUST remain chain-neutral.

`datalens-api` MUST keep its internal orchestration chain-neutral, but it MAY contain
edge-facing compatibility adapters for specific SDK/API caller expectations.

`datalens-evm` is chain-family-specific and MUST keep EVM-specific RPC semantics,
normalization rules, provider behavior, and chain-family capability handling out of the
chain-neutral modules.

Future chain-family modules MUST follow the same role as `datalens-evm`: implement the
chain adapter boundary and return normalized results without changing the chain-neutral
module contract.

## Compatibility Layer Rules

Compatibility layers MUST sit at the system edge, primarily in `datalens-api` or
caller-specific SDK surfaces.

Compatibility layers MUST NOT shape `datalens-core`, `datalens-planner`,
`datalens-storage`, `datalens-writer`, or the chain-neutral parts of `datalens-chain`.

Compatibility behavior includes legacy request names, SDK-specific response aliases,
transport-specific pagination conventions, caller-specific defaults, and other external
contract accommodations.

The core architecture MUST use stable chain-cache concepts instead of compatibility
concepts because core modules must serve multiple callers and multiple chain families.

When compatibility behavior requires translating a caller request, the translation MUST
happen before the query planner receives the request. When compatibility behavior requires
renaming or reshaping a response, the reshaping MUST happen after the response stream has
the planned chain-cache result shape.

## Future Chain Families

The architecture MUST remain open for Tron and Solana by treating chain families as
adapter implementations behind `datalens-chain`.

Adding Tron or Solana MUST NOT require renaming the canonical modules, changing the
canonical data flow, or embedding Tron/Solana schema assumptions in chain-neutral modules.

Tron and Solana support MAY introduce future chain-family-specific modules, for example
`datalens-tron` or `datalens-solana`, when their adapter implementations are designed.

This architecture deliberately does not commit to Tron or Solana schemas, object key
layouts, RPC pagination models, or final normalized field sets. Those choices belong in
future chain-family specs after the chain-family requirements are known.

Chain-neutral modules MAY express capability checks that allow the planner to understand
whether a chain adapter can satisfy a planned request, but those capabilities MUST avoid
encoding one chain family's schema as the universal model.

## Non-Goals

This document MUST NOT be used as authority for full Rust trait signatures.

This document MUST NOT be used as authority for final Parquet schemas.

This document MUST NOT be used as authority for final object key layouts.

This document MUST NOT be used as authority for implementing code.
