# API, SDK, And Compatibility Boundary

Purpose: Define how external consumers interact with datalens without allowing SDK or
compatibility concerns to shape the core archive design.

Status: normative

Read this when: You are designing native service API scope, SDK responsibilities,
indexer integration, RPC fallback behavior, or compatibility adapters.

Not this document: This document does not define exact HTTP endpoint schemas, SDK method
signatures, compatibility endpoint schemas, authentication flows, deployment topology, or
implementation code.

Defines:

- Native service API authority for query-driven structured historical cache access.
- SDK responsibility boundaries.
- Direct RPC indexing relationship to datalens.
- Indexer-facing service sufficiency expectations.
- Compatibility adapter placement rules.
- Open decisions for future API and SDK implementation issues.

Paired translation: `../zh-CN/api-sdk-boundary.md`

## Interface Authority

The native datalens service API MUST be the primary external interface for query-driven
structured historical cache access.

The native service API MUST expose datalens behavior in datalens terms: chain identity,
structured dataset identity, bounded ranges, field requirements, coverage, cache fill
behavior, finalized/safe height policy, and planned response shape.

The native service API MUST route requests through the chain-cache query path defined by
`query-cache-behavior.md`: plan, inspect coverage, read covered objects, resolve missing
ranges, fetch missing data through chain adapters, write structured chunks, update
coverage, and stream or return the planned response.

The native service API MUST NOT be treated as a thin wrapper around one SDK runtime, one
compatibility protocol, or one external indexer framework.

The native service API MAY be exposed over HTTP, IPC, RPC-style service calls, or another
transport. Transport selection MUST NOT change the native behavior contract.

## SDK Boundary

The SDK MUST be optional developer convenience over the native datalens service API.

Consumers MUST be able to use datalens through the native service API without installing
or embedding a datalens SDK.

The SDK MAY provide typed clients, request builders, pagination helpers, streaming
helpers, retry wrappers, authentication helpers, local validation, and language-specific
ergonomics.

The SDK MUST preserve native datalens semantics when it wraps service behavior. It MUST
NOT redefine cache coverage, missing range resolution, finalized/safe height behavior, or
structured dataset identity as SDK-only concepts.

The SDK MAY contain compatibility conveniences for developers, but those conveniences
MUST translate into native service requests before reaching the query planner.

The SDK does not necessarily need to own a full indexing runtime. A full indexing runtime
MAY be unnecessary when the datalens service interface is sufficient for indexer usage.

## Direct RPC Indexing

Developers MAY still index directly from chain RPC providers without using datalens.

datalens MUST NOT be specified as the only valid way to build an indexer or access chain
history.

Direct RPC indexing is outside the datalens compatibility contract. datalens does not
guarantee that direct RPC indexers observe the same latency, coverage, normalization,
retry, or finalized/safe height behavior as datalens service consumers.

The existence of direct RPC indexing MUST NOT force datalens storage or query schemas to
mirror raw RPC payloads.

## Indexer-Facing Service Sufficiency

datalens MAY provide a full service interface sufficient for indexer usage.

An indexer SHOULD be able to use the native service API as its chain-history source when
the service exposes the required datasets, ranges, ordering, field selection, coverage,
and finalized/safe height behavior.

When the native service API is sufficient for indexer usage, the SDK SHOULD remain a
convenience client rather than the mandatory owner of indexing orchestration.

Future implementations MAY still provide SDK-side indexing helpers, but those helpers
MUST be clients of native service behavior unless a future spec explicitly assigns a
broader runtime role to the SDK.

## Compatibility Layers

Compatibility layers MUST be edge adapters over native datalens behavior.

Compatibility layers MAY include SQD Gateway compatibility, legacy request names,
external framework response shapes, SDK-specific aliases, pagination translations,
transport-specific defaults, or migration helpers.

SQD Gateway compatibility MUST NOT be treated as a phase-1 requirement by this document.

Compatibility adapters MUST translate external requests into native datalens requests
before the query planner receives them.

Compatibility adapters MUST reshape native datalens responses only after the response has
the planned native result shape.

Compatibility adapters MUST NOT bypass native coverage, storage, missing range, writer,
or chain adapter boundaries.

## Storage And Query Schema Boundary

Compatibility schemas MUST NOT become the core storage schema.

Compatibility schemas MUST NOT become the core query schema.

Core storage and query schemas MUST be based on stable datalens structured cache
concepts, not on one external compatibility protocol.

Compatibility field names, pagination models, response envelopes, and legacy payload
shapes MAY exist at the edge, but they MUST NOT define durable chunk schemas, manifest
coverage authority, planner vocabulary, or chain-neutral module contracts.

If a compatibility adapter needs fields that are not present in native structured
datasets, future implementation issues MUST decide whether to extend native datasets,
derive the fields at the edge, or reject the compatibility behavior. The adapter MUST NOT
silently introduce a separate durable compatibility cache as the authoritative store.

## Open Decisions

Future API and SDK implementation issues MUST resolve at least these decisions before
implementation:

- Whether the SDK talks only to the datalens service or also owns fallback-to-RPC
  behavior.
- If fallback-to-RPC exists, whether fallback results may be written back through
  datalens or remain caller-local.
- Which transports are phase-1 native service API targets, such as HTTP, IPC, or
  RPC-style service calls.
- Which request and response schemas belong to the native service API.
- Which SDK languages are phase-1 targets.
- Which SDK helpers are convenience-only and which, if any, are required for supported
  indexer workflows.
- Whether any indexer runtime responsibilities belong in the SDK, a separate tool, or the
  datalens service.
- Whether SQD Gateway compatibility is required in a later phase and what subset of its
  behavior should be adapted.
- How native datalens errors map into SDK exceptions, service status codes, and
  compatibility adapter errors.
- How authentication, authorization, rate limits, and multi-tenant controls are exposed
  without changing core storage/query semantics.

## Non-Goals

This document MUST NOT define exact HTTP endpoint schemas.

This document MUST NOT define SDK method signatures.

This document MUST NOT implement SDK behavior or compatibility endpoints.

This document MUST NOT make SQD Gateway compatibility a phase-1 requirement.

This document MUST NOT define final durable storage schemas or final query planner
schemas.
