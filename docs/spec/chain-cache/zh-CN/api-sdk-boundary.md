# API, SDK, And Compatibility Boundary

Purpose: 定义 external consumers 如何与 datalens 交互，同时避免 SDK 或 compatibility
concerns 塑造 core archive design。

Status: normative

Read this when: 当你要设计 native service API scope、SDK responsibilities、indexer
integration、RPC fallback behavior 或 compatibility adapters 时阅读本文件。

Not this document: 本文件不定义 exact HTTP endpoint schemas、SDK method signatures、
compatibility endpoint schemas、authentication flows、deployment topology 或
implementation code。

Defines:

- Native service API authority for query-driven structured historical cache access。
- SDK responsibility boundaries。
- Direct RPC indexing relationship to datalens。
- Indexer-facing service sufficiency expectations。
- Compatibility adapter placement rules。
- Open decisions for future API and SDK implementation issues。

Paired translation: `../en/api-sdk-boundary.md`

## Interface Authority

Native datalens service API MUST be the primary external interface for query-driven
structured historical cache access。

Native service API MUST 用 datalens terms 暴露 datalens behavior：chain identity、
structured dataset identity、bounded ranges、field requirements、coverage、cache fill
behavior、finalized/safe height policy，以及 planned response shape。

Native service API MUST 通过 `query-cache-behavior.md` 定义的 chain-cache query path
路由 requests：plan、inspect coverage、read covered objects、resolve missing ranges、
fetch missing data through chain adapters、write structured chunks、update coverage，并
stream or return the planned response。

Native service API MUST NOT 被视为 one SDK runtime、one compatibility protocol 或 one
external indexer framework 的 thin wrapper。

Native service API MAY 通过 HTTP、IPC、RPC-style service calls 或其他 transport 暴露。
Transport selection MUST NOT 改变 native behavior contract。

## SDK Boundary

SDK MUST be optional developer convenience over the native datalens service API。

Consumers MUST 能够通过 native service API 使用 datalens，而不需要安装或嵌入 datalens
SDK。

SDK MAY 提供 typed clients、request builders、pagination helpers、streaming helpers、
retry wrappers、authentication helpers、local validation，以及 language-specific
ergonomics。

SDK 在包装 service behavior 时 MUST preserve native datalens semantics。它 MUST NOT 把
cache coverage、missing range resolution、finalized/safe height behavior 或 structured
dataset identity 重新定义为 SDK-only concepts。

SDK MAY contain compatibility conveniences for developers，但这些 conveniences MUST
translate into native service requests before reaching the query planner。

SDK does not necessarily need to own a full indexing runtime。当 datalens service
interface 足以支持 indexer usage 时，full indexing runtime MAY be unnecessary。

## Direct RPC Indexing

Developers MAY still index directly from chain RPC providers without using datalens。

datalens MUST NOT 被规定为 build an indexer 或 access chain history 的唯一有效方式。

Direct RPC indexing 位于 datalens compatibility contract 之外。datalens 不保证 direct
RPC indexers 会观察到与 datalens service consumers 相同的 latency、coverage、
normalization、retry 或 finalized/safe height behavior。

Direct RPC indexing 的存在 MUST NOT force datalens storage or query schemas to mirror
raw RPC payloads。

## Indexer-Facing Service Sufficiency

datalens MAY provide a full service interface sufficient for indexer usage。

当 service 暴露所需 datasets、ranges、ordering、field selection、coverage 与
finalized/safe height behavior 时，indexer SHOULD be able to use the native service API
as its chain-history source。

When the native service API is sufficient for indexer usage，SDK SHOULD remain a
convenience client，而不是 indexing orchestration 的 mandatory owner。

Future implementations MAY still provide SDK-side indexing helpers，但这些 helpers MUST
be clients of native service behavior，除非 future spec explicitly assigns a broader
runtime role to the SDK。

## Compatibility Layers

Compatibility layers MUST be edge adapters over native datalens behavior。

Compatibility layers MAY include SQD Gateway compatibility、legacy request names、
external framework response shapes、SDK-specific aliases、pagination translations、
transport-specific defaults，或 migration helpers。

SQD Gateway compatibility MUST NOT 被本文件视为 phase-1 requirement。

Compatibility adapters MUST translate external requests into native datalens requests
before the query planner receives them。

Compatibility adapters MUST reshape native datalens responses only after the response has
the planned native result shape。

Compatibility adapters MUST NOT bypass native coverage、storage、missing range、writer 或
chain adapter boundaries。

## Storage And Query Schema Boundary

Compatibility schemas MUST NOT become the core storage schema。

Compatibility schemas MUST NOT become the core query schema。

Core storage and query schemas MUST be based on stable datalens structured cache
concepts，而不是 one external compatibility protocol。

Compatibility field names、pagination models、response envelopes 与 legacy payload
shapes MAY exist at the edge，但它们 MUST NOT define durable chunk schemas、manifest
coverage authority、planner vocabulary 或 chain-neutral module contracts。

如果 compatibility adapter 需要 native structured datasets 中不存在的 fields，future
implementation issues MUST decide whether to extend native datasets、derive the fields at
the edge，或 reject the compatibility behavior。Adapter MUST NOT silently introduce a
separate durable compatibility cache as the authoritative store。

## Open Decisions

Future API and SDK implementation issues MUST resolve at least these decisions before
implementation:

- Whether the SDK talks only to the datalens service or also owns fallback-to-RPC
  behavior。
- If fallback-to-RPC exists, whether fallback results may be written back through
  datalens or remain caller-local。
- Which transports are phase-1 native service API targets, such as HTTP, IPC, or
  RPC-style service calls。
- Which request and response schemas belong to the native service API。
- Which SDK languages are phase-1 targets。
- Which SDK helpers are convenience-only and which, if any, are required for supported
  indexer workflows。
- Whether any indexer runtime responsibilities belong in the SDK, a separate tool, or the
  datalens service。
- Whether SQD Gateway compatibility is required in a later phase and what subset of its
  behavior should be adapted。
- How native datalens errors map into SDK exceptions, service status codes, and
  compatibility adapter errors。
- How authentication, authorization, rate limits, and multi-tenant controls are exposed
  without changing core storage/query semantics。

## Non-Goals

This document MUST NOT define exact HTTP endpoint schemas。

This document MUST NOT define SDK method signatures。

This document MUST NOT implement SDK behavior or compatibility endpoints。

This document MUST NOT make SQD Gateway compatibility a phase-1 requirement。

This document MUST NOT define final durable storage schemas or final query planner
schemas。
