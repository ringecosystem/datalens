# Chain-Cache Architecture

Purpose: 定义 query-driven structured archive design 的 chain-cache module boundary
contract 与 canonical data flow。

Status: normative

Read this when: 当你要创建 workspace crates、分配 module ownership，或检查 chain-cache
design 是否把 chain-neutral 与 chain-family-specific concerns 放在正确层级时阅读本文件。

Not this document: 本文件不定义 Rust trait signatures、final Parquet schemas、object
key layouts、service deployment topology 或 per-chain data schemas。

Defines:

- Canonical chain-cache request and fill data flow。
- Canonical module names and responsibilities。
- Chain-neutral and chain-family-specific module boundaries。
- Compatibility layer placement rules。
- Extension rules for future Tron and Solana support。

Paired translation: `../en/architecture.md`

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

即使 implementation 为了降低 latency 而 pipeline reads、fetches、writes 与 streaming，
request path 也 MUST preserve this ordering。

Query planner MUST 在选择 storage 或 chain-family code 之前，把 SDK/API request
转换成 chain-neutral plan。

Coverage/manifest lookup MUST 判断 requested ranges 中哪些已经 durable in object
storage，以及哪些 ranges missing。

Object storage reads MUST 通过 storage boundary 提供 cached ranges，planner 或 API
code 不得直接读取 chain-family objects。

Missing range resolver MUST 根据 coverage/manifest results 推导完成 planned request
所需的最小 fetch work。

Chain adapter fetches MUST 只针对 missing ranges 调用，并且 MUST 通过 chain adapter
boundary 进入。

Chunk writer MUST 在 fetched normalized chunks 被视为 covered durable data 之前持久化
这些 chunks。

Response stream MAY 组合 cached chunks 与 newly fetched chunks，但它 MUST 暴露 planned
response shape，而不是 adapter-native payloads。

## Module Boundary Table

| Module | Boundary type | Responsibilities | Must not own |
| --- | --- | --- | --- |
| `datalens-core` | Chain-neutral | Shared domain vocabulary、request range concepts、normalized result envelopes、error categories，以及 cross-module invariants。 | Chain-family RPC clients、object storage drivers、query planning policy、write orchestration、API compatibility behavior。 |
| `datalens-chain` | Chain-neutral adapter contract | Chain adapter interfaces at the conceptual boundary、chain family identifiers、adapter capability metadata，以及 normalized fetch result expectations。 | EVM-specific RPC semantics、final schemas、storage layout、API request parsing。 |
| `datalens-evm` | Chain-family-specific | EVM chain adapter implementation、EVM RPC/provider integration，以及在通过 `datalens-chain` 返回之前的 EVM-specific normalization。 | Chain-neutral planning policy、object storage persistence policy、SDK/API compatibility layers。 |
| `datalens-storage` | Chain-neutral | Coverage/manifest lookup、object storage read/write primitives、durable chunk existence checks、storage error mapping，以及 manifest consistency requirements。 | Chain-family RPC fetches、query request interpretation、final Parquet schema authority、API response compatibility。 |
| `datalens-planner` | Chain-neutral | Query planning、request normalization into executable plans、requested range decomposition，以及针对 chain-neutral capabilities 的 plan validation。 | Object storage drivers、chain-family RPC details、chunk serialization、API transport concerns。 |
| `datalens-writer` | Chain-neutral | Chunk write orchestration、idempotent write behavior、manifest update sequencing，以及在 storage succeeds 后将 fetched chunks 标记为 durable。 | Chain adapter RPC logic、SDK/API request parsing、final object key layout authority。 |
| `datalens-api` | Edge-facing | SDK/API request parsing、transport-level validation、authentication/authorization integration points、response streaming，以及 external callers 的 compatibility adapters。 | Core architecture rules、chain-family normalization、object storage internals、planner internals。 |

## Chain Neutrality Contract

`datalens-core`、`datalens-chain`、`datalens-storage`、`datalens-planner` 与
`datalens-writer` MUST remain chain-neutral。

`datalens-api` MUST keep its internal orchestration chain-neutral，但 MAY contain
edge-facing compatibility adapters for specific SDK/API caller expectations。

`datalens-evm` 是 chain-family-specific，并且 MUST 把 EVM-specific RPC semantics、
normalization rules、provider behavior 与 chain-family capability handling 留在
chain-neutral modules 之外。

Future chain-family modules MUST follow the same role as `datalens-evm`：实现 chain
adapter boundary，并在不改变 chain-neutral module contract 的前提下返回 normalized
results。

## Compatibility Layer Rules

Compatibility layers MUST sit at the system edge，主要位于 `datalens-api` 或
caller-specific SDK surfaces。

Compatibility layers MUST NOT shape `datalens-core`、`datalens-planner`、
`datalens-storage`、`datalens-writer` 或 `datalens-chain` 的 chain-neutral parts。

Compatibility behavior includes legacy request names、SDK-specific response aliases、
transport-specific pagination conventions、caller-specific defaults，以及其他 external
contract accommodations。

Core architecture MUST 使用 stable chain-cache concepts，而不是 compatibility concepts，
因为 core modules 必须服务 multiple callers 与 multiple chain families。

当 compatibility behavior 需要转换 caller request 时，translation MUST happen before
the query planner receives the request。当 compatibility behavior 需要 renaming 或
reshaping a response 时，reshaping MUST happen after the response stream has the planned
chain-cache result shape。

## Future Chain Families

The architecture MUST remain open for Tron and Solana，方式是把 chain families 视为
`datalens-chain` 后面的 adapter implementations。

Adding Tron or Solana MUST NOT require renaming the canonical modules、changing the
canonical data flow，或在 chain-neutral modules 中嵌入 Tron/Solana schema assumptions。

Tron and Solana support MAY introduce future chain-family-specific modules，例如
`datalens-tron` 或 `datalens-solana`，当它们的 adapter implementations 被设计时。

This architecture deliberately does not commit to Tron or Solana schemas、object key
layouts、RPC pagination models，或 final normalized field sets。这些选择属于未来
chain-family specs，应在 chain-family requirements 已知之后定义。

Chain-neutral modules MAY express capability checks，使 planner 能理解 chain adapter
是否可以满足 planned request，但这些 capabilities MUST avoid encoding one chain
family's schema as the universal model。

## Non-Goals

This document MUST NOT be used as authority for full Rust trait signatures。

This document MUST NOT be used as authority for final Parquet schemas。

This document MUST NOT be used as authority for final object key layouts。

This document MUST NOT be used as authority for implementing code。
