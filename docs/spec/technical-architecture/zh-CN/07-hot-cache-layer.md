# 07 - Reorg-Aware Hot Cache Layer

本阶段定义 safe/finalized height 之上的 hot cache 边界。durable cache 仍然是长期历史归档。
hot cache 是独立的、reorg-aware 的 layer，用于尚未到达 durable boundary 的 unsafe 或 latest 数据。

本文只定义架构和 contract。不要求实现具体 hot storage layout、完整 rollback algorithm、
promotion writer、durable Manifest schema 变更，或 Tron/Solana adapter。

## Durable And Hot Boundary

durable cache 只能包含 safe 或 finalized coverage。durable object chunk、Manifest coverage、
Manifest empty coverage 必须继续满足 durable write invariant：

```text
range.end <= adapter.safe_height().value
adapter.safe_height().finality in { Safe, Finalized }
```

hot cache data 是 non-durable query acceleration。它可以包含 unsafe、latest，或其他尚未 durable
的 range，但不能直接写 durable Manifest coverage 或 durable empty coverage。hot cache rows、
indexes、block metadata、rollback state 都必须与 durable object layout 和 Manifest truth 分离。

从 hot cache 到 durable cache 的 promotion 必须是显式流程。promotion 只能在相关 range 已经等于或低于
adapter safe/finalized boundary 后读取 hot data，然后必须经过正常 durable writer 和 Manifest update path。
promotion 不是允许 hot storage 直接修改 durable Manifest 的权限。

durable cache 不因为 hot reorg 做 rollback。如果 reorg 影响已经 promotion 的数据，这属于 durable
finality violation，必须作为 adapter 或 operator incident 处理，而不是普通 hot cache rollback。

## Query Flow

planner 必须按三个 boundary 分类请求 range：

- Durable boundary：最高 safe/finalized height，可由 durable path 读取或填充。
- Hot boundary：配置允许 hot layer 检查的 latest/unsafe window。
- Provider boundary：adapter/provider 报告的 latest canonical height。

当请求跨越多个 boundary 时，第一版 hot-capable planner 应将请求拆成有序 segment：

- `durable_read`：durable Manifest 已覆盖的 range。
- `durable_fetch`：位于 durable boundary 之内但缺失的 range；通过 durable writer fetch 并写入。
- `hot_read`：hot cache 已覆盖且根据 hot metadata 仍为 canonical 的 range。
- `live_fetch`：hot cache 未覆盖但位于 supported hot/latest window 内的 range。

当请求明确使用 latest-capable behavior 时，hot-capable API contract 允许 mixed response。response
可以组合 durable cache、hot cache、live provider segment，但每个 segment 都必须在 response metadata
中标记 source 和 finality。durable-only request 保持当前行为：range 超过 durable boundary 时返回稳定
invalid-input 或 unsupported error，不能静默变成 mixed hot response。

共享 response metadata vocabulary：

- `source = durable_cache`：rows 来自 durable object storage 和 Manifest coverage。
- `source = hot_cache`：rows 来自 hot cache。
- `source = live_provider`：rows 直接来自 adapter/provider fetch。
- `finality = finalized`：data 已由 adapter 判定 finalized。
- `finality = safe`：data 已由 adapter 判定 safe。
- `finality = unsafe`：data 低于 latest，但尚未 safe/finalized。
- `finality = latest`：data 位于 provider latest boundary。

对应 Rust contract 是 `QuerySegmentMetadata`，以及 `QuerySegmentSource` 和 `QueryDataFinality`。

## Reorg-Aware Requirements

每个 hot cache entry 都必须绑定该 chain-family range model 的 block metadata。对 block-height chain，
最小 metadata 为：

- `chain`：完整 `ChainIdentity`，包含 family、configured name，以及可用时的 network id。
- `range_kind`：EVM 使用 `block`。
- `height`。
- `hash`。
- `parent_hash`。
- `finality`。
- Dataset key、selector fingerprint、normalized range。

## Storage And Key Model

hot cache storage 使用与 durable cache storage 分离的 namespace：

```text
hot-cache/chains/<chain-kind>/<chain-name>/<network-id>/ranges/<range-kind>/<start>-<end>/datasets/<dataset-key>/<schema-version>/<selector-fingerprint>/height-<height>/<block-hash>/<block-hash>.rows.<encoding-extension>
hot-cache/chains/<chain-kind>/<chain-name>/<network-id>/ranges/<range-kind>/<start>-<end>/datasets/<dataset-key>/<schema-version>/<selector-fingerprint>/height-<height>/<block-hash>/<block-hash>.metadata.json
```

durable namespace 仍是 `chains/...`，durable Manifest 仍是 `chains/<chain>/manifest.json`。
hot cache write 不能写 durable Manifest、durable empty coverage 或 durable object key。hot layer 的 cleanup
或 rollback 只能删除 `hot-cache/` 下的 key。

hot key model 默认不包含 application identity。同一 chain identity、dataset key、selector fingerprint、
range kind、range、schema version、height 和 block hash 的 hot cache object 由所有 application 共享。
application identity 仍属于 API、quota、metrics 和 usage attribution boundary，除非后续设计明确加入
per-application hot cache isolation。

第一版 implementation 通过与 durable storage 相同的 `ObjectStore` interface 保存 hot rows 和 metadata。
local development 使用以 configured hot cache path 为 root 的独立 local store。S3-compatible deployment 使用同一
object-store contract，并配置 dedicated bucket 或类似 `hot-cache/` 的 prefix；required isolation 来自 key
prefix 和 Manifest rule，而不是 local-only filesystem assumption。

第一版 `schema-version` 是 `hot-cache-v1`。metadata 同时记录 object encoding，让未来 reader 可以拒绝
unsupported schema 或 encoding combination，而不是静默 decode incompatible objects。

## Metadata And Candidate Semantics

每个 `.metadata.json` sidecar 记录：

- Chain identity、dataset key、selector fingerprint、selector canonical key 和 range。
- Block hash、parent hash、height、observed Unix timestamp 和 source provider。
- Finality status：`finalized`、`safe`、`unsafe` 或 `latest`。
- Row count、object size、SHA-256 checksum、object key、metadata key、schema version 和 object encoding。
- Candidate status：`active`、`candidate` 或 `stale`。
- Optional active branch id。
- Promotion eligibility。
- Query segment metadata values：`source = hot_cache` 和 matching query finality。

同一 logical chain/dataset/selector/range/height 以不同 block hash 重复写入时，hot layer 保留多个
candidate。如果新写入标记为 `active`，同一 logical key 下先前的 active candidate 会被降级为
`candidate`；durable coverage 不会变化。这样 reorg detection 可以比较 candidate hashes，同时 read selection
保持 deterministic。

hot read 返回 rows 以及选择这些 rows 使用的 hot metadata。第一版 read selection 只返回 requested chain、
dataset、selector、range kind 和 height window 内的 active candidates。metadata 缺失、schema version
unsupported 或 checksum/size mismatch 时，read 必须以 storage read failure 失败，不能返回未标记的 stale rows。

## Retention Boundary

hot cache data 受显式 retention policy 限制。第一版 local backend 基于 `observed_at_unix_seconds` 做 age-based
cleanup，并支持保留 active candidates，即使较旧 candidate 被 prune。cleanup 只删除 expired hot entry 的 row
object 和 metadata sidecar。它绝不能删除 durable object、durable Manifest 或 usage ledger entry。

支持 hot cache 的 chain adapter 必须暴露 canonical block lookup。共享 contract 是
`canonical_block(CanonicalBlockRequest) -> CanonicalBlock`。不能回答 canonical hash query 的 adapter
必须返回 `UnsupportedDataset`，hot path 必须拒绝 latest-capable query，不能把它伪装成 hot cache miss。

最小 reorg detection signals：

- Parent mismatch：某个 block 的 `parent_hash` 与同一 chain identity 和 range kind 的前一个 stored hot
  block hash 不一致。
- Same height different hash：hot metadata 已经有 height `H` 和 hash `A`，但 provider canonical block
  在 `H` 的 hash 是 `B`。
- Provider canonical hash change：通过 `canonical_block` 重新检查时，先前观察到的 canonical hash 发生变化。

hot rollback 只限 unsafe data。hot layer 可以删除、替换或标记 stale 的未 safe/finalized hot entries。
它不能在正常 reorg handling 中删除 durable chunk、移除 durable Manifest entry，或 rewrite durable object storage。

rollback 之后，受影响的后续 query 必须重新 fetch canonical hot range，或返回稳定 reorg/unsupported error。
不能在没有 metadata 标记 reorg outcome 的情况下返回 stale rows。

## Multi-Chain And Application Context

hot cache state 必须按 `ChainIdentity` 隔离。configured name 相同但 family 或 network id 不同的两个 chain，
不得共享 hot entries、block metadata、rollback cursors、promotion state、metrics 或 usage ledger attribution。

hot layer 位于正常 application boundary 内。它不能绕过 API authentication、chain/dataset allowlists、
request range limits、quota validation 或 normalized application identity。hot hit、hot miss、live fetch、
reorg rollback、promotion outcome 都必须归因到发起请求或拥有 promotion 的 application context。

usage ledger outcomes 必须区分 durable 与 hot 行为。第一版共享 vocabulary 包含：

- Query outcomes：`hot_hit`、`hot_miss`、`mixed`、`reorg_rollback`、
  `promotion_completed`、`promotion_skipped`。
- Cache outcomes：`hot_hit`、`hot_miss`、`mixed`。
- Fill outcomes：`live_fetch`、`reorg_rollback`、`promotion_written`、
  `promotion_skipped`。

metrics 必须使用不同 outcome label 区分 durable cache、hot cache、live fetch、reorg rollback、promotion。
labels 必须继续使用 normalized application id、chain、chain kind、dataset。metrics labels 不能包含 selector
values、block hashes、raw credentials 或 untrusted headers。

## Unsupported First-Version Behavior

第一版 hot-capable implementation 必须对以下情况返回稳定 unsupported errors：

- adapter 不支持 `canonical_block`。
- adapter 无法确定 latest height。
- adapter 无法确定 durable safe/finalized boundary。
- 请求深度超过 configured hot reorg window。
- chain finality 无法表示为 `safe`、`finalized`、`unsafe` 或 `latest`。
- hot hit 缺少 reorg metadata。
- chain identity 模糊或未注册。
- Tron 或 Solana latest/hot cache behavior 在其 adapter 定义 canonicality contract 前被请求。

这些 error 不能被记录成 durable cache miss。durable cache miss 表示 safe/finalized durable coverage 缺失，
并且可以通过 durable writer 填充。unsupported hot behavior 表示 datalens 无法安全服务 latest-capable request。

## First Implementation Scope

基于本架构的第一版 implementation 应只加入最小 hot-aware runtime path：

- planner segmentation：durable、hot、live provider ranges。
- hot cache interface traits 或等价 module boundary。
- 支持能力足够的 EVM provider 的 adapter canonical block lookup。
- API 和 SDK model 的 response segment metadata。
- hot outcomes 的 metrics 和 usage ledger recording。
- 对不满足 hot requirements 的 adapter 或 chain 返回 unsupported errors。

storage layout、完整 rollback replacement、promotion scheduling、durable promotion writes 应保留给后续 issue，
除非具体 implementation issue 明确包含这些内容。
