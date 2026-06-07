# 06 - API、SDK 和兼容层

这一步决定用户如何使用 datalens，同时避免任何单一外部协议控制内部架构。

## 原生服务 API

原生 edge 查询是产品契约。它暴露 datalens 自己的概念，而不是复制某个网关的 schema：

- `chain`：`ChainIdentity`，包含链家族、配置中的链名称，以及可选 network id。
- `dataset_key`：带链家族前缀的原生数据集键，例如 `evm.blocks`、`solana.slots`
  或 `tron.blocks`。
- `selector`：数据集选择器，例如 `all`、`evm_logs`，或适配器原生 selector。
- `range`：带类型的账本范围，例如 block、slot 或 height，起止点都包含在内。
- `finality`：`QueryFinalityRequirement`。
- `fields`：`all` 或 include 列表。
- 可选的缓存命中、部分命中或补齐行为元数据。

原生 API 应该能干净映射到规划器。API handlers 不应该需要知道对象存储细节或 EVM RPC
细节。

## Application 边界

当配置中 `applications.required = true` 时，原生 HTTP API 必须在查询执行前完成
application 认证。

- Application identity 通过 `x-datalens-application` 传递。
- 凭据通过 `Authorization: Bearer <token>` 传递。
- 配置中的 application id/name 会规范化为 lowercase ASCII，只允许 letters、digits、dot、
  underscore 和 hyphen。
- 缺失、未知、无效或禁用的 application 必须在 provider fetch 或 durable cache write 前失败。
- Application allowlist 是第一版授权边界：每个 application 声明允许访问的 `chains` 和
  `datasets`。
- Quota 配置是第一版 request validation。`max_query_range_blocks` 在执行前强制检查；
  `max_requests_per_minute` 和 `max_concurrent_requests` 先作为 registry 边界解析，后续再接入
  runtime limiting。

Metrics labels 必须使用 registry 规范化后的 application id，不能使用原始 header 值。
Authentication token 不能出现在日志或 API error response 中。

## HTTP Contract

Edge 通过 REST 和 GraphQL 暴露同一份原生查询能力。REST 使用 `POST /v1/query`。GraphQL 在
`POST /native/graphql` 上使用 `query(input:)`。两种入口必须使用相同的 application identity、相同的
原生请求校验、相同的 `NativeQueryInput` 执行路径，并返回相同的 cache 和 row 语义。GraphQL
可以为了客户端易用性包装 JSON scalar，但不能引入独立的 planner、storage contract 或 dataset
词汇。

`POST /v1/query` 请求字段：

- `chain`：`ChainIdentity`。
  - `family` 是链家族，例如 `Evm`。
  - `configured_name` 是服务配置中的链名称，例如 `ethereum`。
  - `network_id` 在可用时标识上游网络。
- `dataset_key`：`family.name` 形式的原生数据集键。
- `selector`：数据集选择器。
  - `{ "kind": "all" }` 选择整段范围数据集。
  - `{ "kind": "evm_logs", "value": { ... } }` 用 address/topic filter 选择 EVM logs。
  - `{ "kind": "other", "value": { "kind": "...", "fingerprint": "...",
    "canonical_key": "..." } }` 承载适配器原生 selector，不改变核心契约。
- `range`：带类型的账本范围，例如 `{ "kind": "block", "start": 1, "end": 2 }`、
  `{ "kind": "slot", "start": 1, "end": 2 }` 或
  `{ "kind": "height", "start": 1, "end": 2 }`。
- `finality`：请求的 `QueryFinalityRequirement`，默认是 `durable_only`。
  - `durable_only`：只查询 durable safe/finalized cache 和 durable provider fill。
  - `safe_to_latest`：调用方接受 durable、hot 和 provider latest segment 混合返回。
  - `latest_only`：调用方只接受 latest-capable hot/provider 行为。
- `fields`：请求的 `FieldSelection`，默认是 `all`；include 列表使用
  `{ "include": ["field_name"] }`。

Hot/latest behavior 不能隐式启用，也没有独立的传输层开关。`QueryFinalityRequirement` 就是
边界：`durable_only` 保持在 durable safe/finalized path；`safe_to_latest` 把 safe/finalized
durable coverage 和 adapter 可服务的 latest-capable tail 拆开；`latest_only` 使用 live provider
read-through path，不读取也不写入 durable cache。如果 adapter 无法安全服务请求的 hot/latest
contract，server 必须在 durable cache write 前返回 `unsupported_hot_query`。

EVM logs selector value 字段：

- `addresses`：空列表表示任意 address。
- `topics`：按位置排列的 topic 条件。
- `null` topic position 表示 wildcard。
- 某个位置的空 topic value set 表示该位置不匹配任何 topic。
- 非空 topic value set 表示该位置匹配其中任意一个 topic。

`POST /v1/query` 响应字段：

- `chain`：解析后的 `ChainIdentity`。
- `dataset_key`：原生数据集键。
- `range`：请求的账本范围。
- `cache.hit_ranges`：由 durable cache 服务的范围。
- `cache.missing_ranges`：未由 durable cache 服务的范围。
- `cache.durable_hit_ranges`：由 durable cache 命中的范围。
- `cache.hot_hit_ranges`：由 hot cache 命中的范围。
- `cache.provider_fill_ranges`：本次响应中由 provider fetch 返回的范围。
- `cache.promotion_pending_ranges`：尚未提升为 durable cache 的 hot/provider 范围。
- `cache.segments[]`：有序响应片段，包含 `range`、`source` 和 `finality`。
- `cache.segments[].source`：`durable`、`hot` 或 `provider`。
- `cache.segments[].finality`：`finalized`、`safe`、`unsafe` 或 `latest`。
- `rows`：`DatasetRows`，按数据集顺序排序，并在响应前去重。
- 空结果用所选数据集下的空 `rows` 数组表示。

Safe-default client 只能把 `finalized` 或 `safe` 数据写入最终业务表，并且只能基于 finalized 或
safe coverage 推进最终业务 cursor。Latest、unsafe、hot 或其他 provisional segment 只能进入
application 自己的 provisional state，除非 application 已证明它们匹配 canonical safe 或 finalized
coverage。

响应 metadata 必须区分 durable cache、hot cache 和 live provider segment，并标记
`finalized`、`safe`、`unsafe` 或 `latest` finality。调用方必须基于稳定的 `error.kind` 分支，而
不是解析 `error.message`。

## SDK 角色

SDK 是便捷层。它可以提供类型化请求、分页帮助方法、重试帮助方法、认证帮助方法，以及给
索引器作者使用的集成工具。

SDK 不一定要拥有完整索引运行时。datalens 本身已经是一个能回答历史结构化查询的服务。未来
SDK 可以在两种形态之间选择：

- 仅服务客户端：SDK 只和 datalens 通信，直接 RPC 索引交给用户自己的工具。
- 混合帮助层：当 datalens 未配置或请求超出 datalens 范围时，SDK 可以选择回退到直接 RPC。

这个决策应根据真实集成需要后续再做。架构只要求直接 RPC 索引仍然是合法工作流。

Rust client 可以发送 service-side hot/latest contract fields。它不实现 client-side RPC fallback。
`FallbackMode::Rpc` 仍返回 `UnsupportedFallback`，并且不能写 durable cache。Service-side hot/latest
read-through 和未来任何 client-side RPC fallback 都必须与 durable safe/finalized cache write 隔离。

外部 Rust SDK 是 `sdks/rust` 下的 `datalens-sdk` package。它只能作为 GraphQL/HTTP
client，不能依赖任何 `crates/` 下的 workspace crate，包括 `datalens-core`。

`crates/client` 下的内部 REST client package 是 `datalens-internal-client`。它只供
workspace crate 实现使用，不是外部 SDK package。

默认 SDK 行为是 service-client only，并使用 edge 暴露的 native GraphQL request shape。
`DatalensClient::native().query(QueryInput)` 是第一类 native Rust SDK API：

- SDK 自有 input struct 镜像 checked-in `schemas/native.graphql` SDL。
- `QueryResponse` 为 chain、range、cache 和 rows 保留 GraphQL JSON scalar，不暴露内部
  server/runtime model。
- `solana.slots`、`tron.blocks` 等非 EVM dataset 通过同一个原生方法查询。
- 缺失 `QueryInput.finality` 时默认使用 `durable_only`。
- `query` 会拒绝 `latest_only` 和 `safe_to_latest`；需要 hot/latest 数据的调用方必须显式使用
  `query_provisional`。
- REST `query` 在缺失 explicit finality 时会先解析 finalized head；如果 finalized head 不可用，
  再回退到 safe head，然后才发送 durable query。
- `query_provisional` 要求 explicit `latest_only` 或 `safe_to_latest` finality，并且不能用于推进
  最终业务 cursor。

Rust SDK safety helpers 是公开的集成工具：

- `DataFinality` 把 `finalized` 和 `safe` 归类为 durable，把 `latest` 或 provisional 值归类为
  provisional。
- `BlockAnchor`、`FinalizedCursor` 和 `ProvisionalCursor` 帮助 application 分离 durable 与
  provisional checkpoint state。`FinalizedCursor` 必须拒绝 non-durable finality、unknown
  finality、range-kind mismatch 和 height regression。
- `extract_cache_segments` 读取 response segment 的 range、source、finality，以及可用的 anchor
  metadata，方便 application 持久化后续 reconciliation 所需证据。
- `plan_promotion` 返回 `Promote`、`KeepProvisional`、`Recheck` 或 `Rollback`。如果没有由
  durable safe/finalized anchor coverage 表示的 canonical proof，latest 或 provisional range 必须
  recheck，不能 promote。

SDK 也通过 `DatalensClient::index()` 暴露 checked-in `schemas/index.graphql` SDL 的 wrapper：

- Raw indexed events 使用 `events` 和 `eventsConnection`。
- Decoded indexed events 使用 `decodedEvents` 和 `decodedEventsConnection`。
- Cursor pagination 暴露 `pageInfo`、`edges` 和 `nodes`。
- SDK 不能直接调用 executor、storage、writer、chain adapter 或 RPC provider。

## 未来兼容适配面

兼容适配层是未来的 edge adapter，不是当前 API 架构。后续可以把外部协议请求转换成
datalens 原生请求，再在 edge 把原生响应映射成外部协议要求的响应形态。

兼容层不能定义：

- 核心数据集名称。
- Manifest 覆盖范围语义。
- 分片存储格式。
- 链适配器接口。
- 规划器行为。
- 当前 SDK 行为。

这样 datalens 才能服务多种调用方，而不用把同一份历史数据存成多种调用方专属格式。

## 这一步要实现什么

第一版 API 实现应包含：

- 原生查询 endpoint。
- Height 或 status endpoint。
- 大结果的流式响应。
- 输入无效、不支持的数据集、provider 失败和存储失败的清晰错误响应。
- 只有在原生 API 稳定后才增加小型 SDK 或客户端示例。

兼容层不应该最先做。它应该在原生请求、规划器、存储和 EVM 补齐路径被证明可行之后再做。
