# Query-Cache Behavior

Purpose: 定义 datalens chain-cache query execution 的 normative query lifecycle、
cache-hit behavior、cache-miss fill behavior 与 correctness expectations。

Status: normative

Read this when: 当你要设计或审查 query planner、cache coverage、missing range
resolution、writer 或 response streaming behavior 时阅读本文件。

Not this document: 本文件不定义 API endpoint names、chain-specific fetch algorithms、
final object key layouts、final chunk schemas 或 implementation code。

Defines:

- Query-driven request lifecycle。
- Cache hit、partial hit 与 miss behavior。
- Field selection 与 selective dataset storage rules。
- Missing range materialization behavior。
- Coverage、retries、limits、concurrency、streaming、safe heights 与 reorg posture 的
  correctness expectations。

Paired translation: `../en/query-cache-behavior.md`

## Query-Driven Cache Model

datalens chain-cache MUST cache structured chain data，这些数据来自 normalized dataset
requirements。它 MUST NOT 把 arbitrary raw SDK response blobs、raw RPC responses 或
adapter-native payloads 作为 durable cache authority。

Query path MUST be demand-driven。一个 query MAY 触发 missing historical data 被
fetched、normalized、写入为 structured chunks，并在 response 完成前被标记为 covered。

某个 dataset 与 range 的 first query MAY 比后续 equivalent queries 更慢，因为 first
query 会 materialize missing history。后续 equivalent queries SHOULD 读取 durable
structured chunks，而不是再次 fetch 同一个 source range。

当 already covered structured chunks 满足 planned dataset、range、chain、
finalized/safe height 与 field requirements 时，cache MUST 可被 compatible callers
复用。

## Request Lifecycle

Canonical query lifecycle 是：

1. Accept query。
2. Plan required datasets and bounded ranges。
3. Inspect manifest coverage。
4. Read covered objects。
5. Resolve missing ranges。
6. Fetch missing ranges from the chain adapter。
7. Write fetched normalized data as structured chunks。
8. Update manifest coverage after durable chunk writes succeed。
9. Stream or return the planned response。

Implementation MAY 为了 pipeline reads、fetches、writes 与 response streaming 而并行
执行部分工作，但 observable behavior MUST preserve this lifecycle ordering：

- Query MUST 在 coverage 被信任之前完成 planning。
- Covered objects MUST 通过 storage boundary 读取。
- Missing ranges MUST 从 manifest coverage 与 planned requirements 推导。
- Chain adapter fetches MUST 限制在 planned response 所需的 missing ranges。
- Fetched chunk MUST NOT 在 structured chunk write succeeds 之前扩大 durable
  coverage。
- Response MUST 暴露 planned result shape，而不是 adapter-native payloads。

## Cache Hit States

当 manifest coverage 证明 planned query 所需的每一个 dataset 与 range，都已经在 required
finalized/safe height 与 field coverage level 下 durable 时，即为 cache hit。

On a cache hit:

- Query MUST 从 storage 读取 covered structured chunks。
- Query MUST NOT 从 chain adapter fetch equivalent data。
- Response MAY 从 stored chunks stream。
- Response ordering、filtering 与 projection MUST match planned query semantics。

当 manifest coverage 只满足 planned query 的一部分时，即为 partial hit。

On a partial hit:

- Covered ranges MUST 从 storage 读取。
- Missing ranges MUST 通过 chain adapter resolve and fetch。
- Newly fetched data MUST 被 normalized 并写入为 structured chunks，之后才可以被标记为
  covered。
- Response MAY 组合 stored chunks 与 newly fetched chunks。
- 除 latency 与 streaming timing 之外，combined response MUST 与 complete durable cache
  产生的 response indistinguishable。

当 planned query 所需的任何 dataset/range segment 都没有 coverage 时，即为 cache miss。

On a cache miss:

- Missing range resolver MUST 从 planned query 推导 required fetch work。
- Chain adapter MUST 只 fetch plan 所需的 bounded ranges 与 datasets。
- Writer MUST persist normalized structured chunks，并在 durable writes succeed 之后更新
  manifest coverage。
- Response MAY 只有在 streamed items 有 defined source 时才开始 streaming：durable
  storage，或属于 planned result 的 newly fetched normalized chunks。

## Field Selection And Dataset Requirements

Query planner MUST 把 caller field selection 转换成 explicit dataset and field coverage
requirements。

Field selection MAY reduce response projection。Field selection MUST NOT 使系统把
arbitrary caller-shaped response blobs 存为 cache artifacts。

当 selected field 属于已经以 sufficient field coverage 缓存的 structured dataset 时，
response SHOULD 从 cached structured chunk 做 projection。

当 selected field 需要尚未 covered 的 dataset 或 field group 时，missing range resolver
MUST 把该 dataset 或 field group 纳入 fetch plan。

当 superset 是该 dataset 的 canonical structured chunk shape 时，implementation MAY
存储 selected fields 的 superset。Implementation MUST NOT 对未 durably written 的 fields
或 datasets 宣称 coverage。

Selective dataset requirements MUST avoid forcing unrelated datasets to be fetched。例如，
需要 logs 的 query MUST NOT fetch transaction receipts、traces 或 state items，除非
planned response semantics 需要这些 datasets。

## Coverage And Manifest Expectations

对于相同的 chain、dataset、normalized range、finalized/safe height policy 与 field
coverage policy，manifest coverage MUST be deterministic。

Coverage entries MUST 至少区分：

- Chain or chain family identity。
- Structured dataset identity。
- Covered range。
- Field coverage level or canonical chunk shape。
- Finalized/safe height basis。
- Durable chunk identity or storage reference。

Manifest updates MUST 在 structured chunks 被 durably written 之后发生。Failed write
MUST NOT create coverage。Failed manifest update MAY 留下没有被 coverage 引用的 durable
chunk，但 retry behavior MUST 能够 detect or overwrite 同一个 logical chunk，且不 corrupt
coverage。

Retries MUST be idempotent。对同一个 logical chunk 重复相同的 fetch/write/manifest
update，MUST converge on the same durable coverage state，或 fail without creating
contradictory coverage。

## Bounds, Concurrency, And Streaming

Every planned query MUST 在 storage lookup 或 adapter fetch 开始之前拥有 bounded ranges。
Planner MUST 在 unbounded work 到达 chain adapter 之前 reject or split 它。

Implementations MUST enforce concurrency limits for missing range fetches and writes。
Concurrency limits MAY 按 deployment、chain family、provider capability 或 storage backend
配置，但 query execution MUST NOT 允许 unbounded adapter calls 或 unbounded writer
fan-out。

Large result sets SHOULD be streamed。Streaming MUST preserve planned response order，并且
MUST NOT 暴露可能因为 failed required write 或 manifest update 而随后 rolled back 的 chunks，
除非 response 明确把这些 chunks 视为 non-durable in-flight results。

当 query 因 required missing ranges 无法被 fetched 或 written 而不能完成时，response MUST
report an error for the incomplete portion，而不是 silently returning an apparently complete
result。

## Finalized And Safe Height Handling

Planner MUST 在 coverage 被 evaluated 之前，为每个 query 绑定 explicit finalized/safe
height policy。

对于 finalized 或 safe historical queries，manifest coverage MUST 只针对满足 required
finalized/safe height basis 的 chunks 进行 evaluation。高于 accepted safe height 的 chunks
MUST NOT satisfy finalized/safe coverage。

只有当 planned query 与 finalized/safe height policy 允许对应 range 时，chain adapter MAY
fetch near the chain head 的数据。Cache MUST distinguish head-adjacent or provisional data
from finalized/safe durable coverage。

## Reorg Posture

Specification-level reorg posture 是 conservative：

- Finalized/safe coverage MUST be preferred for durable reusable cache entries。
- Provisional head-adjacent data MUST NOT 被提升为 finalized/safe coverage，直到
  finalized/safe height policy 允许 promotion。
- 如果 reorg invalidates provisional cached data，later manifest coverage MUST exclude or
  supersede invalidated chunks。
- Queries requiring finalized/safe data MUST NOT 使用 known-invalidated provisional chunks
  回答。

This document does not define chain-family-specific reorg detection、rollback 或 refetch
algorithms。

## Non-Goals

This document MUST NOT define API endpoint names。

This document MUST NOT define chain-specific fetch algorithms。

This document MUST NOT define final storage object key layouts。

This document MUST NOT require complete chain-wide archival capture before a query can be
served。

This document MUST NOT be used as authority for implementation code。
