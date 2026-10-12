# DataLens PostgreSQL 重设计与开发方案

Status: accepted（待开发）

Date: 2026-10-12

Question: 在保留现有索引思路的前提下，如何彻底重设计 datalens，去掉对象存储，并从结构上消除
RPC 请求放大？

Decision: EVM-only；PostgreSQL 作为唯一持久化层（数据行 + coverage + 任务 + 链头 + 用量）。查询链路改为
"先查数据库 → 缺口按对齐 chunk 建任务、全集群去重 → 补齐结果与 coverage 同一事务写入"。safe 头附近与
latest 尾段由每条链的 follower 统一拉取、finalizer 在库内转正，客户端轮询只读数据库。不兼容旧存储、旧配置、
旧 API；degov 与 ormp 两个客户端随新 SDK/API 一起改。

Consequences: 删除对象存储及 compaction 全部代码、Solana/Tron 适配器、`evm.transactions` /
`evm.receipts` 数据集、declarative indexer 与示例客户端；RPC 用量与客户端数量、轮询频率解耦。

---

## 1. 背景

### 1.1 生产事故（2026-10-12）

- erpc 上 `datalens` project 对 Arbitrum（42161）约 235 req/s，外推约 2000 万/天，几乎全是
  `eth_getLogs`；上游 Dwellir 包月额度已用约 90%。
- 9 秒日志采样中 2642/2695 次是单区块 `eth_getLogs`；同一 25 区块区间 9 秒内重复 11 次。
- datalens 日志持续出现 `StorageReadFailure`、`over-budget pending deltas`（`snapshot_entries=0`）、
  `durable intent promotion failed`（`retry_delay_seconds=3633`）。

### 1.2 放大机制（现有代码）

| # | 机制 | 位置 |
| --- | --- | --- |
| 1 | coverage v2 bucket 待合并 delta 超过 128 个时，精确查找直接返回"未覆盖"；生产 compaction 关闭，miss 永久化；每次补齐又写新 delta | `crates/storage/src/coverage_index.rs`、`crates/cli/src/runtime.rs` |
| 2 | durable intent worker 重拉查询已拉过的数据，写完因 coverage 不可见报失败，退避 1 小时再来 | `crates/runtime/executor/src/durable_promotion.rs` |
| 3 | 任何包含 "range"/"limit" 的报错都归为 `ProviderLimit`；对半拆分到 1 块；进程级区间上限只降不升 | `crates/adapters/evm/src/provider_payload.rs`、`crates/runtime/executor/src/provider_range.rs` |
| 4 | latest 尾段每次轮询直接打 provider；singleflight 不保留结果、不跨副本 | `crates/runtime/planner/src/planning.rs`、`crates/runtime/executor/src/provider_singleflight.rs` |
| 5 | 每次查询实时请求 latest/safe 高度 | `crates/runtime/executor/src/execution.rs` |
| 6 | 一段失败整个查询失败，已拉到的段全部丢弃 | `crates/runtime/executor/src/execution.rs` |
| 7 | 没有 RPC 预算，没有按 application 的 provider 调用指标 | `crates/metrics` |

根因：对象存储没有事务、索引和安全的读改写，系统为此手写了一套分布式数据库（coverage index v1/v2、
delta/snapshot/head、分布式锁、compaction 队列、cursor 文件、intent marker），超过 1.5 万行，最近 300 个
提交中 114 个是存储修复。**存储出错被翻译成 RPC 重拉**，这是放大的主循环；机制 3–7 是独立于存储的缺陷。

### 1.3 生产现状（2026-10-12，hast devops 只读排查）

| 工作负载 | application | 链 | 用法 |
| --- | --- | --- | --- |
| prod-degov/degov-production-indexer | degov-live | 1, 10, 46, 56, 8453, 42161 | `evm.logs`，`durable_only`，blockRangeLimit 50000 |
| stg-degov/degov-staging-indexer | degov-live + degov-realtime | 同上 | 指向生产 datalens；realtime worker 每 3 秒以 `latest_only` 查最新 8 块 |
| prod-ringdao/msgport-production-ormpindexer | ormp | 1, 46, 137, 42161（Tron 不再需要） | `evm.logs`；`evm.blocks` 取 reorg 锚点；对每个含 `MessageSent` 的区块查 `evm.transactions`（`all`）只为取 `transaction_from` |

- 活跃 selector 116 个（degov-live 110、ormp 6），7 天内活跃 47 个；evm.logs selector 地址数 1–3 个；
  **没有不带地址过滤的 logs selector**。
- 自 2026-09-11 起 423 万次查询：`empty` 304 万、`hot_miss` 94 万、`hit` 仅 574 次；arbitrum 102 万次最多；
  ormp darwinia `evm.blocks` hot_miss 49 万次。
- 旧存储：`usage/` 31 万对象 18.9 GiB，arbitrum 前缀超过 53 万对象 16 GiB，durable intent 超过 3.4 万对象。

推论：

- 数据规模很小，PostgreSQL 单实例足够。
- prod degov 使用 `durable_only` 追着 safe 头轮询，所以 arbitrum 放大主要来自 durable 路径的永久 miss。新设计
  中追 safe 头的 durable 查询也必须由 follower + finalizer 服务（6.3），否则每次轮询都要为新推进的几十个块发一次 RPC。
- `evm.transactions` 的唯一用途是给 log 补发送方地址，改为 `evm.logs` 的可选字段（5.3），删除该数据集。

## 2. 保留与丢弃

### 2.1 保留的索引思路（不变量）

1. **按查询驱动的历史缓存**：调用方请求（chain, dataset, selector, range, finality, fields）；有覆盖就从缓存返回，
   缺多少补多少，补完持久化，下次复用。不做全链归档。
2. **coverage 是完整性的唯一依据**：显式记录"这个 selector 在这个区间的数据已完整"，没有数据的区间也可以被覆盖。
3. **selector 包含关系**：更宽的已覆盖 selector 满足更窄的查询（地址为空 = 任意地址；topic 位置为通配或超集）；
   多地址查询可以由每个地址各自的覆盖组合满足。
4. **finality 边界**：只有 `<= safe/finalized` 的数据成为 durable coverage；latest/unsafe 数据带 block hash，可因
   reorg 回滚，不写 durable coverage。
5. **原生查询契约**：`chain`、`dataset_key`、`selector`、`range`、`finality`（`durable_only` / `safe_to_latest` /
   `latest_only`）、`fields`；REST 与 GraphQL 等价。
6. **响应 segment 元数据**：每段标记 source 与 finality；客户端只能用 safe/finalized 段推进最终 cursor。
7. **application 边界**：认证、chain/dataset allowlist、quota、用量归因；application 不进入缓存 key。

### 2.2 丢弃

| 丢弃 | 说明 |
| --- | --- |
| 对象存储全部 | Parquet 对象、manifest、coverage index v1/v2、bucket 锁、compaction/cleanup、cursor 文件、intent JSON/marker、JSONL usage ledger、query watermark/activity、对象存储版 hot cache、warmup/cache-repair 注册表 |
| 非 EVM | Solana、Tron 适配器与数据集，`slot` range kind，`AdapterJson` 行类型 |
| 数据集 | `evm.transactions`、`evm.receipts`（由 `transaction_from` 字段替代） |
| 运行时机制 | intent 两段式写入、进程级只降不升的区间状态机、read-through cache、over-budget/throttle/coalesce 启发式 |
| 代码 | `crates/indexer`（declarative indexer）、`examples/degov-client`、`examples/ormp-client`（真实客户端已在各自仓库）、`crates/testing/chain-conformance` 的非 EVM 部分 |
| 兼容 | 旧配置格式、旧 API 响应字段、旧 SDK 形态、旧 RustFS 数据（不导入） |

## 3. 验收指标

| 指标 | 目标 |
| --- | --- |
| Arbitrum 稳态 provider 调用（degov prod + stg + realtime + ormp） | ≤ 5 req/s（当前约 235） |
| provider 调用与客户端数、轮询频率的关系 | 无关；只与链数、tick 频率、活跃 selector 批次数相关 |
| 同一（chain, dataset, selector, chunk）的历史区间 | 全集群只拉一次 |
| 已覆盖区间查询（10k 块、单合约） | p95 < 100 ms |
| 每次查询的高度 RPC | 0 |
| 存储相关代码量 | < 4 千行 |

## 4. 总体架构

```text
               ┌──────────────── datalens（单一二进制，按 role 启动）────────────────┐
 degov ─┐      │ api role（无状态，N 副本）          worker role（≥1 副本）            │
 ormp  ─┼──►   │ - auth / quota / usage              - fill worker：领 fill_jobs 拉 RPC │
 SDK    │      │ - query：coverage → 缺口 → 建/等    - head tracker：每链刷新 chain_heads│
        ┘      │   fill_jobs → 读行 → 渐进返回       - follower：按订阅合批拉新块       │
               │ - heads / wait 长轮询               - finalizer：safe 后库内转正       │
               │                                     - reorg / retention / usage 落库   │
               └─────────────┬──────────────────────────────────┬────────────────────────┘
                             ▼ sqlx                              ▼ provider gateway（预算）
                    PostgreSQL（唯一真相）                  erpc → 上游 RPC
```

设计要点：

1. **PostgreSQL 唯一持久化**：数据行、coverage、任务状态同一事务提交，写完即可见。
2. **数据按行存一份，coverage 按 selector 记**：同一条 log 无论被多少 selector 拉到都只存一次；查询 = 校验
   coverage + SQL 过滤。
3. **补齐经数据库任务去重**：缺口按固定网格对齐成 chunk；`fill_jobs` 唯一约束保证全集群只有一个 worker 拉同一段；
   其他请求 `LISTEN/NOTIFY` 等结果。
4. **follower 统一拉 head**：查询自动注册订阅；follower 每链每 tick 按订阅合批拉新块写 unsafe 行；finalizer 在 safe
   推进后库内转正并扩展 coverage，不重拉 RPC。
5. **provider gateway**：每链 token bucket 与日预算、正确的错误分类、有下限且可恢复的自适应区间、按 application/purpose
   的调用指标。
6. **渐进返回**：能补到哪里就返回到哪里，附 `next_start`；不再因一段失败丢弃整次结果。
7. **head 推送**：`heads/wait` 长轮询替代客户端固定间隔轮询。

## 5. 数据模型（PostgreSQL 16）

约定：区块号 `bigint`；hash/地址/topic 用 `bytea`；`finality smallint`（1 unsafe、2 safe、3 finalized）；所有表带
`chain_id integer`（即 EVM chain id，例如 42161）。迁移文件放在 `crates/store/migrations/`，由 `sqlx migrate` 管理。

### 5.1 注册表

```sql
create table chains (
  chain_id    integer primary key,       -- EVM chain id
  name        text not null unique       -- 配置中的链名，例如 arbitrum
);

create table selectors (
  id            bigserial primary key,
  chain_id      integer not null references chains,
  dataset       text not null,           -- evm.logs | evm.blocks
  fingerprint   text not null,
  canonical_key text not null,
  addresses     bytea[] not null default '{}',   -- 空 = 任意地址
  topics        jsonb not null default '[]',     -- 每个位置：null=通配，[]=不匹配，[..]=任一
  created_at    timestamptz not null default now(),
  last_used_at  timestamptz not null default now(),
  unique (chain_id, dataset, fingerprint)
);
create index on selectors using gin (addresses);
```

`chains` 启动时由配置同步；RPC 地址等运行参数只在配置文件里。`evm.blocks` 只有一个 `all` selector。

### 5.2 coverage

```sql
create extension if not exists btree_gist;

create table coverage (
  chain_id    integer not null,
  dataset     text not null,
  selector_id bigint not null references selectors,
  blocks      int8range not null,        -- [start, end+1)
  finality    smallint not null,         -- 2 safe | 3 finalized
  updated_at  timestamptz not null default now(),
  exclude using gist (chain_id with =, dataset with =, selector_id with =, blocks with &&)
);
```

- 写入时在事务内合并相邻/重叠区间（删旧行、插入合并行），每个 selector 通常只剩几行。
- 空覆盖不是单独概念：coverage 只表达"完整"，有没有行由数据表决定。
- 只记录 durable 覆盖；latest 尾段完整性由 `follow_progress`（5.5）表达。

缺口计算（multirange）：

```sql
select int8multirange(int8range($start, $end + 1))
       - coalesce(range_agg(blocks), '{}'::int8multirange) as missing
from coverage
where chain_id = $chain and dataset = $dataset and selector_id = any($candidates)
  and blocks && int8range($start, $end + 1);
```

包含关系：

1. 候选 selector：`addresses = '{}' or addresses @> $query_addresses`（GIN），topic 位置的超集判断在 Rust 中对候选集做。
2. 多地址查询按地址拆分：查询 `{A, B}` 在 r 内被覆盖，当且仅当 `{A}` 与 `{B}`（相同 topic 条件）各自在 r 内被覆盖；
   分别求覆盖 multirange 后取交集。

### 5.3 数据表

```sql
create table evm_blocks (
  chain_id    integer not null,
  number      bigint not null,
  hash        bytea not null,
  parent_hash bytea not null,
  timestamp   bigint not null,
  finality    smallint not null,
  header      jsonb not null,            -- 其余头字段，按 fields 投影
  primary key (chain_id, number)
) partition by list (chain_id);

create table evm_logs (
  chain_id          integer not null,
  block_number      bigint not null,
  log_index         integer not null,
  block_hash        bytea not null,
  transaction_index integer not null,
  transaction_hash  bytea not null,
  address           bytea not null,
  topic0 bytea, topic1 bytea, topic2 bytea, topic3 bytea,
  data              bytea not null,
  finality          smallint not null,
  primary key (chain_id, block_number, log_index)
) partition by list (chain_id);
create index on evm_logs (chain_id, address, block_number);

create table evm_tx_senders (
  chain_id         integer not null,
  transaction_hash bytea not null,
  block_number     bigint not null,
  sender           bytea not null,
  primary key (chain_id, transaction_hash)
);
```

- 每条配置的链一个分区，启动时自动创建；数据量小，第一版不做二级区块分区。
- 写入 `INSERT ... ON CONFLICT DO NOTHING`；unsafe 行由 reorg handler 删除重建（6.4）。
- **`transaction_from` 字段**：`evm.logs` 查询的 `fields` 包含 `transaction_from` 时，api 对结果中缺少发送方的
  `transaction_hash` 批量调用 `eth_getTransactionByHash`（JSON-RPC batch，经 gateway），写入 `evm_tx_senders` 后
  join 返回。ormp 的 `MessageSent` 很稀疏，调用量可忽略；同一 hash 全局只查一次。reorg 回滚时按 `block_number`
  一起删除。

查询正确性：覆盖区间内任何匹配查询的行都已写入（coverage 保证）；其他 selector 拉到的额外行同样是规范链数据，
匹配条件即可返回。所以"coverage 校验 + `where address = any(..) and topic.. and block_number between ..`"与直接查 RPC 一致。

### 5.4 任务

```sql
create table fill_jobs (
  id            bigserial primary key,
  chain_id      integer not null,
  dataset       text not null,
  selector_id   bigint not null,
  blocks        int8range not null,      -- 对齐后的 chunk，已裁剪到 safe
  purpose       text not null,           -- query | backfill
  application_id text not null,          -- 首个创建者，用于归因
  status        text not null,           -- pending | running | done | failed
  attempts      integer not null default 0,
  next_run_at   timestamptz not null default now(),
  lease_until   timestamptz,
  worker_id     text,
  last_error    jsonb,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);
create unique index fill_jobs_active on fill_jobs (chain_id, dataset, selector_id, blocks)
  where status in ('pending', 'running');
create index on fill_jobs (status, next_run_at);
```

- 领取：`for update skip locked`，设置 `lease_until`；租约过期可被重新领取。
- 完成：同一事务写行、合并 coverage、置 `done`，然后 `pg_notify('fill_done', id)`。
- `done` 任务保留 1 天后清理。

### 5.5 链头、订阅与 follow 进度

```sql
create table chain_heads (
  chain_id     integer primary key,
  latest       bigint not null,
  latest_hash  bytea not null,
  safe         bigint not null,
  finalized    bigint not null,
  updated_at   timestamptz not null
);

create table subscriptions (
  chain_id       integer not null,
  dataset        text not null,
  selector_id    bigint not null,
  application_id text not null,
  source         text not null,          -- auto | explicit
  last_query_at  timestamptz not null,
  expires_at     timestamptz not null,
  primary key (chain_id, dataset, selector_id, application_id)
);

create table follow_progress (
  chain_id     integer not null,
  dataset      text not null,
  selector_id  bigint not null,
  from_block   bigint not null,          -- 首次 follow 时的 safe+1
  head_block   bigint not null,          -- 已完整拉取到的最高块
  head_hash    bytea not null,
  promoted_to  bigint not null,          -- 已转正到的高度
  primary key (chain_id, dataset, selector_id)
);
```

### 5.6 用量

```sql
create table usage_daily (
  day             date not null,
  application_id  text not null,
  chain_id        integer not null,
  dataset         text not null,
  queries         bigint not null default 0,
  rows_returned   bigint not null default 0,
  blocks_served   bigint not null default 0,
  blocks_filled   bigint not null default 0,
  provider_calls  bigint not null default 0,
  primary key (day, application_id, chain_id, dataset)
);
```

api/worker 进程内聚合，每 10 秒批量 upsert。Prometheus 指标并行暴露。

## 6. 核心流程

### 6.1 durable 查询（`durable_only`）

1. 认证、allowlist、quota；规范化 selector，upsert `selectors` 得到 `selector_id`（`last_used_at` 节流更新）。
2. 从 `chain_heads` 读 safe/finalized（进程内缓存 500 ms）；`end > safe` 返回 `invalid_input`。
3. 若 `end >= safe - follow_attach_blocks`（默认 1000），upsert 订阅（6.3）。
4. 计算缺口（5.2）。没有缺口直接读行。
5. 缺口对齐到固定网格（该链 `chunk_blocks`，例如 arbitrum logs 10000），裁剪到 safe；对每个 chunk
   `insert into fill_jobs ... on conflict do nothing`；`LISTEN fill_done` 等待，最长 `min(wait_ms, query.max_wait_ms)`。
   api 进程空闲时也可以作为 inline worker 领取自己等待的任务。
6. 读行：对已覆盖的连续前缀 `[start, x]` 做 SQL 过滤、排序、字段投影、分页。
7. `x < end` 时返回前缀和 `next_start = x + 1`，附 `incomplete_reason`。

### 6.2 fill worker

1. 领取任务。同一（chain, dataset, chunk）下不同 selector 的 pending 任务**合批**：地址并集（不超过
   `max_addresses_per_query`），topic 取覆盖所有成员的最小公共条件（不同 topic0 时取并集，其余位置取通配）。
2. 经 gateway 拉取；gateway 按 provider 上限拆子区间。
3. 拉回后按每个成员 selector 重新过滤校验；一个事务：写行 → 为每个成员 selector 合并 coverage → 任务 `done` → notify。
4. 可重试错误指数退避（上限 5 分钟），记录 `last_error`；不可重试错误置 `failed`，等待中的查询立即收到原因。

### 6.3 follow：safe 头附近与 latest 尾段

1. **订阅**：以下查询会 upsert 订阅（source=auto，`expires_at = now() + 15 min`）：
   - `safe_to_latest`、`latest_only`；
   - `end >= safe - follow_attach_blocks` 的 `durable_only`（例如 prod degov 追 safe 头）。
   SDK 也可以显式 `watch`（source=explicit，自行续约）。
2. **head tracker**（每链一个 leader，`pg_try_advisory_lock(chain_id)` 选主），每 tick（按链配置，默认 2 s）：
   - 1 次 `eth_getBlockByNumber("latest", false)`；safe/finalized 用 RPC tag 或 lag 规则（与现有配置语义一致）；
   - 写 `chain_heads`。
3. **follower**（同一 leader），每 tick：
   - 对每个活跃订阅，新订阅的 `from_block` 取当前 safe+1，之前的部分由 durable 路径（6.1）负责；
   - 把 `head_block` 相同或相近的 logs 订阅合批，对 `[head_block + 1, latest]` 各发一次合并后的 `eth_getLogs`；
   - 有 `evm.blocks` 订阅的链，对同一区间批量拉区块头；
   - 一个事务：写 unsafe 行（带 block_hash）、推进 `follow_progress.head_block/head_hash`。
4. **finalizer**：safe 推进后，对每个订阅的 `(promoted_to, min(safe, head_block)]`：
   - 校验该段末尾块的 hash 仍为规范链（若不一致走 6.4）；
   - 一个事务：把行的 `finality` 更新为 safe/finalized，为该 selector 合并 durable coverage，推进 `promoted_to`。
   - 这就是 promotion，不重拉 RPC。
5. **查询读尾段**：`(safe, head_block]` 从库里读 unsafe 行，segment 标记 `source = hot`、`finality = unsafe`；
   `head_block` 到 `latest` 之间还没拉到的块不返回，`served.end = head_block`，客户端下一轮读到。
   新订阅首次追赶未完成时 `incomplete_reason = not_followed_yet`。
6. **订阅过期**：停止 follow；已转正的 coverage 保留，未转正的 unsafe 行由 retention 清理。

### 6.4 reorg

- 检测：每 tick 用 1 次 `eth_getBlockByNumber(head_block)` 校验各链最旧的未转正 follow 头的 hash；新拉的 logs 的
  `blockHash` 与已存同高度行不一致也视为 reorg。
- 处理：向下二分找分叉点 F（最多 `reorg_window_blocks`，超过则告警并停止该链 follow）；一个事务删除
  `block_number >= F` 且 `finality = unsafe` 的行（logs、blocks、tx_senders），回退相关 `follow_progress`；下一 tick 重拉。
- durable 行（≤ safe）永不回滚；如果 reorg 深于 safe，记 finality violation 并告警（保持现有规范）。

### 6.5 稳态 RPC 估算

每链每 tick：head 1 次 + reorg 校验 1 次 + logs 合批 k 次（degov/ormp 现有订阅通常 k ≤ 3）+ 有 `evm.blocks`
订阅时的区块头批量。arbitrum 2 s tick：约 (2 + 2) / 2 = 2 req/s。8 条链合计约 10–15 req/s，arbitrum ≤ 5 req/s。
与客户端数和轮询频率无关。

## 7. Provider gateway

| 能力 | 规则 |
| --- | --- |
| 预算 | 每链 token bucket（`rps`、`burst`）与可选 `daily_calls`；优先级 head/reorg > follow > query fill > backfill；预算耗尽时 fill 退避，查询返回 `budget_exhausted` 前缀 |
| 错误分类 | 只有明确的"区间/结果过大"（如 `query returned more than`、`block range too large`、`exceed maximum block range`）才是 `ProviderLimit`；`header not found`、`block not available`、`unknown block` 归为 `ProviderNotReady`（可重试，不拆分）；429 是 `RateLimited` 并尊重 `Retry-After` |
| 自适应区间 | 状态按（chain, dataset）；下限 `min_range_blocks`（默认 16）；`ProviderLimit` 时减半；连续成功 8 次乘 2 恢复；状态 TTL 10 分钟 |
| 并发 | 每链 `max_in_flight` |
| 重试 | 单次调用最多 3 次指数退避 |
| 指标 | `datalens_provider_calls_total{chain, method, purpose, application, outcome}`、`datalens_provider_call_duration_seconds`、`datalens_provider_budget_tokens{chain}` |
| 归因 | fill 任务记录首个创建者；合批调用按成员平摊到 `usage_daily.provider_calls`；follow 调用按订阅 application 平摊 |

现有 reliability 补查（bloom 命中但无 log 时查 secondary）不保留；如需多 provider 冗余交给 erpc。

## 8. API

### 8.1 查询：REST `POST /v1/query`，GraphQL `query(input:)`

请求：

| 字段 | 说明 |
| --- | --- |
| `chain` | 链名，例如 `"arbitrum"`（EVM-only，不再需要 family / network_id） |
| `dataset` | `evm.logs` 或 `evm.blocks` |
| `selector` | `evm.logs`：`{ "addresses": [...], "topics": [...] }`；`evm.blocks`：省略 |
| `range` | `{ "start": 100, "end": 5000 }`（闭区间区块号） |
| `finality` | `durable_only`（默认）、`safe_to_latest`、`latest_only` |
| `fields` | 省略 = 全部；或字段名数组；`transaction_from` 只有显式要求时才返回 |
| `page` | `{ "limit": 10000, "cursor": "..." }` |
| `wait_ms` | 愿意等待补齐的时间，服务端上限 `query.max_wait_ms` |

响应：

```json
{
  "chain": "arbitrum",
  "dataset": "evm.logs",
  "requested": { "start": 100, "end": 5000 },
  "served":    { "start": 100, "end": 4200 },
  "next_start": 4201,
  "incomplete_reason": "pending",
  "segments": [
    { "start": 100,  "end": 3999, "source": "durable", "finality": "finalized" },
    { "start": 4000, "end": 4200, "source": "hot", "finality": "unsafe",
      "anchor": { "number": 4200, "hash": "0x..." } }
  ],
  "rows": [],
  "page": { "next_cursor": null }
}
```

- `served` 永远是从 `requested.start` 开始的连续前缀；`next_start` 为 `null` 表示全部完成。
- `incomplete_reason`：`pending`、`provider_error`、`budget_exhausted`、`not_followed_yet`。
- 不可恢复错误使用稳定的 `error.kind`：`unauthorized`、`forbidden`、`invalid_input`、`quota_exceeded`、
  `unsupported`、`internal`。
- `latest_only` 只返回 hot 段（`(safe, head_block]` 与请求区间的交集）。

### 8.2 其他接口

| 接口 | 说明 |
| --- | --- |
| `GET /v1/heads` / `GET /v1/heads/{chain}` | 读 `chain_heads` |
| `GET /v1/heads/{chain}/wait?after=<n>&kind=latest|safe|finalized&timeout_ms=30000` | 长轮询，有更新立即返回 |
| `POST /v1/watch` / `DELETE /v1/watch` | 显式订阅 |
| `POST /v1/admin/backfill` | 提交历史补齐（`purpose = backfill`） |
| `GET /v1/admin/coverage` | 查看 selector 覆盖与缺口 |
| `GET /v1/admin/jobs` | 查看任务与失败原因 |
| `GET /metrics`、`/health`、`/ready` | 运维（metrics 需 bearer token） |

### 8.3 application 与 quota

- 配置文件声明 application（id、token、chains、datasets、quota）。
- quota：`max_range_blocks`、`max_requests_per_minute`、`max_concurrent_requests`、`max_subscriptions`、
  `max_fill_blocks_per_minute`；第一版不允许不带地址过滤的 `evm.logs`（`allow_unfiltered_logs = false`）。
- 内存中按副本计数；多副本时按副本数折算（第一版 api 1–2 副本，足够）。
- 生产配置：`degov-live`、`degov-realtime`、`ormp`；删除 `public`。建议 stg indexer 改用独立的 `degov-stg`
  application，便于用量归因。

### 8.4 SDK

- Rust SDK（`sdks/rust`，degov 使用）：新请求/响应类型；`follow(query)` 异步流，内部 `heads/wait` + `next_start` 推进；
  保留 `FinalizedCursor`（只接受 safe/finalized 段）与 `ProvisionalCursor`（anchor hash 不匹配时回退）。
- TypeScript / Go SDK 同步新类型与 `follow`；没有已知生产使用方，优先级低于 Rust。
- `schemas/native.graphql` 重新导出。

## 9. 代码结构

```text
crates/
  core/       领域词汇：ChainName、BlockRange、LogSelector（含 covers / 合批）、Finality、错误
  evm/        EVM RPC：eth_getLogs、区块头、heads/finality、getTransactionByHash；行标准化（原 adapters/evm 精简）
  provider/   gateway：预算、错误分类、自适应区间、重试、指标
  store/      PostgreSQL：migrations、repositories（selectors、coverage、logs、blocks、tx_senders、jobs、heads、subscriptions、follow、usage）
  engine/     query planner（缺口、对齐、包含关系）、fill worker、head tracker、follower、finalizer、reorg、retention
  api/        REST + GraphQL + auth + quota + metrics
  server/     二进制、配置、role 启动、优雅退出
  client/     内部 REST client（测试用）
sdks/{rust,typescript,go}
```

删除：`crates/storage`、`crates/runtime/*`、`crates/edge`、`crates/cli`、`crates/chain`、`crates/adapters/{solana,tron}`、
`crates/indexer`、`crates/metrics`（并入 `server`）、`crates/testing`、`examples/*`（token-sdk 示例随 SDK 更新保留或删除，
M7 决定）、`config/*.toml`、RustFS 相关 compose 与 runbook。

约束：源文件 ≤ 800 行；测试放 `tests/`；`store`/`engine` 测试用真实 PostgreSQL（docker compose，CI 中 PG service），不 mock。
EVM RPC 测试用本地 mock JSON-RPC server（录制 fixture），另有 live smoke（需要 RPC URL 环境变量，默认跳过）。

## 10. 配置

```toml
[server]
bind = "0.0.0.0:3000"
roles = ["api", "worker"]           # 生产可拆成两个 Deployment

[database]
url = "${DATALENS_DATABASE_URL}"
max_connections = 32

[query]
max_wait_ms = 15000
page_limit = 10000
follow_attach_blocks = 1000

[follow]
subscription_ttl_s = 900
unsafe_retention_s = 86400

[metrics]
bearer_token = "${DATALENS_METRICS_TOKEN}"

[chains.arbitrum]
chain_id = 42161
rpc_url = "${DATALENS_ARBITRUM_RPC_URL}"
finality = { mode = "lag", safe_lag_blocks = 64, finalized_lag_blocks = 256 }
tick_ms = 2000
reorg_window_blocks = 512
chunk_blocks = 10000
max_get_logs_range_blocks = 10000
max_addresses_per_query = 32
provider = { rps = 20, burst = 40, daily_calls = 2000000, max_in_flight = 8, min_range_blocks = 16 }

[[applications]]
id = "degov-live"
token = "${DATALENS_DEGOV_LIVE_TOKEN}"
chains = ["ethereum", "optimism", "darwinia", "bsc", "base", "arbitrum", "lisk"]
datasets = ["evm.logs", "evm.blocks"]
quota = { max_range_blocks = 200000, max_requests_per_minute = 1800, max_concurrent_requests = 96, max_subscriptions = 200 }
```

生产 8 条链（ethereum、darwinia、base、polygon、lisk、optimism、bsc、arbitrum）的 finality lag 与 range 上限沿用现有
`datalens.toml` 的值（darwinia `max_get_logs_range_blocks = 1000`）。

本地开发 `docker-compose.yml` 只保留 PostgreSQL：

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${DATALENS_PG_USER:-datalens}
      POSTGRES_PASSWORD: ${DATALENS_PG_PASSWORD:-datalens}
      POSTGRES_DB: ${DATALENS_PG_DB:-datalens}
    ports:
      - "${DATALENS_PG_PORT:-5432}:5432"
    volumes:
      - ./.data/pg:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 2s
      timeout: 5s
      retries: 20
```

## 11. 开发计划

每个里程碑一个或多个可独立合并的 PR。开发在 datalens 新分支上进行，M1–M5 完成后发布新镜像；degov 与 ormp 的改造
在各自仓库（`ringecosystem/degov` 的 `apps/indexer`、`ringecosystem/ormpindexer`）。

| 里程碑 | 内容 | 验收 | 估算 |
| --- | --- | --- | --- |
| M0 骨架 | 删除旧 crate，建立第 9 节布局；`server` 配置解析与 role 启动；sqlx 迁移框架；本地 compose（`.data/pg`）；`Justfile`、CI（PG service）更新 | `just check` 通过；服务可启动、迁移自动执行、`/health` 正常 | 2 天 |
| M1 store | 第 5 节全部表与 repositories：selector upsert、coverage 合并与缺口 multirange、包含关系（多地址交集、topic 通配）、logs/blocks/senders 写读与分页、`fill_jobs` 领取/租约/完成/通知、heads、subscriptions、follow、usage | 真实 PG 测试：相邻区间合并、包含关系各分支、两个连接并发领取同一任务只成功一个、租约过期重领、unsafe 删除与转正 | 5 天 |
| M2 evm + provider | gateway（预算、错误分类、自适应区间、重试、指标）；EVM RPC client：logs、区块头批量、heads/finality、`eth_getTransactionByHash` 批量 | mock RPC 测试：链头错误不拆分、区间缩小后恢复、预算耗尽行为、429 尊重 `Retry-After`；对公共 RPC 的 live smoke | 4 天 |
| M3 durable 闭环 | engine：缺口、对齐、合批、fill worker、渐进返回、`transaction_from` 补全；api：`/v1/query` REST + GraphQL、auth/allowlist/quota、usage | 集成测试：冷查询补齐后二次查询 0 次 RPC；两个 engine 实例并发查同一区间只产生一次 RPC；合批后各 selector 结果与单独查询一致；部分失败返回前缀与 `next_start` | 6 天 |
| M4 follow | head tracker、订阅、follower 合批、finalizer、reorg、retention；`/v1/heads`、`/wait`、`/v1/watch`、admin 接口 | mock 链测试：替换 hash 模拟 reorg 后 unsafe 行回滚并重拉；safe 推进后行转正且 coverage 扩展、无额外 `eth_getLogs`；100 个并发轮询客户端下 provider 调用数不变；追 safe 头的 `durable_only` 查询 0 次 RPC | 6 天 |
| M5 可观测与压测 | 指标齐全、Grafana dashboard JSON；压测脚本模拟 degov prod/stg/realtime + ormp 在 8 条链上的负载（录制 RPC 回放） | arbitrum provider ≤ 5 req/s；已覆盖查询 p95 < 100 ms | 3 天 |
| M6 SDK | Rust SDK 新类型、`follow`、cursor helpers；TS/Go SDK 同步；`schemas/native.graphql` 导出；README | SDK 测试通过；对本地 compose 服务的 e2e | 3 天 |
| M7 degov 改造 | `apps/indexer`：升级 `datalens-sdk` rev；主索引改用 `follow`（替代固定间隔轮询）；provisional/realtime worker 改用新 `latest_only` 语义与 anchor 回退；处理 `next_start` / `incomplete_reason`；配置项更新；stg 改用独立 application | degov 现有测试通过；本地 compose（datalens + PG + degov）对一条 live 链的 e2e：回放、追头、realtime 正常 | 4 天 |
| M8 ormp 改造 | `ormpindexer`：删除 Tron（`decoder/tron.rs`、Tron 配置与测试）；`transaction_from` 改用 `evm.logs` 的 `fields`，删除 `evm.transactions` 查询；`evm.blocks` 锚点查询与 `latest_block` 改用新 API/heads；适配新响应 | ormp 现有测试（含 legacy parity）通过；本地 e2e 对 darwinia/arbitrum 正常 | 3 天 |
| M9 上线与清理 | 发布镜像；按 degov-stg → ormp → degov-prod 顺序切换；观察 erpc `datalens` project 用量；重写 `docs/spec`（02/03/04/07、production-runtime）与 runbook；删除过时文档；本计划转为 decision | 切换后 arbitrum 请求降到目标值；三方业务数据对账一致；文档与代码一致 | 3 天 |

合计约 39 个工作日。M1 与 M2 可以并行；M6 可以在 M3 接口稳定后与 M4 并行；M7 与 M8 可以并行。

### 11.1 历史数据

不导入。新库从空开始，切换后由客户端回放按需补齐：每个 selector、每个 chunk 全集群只拉一次，并可用
`POST /v1/admin/backfill` 在低峰期以受限预算预热。估算 arbitrum 从约 1.95 亿块回放到 5.14 亿块约 3.2 万个
10000 块的 chunk，按合批与 20 req/s 预算可在数小时内完成。

### 11.2 旧服务

开发期间旧服务继续运行，切换完成后下线；RustFS 上的旧数据在下线后删除（`usage/`、`chains/` 等全部前缀）。

## 12. 风险与对策

| 风险 | 对策 |
| --- | --- |
| 合批放宽过滤条件导致结果错误 | 合批规则集中在 `core::LogSelector`，有专门测试；写入前按成员 selector 重新过滤；coverage 只记给成员 |
| follower 落后于 head | `served.end` 明确告知；follow 优先级高于 fill；告警 `datalens_follow_lag_blocks` |
| 单 PG 故障 | 数据是缓存，最坏情况重新按需补齐；api `/ready` 依赖 PG 连通性 |
| unsafe 行频繁写删 | 只在 follow 窗口内存在；转正是 UPDATE；`unsafe_retention_s` 清理过期订阅留下的行 |
| 新 API 不兼容 | 只有 degov 与 ormp 两个客户端，M7/M8 同步改造，按 application 逐个切换 |
| 回放初期 RPC 突增 | gateway 预算兜底；backfill 优先级最低 |

## 13. 已确认的决定（2026-10-12）

1. EVM-only；Solana 和 Tron 都不再支持，ormp 删除 Tron。
2. PostgreSQL 由运维提供，datalens 只需要 `DATALENS_DATABASE_URL`；本地开发使用 docker compose，数据目录 `.data/pg`。
3. degov 和 ormp 由本计划一并改造，跟随新 SDK/API 升级。
4. 不导入历史数据。
5. ormp 的 `evm.transactions` 用法改为 `evm.logs` 的 `transaction_from` 字段（5.3）。
6. 保留 `degov-realtime`（latest 数据实时性），由 follow 机制从库内服务。
