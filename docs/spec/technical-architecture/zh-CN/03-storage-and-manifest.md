# 03 - 存储和 Manifest

这一步设计 datalens 如何记住自己缓存了什么。关键点是，持久化归档不是“本地刚好存在的
文件”，而是对象存储中的数据分片加 Manifest 覆盖范围记录。

## 存储的角色

对象存储是长期依据。第一版实现可以本地使用 MinIO，后续使用 DigitalOcean Spaces 这类
S3-compatible service。存储层应把 provider 细节藏在小接口后面，让查询、规划器和写入器
不需要知道后端是 MinIO、Spaces、R2、S3 还是其他兼容存储。

本地磁盘是临时工作区。它可以保存查询期间下载的数据分片、上传前暂存的文件、临时重试产物
和本地测试 fixtures。它不应该决定某个范围是否已覆盖。如果进程重启导致本地临时文件消失，
持久化状态仍应能从对象存储和 Manifest 恢复。

## 临时对象布局

最终对象布局可以演进，但第一版实现应使用可预测结构：

```text
chains/<chain-kind>/<chain-name>/
├── manifest.json
└── datasets/
    └── <dataset>/<schema-version>/<coverage-key>/<range-key>.parquet
```

示例：

```text
chains/evm/ethereum-mainnet/manifest.json
chains/evm/ethereum-mainnet/datasets/evm.logs/parquet-v1/block/addr-topic-7f3a91/018000000-018099999.parquet
chains/evm/darwinia/datasets/evm.blocks/parquet-v1/block/all/000000000-000099999.parquet
```

布局使用 `chain-kind`，这样未来 `tron` 或 `solana` 适配器不需要塞进 EVM-only namespace。

Application identity 不属于第一阶段 durable object 或 manifest coverage key。相同 chain、
dataset、selector、range 和 finality 的请求共享 cache object。Application 边界用于
authentication、authorization、quota validation、metrics，以及未来审计或归因；per-application
durable object layout 需要单独的 storage design。

`range-key` 只是对象标识中的范围部分。对于 EVM 区块号范围，`018000000-018099999` 表示
区块 `18,000,000` 到 `18,099,999`，补零是为了让对象列表按字典序排序时也符合区块顺序。
它不表示 datalens 已经完整覆盖这个区块范围里的所有数据集或所有合约。

`coverage-key` 表示这个分片的逻辑覆盖形态。对完整区块头来说，它可以是 `all`。对过滤后的
logs 来说，它应该由标准化后的过滤条件确定性生成，例如 addresses、topics 和字段覆盖范围。
如果第二个合约查询了同一个区块范围，除非已有覆盖范围已经能满足第二个查询，否则它应该产生
另一个 `coverage-key`。

## Manifest 覆盖范围

Manifest 描述 safe 或 finalized 的历史持久化覆盖范围。第一阶段的持久化覆盖范围不能表达
latest、不稳定或 hot 数据。只有当一个范围的结束高度小于等于该 adapter 针对对应链族范围模型
给出的 safe/finalized height 时，才能写入对象存储或在 Manifest 中声明覆盖。

对于 EVM block range，持久化写入不变量是：

```text
range.to_block <= adapter.safe_height().value
```

adapter safe height 必须带有 `Safe` 或 `Finalized` finality。未来链族可以使用非 EVM 的范围
kind，但在记录持久化覆盖之前，必须暴露等价的 safe/finalized height。

一个覆盖范围记录应包含：

- 链家族，例如 `evm`。
- 链名称或配置中的链标识；如果配置了 network id，也应包含 network id。
- 数据集 key，例如 `evm.blocks`、`evm.logs`、`tron.events` 或
  `solana.transactions`。
- 已覆盖 ledger range，包括 range kind、start 和 end。
- Schema 或标准化版本。
- Object encoding，例如 `parquet-v1`。
- 用于查找的 selector fingerprint，以及用于审计的 selector canonical key。
- 字段覆盖范围或标准分片形态。
- 持久化分片的 object key。
- 可用时记录 size、checksum 和写入元数据。
- finality 元数据。durable Manifest 只能记录 safe 或 finalized coverage。

因为 datalens 缓存的是按需求选择的数据，某个带 address filter 的 `logs` 分片并不代表同一区间
的完整日志覆盖，也不代表另一个合约的覆盖。Manifest 必须明确区分这些含义。

Manifest 覆盖范围不能用于 non-durable hot query 结果。empty coverage 和 data-object coverage
具有相同 finality 要求：如果请求范围超过 safe/finalized height，datalens 不能写入 empty
coverage 记录。

正常写入路径不应该把已有范围分片解压出来、追加另一个合约的数据、重新压缩并覆盖上传。那会
让并发补齐很难推理，也会让每个新过滤条件都变成读改写操作。第一版实现应该写入不可变的逻辑
分片，分片由数据集、覆盖 key、schema 版本和范围共同确定。后续可以做离线 compaction 来合并
兼容分片，但 compaction 是优化，不应成为正确性的前提。

## 分片范围大小

分片范围不应该是一个全局固定常量。范围大小应该按链家族和数据集配置，并同时考虑四个约束：

- Provider 限制：这个范围必须落在对应 RPC/provider 对该数据集允许的查询范围内。
- 目标对象大小：写入器应尽量避免稀疏过滤条件产生大量很小的对象。
- 最大扫描跨度：范围仍然必须有上限，避免一次补齐任务扫描过大的历史区间。
- 复用边界：范围划分应足够稳定，让未来等价查询可以复用同一批覆盖范围记录。

第一版 EVM logs 实现不要把范围大小写死成永久规则，而应使用可配置默认值。一个实际起点可以是：

- `max_range_blocks`：单个补齐任务允许扫描的最大区块跨度，例如 provider 允许时使用
  `100_000` 个区块。
- `target_object_bytes`：期望的压缩后对象大小，例如 16-64 MiB。
- `min_object_rows`：非空对象在 flush 前期望达到的最小行数。
- `empty_coverage`：某个范围和过滤条件没有返回任何行时，写入 Manifest 的空覆盖记录。

稀疏过滤是预期情况。如果某个合约在一个范围内没有匹配日志，系统不应该为了记住这次空结果
写一个很小的空 Parquet 文件。它应该在 Manifest 中记录一条行数为 `0`、没有数据对象的空覆盖
记录，这样同一个范围和过滤条件下次不需要再次拉取。

如果非空结果仍然太小，writer 可以继续累计相邻范围，但前提是数据集、selector 覆盖形态、
finality level 和 range kind 都兼容。达到配置的 flush threshold 后，再把合并范围的一次不可变
对象写入委托给 storage，并通过 storage 记录这个对象实际覆盖的合并范围。

这意味着 `018000000-018099999` 不是通用规则，它只是范围 key 的示例。实现时应让真实范围大小
可配置、可观测。

## 写入顺序

writer 协调这条安全顺序；storage 拥有对象编码、object key 构造、对象字节和 Manifest
repository 更新：

1. 接收一个计划补齐片段的标准化拉取数据。
2. 在任何持久化写入或 Manifest 更新前，验证该片段处于 adapter safe/finalized height 内。
3. 合并相邻且兼容的片段，以改善对象大小。
4. 片段有数据行时，请 storage 写入数据对象。
5. 片段没有数据行时，请 storage 只写入 Manifest empty coverage。
6. 由 storage 根据后端能力验证或信任对象写入结果。
7. 由 storage 更新 Manifest 覆盖范围记录。
8. 把 object metadata、empty coverage 和 skipped range summary 返回给调用方。
9. 让新的覆盖范围对后续查询可见。

如果对象写入失败，Manifest 覆盖范围不能变化。如果对象写入成功但 Manifest 更新失败，重试
应能收敛到一致状态，不破坏覆盖范围。

第一阶段通过拒绝持久化 unsafe range 来避免 durable rollback。如果未来需要更强的 canonical
chain proof，可以在覆盖记录中扩展 block hash、parent hash 或其他链族 canonicality proof。
这些 proof 是持久化验证能力的扩展，不是把不稳定 latest 数据写入 durable coverage 的许可。

## 这一步要实现什么

第一版存储实现应包含：

- object get、put、exists、list 和必要 delete 的 storage trait。
- local filesystem 或 MinIO-backed development implementation。
- 位于 `datalens-core` 或 `datalens-storage` 的 Manifest 数据结构。
- 能回答已覆盖、部分覆盖或缺失的覆盖范围匹配代码。
- 同一逻辑分片的幂等写入测试。
- 证明本地临时文件不会被当成持久化覆盖范围的测试。

这一步不需要决定所有未来 Parquet columns。它需要让覆盖范围事实足够精确，让查询规划可以
安全依赖它。
