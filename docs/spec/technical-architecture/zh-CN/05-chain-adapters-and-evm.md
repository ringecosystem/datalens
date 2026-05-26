# 05 - 链适配器和 EVM

这一步防止项目变成 EVM-only，同时让第一版有一个真实链家族可以实现。

## 适配器模型

链适配器将特定链家族的数据源转换成 datalens 的结构化数据集。核心系统请求范围和数据集；
适配器决定如何从对应链家族获取数据。

适配器应暴露：

- 链家族和链标识。
- 可用时的 latest 和 safe/finalized 高度信息。
- 支持的数据集和过滤条件。
- 有边界范围的拉取操作。
- 数据源健康状态和 provider 能力信息。
- 错误分类，用来区分可重试失败、永久不支持、provider 限制和无效输入。

核心模块不应该知道 EVM 是否使用 `eth_getLogs`，Solana 是否使用 slots，或者 Tron 是否有
不同的事件模型。

## EVM 优先

第一个适配器应是 `datalens-evm`。它应支持能证明按查询驱动历史缓存的最小数据集：

- Block headers。
- 按 address 和 topics 过滤的 logs。
- 请求需要时的 transactions。
- 只有计划响应需要时才拉取 receipts。

第一条补齐路径可以聚焦 logs，因为 logs 是常见索引基础数据，而且适合选择性缓存。Block
headers 也应尽早加入，因为很多索引器需要 height、hash、parent hash 和 timestamp 上下文。

## EVM 数据源策略

适配器应先支持可配置 RPC endpoints。IPC 或 node-local transports 可以后续作为 provider
选项增加，但 datalens 不应该因为支持某种适配器传输方式就声称替代所有 RPC 工作流。

Provider 行为应隔离在 `datalens-evm`：batching、rate-limit backoff、range splitting、
retry decisions 和 EVM response decoding 都属于这里。数据离开适配器边界后，应被标准化成
datalens 数据集记录。

## 未来 Tron 和 Solana 支持

未来适配器不应该被迫模仿 EVM。Solana 可能使用 slots 和 account changes；Tron 可能有自己的
事件和交易表面。这些概念应该成为同一个适配器边界后面的链家族专属数据集。

增加新链家族应需要：

1. 新适配器 crate，例如 `datalens-solana` 或 `datalens-tron`。
2. 该链家族的数据集定义。
3. 该链家族的拉取和标准化代码。
4. 让规划器知道适配器能满足什么的能力元数据。
5. 包含新 `chain-kind` 的存储分片和覆盖范围记录。

它不应该要求重写 `datalens-core`、`datalens-storage` 或原生 API。

## 这一步要实现什么

第一版适配器实现应包含：

- 链适配器 traits 或等价接口。
- chain name、chain id 和 RPC URLs 的 EVM 适配器配置。
- Safe/latest height lookup。
- 有边界范围上的 EVM logs fetch。
- 标准化到 datalens 自有 log records。
- Provider error classification。
- 使用 mock provider 或 recorded fixture 的测试。

这一步完成后，查询/补齐闭环应能获取真实或 fixture-backed EVM logs，同时不把 EVM 假设嵌入
链无关 crates。
