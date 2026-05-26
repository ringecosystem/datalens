# Chain-Cache 项目章程

Purpose: 定义 datalens chain-cache 项目的规范性项目身份、范围边界、目标和非目标。

Status: normative

Read this when: 需要判断拟议的 chain-cache 工作是否属于 datalens，或实现工作需要稳定的项目范围边界时阅读。

Not this document: 本文档不定义模块 API、存储对象 key、manifest 字段、运行时拓扑或实现流程。

Defines:

- datalens chain-cache 是什么。
- datalens chain-cache 必须为哪些方向保留空间。
- datalens chain-cache 不是什么。
- 哪些参考项目可以提供参考，但不能控制实现。

## 项目定义

datalens chain-cache 是面向区块链数据的 query-driven structured historical archive/cache service。

该服务用于以结构化形式持久化选定的历史链上数据，并通过 datalens 自有的 SDK 和 API 供消费者使用。它优先支持可复用、持久化的历史数据集，而不是一次性的 live RPC 读取；它优先按查询需求捕获数据，而不是默认收集所有可能的链上数据。

第一个目标链族是 EVM。架构仍必须为 Tron、Solana 和其他未来链族保留空间，不得让只适用于 EVM 的假设成为项目身份的一部分。

## 范围边界

datalens chain-cache 不只是 SQD compatibility layer。SQD-compatible 行为在服务 datalens 用户时可以有价值，但兼容性不是项目定义，也不得阻止 datalens 自有的数据模型、API 或执行选择。

datalens chain-cache 不是完整 RPC replacement。它可以回答原本需要重复 RPC 访问的历史和结构化查询，但不以暴露每一个 live node method、每一个 mempool 或 pending-state surface，或每一种低延迟运维 RPC 行为为目标。

## 核心目标

- Persistent historical cache: 存储可复用的历史链上数据，使后续消费者不需要重复拉取和规范化相同的源数据范围。
- SDK/API consumption: 通过稳定的 datalens 自有 SDK 和 API surface 暴露已捕获数据，供应用和 indexing 工作流使用。
- Object-storage durability: 将 object storage 作为历史 cache artifacts 的持久化 backing layer，使缓存数据能够在进程重启和 hot-storage turnover 后继续存在。
- Selective structured data capture: 根据选定的查询需求和结构化数据形态捕获数据，而不是默认要求完整的全链捕获。
- Future chain-family support: 让设计持续开放给 EVM、Tron、Solana 和其他链族，并将链族特定行为隔离在 datalens 自有边界之后。

## 非目标

- Full-chain complete archive by default: datalens chain-cache 不得要求先完整捕获每一个 block、transaction、receipt、trace、log、state item 或 chain surface，才能产生价值。
- Mandatory hot data storage: 当 object storage 可以作为较冷 artifacts 的 durable source of truth 时，datalens chain-cache 不得要求所有缓存的历史数据都常驻 always-online database storage。
- Multi-tenant billing: Billing、tenant metering、pricing plans、invoices 和 account subscription management 不属于 chain-cache 项目章程范围。
- P2P scheduling: Peer-to-peer work distribution、decentralized scheduling 和 network participant incentive design 不属于 chain-cache 项目章程范围。
- Copying GPL code from reference projects: datalens 实现不得从参考项目复制 GPL code 到本仓库。

## 参考材料

`subsquid/eth-archive` 和相关 SQD archive work 仅作为参考材料。它们可以用于术语、tradeoff analysis、compatibility decisions 和 operational lessons，但不定义 datalens 的实现所有权。

所有生产实现都必须使用 datalens-owned logic、datalens-owned interfaces 和 license-compatible dependencies。
