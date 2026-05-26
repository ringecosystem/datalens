# 00 - 技术架构阅读顺序和开发路线

这个目录是 datalens 历史链数据缓存项目的开发蓝图，不是一组抽象规则。需要理解这个
项目要做什么、怎么做、每一步产出什么时，按编号顺序阅读这些文档。

每一份文档都有对应的英文版本，位置在
`docs/spec/technical-architecture/en/`。中英文版本应保持相同文件名、相同章节顺序、
相同技术标识符，以及相同实现含义。

## 目录结构

```text
docs/spec/technical-architecture/
├── en/
│   ├── 00-reading-order.md
│   ├── 01-project-direction.md
│   ├── 02-system-architecture.md
│   ├── 03-storage-and-manifest.md
│   ├── 04-query-and-fill-flow.md
│   ├── 05-chain-adapters-and-evm.md
│   └── 06-api-sdk-and-compatibility.md
└── zh-CN/
    ├── 00-reading-order.md
    ├── 01-project-direction.md
    ├── 02-system-architecture.md
    ├── 03-storage-and-manifest.md
    ├── 04-query-and-fill-flow.md
    ├── 05-chain-adapters-and-evm.md
    └── 06-api-sdk-and-compatibility.md
```

## 阅读顺序

1. `01-project-direction.md`
   说明 datalens 要构建什么，以及它不应该变成什么。创建实现任务之前先读这份，
   因为它决定产品方向和工程边界。

2. `02-system-architecture.md`
   说明系统端到端形态：请求规划、缓存查找、缺失数据补齐、对象存储持久化、响应流式返回，
   以及模块归属。

3. `03-storage-and-manifest.md`
   说明持久化对象存储、数据分片、Manifest、覆盖范围记录、临时本地工作区和幂等写入应该
   如何工作。

4. `04-query-and-fill-flow.md`
   说明最核心的运行路径：查询如何变成执行计划，如何判断缓存命中/未命中，以及缺失范围
   如何被拉取、写入和返回。

5. `05-chain-adapters-and-evm.md`
   说明链适配器模型。EVM 是第一个具体链家族，但核心模块必须为 Tron、Solana 和其他未来
   链家族保留空间。

6. `06-api-sdk-and-compatibility.md`
   说明用户如何通过原生 API、可选 SDK 帮助方法，以及后续兼容适配层使用 datalens。

## 如何使用这些文档

默认按编号顺序使用。前面的文档会给后面的文档建立约束。例如存储工作不能一开始就把
本地数据库设计成权威数据源，因为 `03-storage-and-manifest.md` 会把对象存储和 Manifest
覆盖范围定义为持久化依据。

创建实现任务时，要引用对应编号文档和具体章节。任务描述代码改动和验证步骤；这些文档
描述支撑任务的稳定技术方案。

如果后续实现发现架构需要调整，应在同一次改动中更新对应的中英文文档。不要在这个架构
目录下添加临时草稿目录、任务编号目录或一次性笔记。

## 这套架构文档的写法

这些文件应该像具体的技术开发方案，而不是通用规则。避免使用 `Purpose`、`Status`、
`Read this when` 这类模板。

每一份编号文档都应该回答：

- 这一部分系统负责什么。
- 系统为什么需要这一部分。
- 实现应该如何组织。
- 可能影响哪些代码模块或 crates。
- 这个实现阶段的预期产出是什么。
- 阶段完成前应该验证什么。

只有在能消除歧义时才使用 `MUST` 和 `SHOULD` 这类规范词。整体阅读体验仍然应该像一份
开发者可以照着推进的方案。

## 开发路线

这部分把架构转换成有顺序的构建路线。它仍然是技术开发方案，不是完整任务列表。具体实现
任务应从这些阶段拆出来，并写更窄的验收标准和验证命令。

## Stage 1 - Workspace And Boundaries

先构建 Rust workspace 和 crate layout。这一步创建所有未来行为应该放置的位置。

预期工作：

- 确认 Rust edition 2024。
- 保持 `cargo fmt`、`cargo check --workspace` 和 CI checks 可用。
- 创建或确认 core、chain adapter boundary、EVM adapter、storage、planner、writer、API 和
  CLI/service entrypoint crates。
- 增加链标识、范围、数据集、覆盖范围和错误类别的最小类型。
- 函数保持小而偏 skeleton。

完成标准是 workspace 可以编译，后续工作有稳定的归属边界。

## Stage 2 - Storage And Manifest Foundation

先构建持久化缓存依据，再构建复杂查询行为。

预期工作：

- 实现对象存储抽象。
- 提供 local/MinIO development backend。
- 定义 Manifest 结构和覆盖范围记录。
- 实现命中、部分命中和未命中的覆盖范围匹配。
- 实现确定性的分片标识。
- 增加幂等写入测试。

完成标准是系统可以记录和判断哪些历史数据已持久化，即使还没有接入真实链适配器。

## Stage 3 - EVM Adapter Minimum

构建第一个真实链家族适配器，同时不削弱链无关边界。

预期工作：

- 配置 EVM chains 和 RPC endpoints。
- 获取 latest 和 safe height。
- 按 address/topic/range 拉取有边界的 logs。
- 为查询计划需要的范围拉取 block headers。
- 将 EVM responses 标准化成 datalens 自有 records。
- 分类 provider errors。
- 使用 mock 或 fixture-backed provider data 测试。

完成标准是 datalens 可以通过适配器边界获取真实 EVM 历史数据。

## Stage 4 - Query Planner And Demand Fill

连接请求规划、覆盖范围查找、缺失范围解析、适配器拉取、写入器持久化和响应组装。

预期工作：

- 定义原生查询请求/响应模型。
- 构建有边界的查询计划。
- 检查 Manifest 覆盖范围。
- 解析缺失范围。
- 通过 `datalens-evm` 拉取缺失范围。
- 持久化标准化分片。
- 持久化写入后更新 Manifest。
- 合并缓存数据和新拉取结果。

完成标准是系统能针对至少一个 EVM 数据集端到端展示缓存命中、部分命中和未命中行为。

## Stage 5 - Native API

通过服务 API 暴露已经证明过的查询/补齐行为。

预期工作：

- 增加原生查询 endpoint。
- 增加 status 和 height endpoints。
- 增加流式响应。
- 增加 request limits 和 concurrency controls。
- 返回结构化错误。
- 增加启动服务并查询 fixture-backed data 的集成测试。

完成标准是使用方可以直接使用 datalens 服务，而不需要了解内部 crates。

## Stage 6 - SDK Or Client Convenience

增加最小的面向用户客户端层，帮助真实用户使用，但不要过早拥有全部索引行为。

预期工作：

- 提供类型化请求帮助方法。
- 提供分页或流式响应帮助方法。
- 提供查询历史 logs 的 examples。
- 保持直接 RPC 索引是合法外部工作流。
- 后续再决定 SDK fallback-to-RPC 是否属于范围。

完成标准是开发者可以更方便地集成 datalens，同时原生 API 仍是行为依据。

## Stage 7 - Compatibility Adapters

只有原生行为稳定之后，再增加网关兼容这类兼容适配层。

预期工作：

- 将外部协议请求转成 datalens 原生请求。
- 复用规划器、存储、适配器、写入器和响应组装层。
- 在边界层把原生响应转换成外部协议响应格式。
- 使用真实或 fixture indexer 增加兼容测试。
- 不让兼容字段进入核心存储 schema。

完成标准是兼容能力作为适配层工作，而不是变成架构本身。

## Stage 8 - Operations And Production Readiness

将服务强化到真实部署可用。

预期工作：

- 增加缓存命中/未命中、补齐延迟、存储延迟、provider 失败和 Manifest 更新失败指标。
- 增加 health checks。
- 增加 retry 和 backoff controls。
- 增加对象存储验证或修复命令。
- 增加 deployment examples。
- 行为稳定后再增加 runbooks。

完成标准是 datalens 可以作为持久化历史缓存服务运维。

## Stage 9 - Future Chain Families

只有在 EVM 已经证明链无关契约后，再增加 Tron、Solana 或其他适配器。

预期工作：

- 定义链家族专属数据集。
- 实现新的适配器 crate。
- 增加链家族专属 fixtures。
- 使用新的 `chain-kind` 存储分片。
- 证明没有链无关 crate 需要 EVM-specific assumptions。

完成标准是 datalens 验证自己是多链架构，而不是名字通用的 EVM cache。
