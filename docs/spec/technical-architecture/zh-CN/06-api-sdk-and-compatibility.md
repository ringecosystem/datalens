# 06 - API、SDK 和兼容层

这一步决定用户如何使用 datalens，同时避免任何单一外部协议控制内部架构。

## 原生服务 API

原生 API 是主要服务契约。它应该暴露 datalens 自己的概念，而不是复制某个网关的 schema：

- 链家族和链名称。
- 数据集选择。
- 范围选择。
- 过滤条件和谓词。
- 字段选择。
- 安全高度或 finalized 高度策略。
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

## SDK 角色

SDK 是便捷层。它可以提供类型化请求、分页帮助方法、重试帮助方法、认证帮助方法，以及给
索引器作者使用的集成工具。

SDK 不一定要拥有完整索引运行时。datalens 本身已经是一个能回答历史结构化查询的服务。未来
SDK 可以在两种形态之间选择：

- 仅服务客户端：SDK 只和 datalens 通信，直接 RPC 索引交给用户自己的工具。
- 混合帮助层：当 datalens 未配置或请求超出 datalens 范围时，SDK 可以选择回退到直接 RPC。

这个决策应根据真实集成需要后续再做。架构只要求直接 RPC 索引仍然是合法工作流。

## 兼容适配层

兼容适配层应位于系统边界。后续可以通过把外部协议请求转换成 datalens 原生请求，再把原生
响应转换成外部协议要求的响应格式来实现兼容。

兼容层不能定义：

- 核心数据集名称。
- Manifest 覆盖范围语义。
- 分片存储格式。
- 链适配器接口。
- 规划器行为。

这样 datalens 才能服务多种调用方，而不用把同一份历史数据存成多种调用方专属格式。

## 这一步要实现什么

第一版 API 实现应包含：

- 原生查询 endpoint。
- Height 或 status endpoint。
- 大结果的流式响应。
- 输入无效、不支持的数据集、provider 失败和存储失败的清晰错误响应。
- 只有在原生 API 稳定后才增加小型 SDK 或客户端示例。

兼容层不应该最先做。它应该在原生请求、规划器、存储和 EVM 补齐路径被证明可行之后再做。
