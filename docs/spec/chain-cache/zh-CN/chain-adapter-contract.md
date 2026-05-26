# Chain Adapter Contract

Purpose: 定义 conceptual chain adapter contract，使 datalens 保持 chain-neutral，同时允许
EVM 成为第一个 concrete chain-family target。

Status: normative

Read this when: 当你要设计 chain adapter boundaries、审查 chain-family modules，或检查
EVM assumptions 是否泄漏到 chain-neutral modules 时阅读本文件。

Not this document: 本文件不定义 Rust trait signatures、final Parquet schemas、object key
layouts、concrete RPC algorithms、adapter implementation code 或 final Tron/Solana
schemas。

Defines:

- Chain adapter role and ownership boundary。
- Required adapter capabilities at a conceptual level。
- EVM-first dataset families。
- Future chain-family namespace rules。
- External reference and RPC/SDK boundary rules。

Paired translation: `../en/chain-adapter-contract.md`

## Adapter Role

Chain adapter MUST 将 chain-family-specific sources 转换为 datalens structured
datasets。

Chain-family-specific sources 包括 RPC responses、provider SDK responses、archival
provider exports、indexed source snapshots，以及其他 chain family native 的 source
forms。

Chain adapter MUST 为其 chain family 拥有 source-specific fetch behavior、source
response interpretation、chain-family normalization，以及 source error mapping。

Chain adapter MUST 通过 `datalens-chain` boundary 返回 normalized dataset results。它
MUST NOT 将 adapter-native payloads 暴露为 durable cache authority。

Chain-neutral modules MUST 依赖 adapter capabilities、dataset identities、ranges、
coverage requirements 与 normalized result envelopes。它们 MUST NOT 依赖 EVM RPC
method names、EVM log semantics、EVM receipt semantics、EVM transaction envelope
details，或任何其他 chain-family-native structure。

第一个 concrete adapter target 是 EVM。EVM support MUST 作为 EVM chain-family adapter
实现，而不是作为 datalens core concepts 的 universal shape。

## Required Adapter Capabilities

Every chain adapter MUST describe its chain identity。

Chain identity MUST 至少区分 chain family 与 concrete network 或 chain instance。对
EVM 而言，这 MAY include EVM chain id and network name。其他 chain families MAY 使用
不同的 native identifiers。

Every chain adapter MUST expose latest height discovery，当 underlying source 可以报告
current head 时。

Every chain adapter MUST expose finalized or safe height discovery，当 underlying chain
family 与 source 提供 finalized、safe、confirmed 或 equivalent stability concept 时。

当某个 chain family 没有 native finalized/safe concept 时，adapter MUST 明确报告该
capability，而不是发明 EVM-style finality。

Every chain adapter MUST support bounded range fetches for the datasets it advertises。

Range fetch capability MUST state the accepted range unit for the chain-family dataset。
对于 EVM-first datasets，primary range unit 是 block height。

Every chain adapter MUST expose dataset support discovery，使 planner 能判断 adapter
可以满足哪些 structured datasets、field groups 与 range units。

Dataset support discovery MUST 足够 chain-family-specific，以避免把 EVM dataset 视为
每个 chain family 的 mandatory dataset。

Every chain adapter MUST expose source health signals，使 orchestration 能在 conceptual
level 区分 healthy、degraded、unavailable、lagging、rate-limited 与 misconfigured
source states。

Every chain adapter MUST 将 source and normalization failures 分类到 stable
chain-cache error categories。Error classification MUST 保留 failure 是 retryable、
permanent for the request、caused by caller input、caused by source health，还是 caused
by unsupported capability。

## Range Fetch Contract

Range fetch request MUST 在到达 chain adapter 之前 bounded。

Range fetch request MUST 指定 chain identity、dataset family、range、required field
coverage 或 canonical dataset shape，以及 planner 要求的 finalized/safe height
policy。

Chain adapter MUST 只 fetch request 所需的 datasets and ranges。当 source-side
supporting data 是生成 requested structured dataset 的必要条件时，它 MAY fetch 这些
data，但 MUST NOT 强制 unrelated datalens datasets 成为 durable coverage。

Range fetch result MUST 标识 dataset family、concrete chain identity、covered range、
source height basis，以及 result 是否满足 finalized/safe 或 provisional coverage。

Chain adapter MUST not mark data durable。Durable coverage 属于 writer and storage
workflow，并且只在 structured chunks 成功写入之后产生。

## EVM-First Dataset Families

EVM adapter MUST 使用 EVM dataset namespace。EVM dataset identities MUST remain
EVM-specific，并且 MUST NOT 成为所有 chain families 的 universal dataset names。

The EVM-first dataset families are:

| Dataset family | Purpose | Fetch posture |
| --- | --- | --- |
| `evm.block_headers` | EVM block header data，用于 block range navigation、timestamps、hashes、parent linkage 与 finality/safe-height reasoning。 | 大多数 block-height range plans 的 core EVM dataset。 |
| `evm.logs` | EVM event log records，由 contract execution emitted，包括 address、topics、data、transaction position 与 block position。 | 当 query semantics require logs 或 event-derived data 时 fetch。 |
| `evm.transactions` | EVM transaction records 与 transactions 在 blocks 中的位置。 | 当 query semantics require transaction-level data 时 fetch。 |
| `evm.receipts` | EVM transaction receipt data，包括 execution result 以及 planned queries 所需的 receipt-only fields。 | Only where needed 才 fetch；logs queries MAY require receipt-derived source data，但 cache MUST explicitly track durable dataset coverage。 |

EVM receipts MUST NOT be fetched for every EVM query by default。Receipts are required
when the planned dataset or requested fields need receipt-only information。

EVM logs MUST remain EVM dataset semantics。Core modules MUST NOT assume that every chain
family has logs、topics、transaction receipts，或 Ethereum-compatible block structures。

## Future Chain Families

Future Tron and Solana adapters MAY have different native concepts from EVM。

Future Tron and Solana adapters MUST 将其 native concepts 映射到自己的 chain-family
dataset namespaces，例如 future `tron.*` 或 `solana.*` dataset families，而不是把 EVM
semantics 强加到它们的数据上。

Future Tron adapter MUST NOT be required to expose `evm.logs`、`evm.receipts`，或
Ethereum-compatible transaction envelope semantics，除非该 adapter 明确提供
EVM-compatibility dataset as an edge compatibility surface。

Future Solana adapter MUST NOT be required to model data as EVM blocks、logs、
transactions and receipts，当 Solana-native datasets 需要不同的 names、ranges 或
stability concepts 时。

Chain-neutral modules MAY ask whether an adapter supports a planned dataset。它们 MUST
NOT assume a planned dataset exists because it exists for EVM。

This document does not decide final Tron or Solana schemas、object layouts、range units，
或 source algorithms。

## External References

External references such as subsquid、eth-archive、provider RPC documentation，以及
archive service behavior MAY inform datalens design。

External references MUST NOT define datalens implementation logic、module ownership、
dataset authority、storage coverage rules，或 adapter contracts。

当 external source shape 与 datalens chain adapter contract 冲突时，datalens MUST
through the adapter boundary normalize，而不是让 core modules 依赖 external source
shape。

## RPC And SDK Boundary

datalens provides a cache service，并且 MAY provide SDK/API surfaces 给需要 structured
cached access 的 callers。

datalens does not have to replace developers' ability to index directly from RPC、
provider SDKs，或 other chain-native sources。

SDK/API compatibility behavior MUST sit at the edge。它 MUST NOT redefine the chain
adapter contract，也 MUST NOT force chain-neutral modules to expose RPC-native payloads。

Direct RPC indexing and datalens cache-backed access MAY coexist。Corrected boundary 是
datalens owns structured cache materialization and optional access surfaces，而不是
exclusive control over every developer indexing path。

## Non-Goals

This document MUST NOT be used as authority for exact Rust trait signatures。

This document MUST NOT be used as authority for adapter implementation code。

This document MUST NOT be used as authority for final Tron or Solana schemas。

This document MUST NOT be used to force EVM datasets into chain-neutral modules。
