# Chain Adapter Contract

Purpose: Define the conceptual chain adapter contract that keeps datalens chain-neutral
while allowing EVM to be the first concrete chain-family target.

Status: normative

Read this when: You are designing chain adapter boundaries, reviewing chain-family
modules, or checking whether EVM assumptions are leaking into chain-neutral modules.

Not this document: This document does not define Rust trait signatures, final Parquet
schemas, object key layouts, concrete RPC algorithms, adapter implementation code, or
final Tron/Solana schemas.

Defines:

- Chain adapter role and ownership boundary.
- Required adapter capabilities at a conceptual level.
- EVM-first dataset families.
- Future chain-family namespace rules.
- External reference and RPC/SDK boundary rules.

Paired translation: `../zh-CN/chain-adapter-contract.md`

## Adapter Role

A chain adapter MUST convert chain-family-specific sources into datalens structured
datasets.

Chain-family-specific sources include RPC responses, provider SDK responses, archival
provider exports, indexed source snapshots, and other source forms native to a chain
family.

A chain adapter MUST own source-specific fetch behavior, source response interpretation,
chain-family normalization, and source error mapping for its chain family.

A chain adapter MUST return normalized dataset results through the `datalens-chain`
boundary. It MUST NOT expose adapter-native payloads as durable cache authority.

Chain-neutral modules MUST depend on adapter capabilities, dataset identities, ranges,
coverage requirements, and normalized result envelopes. They MUST NOT depend on EVM RPC
method names, EVM log semantics, EVM receipt semantics, EVM transaction envelope details,
or any other chain-family-native structure.

The first concrete adapter target is EVM. EVM support MUST be implemented as an EVM
chain-family adapter, not as the universal shape of datalens core concepts.

## Required Adapter Capabilities

Every chain adapter MUST describe its chain identity.

Chain identity MUST distinguish at least the chain family and the concrete network or
chain instance. For EVM, this MAY include EVM chain id and network name. Other chain
families MAY use different native identifiers.

Every chain adapter MUST expose latest height discovery when the underlying source can
report a current head.

Every chain adapter MUST expose finalized or safe height discovery when the underlying
chain family and source provide a finalized, safe, confirmed, or equivalent stability
concept.

When a chain family has no native finalized/safe concept, the adapter MUST report that
capability explicitly instead of inventing EVM-style finality.

Every chain adapter MUST support bounded range fetches for the datasets it advertises.

Range fetch capability MUST state the accepted range unit for the chain-family dataset.
For EVM-first datasets, the primary range unit is block height.

Every chain adapter MUST expose dataset support discovery so the planner can determine
which structured datasets, field groups, and range units the adapter can satisfy.

Dataset support discovery MUST be chain-family-specific enough to avoid treating an EVM
dataset as mandatory for every chain family.

Every chain adapter MUST expose source health signals that allow orchestration to
distinguish healthy, degraded, unavailable, lagging, rate-limited, and misconfigured
source states at a conceptual level.

Every chain adapter MUST classify source and normalization failures into stable
chain-cache error categories. Error classification MUST preserve whether a failure is
retryable, permanent for the request, caused by caller input, caused by source health, or
caused by unsupported capability.

## Range Fetch Contract

A range fetch request MUST be bounded before it reaches a chain adapter.

A range fetch request MUST name the chain identity, dataset family, range, required field
coverage or canonical dataset shape, and finalized/safe height policy that the planner
requires.

A chain adapter MUST fetch only datasets and ranges required by the request. It MAY fetch
source-side supporting data when that data is necessary to produce the requested
structured dataset, but it MUST NOT force unrelated datalens datasets to become durable
coverage.

A range fetch result MUST identify the dataset family, concrete chain identity, covered
range, source height basis, and whether the result satisfies finalized/safe or provisional
coverage.

A chain adapter MUST not mark data durable. Durable coverage belongs to the writer and
storage workflow after structured chunks are written successfully.

## EVM-First Dataset Families

The EVM adapter MUST use an EVM dataset namespace. EVM dataset identities MUST remain
EVM-specific and MUST NOT become universal dataset names for all chain families.

The EVM-first dataset families are:

| Dataset family | Purpose | Fetch posture |
| --- | --- | --- |
| `evm.block_headers` | EVM block header data needed for block range navigation, timestamps, hashes, parent linkage, and finality/safe-height reasoning. | Core EVM dataset for most block-height range plans. |
| `evm.logs` | EVM event log records emitted by contract execution, including address, topics, data, transaction position, and block position. | Fetched when query semantics require logs or event-derived data. |
| `evm.transactions` | EVM transaction records and transaction placement within blocks. | Fetched when query semantics require transaction-level data. |
| `evm.receipts` | EVM transaction receipt data, including execution result and receipt-only fields needed by planned queries. | Fetched only where needed; logs queries MAY require receipt-derived source data, but the cache MUST track durable dataset coverage explicitly. |

EVM receipts MUST NOT be fetched for every EVM query by default. Receipts are required
when the planned dataset or requested fields need receipt-only information.

EVM logs MUST remain EVM dataset semantics. Core modules MUST NOT assume that every chain
family has logs, topics, transaction receipts, or Ethereum-compatible block structures.

## Future Chain Families

Future Tron and Solana adapters MAY have different native concepts from EVM.

Future Tron and Solana adapters MUST map their native concepts into their own
chain-family dataset namespaces, such as future `tron.*` or `solana.*` dataset families,
instead of forcing EVM semantics onto their data.

A future Tron adapter MUST NOT be required to expose `evm.logs`, `evm.receipts`, or
Ethereum-compatible transaction envelope semantics unless that adapter explicitly offers
an EVM-compatibility dataset as an edge compatibility surface.

A future Solana adapter MUST NOT be required to model data as EVM blocks, logs,
transactions, and receipts when Solana-native datasets require different names, ranges,
or stability concepts.

Chain-neutral modules MAY ask whether an adapter supports a planned dataset. They MUST
NOT assume a planned dataset exists because it exists for EVM.

This document does not decide final Tron or Solana schemas, object layouts, range units,
or source algorithms.

## External References

External references such as subsquid, eth-archive, provider RPC documentation, and
archive service behavior MAY inform datalens design.

External references MUST NOT define datalens implementation logic, module ownership,
dataset authority, storage coverage rules, or adapter contracts.

When an external source shape conflicts with the datalens chain adapter contract, datalens
MUST normalize through the adapter boundary instead of making core modules depend on the
external source shape.

## RPC And SDK Boundary

datalens provides a cache service and MAY provide SDK/API surfaces for callers that want
structured cached access.

datalens does not have to replace developers' ability to index directly from RPC,
provider SDKs, or other chain-native sources.

SDK/API compatibility behavior MUST sit at the edge. It MUST NOT redefine the chain
adapter contract or force chain-neutral modules to expose RPC-native payloads.

Direct RPC indexing and datalens cache-backed access MAY coexist. The corrected boundary
is that datalens owns structured cache materialization and optional access surfaces, not
exclusive control over every developer indexing path.

## Non-Goals

This document MUST NOT be used as authority for exact Rust trait signatures.

This document MUST NOT be used as authority for adapter implementation code.

This document MUST NOT be used as authority for final Tron or Solana schemas.

This document MUST NOT be used to force EVM datasets into chain-neutral modules.
