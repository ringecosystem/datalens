# Chain-Cache Project Charter

Purpose: Define the normative project identity, scope boundary, goals, and non-goals for
the datalens chain-cache project.

Status: normative

Read this when: You need to decide whether proposed chain-cache work belongs in
datalens, or when implementation work needs a stable project scope boundary.

Not this document: This document does not define module APIs, storage object keys,
manifest fields, runtime topology, or implementation procedures.

Defines:

- What datalens chain-cache is.
- What datalens chain-cache must leave room for.
- What datalens chain-cache is not.
- Which reference projects may inform the work without controlling implementation.

## Project Definition

datalens chain-cache is a query-driven structured historical archive/cache service for
blockchain data.

The service exists to persist selected historical chain data in structured forms that can
be consumed through datalens-owned SDKs and APIs. It favors durable, reusable historical
datasets over one-off live RPC reads, and it favors query-shaped capture over collecting
all possible chain data by default.

The first target chain family is EVM. The architecture must still leave room for Tron,
Solana, and other future chain families without making EVM-only assumptions part of the
project identity.

## Scope Boundary

datalens chain-cache is not merely an SQD compatibility layer. SQD-compatible behavior may
be useful where it serves datalens users, but compatibility is not the project definition
and must not prevent datalens-owned data models, APIs, or execution choices.

datalens chain-cache is not a full RPC replacement. It may answer historical and
structured queries that would otherwise require repeated RPC access, but it does not aim
to expose every live node method, every mempool or pending-state surface, or every
low-latency operational RPC behavior.

## Core Goals

- Persistent historical cache: Store reusable historical chain data so later consumers do
  not need to repeatedly fetch and normalize the same source ranges.
- SDK/API consumption: Expose captured data through stable datalens-owned SDK and API
  surfaces suitable for application and indexing workflows.
- Object-storage durability: Treat object storage as a durable backing layer for
  historical cache artifacts, so cached data can survive process restarts and hot-storage
  turnover.
- Selective structured data capture: Capture data according to selected query needs and
  structured data shapes instead of requiring complete chain-wide capture by default.
- Future chain-family support: Keep the design open to EVM, Tron, Solana, and other chain
  families, with family-specific behavior isolated behind datalens-owned boundaries.

## Non-Goals

- Full-chain complete archive by default: datalens chain-cache must not require complete
  archival capture of every block, transaction, receipt, trace, log, state item, or chain
  surface before it can be useful.
- Mandatory hot data storage: datalens chain-cache must not require all cached historical
  data to remain in always-online database storage when object storage can provide the
  durable source of truth for colder artifacts.
- Multi-tenant billing: Billing, tenant metering, pricing plans, invoices, and account
  subscription management are outside the chain-cache project charter.
- P2P scheduling: Peer-to-peer work distribution, decentralized scheduling, and network
  participant incentive design are outside the chain-cache project charter.
- Copying GPL code from reference projects: datalens implementation must not copy GPL code
  from reference projects into this repository.

## Reference Material

`subsquid/eth-archive` and related SQD archive work are reference material only. They may
inform terminology, tradeoff analysis, compatibility decisions, and operational lessons,
but they do not define datalens implementation ownership.

All production implementation must use datalens-owned logic, datalens-owned interfaces,
and license-compatible dependencies.
