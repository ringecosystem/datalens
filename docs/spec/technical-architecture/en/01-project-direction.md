# 01 - Project Direction

This step defines what datalens is building before any crate, API, schema, or storage
layout becomes permanent.

datalens should become a query-driven structured historical archive/cache for blockchain
data. EVM was the first implementation target, and the architecture must continue to
support Tron, Solana, and other chain families. The system is not only a compatibility
layer for any existing gateway protocol. Compatibility can be useful, but it must sit on
top of datalens-owned architecture.

## The Problem

Indexers and data applications repeatedly ask chain nodes for historical data. For small
or uncommon chains, public gateways may be unavailable, rate-limited, expensive, or shaped
around another provider's product assumptions. Direct RPC indexing still works, but it
forces every project to repeat the same historical reads, normalization, retry behavior,
and storage work.

datalens should make that repeated historical work reusable. When a caller asks for a
structured range of historical data, datalens should answer from durable cache when it can.
When the required data is missing, datalens should fetch the missing range from configured
chain sources, normalize it, persist it into durable object storage, update coverage
metadata, and then return the requested result.

## Product Shape

The core product is a service, not only a library. A user can run datalens as a persistent
cache service backed by object storage. Applications, indexers, and future SDKs call the
service to retrieve historical chain data.

The service should expose a native API that reflects datalens concepts: chain family,
chain identity, dataset, range, filters, field selection, coverage, and response stream.
An SDK can make that API easier to consume, but the SDK should not be the only valid way
to use the system.

Developers should still be able to index directly from RPC if they do not want datalens.
datalens should improve repeated historical access; it should not pretend to replace every
node method, mempool surface, pending-state query, or low-latency operational RPC use case.

## Technical Direction

The durable cache should store structured datasets, not arbitrary caller response blobs.
For EVM this includes block headers, logs, transactions, and receipts where needed. For
other chain families, the datasets may differ. The shared architecture should not force
Solana or Tron data into EVM concepts such as log topics or transaction receipts.

Object storage is the durable persistence layer. Local disk can be used as temporary
workspace for downloads, uploads, retries, and query execution, but it should not be the
long-term archive authority.

Coverage metadata is as important as the data itself. Because datalens will not archive
every possible chain field by default, the system must be able to say exactly what a
stored chunk covers: chain, dataset, range, schema/normalization version, filters, and
field coverage.

## What This Step Produces

The first project-direction work should produce:

- A shared understanding that datalens is a structured historical cache service.
- A clear decision that EVM is first but not the only supported chain family.
- A clear separation between datalens-owned native behavior and compatibility adapters.
- A durable object-storage-first posture.
- A development sequence that starts with stable architecture boundaries instead of
  jumping directly into gateway compatibility.

## What This Step Does Not Do

This step does not choose final Parquet schemas, final API endpoints, final object key
formats, or final SDK ergonomics. Those decisions come later, after the architecture has
made the system boundaries clear.

This step also does not require full-chain archival ingestion. datalens should be useful
when it stores only the structured historical data that callers actually need.
