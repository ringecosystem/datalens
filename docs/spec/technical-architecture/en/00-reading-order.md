# 00 - Technical Architecture Reading Order And Development Roadmap

This directory is the development blueprint for the datalens historical chain data
cache. It is not a set of abstract policy notes. Read the documents in numeric order
when you want to understand what the project is, how it should be built, and what each
implementation stage is expected to produce.

Every document has a paired Simplified Chinese version under
`docs/spec/technical-architecture/zh-CN/`. The two language versions should keep the
same file names, section order, technical identifiers, and implementation meaning.

## Directory Layout

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

## Reading Sequence

1. `01-project-direction.md`
   explains what datalens is trying to build and what it must not become. Start here
   before creating implementation issues, because it sets the product and engineering
   direction.

2. `02-system-architecture.md`
   explains the end-to-end system shape: request planning, cache lookup, missing data
   fill, object storage persistence, response streaming, and module ownership.

3. `03-storage-and-manifest.md`
   explains how durable object storage, chunks, manifests, coverage records, local
   scratch space, and idempotent writes should work.

4. `04-query-and-fill-flow.md`
   explains the main runtime path: how a query becomes a plan, how cache hits and misses
   are detected, and how missing ranges are fetched, written, and returned.

5. `05-chain-adapters-and-evm.md`
   explains the chain adapter model. EVM is the first concrete family, but the core
   must remain open to Tron, Solana, and other future families.

6. `06-api-sdk-and-compatibility.md`
   explains how users interact with datalens through native APIs, optional SDK helpers,
   and later compatibility adapters such as SQD Gateway-compatible surfaces.

## How To Use These Documents

Use the numbered order as the default development order. Earlier documents establish
constraints that later documents rely on. For example, storage work should not start by
inventing a local database authority, because `03-storage-and-manifest.md` makes object
storage and manifest coverage the durable authority.

When creating an implementation issue, reference the numbered document and the exact
section that the issue will implement. The issue should describe the code change and
validation steps; the document should describe the stable technical design behind that
issue.

When a later implementation discovers that the architecture needs to change, update the
affected English and Chinese documents in the same change. Do not add temporary draft
directories, issue-number directories, or one-off notes under this architecture tree.

## Writing Rules For This Architecture Set

These files should read like a concrete technical development plan, not like generic
policy. Avoid front-matter templates such as `Purpose`, `Status`, or `Read this when`.

Each numbered document should answer:

- What this part of the system is responsible for.
- Why the system needs this part.
- How the implementation should be shaped.
- What code modules or crates the work will likely affect.
- What the expected output of the implementation stage is.
- What should be verified before the stage is treated as complete.

Keep normative terms such as `MUST` and `SHOULD` only when they remove ambiguity. The
document should still be readable as a plan that a developer can follow.

## Development Roadmap

This section turns the architecture into an ordered build sequence. It is still a
technical development plan, not a complete issue list. Implementation issues should be
created from these stages with narrower acceptance criteria and verification commands.

## Stage 1 - Workspace And Boundaries

Build the Rust workspace and crate layout first. This stage creates the places where all
future behavior belongs.

Expected work:

- Confirm Rust edition 2024.
- Keep `cargo fmt`, `cargo check --workspace`, and CI checks ready.
- Create or verify crates for core, chain adapter boundary, EVM adapter, storage,
  planner, writer, API, and CLI/service entrypoint.
- Add minimal types for chain identity, range, dataset, coverage, and error categories.
- Keep functions small and mostly skeletal.

Done means the workspace compiles and future work has stable ownership boundaries.

## Stage 2 - Storage And Manifest Foundation

Build durable cache truth before building complex query behavior.

Expected work:

- Implement object storage abstraction.
- Provide local/MinIO development backend.
- Define manifest structures and coverage entries.
- Implement coverage matching for hit, partial hit, and miss.
- Implement deterministic chunk identity.
- Add idempotent write tests.

Done means the system can record and evaluate what historical data is durable, even if no
real chain adapter is wired yet.

## Stage 3 - EVM Adapter Minimum

Build the first real chain-family adapter without weakening chain-neutral boundaries.

Expected work:

- Configure EVM chains and RPC endpoints.
- Fetch latest and safe height.
- Fetch bounded logs by address/topic/range.
- Fetch block headers for ranges needed by the query plan.
- Normalize EVM responses into datalens-owned records.
- Classify provider errors.
- Test against mock or fixture-backed provider data.

Done means datalens can obtain real EVM historical data through the adapter boundary.

## Stage 4 - Query Planner And Demand Fill

Connect request planning, coverage lookup, missing range resolution, adapter fetch, writer
persistence, and response assembly.

Expected work:

- Define native query request/response model.
- Build bounded query plans.
- Check manifest coverage.
- Resolve missing ranges.
- Fetch missing ranges through `datalens-evm`.
- Persist normalized chunks.
- Update manifest after durable write.
- Assemble cached and newly fetched results.

Done means the system can demonstrate cache hit, partial hit, and miss behavior end to
end for at least one EVM dataset.

## Stage 5 - Native API

Expose the proven query/fill behavior through a service API.

Expected work:

- Add native query endpoint.
- Add status and height endpoints.
- Add response streaming.
- Add request limits and concurrency controls.
- Return structured errors.
- Add integration tests that start the service and query fixture-backed data.

Done means a consumer can use datalens service directly without knowing internal crates.

## Stage 6 - SDK Or Client Convenience

Add the smallest consumer-facing client layer that helps real users without owning all
indexing behavior too early.

Expected work:

- Provide typed request helpers.
- Provide pagination or streaming helpers.
- Provide examples for querying historical logs.
- Keep direct RPC indexing as a valid external workflow.
- Decide later whether SDK fallback-to-RPC belongs in scope.

Done means a developer can integrate with datalens ergonomically while the native API
remains the source of truth.

## Stage 7 - Compatibility Adapters

Only after native behavior is stable, add compatibility surfaces such as SQD Gateway
compatibility.

Expected work:

- Translate compatibility requests into native datalens requests.
- Reuse planner, storage, adapter, writer, and response assembly.
- Reshape native responses into compatibility response formats at the edge.
- Add compatibility tests with a real or fixture indexer.
- Keep compatibility fields out of the core storage schema.

Done means compatibility works as an adapter, not as the architecture.

## Stage 8 - Operations And Production Readiness

Harden the service for real deployments.

Expected work:

- Add metrics for cache hit/miss, fill latency, storage latency, provider failures, and
  manifest update failures.
- Add health checks.
- Add retry and backoff controls.
- Add object storage validation or repair commands.
- Add deployment examples.
- Add runbooks after behavior is stable.

Done means datalens can be operated as a persistent historical cache service.

## Stage 9 - Future Chain Families

Add Tron, Solana, or other adapters only after EVM has proven the chain-neutral contract.

Expected work:

- Define family-specific datasets.
- Implement a new adapter crate.
- Add family-specific fixtures.
- Store chunks under a new `chain-kind`.
- Prove no chain-neutral crate needs EVM-specific assumptions.

Done means datalens has validated that it is a multi-chain architecture, not just an EVM
cache with a generic name.
