# 02 - System Architecture

This step turns the project direction into a concrete system shape. The goal is to make
every implementation issue land in the right module instead of mixing API compatibility,
EVM details, query planning, storage, and persistence into one service file.

The architecture is built around one runtime loop: a caller asks for historical data,
datalens plans the required structured datasets, checks durable coverage, fills missing
ranges, persists the fill, and streams a response.

## End-To-End Flow

The canonical flow is:

1. SDK/API request enters the edge.
2. Edge validation checks transport-level correctness.
3. Compatibility translation converts external vocabulary into native datalens vocabulary
   when needed.
4. Query planner builds an executable plan.
5. Storage reads manifest coverage.
6. Planner classifies ranges as covered, partially covered, or missing.
7. Storage reads covered chunks from object storage.
8. Missing range resolver creates fetch work.
9. Chain adapter fetches missing ranges from chain sources.
10. Writer normalizes and persists chunks.
11. Manifest is updated after durable writes succeed.
12. Response assembly streams one coherent result.

The implementation may pipeline these operations for performance, but the observable
meaning should stay the same. Missing data must not be marked covered before it is durably
written. Compatibility output must not leak into the core storage model.

## Crate And Module Boundaries

`datalens-core` holds shared vocabulary: chain family, chain identity, range, dataset,
coverage level, normalized result envelope, and common error categories. It should not
know how EVM RPC works or how S3 clients are configured.

`datalens-chain` defines the chain adapter boundary. It should describe what an adapter
can provide, how capabilities are reported, and how normalized fetch results move back
into the chain-neutral system.

`datalens-evm` is the first chain-family adapter. It owns EVM RPC/provider integration,
EVM-specific pagination, EVM response normalization, and EVM error interpretation. It
should not decide storage policy or native API shape.

`datalens-storage` owns object storage access, object encoding, object keys, object bytes,
manifest reads and writes, durable chunk existence checks, and local scratch workspace
behavior. It should be the only boundary that treats object storage as persistence
authority.

`datalens-planner` owns query planning. It converts native requests into executable plans,
validates ranges and capabilities, decides what datasets are required, and works with
manifest coverage to identify missing work.

`datalens-writer` owns durable write policy and coordination. It receives normalized
fetched segments, merges adjacent compatible sparse segments, delegates object and
manifest writes to storage, records empty coverage when configured, tracks skipped ranges,
and returns the storage metadata summary to the query flow. It must not own object
encoding, object key layout, object store providers, or manifest repository details.

`datalens-edge` owns the multi-transport service boundary: REST routes, GraphQL,
GraphiQL, metrics, discovery, warmup task operations, authentication, application
authorization, quota checks, service registry routing, and native query entrypoints. REST
and GraphQL query surfaces expose equivalent query capability over the same native
contract; transport handlers reshape requests and responses but must not define separate
planner, storage, or dataset semantics. The edge translates at the boundary before
calling the planner and reshapes responses after native response assembly.

## What This Step Implements First

The first architecture implementation should create the workspace and crate boundaries
even if many functions are still stubs. That gives later issues a stable place to add
storage, planning, EVM fetching, and APIs.

The first concrete module interfaces should be small:

- Chain identity and range types in `datalens-core`.
- Dataset and coverage vocabulary in `datalens-core`.
- Adapter capability metadata in `datalens-chain`.
- Storage trait shape in `datalens-storage`.
- Planner input/output skeleton in `datalens-planner`.
- Durable writer coordinator input/output contracts in `datalens-writer`.
- A minimal CLI or service binary that proves the crates compile together.

## Why The Boundaries Matter

If EVM types enter `datalens-core`, adding Tron or Solana later becomes a rewrite. If API
compatibility shapes storage, every external protocol creates a new cache format. If local
disk becomes coverage authority, object storage can no longer safely recover or scale the
archive.

The architecture keeps these concerns separate so each future issue can be small:

- EVM adapter issues can change provider behavior without touching manifests.
- Storage issues can change object backends without touching EVM logic.
- API issues can add compatibility without changing core datasets.
- Planner issues can improve range splitting without changing HTTP routes.

## Expected Output Of This Step

By the end of the system architecture stage, the repository should have:

- A Rust workspace whose crate names match the architecture.
- Minimal public modules for the core boundaries.
- No final storage schema requirement yet.
- No gateway compatibility requirement yet.
- A compiling baseline that later implementation issues can extend.

The code does not need to fetch real chain data at this stage. The success condition is
that the system has the correct shape before behavior is added.
