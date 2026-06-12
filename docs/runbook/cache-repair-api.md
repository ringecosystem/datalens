# Cache Repair API Development Plan

Goal: Add an authorized repair path that rebuilds known-bad durable cache coverage for a
specific chain, dataset, selector, finality, and block range.

Read this when: A durable `FullHit` is known to be logically wrong and the normal query
path must keep trusting durable coverage.

Inputs: Bad coverage evidence, target chain, dataset key, selector, finality, range, and
an application token with cache repair operations.

Depends on: Durable storage manifest coverage, `write_rows_replacing_existing`, warmup
registry/runtime patterns, HTTP handlers, and GraphQL schema.

Verification: Existing `FullHit` query behavior remains unchanged; repair task fetches
provider data, atomically replaces scoped coverage, and subsequent broad-selector
queries return repaired rows without read-time verification.

## Constraints

- Do not change normal query planning or execution to re-verify `FullHit`.
- Do not make provider/bloom verification run on every durable hit.
- Do not expose repair through CLI as the primary control plane.
- Add both HTTP and GraphQL surfaces.
- Use application auth and explicit operations, separate from warmup permissions.
- If provider fetch fails, keep existing durable coverage visible.

## Current Code Anchors

- Query `FullHit` is produced in `crates/runtime/planner/src/planning.rs`.
- `FullHit` execution reads durable objects only in `crates/runtime/executor/src/execution.rs`.
- Warmup HTTP patterns live in `crates/edge/src/http/handlers.rs` and
  `crates/edge/src/http/router.rs`.
- Warmup GraphQL patterns live in `crates/edge/src/graphql/input.rs` and
  `crates/edge/src/graphql/schema.rs`.
- Application operation auth lives in `crates/edge/src/config.rs` and
  `crates/edge/src/auth/application.rs`.
- Chain service registry task routing lives in `crates/edge/src/service/registry.rs`.
- Repair execution should reuse adapter fetch and durable write semantics from
  `crates/runtime/warmup/src/runtime.rs`.
- Storage already has `StorageRepository::write_rows_replacing_existing`, but it needs
  scoped manifest/coverage replacement for overlapping entries, not only same segment
  replacement.

## API Shape

HTTP:

- `POST /v1/cache/repairs`
- `GET /v1/cache/repairs`
- `GET /v1/cache/repairs/{task_id}`
- `POST /v1/cache/repairs/{task_id}/cancel`
- `POST /v1/cache/repairs/{task_id}/retry`
- `POST /v1/cache/repairs/run-once`
- `POST /v1/cache/repairs/{task_id}/run-once`

GraphQL:

- Query `cacheRepairTask(id: ID!): CacheRepairTask`
- Query `cacheRepairTasks(filter: CacheRepairTaskFilterInput): [CacheRepairTask!]!`
- Mutation `submitCacheRepairTask(input: CacheRepairSubmitInput!): CacheRepairSubmitPayload!`
- Mutation `cancelCacheRepairTask(id: ID!): CacheRepairTask!`
- Mutation `retryCacheRepairTask(id: ID!): CacheRepairTask!`
- Mutation `runCacheRepairOnce: CacheRepairRunOncePayload!`
- Mutation `runCacheRepairTaskOnce(id: ID!): CacheRepairRunOncePayload!`

Submit request:

```json
{
  "chain": {
    "family": "evm",
    "configured_name": "ethereum",
    "network_id": { "numeric": 1 }
  },
  "dataset_key": "evm.logs",
  "selector": {
    "kind": "evm_logs",
    "value": {
      "addresses": ["0x..."],
      "topics": [["0x...", "0x..."]]
    }
  },
  "source_selectors": [
    {
      "kind": "evm_logs",
      "value": {
        "addresses": ["0x..."],
        "topics": [["0x..."]]
      }
    }
  ],
  "range_kind": "block",
  "start": 24849000,
  "end": 24854000,
  "finality": "finalized",
  "chunk_policy": { "max_chunk_range": 1000 },
  "reason": "ENS broad selector durable cache missing VoteCast logs"
}
```

`source_selectors` is optional. When omitted or empty, repair fetches the target
selector directly. When present, repair fetches exact source selectors, merges
and deduplicates rows, and writes replacement coverage only for the target
selector. In v1, source selectors are supported for EVM logs only and must be
covered by the target EVM log selector.

## Implementation Steps

1. Add auth operations.
   - Extend `ApplicationOperationConfig` with:
     - `CacheRepairSubmit`
     - `CacheRepairRead`
     - `CacheRepairMutate`
     - `CacheRepairRun`
   - Add registry helper methods mirroring warmup auth, or reuse existing generic
     helpers with these operations.

2. Add storage scoped replacement.
   - Add a public storage API that replaces coverage for one logical scope:
     `chain`, `dataset_key`, `selector_fingerprint`, `range_kind`, intersecting range,
     and finality.
   - The replacement must remove or split overlapping manifest entries for the same
     logical scope before publishing the new entry.
   - Preserve entries outside the repaired range and entries for other selectors,
     datasets, chains, or finalities.
   - Update coverage index and manifest cache in the same locked manifest update.
   - Write tests for replacing an empty wide entry with a data subrange and for
     replacing a data subrange with empty coverage.

3. Add a repair runtime and registry.
   - Prefer a small crate/module modeled after `datalens_warmup` if existing workspace
     boundaries make that natural.
   - Task fields:
     - `task_id`, `application_id`, `chain`, `dataset_key`, `selector`
     - `range_kind`, `start`, `end`, `finality`, `chunk_policy`, `retry_policy`
     - `reason`, `state`, `created_at`, `updated_at`, `last_error`, `stats`
   - Runtime behavior:
     - Validate dataset and selector against adapter capabilities.
     - Validate range is safe/finalized writable for requested finality.
     - Fetch chunks from provider even if durable cache is already covered.
     - Use existing adapter EVM log reliability/bloom/secondary provider logic.
     - Validate provider response against request.
     - Write via scoped replacement only after fetch succeeds.
     - Mark failed without invalidating old coverage on provider/write failure.

4. Wire repair service into edge registry.
   - Add optional repair service accessors beside `RegisteredWarmupService`.
   - Add submit/list/get/cancel/retry/run-once methods to `QueryServiceRegistry`.
   - Start scheduler with the existing lifecycle pattern if configured enabled, or keep
     `run-once` as the first implementation if it fits current config better.

5. Add HTTP contracts and handlers.
   - Add `crates/edge/src/contract/cache_repair.rs`.
   - Mirror warmup submit/list/get/run response shapes.
   - Auth mapping:
     - submit: `CacheRepairSubmit` with chain and dataset auth
     - list/get: `CacheRepairRead`, plus task application ownership check
     - cancel/retry: `CacheRepairMutate`, plus ownership check
     - run-once: `CacheRepairRun`
   - Add routes in `crates/edge/src/http/router.rs`.

6. Add GraphQL parity.
   - Add GraphQL input types in `crates/edge/src/graphql/input.rs`.
   - Add query and mutation fields in `crates/edge/src/graphql/schema.rs`.
   - Use the same auth operations and task ownership checks as HTTP.

7. Tests.
   - Storage: scoped replacement removes/splits only overlapping entries for the same
     logical scope and updates coverage lookup.
   - Runtime: bad empty durable coverage is repaired by provider fetch; later query
     returns repaired rows while still reporting durable hit.
   - Runtime: provider failure leaves old coverage intact.
   - Edge HTTP: submit forbidden without `cache_repair_submit`; read/mutate/run require
     their own operations.
   - GraphQL: submit/read/run paths are authorized and map inputs correctly.

8. Deployment and data repair.
   - Add the new operations to the production Datalens application config for the
     authorized DeGov/Datalens maintenance token only.
   - Deploy Datalens through the existing GitOps flow.
   - Submit repair tasks for ENS governor and token broad selectors over the known bad
     Ethereum ranges.
   - Run repair tasks to completion.
   - Re-query the known evidence blocks:
     - Governor broad selector at `24850076`, `24850304`, `24852885`, `24853888`
     - Token broad selector at `24850565`, `24851872`
   - Clear and rerun the DeGov ENS index only after Datalens broad-selector queries are
     repaired.
