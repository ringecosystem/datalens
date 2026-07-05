# DataLens Fragmentation Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DataLens storage compaction safely reduce fragmentation without degrading query, indexer, or RustFS health.

**Architecture:** Treat fragmentation cleanup as bounded background maintenance, not a query-path requirement. Split the fix into three independently reviewable PRs: first remove expensive no-cleanup reconciliation from the worker hot path, then fix compaction queue cursor progress, then add metadata fragmentation cleanup for manifest segments and queue entries.

**Tech Stack:** Rust workspace, `datalens-storage`, `datalens-cli`, local `LocalStorage` tests, Kubernetes/RustFS rollout after merge.

---

## Production Evidence To Preserve

- Current RustFS bucket: `ringdao-datalens`, prefix `datalens`.
- Current online config must stay paused until code is fixed: `storage.compaction.enabled = false`.
- Current bucket sample showed:
  - `datalens/chains`: about 55,963 object metadata files, about 586M.
  - `manifest-segments`: about 5,223 to 5,264 objects.
  - `metadata/compaction-queue`: about 5,224 objects.
  - `compacted`: 0 objects.
  - `compaction-cursor`: 0 objects.
  - `datalens inspect maintenance` took about 4m43s and returned `status=ok`, `issue_count=0`, `small_object_count=322`, `candidate_count=65`, `candidate_backlog=65`.
- The first queue scopes for Arbitrum/Base are not in the candidate backlog, which reproduces the prior `candidate_count=0` empty tick condition without enabling compaction.

## PR 1: Keep Worker Reconciliation Off The Hot Path

**Intent:** When `cleanup_enabled=false`, the worker must not run expensive reconciliation before every compaction tick. This protects query/index latency during low-rate observation.

**Files:**
- Modify: `crates/cli/src/runtime.rs`
- Test: `crates/cli/src/runtime.rs`

- [ ] **Step 1: Add a worker test that no-cleanup ticks skip reconciliation**

Add a focused unit test near existing `storage_compaction_*` tests in `crates/cli/src/runtime.rs`.

The test should cover a helper that decides whether a tick needs reconciliation:

```rust
#[test]
fn storage_compaction_reconciliation_is_skipped_when_cleanup_disabled() {
    let config = MaintenanceCompactionConfig {
        cleanup_enabled: false,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    assert!(!storage_compaction_tick_needs_reconciliation(config));
}
```

Also add the positive case:

```rust
#[test]
fn storage_compaction_reconciliation_runs_when_cleanup_enabled() {
    let config = MaintenanceCompactionConfig {
        cleanup_enabled: true,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    assert!(storage_compaction_tick_needs_reconciliation(config));
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test -p datalens-cli storage_compaction_reconciliation -- --nocapture
```

Expected: FAIL because `storage_compaction_tick_needs_reconciliation` does not exist.

- [ ] **Step 3: Add the helper and gate reconciliation**

In `crates/cli/src/runtime.rs`, add:

```rust
fn storage_compaction_tick_needs_reconciliation(config: MaintenanceCompactionConfig) -> bool {
    config.cleanup_enabled
}
```

Then change `StorageCompactionWorker::start` so the reconciliation block only runs when the helper returns true. When it is false, log a bounded skip line:

```rust
log::info!(
    "storage compaction reconciliation skipped chain_key={} reason=cleanup_disabled",
    chain.key_prefix()
);
```

The subsequent `compact_small_objects_for_chain_with_checkpoint` call must still run.

- [ ] **Step 4: Verify PR 1**

Run:

```bash
cargo test -p datalens-cli storage_compaction -- --nocapture
cargo test -p datalens-storage --test maintenance compaction_tick -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit PR 1**

```bash
git add crates/cli/src/runtime.rs
git commit -m "fix(cli): skip compaction reconciliation when cleanup is disabled"
```

## PR 2: Make Compaction Queue Cursor Always Make Progress

**Intent:** Non-candidate queue scopes must not trap the worker. A tick that scans a complete scope and finds zero candidates must advance the chain-level queue cursor to the next scope.

**Files:**
- Modify: `crates/storage/src/maintenance.rs`
- Test: `crates/storage/tests/maintenance.rs`

- [ ] **Step 1: Add a regression test for non-candidate queue scope progress**

Add a test near existing compaction queue tests in `crates/storage/tests/maintenance.rs`:

```rust
#[test]
fn test_compaction_queue_advances_after_non_candidate_scope() {
    let storage = LocalStorage::new(temp_storage_root("queue-non-candidate-progress"));
    let chain = test_chain();
    let first_selector = DatasetSelector::try_other(
        AdapterKey::try_new("test").expect("adapter key"),
        "selector-a",
        "selector-a",
    )
    .expect("selector");
    let second_selector = DatasetSelector::try_other(
        AdapterKey::try_new("test").expect("adapter key"),
        "selector-b",
        "selector-b",
    )
    .expect("selector");

    write_block_object_with_selector(&storage, &chain, &first_selector, 10, FinalityLevel::Safe);
    write_block_object_with_selector(&storage, &chain, &second_selector, 20, FinalityLevel::Safe);
    write_block_object_with_selector(&storage, &chain, &second_selector, 21, FinalityLevel::Safe);

    let config = MaintenanceCompactionConfig {
        min_object_bytes: u64::MAX,
        max_input_objects_per_candidate: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 1,
        max_manifest_entries_per_tick: 20_000,
        cleanup_enabled: false,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    let first = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("first queue tick");
    assert_eq!(first.candidate_count, 0);
    assert_eq!(first.processed_candidates, 0);

    let second = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("second queue tick");
    assert_eq!(second.candidate_count, 1);
    assert_eq!(second.processed_candidates, 1);
    assert_eq!(second.compacted_objects, 1);
}
```

- [ ] **Step 2: Run the failing storage test**

Run:

```bash
cargo test -p datalens-storage --test maintenance test_compaction_queue_advances_after_non_candidate_scope -- --nocapture
```

Expected: FAIL because the second tick still scans the first non-candidate scope.

- [ ] **Step 3: Fix queue cursor semantics**

In `scan_compaction_queue_entries`, track whether the current active scope was completely scanned separately from whether the page contained more scopes.

Required behavior:

- If the loop breaks because the next queue object belongs to a different scope, the current scope is complete.
- If the current scope is complete, set `queue_cursor_advance = cursor_advance.clone()` even when `queue_objects.len()` is larger.
- Preserve the per-scope cursor for partial active queue scopes.
- Separately advance the chain-level queue cursor only when the active queue scope is complete.
- Keep `partial=true` when the page still has more objects, but allow chain-level queue cursor progress after a completed scope.

- [ ] **Step 4: Add queue scan tick budget test**

Add a test that uses a very small `max_manifest_entries_per_tick` or `max_tick_duration_ms` to prove the queue scanner can stop without losing progress. The assertion should be that a later tick resumes after the last scanned queue object and eventually reaches a candidate scope.

- [ ] **Step 5: Verify PR 2**

Run:

```bash
cargo test -p datalens-storage --test maintenance compaction_queue -- --nocapture
cargo test -p datalens-storage --test maintenance compaction_cursor -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit PR 2**

```bash
git add crates/storage/src/maintenance.rs crates/storage/tests/maintenance.rs
git commit -m "fix(storage): advance compaction queue past non-candidate scopes"
```

## PR 3: Add Stale Queue Metadata Cleanup

**Intent:** Data object compaction already consolidates matching manifest segments when replacement publish succeeds. PR 3 should safely reduce stale `metadata/compaction-queue` objects without deleting query-visible data, live manifest segments, or live non-candidate queue entries.

**Files:**
- Modify: `crates/storage/src/maintenance.rs`
- Test: `crates/storage/tests/maintenance.rs`

- [ ] **Step 1: Add stale queue cleanup regression tests**

Add tests near the existing compaction queue tests:

- `test_compaction_cleanup_deletes_stale_queue_entries_for_missing_manifest_segments`
- `test_compaction_cleanup_preserves_live_non_candidate_queue_entries`
- `test_compaction_queue_stale_cleanup_obeys_delete_budget`

The missing-segment test should:

```rust
#[test]
fn test_compaction_cleanup_deletes_stale_queue_entries_for_missing_manifest_segments() {
    let storage = LocalStorage::new(temp_storage_root("stale-queue-missing-segment"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 1, FinalityLevel::Safe);
    let queue_prefix = format!("chains/{}/metadata/compaction-queue", chain.key_prefix());
    let queue_keys_before = list_prefix(&storage, &queue_prefix);
    assert_eq!(queue_keys_before.len(), 1);
    for segment_key in manifest_segment_keys(&storage, &chain) {
        storage.object_store().delete(&segment_key).expect("delete segment");
    }

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 1,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: true,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("stale queue cleanup");

    assert_eq!(report.processed_candidates, 0);
    assert!(list_prefix(&storage, &queue_prefix).is_empty());
}
```

- [ ] **Step 2: Verify manifest segment consolidation is already covered**

Do not add a broad manifest segment cleanup path. Existing replacement publish already writes the replacement entry, deletes old segment keys, rewrites the base manifest, and bumps the manifest version. Preserve existing tests that prove:

- source segments become one compacted manifest segment after successful compaction.
- failed replacement publish keeps old manifest/segments current.
- reads use the compacted replacement after publish.

- [ ] **Step 3: Implement stale queue cleanup behind `cleanup_enabled`**

Implement metadata cleanup in the compaction maintenance flow:

- Only run when `cleanup_enabled=true`.
- Continue deleting consumed queue entries for compacted candidates.
- Delete stale queue entries whose `segment_key` is missing.
- Preserve live non-candidate queue entries. A singleton entry is not stale just because it cannot currently form a candidate.
- Preserve retry progress when queue cleanup is limited by delete budget or delete failure; do not advance queue or scope cursors past entries that still need cleanup.
- Respect `max_deletes_per_tick` and `max_tick_duration_ms`.
- Do not delete source data objects outside the existing superseded-source grace flow.
- Do not delete current manifest objects or manifest segments by broad prefix scan.
- Do not change coverage-index v2 cleanup semantics.

- [ ] **Step 4: Verify PR 3**

Run:

```bash
cargo test -p datalens-storage --test maintenance compaction_cleanup -- --nocapture
cargo test -p datalens-storage --test maintenance compaction_queue -- --nocapture
cargo test -p datalens-storage --test maintenance coverage_index_v2 -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit PR 3**

```bash
git add crates/storage/src/maintenance.rs crates/storage/tests/maintenance.rs
git commit -m "fix(storage): clean compaction metadata fragments"
```

## PR 4: Local RustFS Validation And Rollout Guardrails

**Intent:** Prove the fix works against object-store behavior close to RustFS before deploying.

**Files:**
- Modify only if needed: `docs/runbook/compaction-production-backlog-drain.md`
- No code changes unless tests reveal a product bug.

- [ ] **Step 1: Run local RustFS or S3-compatible validation**

Use the existing compose setup if available. Generate synthetic fragmentation with:

- Many manifest segments.
- Many queue entries.
- Several non-candidate scopes before candidate scopes.
- At least one chain with manifest metadata fragments but no small data object candidates.

- [ ] **Step 2: Capture before/after metrics**

Record:

- Total object count.
- `manifest-segments` count.
- `metadata/compaction-queue` count.
- `compacted` count.
- `datalens inspect maintenance` duration.
- Query/read correctness before and after.

- [ ] **Step 3: Commit any runbook guardrail updates**

Only update the runbook if the rollout commands or safety gates changed.

```bash
git add docs/runbook/compaction-production-backlog-drain.md
git commit -m "docs(runbook): update compaction rollout guardrails"
```

## PR 5: Deployment And Observation

**Intent:** Deploy only after PRs merge and image is available. Online success requires background cleanup progress and no query/index regression.

**Files:**
- GitOps repo, not this DataLens repo.

- [ ] **Step 1: Build and publish image through CI**

Trigger CI from the merged DataLens commit and record the image tag.

- [ ] **Step 2: Upgrade staging and production DataLens**

Update GitOps image tag. Keep compaction disabled immediately after deploy.

- [ ] **Step 3: Verify services before enabling compaction**

Check:

- DeGov indexer query success.
- ORMP indexer query success.
- DataLens query logs have no new timeout spike.
- RustFS CPU/disk/inode are stable.

- [ ] **Step 4: Enable low-rate compaction**

Use:

```toml
[storage.compaction]
enabled = true
interval_ms = 300000
max_candidates_per_tick = 1
max_manifest_entries_per_tick = 1000
max_tick_duration_ms = 5000
cleanup_enabled = false
delete_source_objects = false
```

Observe at least two worker cycles per active chain. Success means:

- Non-candidate queue scopes do not repeat forever.
- `compacted` object count increases when candidates exist.
- Query/index paths remain healthy.

- [ ] **Step 5: Enable metadata cleanup only after low-rate compaction is stable**

Use `cleanup_enabled=true` only after low-rate compaction is healthy. Keep `delete_source_objects=false` until source-object deletion is separately approved.

Success means:

- `manifest-segments` count trends down.
- `metadata/compaction-queue` count trends down.
- `compacted` object count is stable or increasing as expected.
- DeGov/ORMP/indexer behavior remains normal.

- [ ] **Step 6: Finish Plane issue**

Move HBX-1192 to Done only after production has demonstrated stable cleanup progress and no query/index regression.
