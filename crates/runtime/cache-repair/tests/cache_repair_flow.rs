use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use datalens_cache_repair::{
    CacheRepairChunkPolicy, CacheRepairFinality, CacheRepairRegistry, CacheRepairRunStatus,
    CacheRepairRuntime, CacheRepairRuntimeConfig, CacheRepairSubmitRequest, CacheRepairTaskPool,
    CacheRepairTaskState, LocalCacheRepairRegistry,
};
use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityLevel, ProviderDiagnostics, SelectorKind,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows,
    LedgerRange, LedgerRangeKind, LogFilter, LogRecord, NetworkId, QueryRows,
};
use datalens_storage::{
    LocalObjectStore, LocalStorage, Manifest, ObjectStore, StorageRepository, StorageWriteOutcome,
    StorageWriteRequest,
};

#[test]
fn test_cache_repair_replaces_bad_empty_coverage_with_provider_rows() {
    let root = temp_root("repair-replaces-empty");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let bad_range = LedgerRange::blocks(10, 12).expect("valid range");
    let repair_range = LedgerRange::blocks(11, 11).expect("valid range");
    let empty_rows =
        datalens_core::DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("empty rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: bad_range.clone(),
            rows: &empty_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write bad empty coverage");

    let adapter = FixtureAdapter::new(chain.clone(), Ok(vec![log_record(11, 3)]));
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let submit = pool
        .submit(submit_request(chain.clone(), selector.clone()))
        .expect("submit repair");
    let results = pool.run_available_once().expect("run repair");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, CacheRepairRunStatus::Completed);
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::Completed);

    let rows = storage
        .read_rows(&chain, &DatasetKey::evm_logs(), &selector, repair_range)
        .expect("read repaired rows");
    match rows.into_rows() {
        QueryRows::EvmLogs(logs) => {
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].log_index, 3);
        }
        rows => panic!("expected evm logs, got {rows:?}"),
    }

    assert_eq!(
        storage
            .covered_ranges(&chain, &DatasetKey::evm_logs(), &selector, bad_range)
            .expect("covered ranges"),
        vec![LedgerRange::blocks(10, 12).expect("valid range")]
    );
}

#[test]
fn test_cache_repair_registry_writes_clean_task_path() {
    let store = LocalObjectStore::new(temp_root("registry-writes-clean-path"));
    let registry = LocalCacheRepairRegistry::new(store.clone());

    let task_id = registry
        .submit(submit_request(test_chain(), selector()))
        .expect("submit")
        .task_id;

    assert!(
        store
            .exists(&cache_repair_clean_task_key(&task_id))
            .expect("clean task exists")
    );
    assert!(
        !store
            .exists(&cache_repair_legacy_task_key(&task_id))
            .expect("legacy task missing")
    );
}

#[test]
fn test_cache_repair_registry_reads_and_lists_legacy_task() {
    let store = LocalObjectStore::new(temp_root("registry-reads-legacy-path"));
    let registry = LocalCacheRepairRegistry::new(store.clone());
    let task_id = move_cache_repair_task_to_legacy(&store, &registry);

    let task = registry
        .get(&task_id)
        .expect("get legacy task")
        .expect("legacy task");
    let listed = registry.list(Default::default()).expect("list tasks");

    assert_eq!(task.task_id, task_id);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].task_id, task_id);
}

#[test]
fn test_cache_repair_registry_prefers_clean_task_when_legacy_duplicate_exists() {
    let store = LocalObjectStore::new(temp_root("registry-prefers-clean-path"));
    let registry = LocalCacheRepairRegistry::new(store.clone());
    let task_id = registry
        .submit(submit_request(test_chain(), selector()))
        .expect("submit")
        .task_id;
    copy_cache_repair_clean_to_legacy(&store, &task_id);
    mutate_json_object(&store, &cache_repair_clean_task_key(&task_id), |value| {
        value["state"] = serde_json::json!("cancelled");
    });

    let task = registry
        .get(&task_id)
        .expect("get task")
        .expect("task exists");
    let listed = registry.list(Default::default()).expect("list tasks");

    assert_eq!(task.state, CacheRepairTaskState::Cancelled);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].state, CacheRepairTaskState::Cancelled);
}

#[test]
fn test_cache_repair_registry_mutating_legacy_task_writes_clean_path_without_deleting_legacy() {
    let store = LocalObjectStore::new(temp_root("registry-mutates-legacy-to-clean"));
    let registry = LocalCacheRepairRegistry::new(store.clone());
    let task_id = move_cache_repair_task_to_legacy(&store, &registry);

    registry.cancel(&task_id).expect("cancel legacy task");

    assert!(
        store
            .exists(&cache_repair_clean_task_key(&task_id))
            .expect("clean task exists")
    );
    assert!(
        store
            .exists(&cache_repair_legacy_task_key(&task_id))
            .expect("legacy task preserved")
    );
    assert_eq!(
        registry
            .get(&task_id)
            .expect("get task")
            .expect("task exists")
            .state,
        CacheRepairTaskState::Cancelled
    );
}

#[test]
fn test_cache_repair_registry_migration_copies_legacy_paths_idempotently() {
    let store = LocalObjectStore::new(temp_root("registry-migrates-legacy-path"));
    let registry = LocalCacheRepairRegistry::new(store.clone());
    let task_id = move_cache_repair_task_to_legacy(&store, &registry);

    let first = registry
        .migrate_legacy_paths()
        .expect("first migration succeeds");
    let second = registry
        .migrate_legacy_paths()
        .expect("second migration succeeds");

    assert_eq!(first.tasks.copied, 1);
    assert_eq!(first.tasks.skipped, 0);
    assert_eq!(first.tasks.conflicts, 0);
    assert_eq!(first.tasks.failed, 0);
    assert_eq!(second.tasks.copied, 0);
    assert_eq!(second.tasks.skipped, 1);
    assert_eq!(second.tasks.conflicts, 0);
    assert_eq!(second.tasks.failed, 0);
    assert!(
        store
            .exists(&cache_repair_clean_task_key(&task_id))
            .expect("clean task exists")
    );
    assert!(
        store
            .exists(&cache_repair_legacy_task_key(&task_id))
            .expect("legacy task preserved")
    );
}

#[test]
fn test_cache_repair_registry_migration_reports_conflict_without_overwriting_clean_object() {
    let store = LocalObjectStore::new(temp_root("registry-migration-conflict"));
    let registry = LocalCacheRepairRegistry::new(store.clone());
    let task_id = move_cache_repair_task_to_legacy(&store, &registry);
    let clean_key = cache_repair_clean_task_key(&task_id);
    let clean_bytes = br#"{"existing":"clean"}"#;
    store
        .put(&clean_key, clean_bytes)
        .expect("write clean object");

    let report = registry
        .migrate_legacy_paths()
        .expect("migration reports conflict");

    assert_eq!(report.tasks.copied, 0);
    assert_eq!(report.tasks.skipped, 0);
    assert_eq!(report.tasks.conflicts, 1);
    assert_eq!(report.tasks.failed, 0);
    assert_eq!(
        store.get(&clean_key).expect("read clean object"),
        clean_bytes
    );
    assert!(
        store
            .exists(&cache_repair_legacy_task_key(&task_id))
            .expect("legacy task preserved")
    );
}

#[test]
fn test_cache_repair_provider_failure_preserves_existing_coverage() {
    let root = temp_root("repair-failure-preserves-empty");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let range = LedgerRange::blocks(11, 11).expect("valid range");
    let empty_rows =
        datalens_core::DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("empty rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: range.clone(),
            rows: &empty_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    let adapter = FixtureAdapter::new(
        chain.clone(),
        Err(DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            "provider unavailable",
        )),
    );
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let submit = pool
        .submit(submit_request(chain.clone(), selector.clone()))
        .expect("submit repair");
    let error = pool.run_available_once().expect_err("repair fails");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::Failed);
    assert_eq!(
        storage
            .covered_ranges(&chain, &DatasetKey::evm_logs(), &selector, range)
            .expect("covered ranges"),
        vec![LedgerRange::blocks(11, 11).expect("valid range")]
    );
}

#[test]
fn test_cache_repair_fetch_timeout_marks_task_failed_without_writing() {
    let root = temp_root("repair-fetch-timeout");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let adapter = FixtureAdapter::new(chain.clone(), Ok(vec![log_record(11, 3)]))
        .with_delay(Duration::from_millis(200));
    let pool = CacheRepairTaskPool::new(
        CacheRepairRuntime::new(adapter, storage.clone(), registry).with_runtime_config(
            CacheRepairRuntimeConfig {
                fetch_timeout_ms: 25,
                ..CacheRepairRuntimeConfig::default()
            },
        ),
    );

    let submit = pool
        .submit(submit_request(chain.clone(), selector.clone()))
        .expect("submit repair");
    let error = pool.run_available_once().expect_err("repair times out");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("timed out"));
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::Failed);
    assert!(
        task.last_error
            .as_deref()
            .expect("last error")
            .contains("timed out")
    );
    assert!(
        storage
            .covered_ranges(&chain, &DatasetKey::evm_logs(), &selector, repair_range())
            .expect("covered ranges")
            .is_empty()
    );
}

#[test]
fn test_cache_repair_height_timeout_marks_task_failed() {
    let root = temp_root("repair-height-timeout");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let adapter = FixtureAdapter::new(chain.clone(), Ok(vec![log_record(11, 3)]))
        .with_height_delay(Duration::from_millis(200));
    let pool = CacheRepairTaskPool::new(
        CacheRepairRuntime::new(adapter, storage.clone(), registry).with_runtime_config(
            CacheRepairRuntimeConfig {
                fetch_timeout_ms: 25,
                ..CacheRepairRuntimeConfig::default()
            },
        ),
    );

    let submit = pool
        .submit(submit_request(chain, selector))
        .expect("submit repair");
    let error = pool.run_available_once().expect_err("height times out");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("height"));
    assert!(error.message.contains("timed out"));
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::Failed);
    assert_eq!(task.lease_owner, None);
    assert_eq!(task.lease_expires_at, None);
    assert_eq!(task.current_phase.as_deref(), Some("height"));
    assert!(
        task.last_error
            .as_deref()
            .expect("last error")
            .contains("height")
    );
}

#[test]
fn test_cache_repair_write_timeout_marks_task_uncertain_without_completed_write() {
    let root = temp_root("repair-write-timeout");
    let storage = BlockingReplacementStorage::new(LocalStorage::new(root.join("storage")));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let adapter = FixtureAdapter::new(chain.clone(), Ok(vec![log_record(11, 3)]));
    let pool = CacheRepairTaskPool::new(
        CacheRepairRuntime::new(adapter, storage.clone(), registry).with_runtime_config(
            CacheRepairRuntimeConfig {
                fetch_timeout_ms: 25,
                ..CacheRepairRuntimeConfig::default()
            },
        ),
    );

    let submit = pool
        .submit(submit_request(chain.clone(), selector.clone()))
        .expect("submit repair");
    let error = pool.run_available_once().expect_err("write times out");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert!(error.message.contains("write"));
    assert!(error.message.contains("timed out"));
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::WriteTimedOut);
    assert!(task.lease_owner.is_some());
    assert!(task.lease_expires_at.is_some());
    assert_eq!(task.current_phase.as_deref(), Some("write"));
    assert_eq!(task.current_range_start, Some(11));
    assert_eq!(task.current_range_end, Some(11));
    assert!(
        task.last_error
            .as_deref()
            .expect("last error")
            .contains("write range=11-11")
    );
    assert!(!storage.completed_write());
    assert!(
        storage
            .covered_ranges(&chain, &DatasetKey::evm_logs(), &selector, repair_range())
            .expect("covered ranges")
            .is_empty()
    );
    storage.release_write();
}

#[test]
fn test_cache_repair_write_timeout_is_not_retryable_while_original_write_is_uncertain() {
    let root = temp_root("repair-write-timeout-no-retry");
    let storage = BlockingReplacementStorage::new(LocalStorage::new(root.join("storage")));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let adapter = FixtureAdapter::new(chain.clone(), Ok(vec![log_record(11, 3)]));
    let pool = CacheRepairTaskPool::new(
        CacheRepairRuntime::new(adapter, storage.clone(), registry).with_runtime_config(
            CacheRepairRuntimeConfig {
                fetch_timeout_ms: 25,
                ..CacheRepairRuntimeConfig::default()
            },
        ),
    );

    let submit = pool
        .submit(submit_request(chain.clone(), selector.clone()))
        .expect("submit repair");
    let error = pool.run_available_once().expect_err("write times out");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert_eq!(storage.write_attempts(), 1);
    let retry_error = pool
        .retry_failed(&submit.task_id)
        .expect_err("write timeout is not retryable");
    assert_eq!(retry_error.kind, DatalensErrorKind::InvalidInput);
    let results = pool
        .run_available_once()
        .expect("write timeout task is not runnable");
    assert!(results.is_empty());
    let result = pool
        .run_task_once(&submit.task_id)
        .expect("specific write timeout run is stopped");
    assert_eq!(result.status, CacheRepairRunStatus::Stopped);
    assert_eq!(storage.write_attempts(), 1);

    storage.release_write();
    storage.wait_for_completed_write(Duration::from_secs(1));
    assert_eq!(storage.write_attempts(), 1);
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::WriteTimedOut);
    assert_eq!(task.current_phase.as_deref(), Some("write"));
    assert_eq!(
        storage
            .covered_ranges(&chain, &DatasetKey::evm_logs(), &selector, repair_range())
            .expect("covered ranges"),
        vec![repair_range()]
    );
}

#[test]
fn test_cache_repair_running_task_with_future_lease_is_not_picked() {
    let root = temp_root("repair-running-future-lease");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let adapter = FixtureAdapter::new(chain.clone(), Ok(vec![log_record(11, 3)]));
    let pool = CacheRepairTaskPool::new(CacheRepairRuntime::new(
        adapter,
        storage.clone(),
        registry.clone(),
    ));

    let submit = pool
        .submit(submit_request(chain, selector))
        .expect("submit repair");
    let mut task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    task.state = CacheRepairTaskState::Running;
    task.lease_owner = Some("previous-worker".to_owned());
    task.lease_expires_at = Some(unix_milliseconds_now() + 60_000);
    registry.save_task(&task).expect("save running task");

    let results = pool.run_available_once().expect("run available once");

    assert!(results.is_empty());
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::Running);
    assert_eq!(task.lease_owner.as_deref(), Some("previous-worker"));
}

#[test]
fn test_cache_repair_running_task_with_expired_lease_is_recovered() {
    let root = temp_root("repair-running-expired-lease");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let selector = selector();
    let adapter = FixtureAdapter::new(chain.clone(), Ok(vec![log_record(11, 3)]));
    let pool = CacheRepairTaskPool::new(CacheRepairRuntime::new(
        adapter,
        storage.clone(),
        registry.clone(),
    ));

    let submit = pool
        .submit(submit_request(chain, selector))
        .expect("submit repair");
    let mut task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    task.state = CacheRepairTaskState::Running;
    task.lease_owner = Some("previous-worker".to_owned());
    task.lease_expires_at = Some(unix_milliseconds_now().saturating_sub(1));
    registry.save_task(&task).expect("save running task");

    let results = pool.run_available_once().expect("run available once");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, CacheRepairRunStatus::Completed);
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::Completed);
    assert_eq!(task.lease_owner, None);
    assert_eq!(task.lease_expires_at, None);
}

#[test]
fn test_cache_repair_run_task_once_runs_requested_task() {
    let root = temp_root("repair-run-specific-task");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let first_selector = selector();
    let second_selector = exact_selector(other_topic());
    let adapter = FixtureAdapter::new(
        chain.clone(),
        Ok(vec![log_record_with_topic(11, 3, topic())]),
    )
    .with_selector_result(
        second_selector.clone(),
        Ok(vec![log_record_with_topic(11, 4, other_topic())]),
    );
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let first = pool
        .submit(submit_request(chain.clone(), first_selector.clone()))
        .expect("submit first repair");
    let second = pool
        .submit(submit_request(chain.clone(), second_selector.clone()))
        .expect("submit second repair");

    let result = pool
        .run_task_once(&second.task_id)
        .expect("run requested repair");

    assert_eq!(result.status, CacheRepairRunStatus::Completed);
    assert_eq!(
        pool.get(&first.task_id)
            .expect("get first")
            .expect("first exists")
            .state,
        CacheRepairTaskState::Queued
    );
    assert_eq!(
        pool.get(&second.task_id)
            .expect("get second")
            .expect("second exists")
            .state,
        CacheRepairTaskState::Completed
    );
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &second_selector,
            repair_range(),
        )
        .expect("read requested repair rows");
    assert_eq!(rows.row_count(), 1);
}

#[test]
fn test_cache_repair_source_selectors_repair_broad_target_without_fetching_target() {
    let root = temp_root("repair-source-selectors");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let target_selector = broad_selector();
    let source_selector = exact_selector(topic());
    write_empty_coverage(&storage, &chain, &target_selector);
    let adapter = FixtureAdapter::target_fetch_fails(chain.clone(), target_selector.clone())
        .with_selector_result(
            source_selector.clone(),
            Ok(vec![log_record_with_topic(11, 3, topic())]),
        );
    let calls = adapter.calls.clone();
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let mut request = submit_request(chain.clone(), target_selector.clone());
    request.source_selectors = vec![source_selector.clone()];
    let submit = pool.submit(request).expect("submit repair");

    let result = pool
        .run_task_once(&submit.task_id)
        .expect("run source selector repair");

    assert_eq!(result.status, CacheRepairRunStatus::Completed);
    assert_eq!(calls_for(&calls, &target_selector), 0);
    assert_eq!(calls_for(&calls, &source_selector), 1);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &target_selector,
            repair_range(),
        )
        .expect("read repaired target rows");
    match rows.into_rows() {
        QueryRows::EvmLogs(logs) => {
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].topics[0], topic());
        }
        rows => panic!("expected evm logs, got {rows:?}"),
    }
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_logs(),
                &target_selector,
                repair_range()
            )
            .expect("covered ranges"),
        vec![repair_range()]
    );
}

#[test]
fn test_cache_repair_broad_source_selector_is_filtered_before_target_write() {
    let root = temp_root("repair-broad-source-filtered");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let target_selector = exact_selector(topic());
    let source_selector = address_only_selector();
    let adapter = FixtureAdapter::target_fetch_fails(chain.clone(), target_selector.clone())
        .with_selector_result(
            source_selector.clone(),
            Ok(vec![
                log_record_with_topic(11, 3, topic()),
                log_record_with_topic(11, 4, other_topic()),
            ]),
        );
    let calls = adapter.calls.clone();
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let mut request = submit_request(chain.clone(), target_selector.clone());
    request.source_selectors = vec![source_selector.clone()];
    let submit = pool.submit(request).expect("submit repair");

    let result = pool
        .run_task_once(&submit.task_id)
        .expect("run source selector repair");

    assert_eq!(result.status, CacheRepairRunStatus::Completed);
    assert_eq!(calls_for(&calls, &target_selector), 0);
    assert_eq!(calls_for(&calls, &source_selector), 1);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &target_selector,
            repair_range(),
        )
        .expect("read repaired target rows");
    match rows.into_rows() {
        QueryRows::EvmLogs(logs) => {
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].topics[0], topic());
        }
        rows => panic!("expected evm logs, got {rows:?}"),
    }
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_logs(),
                &target_selector,
                repair_range()
            )
            .expect("covered ranges"),
        vec![repair_range()]
    );
}

#[test]
fn test_cache_repair_safe_task_replaces_finalized_coverage_when_range_is_finalized() {
    let root = temp_root("repair-safe-task-replaces-finalized");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let target_selector = broad_selector();
    let source_selector = exact_selector(topic());
    write_empty_coverage_with_finality(
        &storage,
        &chain,
        &target_selector,
        FinalityLevel::Finalized,
    );
    let adapter = FixtureAdapter::target_fetch_fails(chain.clone(), target_selector.clone())
        .with_finalized_height(ChainHeight::block(20).with_finality(FinalityLevel::Finalized))
        .with_selector_result(
            source_selector.clone(),
            Ok(vec![log_record_with_topic(11, 3, topic())]),
        );
    let calls = adapter.calls.clone();
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let mut request = submit_request(chain.clone(), target_selector.clone());
    request.source_selectors = vec![source_selector.clone()];
    let submit = pool.submit(request).expect("submit repair");

    let result = pool
        .run_task_once(&submit.task_id)
        .expect("run source selector repair");

    assert_eq!(result.status, CacheRepairRunStatus::Completed);
    assert_eq!(calls_for(&calls, &target_selector), 0);
    assert_eq!(calls_for(&calls, &source_selector), 1);
    let finalized_rows = storage
        .read_rows_for_finality(
            &chain,
            &DatasetKey::evm_logs(),
            &target_selector,
            repair_range(),
            FinalityLevel::Finalized,
        )
        .expect("read finalized repaired target rows");
    match finalized_rows.into_rows() {
        QueryRows::EvmLogs(logs) => {
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].topics[0], topic());
        }
        rows => panic!("expected evm logs, got {rows:?}"),
    }
}

#[test]
fn test_cache_repair_source_selector_failure_preserves_target_coverage() {
    let root = temp_root("repair-source-selector-failure");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let target_selector = broad_selector();
    let source_selector = exact_selector(topic());
    write_empty_coverage(&storage, &chain, &target_selector);
    let adapter = FixtureAdapter::new(
        chain.clone(),
        Err(DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            "source provider unavailable",
        )),
    );
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let mut request = submit_request(chain.clone(), target_selector.clone());
    request.source_selectors = vec![source_selector];
    let submit = pool.submit(request).expect("submit repair");

    let error = pool
        .run_task_once(&submit.task_id)
        .expect_err("repair fails");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    let task = pool
        .get(&submit.task_id)
        .expect("get task")
        .expect("task exists");
    assert_eq!(task.state, CacheRepairTaskState::Failed);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &target_selector,
            repair_range(),
        )
        .expect("read existing target rows");
    assert_eq!(rows.row_count(), 0);
}

#[test]
fn test_cache_repair_source_selectors_dedupe_duplicate_logs() {
    let root = temp_root("repair-source-selector-dedupe");
    let storage = LocalStorage::new(root.join("storage"));
    let registry = LocalCacheRepairRegistry::new(LocalObjectStore::new(root.join("registry")));
    let chain = test_chain();
    let target_selector = broad_selector();
    let source_selector = exact_selector(topic());
    let duplicate_source_selector = exact_selector(other_topic());
    let duplicate_log = log_record_with_topic(11, 3, topic());
    let adapter = FixtureAdapter::target_fetch_fails(chain.clone(), target_selector.clone())
        .with_selector_result(source_selector.clone(), Ok(vec![duplicate_log.clone()]))
        .with_selector_result(duplicate_source_selector.clone(), Ok(vec![duplicate_log]));
    let pool =
        CacheRepairTaskPool::new(CacheRepairRuntime::new(adapter, storage.clone(), registry));
    let mut request = submit_request(chain.clone(), target_selector.clone());
    request.source_selectors = vec![source_selector, duplicate_source_selector];
    let submit = pool.submit(request).expect("submit repair");

    let result = pool
        .run_task_once(&submit.task_id)
        .expect("run source selector repair");

    assert_eq!(result.status, CacheRepairRunStatus::Completed);
    assert_eq!(result.rows_fetched, 1);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &target_selector,
            repair_range(),
        )
        .expect("read repaired target rows");
    assert_eq!(rows.row_count(), 1);
}

#[derive(Clone)]
struct FixtureAdapter {
    chain: ChainIdentity,
    result: Arc<Mutex<Result<Vec<LogRecord>, DatalensError>>>,
    selector_results: SharedSelectorResults,
    calls: Arc<Mutex<BTreeMap<String, usize>>>,
    delay: Option<Duration>,
    height_delay: Option<Duration>,
    finalized_height: Option<ChainHeight>,
}

type SelectorResult = Result<Vec<LogRecord>, DatalensError>;
type SharedSelectorResults = Arc<Mutex<BTreeMap<String, SelectorResult>>>;

impl FixtureAdapter {
    fn new(chain: ChainIdentity, result: Result<Vec<LogRecord>, DatalensError>) -> Self {
        Self {
            chain,
            result: Arc::new(Mutex::new(result)),
            selector_results: Arc::new(Mutex::new(BTreeMap::new())),
            calls: Arc::new(Mutex::new(BTreeMap::new())),
            delay: None,
            height_delay: None,
            finalized_height: None,
        }
    }

    fn target_fetch_fails(chain: ChainIdentity, target_selector: DatasetSelector) -> Self {
        Self::new(chain, Ok(Vec::new())).with_selector_result(
            target_selector,
            Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "target selector must not be fetched",
            )),
        )
    }

    fn with_selector_result(
        self,
        selector: DatasetSelector,
        result: Result<Vec<LogRecord>, DatalensError>,
    ) -> Self {
        self.selector_results
            .lock()
            .expect("selector results lock")
            .insert(selector.canonical_key(), result);
        self
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    fn with_height_delay(mut self, delay: Duration) -> Self {
        self.height_delay = Some(delay);
        self
    }

    fn with_finalized_height(mut self, finalized_height: ChainHeight) -> Self {
        self.finalized_height = Some(finalized_height);
        self
    }
}

impl ChainAdapter for FixtureAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone()).with_dataset_capability(
            DatasetCapability::new(DatasetKey::evm_logs())
                .with_selector(SelectorKind::EvmLogs)
                .with_range(LedgerRangeKind::Block)
                .with_empty_coverage(true)
                .with_safe_height(true),
        )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(20))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        if let Some(delay) = self.height_delay {
            thread::sleep(delay);
        }
        Ok(ChainHeight::block(20).with_finality(FinalityLevel::Safe))
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        self.finalized_height.clone().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "fixture does not expose finalized height",
            )
        })
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        if let Some(delay) = self.delay {
            thread::sleep(delay);
        }
        let selector_key = request.selector.canonical_key();
        *self
            .calls
            .lock()
            .map_err(|_| DatalensError::internal("fixture calls lock poisoned"))?
            .entry(selector_key.clone())
            .or_default() += 1;
        let logs = self
            .selector_results
            .lock()
            .map_err(|_| DatalensError::internal("fixture selector lock poisoned"))?
            .get(&selector_key)
            .cloned()
            .unwrap_or_else(|| {
                self.result
                    .lock()
                    .map_err(|_| DatalensError::internal("fixture lock poisoned"))
                    .and_then(|result| result.clone())
            })?;
        ChainFetchResponse::try_new(
            request.chain.clone(),
            request.dataset_key.clone(),
            request.range.clone(),
            request.selector.clone(),
            QueryRows::EvmLogs(logs),
        )
        .map(|response| {
            response.with_provider_diagnostics(ProviderDiagnostics {
                calls: 1,
                rows_scanned: 1,
                warnings: Vec::new(),
            })
        })
    }
}

#[derive(Clone)]
struct BlockingReplacementStorage {
    inner: LocalStorage,
    gate: Arc<(Mutex<bool>, Condvar)>,
    completed_write: Arc<AtomicBool>,
    write_attempts: Arc<AtomicUsize>,
}

impl BlockingReplacementStorage {
    fn new(inner: LocalStorage) -> Self {
        Self {
            inner,
            gate: Arc::new((Mutex::new(false), Condvar::new())),
            completed_write: Arc::new(AtomicBool::new(false)),
            write_attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn completed_write(&self) -> bool {
        self.completed_write.load(Ordering::SeqCst)
    }

    fn write_attempts(&self) -> usize {
        self.write_attempts.load(Ordering::SeqCst)
    }

    fn release_write(&self) {
        let (lock, cvar) = &*self.gate;
        let mut released = lock.lock().expect("fixture write gate lock");
        *released = true;
        cvar.notify_all();
    }

    fn wait_for_completed_write(&self, timeout: Duration) {
        let started = std::time::Instant::now();
        while !self.completed_write() && started.elapsed() < timeout {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(self.completed_write());
    }
}

impl StorageRepository for BlockingReplacementStorage {
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        self.inner.manifest()
    }

    fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        self.inner
            .covered_ranges(chain, dataset_key, selector, range)
    }

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.inner.read_rows(chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.inner.write_rows(request)
    }

    fn write_rows_replacing_existing(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        let (lock, cvar) = &*self.gate;
        let mut released = lock
            .lock()
            .map_err(|_| DatalensError::internal("fixture write gate lock poisoned"))?;
        while !*released {
            released = cvar
                .wait(released)
                .map_err(|_| DatalensError::internal("fixture write gate lock poisoned"))?;
        }
        let outcome = self.inner.write_rows_replacing_existing(request)?;
        self.completed_write.store(true, Ordering::SeqCst);
        Ok(outcome)
    }
}

fn submit_request(chain: ChainIdentity, selector: DatasetSelector) -> CacheRepairSubmitRequest {
    CacheRepairSubmitRequest {
        application_id: "test".to_owned(),
        chain,
        dataset_key: DatasetKey::evm_logs(),
        selector,
        source_selectors: Vec::new(),
        range_kind: LedgerRangeKind::Block,
        start: 11,
        end: 11,
        finality: CacheRepairFinality::Safe,
        chunk_policy: CacheRepairChunkPolicy { max_range_len: 1 },
        reason: "test repair".to_owned(),
    }
}

fn selector() -> DatasetSelector {
    exact_selector(topic())
}

fn exact_selector(topic: String) -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec![address()],
        topics: vec![Some(vec![topic])],
    })
    .expect("selector")
}

fn broad_selector() -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec![address()],
        topics: vec![Some(vec![topic(), other_topic()])],
    })
    .expect("broad selector")
}

fn address_only_selector() -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec![address()],
        topics: Vec::new(),
    })
    .expect("address-only selector")
}

fn log_record(block_number: u64, log_index: u64) -> LogRecord {
    log_record_with_topic(block_number, log_index, topic())
}

fn log_record_with_topic(block_number: u64, log_index: u64, topic: String) -> LogRecord {
    LogRecord::try_new(
        block_number,
        format!("0x{:064x}", block_number),
        format!("0x{:064x}", block_number + 1_000),
        0,
        log_index,
        address(),
        vec![topic],
        "0x".to_owned(),
        false,
    )
    .expect("log record")
}

fn address() -> String {
    "0x1111111111111111111111111111111111111111".to_owned()
}

fn topic() -> String {
    "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned()
}

fn other_topic() -> String {
    "0x3333333333333333333333333333333333333333333333333333333333333333".to_owned()
}

fn repair_range() -> LedgerRange {
    LedgerRange::blocks(11, 11).expect("valid range")
}

fn write_empty_coverage(storage: &LocalStorage, chain: &ChainIdentity, selector: &DatasetSelector) {
    write_empty_coverage_with_finality(storage, chain, selector, FinalityLevel::Safe);
}

fn write_empty_coverage_with_finality(
    storage: &LocalStorage,
    chain: &ChainIdentity,
    selector: &DatasetSelector,
    finality_level: FinalityLevel,
) {
    let empty_rows =
        datalens_core::DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("empty rows");
    storage
        .write_rows(StorageWriteRequest {
            chain,
            dataset_key: DatasetKey::evm_logs(),
            selector,
            range: repair_range(),
            rows: &empty_rows,
            finality_level,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");
}

fn calls_for(calls: &Arc<Mutex<BTreeMap<String, usize>>>, selector: &DatasetSelector) -> usize {
    calls
        .lock()
        .expect("calls lock")
        .get(&selector.canonical_key())
        .copied()
        .unwrap_or_default()
}

fn unix_milliseconds_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_millis()
        .try_into()
        .expect("epoch millis fit in u64")
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("chain")
}

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-cache-repair-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn move_cache_repair_task_to_legacy(
    store: &LocalObjectStore,
    registry: &LocalCacheRepairRegistry<LocalObjectStore>,
) -> datalens_cache_repair::CacheRepairTaskId {
    let task_id = registry
        .submit(submit_request(test_chain(), selector()))
        .expect("submit")
        .task_id;
    copy_cache_repair_clean_to_legacy(store, &task_id);
    delete_if_distinct(
        store,
        &cache_repair_clean_task_key(&task_id),
        &cache_repair_legacy_task_key(&task_id),
    );
    task_id
}

fn copy_cache_repair_clean_to_legacy(
    store: &LocalObjectStore,
    task_id: &datalens_cache_repair::CacheRepairTaskId,
) {
    copy_existing_object(
        store,
        &cache_repair_clean_task_key(task_id),
        &cache_repair_legacy_task_key(task_id),
    );
}

fn copy_existing_object(store: &LocalObjectStore, clean_key: &str, legacy_key: &str) {
    let source_key = if store.exists(clean_key).expect("check clean source") {
        clean_key
    } else {
        legacy_key
    };
    let bytes = store.get(source_key).expect("read object source");
    store.put(legacy_key, &bytes).expect("write legacy object");
}

fn delete_if_distinct(store: &LocalObjectStore, key: &str, other_key: &str) {
    if key != other_key {
        store.delete(key).expect("delete object");
    }
}

fn mutate_json_object<F>(store: &LocalObjectStore, key: &str, mutate: F)
where
    F: FnOnce(&mut serde_json::Value),
{
    let mut value: serde_json::Value =
        serde_json::from_slice(&store.get(key).expect("read json object")).expect("decode json");
    mutate(&mut value);
    let bytes = serde_json::to_vec_pretty(&value).expect("encode json");
    store.put(key, &bytes).expect("write json object");
}

fn cache_repair_clean_task_key(task_id: &datalens_cache_repair::CacheRepairTaskId) -> String {
    format!("tasks/{}.json", task_id.as_str())
}

fn cache_repair_legacy_task_key(task_id: &datalens_cache_repair::CacheRepairTaskId) -> String {
    format!("cache-repair/tasks/{}.json", task_id.as_str())
}
