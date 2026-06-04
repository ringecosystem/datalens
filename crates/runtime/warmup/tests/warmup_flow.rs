use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityLevel, ProviderDiagnostics, SelectorKind,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows,
    LedgerRange, LedgerRangeKind, LogFilter, LogRecord, NetworkId, QueryFinalityRequirement,
    QueryRows,
};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput};
use datalens_solana::{SolanaAdapter, solana_all_selector};
use datalens_storage::{
    LocalObjectStore, LocalStorage, QueryWatermark, QueryWatermarkKey, QueryWatermarkRepository,
    QueryWatermarkStore,
};
use datalens_tron::{TronAdapter, tron_all_selector};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupChunkPolicy, WarmupRetryPolicy, WarmupRunStatus, WarmupRuntime,
    WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupSubmitRequest, WarmupTaskMode,
    WarmupTaskPool, WarmupTaskState,
};
use datalens_writer::DurableWriterConfig;

#[test]
fn test_submit_duplicate_task_returns_existing_task_id() {
    let store = object_store("dedupe");
    let registry = LocalWarmupRegistry::new(store);
    let request = submit_request(Some(10), WarmupTaskMode::FixedRange);

    let first = registry.submit(request.clone()).expect("first submit");
    let second = registry.submit(request).expect("second submit");

    assert!(first.created);
    assert!(!second.created);
    assert_eq!(first.task_id, second.task_id);
    let tasks = registry
        .list(datalens_warmup::WarmupTaskFilter::default())
        .unwrap();
    assert_eq!(tasks.len(), 1);
}

#[test]
fn test_warmup_fetches_evm_logs_in_chunks_and_writes_durable_cache() {
    let storage = LocalStorage::new(temp_root("chunked-storage"));
    let registry = LocalWarmupRegistry::new(object_store("chunked-registry"));
    let adapter = FixtureAdapter::new(6).with_max_range_len(2).with_logs(vec![
        log_record(1, 0),
        log_record(3, 0),
        log_record(6, 0),
    ]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone());
    let task_id = registry
        .submit(submit_request(Some(6), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Completed);
    assert_eq!(
        adapter.fetches(),
        vec![blocks(1, 2), blocks(3, 4), blocks(5, 6)]
    );
    assert_eq!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 6))
            .unwrap(),
        vec![blocks(1, 6)]
    );
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 7);
    let task = registry.get(&task_id).unwrap().expect("task");
    assert_eq!(task.state, WarmupTaskState::Completed);
    assert_eq!(task.stats.rows_fetched, 3);
    assert_eq!(task.stats.provider_calls, 3);
}

#[test]
fn test_warmup_fetches_solana_slots_and_writes_durable_cache() {
    let storage = LocalStorage::new(temp_root("solana-storage"));
    let registry = LocalWarmupRegistry::new(object_store("solana-registry"));
    let adapter = SolanaAdapter::with_fixture_defaults();
    let chain = adapter.capabilities().chain().clone();
    let selector = solana_all_selector().expect("selector");
    let runtime = WarmupRuntime::new(adapter, storage.clone(), registry.clone(), writer_config());
    let task_id = registry
        .submit(WarmupSubmitRequest {
            application_id: "app-a".to_owned(),
            chain: chain.clone(),
            dataset_key: DatasetKey::solana_slots(),
            selector: selector.clone(),
            range_kind: LedgerRangeKind::Slot,
            start: 10,
            end: Some(12),
            mode: WarmupTaskMode::FixedRange,
            chunk_policy: WarmupChunkPolicy {
                max_range_len: 3,
                target_rows_hint: None,
            },
            retry_policy: WarmupRetryPolicy::default(),
        })
        .unwrap()
        .task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Completed);
    assert_eq!(result.rows_fetched, 2);
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::solana_slots(),
                &selector,
                LedgerRange::slots(10, 12).unwrap(),
            )
            .unwrap(),
        vec![LedgerRange::slots(10, 12).unwrap()]
    );
}

#[test]
fn test_warmup_metrics_use_dataset_and_selector_labels() {
    let storage = LocalStorage::new(temp_root("solana-metrics-storage"));
    let registry = LocalWarmupRegistry::new(object_store("solana-metrics-registry"));
    let adapter = SolanaAdapter::with_fixture_defaults();
    let chain = adapter.capabilities().chain().clone();
    let selector = solana_all_selector().expect("selector");
    let metrics = MetricsRecorder::new().expect("metrics recorder");
    let runtime = WarmupRuntime::new(adapter, storage, registry.clone(), writer_config())
        .with_metrics(metrics.clone());
    let task_id = registry
        .submit(WarmupSubmitRequest {
            application_id: "app-a".to_owned(),
            chain,
            dataset_key: DatasetKey::solana_slots(),
            selector,
            range_kind: LedgerRangeKind::Slot,
            start: 10,
            end: Some(12),
            mode: WarmupTaskMode::FixedRange,
            chunk_policy: WarmupChunkPolicy {
                max_range_len: 3,
                target_rows_hint: None,
            },
            retry_policy: WarmupRetryPolicy::default(),
        })
        .unwrap()
        .task_id;

    runtime.run_task_once(&task_id).expect("warmup run");

    let output = metrics.encode().expect("metrics text");
    assert!(output.contains(
        r#"datalens_warmup_fetch_total{application="app-a",chain="solana-mainnet-beta",chain_kind="solana",dataset="solana.slots",outcome="fetched",selector_kind="solana_all"} 1"#
    ));
}

#[test]
fn test_warmup_fetches_tron_blocks_and_writes_durable_cache() {
    let storage = LocalStorage::new(temp_root("tron-storage"));
    let registry = LocalWarmupRegistry::new(object_store("tron-registry"));
    let adapter = TronAdapter::with_fixture_defaults();
    let chain = adapter.capabilities().chain().clone();
    let selector = tron_all_selector().expect("selector");
    let runtime = WarmupRuntime::new(adapter, storage.clone(), registry.clone(), writer_config());
    let task_id = registry
        .submit(WarmupSubmitRequest {
            application_id: "app-a".to_owned(),
            chain: chain.clone(),
            dataset_key: DatasetKey::tron_blocks(),
            selector: selector.clone(),
            range_kind: LedgerRangeKind::Block,
            start: 10,
            end: Some(12),
            mode: WarmupTaskMode::FixedRange,
            chunk_policy: WarmupChunkPolicy {
                max_range_len: 3,
                target_rows_hint: None,
            },
            retry_policy: WarmupRetryPolicy::default(),
        })
        .unwrap()
        .task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Completed);
    assert_eq!(result.rows_fetched, 3);
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::tron_blocks(),
                &selector,
                LedgerRange::blocks(10, 12).unwrap(),
            )
            .unwrap(),
        vec![LedgerRange::blocks(10, 12).unwrap()]
    );
}

#[test]
fn test_submit_rejects_unsupported_selector_before_provider_fetch() {
    let storage = LocalStorage::new(temp_root("submit-validation-storage"));
    let registry = LocalWarmupRegistry::new(object_store("submit-validation-registry"));
    let adapter = TronAdapter::with_fixture_defaults();
    let chain = adapter.capabilities().chain().clone();
    let pool = WarmupTaskPool::new(
        WarmupRuntime::new(adapter, storage, registry.clone(), writer_config()),
        WarmupSchedulerConfig::default(),
    );

    let error = pool
        .submit(WarmupSubmitRequest {
            application_id: "app-a".to_owned(),
            chain,
            dataset_key: DatasetKey::tron_blocks(),
            selector: DatasetSelector::all(),
            range_kind: LedgerRangeKind::Block,
            start: 10,
            end: Some(10),
            mode: WarmupTaskMode::FixedRange,
            chunk_policy: WarmupChunkPolicy::default(),
            retry_policy: WarmupRetryPolicy::default(),
        })
        .expect_err("unsupported selector");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
    assert!(registry.list(Default::default()).unwrap().is_empty());
}

#[test]
fn test_persisted_generic_selectors_reload_correctly() {
    let registry = LocalWarmupRegistry::new(object_store("generic-selector-registry"));
    let tron_selector = tron_all_selector().expect("selector");
    let tron_chain = TronAdapter::with_fixture_defaults()
        .capabilities()
        .chain()
        .clone();
    let all_selector_task = registry
        .submit(WarmupSubmitRequest {
            application_id: "app-a".to_owned(),
            chain: chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: DatasetSelector::all(),
            range_kind: LedgerRangeKind::Block,
            start: 1,
            end: Some(1),
            mode: WarmupTaskMode::FixedRange,
            chunk_policy: WarmupChunkPolicy::default(),
            retry_policy: WarmupRetryPolicy::default(),
        })
        .unwrap()
        .task_id;
    let other_selector_task = registry
        .submit(WarmupSubmitRequest {
            application_id: "app-a".to_owned(),
            chain: tron_chain,
            dataset_key: DatasetKey::tron_blocks(),
            selector: tron_selector.clone(),
            range_kind: LedgerRangeKind::Block,
            start: 10,
            end: Some(10),
            mode: WarmupTaskMode::FixedRange,
            chunk_policy: WarmupChunkPolicy::default(),
            retry_policy: WarmupRetryPolicy::default(),
        })
        .unwrap()
        .task_id;

    assert_eq!(
        registry.get(&all_selector_task).unwrap().unwrap().selector,
        DatasetSelector::all()
    );
    assert_eq!(
        registry
            .get(&other_selector_task)
            .unwrap()
            .unwrap()
            .selector,
        tron_selector
    );
}

#[test]
fn test_resume_rechecks_manifest_coverage_instead_of_trusting_cursor() {
    let storage = LocalStorage::new(temp_root("resume-storage"));
    let registry = LocalWarmupRegistry::new(object_store("resume-registry"));
    let adapter = FixtureAdapter::new(6)
        .with_max_range_len(2)
        .with_logs(vec![log_record(5, 0)]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone());
    let task_id = registry
        .submit(submit_request(Some(6), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;

    seed_coverage(&storage, blocks(1, 4));
    registry
        .save_cursor(&datalens_warmup::WarmupCursor {
            task_id: task_id.clone(),
            next: 1,
            last_committed: None,
            current_attempt: 0,
            last_processed_range: None,
            last_error: None,
            updated_at: 1,
        })
        .unwrap();

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Completed);
    assert_eq!(adapter.fetches(), vec![blocks(5, 6)]);
    assert_eq!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 6))
            .unwrap(),
        vec![blocks(1, 6)]
    );
}

#[test]
fn test_empty_fetch_records_empty_coverage_when_writer_allows_it() {
    let storage = LocalStorage::new(temp_root("empty-storage"));
    let registry = LocalWarmupRegistry::new(object_store("empty-registry"));
    let adapter = FixtureAdapter::new(3).with_max_range_len(3);
    let runtime = runtime(adapter, storage.clone(), registry.clone());
    let task_id = registry
        .submit(submit_request(Some(3), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.empty_ranges, 1);
    assert_eq!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 3))
            .unwrap(),
        vec![blocks(1, 3)]
    );
}

#[test]
fn test_warmup_skips_provider_for_mixed_empty_and_data_coverage_and_checkpoints() {
    let storage = LocalStorage::new(temp_root("mixed-hit-storage"));
    let registry = LocalWarmupRegistry::new(object_store("mixed-hit-registry"));
    let adapter = FixtureAdapter::new(3).with_max_range_len(3).with_logs(vec![
        log_record(1, 0),
        log_record(2, 0),
        log_record(3, 0),
    ]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone());
    let task_id = registry
        .submit(submit_request(Some(3), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;

    seed_log_coverage(&storage, blocks(1, 1), vec![log_record(1, 0)]);
    seed_coverage(&storage, blocks(2, 3));

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert!(adapter.fetches().is_empty());
    assert_eq!(result.status, WarmupRunStatus::Completed);
    assert_eq!(result.rows_fetched, 0);
    assert_eq!(result.provider_calls, 0);
    assert_eq!(result.written_ranges, 0);
    assert_eq!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 3))
            .unwrap(),
        vec![blocks(1, 3)]
    );
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 4);
    let task = registry.get(&task_id).unwrap().expect("task");
    assert_eq!(task.state, WarmupTaskState::Completed);
    assert_eq!(task.stats.provider_calls, 0);
}

#[test]
fn test_provider_failure_does_not_advance_cursor_and_marks_task_failed() {
    let storage = LocalStorage::new(temp_root("failure-storage"));
    let registry = LocalWarmupRegistry::new(object_store("failure-registry"));
    let adapter = FixtureAdapter::new(3).with_failure(blocks(1, 3));
    let runtime = runtime(adapter, storage, registry.clone());
    let task_id = registry
        .submit(submit_request(Some(3), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;

    let error = runtime
        .run_task_once(&task_id)
        .expect_err("provider failure");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 1);
    assert_eq!(cursor.current_attempt, 1);
    let task = registry.get(&task_id).unwrap().expect("task");
    assert_eq!(task.state, WarmupTaskState::Failed);
}

#[test]
fn test_cancelled_task_stops_before_fetch_boundary() {
    let storage = LocalStorage::new(temp_root("cancel-storage"));
    let registry = LocalWarmupRegistry::new(object_store("cancel-registry"));
    let adapter = FixtureAdapter::new(3);
    let runtime = runtime(adapter.clone(), storage, registry.clone());
    let task_id = registry
        .submit(submit_request(Some(3), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;
    registry.cancel(&task_id).unwrap();

    let result = runtime.run_task_once(&task_id).expect("cancelled run");

    assert_eq!(result.status, WarmupRunStatus::Stopped);
    assert!(adapter.fetches().is_empty());
}

#[test]
fn test_query_path_hits_warmup_generated_durable_coverage() {
    let storage = LocalStorage::new(temp_root("query-hit-storage"));
    let registry = LocalWarmupRegistry::new(object_store("query-hit-registry"));
    let adapter = FixtureAdapter::new(3)
        .with_max_range_len(3)
        .with_logs(vec![log_record(2, 0)]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone());
    let task_id = registry
        .submit(submit_request(Some(3), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;
    runtime.run_task_once(&task_id).expect("warmup run");
    adapter.clear_fetches();

    let executor = NativeQueryExecutor::new(
        storage,
        adapter.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 10,
                default_chunk_range_len: 2,
            },
            writer: writer_config(),
        },
    );
    let result = executor
        .execute_with_application(
            NativeQueryInput {
                chain: chain(),
                dataset_key: DatasetKey::evm_logs(),
                ledger_range: blocks(1, 3),
                selector: selector(),
                field_selection: FieldSelection::All,
                finality: QueryFinalityRequirement::DurableOnly,
            },
            Some(ApplicationIdentity::named("app-a")),
        )
        .expect("query");

    assert_eq!(result.rows.row_count(), 1);
    assert!(adapter.fetches().is_empty());
}

#[test]
fn test_query_watermark_does_not_directly_update_warmup_cursor() {
    let root = temp_root("follow-query-no-fast-forward");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry = LocalWarmupRegistry::new(object_store("follow-query-no-fast-forward-registry"));
    let adapter = FixtureAdapter::new(12)
        .with_max_range_len(2)
        .with_logs(vec![log_record(9, 0)]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone())
        .with_query_watermarks(watermarks.clone())
        .with_follow_query_lookahead_blocks(0)
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 1,
        });
    let task_id = registry
        .submit(follow_query_request())
        .expect("submit follow query")
        .task_id;
    let cursor_before_query = registry.load_cursor(&task_id).unwrap();
    let executor = NativeQueryExecutor::new(
        storage,
        adapter.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 10,
                default_chunk_range_len: 10,
            },
            writer: writer_config(),
        },
    )
    .with_query_watermarks(watermarks.clone(), ApplicationIdentity::named("app-a"));

    executor
        .execute_with_application(
            NativeQueryInput {
                chain: chain(),
                dataset_key: DatasetKey::evm_logs(),
                ledger_range: blocks(8, 10),
                selector: selector(),
                field_selection: FieldSelection::All,
                finality: QueryFinalityRequirement::DurableOnly,
            },
            Some(ApplicationIdentity::named("app-a")),
        )
        .expect("query fills high range");

    assert_eq!(registry.load_cursor(&task_id).unwrap(), cursor_before_query);
    adapter.clear_fetches();

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(11, 11)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 12);
}

#[test]
fn test_repeated_query_progress_moves_follow_query_target_forward() {
    let root = temp_root("follow-query-repeated-progress");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry =
        LocalWarmupRegistry::new(object_store("follow-query-repeated-progress-registry"));
    let adapter = FixtureAdapter::new(5)
        .with_max_range_len(100)
        .with_logs(vec![log_record(2, 0), log_record(5, 0)]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone())
        .with_query_watermarks(watermarks.clone())
        .with_follow_query_lookahead_blocks(0)
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 10,
        });
    let task_id = registry
        .submit(follow_query_request())
        .expect("submit follow query")
        .task_id;
    let executor = NativeQueryExecutor::new(
        storage,
        adapter.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 10,
                default_chunk_range_len: 10,
            },
            writer: writer_config(),
        },
    )
    .with_query_watermarks(watermarks.clone(), ApplicationIdentity::named("app-a"));

    execute_log_query(&executor, blocks(1, 3));
    let first = runtime.run_task_once(&task_id).expect("first warmup");
    assert_eq!(first.status, WarmupRunStatus::Partial);
    assert_eq!(registry.load_cursor(&task_id).unwrap().unwrap().next, 6);

    execute_log_query(&executor, blocks(4, 5));
    let second = runtime.run_task_once(&task_id).expect("second warmup");

    assert_eq!(second.status, WarmupRunStatus::Stopped);
    assert_eq!(registry.load_cursor(&task_id).unwrap().unwrap().next, 6);
    let key = QueryWatermarkKey::new(
        "app-a",
        chain(),
        DatasetKey::evm_logs(),
        &selector(),
        LedgerRangeKind::Block,
    );
    assert_eq!(
        watermarks
            .read(&key)
            .expect("read watermark")
            .expect("watermark")
            .latest_block,
        5
    );
}

#[test]
fn test_follow_query_realigns_old_cursor_to_adaptive_lookahead_frontier() {
    let root = temp_root("follow-query-realigns-old-cursor");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry = LocalWarmupRegistry::new(object_store("follow-query-realigns-old-cursor"));
    let adapter = FixtureAdapter::new(110_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(105_000, 0)]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone())
        .with_query_watermarks(watermarks.clone())
        .with_follow_query_lookahead_blocks(3)
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 1,
        });
    let task_id = registry
        .submit(follow_query_request())
        .expect("submit follow query")
        .task_id;
    save_query_watermark(&watermarks, 100_000);

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(105_000, 105_000)]);
    assert!(
        storage
            .covered_ranges(
                &chain(),
                &DatasetKey::evm_logs(),
                &selector(),
                blocks(100, 104_999)
            )
            .unwrap()
            .is_empty()
    );
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 105_001);
}

#[test]
fn test_follow_query_missing_watermark_noops_without_cursor_change() {
    let storage = LocalStorage::new(temp_root("follow-query-missing-watermark-storage"));
    let registry = LocalWarmupRegistry::new(object_store("follow-query-missing-watermark"));
    let adapter = FixtureAdapter::new(110_000).with_max_range_len(1);
    let runtime = runtime(adapter.clone(), storage, registry.clone());
    let task_id = registry
        .submit(follow_query_request())
        .expect("submit follow query")
        .task_id;
    let cursor_before = registry.load_cursor(&task_id).unwrap();

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Stopped);
    assert!(adapter.fetches().is_empty());
    assert_eq!(registry.load_cursor(&task_id).unwrap(), cursor_before);
}

#[test]
fn test_follow_query_skips_existing_coverage_inside_lookahead_range() {
    let root = temp_root("follow-query-skips-existing-lookahead-coverage");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry = LocalWarmupRegistry::new(object_store(
        "follow-query-skips-existing-lookahead-coverage",
    ));
    let adapter = FixtureAdapter::new(106_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(105_001, 0)]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone())
        .with_query_watermarks(watermarks.clone())
        .with_follow_query_lookahead_blocks(3)
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 1,
        });
    let task_id = registry
        .submit(follow_query_request())
        .expect("submit follow query")
        .task_id;
    save_query_watermark(&watermarks, 100_000);
    seed_coverage(&storage, blocks(105_000, 105_000));

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(105_001, 105_001)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 105_002);
}

#[test]
fn test_task_pool_runs_available_tasks_with_global_bound() {
    let storage = LocalStorage::new(temp_root("pool-storage"));
    let registry = LocalWarmupRegistry::new(object_store("pool-registry"));
    let adapter = FixtureAdapter::new(5).with_max_range_len(5);
    let pool = WarmupTaskPool::new(
        runtime(adapter.clone(), storage, registry.clone()),
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    );
    let first = registry
        .submit(submit_request(Some(1), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;
    let mut second_request = submit_request(Some(5), WarmupTaskMode::FixedRange);
    second_request.start = 2;
    let second = registry.submit(second_request).unwrap().task_id;

    let first_tick = pool.run_available_once().expect("first tick");
    let second_tick = pool.run_available_once().expect("second tick");

    assert_eq!(first_tick.len(), 1);
    assert_eq!(second_tick.len(), 1);
    assert_eq!(
        registry.get(&first).unwrap().unwrap().state,
        WarmupTaskState::Completed
    );
    assert_eq!(
        registry.get(&second).unwrap().unwrap().state,
        WarmupTaskState::Completed
    );
}

#[test]
fn test_task_pool_scopes_shared_registry_to_adapter_chain() {
    let storage = LocalStorage::new(temp_root("pool-shared-chain-storage"));
    let registry = LocalWarmupRegistry::new(object_store("pool-shared-chain-registry"));
    let adapter = FixtureAdapter::new(3).with_max_range_len(3);
    let pool = WarmupTaskPool::new(
        runtime(adapter.clone(), storage, registry.clone()),
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 10,
            max_concurrent_tasks_per_chain: 10,
        },
    );
    let ethereum = registry
        .submit(submit_request(Some(1), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;
    let mut polygon_request = submit_request(Some(1), WarmupTaskMode::FixedRange);
    polygon_request.chain = polygon_chain();
    let polygon = registry.submit(polygon_request).unwrap().task_id;

    assert_eq!(pool.list(Default::default()).unwrap().len(), 1);
    assert_eq!(
        pool.list(datalens_warmup::WarmupTaskFilter {
            chain_key: Some(polygon_chain().key_prefix()),
            ..Default::default()
        })
        .unwrap(),
        Vec::new()
    );

    let results = pool.run_available_once().expect("run matching task only");

    assert_eq!(results.len(), 1);
    assert_eq!(
        registry.get(&ethereum).unwrap().unwrap().state,
        WarmupTaskState::Completed
    );
    assert_eq!(
        registry.get(&polygon).unwrap().unwrap().state,
        WarmupTaskState::Queued
    );
    assert_eq!(adapter.fetches(), vec![blocks(1, 1)]);
}

#[test]
fn test_fetch_loop_bound_leaves_fixed_task_partial_until_next_run() {
    let storage = LocalStorage::new(temp_root("bounded-storage"));
    let registry = LocalWarmupRegistry::new(object_store("bounded-registry"));
    let adapter = FixtureAdapter::new(4).with_max_range_len(2);
    let runtime =
        runtime(adapter, storage, registry.clone()).with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 1,
        });
    let task_id = registry
        .submit(submit_request(Some(4), WarmupTaskMode::FixedRange))
        .unwrap()
        .task_id;

    let first = runtime.run_task_once(&task_id).expect("first run");
    assert_eq!(first.status, WarmupRunStatus::Partial);
    assert_eq!(
        registry.get(&task_id).unwrap().unwrap().state,
        WarmupTaskState::Queued
    );

    let second = runtime.run_task_once(&task_id).expect("second run");

    assert_eq!(
        registry.get(&task_id).unwrap().unwrap().state,
        WarmupTaskState::Completed
    );
    assert_eq!(second.status, WarmupRunStatus::Completed);
}

fn runtime(
    adapter: FixtureAdapter,
    storage: LocalStorage,
    registry: LocalWarmupRegistry<LocalObjectStore>,
) -> WarmupRuntime<FixtureAdapter, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    WarmupRuntime::new(adapter, storage, registry, writer_config())
}

fn writer_config() -> DurableWriterConfig {
    DurableWriterConfig {
        target_object_bytes: 1_000_000,
        min_object_rows: 10,
        record_empty_coverage: true,
        staging: Default::default(),
    }
}

fn submit_request(end: Option<u64>, mode: WarmupTaskMode) -> WarmupSubmitRequest {
    WarmupSubmitRequest {
        application_id: "app-a".to_owned(),
        chain: chain(),
        dataset_key: DatasetKey::evm_logs(),
        selector: selector(),
        range_kind: LedgerRangeKind::Block,
        start: 1,
        end,
        mode,
        chunk_policy: WarmupChunkPolicy {
            max_range_len: 100,
            target_rows_hint: None,
        },
        retry_policy: WarmupRetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
    }
}

fn follow_query_request() -> WarmupSubmitRequest {
    WarmupSubmitRequest {
        end: None,
        mode: WarmupTaskMode::FollowQuery,
        ..submit_request(None, WarmupTaskMode::FollowQuery)
    }
}

fn execute_log_query<R>(executor: &NativeQueryExecutor<R, FixtureAdapter>, range: LedgerRange)
where
    R: datalens_storage::StorageRepository + Clone,
{
    executor
        .execute_with_application(
            NativeQueryInput {
                chain: chain(),
                dataset_key: DatasetKey::evm_logs(),
                ledger_range: range,
                selector: selector(),
                field_selection: FieldSelection::All,
                finality: QueryFinalityRequirement::DurableOnly,
            },
            Some(ApplicationIdentity::named("app-a")),
        )
        .expect("query succeeds");
}

fn save_query_watermark<S>(watermarks: &QueryWatermarkStore<S>, latest_block: u64)
where
    S: datalens_storage::ObjectStore + 'static,
{
    watermarks
        .update(&QueryWatermark {
            key: QueryWatermarkKey::new(
                "app-a",
                chain(),
                DatasetKey::evm_logs(),
                &selector(),
                LedgerRangeKind::Block,
            ),
            latest_block,
            updated_at_unix_seconds: 1,
        })
        .expect("save query watermark");
}

fn object_store(name: &str) -> LocalObjectStore {
    LocalObjectStore::new(temp_root(name))
}

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("datalens-warmup-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    root
}

fn chain() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1))).unwrap()
}

fn polygon_chain() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137))).unwrap()
}

fn selector() -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec!["0x0000000000000000000000000000000000000001".to_owned()],
        topics: Vec::new(),
    })
    .unwrap()
}

fn blocks(start: u64, end: u64) -> LedgerRange {
    LedgerRange::blocks(start, end).unwrap()
}

fn log_record(block_number: u64, log_index: u64) -> LogRecord {
    LogRecord {
        block_number,
        block_hash: format!("0x{:064x}", block_number + 10_000),
        parent_hash: None,
        block_timestamp: None,
        transaction_hash: format!("0x{block_number:064x}"),
        transaction_index: 0,
        log_index,
        address: "0x0000000000000000000000000000000000000001".to_owned(),
        topics: Vec::new(),
        data: "0x".to_owned(),
        removed: false,
    }
}

fn seed_coverage(storage: &LocalStorage, range: LedgerRange) {
    seed_log_coverage(storage, range, Vec::new());
}

fn seed_log_coverage(storage: &LocalStorage, range: LedgerRange, logs: Vec<LogRecord>) {
    datalens_writer::DurableWriter::new(storage.clone(), writer_config())
        .write(datalens_writer::DurableWriteRequest {
            chain: chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: selector(),
            finality_level: FinalityLevel::Safe,
            segments: vec![datalens_writer::DurableWriteSegment {
                range,
                rows: DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(logs)).unwrap(),
            }],
        })
        .unwrap();
}

#[derive(Clone)]
struct FixtureAdapter {
    inner: Arc<Mutex<FixtureState>>,
}

#[derive(Default)]
struct FixtureState {
    safe_height: u64,
    max_range_len: u64,
    logs: Vec<LogRecord>,
    failures: Vec<(LedgerRange, DatalensError)>,
    fetches: Vec<LedgerRange>,
}

impl FixtureAdapter {
    fn new(safe_height: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FixtureState {
                safe_height,
                max_range_len: 1_000,
                ..FixtureState::default()
            })),
        }
    }

    fn with_max_range_len(self, max_range_len: u64) -> Self {
        self.inner.lock().unwrap().max_range_len = max_range_len;
        self
    }

    fn with_logs(self, logs: Vec<LogRecord>) -> Self {
        self.inner.lock().unwrap().logs = logs;
        self
    }

    fn with_failure(self, range: LedgerRange) -> Self {
        self.inner.lock().unwrap().failures.push((
            range,
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "fixture provider failure",
            ),
        ));
        self
    }

    fn fetches(&self) -> Vec<LedgerRange> {
        self.inner.lock().unwrap().fetches.clone()
    }

    fn clear_fetches(&self) {
        self.inner.lock().unwrap().fetches.clear();
    }
}

impl ChainAdapter for FixtureAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        let state = self.inner.lock().unwrap();
        AdapterCapabilities::new(chain()).with_dataset_capability(
            DatasetCapability::new(DatasetKey::evm_logs())
                .with_selector(SelectorKind::EvmLogs)
                .with_range(LedgerRangeKind::Block)
                .with_max_range_len(state.max_range_len)
                .with_empty_coverage(true)
                .with_safe_height(true),
        )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        self.cache_safe_height()
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(self.inner.lock().unwrap().safe_height)
            .with_finality(FinalityLevel::Safe))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let mut state = self.inner.lock().unwrap();
        state.fetches.push(request.range.clone());
        if let Some((_, error)) = state
            .failures
            .iter()
            .find(|(range, _)| range == &request.range)
        {
            return Err(error.clone());
        }
        let logs = state
            .logs
            .iter()
            .filter(|log| request.range.start() <= log.block_number)
            .filter(|log| log.block_number <= request.range.end())
            .cloned()
            .collect::<Vec<_>>();
        Ok(ChainFetchResponse::try_new(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
            QueryRows::EvmLogs(logs),
        )
        .unwrap()
        .with_provider_diagnostics(ProviderDiagnostics {
            calls: 1,
            rows_scanned: 0,
            warnings: Vec::new(),
        }))
    }
}
