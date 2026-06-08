use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
    CreateDurablePromotionIntent, DurablePromotionIntent, DurablePromotionIntentCreateOutcome,
    DurablePromotionIntentRepository, DurablePromotionIntentStatus, LocalObjectStore, LocalStorage,
    QueryActivity, QueryActivityKey, QueryActivityRepository, QueryActivityStore, QueryWatermark,
    QueryWatermarkKey, QueryWatermarkRepository, QueryWatermarkStore,
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
fn test_ensure_follow_query_with_different_start_returns_existing_task_id() {
    let registry = LocalWarmupRegistry::new(object_store("ensure-follow-query-dedupe"));
    let first_request = follow_query_request();
    let mut second_request = follow_query_request();
    second_request.start = 500;
    second_request.end = Some(550);

    let first = registry.ensure(first_request).expect("first ensure");
    let second = registry.ensure(second_request).expect("second ensure");

    assert!(first.created);
    assert!(!second.created);
    assert_eq!(first.task_id, second.task_id);
    assert_eq!(first.state, WarmupTaskState::Queued);
    assert_eq!(second.state, WarmupTaskState::Queued);
    let tasks = registry
        .list(datalens_warmup::WarmupTaskFilter::default())
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].start, 1);
    assert_eq!(tasks[0].end, None);
}

#[test]
fn test_submit_follow_query_with_different_start_uses_ensure_identity() {
    let registry = LocalWarmupRegistry::new(object_store("submit-follow-query-ensure-identity"));
    let first_request = follow_query_request();
    let mut second_request = follow_query_request();
    second_request.start = 500;
    second_request.end = Some(550);

    let first = registry.submit(first_request).expect("first submit");
    let second = registry.submit(second_request).expect("second submit");

    assert!(first.created);
    assert!(!second.created);
    assert_eq!(first.task_id, second.task_id);
    let tasks = registry
        .list(datalens_warmup::WarmupTaskFilter::default())
        .unwrap();
    assert_eq!(tasks.len(), 1);
}

#[test]
fn test_ensure_follow_query_different_selector_or_chain_creates_different_tasks() {
    let registry = LocalWarmupRegistry::new(object_store("ensure-follow-query-scope"));
    let base = registry
        .ensure(follow_query_request())
        .expect("base ensure")
        .task_id;
    let mut selector_request = follow_query_request();
    selector_request.selector = DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec!["0x0000000000000000000000000000000000000002".to_owned()],
        topics: Vec::new(),
    })
    .unwrap();
    let selector = registry
        .ensure(selector_request)
        .expect("selector ensure")
        .task_id;
    let mut chain_request = follow_query_request();
    chain_request.chain = polygon_chain();
    let chain = registry
        .ensure(chain_request)
        .expect("chain ensure")
        .task_id;

    assert_ne!(base, selector);
    assert_ne!(base, chain);
    assert_ne!(selector, chain);
    let tasks = registry
        .list(datalens_warmup::WarmupTaskFilter::default())
        .unwrap();
    assert_eq!(tasks.len(), 3);
}

#[test]
fn test_ensure_follow_query_returns_existing_non_running_task_state() {
    let registry = LocalWarmupRegistry::new(object_store("ensure-follow-query-states"));

    let paused = registry
        .ensure(follow_query_request())
        .expect("paused ensure")
        .task_id;
    registry.pause(&paused).expect("pause task");
    assert_existing_ensure_state(&registry, WarmupTaskState::Paused);

    let mut failed_task = registry.get(&paused).unwrap().unwrap();
    failed_task.state = WarmupTaskState::Failed;
    failed_task.last_error = Some("provider unavailable".to_owned());
    registry.save_task(&failed_task).expect("save failed task");
    assert_existing_ensure_state(&registry, WarmupTaskState::Failed);

    registry.cancel(&paused).expect("cancel task");
    assert_existing_ensure_state(&registry, WarmupTaskState::Cancelled);

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
fn test_warmup_with_durable_intents_schedules_without_fetching_or_advancing_cursor() {
    let adapter = FixtureAdapter::new(10).with_logs(vec![log_record(1, 0)]);
    let storage = LocalStorage::new(temp_root("intent-warmup-storage"));
    let registry = LocalWarmupRegistry::new(object_store("intent-warmup-registry"));
    let intents = RecordingIntentRepository::default();
    let recorded = intents.recorded.clone();
    let runtime = WarmupRuntime::new(adapter.clone(), storage, registry.clone(), writer_config())
        .with_durable_intents(intents);
    let request = submit_request(Some(3), WarmupTaskMode::FixedRange);
    let task_id = registry.submit(request).expect("submit").task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert!(adapter.fetches().is_empty());
    let cursor = registry
        .load_cursor(&task_id)
        .expect("load cursor")
        .expect("cursor exists");
    assert_eq!(cursor.next, 1);
    let recorded = recorded.lock().expect("recorded intents");
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].application, "app-a");
    assert_eq!(recorded[0].ranges, vec![blocks(1, 3)]);
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
    wait_for_query_watermark(&watermarks, 10);

    assert_eq!(registry.load_cursor(&task_id).unwrap(), cursor_before_query);
    adapter.clear_fetches();

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(11, 12)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 13);
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
    wait_for_query_watermark(&watermarks, 3);
    let first = runtime.run_task_once(&task_id).expect("first warmup");
    assert_eq!(first.status, WarmupRunStatus::Partial);
    assert_eq!(registry.load_cursor(&task_id).unwrap().unwrap().next, 6);

    execute_log_query(&executor, blocks(4, 5));
    wait_for_query_watermark(&watermarks, 5);
    let second = runtime.run_task_once(&task_id).expect("second warmup");

    assert_eq!(second.status, WarmupRunStatus::Stopped);
    assert_eq!(registry.load_cursor(&task_id).unwrap().unwrap().next, 6);
    assert_eq!(wait_for_query_watermark(&watermarks, 5), 5);
}

#[test]
fn test_follow_query_realigns_old_cursor_to_adaptive_lookahead_frontier() {
    let root = temp_root("follow-query-realigns-old-cursor");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry = LocalWarmupRegistry::new(object_store("follow-query-realigns-old-cursor"));
    let adapter = FixtureAdapter::new(110_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(101_000, 0)]);
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
    assert_eq!(adapter.fetches(), vec![blocks(101_000, 101_000)]);
    assert!(
        storage
            .covered_ranges(
                &chain(),
                &DatasetKey::evm_logs(),
                &selector(),
                blocks(100, 100_999)
            )
            .unwrap()
            .is_empty()
    );
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 101_001);
}

#[test]
fn test_follow_query_jumps_forward_when_query_nears_current_cursor() {
    let root = temp_root("follow-query-jumps-near-current-cursor");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry = LocalWarmupRegistry::new(object_store("follow-query-jumps-near-current-cursor"));
    let adapter = FixtureAdapter::new(110_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(101_800, 0)]);
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
    registry
        .save_cursor(&datalens_warmup::WarmupCursor {
            task_id: task_id.clone(),
            next: 101_000,
            last_committed: None,
            current_attempt: 0,
            last_processed_range: None,
            last_error: None,
            updated_at: 1,
        })
        .unwrap();
    save_query_watermark(&watermarks, 100_800);

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(101_800, 101_800)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 101_801);
}

#[test]
fn test_follow_query_keeps_healthy_cursor_ahead_when_query_is_outside_catchup_threshold() {
    let root = temp_root("follow-query-keeps-cursor-outside-threshold");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry =
        LocalWarmupRegistry::new(object_store("follow-query-keeps-cursor-outside-threshold"));
    let adapter = FixtureAdapter::new(110_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(102_000, 0)]);
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
    registry
        .save_cursor(&datalens_warmup::WarmupCursor {
            task_id: task_id.clone(),
            next: 102_000,
            last_committed: None,
            current_attempt: 0,
            last_processed_range: None,
            last_error: None,
            updated_at: 1,
        })
        .unwrap();
    save_query_watermark(&watermarks, 100_800);

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(102_000, 102_000)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 102_001);
}

#[test]
fn test_follow_query_reanchors_far_ahead_cursor_to_adaptive_offset() {
    let root = temp_root("follow-query-reanchors-far-ahead-cursor");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let registry =
        LocalWarmupRegistry::new(object_store("follow-query-reanchors-far-ahead-cursor"));
    let adapter = FixtureAdapter::new(140_000_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(101_800, 0)]);
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
    registry
        .save_cursor(&datalens_warmup::WarmupCursor {
            task_id: task_id.clone(),
            next: 130_797_500,
            last_committed: None,
            current_attempt: 0,
            last_processed_range: None,
            last_error: None,
            updated_at: 1,
        })
        .unwrap();
    save_query_watermark(&watermarks, 100_800);

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(101_800, 101_800)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 101_801);
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
        .with_logs(vec![log_record(101_001, 0)]);
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
    seed_coverage(&storage, blocks(101_000, 101_000));

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(101_001, 101_001)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 101_002);
}

#[test]
fn test_follow_query_uses_fresh_query_activity_before_monotonic_watermark() {
    let root = temp_root("follow-query-fresh-activity-before-watermark");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let activities = QueryActivityStore::new(LocalObjectStore::new(&root));
    let registry = LocalWarmupRegistry::new(object_store(
        "follow-query-fresh-activity-before-watermark-registry",
    ));
    let adapter = FixtureAdapter::new(10_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(2_001, 0)]);
    let runtime = runtime(adapter.clone(), storage.clone(), registry.clone())
        .with_query_watermarks(watermarks.clone())
        .with_query_activity(activities.clone())
        .with_follow_query_lookahead_blocks(3)
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 1,
        });
    let task_id = registry
        .submit(follow_query_request())
        .expect("submit follow query")
        .task_id;
    save_query_watermark(&watermarks, 100_000);
    save_query_activity(&activities, blocks(990, 1_000), now_unix_seconds());
    seed_coverage(&storage, blocks(2_000, 2_000));

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(2_001, 2_001)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 2_002);
}

#[test]
fn test_follow_query_falls_back_to_watermark_when_query_activity_is_stale() {
    let root = temp_root("follow-query-stale-activity-fallback");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let activities = QueryActivityStore::new(LocalObjectStore::new(&root));
    let registry = LocalWarmupRegistry::new(object_store("follow-query-stale-activity-fallback"));
    let adapter = FixtureAdapter::new(110_000)
        .with_max_range_len(1)
        .with_logs(vec![log_record(101_000, 0)]);
    let runtime = runtime(adapter.clone(), storage, registry.clone())
        .with_query_watermarks(watermarks.clone())
        .with_query_activity(activities.clone())
        .with_query_activity_ttl_seconds(5)
        .with_follow_query_lookahead_blocks(3)
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 1,
        });
    let task_id = registry
        .submit(follow_query_request())
        .expect("submit follow query")
        .task_id;
    save_query_watermark(&watermarks, 100_000);
    save_query_activity(&activities, blocks(990, 1_000), 1);

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(adapter.fetches(), vec![blocks(101_000, 101_000)]);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 101_001);
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
fn test_task_pool_prioritizes_active_follow_query_watermark() {
    let storage = LocalStorage::new(temp_root("pool-follow-query-priority-storage"));
    let registry = LocalWarmupRegistry::new(object_store("pool-follow-query-priority-registry"));
    let watermarks =
        QueryWatermarkStore::new(object_store("pool-follow-query-priority-watermarks"));
    let adapter = FixtureAdapter::new(2_000).with_max_range_len(1);
    let pool = WarmupTaskPool::new(
        runtime(adapter.clone(), storage, registry.clone())
            .with_query_watermarks(watermarks.clone())
            .with_follow_query_start_offset_blocks(Some(1))
            .with_follow_query_lookahead_blocks(1)
            .with_runtime_config(WarmupRuntimeConfig {
                max_fetches_per_task_loop: 1,
            }),
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    );
    let active = registry
        .submit(follow_query_request())
        .expect("active follow query")
        .task_id;
    save_query_watermark(&watermarks, 1_000);
    let stale = ensure_lower_task_id_without_watermark(&registry, &active);

    let results = pool.run_available_once().expect("run prioritized task");

    assert_eq!(results.len(), 1);
    assert_eq!(adapter.fetches(), vec![blocks(1_001, 1_001)]);
    assert_eq!(registry.load_cursor(&active).unwrap().unwrap().next, 1_002);
    assert_eq!(registry.load_cursor(&stale).unwrap().unwrap().next, 1);
}

#[test]
fn test_task_pool_rotates_between_low_lead_follow_query_tasks() {
    let storage = LocalStorage::new(temp_root("pool-follow-query-fairness-storage"));
    let registry = LocalWarmupRegistry::new(object_store("pool-follow-query-fairness-registry"));
    let watermarks =
        QueryWatermarkStore::new(object_store("pool-follow-query-fairness-watermarks"));
    let adapter = FixtureAdapter::new(2_000).with_max_range_len(1);
    let pool = WarmupTaskPool::new(
        runtime(adapter, storage, registry.clone())
            .with_query_watermarks(watermarks.clone())
            .with_follow_query_start_offset_blocks(Some(1))
            .with_follow_query_lookahead_blocks(1)
            .with_runtime_config(WarmupRuntimeConfig {
                max_fetches_per_task_loop: 1,
            }),
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    );
    let first = registry
        .submit(follow_query_request())
        .expect("first follow query")
        .task_id;
    let mut second_request = follow_query_request();
    second_request.selector = second_selector();
    let second = registry
        .submit(second_request)
        .expect("second follow query")
        .task_id;
    save_query_watermark(&watermarks, 1_000);
    save_query_watermark_for(&watermarks, "app-a", &second_selector(), 1_000);

    pool.run_available_once().expect("first tick");
    pool.run_available_once().expect("second tick");

    assert!(
        registry.load_cursor(&first).unwrap().unwrap().next > 1,
        "first follow_query task should get a run opportunity"
    );
    assert!(
        registry.load_cursor(&second).unwrap().unwrap().next > 1,
        "second follow_query task should get a run opportunity"
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

fn ensure_lower_task_id_without_watermark(
    registry: &LocalWarmupRegistry<LocalObjectStore>,
    active: &datalens_warmup::WarmupTaskId,
) -> datalens_warmup::WarmupTaskId {
    for index in 0..512 {
        let mut request = follow_query_request();
        request.application_id = format!("inactive-{index}");
        let stale = registry
            .ensure(request)
            .expect("inactive follow query")
            .task_id;
        if stale.as_str() < active.as_str() {
            return stale;
        }
    }
    panic!("could not create a lower sorted inactive follow_query task");
}

fn assert_existing_ensure_state(
    registry: &LocalWarmupRegistry<LocalObjectStore>,
    state: WarmupTaskState,
) {
    let mut request = follow_query_request();
    request.start = 100;

    let outcome = registry.ensure(request).expect("ensure existing task");

    assert!(!outcome.created);
    assert_eq!(outcome.state, state);
}

fn execute_log_query<R>(executor: &NativeQueryExecutor<R, FixtureAdapter>, range: LedgerRange)
where
    R: datalens_storage::StorageRepository + Clone + 'static,
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
    save_query_watermark_for(watermarks, "app-a", &selector(), latest_block);
}

fn save_query_watermark_for<S>(
    watermarks: &QueryWatermarkStore<S>,
    application_id: &str,
    selector: &DatasetSelector,
    latest_block: u64,
) where
    S: datalens_storage::ObjectStore + 'static,
{
    watermarks
        .update(&QueryWatermark {
            key: QueryWatermarkKey::new(
                application_id,
                chain(),
                DatasetKey::evm_logs(),
                selector,
                LedgerRangeKind::Block,
            ),
            latest_block,
            updated_at_unix_seconds: 1,
        })
        .expect("save query watermark");
}

fn save_query_activity<S>(
    activities: &QueryActivityStore<S>,
    latest_range: LedgerRange,
    updated_at: u64,
) where
    S: datalens_storage::ObjectStore + 'static,
{
    activities
        .update(&QueryActivity {
            key: QueryActivityKey::new(
                "app-a",
                chain(),
                DatasetKey::evm_logs(),
                &selector(),
                LedgerRangeKind::Block,
            ),
            latest_range,
            updated_at_unix_seconds: updated_at,
            request_id: Some("query-activity-test".to_owned()),
        })
        .expect("save query activity");
}

fn wait_for_query_watermark<S>(watermarks: &QueryWatermarkStore<S>, expected: u64) -> u64
where
    S: datalens_storage::ObjectStore + 'static,
{
    let key = QueryWatermarkKey::new(
        "app-a",
        chain(),
        DatasetKey::evm_logs(),
        &selector(),
        LedgerRangeKind::Block,
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(watermark) = watermarks.read(&key).expect("read watermark")
            && watermark.latest_block >= expected
        {
            return watermark.latest_block;
        }
        if Instant::now() >= deadline {
            panic!("watermark did not reach {expected}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_secs()
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

fn second_selector() -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec!["0x0000000000000000000000000000000000000002".to_owned()],
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

#[derive(Clone, Default)]
struct RecordingIntentRepository {
    recorded: Arc<Mutex<Vec<CreateDurablePromotionIntent>>>,
}

impl DurablePromotionIntentRepository for RecordingIntentRepository {
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        self.recorded
            .lock()
            .expect("recorded intent lock")
            .push(request.clone());
        Ok(DurablePromotionIntentCreateOutcome::Created(
            intent_from_request(request),
        ))
    }

    fn get(&self, _intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn list_pending(
        &self,
        _now_unix_seconds: u64,
        _limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        Ok(Vec::new())
    }

    fn list_pending_for_chain(
        &self,
        _chain: &ChainIdentity,
        _now_unix_seconds: u64,
        _limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        Ok(Vec::new())
    }

    fn mark_running(
        &self,
        _intent_id: &str,
        _now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn mark_completed(
        &self,
        _intent_id: &str,
        _now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn mark_retryable_failure(
        &self,
        _intent_id: &str,
        _error: &str,
        _now_unix_seconds: u64,
        _next_retry_at_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn mark_terminal_failure(
        &self,
        _intent_id: &str,
        _error: &str,
        _now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn reset_stale_running(
        &self,
        _stale_before_unix_seconds: u64,
        _now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        Ok(Vec::new())
    }
}

fn intent_from_request(request: CreateDurablePromotionIntent) -> DurablePromotionIntent {
    DurablePromotionIntent {
        intent_id: "test-intent".to_owned(),
        dedupe_key: "test-dedupe".to_owned(),
        source: request.source,
        application: request.application,
        chain: request.chain,
        dataset_key: request.dataset_key,
        selector: request.selector,
        selector_fingerprint: request.selector_fingerprint,
        selector_canonical_key: request.selector_canonical_key,
        finality: request.finality,
        ranges: request.ranges,
        status: DurablePromotionIntentStatus::Pending,
        attempt_count: 0,
        next_retry_at_unix_seconds: None,
        created_at_unix_seconds: request.now_unix_seconds,
        updated_at_unix_seconds: request.now_unix_seconds,
        last_error: None,
        request_id: request.request_id,
        task_id: request.task_id,
    }
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
