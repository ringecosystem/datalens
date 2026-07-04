use std::{
    path::PathBuf,
    sync::{
        Arc, Barrier, Condvar, Mutex, Once,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, DatasetRows, LedgerRange, LogFilter, LogRecord, NetworkId, QueryDataFinality,
    QueryFinalityRequirement, QueryRows, QuerySegmentSource,
};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput};
use datalens_storage::{
    CacheOutcome, CreateDurablePromotionIntent, DurablePromotionIntent,
    DurablePromotionIntentBacklog, DurablePromotionIntentCreateOutcome,
    DurablePromotionIntentRepository, DurablePromotionIntentSource, DurablePromotionIntentStatus,
    DurablePromotionIntentStore, FillOutcome, LocalObjectStore, LocalStorage, Manifest,
    ObjectListPage, ObjectMetadata, ObjectPutIfAbsentResult, ObjectStore, QueryActivityKey,
    QueryActivityRepository, QueryActivityStore, QueryOutcome, QueryWatermarkKey,
    QueryWatermarkRepository, QueryWatermarkStore, StorageRepository, StorageWriteOutcome,
    StorageWriteRequest, UsageLedgerRepository, UsageLedgerStore,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriterConfig, WriteStagingConfig,
};

#[test]
fn test_executor_cache_hit_reads_cache_without_fetch() {
    let storage = LocalStorage::new(temp_storage_root("executor-hit"));
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let source = MockSource::default().with_blocks(vec![block(1, "0x01"), block(2, "0x02")]);
    let executor = executor(storage, source.clone());

    let result = executor
        .execute(blocks_input(1, 2))
        .expect("cache hit succeeds");

    assert_eq!(
        result.cache.hit_ranges,
        vec![LedgerRange::blocks(1, 2).unwrap()]
    );
    assert_eq!(result.cache.missing_ranges, Vec::<LedgerRange>::new());
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert_eq!(block_numbers(&result.rows), vec![1, 2]);
}

#[test]
fn test_executor_full_cache_hit_does_not_require_provider_safe_height() {
    let storage = LocalStorage::new(temp_storage_root("executor-hit-provider-down"));
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let source = MockSource::default().with_safe_height_error(DatalensErrorKind::ProviderFailure);
    let executor = executor(storage, source.clone());

    let result = executor
        .execute(blocks_input(1, 2))
        .expect("full cache hit succeeds without provider safe height");

    assert_eq!(
        result.cache.hit_ranges,
        vec![LedgerRange::blocks(1, 2).unwrap()]
    );
    assert_eq!(result.cache.missing_ranges, Vec::<LedgerRange>::new());
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert_eq!(block_numbers(&result.rows), vec![1, 2]);
}

#[test]
fn test_executor_full_cache_hit_reuses_coverage_lookup_for_durable_read() {
    let object_store = CountingObjectStore::new(LocalObjectStore::new(temp_storage_root(
        "executor-hit-coverage-reuse",
    )));
    let storage = datalens_storage::DurableStorage::from_object_store(object_store.clone());
    seed_blocks_in_storage(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    object_store.reset_counts();
    let source = MockSource::default().with_safe_height_error(DatalensErrorKind::ProviderFailure);
    let executor = executor(storage, source.clone());

    let result = executor
        .execute(blocks_input(1, 2))
        .expect("full cache hit succeeds");

    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert_eq!(block_numbers(&result.rows), vec![1, 2]);
    assert_eq!(object_store.coverage_index_get_count(), 2);
}

#[test]
fn test_executor_full_cache_hit_reuses_mixed_exact_and_semantic_coverage_plan() {
    let storage = datalens_storage::DurableStorage::from_object_store(LocalObjectStore::new(
        temp_storage_root("executor-hit-mixed-coverage-plan"),
    ));
    let query_selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let broad_selector = evm_log_selector(vec![ADDRESS_A, ADDRESS_B], vec![]);
    seed_logs_in_storage(
        &storage,
        &query_selector,
        12,
        12,
        vec![log_record(12, 0, ADDRESS_A, vec![TOPIC_1])],
    );
    seed_logs_in_storage(
        &storage,
        &broad_selector,
        13,
        13,
        vec![
            log_record(13, 0, ADDRESS_A, vec![TOPIC_1]),
            log_record(13, 1, ADDRESS_B, vec![TOPIC_2]),
        ],
    );
    let source = MockSource::default().with_safe_height_error(DatalensErrorKind::ProviderFailure);
    let executor = executor(storage, source.clone());

    let result = executor
        .execute(logs_input(query_selector, 12, 13))
        .expect("mixed exact and semantic durable hit succeeds");

    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    let logs = log_rows(&result.rows);
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].block_number, 12);
    assert_eq!(logs[0].address, ADDRESS_A);
    assert_eq!(logs[0].topics, vec![TOPIC_1.to_owned()]);
    assert_eq!(logs[1].block_number, 13);
    assert_eq!(logs[1].address, ADDRESS_A);
    assert_eq!(logs[1].topics, vec![TOPIC_1.to_owned()]);
}

#[test]
fn test_executor_miss_fetches_and_persists_through_writer() {
    let storage = LocalStorage::new(temp_storage_root("executor-miss"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let executor = executor(storage.clone(), source.clone());

    let first = executor
        .execute(blocks_input(10, 11))
        .expect("miss succeeds");
    assert_eq!(
        first.cache.promotion_pending_ranges,
        vec![LedgerRange::blocks(10, 11).unwrap()]
    );
    executor
        .wait_for_durable_promotions()
        .expect("promotion drain");
    let second = executor
        .execute(blocks_input(10, 11))
        .expect("subsequent hit succeeds");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(10, 11).unwrap()]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(10, 11).unwrap()]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 11))]
    );
    assert_eq!(block_numbers(&second.rows), vec![10, 11]);
}

#[test]
fn test_executor_new_without_durable_promotions_disables_legacy_promotion() {
    let storage = LocalStorage::new(temp_storage_root("executor-explicit-no-promotion"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let executor = NativeQueryExecutor::new_without_durable_promotions(
        storage.clone(),
        source,
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 4,
                default_chunk_range_len: 2,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
        },
    );

    let result = executor
        .execute(blocks_input(10, 10))
        .expect("query result");
    executor
        .wait_for_durable_promotions()
        .expect("promotion drain");

    assert_eq!(block_numbers(&result.rows), vec![10]);
    assert_eq!(
        result.cache.promotion_pending_ranges,
        Vec::<LedgerRange>::new()
    );
    let covered = storage
        .covered_ranges(
            &ethereum_identity(),
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(10, 10).expect("range"),
        )
        .expect("covered ranges");
    assert!(covered.is_empty());
}

#[test]
fn test_executor_miss_submits_durable_intent_when_configured() {
    let storage = LocalStorage::new(temp_storage_root("executor-intent-submit"));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);
    let intents = RecordingIntentRepository::default();
    let recorded = intents.recorded.clone();
    let executor = executor(storage, source).with_durable_intents(intents);

    let result = executor.execute(blocks_input(1, 1)).expect("query result");

    assert_eq!(result.rows.row_count(), 1);
    let recorded = recorded.lock().expect("recorded lock");
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, DurablePromotionIntentSource::Query);
    assert_eq!(
        recorded[0].ranges,
        vec![LedgerRange::blocks(1, 1).expect("range")]
    );
}

#[test]
fn test_durable_intent_startup_maintenance_does_not_block_executor_configuration() {
    let storage = LocalStorage::new(temp_storage_root("executor-intent-nonblocking-startup"));
    let source = MockSource::default();
    let started = Instant::now();

    let _executor = executor(storage, source).with_durable_intents_startup_maintenance_once(
        SlowStartupMaintenanceIntentRepository,
        Arc::new(Once::new()),
    );

    assert!(
        started.elapsed() < Duration::from_millis(250),
        "durable intent worker startup must not synchronously wait for startup maintenance"
    );
}

#[test]
fn test_durable_intent_worker_claims_while_startup_maintenance_is_blocked() {
    let root = temp_storage_root("executor-intent-worker-nonblocking-maintenance");
    let storage = LocalStorage::new(root.join("storage"));
    let inner = DurablePromotionIntentStore::new(LocalObjectStore::new(root.join("intents")));
    inner
        .create_or_get(CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Warmup,
            application: "app-a".to_owned(),
            chain: ethereum_identity(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: DatasetSelector::all().fingerprint(),
            selector_canonical_key: DatasetSelector::all().canonical_key(),
            finality: "safe".to_owned(),
            ranges: vec![LedgerRange::blocks(1, 1).expect("range")],
            request_id: None,
            task_id: Some("task-1".to_owned()),
            now_unix_seconds: 100,
        })
        .expect("create intent");
    let repository = BlockingStartupMaintenancePendingIntentRepository::new(inner);
    let maintenance_gate = repository.maintenance_gate();
    let claimed = repository.claimed();
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);

    let _executor = executor(storage, source)
        .with_durable_intents_startup_maintenance_once(repository, Arc::new(Once::new()));
    maintenance_gate.wait_until_blocked();
    let deadline = Instant::now() + Duration::from_millis(250);
    loop {
        if claimed.load(Ordering::SeqCst) > 0 {
            break;
        }
        if Instant::now() >= deadline {
            maintenance_gate.release();
            panic!("durable intent worker did not claim while startup maintenance was blocked");
        }
        thread::sleep(Duration::from_millis(10));
    }
    maintenance_gate.release();
}

#[test]
fn test_executor_miss_with_durable_intent_does_not_enqueue_legacy_promotion() {
    let storage = LocalStorage::new(temp_storage_root("executor-intent-no-legacy-queue"));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);
    let intents = RecordingIntentRepository::default();
    let executor = executor(storage.clone(), source).with_durable_intents(intents);

    let result = executor.execute(blocks_input(1, 1)).expect("query result");
    executor
        .wait_for_durable_promotions()
        .expect("legacy promotion queue drains");

    assert_eq!(block_numbers(&result.rows), vec![1]);
    let covered = storage
        .covered_ranges(
            &ethereum_identity(),
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(1, 1).expect("range"),
        )
        .expect("covered ranges");
    assert!(covered.is_empty());
}

#[test]
fn test_executor_miss_with_completed_durable_intent_reports_no_pending_promotion() {
    let storage = LocalStorage::new(temp_storage_root("executor-intent-completed"));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);
    let executor =
        executor(storage.clone(), source).with_durable_intents(CompletedIntentRepository);

    let result = executor.execute(blocks_input(1, 1)).expect("query result");
    executor
        .wait_for_durable_promotions()
        .expect("legacy promotion queue drains");

    assert_eq!(block_numbers(&result.rows), vec![1]);
    assert_eq!(
        result.cache.promotion_pending_ranges,
        Vec::<LedgerRange>::new()
    );
    let covered = storage
        .covered_ranges(
            &ethereum_identity(),
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(1, 1).expect("range"),
        )
        .expect("covered ranges");
    assert!(covered.is_empty());
}

#[test]
fn test_executor_miss_with_durable_intent_records_staged_write_metric() {
    let storage = LocalStorage::new(temp_storage_root("executor-intent-staged-metric"));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let executor = executor(storage, source)
        .with_metrics(recorder.clone(), ApplicationIdentity::named("api"))
        .with_durable_intents(RecordingIntentRepository::default());

    executor.execute(blocks_input(1, 1)).expect("query result");

    let output = recorder.encode().expect("prometheus text");
    assert!(output.contains(
        r#"datalens_durable_write_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="staged"} 1"#
    ));
}

#[test]
fn test_executor_durable_intent_empty_fill_does_not_refetch_provider() {
    let root = temp_storage_root("executor-intent-empty-no-refetch");
    let storage = LocalStorage::new(root.join("storage"));
    let intents = DurablePromotionIntentStore::new(LocalObjectStore::new(root.join("intents")));
    let source = MockSource::default();
    let executor = executor(storage.clone(), source.clone()).with_durable_intents(intents.clone());

    let result = executor.execute(blocks_input(1, 1)).expect("query result");

    assert_eq!(result.rows.row_count(), 0);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if source.calls().len() > 1 {
            break;
        }
        let pending = intents.list_pending(u64::MAX, 10).expect("list pending");
        if pending.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("durable intent worker did not finish empty coverage intent");
        }
        thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(1, 1))]
    );
    assert_eq!(
        storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(1, 1).expect("range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(1, 1).expect("range")]
    );
}

#[test]
fn test_executor_miss_returns_rows_when_durable_intent_scheduling_fails() {
    let storage = LocalStorage::new(temp_storage_root("executor-intent-fail"));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);
    let executor = executor(storage, source).with_durable_intents(FailingIntentRepository);

    let result = executor.execute(blocks_input(1, 1)).expect("query result");

    assert_eq!(result.rows.row_count(), 1);
}

#[test]
fn test_durable_intent_worker_replays_pending_intent_into_coverage() {
    let root = temp_storage_root("executor-intent-worker");
    let storage = LocalStorage::new(root.join("storage"));
    let intents = DurablePromotionIntentStore::new(LocalObjectStore::new(root.join("intents")));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);
    intents
        .create_or_get(CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Warmup,
            application: "app-a".to_owned(),
            chain: ethereum_identity(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: DatasetSelector::all().fingerprint(),
            selector_canonical_key: DatasetSelector::all().canonical_key(),
            finality: "safe".to_owned(),
            ranges: vec![LedgerRange::blocks(1, 1).expect("range")],
            request_id: None,
            task_id: Some("task-1".to_owned()),
            now_unix_seconds: 100,
        })
        .expect("create intent");

    let _executor = executor(storage.clone(), source).with_durable_intents(intents.clone());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let covered = storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(1, 1).expect("range"),
            )
            .expect("covered ranges");
        if covered == vec![LedgerRange::blocks(1, 1).expect("range")] {
            break;
        }
        if Instant::now() >= deadline {
            panic!("durable intent worker did not publish coverage");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let pending = intents.list_pending(u64::MAX, 10).expect("list pending");
    assert!(pending.is_empty());
}

#[test]
fn test_durable_intent_worker_splits_provider_limit_ranges_into_coverage() {
    let root = temp_storage_root("executor-intent-worker-provider-limit");
    let storage = LocalStorage::new(root.join("storage"));
    let intents = DurablePromotionIntentStore::new(LocalObjectStore::new(root.join("intents")));
    let source = MockSource::default()
        .with_blocks(vec![
            block(1, "0x01"),
            block(2, "0x02"),
            block(3, "0x03"),
            block(4, "0x04"),
        ])
        .with_provider_limit_for_ranges_longer_than(1);
    intents
        .create_or_get(CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Warmup,
            application: "app-a".to_owned(),
            chain: ethereum_identity(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: DatasetSelector::all().fingerprint(),
            selector_canonical_key: DatasetSelector::all().canonical_key(),
            finality: "safe".to_owned(),
            ranges: vec![LedgerRange::blocks(1, 4).expect("range")],
            request_id: None,
            task_id: Some("task-1".to_owned()),
            now_unix_seconds: 100,
        })
        .expect("create intent");

    let _executor = executor(storage.clone(), source.clone()).with_durable_intents(intents.clone());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let covered = storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(1, 4).expect("range"),
            )
            .expect("covered ranges");
        if covered == vec![LedgerRange::blocks(1, 4).expect("range")] {
            break;
        }
        if Instant::now() >= deadline {
            panic!("durable intent worker did not publish split coverage");
        }
        thread::sleep(Duration::from_millis(10));
    }

    assert!(
        source
            .calls()
            .contains(&SourceCall::Blocks(BlockRange::expect_new(1, 1)))
    );
    assert!(
        source
            .calls()
            .contains(&SourceCall::Blocks(BlockRange::expect_new(4, 4)))
    );
    let pending = intents.list_pending(u64::MAX, 10).expect("list pending");
    assert!(pending.is_empty());
}

#[test]
fn test_executor_miss_returns_before_slow_durable_promotion_completes() {
    let root = temp_storage_root("executor-async-promotion");
    let storage = BlockingWriteStorage::new(LocalStorage::new(&root));
    let write_gate = storage.write_gate();
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let executor = executor(storage.clone(), source);
    let (sender, receiver) = mpsc::channel();
    let query_executor = executor.clone();

    let query = thread::spawn(move || {
        sender
            .send(query_executor.execute(blocks_input(10, 10)))
            .expect("send query result");
    });

    write_gate.wait_until_blocked();
    let result = match receiver.recv_timeout(Duration::from_millis(150)) {
        Ok(result) => result,
        Err(error) => {
            write_gate.release();
            query.join().expect("query thread after timeout");
            panic!("query response waited for durable promotion: {error}");
        }
    };
    let result = result.expect("query succeeds while promotion is blocked");
    assert_eq!(block_numbers(&result.rows), vec![10]);
    assert_eq!(
        result.cache.promotion_pending_ranges,
        vec![LedgerRange::blocks(10, 10).expect("valid range")]
    );
    assert!(
        storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(10, 10).expect("valid range"),
            )
            .expect("covered ranges before release")
            .is_empty()
    );

    write_gate.release();
    query.join().expect("query thread");
    executor
        .wait_for_durable_promotions()
        .expect("promotion drain");
    assert_eq!(
        storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(10, 10).expect("valid range"),
            )
            .expect("covered ranges after release"),
        vec![LedgerRange::blocks(10, 10).expect("valid range")]
    );
}

#[test]
fn test_executor_identical_concurrent_misses_share_provider_fetch_and_promote_once() {
    let storage = LocalStorage::new(temp_storage_root("executor-provider-singleflight"));
    let source = MockSource::default()
        .with_blocks(vec![block(20, "0x20")])
        .with_fetch_delay(Duration::from_millis(200));
    let executor = executor(storage.clone(), source.clone());
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();

    for _ in 0..2 {
        let query_executor = executor.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            query_executor
                .execute(blocks_input(20, 20))
                .expect("query succeeds")
        }));
    }

    barrier.wait();
    let first = handles.remove(0).join().expect("first query");
    let second = handles.remove(0).join().expect("second query");

    assert_eq!(block_numbers(&first.rows), vec![20]);
    assert_eq!(block_numbers(&second.rows), vec![20]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(20, 20))]
    );
    assert_eq!(
        first.cache.promotion_pending_ranges,
        vec![LedgerRange::blocks(20, 20).expect("valid range")]
    );
    assert_eq!(
        second.cache.promotion_pending_ranges,
        vec![LedgerRange::blocks(20, 20).expect("valid range")]
    );

    executor
        .wait_for_durable_promotions()
        .expect("promotion drain");
    let hit = executor
        .execute(blocks_input(20, 20))
        .expect("subsequent hit succeeds");
    assert_eq!(
        hit.cache.hit_ranges,
        vec![LedgerRange::blocks(20, 20).expect("valid range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(20, 20))]
    );
}

#[test]
fn test_executor_duplicate_retry_does_not_queue_duplicate_durable_promotion() {
    let storage = BlockingWriteStorage::new(LocalStorage::new(temp_storage_root(
        "executor-promotion-singleflight",
    )));
    let write_gate = storage.write_gate();
    let source = MockSource::default().with_blocks(vec![block(30, "0x30")]);
    let executor = executor(storage.clone(), source);
    let (sender, receiver) = mpsc::channel();
    let first_executor = executor.clone();

    let first_query = thread::spawn(move || {
        sender
            .send(first_executor.execute(blocks_input(30, 30)))
            .expect("send first query");
    });
    write_gate.wait_until_blocked();
    let first = match receiver.recv_timeout(Duration::from_millis(150)) {
        Ok(result) => result.expect("first miss succeeds"),
        Err(error) => {
            write_gate.release();
            first_query.join().expect("first query after timeout");
            panic!("first query waited for durable promotion: {error}");
        }
    };
    let second = executor
        .execute(blocks_input(30, 30))
        .expect("retry succeeds while promotion is in flight");

    assert_eq!(
        first.cache.promotion_pending_ranges,
        vec![LedgerRange::blocks(30, 30).expect("valid range")]
    );
    assert_eq!(
        second.cache.promotion_pending_ranges,
        vec![LedgerRange::blocks(30, 30).expect("valid range")]
    );
    assert_eq!(storage.write_attempts(), 1);

    write_gate.release();
    first_query.join().expect("first query");
    executor
        .wait_for_durable_promotions()
        .expect("promotion drain");
    assert_eq!(storage.write_attempts(), 1);
}

#[test]
fn test_executor_splits_provider_limit_ranges_without_changing_logical_query_range() {
    let storage = LocalStorage::new(temp_storage_root("executor-provider-limit-split"));
    let source = MockSource::default()
        .with_blocks(vec![
            block(10, "0x10"),
            block(11, "0x11"),
            block(12, "0x12"),
            block(13, "0x13"),
        ])
        .with_capability_max_range_len(4)
        .with_provider_limit_for_ranges_longer_than(2);
    let executor = NativeQueryExecutor::new(
        storage,
        source.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 4,
                default_chunk_range_len: 4,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
        },
    );

    let result = executor
        .execute(blocks_input(10, 13))
        .expect("provider limit split succeeds");

    assert_eq!(result.ledger_range, LedgerRange::blocks(10, 13).unwrap());
    assert_eq!(
        result.cache.missing_ranges,
        vec![LedgerRange::blocks(10, 13).unwrap()]
    );
    assert_eq!(
        result.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(10, 13).unwrap()]
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(10, 13)),
            SourceCall::Blocks(BlockRange::expect_new(10, 11)),
            SourceCall::Blocks(BlockRange::expect_new(12, 13)),
        ]
    );
    assert_eq!(block_numbers(&result.rows), vec![10, 11, 12, 13]);
}

#[test]
fn test_executor_uses_provider_limit_hint_instead_of_repeated_halving() {
    let storage = LocalStorage::new(temp_storage_root("executor-provider-limit-hint"));
    let source = MockSource::default()
        .with_safe_height(5_000)
        .with_capability_max_range_len(5_000)
        .with_provider_limit_for_ranges_longer_than(1_000)
        .with_provider_limit_message(
            "query block range exceeds server limit, narrow your filter: 1000",
        );
    let executor = NativeQueryExecutor::new(
        storage,
        source.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 5_000,
                default_chunk_range_len: 5_000,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
        },
    );

    executor
        .execute(blocks_input(1, 5_000))
        .expect("provider hint split succeeds");

    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 5_000)),
            SourceCall::Blocks(BlockRange::expect_new(1, 1_000)),
            SourceCall::Blocks(BlockRange::expect_new(1_001, 2_000)),
            SourceCall::Blocks(BlockRange::expect_new(2_001, 3_000)),
            SourceCall::Blocks(BlockRange::expect_new(3_001, 4_000)),
            SourceCall::Blocks(BlockRange::expect_new(4_001, 5_000)),
        ]
    );
    assert!(
        !source.calls().iter().any(|call| matches!(
            call,
            SourceCall::Blocks(range)
                if range.from_block == 1 && (range.to_block == 2_500 || range.to_block == 1_250)
        )),
        "provider limit hint should avoid 2500/1250 retry ranges"
    );
}

#[test]
fn test_executor_reuses_provider_limit_hint_on_repeated_latest_query() {
    let storage = LocalStorage::new(temp_storage_root("executor-provider-limit-hint-reuse"));
    let source = MockSource::default()
        .with_latest_height(5_000)
        .with_safe_height(5_000)
        .with_capability_max_range_len(5_000)
        .with_provider_limit_for_ranges_longer_than(1_000)
        .with_provider_limit_message(
            "query block range exceeds server limit, narrow your filter: 1000",
        );
    let executor = NativeQueryExecutor::new(
        storage,
        source.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 5_000,
                default_chunk_range_len: 5_000,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
        },
    );
    let mut input = blocks_input(1, 5_000);
    input.finality = QueryFinalityRequirement::LatestOnly;

    executor
        .execute(input.clone())
        .expect("first provider hint split succeeds");
    source.clear_calls();
    executor
        .execute(input)
        .expect("second provider hint split succeeds");

    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 1_000)),
            SourceCall::Blocks(BlockRange::expect_new(1_001, 2_000)),
            SourceCall::Blocks(BlockRange::expect_new(2_001, 3_000)),
            SourceCall::Blocks(BlockRange::expect_new(3_001, 4_000)),
            SourceCall::Blocks(BlockRange::expect_new(4_001, 5_000)),
        ]
    );
}

#[test]
fn test_executor_pre_splits_by_capability_before_provider_fetch() {
    let storage = LocalStorage::new(temp_storage_root("executor-provider-capability-presplit"));
    let source = MockSource::default()
        .with_safe_height(5_000)
        .with_capability_max_range_len(1_000)
        .with_provider_limit_for_ranges_longer_than(1_000);
    let executor = NativeQueryExecutor::new(
        storage,
        source.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 5_000,
                default_chunk_range_len: 5_000,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
        },
    );

    executor
        .execute(blocks_input(1, 5_000))
        .expect("provider capability pre-split succeeds");

    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 1_000)),
            SourceCall::Blocks(BlockRange::expect_new(1_001, 2_000)),
            SourceCall::Blocks(BlockRange::expect_new(2_001, 3_000)),
            SourceCall::Blocks(BlockRange::expect_new(3_001, 4_000)),
            SourceCall::Blocks(BlockRange::expect_new(4_001, 5_000)),
        ]
    );
}

#[test]
fn test_executor_passes_query_id_to_provider_fetch_context() {
    let storage = LocalStorage::new(temp_storage_root("executor-query-id"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let executor = executor(storage, source.clone());

    executor
        .execute_with_application_and_query_id(blocks_input(10, 11), None, "q-test".to_owned())
        .expect("miss succeeds");

    assert_eq!(source.request_ids(), vec![Some("q-test".to_owned())]);
}

#[test]
fn test_executor_passes_query_id_to_hot_provider_fetch_context() {
    let storage = LocalStorage::new(temp_storage_root("executor-hot-query-id"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let executor = executor(storage, source.clone());
    let mut input = blocks_input(10, 10);
    input.finality = QueryFinalityRequirement::LatestOnly;

    executor
        .execute_with_application_and_query_id(input, None, "q-hot".to_owned())
        .expect("hot query succeeds");

    assert_eq!(source.request_ids(), vec![Some("q-hot".to_owned())]);
}

#[test]
fn test_executor_repeated_query_hits_mixed_empty_and_data_coverage_without_fetch() {
    let storage = LocalStorage::new(temp_storage_root("executor-hit-mixed-empty-data"));
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    seed_empty_blocks(&storage, 3, 4);
    let source = MockSource::default()
        .with_blocks(vec![
            block(1, "0x01"),
            block(2, "0x02"),
            block(3, "0x03"),
            block(4, "0x04"),
        ])
        .with_safe_height_error(DatalensErrorKind::ProviderFailure);
    let executor = executor(storage, source.clone());

    let result = executor
        .execute(blocks_input(1, 4))
        .expect("mixed empty/data cache hit succeeds");

    assert_eq!(block_numbers(&result.rows), vec![1, 2]);
    assert_eq!(
        result.cache.hit_ranges,
        vec![LedgerRange::blocks(1, 4).expect("valid range")]
    );
    assert_eq!(result.cache.missing_ranges, Vec::<LedgerRange>::new());
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
}

#[test]
fn test_executor_returns_staged_query_fill_without_forcing_manifest_flush() {
    let storage = LocalStorage::new(temp_storage_root("executor-staged-miss"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let executor = NativeQueryExecutor::new(
        storage.clone(),
        source.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 4,
                default_chunk_range_len: 2,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024 * 1024,
                min_object_rows: 3,
                record_empty_coverage: true,
                staging: WriteStagingConfig {
                    enabled: true,
                    ..Default::default()
                },
            },
        },
    );

    let first = executor
        .execute(blocks_input(10, 10))
        .expect("query miss returns provider rows");
    assert_eq!(block_numbers(&first.rows), vec![10]);
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 0);
    executor
        .wait_for_durable_promotions()
        .expect("promotion drain");

    let second = executor
        .execute(blocks_input(10, 10))
        .expect("same-process query reads staged rows");

    assert_eq!(block_numbers(&second.rows), vec![10]);
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(10, 10).expect("valid range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
}

#[test]
fn test_executor_usage_ledger_records_query_staging_without_forced_flush() {
    let root = temp_storage_root("executor-ledger-staged");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let executor = NativeQueryExecutor::new(
        storage,
        source,
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 4,
                default_chunk_range_len: 2,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024 * 1024,
                min_object_rows: 3,
                record_empty_coverage: true,
                staging: WriteStagingConfig {
                    enabled: true,
                    ..Default::default()
                },
            },
        },
    )
    .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("analytics-api"));

    executor
        .execute(blocks_input(10, 10))
        .expect("query miss succeeds");

    let events = wait_for_ledger_events(&ledger, "analytics-api", 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].fill_outcome, FillOutcome::LiveFetch);
    assert_eq!(
        events[0].durable_write_outcome,
        datalens_storage::DurableWriteOutcome::Staged
    );
    assert_eq!(events[0].row_count, 1);
}

#[test]
fn test_executor_writes_usage_ledger_for_cache_hit() {
    let root = temp_storage_root("executor-ledger-hit");
    let storage = LocalStorage::new(&root);
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default();
    let executor = executor(storage, source)
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("analytics-api"));

    executor
        .execute(blocks_input(1, 2))
        .expect("cache hit succeeds");

    let events = wait_for_ledger_events(&ledger, "analytics-api", 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].application_id, "analytics-api");
    assert_eq!(events[0].chain, ethereum_identity());
    assert_eq!(events[0].dataset_key, DatasetKey::evm_blocks());
    assert_eq!(events[0].range, LedgerRange::blocks(1, 2).unwrap());
    assert_eq!(events[0].query_outcome, QueryOutcome::Hit);
    assert_eq!(events[0].cache_outcome, CacheOutcome::Hit);
    assert_eq!(events[0].fill_outcome, FillOutcome::NotAttempted);
    assert_eq!(events[0].row_count, 2);
}

#[test]
fn test_executor_writes_usage_ledger_for_miss_fill_and_empty_coverage() {
    let root = temp_storage_root("executor-ledger-fill-empty");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let executor = executor(storage, source)
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("analytics-api"));

    executor
        .execute(blocks_input(10, 11))
        .expect("miss fill succeeds");

    let events = wait_for_ledger_events(&ledger, "analytics-api", 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].query_outcome, QueryOutcome::Filled);
    assert_eq!(events[0].cache_outcome, CacheOutcome::Miss);
    assert_eq!(events[0].fill_outcome, FillOutcome::LiveFetch);
    assert_eq!(
        events[0].durable_write_outcome,
        datalens_storage::DurableWriteOutcome::Staged
    );
    assert_eq!(events[0].row_count, 1);
}

#[test]
fn test_executor_records_query_watermark_after_successful_durable_query() {
    let root = temp_storage_root("executor-query-watermark");
    let storage = LocalStorage::new(&root);
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default().with_blocks(vec![block(30, "0x30")]);
    let executor = executor(storage, source).with_query_watermarks(
        watermarks.clone(),
        ApplicationIdentity::named("analytics-api"),
    );

    executor
        .execute(blocks_input(30, 32))
        .expect("durable query succeeds");

    let key = QueryWatermarkKey::new(
        "analytics-api",
        ethereum_identity(),
        DatasetKey::evm_blocks(),
        &DatasetSelector::all(),
        datalens_core::LedgerRangeKind::Block,
    );
    let watermark = wait_for_query_watermark(&watermarks, &key);
    assert_eq!(watermark.latest_block, 32);
    assert_eq!(
        watermark.key.selector_fingerprint,
        DatasetSelector::all().fingerprint()
    );
    assert_eq!(
        watermark.key.selector_canonical_key,
        DatasetSelector::all().canonical_key()
    );
}

#[test]
fn test_executor_records_query_activity_after_successful_durable_query() {
    let root = temp_storage_root("executor-query-activity");
    let storage = LocalStorage::new(&root);
    let activities = QueryActivityStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default().with_blocks(vec![block(30, "0x30")]);
    let executor = executor(storage, source).with_query_activity(
        activities.clone(),
        ApplicationIdentity::named("analytics-api"),
    );

    executor
        .execute(blocks_input(30, 32))
        .expect("durable query succeeds");

    let key = QueryActivityKey::new(
        "analytics-api",
        ethereum_identity(),
        DatasetKey::evm_blocks(),
        &DatasetSelector::all(),
        datalens_core::LedgerRangeKind::Block,
    );
    let activity = wait_for_query_activity(&activities, &key);
    assert_eq!(activity.latest_range, LedgerRange::blocks(30, 32).unwrap());
    assert_eq!(
        activity.follow_query_range,
        Some(LedgerRange::blocks(30, 32).unwrap())
    );
    assert!(activity.follow_query_updated_at_unix_seconds.is_some());
    assert!(activity.request_id.is_some());
    assert_eq!(
        activity.key.selector_canonical_key,
        DatasetSelector::all().canonical_key()
    );
}

#[test]
fn test_executor_retains_low_follow_query_activity_after_near_head_probe() {
    let root = temp_storage_root("executor-query-activity-retains-low-follow");
    let storage = LocalStorage::new(&root);
    let activities = QueryActivityStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default()
        .with_safe_height(83_612_498)
        .with_capability_max_range_len(100)
        .with_blocks(vec![
            block(5_855_859, "0x5961f3"),
            block(83_612_407, "0x4fc1977"),
        ]);
    let executor = executor(storage, source).with_query_activity(
        activities.clone(),
        ApplicationIdentity::named("analytics-api"),
    );
    let key = QueryActivityKey::new(
        "analytics-api",
        ethereum_identity(),
        DatasetKey::evm_blocks(),
        &DatasetSelector::all(),
        datalens_core::LedgerRangeKind::Block,
    );

    executor
        .execute(blocks_input(5_855_859, 5_855_859))
        .expect("backfill durable query succeeds");
    let low_activity = wait_for_query_activity_end(&activities, &key, 5_855_859);
    let low_follow_updated_at = low_activity
        .follow_query_updated_at_unix_seconds
        .expect("low follow timestamp");

    executor
        .execute(blocks_input(83_612_407, 83_612_407))
        .expect("near-head durable query succeeds");

    let activity = wait_for_query_activity_end(&activities, &key, 83_612_407);
    assert_eq!(
        activity.latest_range,
        LedgerRange::blocks(83_612_407, 83_612_407).unwrap()
    );
    assert_eq!(
        activity.follow_query_range,
        Some(LedgerRange::blocks(5_855_859, 5_855_859).unwrap())
    );
    assert_eq!(
        activity.follow_query_updated_at_unix_seconds,
        Some(low_follow_updated_at)
    );
}

#[test]
fn test_executor_latest_only_fetches_latest_without_durable_read_or_write() {
    let root = temp_storage_root("executor-hot-read-through");
    let storage = LocalStorage::new(&root);
    seed_blocks(&storage, 99, 99, vec![block(99, "0x63")]);
    let counting_storage = CountingStorage::new(storage);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default().with_blocks(vec![block(100, "0x64")]);
    let executor = executor(counting_storage.clone(), source.clone())
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("analytics-api"));
    let mut input = blocks_input(99, 100);
    input.finality = QueryFinalityRequirement::LatestOnly;

    let result = executor.execute(input).expect("hot read-through succeeds");

    assert_eq!(block_numbers(&result.rows), vec![100]);
    assert_eq!(
        result.cache.missing_ranges,
        vec![LedgerRange::blocks(99, 100).expect("valid range")]
    );
    assert_eq!(result.cache.durable_hit_ranges, Vec::<LedgerRange>::new());
    assert_eq!(result.cache.hot_hit_ranges, Vec::<LedgerRange>::new());
    assert_eq!(
        result.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(99, 100).expect("valid range")]
    );
    assert_eq!(
        result.cache.promotion_pending_ranges,
        Vec::<LedgerRange>::new()
    );
    assert_eq!(result.cache.segments.len(), 1);
    assert_eq!(
        result.cache.segments[0].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(result.cache.segments[0].finality, QueryDataFinality::Latest);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(99, 100))]
    );
    assert_eq!(counting_storage.read_ranges(), Vec::<LedgerRange>::new());
    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );

    let events = wait_for_ledger_events(&ledger, "analytics-api", 1);
    assert_eq!(events.len(), 1);
    assert!(events[0].requested_hot);
    assert_eq!(events[0].query_outcome, QueryOutcome::HotMiss);
    assert_eq!(events[0].cache_outcome, CacheOutcome::HotMiss);
    assert_eq!(events[0].fill_outcome, FillOutcome::LiveFetch);
    assert_eq!(
        events[0].durable_write_outcome,
        datalens_storage::DurableWriteOutcome::NotAttempted
    );
}

#[test]
fn test_executor_safe_to_latest_reads_durable_cache_and_fetches_hot_tail() {
    let root = temp_storage_root("executor-safe-to-latest-mixed");
    let storage = LocalStorage::new(&root);
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default()
        .with_safe_height(3)
        .with_latest_height(4)
        .with_blocks(vec![block(3, "0x03"), block(4, "0x04")]);
    let executor = executor(storage.clone(), source.clone())
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("analytics-api"));
    let mut input = blocks_input(1, 4);
    input.finality = QueryFinalityRequirement::SafeToLatest;

    let result = executor.execute(input).expect("mixed query succeeds");

    assert_eq!(block_numbers(&result.rows), vec![1, 2, 3, 4]);
    assert_eq!(
        result.cache.durable_hit_ranges,
        vec![LedgerRange::blocks(1, 2).expect("valid range")]
    );
    assert_eq!(
        result.cache.missing_ranges,
        vec![
            LedgerRange::blocks(3, 3).expect("valid range"),
            LedgerRange::blocks(4, 4).expect("valid range"),
        ]
    );
    assert_eq!(
        result.cache.provider_fill_ranges,
        vec![
            LedgerRange::blocks(3, 3).expect("valid range"),
            LedgerRange::blocks(4, 4).expect("valid range"),
        ]
    );
    assert_eq!(result.cache.segments.len(), 3);
    assert_eq!(result.cache.segments[0].source, QuerySegmentSource::Durable);
    assert_eq!(result.cache.segments[0].finality, QueryDataFinality::Safe);
    assert_eq!(
        result.cache.segments[1].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(result.cache.segments[1].finality, QueryDataFinality::Safe);
    assert_eq!(
        result.cache.segments[2].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(result.cache.segments[2].finality, QueryDataFinality::Latest);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(3, 3)),
            SourceCall::Blocks(BlockRange::expect_new(4, 4)),
        ]
    );
    let events = wait_for_ledger_events(&ledger, "analytics-api", 1);
    assert_eq!(events[0].query_outcome, QueryOutcome::Mixed);
    assert_eq!(events[0].cache_outcome, CacheOutcome::Mixed);
    assert_eq!(events[0].fill_outcome, FillOutcome::LiveFetch);

    let second = executor
        .execute(blocks_input(3, 3))
        .expect("safe gap was durable-written");
    assert_eq!(block_numbers(&second.rows), vec![3]);
    assert_eq!(
        storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(4, 4).expect("valid range"),
            )
            .expect("covered ranges"),
        Vec::<LedgerRange>::new()
    );
}

#[test]
fn test_executor_records_separate_usage_for_shared_durable_cache() {
    let root = temp_storage_root("executor-ledger-shared-cache");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default().with_blocks(vec![block(20, "0x20")]);

    let app_a_executor = executor(storage.clone(), source.clone())
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("app-a"));
    app_a_executor
        .execute(blocks_input(20, 20))
        .expect("first application fills cache");
    app_a_executor
        .wait_for_durable_promotions()
        .expect("promotion drain");
    executor(storage.clone(), source.clone())
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("app-b"))
        .execute(blocks_input(20, 20))
        .expect("second application hits shared cache");

    let app_a_events = wait_for_ledger_events(&ledger, "app-a", 1);
    let app_b_events = wait_for_ledger_events(&ledger, "app-b", 1);
    assert_eq!(app_a_events[0].fill_outcome, FillOutcome::LiveFetch);
    assert_eq!(
        app_a_events[0].durable_write_outcome,
        datalens_storage::DurableWriteOutcome::Staged
    );
    assert_eq!(app_b_events[0].cache_outcome, CacheOutcome::Hit);
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 1);
}

#[test]
fn test_executor_returns_provider_rows_when_durable_write_fails_without_coverage() {
    let root = temp_storage_root("executor-write-failure-provider-rows");
    let storage = FailingWriteStorage::new(LocalStorage::new(&root));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let executor = executor(storage, source.clone());

    let result = executor
        .execute(blocks_input(10, 10))
        .expect("provider rows are returned despite durable write failure");

    assert_eq!(block_numbers(&result.rows), vec![10]);
    assert_eq!(
        result.cache.missing_ranges,
        vec![LedgerRange::blocks(10, 10).expect("valid range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_executor_ledger_write_failure_does_not_block_successful_query() {
    let storage = LocalStorage::new(temp_storage_root("executor-ledger-failure"));
    seed_blocks(&storage, 1, 1, vec![block(1, "0x01")]);
    let source = MockSource::default();
    let ledger = FailingUsageLedgerRepository::default();
    let attempts = ledger.attempts();
    let executor =
        executor(storage, source).with_usage_ledger(ledger, ApplicationIdentity::named("api"));

    let result = executor
        .execute(blocks_input(1, 1))
        .expect("ledger failure does not block query");

    assert_eq!(block_numbers(&result.rows), vec![1]);
    wait_for_attempt(&attempts);
}

#[test]
fn test_executor_query_watermark_write_failure_does_not_block_successful_query() {
    let storage = LocalStorage::new(temp_storage_root("executor-watermark-failure"));
    let source = MockSource::default().with_blocks(vec![block(30, "0x30")]);
    let watermarks = FailingQueryWatermarkRepository::default();
    let attempts = watermarks.attempts();
    let executor = executor(storage, source)
        .with_query_watermarks(watermarks, ApplicationIdentity::named("api"));

    let result = executor
        .execute(blocks_input(30, 32))
        .expect("watermark failure does not block query");

    assert_eq!(block_numbers(&result.rows), vec![30]);
    wait_for_attempt(&attempts);
}

#[test]
fn test_executor_query_activity_write_failure_does_not_block_successful_query() {
    let storage = LocalStorage::new(temp_storage_root("executor-activity-failure"));
    let source = MockSource::default().with_blocks(vec![block(30, "0x30")]);
    let activities = FailingQueryActivityRepository::default();
    let attempts = activities.attempts();
    let executor = executor(storage, source)
        .with_query_activity(activities, ApplicationIdentity::named("api"));

    let result = executor
        .execute(blocks_input(30, 32))
        .expect("activity failure does not block query");

    assert_eq!(block_numbers(&result.rows), vec![30]);
    wait_for_attempt(&attempts);
}

#[test]
fn test_executor_slow_usage_ledger_write_does_not_delay_successful_query() {
    let storage = LocalStorage::new(temp_storage_root("executor-slow-ledger"));
    seed_blocks(&storage, 1, 1, vec![block(1, "0x01")]);
    let source = MockSource::default();
    let ledger = SlowUsageLedgerRepository::new(Duration::from_millis(500));
    let attempts = ledger.attempts();
    let executor =
        executor(storage, source).with_usage_ledger(ledger, ApplicationIdentity::named("api"));

    let start = Instant::now();
    let result = executor
        .execute(blocks_input(1, 1))
        .expect("slow ledger write does not block query");

    assert_eq!(block_numbers(&result.rows), vec![1]);
    assert!(
        start.elapsed() < Duration::from_millis(250),
        "query response waited for slow metadata write"
    );
    wait_for_attempt(&attempts);
}

#[test]
fn test_executor_slow_query_activity_read_does_not_delay_successful_query() {
    let storage = LocalStorage::new(temp_storage_root("executor-slow-activity-read"));
    let source = MockSource::default().with_blocks(vec![block(30, "0x30")]);
    let activities = SlowReadQueryActivityRepository::new(Duration::from_millis(500));
    let attempts = activities.read_attempts();
    let executor = executor(storage, source)
        .with_query_activity(activities, ApplicationIdentity::named("api"));

    let start = Instant::now();
    let result = executor
        .execute(blocks_input(30, 32))
        .expect("slow activity read does not block query");

    assert_eq!(block_numbers(&result.rows), vec![30]);
    assert!(
        start.elapsed() < Duration::from_millis(250),
        "query response waited for query activity read"
    );
    wait_for_attempt(&attempts);
}

#[test]
fn test_executor_miss_requires_provider_safe_height_before_fetch_or_write() {
    let root = temp_storage_root("executor-miss-provider-down");
    let storage = LocalStorage::new(&root);
    let source = MockSource::default()
        .with_blocks(vec![block(1, "0x01")])
        .with_safe_height_error(DatalensErrorKind::ProviderFailure);
    let executor = executor(storage, source.clone());

    let error = executor
        .execute(blocks_input(1, 1))
        .expect_err("miss requires provider safe height");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_executor_partial_hit_fetches_only_missing_range() {
    let storage = LocalStorage::new(temp_storage_root("executor-partial"));
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let source = MockSource::default().with_blocks(vec![
        block(1, "0x01"),
        block(2, "0x02"),
        block(3, "0x03"),
        block(4, "0x04"),
    ]);
    let executor = executor(storage, source.clone());

    let result = executor
        .execute(blocks_input(1, 4))
        .expect("partial hit succeeds");

    assert_eq!(
        result.cache.hit_ranges,
        vec![LedgerRange::blocks(1, 2).unwrap()]
    );
    assert_eq!(
        result.cache.missing_ranges,
        vec![LedgerRange::blocks(3, 4).unwrap()]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(3, 4))]
    );
    assert_eq!(block_numbers(&result.rows), vec![1, 2, 3, 4]);
}

#[test]
fn test_executor_partial_hit_requires_provider_safe_height_before_fetch_or_write() {
    let root = temp_storage_root("executor-partial-provider-down");
    let storage = LocalStorage::new(&root);
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let source = MockSource::default()
        .with_blocks(vec![block(3, "0x03"), block(4, "0x04")])
        .with_safe_height_error(DatalensErrorKind::ProviderFailure);
    let executor = executor(storage, source.clone());

    let error = executor
        .execute(blocks_input(1, 4))
        .expect_err("partial hit requires provider safe height");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(
        !root
            .join("chains/evm/ethereum/1/datasets/evm-blocks/all/block/3-4.parquet")
            .exists()
    );
}

#[test]
fn test_executor_full_miss_does_not_read_cache_rows() {
    let storage = CountingStorage::new(LocalStorage::new(temp_storage_root(
        "executor-miss-no-read",
    )));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01"), block(2, "0x02")]);
    let executor = executor(storage.clone(), source);

    let result = executor.execute(blocks_input(1, 2)).expect("miss succeeds");

    assert_eq!(
        result.cache.missing_ranges,
        vec![LedgerRange::blocks(1, 2).unwrap()]
    );
    assert_eq!(storage.read_ranges(), Vec::<LedgerRange>::new());
}

#[test]
fn test_executor_partial_hit_reads_only_planned_read_segments() {
    let inner = LocalStorage::new(temp_storage_root("executor-partial-read-segments"));
    seed_blocks(&inner, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let storage = CountingStorage::new(inner);
    let source = MockSource::default().with_blocks(vec![
        block(1, "0x01"),
        block(2, "0x02"),
        block(3, "0x03"),
        block(4, "0x04"),
    ]);
    let executor = executor(storage.clone(), source);

    let result = executor
        .execute(blocks_input(1, 4))
        .expect("partial hit succeeds");

    assert_eq!(block_numbers(&result.rows), vec![1, 2, 3, 4]);
    assert_eq!(
        storage.read_ranges(),
        vec![LedgerRange::blocks(1, 2).unwrap()]
    );
}

#[test]
fn test_executor_records_metrics_for_cache_hit_and_fill_paths() {
    let storage = LocalStorage::new(temp_storage_root("executor-metrics-hit-fill"));
    seed_blocks(&storage, 1, 2, vec![block(1, "0x01"), block(2, "0x02")]);
    let source = MockSource::default().with_blocks(vec![
        block(1, "0x01"),
        block(2, "0x02"),
        block(3, "0x03"),
        block(4, "0x04"),
    ]);
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let executor = executor(storage, source.clone())
        .with_metrics(recorder.clone(), ApplicationIdentity::named("api"));

    executor
        .execute(blocks_input(1, 2))
        .expect("cache hit succeeds");
    executor
        .execute(blocks_input(1, 4))
        .expect("partial fill succeeds");
    executor
        .wait_for_durable_promotions()
        .expect("promotion drain");

    let output = recorder.encode().expect("prometheus text");
    assert!(output.contains(
        r#"datalens_query_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_query_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="partial_hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_cache_coverage_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_cache_coverage_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="partial_hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_fill_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="filled"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_durable_write_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="flushed"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_application_chain_latest_requested_block{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks"} 4"#
    ));
    assert!(output.contains(
        r#"datalens_application_chain_latest_filled_block{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks"} 4"#
    ));
}

#[test]
fn test_executor_records_provider_and_storage_errors_without_rewriting_errors() {
    let provider_recorder = MetricsRecorder::new().expect("provider metrics recorder");
    let provider_executor = executor(
        LocalStorage::new(temp_storage_root("executor-provider-metrics")),
        MockSource::default().with_error(DatalensErrorKind::ProviderLimit),
    )
    .with_metrics(provider_recorder.clone(), ApplicationIdentity::named("api"));

    let provider_error = provider_executor
        .execute(blocks_input(1, 1))
        .expect_err("provider error is returned");

    assert_eq!(provider_error.kind, DatalensErrorKind::ProviderLimit);
    let provider_output = provider_recorder.encode().expect("prometheus text");
    assert!(provider_output.contains(
        r#"datalens_query_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="error"} 1"#
    ));
    assert!(provider_output.contains(
        r#"datalens_fill_total{application="api",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="error"} 1"#
    ));
    assert!(provider_output.contains(
        r#"datalens_provider_error_total{chain="ethereum",chain_kind="evm",dataset="evm.blocks",error_kind="provider_limit"} 1"#
    ));

    let storage_recorder = MetricsRecorder::new().expect("storage metrics recorder");
    let storage_executor = executor(
        FailingStorage::new(DatalensErrorKind::StorageReadFailure),
        MockSource::default(),
    )
    .with_metrics(storage_recorder.clone(), ApplicationIdentity::named("api"));

    let storage_error = storage_executor
        .execute(blocks_input(1, 1))
        .expect_err("storage error is returned");

    assert_eq!(storage_error.kind, DatalensErrorKind::StorageReadFailure);
    let storage_output = storage_recorder.encode().expect("prometheus text");
    assert!(storage_output.contains(
        r#"datalens_storage_error_total{chain="ethereum",chain_kind="evm",dataset="evm.blocks",error_kind="storage_read_failure"} 1"#
    ));
}

#[test]
fn test_executor_rejects_fetch_response_chain_mismatch_without_cache_write() {
    assert_response_mismatch_not_cached(
        "executor-chain-mismatch",
        ResponseMutation::Chain(
            ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
                .expect("valid chain"),
        ),
    );
}

#[test]
fn test_executor_rejects_fetch_response_dataset_mismatch_without_cache_write() {
    assert_response_mismatch_not_cached(
        "executor-dataset-mismatch",
        ResponseMutation::Dataset(DatasetKey::evm_logs()),
    );
}

#[test]
fn test_executor_rejects_fetch_response_range_mismatch_without_cache_write() {
    assert_response_mismatch_not_cached(
        "executor-range-mismatch",
        ResponseMutation::Range(LedgerRange::blocks(2, 2).unwrap()),
    );
}

#[test]
fn test_executor_rejects_fetch_response_selector_mismatch_without_cache_write() {
    assert_response_mismatch_not_cached(
        "executor-selector-mismatch",
        ResponseMutation::Selector(
            DatasetSelector::try_evm_logs(LogFilter {
                addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
                topics: Vec::new(),
            })
            .expect("valid selector"),
        ),
    );
}

#[test]
fn test_executor_rejects_fetch_response_rows_mismatch_without_cache_write() {
    assert_response_mismatch_not_cached(
        "executor-rows-mismatch",
        ResponseMutation::Rows(
            DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(vec![])).unwrap(),
        ),
    );
}

fn assert_response_mismatch_not_cached(name: &str, mutation: ResponseMutation) {
    let root = temp_storage_root(name);
    let storage = LocalStorage::new(&root);
    let source = MockSource::default()
        .with_blocks(vec![block(1, "0x01")])
        .with_response_mutation(mutation);
    let executor = executor(storage, source);

    let error = executor
        .execute(blocks_input(1, 1))
        .expect_err("mismatch rejected");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

fn executor<R>(storage: R, source: MockSource) -> NativeQueryExecutor<R, MockSource>
where
    R: StorageRepository + Clone + 'static,
{
    NativeQueryExecutor::new(
        storage,
        source,
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 4,
                default_chunk_range_len: 2,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
        },
    )
}

#[derive(Clone)]
struct CountingStorage {
    inner: LocalStorage,
    read_ranges: Arc<Mutex<Vec<LedgerRange>>>,
}

#[derive(Clone)]
struct CountingObjectStore {
    inner: LocalObjectStore,
    coverage_index_gets: Arc<AtomicUsize>,
}

impl CountingObjectStore {
    fn new(inner: LocalObjectStore) -> Self {
        Self {
            inner,
            coverage_index_gets: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn reset_counts(&self) {
        self.coverage_index_gets.store(0, Ordering::SeqCst);
    }

    fn coverage_index_get_count(&self) -> usize {
        self.coverage_index_gets.load(Ordering::SeqCst)
    }
}

impl ObjectStore for CountingObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        if key.contains("/coverage-index/") || key.contains("/coverage-index-semantic/") {
            self.coverage_index_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        format!("counting:{}", self.inner.lock_namespace())
    }
}

#[derive(Clone, Default)]
struct RecordingIntentRepository {
    recorded: Arc<Mutex<Vec<CreateDurablePromotionIntent>>>,
}

#[derive(Clone)]
struct SlowStartupMaintenanceIntentRepository;

#[derive(Clone)]
struct BlockingStartupMaintenancePendingIntentRepository {
    inner: DurablePromotionIntentStore<LocalObjectStore>,
    maintenance_gate: WriteGate,
    claimed: Arc<AtomicUsize>,
}

impl BlockingStartupMaintenancePendingIntentRepository {
    fn new(inner: DurablePromotionIntentStore<LocalObjectStore>) -> Self {
        Self {
            inner,
            maintenance_gate: WriteGate::default(),
            claimed: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn maintenance_gate(&self) -> WriteGate {
        self.maintenance_gate.clone()
    }

    fn claimed(&self) -> Arc<AtomicUsize> {
        self.claimed.clone()
    }
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
            intent_from_request(request, DurablePromotionIntentStatus::Pending),
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

impl DurablePromotionIntentRepository for BlockingStartupMaintenancePendingIntentRepository {
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        self.inner.create_or_get(request)
    }

    fn get(&self, intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.inner.get(intent_id)
    }

    fn list_pending(
        &self,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        self.inner.list_pending(now_unix_seconds, limit)
    }

    fn list_pending_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        self.inner
            .list_pending_for_chain(chain, now_unix_seconds, limit)
    }

    fn rebuild_pending_indexes(&self, now_unix_seconds: u64) -> Result<usize, DatalensError> {
        self.inner.rebuild_pending_indexes(now_unix_seconds)
    }

    fn pending_backlog_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntentBacklog>, DatalensError> {
        self.inner
            .pending_backlog_for_chain(chain, now_unix_seconds)
    }

    fn mark_running(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        let intent = self.inner.mark_running(intent_id, now_unix_seconds)?;
        if intent.is_some() {
            self.claimed.fetch_add(1, Ordering::SeqCst);
        }
        Ok(intent)
    }

    fn mark_completed(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.inner.mark_completed(intent_id, now_unix_seconds)
    }

    fn mark_retryable_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
        next_retry_at_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.inner.mark_retryable_failure(
            intent_id,
            error,
            now_unix_seconds,
            next_retry_at_unix_seconds,
        )
    }

    fn mark_terminal_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.inner
            .mark_terminal_failure(intent_id, error, now_unix_seconds)
    }

    fn reset_stale_running(
        &self,
        _stale_before_unix_seconds: u64,
        _now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        self.maintenance_gate.block_until_released();
        Ok(Vec::new())
    }
}

impl DurablePromotionIntentRepository for SlowStartupMaintenanceIntentRepository {
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        Ok(DurablePromotionIntentCreateOutcome::Created(
            intent_from_request(request, DurablePromotionIntentStatus::Pending),
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
        thread::sleep(Duration::from_secs(1));
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct CompletedIntentRepository;

impl DurablePromotionIntentRepository for CompletedIntentRepository {
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        Ok(DurablePromotionIntentCreateOutcome::Existing(
            intent_from_request(request, DurablePromotionIntentStatus::Completed),
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

#[derive(Clone)]
struct FailingIntentRepository;

impl DurablePromotionIntentRepository for FailingIntentRepository {
    fn create_or_get(
        &self,
        _request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "intent repository unavailable",
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

fn intent_from_request(
    request: CreateDurablePromotionIntent,
    status: DurablePromotionIntentStatus,
) -> DurablePromotionIntent {
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
        status,
        attempt_count: 0,
        next_retry_at_unix_seconds: None,
        created_at_unix_seconds: request.now_unix_seconds,
        updated_at_unix_seconds: request.now_unix_seconds,
        last_error: None,
        request_id: request.request_id,
        task_id: request.task_id,
    }
}

impl CountingStorage {
    fn new(inner: LocalStorage) -> Self {
        Self {
            inner,
            read_ranges: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn read_ranges(&self) -> Vec<LedgerRange> {
        self.read_ranges.lock().expect("read ranges lock").clone()
    }
}

impl StorageRepository for CountingStorage {
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
        self.read_ranges
            .lock()
            .expect("read ranges lock")
            .push(range.clone());
        self.inner.read_rows(chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.inner.write_rows(request)
    }
}

#[derive(Clone)]
struct BlockingWriteStorage {
    inner: LocalStorage,
    write_gate: WriteGate,
    write_attempts: Arc<AtomicUsize>,
}

impl BlockingWriteStorage {
    fn new(inner: LocalStorage) -> Self {
        Self {
            inner,
            write_gate: WriteGate::default(),
            write_attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn write_gate(&self) -> WriteGate {
        self.write_gate.clone()
    }

    fn write_attempts(&self) -> usize {
        self.write_attempts.load(Ordering::SeqCst)
    }
}

impl StorageRepository for BlockingWriteStorage {
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
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        self.write_gate.block_until_released();
        self.inner.write_rows(request)
    }
}

#[derive(Clone, Default)]
struct WriteGate {
    state: Arc<(Mutex<WriteGateState>, Condvar)>,
}

#[derive(Default)]
struct WriteGateState {
    blocked: bool,
    released: bool,
}

impl WriteGate {
    fn wait_until_blocked(&self) {
        let deadline = Instant::now() + Duration::from_secs(2);
        let (lock, condvar) = self.state.as_ref();
        let mut state = lock.lock().expect("write gate lock");
        while !state.blocked {
            let now = Instant::now();
            if now >= deadline {
                panic!("write_rows did not block");
            }
            let timeout = deadline.saturating_duration_since(now);
            let (next, _) = condvar
                .wait_timeout(state, timeout)
                .expect("write gate wait");
            state = next;
        }
    }

    fn release(&self) {
        let (lock, condvar) = self.state.as_ref();
        let mut state = lock.lock().expect("write gate lock");
        state.released = true;
        condvar.notify_all();
    }

    fn block_until_released(&self) {
        let (lock, condvar) = self.state.as_ref();
        let mut state = lock.lock().expect("write gate lock");
        state.blocked = true;
        condvar.notify_all();
        while !state.released {
            state = condvar.wait(state).expect("write gate wait");
        }
    }
}

#[derive(Clone)]
struct FailingWriteStorage {
    inner: LocalStorage,
}

impl FailingWriteStorage {
    fn new(inner: LocalStorage) -> Self {
        Self { inner }
    }
}

impl StorageRepository for FailingWriteStorage {
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
        _request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected durable write failure",
        ))
    }
}

#[derive(Clone)]
struct FailingStorage {
    kind: DatalensErrorKind,
}

impl FailingStorage {
    fn new(kind: DatalensErrorKind) -> Self {
        Self { kind }
    }
}

#[derive(Clone, Default)]
struct FailingUsageLedgerRepository {
    attempts: Arc<AtomicUsize>,
}

impl FailingUsageLedgerRepository {
    fn attempts(&self) -> Arc<AtomicUsize> {
        self.attempts.clone()
    }
}

impl UsageLedgerRepository for FailingUsageLedgerRepository {
    fn append(&self, _entry: &datalens_storage::UsageLedgerEntry) -> Result<(), DatalensError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected ledger write failure",
        ))
    }

    fn read_application(
        &self,
        _application_id: &str,
    ) -> Result<Vec<datalens_storage::UsageLedgerEntry>, DatalensError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Default)]
struct FailingQueryWatermarkRepository {
    attempts: Arc<AtomicUsize>,
}

impl FailingQueryWatermarkRepository {
    fn attempts(&self) -> Arc<AtomicUsize> {
        self.attempts.clone()
    }
}

impl QueryWatermarkRepository for FailingQueryWatermarkRepository {
    fn update(&self, _watermark: &datalens_storage::QueryWatermark) -> Result<(), DatalensError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected query watermark write failure",
        ))
    }

    fn read(
        &self,
        _key: &QueryWatermarkKey,
    ) -> Result<Option<datalens_storage::QueryWatermark>, DatalensError> {
        Ok(None)
    }
}

#[derive(Clone, Default)]
struct FailingQueryActivityRepository {
    attempts: Arc<AtomicUsize>,
}

impl FailingQueryActivityRepository {
    fn attempts(&self) -> Arc<AtomicUsize> {
        self.attempts.clone()
    }
}

impl QueryActivityRepository for FailingQueryActivityRepository {
    fn update(&self, _activity: &datalens_storage::QueryActivity) -> Result<(), DatalensError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected query activity write failure",
        ))
    }

    fn read(
        &self,
        _key: &QueryActivityKey,
    ) -> Result<Option<datalens_storage::QueryActivity>, DatalensError> {
        Ok(None)
    }
}

#[derive(Clone)]
struct SlowReadQueryActivityRepository {
    read_attempts: Arc<AtomicUsize>,
    delay: Duration,
}

impl SlowReadQueryActivityRepository {
    fn new(delay: Duration) -> Self {
        Self {
            read_attempts: Arc::new(AtomicUsize::new(0)),
            delay,
        }
    }

    fn read_attempts(&self) -> Arc<AtomicUsize> {
        self.read_attempts.clone()
    }
}

impl QueryActivityRepository for SlowReadQueryActivityRepository {
    fn update(&self, _activity: &datalens_storage::QueryActivity) -> Result<(), DatalensError> {
        Ok(())
    }

    fn read(
        &self,
        _key: &QueryActivityKey,
    ) -> Result<Option<datalens_storage::QueryActivity>, DatalensError> {
        self.read_attempts.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        Ok(None)
    }
}

#[derive(Clone)]
struct SlowUsageLedgerRepository {
    attempts: Arc<AtomicUsize>,
    delay: Duration,
}

impl SlowUsageLedgerRepository {
    fn new(delay: Duration) -> Self {
        Self {
            attempts: Arc::new(AtomicUsize::new(0)),
            delay,
        }
    }

    fn attempts(&self) -> Arc<AtomicUsize> {
        self.attempts.clone()
    }
}

impl UsageLedgerRepository for SlowUsageLedgerRepository {
    fn append(&self, _entry: &datalens_storage::UsageLedgerEntry) -> Result<(), DatalensError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        Ok(())
    }

    fn read_application(
        &self,
        _application_id: &str,
    ) -> Result<Vec<datalens_storage::UsageLedgerEntry>, DatalensError> {
        Ok(Vec::new())
    }
}

fn wait_for_attempt(attempts: &AtomicUsize) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if attempts.load(Ordering::SeqCst) > 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        attempts.load(Ordering::SeqCst) > 0,
        "metadata repository was not attempted"
    );
}

fn wait_for_query_activity<S>(
    activities: &QueryActivityStore<S>,
    key: &QueryActivityKey,
) -> datalens_storage::QueryActivity
where
    S: datalens_storage::ObjectStore + 'static,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(activity) = activities.read(key).expect("read activity") {
            return activity;
        }
        if Instant::now() >= deadline {
            panic!("query activity was not recorded");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_query_activity_end<S>(
    activities: &QueryActivityStore<S>,
    key: &QueryActivityKey,
    latest_end: u64,
) -> datalens_storage::QueryActivity
where
    S: datalens_storage::ObjectStore + 'static,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(activity) = activities.read(key).expect("read activity")
            && activity.latest_range.end() == latest_end
        {
            return activity;
        }
        if Instant::now() >= deadline {
            panic!("query activity ending at {latest_end} was not recorded");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_ledger_events<R>(
    ledger: &R,
    application_id: &str,
    expected: usize,
) -> Vec<datalens_storage::UsageLedgerEntry>
where
    R: UsageLedgerRepository,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let events = ledger
            .read_application(application_id)
            .expect("read application usage");
        if events.len() >= expected || Instant::now() >= deadline {
            assert!(
                events.len() >= expected,
                "expected at least {expected} ledger events, got {}",
                events.len()
            );
            return events;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_query_watermark<R>(
    watermarks: &R,
    key: &QueryWatermarkKey,
) -> datalens_storage::QueryWatermark
where
    R: QueryWatermarkRepository,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let watermark = watermarks.read(key).expect("read watermark");
        if let Some(watermark) = watermark {
            return watermark;
        }
        if Instant::now() >= deadline {
            panic!("watermark was not recorded");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

impl StorageRepository for FailingStorage {
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        Err(DatalensError::new(
            self.kind.clone(),
            "injected storage failure",
        ))
    }

    fn covered_ranges(
        &self,
        _chain: &ChainIdentity,
        _dataset_key: &DatasetKey,
        _selector: &DatasetSelector,
        _range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        Err(DatalensError::new(
            self.kind.clone(),
            "injected storage failure",
        ))
    }

    fn read_rows(
        &self,
        _chain: &ChainIdentity,
        _dataset_key: &DatasetKey,
        _selector: &DatasetSelector,
        _range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        Err(DatalensError::new(
            self.kind.clone(),
            "injected storage failure",
        ))
    }

    fn write_rows(
        &self,
        _request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        Err(DatalensError::new(
            self.kind.clone(),
            "injected storage failure",
        ))
    }
}

fn seed_blocks(storage: &LocalStorage, start: u64, end: u64, blocks: Vec<BlockHeader>) {
    seed_blocks_in_storage(storage, start, end, blocks);
}

fn seed_blocks_in_storage<R>(storage: &R, start: u64, end: u64, blocks: Vec<BlockHeader>)
where
    R: StorageRepository + Clone + 'static,
{
    datalens_writer::DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    )
    .write(DurableWriteRequest {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        finality_level: FinalityKind::Safe,
        segments: vec![DurableWriteSegment {
            range: LedgerRange::blocks(start, end).expect("valid range"),
            rows: DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(blocks))
                .expect("dataset rows"),
        }],
    })
    .expect("seed cache");
}

fn seed_empty_blocks(storage: &LocalStorage, start: u64, end: u64) {
    datalens_writer::DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    )
    .write(DurableWriteRequest {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        finality_level: FinalityKind::Safe,
        segments: vec![DurableWriteSegment {
            range: LedgerRange::blocks(start, end).expect("valid range"),
            rows: DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
                .expect("dataset rows"),
        }],
    })
    .expect("seed empty coverage");
}

fn seed_logs_in_storage<R>(
    storage: &R,
    selector: &DatasetSelector,
    start: u64,
    end: u64,
    logs: Vec<LogRecord>,
) where
    R: StorageRepository + Clone + 'static,
{
    datalens_writer::DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    )
    .write(DurableWriteRequest {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_logs(),
        selector: selector.clone(),
        finality_level: FinalityKind::Safe,
        segments: vec![DurableWriteSegment {
            range: LedgerRange::blocks(start, end).expect("valid range"),
            rows: DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(logs))
                .expect("dataset rows"),
        }],
    })
    .expect("seed log cache");
}

fn blocks_input(start: u64, end: u64) -> NativeQueryInput {
    NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        ledger_range: LedgerRange::blocks(start, end).expect("valid range"),
        selector: DatasetSelector::all(),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

fn logs_input(selector: DatasetSelector, start: u64, end: u64) -> NativeQueryInput {
    NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_logs(),
        ledger_range: LedgerRange::blocks(start, end).expect("valid range"),
        selector,
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

fn block_numbers(rows: &DatasetRows) -> Vec<u64> {
    match rows.rows() {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected blocks"),
    }
}

fn log_rows(rows: &DatasetRows) -> &[LogRecord] {
    match rows.rows() {
        QueryRows::EvmLogs(rows) => rows,
        _ => panic!("expected logs"),
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number * 10,
    }
}

const ADDRESS_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ADDRESS_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const TOPIC_1: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const TOPIC_2: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";

fn evm_log_selector(addresses: Vec<&str>, topics: Vec<Option<Vec<&str>>>) -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: addresses.into_iter().map(str::to_owned).collect(),
        topics: topics
            .into_iter()
            .map(|slot| slot.map(|values| values.into_iter().map(str::to_owned).collect()))
            .collect(),
    })
    .expect("valid selector")
}

fn log_record(block_number: u64, log_index: u64, address: &str, topics: Vec<&str>) -> LogRecord {
    LogRecord::try_new(
        block_number,
        format!("0xblock{block_number}"),
        format!("0xtx{block_number}{log_index}"),
        0,
        log_index,
        address,
        topics.into_iter().map(str::to_owned).collect(),
        "0x".to_owned(),
        false,
    )
    .expect("valid log record")
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceCall {
    Blocks(BlockRange),
}

#[derive(Clone)]
struct MockSource {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    request_ids: Arc<Mutex<Vec<Option<String>>>>,
    latest_height: Arc<Mutex<u64>>,
    safe_height: Arc<Mutex<u64>>,
    response_mutation: Arc<Mutex<Option<ResponseMutation>>>,
    safe_height_error: Arc<Mutex<Option<DatalensErrorKind>>>,
    error: Arc<Mutex<Option<DatalensErrorKind>>>,
    provider_limit_len: Arc<Mutex<Option<u128>>>,
    provider_limit_message: Arc<Mutex<Option<String>>>,
    capability_max_range_len: Arc<Mutex<u64>>,
    fetch_delay: Arc<Mutex<Option<Duration>>>,
}

impl Default for MockSource {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            request_ids: Arc::new(Mutex::new(Vec::new())),
            latest_height: Arc::new(Mutex::new(100)),
            safe_height: Arc::new(Mutex::new(100)),
            response_mutation: Arc::new(Mutex::new(None)),
            safe_height_error: Arc::new(Mutex::new(None)),
            error: Arc::new(Mutex::new(None)),
            provider_limit_len: Arc::new(Mutex::new(None)),
            provider_limit_message: Arc::new(Mutex::new(None)),
            capability_max_range_len: Arc::new(Mutex::new(2)),
            fetch_delay: Arc::new(Mutex::new(None)),
        }
    }
}

impl MockSource {
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    fn with_response_mutation(self, mutation: ResponseMutation) -> Self {
        *self
            .response_mutation
            .lock()
            .expect("response mutation lock") = Some(mutation);
        self
    }

    fn with_latest_height(self, height: u64) -> Self {
        *self.latest_height.lock().expect("latest height lock") = height;
        self
    }

    fn with_safe_height(self, height: u64) -> Self {
        *self.safe_height.lock().expect("safe height lock") = height;
        self
    }

    fn with_safe_height_error(self, kind: DatalensErrorKind) -> Self {
        *self
            .safe_height_error
            .lock()
            .expect("safe height error lock") = Some(kind);
        self
    }

    fn with_error(self, kind: DatalensErrorKind) -> Self {
        *self.error.lock().expect("error lock") = Some(kind);
        self
    }

    fn with_capability_max_range_len(self, len: u64) -> Self {
        *self
            .capability_max_range_len
            .lock()
            .expect("capability max range len lock") = len;
        self
    }

    fn with_provider_limit_for_ranges_longer_than(self, len: u128) -> Self {
        *self
            .provider_limit_len
            .lock()
            .expect("provider limit len lock") = Some(len);
        self
    }

    fn with_provider_limit_message(self, message: impl Into<String>) -> Self {
        *self
            .provider_limit_message
            .lock()
            .expect("provider limit message lock") = Some(message.into());
        self
    }

    fn with_fetch_delay(self, delay: Duration) -> Self {
        *self.fetch_delay.lock().expect("fetch delay lock") = Some(delay);
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn clear_calls(&self) {
        self.calls.lock().expect("calls lock").clear();
        self.request_ids.lock().expect("request ids lock").clear();
    }

    fn request_ids(&self) -> Vec<Option<String>> {
        self.request_ids.lock().expect("request ids lock").clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResponseMutation {
    Chain(ChainIdentity),
    Dataset(DatasetKey),
    Range(LedgerRange),
    Selector(DatasetSelector),
    Rows(DatasetRows),
}

impl ChainAdapter for MockSource {
    fn capabilities(&self) -> AdapterCapabilities {
        let max_range_len = *self
            .capability_max_range_len
            .lock()
            .expect("capability max range len lock");
        AdapterCapabilities::new(ethereum_identity())
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Blocks)
                    .with_selector(SelectorKind::All)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(max_range_len)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Logs)
                    .with_selector(SelectorKind::All)
                    .with_selector(SelectorKind::EvmLogs)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(max_range_len)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(
            *self.latest_height.lock().expect("latest height lock"),
        ))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        if let Some(kind) = self
            .safe_height_error
            .lock()
            .expect("safe height error lock")
            .clone()
        {
            return Err(DatalensError::new(kind, "injected safe height failure"));
        }
        Ok(
            ChainHeight::block(*self.safe_height.lock().expect("safe height lock"))
                .with_finality(FinalityKind::Safe),
        )
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = request.range.block_range().expect("expected block range");
        self.calls
            .lock()
            .expect("calls lock")
            .push(SourceCall::Blocks(range));
        self.request_ids
            .lock()
            .expect("request ids lock")
            .push(request.context.request_id.clone());
        if let Some(delay) = *self.fetch_delay.lock().expect("fetch delay lock") {
            thread::sleep(delay);
        }
        if let Some(kind) = self.error.lock().expect("error lock").clone() {
            return Err(DatalensError::new(kind, "injected provider failure"));
        }
        if self
            .provider_limit_len
            .lock()
            .expect("provider limit len lock")
            .is_some_and(|limit| request.range.len() > limit)
        {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderLimit,
                self.provider_limit_message
                    .lock()
                    .expect("provider limit message lock")
                    .clone()
                    .unwrap_or_else(|| "injected provider limit".to_owned()),
            ));
        }
        let rows = self
            .blocks
            .lock()
            .expect("blocks lock")
            .iter()
            .filter(|block| range.contains(block.number))
            .cloned()
            .collect();
        let mut response = ChainFetchResponse::try_new(
            request.chain,
            DatasetKey::evm_blocks(),
            request.range,
            request.selector,
            QueryRows::EvmBlocks(rows),
        )?
        .with_provider_diagnostics(datalens_chain::ProviderDiagnostics {
            calls: 1,
            rows_scanned: 0,
            warnings: Vec::new(),
        });
        match self
            .response_mutation
            .lock()
            .expect("response mutation lock")
            .clone()
        {
            Some(ResponseMutation::Chain(chain)) => response.chain = chain,
            Some(ResponseMutation::Dataset(dataset_key)) => response.dataset_key = dataset_key,
            Some(ResponseMutation::Range(range)) => response.range = range,
            Some(ResponseMutation::Selector(selector)) => response.coverage_selector = selector,
            Some(ResponseMutation::Rows(rows)) => response.rows = rows,
            None => {}
        }
        Ok(response)
    }
}
