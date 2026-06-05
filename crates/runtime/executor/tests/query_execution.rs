use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, DatasetRows, LedgerRange, LogFilter, NetworkId, QueryDataFinality,
    QueryFinalityRequirement, QueryRows, QuerySegmentSource,
};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput};
use datalens_storage::{
    CacheOutcome, FillOutcome, LocalObjectStore, LocalStorage, Manifest, ObjectMetadata,
    ObjectStore, QueryOutcome, QueryWatermarkKey, QueryWatermarkRepository, QueryWatermarkStore,
    StorageRepository, StorageWriteOutcome, StorageWriteRequest, UsageLedgerRepository,
    UsageLedgerStore,
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
fn test_executor_miss_fetches_and_persists_through_writer() {
    let storage = LocalStorage::new(temp_storage_root("executor-miss"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let executor = executor(storage.clone(), source.clone());

    let first = executor
        .execute(blocks_input(10, 11))
        .expect("miss succeeds");
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
fn test_executor_flushes_staged_query_fill_before_returning() {
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
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 1);

    let second = executor
        .execute(blocks_input(10, 10))
        .expect("same-process query reads durable rows");

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
fn test_executor_usage_ledger_records_query_staging_flush_as_durable_write() {
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

    let events = ledger
        .read_application("analytics-api")
        .expect("read application usage");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].fill_outcome, FillOutcome::LiveFetch);
    assert_eq!(
        events[0].durable_write_outcome,
        datalens_storage::DurableWriteOutcome::Flushed
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

    let events = ledger
        .read_application("analytics-api")
        .expect("read application usage");
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

    let events = ledger
        .read_application("analytics-api")
        .expect("read application usage");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].query_outcome, QueryOutcome::Filled);
    assert_eq!(events[0].cache_outcome, CacheOutcome::Miss);
    assert_eq!(events[0].fill_outcome, FillOutcome::LiveFetch);
    assert_eq!(
        events[0].durable_write_outcome,
        datalens_storage::DurableWriteOutcome::Flushed
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
    let watermark = watermarks
        .read(&key)
        .expect("read watermark")
        .expect("watermark");
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
    assert!(root.join("chains/evm/ethereum/1/manifest.json").exists());

    let events = ledger
        .read_application("analytics-api")
        .expect("read application usage");
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

    let events = ledger
        .read_application("analytics-api")
        .expect("read application usage");
    assert_eq!(events[0].query_outcome, QueryOutcome::Mixed);
    assert_eq!(events[0].cache_outcome, CacheOutcome::Mixed);
    assert_eq!(events[0].fill_outcome, FillOutcome::LiveFetch);
}

#[test]
fn test_executor_records_separate_usage_for_shared_durable_cache() {
    let root = temp_storage_root("executor-ledger-shared-cache");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let source = MockSource::default().with_blocks(vec![block(20, "0x20")]);

    executor(storage.clone(), source.clone())
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("app-a"))
        .execute(blocks_input(20, 20))
        .expect("first application fills cache");
    executor(storage.clone(), source.clone())
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("app-b"))
        .execute(blocks_input(20, 20))
        .expect("second application hits shared cache");

    assert_eq!(
        ledger.read_application("app-a").expect("read app-a usage")[0].fill_outcome,
        FillOutcome::LiveFetch
    );
    assert_eq!(
        ledger.read_application("app-a").expect("read app-a usage")[0].durable_write_outcome,
        datalens_storage::DurableWriteOutcome::Flushed
    );
    assert_eq!(
        ledger.read_application("app-b").expect("read app-b usage")[0].cache_outcome,
        CacheOutcome::Hit
    );
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
fn test_executor_ledger_write_failure_blocks_successful_query() {
    let storage = LocalStorage::new(temp_storage_root("executor-ledger-failure"));
    seed_blocks(&storage, 1, 1, vec![block(1, "0x01")]);
    let source = MockSource::default();
    let executor = executor(storage, source).with_usage_ledger(
        UsageLedgerStore::new(FailingPutObjectStore),
        ApplicationIdentity::named("api"),
    );

    let error = executor
        .execute(blocks_input(1, 1))
        .expect_err("ledger failure blocks query");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
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
    R: StorageRepository + Clone,
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

#[derive(Clone, Debug)]
struct FailingPutObjectStore;

impl ObjectStore for FailingPutObjectStore {
    fn get(&self, _key: &str) -> Result<Vec<u8>, DatalensError> {
        Ok(Vec::new())
    }

    fn put(&self, _key: &str, _bytes: &[u8]) -> Result<(), DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected ledger write failure",
        ))
    }

    fn exists(&self, _key: &str) -> Result<bool, DatalensError> {
        Ok(false)
    }

    fn list(&self, _prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        Ok(Vec::new())
    }

    fn delete(&self, _key: &str) -> Result<(), DatalensError> {
        Ok(())
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

fn block_numbers(rows: &DatasetRows) -> Vec<u64> {
    match rows.rows() {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected blocks"),
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

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
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
        AdapterCapabilities::new(ethereum_identity()).with_dataset_capability(
            DatasetCapability::new(Dataset::Blocks)
                .with_selector(SelectorKind::All)
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(2)
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
        if let Some(kind) = self.error.lock().expect("error lock").clone() {
            return Err(DatalensError::new(kind, "injected provider failure"));
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
