use datalens_chain::ChainAdapter;
use datalens_core::{DatasetKey, LedgerRange, QueryFinalityRequirement, QueryRows};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_indexer::{
    InMemoryIndexCursorStore, IndexDatasetRequest, IndexDatasetSelection, IndexFinalityRequirement,
    IndexJob, IndexJobId, IndexRetryPolicy, IndexRunMode, IndexRunStatus, IndexRuntime,
    IndexRuntimeConfig,
};
use datalens_metrics::ApplicationIdentity;
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput, ResponseShape};
use datalens_storage::LocalStorage;
use datalens_tron::{TronAdapter, TronFixtureProviderRpc, tron_all_selector};
use datalens_writer::DurableWriterConfig;

#[test]
fn test_tron_full_indexing_writes_all_durable_datasets() {
    let storage = LocalStorage::new(temp_storage_root("full-indexing"));
    let adapter = TronAdapter::with_fixture_defaults();
    let runtime = runtime(adapter.clone(), storage.clone());

    let result = runtime
        .run(full_tron_job(10, 12, IndexRunMode::Backfill))
        .expect("index Tron datasets");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(result.accounting.chunks_planned, 4);
    assert_eq!(result.accounting.rows_written, 6);

    let selector = tron_all_selector().expect("selector");
    for (dataset_key, expected_rows) in [
        (DatasetKey::tron_blocks(), 3),
        (DatasetKey::tron_transactions(), 1),
        (DatasetKey::tron_transaction_infos(), 1),
        (DatasetKey::tron_events(), 1),
    ] {
        assert_eq!(
            storage
                .covered_ranges(
                    adapter.capabilities().chain(),
                    &dataset_key,
                    &selector,
                    LedgerRange::blocks(10, 12).expect("range"),
                )
                .expect("coverage"),
            vec![LedgerRange::blocks(10, 12).expect("range")]
        );
        assert_eq!(
            storage
                .read_rows(
                    adapter.capabilities().chain(),
                    &dataset_key,
                    &selector,
                    LedgerRange::blocks(10, 12).expect("range"),
                )
                .expect("read rows")
                .row_count(),
            expected_rows
        );
    }
}

#[test]
fn test_tron_empty_events_record_durable_coverage() {
    let storage = LocalStorage::new(temp_storage_root("empty-events"));
    let adapter = TronAdapter::with_fixture_defaults();
    let runtime = runtime(adapter.clone(), storage.clone());

    let result = runtime
        .run(selected_tron_job(
            DatasetKey::tron_events(),
            11,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("index empty events");

    assert_eq!(result.accounting.rows_written, 0);
    assert_eq!(
        storage
            .covered_ranges(
                adapter.capabilities().chain(),
                &DatasetKey::tron_events(),
                &tron_all_selector().expect("selector"),
                LedgerRange::blocks(11, 12).expect("range"),
            )
            .expect("coverage"),
        vec![LedgerRange::blocks(11, 12).expect("range")]
    );
}

#[test]
fn test_tron_resume_after_partial_indexing_is_idempotent() {
    let storage = LocalStorage::new(temp_storage_root("resume"));
    let cursor_store = InMemoryIndexCursorStore::default();
    let adapter = TronAdapter::with_fixture_defaults();
    let first = IndexRuntime::new(
        adapter.clone(),
        storage.clone(),
        cursor_store.clone(),
        writer_config(),
    );

    first
        .run(selected_tron_job(
            DatasetKey::tron_blocks(),
            10,
            10,
            IndexRunMode::Backfill,
        ))
        .expect("seed first chunk");

    let resumed = IndexRuntime::new(
        adapter.clone(),
        storage.clone(),
        cursor_store,
        writer_config(),
    );
    let result = resumed
        .run(selected_tron_job(
            DatasetKey::tron_blocks(),
            10,
            12,
            IndexRunMode::Resume,
        ))
        .expect("resume");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::tron_blocks(),
                &tron_all_selector().expect("selector"),
                LedgerRange::blocks(10, 12).expect("range"),
            )
            .expect("read rows")
            .row_count(),
        3
    );

    let rerun = resumed
        .run(selected_tron_job(
            DatasetKey::tron_blocks(),
            10,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("rerun");
    assert_eq!(rerun.accounting.chunks_planned, 0);
}

#[test]
fn test_tron_indexing_chunks_according_to_provider_limit() {
    let storage = LocalStorage::new(temp_storage_root("provider-limit"));
    let adapter = TronAdapter::with_provider_limits(TronFixtureProviderRpc, 1);
    let runtime = runtime(adapter, storage);

    let result = runtime
        .run(selected_tron_job(
            DatasetKey::tron_transactions(),
            10,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("provider limit split succeeds");

    assert_eq!(result.accounting.chunks_planned, 3);
    assert_eq!(result.accounting.provider_limit_splits, 0);
}

#[test]
fn test_tron_durable_query_reads_indexed_transactions() {
    let root = temp_storage_root("query-indexed");
    let storage = LocalStorage::new(&root);
    let adapter = TronAdapter::with_fixture_defaults();
    runtime(adapter.clone(), storage.clone())
        .run(selected_tron_job(
            DatasetKey::tron_transactions(),
            10,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("index transactions");

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
    );
    let result = executor
        .execute(NativeQueryInput {
            chain: adapter.capabilities().chain().clone(),
            dataset_key: DatasetKey::tron_transactions(),
            ledger_range: LedgerRange::blocks(10, 12).expect("range"),
            selector: tron_all_selector().expect("selector"),
            response_shape: ResponseShape::NativeRows,
            field_selection: FieldSelection::All,
            finality: QueryFinalityRequirement::DurableOnly,
        })
        .expect("query indexed transactions");

    let QueryRows::AdapterJson { rows, .. } = result.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["transaction_id"], "tron-tx-10");
    assert_eq!(rows[0]["block_number"], 10);
}

fn runtime(
    adapter: TronAdapter<TronFixtureProviderRpc>,
    storage: LocalStorage,
) -> IndexRuntime<TronAdapter<TronFixtureProviderRpc>, LocalStorage, InMemoryIndexCursorStore> {
    IndexRuntime::new(
        adapter,
        storage,
        InMemoryIndexCursorStore::default(),
        writer_config(),
    )
}

fn full_tron_job(start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    let selector = tron_all_selector().expect("selector");
    tron_job(
        vec![
            DatasetKey::tron_blocks(),
            DatasetKey::tron_transactions(),
            DatasetKey::tron_transaction_infos(),
            DatasetKey::tron_events(),
        ]
        .into_iter()
        .map(|dataset_key| IndexDatasetRequest {
            dataset_key,
            selector: selector.clone(),
        })
        .collect(),
        start,
        end,
        run_mode,
    )
}

fn selected_tron_job(
    dataset_key: DatasetKey,
    start: u64,
    end: u64,
    run_mode: IndexRunMode,
) -> IndexJob {
    tron_job(
        vec![IndexDatasetRequest {
            dataset_key,
            selector: tron_all_selector().expect("selector"),
        }],
        start,
        end,
        run_mode,
    )
}

fn tron_job(
    datasets: Vec<IndexDatasetRequest>,
    start: u64,
    end: u64,
    run_mode: IndexRunMode,
) -> IndexJob {
    IndexJob {
        id: IndexJobId::new("tron-indexing-fixture").expect("job id"),
        application: ApplicationIdentity::named("indexer"),
        chain: TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: IndexDatasetSelection::Selected(datasets),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 3 },
        run_mode,
        retry_policy: IndexRetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
    }
}

fn writer_config() -> DurableWriterConfig {
    DurableWriterConfig {
        target_object_bytes: 4096,
        min_object_rows: 1,
        record_empty_coverage: true,
        staging: Default::default(),
    }
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-tron-indexing-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
    root
}
