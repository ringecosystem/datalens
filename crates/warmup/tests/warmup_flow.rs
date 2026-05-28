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
use datalens_metrics::ApplicationIdentity;
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput, ResponseShape};
use datalens_storage::{LocalObjectStore, LocalStorage};
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
                response_shape: ResponseShape::LegacyEvmLogs,
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
    datalens_writer::DurableWriter::new(storage.clone(), writer_config())
        .write(datalens_writer::DurableWriteRequest {
            chain: chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: selector(),
            finality_level: FinalityLevel::Safe,
            segments: vec![datalens_writer::DurableWriteSegment {
                range,
                rows: DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
                    .unwrap(),
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
