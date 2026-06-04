use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
    LocalObjectStore, LocalStorage, Manifest, StorageRepository, StorageWriteOutcome,
    StorageWriteRequest,
};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupChunkPolicy, WarmupRetryPolicy, WarmupRunStatus, WarmupRuntime,
    WarmupSubmitRequest, WarmupTaskMode, WarmupTaskState,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig,
    WriteStagingConfig,
};

#[test]
fn test_staged_warmup_shutdown_flush_counts_written_ranges() {
    let storage = LocalStorage::new(temp_root("staged-success-storage"));
    let registry = LocalWarmupRegistry::new(object_store("staged-success-registry"));
    let adapter = FixtureAdapter::new(1).with_logs(vec![log_record(1, 0)]);
    let runtime = WarmupRuntime::new(adapter, storage.clone(), registry.clone(), staging_config());
    let task_id = registry.submit(submit_request(Some(1))).unwrap().task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(result.status, WarmupRunStatus::Completed);
    assert_eq!(result.rows_fetched, 1);
    assert!(result.written_ranges > 0);
    assert_eq!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 1))
            .unwrap(),
        vec![blocks(1, 1)]
    );
    let task = registry.get(&task_id).unwrap().expect("task");
    assert_eq!(task.stats.written_ranges, result.written_ranges);
    assert!(task.stats.written_ranges > 0);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 2);
}

#[test]
fn test_staged_warmup_shutdown_flush_failure_does_not_advance_cursor() {
    let storage = ToggleFailStorage::new(LocalStorage::new(temp_root("staged-failure-storage")));
    let registry = LocalWarmupRegistry::new(object_store("staged-failure-registry"));
    let adapter = FixtureAdapter::new(1).with_logs(vec![log_record(1, 0)]);
    let runtime = WarmupRuntime::new(adapter, storage.clone(), registry.clone(), staging_config());
    let task_id = registry.submit(submit_request(Some(1))).unwrap().task_id;
    storage.set_fail_writes(true);

    let error = runtime
        .run_task_once(&task_id)
        .expect_err("shutdown flush failure");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 1))
            .unwrap()
            .is_empty()
    );
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 1);
    assert_eq!(cursor.current_attempt, 1);
    let task = registry.get(&task_id).unwrap().expect("task");
    assert_eq!(task.state, WarmupTaskState::Failed);
}

#[test]
fn test_warmup_skips_provider_for_shared_staged_coverage_without_advancing_cursor() {
    let storage = LocalStorage::new(temp_root("shared-staged-storage"));
    let registry = LocalWarmupRegistry::new(object_store("shared-staged-registry"));
    let adapter = FixtureAdapter::new(1).with_logs(vec![log_record(1, 0)]);
    let writer = DurableWriter::new(storage.clone(), staging_config());
    writer
        .write(DurableWriteRequest {
            chain: chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: selector(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: blocks(1, 1),
                rows: log_rows(vec![log_record(1, 0)]),
            }],
        })
        .expect("stage query write");
    let runtime = WarmupRuntime::new(
        adapter.clone(),
        storage.clone(),
        registry.clone(),
        staging_config(),
    )
    .with_durable_writer(writer);
    let task_id = registry.submit(submit_request(Some(1))).unwrap().task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert!(adapter.fetches().is_empty());
    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 1))
            .unwrap()
            .is_empty()
    );
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 1);
    assert_eq!(cursor.last_committed, None);
    let task = registry.get(&task_id).unwrap().expect("task");
    assert_eq!(task.state, WarmupTaskState::Queued);
}

#[test]
fn test_warmup_fetches_ranges_not_durable_or_staged() {
    let storage = LocalStorage::new(temp_root("shared-staged-gap-storage"));
    let registry = LocalWarmupRegistry::new(object_store("shared-staged-gap-registry"));
    let adapter = FixtureAdapter::new(2).with_logs(vec![log_record(1, 0), log_record(2, 0)]);
    let writer = DurableWriter::new(storage.clone(), staging_config());
    writer
        .write(DurableWriteRequest {
            chain: chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: selector(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: blocks(1, 1),
                rows: log_rows(vec![log_record(1, 0)]),
            }],
        })
        .expect("stage query write");
    let runtime = WarmupRuntime::new(
        adapter.clone(),
        storage.clone(),
        registry.clone(),
        staging_config(),
    )
    .with_durable_writer(writer);
    let mut request = submit_request(Some(2));
    request.chunk_policy.max_range_len = 1;
    let task_id = registry.submit(request).unwrap().task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(adapter.fetches(), vec![blocks(2, 2)]);
    assert_eq!(result.status, WarmupRunStatus::Partial);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 1);
    assert!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 2))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn test_warmup_flushes_own_staged_commit_before_external_staged_gap() {
    let storage = LocalStorage::new(temp_root("own-staged-before-shared-gap-storage"));
    let registry = LocalWarmupRegistry::new(object_store("own-staged-before-shared-gap-registry"));
    let adapter = FixtureAdapter::new(2).with_logs(vec![log_record(1, 0), log_record(2, 0)]);
    let writer = DurableWriter::new(storage.clone(), staging_config());
    writer
        .write(DurableWriteRequest {
            chain: chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: selector(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: blocks(2, 2),
                rows: log_rows(vec![log_record(2, 0)]),
            }],
        })
        .expect("stage query write");
    let runtime = WarmupRuntime::new(
        adapter.clone(),
        storage.clone(),
        registry.clone(),
        staging_config(),
    )
    .with_durable_writer(writer);
    let mut request = submit_request(Some(2));
    request.chunk_policy.max_range_len = 1;
    let task_id = registry.submit(request).unwrap().task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert_eq!(adapter.fetches(), vec![blocks(1, 1)]);
    assert_eq!(result.status, WarmupRunStatus::Partial);
    assert_eq!(
        storage
            .covered_ranges(&chain(), &DatasetKey::evm_logs(), &selector(), blocks(1, 2))
            .unwrap(),
        vec![blocks(1, 1)]
    );
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 2);
    let task = registry.get(&task_id).unwrap().expect("task");
    assert_eq!(task.state, WarmupTaskState::Queued);
}

#[test]
fn test_warmup_commits_shared_staged_coverage_after_durable_visibility() {
    let storage = LocalStorage::new(temp_root("shared-staged-flushed-storage"));
    let registry = LocalWarmupRegistry::new(object_store("shared-staged-flushed-registry"));
    let adapter = FixtureAdapter::new(1).with_logs(vec![log_record(1, 0)]);
    let writer = DurableWriter::new(storage.clone(), staging_config());
    writer
        .write(DurableWriteRequest {
            chain: chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: selector(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: blocks(1, 1),
                rows: log_rows(vec![log_record(1, 0)]),
            }],
        })
        .expect("stage query write");
    writer.flush().expect("flush staged query write");
    let runtime = WarmupRuntime::new(adapter.clone(), storage, registry.clone(), staging_config())
        .with_durable_writer(writer);
    let task_id = registry.submit(submit_request(Some(1))).unwrap().task_id;

    let result = runtime.run_task_once(&task_id).expect("warmup run");

    assert!(adapter.fetches().is_empty());
    assert_eq!(result.status, WarmupRunStatus::Completed);
    let cursor = registry.load_cursor(&task_id).unwrap().expect("cursor");
    assert_eq!(cursor.next, 2);
}

fn staging_config() -> DurableWriterConfig {
    DurableWriterConfig {
        target_object_bytes: 1_000_000,
        min_object_rows: 10,
        record_empty_coverage: true,
        staging: WriteStagingConfig {
            enabled: true,
            min_rows: Some(10),
            flush_on_shutdown: true,
            ..Default::default()
        },
    }
}

fn submit_request(end: Option<u64>) -> WarmupSubmitRequest {
    WarmupSubmitRequest {
        application_id: "app-a".to_owned(),
        chain: chain(),
        dataset_key: DatasetKey::evm_logs(),
        selector: selector(),
        range_kind: LedgerRangeKind::Block,
        start: 1,
        end,
        mode: WarmupTaskMode::FixedRange,
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

fn log_rows(logs: Vec<LogRecord>) -> DatasetRows {
    DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(logs)).unwrap()
}

#[derive(Clone)]
struct FixtureAdapter {
    inner: Arc<Mutex<FixtureState>>,
}

#[derive(Default)]
struct FixtureState {
    safe_height: u64,
    logs: Vec<LogRecord>,
    fetches: Vec<LedgerRange>,
}

impl FixtureAdapter {
    fn new(safe_height: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FixtureState {
                safe_height,
                ..FixtureState::default()
            })),
        }
    }

    fn with_logs(self, logs: Vec<LogRecord>) -> Self {
        self.inner.lock().unwrap().logs = logs;
        self
    }

    fn fetches(&self) -> Vec<LedgerRange> {
        self.inner.lock().unwrap().fetches.clone()
    }
}

impl ChainAdapter for FixtureAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(chain()).with_dataset_capability(
            DatasetCapability::new(DatasetKey::evm_logs())
                .with_selector(SelectorKind::EvmLogs)
                .with_range(LedgerRangeKind::Block)
                .with_max_range_len(1_000)
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

#[derive(Clone)]
struct ToggleFailStorage {
    inner: LocalStorage,
    fail_writes: Arc<AtomicBool>,
}

impl ToggleFailStorage {
    fn new(inner: LocalStorage) -> Self {
        Self {
            inner,
            fail_writes: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }
}

impl StorageRepository for ToggleFailStorage {
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
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "injected durable write failure",
            ));
        }
        self.inner.write_rows(request)
    }
}
