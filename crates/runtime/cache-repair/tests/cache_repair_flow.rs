use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
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
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange,
    LedgerRangeKind, LogFilter, LogRecord, NetworkId, QueryRows,
};
use datalens_storage::{LocalObjectStore, LocalStorage, StorageWriteRequest};

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
        Ok(ChainHeight::block(20).with_finality(FinalityLevel::Safe))
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
            finality_level: FinalityLevel::Safe,
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
