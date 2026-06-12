use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_cache_repair::{
    CacheRepairChunkPolicy, CacheRepairFinality, CacheRepairRunStatus, CacheRepairRuntime,
    CacheRepairSubmitRequest, CacheRepairTaskPool, CacheRepairTaskState, LocalCacheRepairRegistry,
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

#[derive(Clone)]
struct FixtureAdapter {
    chain: ChainIdentity,
    result: Arc<Mutex<Result<Vec<LogRecord>, DatalensError>>>,
}

impl FixtureAdapter {
    fn new(chain: ChainIdentity, result: Result<Vec<LogRecord>, DatalensError>) -> Self {
        Self {
            chain,
            result: Arc::new(Mutex::new(result)),
        }
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
        let logs = self
            .result
            .lock()
            .map_err(|_| DatalensError::internal("fixture lock poisoned"))?
            .clone()?;
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
        range_kind: LedgerRangeKind::Block,
        start: 11,
        end: 11,
        finality: CacheRepairFinality::Safe,
        chunk_policy: CacheRepairChunkPolicy { max_range_len: 1 },
        reason: "test repair".to_owned(),
    }
}

fn selector() -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec![address()],
        topics: vec![Some(vec![topic()])],
    })
    .expect("selector")
}

fn log_record(block_number: u64, log_index: u64) -> LogRecord {
    LogRecord::try_new(
        block_number,
        format!("0x{:064x}", block_number),
        format!("0x{:064x}", block_number + 1_000),
        0,
        log_index,
        address(),
        vec![topic()],
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
