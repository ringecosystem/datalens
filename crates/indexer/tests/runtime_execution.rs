use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityLevel, HeightRangeKind, ProviderDiagnostics,
    SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, DatasetRows, LedgerRange, NetworkId, QueryRows,
};
use datalens_indexer::*;
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_storage::{
    CacheOutcome, FillOutcome, LocalObjectStore, LocalStorage, QueryOutcome, UsageLedgerRepository,
    UsageLedgerStore,
};
use datalens_writer::DurableWriterConfig;

#[test]
fn test_runtime_backfill_writes_chunks_updates_cursor_and_ledger() {
    let root = temp_storage_root("backfill");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let cursor_store = InMemoryIndexCursorStore::default();
    let source = FixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);
    let runtime = runtime(source.clone(), storage.clone(), cursor_store.clone())
        .with_usage_ledger(ledger.clone())
        .with_metrics(MetricsRecorder::new().expect("metrics recorder"));

    let result = runtime
        .run(block_job(1, 3, IndexRunMode::Backfill))
        .expect("run");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(result.accounting.chunks_planned, 2);
    assert_eq!(result.accounting.chunks_fetched, 2);
    assert_eq!(result.accounting.chunks_written, 2);
    assert_eq!(result.accounting.provider_calls, 2);
    assert_eq!(result.accounting.rows_written, 3);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 2)),
            SourceCall::Blocks(BlockRange::expect_new(3, 3)),
        ]
    );

    let cursor = cursor_store
        .load(&job_id())
        .expect("load cursor")
        .expect("cursor");
    assert_eq!(cursor.next_height, 4);
    assert_eq!(cursor.completed_chunks.len(), 2);
    assert!(!cursor.is_durable_coverage());
    assert_eq!(
        storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(1, 3).expect("range"),
            )
            .expect("coverage"),
        vec![LedgerRange::blocks(1, 3).expect("range")]
    );

    let events = ledger.read_application("indexer").expect("ledger");
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|event| event.query_outcome == QueryOutcome::Filled)
    );
    assert!(
        events
            .iter()
            .all(|event| event.cache_outcome == CacheOutcome::Miss)
    );
    assert!(
        events
            .iter()
            .all(|event| event.fill_outcome == FillOutcome::Written)
    );
}

#[test]
fn test_runtime_resume_skips_completed_cursor_chunks_after_restart() {
    let storage = LocalStorage::new(temp_storage_root("resume"));
    let cursor_store = InMemoryIndexCursorStore::default();
    let first_source = FixtureAdapter::default()
        .with_blocks(vec![block(1), block(2), block(3)])
        .with_fail_after_calls(1, DatalensErrorKind::ProviderFailure);
    let first = runtime(first_source.clone(), storage.clone(), cursor_store.clone());

    let error = first
        .run(block_job(1, 3, IndexRunMode::Backfill))
        .expect_err("first run fails after one durable chunk");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert_eq!(
        first_source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 2)),
            SourceCall::Blocks(BlockRange::expect_new(3, 3)),
        ]
    );

    let resumed_source = FixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);
    let resumed = runtime(
        resumed_source.clone(),
        storage.clone(),
        cursor_store.clone(),
    );
    let result = resumed
        .run(block_job(1, 3, IndexRunMode::Resume))
        .expect("resume");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(
        resumed_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(3, 3))]
    );
    assert_eq!(result.accounting.chunks_skipped, 1);
    assert_eq!(
        storage
            .covered_ranges(
                &ethereum_identity(),
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(1, 3).expect("range"),
            )
            .expect("coverage"),
        vec![LedgerRange::blocks(1, 3).expect("range")]
    );
}

#[test]
fn test_runtime_repair_fills_manifest_coverage_gaps_only() {
    let storage = LocalStorage::new(temp_storage_root("repair"));
    seed_blocks(&storage, 1, 2, vec![block(1), block(2)]);
    seed_blocks(&storage, 5, 5, vec![block(5)]);
    let source = FixtureAdapter::default().with_blocks(vec![
        block(1),
        block(2),
        block(3),
        block(4),
        block(5),
    ]);
    let result = runtime(
        source.clone(),
        storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .run(block_job(1, 5, IndexRunMode::Repair))
    .expect("repair");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(3, 4))]
    );
    assert_eq!(result.accounting.chunks_skipped, 2);
    assert_eq!(result.accounting.chunks_written, 1);
}

#[test]
fn test_runtime_verify_reads_coverage_without_fetch_or_write() {
    let storage = LocalStorage::new(temp_storage_root("verify"));
    seed_blocks(&storage, 1, 2, vec![block(1), block(2)]);
    let source = FixtureAdapter::default().with_blocks(vec![block(1), block(2)]);

    let result = runtime(
        source.clone(),
        storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .run(block_job(1, 2, IndexRunMode::Verify))
    .expect("verify");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert_eq!(result.accounting.chunks_written, 0);
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 1);
}

#[test]
fn test_runtime_retries_transient_provider_failure_with_bound() {
    let storage = LocalStorage::new(temp_storage_root("retry"));
    let source = FixtureAdapter::default()
        .with_blocks(vec![block(1)])
        .with_transient_failures(2, DatalensErrorKind::ProviderTimeout);
    let mut job = block_job(1, 1, IndexRunMode::Backfill);
    job.retry_policy = IndexRetryPolicy {
        max_attempts: 3,
        initial_backoff_ms: 0,
        max_backoff_ms: 0,
    };

    let result = runtime(source.clone(), storage, InMemoryIndexCursorStore::default())
        .run(job)
        .expect("retry succeeds");

    assert_eq!(result.accounting.retries, 2);
    assert_eq!(result.accounting.provider_calls, 3);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 1)),
            SourceCall::Blocks(BlockRange::expect_new(1, 1)),
            SourceCall::Blocks(BlockRange::expect_new(1, 1)),
        ]
    );
}

#[test]
fn test_runtime_splits_provider_limit_chunks_and_fails_singletons_clearly() {
    let storage = LocalStorage::new(temp_storage_root("provider-limit"));
    let source = FixtureAdapter::default()
        .with_blocks(vec![block(1), block(2)])
        .with_provider_limit_for_ranges_larger_than(1);
    let mut job = block_job(1, 2, IndexRunMode::Backfill);
    job.runtime_config.max_chunk_len = 2;

    let result = runtime(source.clone(), storage, InMemoryIndexCursorStore::default())
        .run(job)
        .expect("provider limit split succeeds");

    assert_eq!(result.accounting.provider_limit_splits, 1);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 2)),
            SourceCall::Blocks(BlockRange::expect_new(1, 1)),
            SourceCall::Blocks(BlockRange::expect_new(2, 2)),
        ]
    );

    let failing = FixtureAdapter::default().with_provider_limit_for_ranges_larger_than(0);
    let error = runtime(
        failing,
        LocalStorage::new(temp_storage_root("provider-limit-single")),
        InMemoryIndexCursorStore::default(),
    )
    .run(block_job(1, 1, IndexRunMode::Backfill))
    .expect_err("singleton provider limit fails clearly");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(error.message.contains("range 1-1"));
}

#[test]
fn test_runtime_enforces_durable_finality_before_fetch_and_write() {
    let root = temp_storage_root("finality");
    let storage = LocalStorage::new(&root);
    let source = FixtureAdapter::default()
        .with_blocks(vec![block(1), block(2)])
        .with_safe_height(ChainHeight::block(1).with_finality(FinalityLevel::Safe));

    let result = runtime(
        source.clone(),
        storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .run(block_job(1, 2, IndexRunMode::Backfill))
    .expect("capped run");

    assert_eq!(result.accounting.finality_capped_ranges, 1);
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
                LedgerRange::blocks(1, 2).expect("range"),
            )
            .expect("coverage"),
        vec![LedgerRange::blocks(1, 1).expect("range")]
    );
}

fn runtime(
    source: FixtureAdapter,
    storage: LocalStorage,
    cursor_store: InMemoryIndexCursorStore,
) -> IndexRuntime<FixtureAdapter, LocalStorage, InMemoryIndexCursorStore> {
    IndexRuntime::new(
        source,
        storage,
        cursor_store,
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
        },
    )
}

fn block_job(start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    IndexJob {
        id: job_id(),
        application: ApplicationIdentity::named("indexer"),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: IndexDatasetSelection::Selected(vec![IndexDatasetRequest {
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
        }]),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 2 },
        run_mode,
        retry_policy: IndexRetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
    }
}

fn seed_blocks(storage: &LocalStorage, start: u64, end: u64, blocks: Vec<BlockHeader>) {
    datalens_writer::DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
        },
    )
    .write(datalens_writer::DurableWriteRequest {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        finality_level: FinalityLevel::Safe,
        segments: vec![datalens_writer::DurableWriteSegment {
            range: LedgerRange::blocks(start, end).expect("range"),
            rows: DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(blocks))
                .expect("rows"),
        }],
    })
    .expect("seed blocks");
}

fn job_id() -> IndexJobId {
    IndexJobId::new("fixture-job").expect("job id")
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("chain")
}

fn block(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        hash: format!("0x{number:064x}"),
        parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
        timestamp: number * 10,
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-indexer-runtime-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
    root
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceCall {
    Blocks(BlockRange),
}

#[derive(Clone)]
struct FixtureAdapter {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    safe_height: Arc<Mutex<ChainHeight>>,
    transient_failures: Arc<Mutex<Vec<DatalensErrorKind>>>,
    fail_after_calls: Arc<Mutex<Option<(usize, DatalensErrorKind)>>>,
    provider_limit_len: Arc<Mutex<Option<u64>>>,
}

impl Default for FixtureAdapter {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            safe_height: Arc::new(Mutex::new(
                ChainHeight::block(100).with_finality(FinalityLevel::Safe),
            )),
            transient_failures: Arc::new(Mutex::new(Vec::new())),
            fail_after_calls: Arc::new(Mutex::new(None)),
            provider_limit_len: Arc::new(Mutex::new(None)),
        }
    }
}

impl FixtureAdapter {
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks") = blocks;
        self
    }

    fn with_safe_height(self, height: ChainHeight) -> Self {
        *self.safe_height.lock().expect("safe height") = height;
        self
    }

    fn with_transient_failures(self, count: usize, kind: DatalensErrorKind) -> Self {
        *self.transient_failures.lock().expect("transient failures") = vec![kind; count];
        self
    }

    fn with_fail_after_calls(self, calls: usize, kind: DatalensErrorKind) -> Self {
        *self.fail_after_calls.lock().expect("fail after") = Some((calls, kind));
        self
    }

    fn with_provider_limit_for_ranges_larger_than(self, max_len: u64) -> Self {
        *self.provider_limit_len.lock().expect("provider limit") = Some(max_len);
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls").clone()
    }
}

impl ChainAdapter for FixtureAdapter {
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
        Ok(ChainHeight::block(100))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(self.safe_height.lock().expect("safe height").clone())
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(self.safe_height.lock().expect("safe height").clone())
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = request.range.block_range().expect("block range");
        let call_count = {
            let mut calls = self.calls.lock().expect("calls");
            calls.push(SourceCall::Blocks(range));
            calls.len()
        };
        if let Some((fail_after, kind)) = self.fail_after_calls.lock().expect("fail after").clone()
            && call_count > fail_after
        {
            return Err(DatalensError::new(
                kind,
                "injected failure after call limit",
            ));
        }
        if let Some(kind) = self.transient_failures.lock().expect("failures").pop() {
            return Err(DatalensError::new(kind, "injected transient failure"));
        }
        if let Some(max_len) = *self.provider_limit_len.lock().expect("provider limit")
            && request.range.len() > u128::from(max_len)
        {
            return Err(DatalensError::provider_limit("fixture provider limit"));
        }
        let rows = self
            .blocks
            .lock()
            .expect("blocks")
            .iter()
            .filter(|block| request.range.contains(block.number))
            .cloned()
            .collect();
        ChainFetchResponse::try_new(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
            QueryRows::EvmBlocks(rows),
        )
        .map(|response| {
            response.with_provider_diagnostics(ProviderDiagnostics {
                calls: 1,
                rows_scanned: 0,
                warnings: Vec::new(),
            })
        })
    }
}
