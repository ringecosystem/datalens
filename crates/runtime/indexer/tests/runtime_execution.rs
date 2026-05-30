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
    DatasetKey, DatasetRows, EvmReceipt, EvmTransaction, LedgerRange, LogRecord, NetworkId,
    QueryRows,
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

#[test]
fn test_runtime_all_supported_backfills_full_evm_durable_datasets() {
    let storage = LocalStorage::new(temp_storage_root("full-evm-datasets"));
    let source = FixtureAdapter::default()
        .with_blocks(vec![block(1), block(2), block(3)])
        .with_transactions(vec![
            transaction(1, 0),
            transaction(2, 0),
            transaction(3, 0),
        ])
        .with_receipts(vec![receipt(1, 0), receipt(2, 0), receipt(3, 0)])
        .with_logs(vec![log(1, 0), log(2, 0), log(3, 0)]);

    let result = runtime(
        source.clone(),
        storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .run(full_evm_job(1, 3, IndexRunMode::Backfill))
    .expect("backfill all EVM datasets");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(result.accounting.chunks_planned, 8);
    assert_eq!(result.accounting.rows_written, 12);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 2)),
            SourceCall::Blocks(BlockRange::expect_new(3, 3)),
            SourceCall::Transactions(BlockRange::expect_new(1, 2)),
            SourceCall::Transactions(BlockRange::expect_new(3, 3)),
            SourceCall::Receipts(BlockRange::expect_new(1, 2)),
            SourceCall::Receipts(BlockRange::expect_new(3, 3)),
            SourceCall::Logs(BlockRange::expect_new(1, 2)),
            SourceCall::Logs(BlockRange::expect_new(3, 3)),
        ]
    );

    for dataset_key in [
        DatasetKey::evm_blocks(),
        DatasetKey::evm_transactions(),
        DatasetKey::evm_receipts(),
        DatasetKey::evm_logs(),
    ] {
        assert_eq!(
            storage
                .covered_ranges(
                    &ethereum_identity(),
                    &dataset_key,
                    &DatasetSelector::all(),
                    LedgerRange::blocks(1, 3).expect("range"),
                )
                .expect("coverage"),
            vec![LedgerRange::blocks(1, 3).expect("range")]
        );
        assert_eq!(
            storage
                .read_rows(
                    &ethereum_identity(),
                    &dataset_key,
                    &DatasetSelector::all(),
                    LedgerRange::blocks(1, 3).expect("range"),
                )
                .expect("read indexed dataset")
                .row_count(),
            3
        );
    }

    let rerun = runtime(
        source.clone(),
        storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .run(full_evm_job(1, 3, IndexRunMode::Backfill))
    .expect("rerun");
    assert_eq!(rerun.accounting.chunks_planned, 0);
    assert_eq!(rerun.accounting.chunks_skipped, 4);
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
            staging: Default::default(),
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

fn full_evm_job(start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    IndexJob {
        id: IndexJobId::new("fixture-full-evm-job").expect("job id"),
        application: ApplicationIdentity::named("indexer"),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: IndexDatasetSelection::AllSupported,
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
            staging: Default::default(),
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

fn transaction(block_number: u64, transaction_index: u64) -> EvmTransaction {
    EvmTransaction {
        hash: format!("0xtx-{block_number}-{transaction_index}"),
        block_number,
        block_hash: format!("0x{block_number:064x}"),
        transaction_index,
        from: "0x1111111111111111111111111111111111111111".to_owned(),
        to: Some("0x2222222222222222222222222222222222222222".to_owned()),
        value: "0x1".to_owned(),
        input: "0x".to_owned(),
        nonce: transaction_index,
        gas: 21_000,
        gas_price: Some("0x3b9aca00".to_owned()),
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
        transaction_type: Some("0x2".to_owned()),
    }
}

fn receipt(block_number: u64, transaction_index: u64) -> EvmReceipt {
    EvmReceipt {
        transaction_hash: format!("0xtx-{block_number}-{transaction_index}"),
        block_number,
        block_hash: format!("0x{block_number:064x}"),
        transaction_index,
        status: Some(1),
        gas_used: 21_000,
        cumulative_gas_used: 21_000,
        effective_gas_price: Some("0x3b9aca00".to_owned()),
        contract_address: None,
        logs_bloom: Some(format!("0x{}", "0".repeat(512))),
    }
}

fn log(block_number: u64, log_index: u64) -> LogRecord {
    LogRecord::try_new(
        block_number,
        format!("0x{block_number:064x}"),
        format!("0xtx-{block_number}-0"),
        0,
        log_index,
        "0x3333333333333333333333333333333333333333",
        Vec::new(),
        "0x".to_owned(),
        false,
    )
    .expect("log")
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
    Transactions(BlockRange),
    Receipts(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone)]
struct FixtureAdapter {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    transactions: Arc<Mutex<Vec<EvmTransaction>>>,
    receipts: Arc<Mutex<Vec<EvmReceipt>>>,
    logs: Arc<Mutex<Vec<LogRecord>>>,
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
            transactions: Arc::new(Mutex::new(Vec::new())),
            receipts: Arc::new(Mutex::new(Vec::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
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

    fn with_transactions(self, transactions: Vec<EvmTransaction>) -> Self {
        *self.transactions.lock().expect("transactions") = transactions;
        self
    }

    fn with_receipts(self, receipts: Vec<EvmReceipt>) -> Self {
        *self.receipts.lock().expect("receipts") = receipts;
        self
    }

    fn with_logs(self, logs: Vec<LogRecord>) -> Self {
        *self.logs.lock().expect("logs") = logs;
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
        let dataset = |dataset| {
            DatasetCapability::new(dataset)
                .with_selector(SelectorKind::All)
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(2)
                .with_empty_coverage(true)
                .with_safe_height(true)
                .with_finalized_height(true)
                .with_range_split(true)
        };
        AdapterCapabilities::new(ethereum_identity())
            .with_dataset_capability(dataset(Dataset::Blocks.into()))
            .with_dataset_capability(dataset(DatasetKey::evm_transactions()))
            .with_dataset_capability(dataset(DatasetKey::evm_receipts()))
            .with_dataset_capability(dataset(Dataset::Logs.into()))
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
            calls.push(match request.dataset_key.as_str() {
                "evm.blocks" => SourceCall::Blocks(range),
                "evm.transactions" => SourceCall::Transactions(range),
                "evm.receipts" => SourceCall::Receipts(range),
                "evm.logs" => SourceCall::Logs(range),
                _ => SourceCall::Blocks(range),
            });
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
        let rows = match request.dataset_key.as_str() {
            "evm.blocks" => QueryRows::EvmBlocks(
                self.blocks
                    .lock()
                    .expect("blocks")
                    .iter()
                    .filter(|block| request.range.contains(block.number))
                    .cloned()
                    .collect(),
            ),
            "evm.transactions" => QueryRows::EvmTransactions(
                self.transactions
                    .lock()
                    .expect("transactions")
                    .iter()
                    .filter(|transaction| request.range.contains(transaction.block_number))
                    .cloned()
                    .collect(),
            ),
            "evm.receipts" => QueryRows::EvmReceipts(
                self.receipts
                    .lock()
                    .expect("receipts")
                    .iter()
                    .filter(|receipt| request.range.contains(receipt.block_number))
                    .cloned()
                    .collect(),
            ),
            "evm.logs" => QueryRows::EvmLogs(
                self.logs
                    .lock()
                    .expect("logs")
                    .iter()
                    .filter(|log| request.range.contains(log.block_number))
                    .cloned()
                    .collect(),
            ),
            _ => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "fixture dataset is not supported",
                ));
            }
        };
        ChainFetchResponse::try_new(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
            rows,
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
