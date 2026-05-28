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
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, Dataset, DatasetKey, DatasetRows,
    EvmReceipt, EvmTransaction, LedgerRange, LogRecord, NetworkId, QueryRows,
};
use datalens_indexer::{
    InMemoryIndexCursorStore, IndexDatasetRequest, IndexFinalityRequirement, IndexJob, IndexJobId,
    IndexRunMode, IndexRunStatus, IndexRuntime, IndexRuntimeConfig,
};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_solana::{
    SolanaAdapter, SolanaFixtureRpc, solana_all_selector, solana_program_selector,
};
use datalens_storage::{
    CacheOutcome, FillOutcome, LocalObjectStore, LocalStorage, QueryOutcome, UsageLedgerRepository,
    UsageLedgerStore,
};
use datalens_tron::{TronAdapter, TronFixtureProviderRpc, tron_all_selector};
use datalens_writer::DurableWriterConfig;
use serde_json::Value;

const APPLICATION: &str = "cross-chain-indexer";
const SOLANA_PROGRAM: &str = "program1111111111111111111111111111111111";

#[test]
fn test_evm_full_indexing_backfills_queries_verifies_repairs_resumes_and_reruns() {
    let root = temp_storage_root("evm");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let metrics = MetricsRecorder::new().expect("metrics recorder");
    let cursor_store = InMemoryIndexCursorStore::default();
    let source = EvmFixtureAdapter::default()
        .with_blocks(vec![evm_block(1), evm_block(2), evm_block(3)])
        .with_transactions(vec![
            evm_transaction(1),
            evm_transaction(2),
            evm_transaction(3),
        ])
        .with_receipts(vec![evm_receipt(1), evm_receipt(2), evm_receipt(3)])
        .with_logs(vec![evm_log(1), evm_log(2), evm_log(3)]);
    let runtime = make_runtime(source.clone(), storage.clone(), cursor_store.clone())
        .with_usage_ledger(ledger.clone())
        .with_metrics(metrics.clone());

    let backfill = runtime
        .run(evm_job("evm-backfill", 1, 3, IndexRunMode::Backfill))
        .expect("backfill");

    assert_eq!(backfill.status, IndexRunStatus::Completed);
    assert_eq!(backfill.accounting.chunks_written, 8);
    assert_eq!(backfill.accounting.rows_written, 12);
    assert_queryable_counts(
        &storage,
        &ethereum_identity(),
        LedgerRange::blocks(1, 3).expect("range"),
        &[
            (DatasetKey::evm_blocks(), DatasetSelector::all(), 3),
            (DatasetKey::evm_transactions(), DatasetSelector::all(), 3),
            (DatasetKey::evm_receipts(), DatasetSelector::all(), 3),
            (DatasetKey::evm_logs(), DatasetSelector::all(), 3),
        ],
    );
    assert_ledger_application_attribution(&ledger, 8);
    let metrics_text = metrics.encode().expect("metrics encode");
    assert!(metrics_text.contains("datalens_fill_total"));
    assert!(metrics_text.contains("cross-chain-indexer"));
    assert!(metrics_text.contains("ethereum"));

    let verify = runtime
        .run(evm_job("evm-verify", 1, 3, IndexRunMode::Verify))
        .expect("verify");
    assert_eq!(verify.accounting.chunks_written, 0);
    assert_eq!(verify.accounting.chunks_fetched, 4);

    let rerun = runtime
        .run(evm_job("evm-rerun", 1, 3, IndexRunMode::Backfill))
        .expect("idempotent rerun");
    assert_eq!(rerun.accounting.chunks_planned, 0);
    assert_eq!(rerun.accounting.chunks_skipped, 4);

    let repair_storage = LocalStorage::new(temp_storage_root("evm-repair"));
    seed_evm_blocks(&repair_storage, 1, 1, vec![evm_block(1)]);
    seed_evm_blocks(&repair_storage, 3, 3, vec![evm_block(3)]);
    let repair_source =
        EvmFixtureAdapter::default().with_blocks(vec![evm_block(1), evm_block(2), evm_block(3)]);
    let repair = make_runtime(
        repair_source.clone(),
        repair_storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .run(evm_blocks_job("evm-repair", 1, 3, IndexRunMode::Repair))
    .expect("repair");
    assert_eq!(repair.accounting.chunks_written, 1);
    assert_eq!(
        repair_source.calls(),
        vec![EvmSourceCall::Blocks(
            LedgerRange::blocks(2, 2).expect("range")
        )]
    );

    let resume_storage = LocalStorage::new(temp_storage_root("evm-resume"));
    let resume_cursor_store = InMemoryIndexCursorStore::default();
    let failing_source = EvmFixtureAdapter::default()
        .with_blocks(vec![evm_block(1), evm_block(2), evm_block(3)])
        .with_fail_after_calls(1);
    let first_error = make_runtime(
        failing_source,
        resume_storage.clone(),
        resume_cursor_store.clone(),
    )
    .run(evm_blocks_job("evm-resume", 1, 3, IndexRunMode::Backfill))
    .expect_err("first resume attempt fails after checkpoint");
    assert!(first_error.is_retryable());
    let resumed_source =
        EvmFixtureAdapter::default().with_blocks(vec![evm_block(1), evm_block(2), evm_block(3)]);
    let resumed = make_runtime(resumed_source.clone(), resume_storage, resume_cursor_store)
        .run(evm_blocks_job("evm-resume", 1, 3, IndexRunMode::Resume))
        .expect("resume");
    assert_eq!(resumed.accounting.chunks_skipped, 1);
    assert_eq!(
        resumed_source.calls(),
        vec![EvmSourceCall::Blocks(
            LedgerRange::blocks(3, 3).expect("range")
        )]
    );
}

#[test]
fn test_solana_full_indexing_backfills_skipped_slots_queries_verifies_and_reruns() {
    let root = temp_storage_root("solana");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let metrics = MetricsRecorder::new().expect("metrics recorder");
    let adapter = SolanaAdapter::with_provider_limits(SolanaFixtureRpc, 2);
    let chain = adapter.capabilities().chain().clone();
    let program_selector = solana_program_selector(SOLANA_PROGRAM).expect("program selector");
    let runtime = make_runtime(
        adapter.clone(),
        storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .with_usage_ledger(ledger.clone())
    .with_metrics(metrics.clone());

    let backfill = runtime
        .run(solana_job(
            "solana-backfill",
            10,
            12,
            IndexRunMode::Backfill,
            program_selector.clone(),
        ))
        .expect("backfill");

    assert_eq!(backfill.status, IndexRunStatus::Completed);
    assert_eq!(backfill.accounting.chunks_written, 6);
    assert_eq!(backfill.accounting.rows_written, 5);
    assert_queryable_counts(
        &storage,
        &chain,
        LedgerRange::slots(10, 12).expect("range"),
        &[
            (
                DatasetKey::solana_slots(),
                solana_all_selector().expect("selector"),
                2,
            ),
            (
                DatasetKey::solana_transactions(),
                program_selector.clone(),
                1,
            ),
            (
                DatasetKey::solana_instructions(),
                program_selector.clone(),
                2,
            ),
        ],
    );
    assert_ledger_application_attribution(&ledger, 6);
    let metrics_text = metrics.encode().expect("metrics encode");
    assert!(metrics_text.contains("solana-mainnet-beta"));
    assert!(metrics_text.contains("solana.transactions"));

    let verify = runtime
        .run(solana_job(
            "solana-verify",
            10,
            12,
            IndexRunMode::Verify,
            program_selector.clone(),
        ))
        .expect("verify");
    assert_eq!(verify.accounting.chunks_written, 0);
    assert_eq!(verify.accounting.chunks_fetched, 3);

    let rerun = runtime
        .run(solana_job(
            "solana-rerun",
            10,
            12,
            IndexRunMode::Backfill,
            program_selector,
        ))
        .expect("idempotent rerun");
    assert_eq!(rerun.accounting.chunks_planned, 0);
    assert_eq!(rerun.accounting.chunks_skipped, 3);
}

#[test]
fn test_tron_full_indexing_backfills_supported_durable_datasets_queries_verifies_and_reruns() {
    let root = temp_storage_root("tron");
    let storage = LocalStorage::new(&root);
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(&root));
    let metrics = MetricsRecorder::new().expect("metrics recorder");
    let adapter = TronAdapter::with_provider_limits(TronFixtureProviderRpc, 2);
    let chain = adapter.capabilities().chain().clone();
    let runtime = make_runtime(
        adapter.clone(),
        storage.clone(),
        InMemoryIndexCursorStore::default(),
    )
    .with_usage_ledger(ledger.clone())
    .with_metrics(metrics.clone());

    let backfill = runtime
        .run(tron_job("tron-backfill", 10, 12, IndexRunMode::Backfill))
        .expect("backfill");

    assert_eq!(backfill.status, IndexRunStatus::Completed);
    assert_eq!(backfill.accounting.chunks_written, 2);
    assert_eq!(backfill.accounting.rows_written, 3);
    assert_queryable_counts(
        &storage,
        &chain,
        LedgerRange::blocks(10, 12).expect("range"),
        &[(
            DatasetKey::tron_blocks(),
            tron_all_selector().expect("selector"),
            3,
        )],
    );
    assert_ledger_application_attribution(&ledger, 2);
    let metrics_text = metrics.encode().expect("metrics encode");
    assert!(metrics_text.contains("tron-mainnet"));
    assert!(metrics_text.contains("tron.blocks"));

    let verify = runtime
        .run(tron_job("tron-verify", 10, 12, IndexRunMode::Verify))
        .expect("verify");
    assert_eq!(verify.accounting.chunks_written, 0);
    assert_eq!(verify.accounting.chunks_fetched, 1);

    let rerun = runtime
        .run(tron_job("tron-rerun", 10, 12, IndexRunMode::Backfill))
        .expect("idempotent rerun");
    assert_eq!(rerun.accounting.chunks_planned, 0);
    assert_eq!(rerun.accounting.chunks_skipped, 1);
}

fn make_runtime<A>(
    adapter: A,
    storage: LocalStorage,
    cursor_store: InMemoryIndexCursorStore,
) -> IndexRuntime<A, LocalStorage, InMemoryIndexCursorStore>
where
    A: ChainAdapter,
{
    IndexRuntime::new(
        adapter,
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

fn assert_queryable_counts(
    storage: &LocalStorage,
    chain: &ChainIdentity,
    range: LedgerRange,
    datasets: &[(DatasetKey, DatasetSelector, usize)],
) {
    for (dataset_key, selector, expected_count) in datasets {
        assert_eq!(
            storage
                .covered_ranges(chain, dataset_key, selector, range.clone())
                .expect("coverage"),
            vec![range.clone()],
            "coverage for {}",
            dataset_key.as_str()
        );
        let rows = storage
            .read_rows(chain, dataset_key, selector, range.clone())
            .expect("read durable rows");
        assert_eq!(
            rows.row_count(),
            *expected_count,
            "row count for {}",
            dataset_key.as_str()
        );
        if dataset_key == &DatasetKey::solana_slots() {
            let QueryRows::AdapterJson { rows, .. } = rows.rows() else {
                panic!("expected Solana adapter JSON rows");
            };
            assert_eq!(json_u64s(rows, "slot"), vec![10, 12]);
        }
    }
}

fn assert_ledger_application_attribution(ledger: &UsageLedgerStore<LocalObjectStore>, len: usize) {
    let events = ledger
        .read_application(APPLICATION)
        .expect("ledger application");
    assert_eq!(events.len(), len);
    assert!(
        events
            .iter()
            .all(|event| event.application_id == APPLICATION)
    );
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
    assert!(events.iter().all(|event| {
        matches!(
            event.fill_outcome,
            FillOutcome::Written | FillOutcome::EmptyCoverageRecorded
        )
    }));
}

fn json_u64s(rows: &[Value], field: &str) -> Vec<u64> {
    rows.iter()
        .map(|row| row[field].as_u64().expect("u64 field"))
        .collect()
}

fn evm_job(id: &str, start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    let all = DatasetSelector::all();
    IndexJob {
        id: IndexJobId::new(id).expect("job id"),
        application: ApplicationIdentity::named(APPLICATION),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: datalens_indexer::IndexDatasetSelection::Selected(vec![
            IndexDatasetRequest {
                dataset_key: DatasetKey::evm_blocks(),
                selector: all.clone(),
            },
            IndexDatasetRequest {
                dataset_key: DatasetKey::evm_transactions(),
                selector: all.clone(),
            },
            IndexDatasetRequest {
                dataset_key: DatasetKey::evm_receipts(),
                selector: all.clone(),
            },
            IndexDatasetRequest {
                dataset_key: DatasetKey::evm_logs(),
                selector: all,
            },
        ]),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 2 },
        run_mode,
        retry_policy: no_retry(),
    }
}

fn evm_blocks_job(id: &str, start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    IndexJob {
        id: IndexJobId::new(id).expect("job id"),
        application: ApplicationIdentity::named(APPLICATION),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: datalens_indexer::IndexDatasetSelection::Selected(vec![
            IndexDatasetRequest {
                dataset_key: DatasetKey::evm_blocks(),
                selector: DatasetSelector::all(),
            },
        ]),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 2 },
        run_mode,
        retry_policy: no_retry(),
    }
}

fn solana_job(
    id: &str,
    start: u64,
    end: u64,
    run_mode: IndexRunMode,
    program_selector: DatasetSelector,
) -> IndexJob {
    IndexJob {
        id: IndexJobId::new(id).expect("job id"),
        application: ApplicationIdentity::named(APPLICATION),
        chain: SolanaAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        range: LedgerRange::slots(start, end).expect("range"),
        dataset_selection: datalens_indexer::IndexDatasetSelection::Selected(vec![
            IndexDatasetRequest {
                dataset_key: DatasetKey::solana_slots(),
                selector: solana_all_selector().expect("selector"),
            },
            IndexDatasetRequest {
                dataset_key: DatasetKey::solana_transactions(),
                selector: program_selector.clone(),
            },
            IndexDatasetRequest {
                dataset_key: DatasetKey::solana_instructions(),
                selector: program_selector,
            },
        ]),
        finality_requirement: IndexFinalityRequirement::Finalized,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 2 },
        run_mode,
        retry_policy: no_retry(),
    }
}

fn tron_job(id: &str, start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    IndexJob {
        id: IndexJobId::new(id).expect("job id"),
        application: ApplicationIdentity::named(APPLICATION),
        chain: TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: datalens_indexer::IndexDatasetSelection::Selected(vec![
            IndexDatasetRequest {
                dataset_key: DatasetKey::tron_blocks(),
                selector: tron_all_selector().expect("selector"),
            },
        ]),
        finality_requirement: IndexFinalityRequirement::Finalized,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 2 },
        run_mode,
        retry_policy: no_retry(),
    }
}

fn no_retry() -> datalens_indexer::IndexRetryPolicy {
    datalens_indexer::IndexRetryPolicy {
        max_attempts: 1,
        initial_backoff_ms: 0,
        max_backoff_ms: 0,
    }
}

fn seed_evm_blocks(storage: &LocalStorage, start: u64, end: u64, blocks: Vec<BlockHeader>) {
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

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("chain")
}

fn evm_block(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        hash: format!("0x{number:064x}"),
        parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
        timestamp: number * 10,
    }
}

fn evm_transaction(block_number: u64) -> EvmTransaction {
    EvmTransaction {
        hash: format!("0xtx-{block_number}"),
        block_number,
        block_hash: format!("0x{block_number:064x}"),
        transaction_index: 0,
        from: "0x1111111111111111111111111111111111111111".to_owned(),
        to: Some("0x2222222222222222222222222222222222222222".to_owned()),
        value: "0x1".to_owned(),
        input: "0x".to_owned(),
        nonce: block_number,
        gas: 21_000,
        gas_price: Some("0x3b9aca00".to_owned()),
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
        transaction_type: Some("0x2".to_owned()),
    }
}

fn evm_receipt(block_number: u64) -> EvmReceipt {
    EvmReceipt {
        transaction_hash: format!("0xtx-{block_number}"),
        block_number,
        block_hash: format!("0x{block_number:064x}"),
        transaction_index: 0,
        status: Some(1),
        gas_used: 21_000,
        cumulative_gas_used: 21_000,
        effective_gas_price: Some("0x3b9aca00".to_owned()),
        contract_address: None,
        logs_bloom: Some(format!("0x{}", "0".repeat(512))),
    }
}

fn evm_log(block_number: u64) -> LogRecord {
    LogRecord::try_new(
        block_number,
        format!("0x{block_number:064x}"),
        format!("0xtx-{block_number}"),
        0,
        0,
        "0x3333333333333333333333333333333333333333",
        Vec::new(),
        "0x".to_owned(),
        false,
    )
    .expect("log")
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EvmSourceCall {
    Blocks(LedgerRange),
    Transactions(LedgerRange),
    Receipts(LedgerRange),
    Logs(LedgerRange),
}

#[derive(Clone)]
struct EvmFixtureAdapter {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    transactions: Arc<Mutex<Vec<EvmTransaction>>>,
    receipts: Arc<Mutex<Vec<EvmReceipt>>>,
    logs: Arc<Mutex<Vec<LogRecord>>>,
    calls: Arc<Mutex<Vec<EvmSourceCall>>>,
    fail_after_calls: Arc<Mutex<Option<usize>>>,
}

impl Default for EvmFixtureAdapter {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
            transactions: Arc::new(Mutex::new(Vec::new())),
            receipts: Arc::new(Mutex::new(Vec::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            fail_after_calls: Arc::new(Mutex::new(None)),
        }
    }
}

impl EvmFixtureAdapter {
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

    fn with_fail_after_calls(self, calls: usize) -> Self {
        *self.fail_after_calls.lock().expect("fail after") = Some(calls);
        self
    }

    fn calls(&self) -> Vec<EvmSourceCall> {
        self.calls.lock().expect("calls").clone()
    }
}

impl ChainAdapter for EvmFixtureAdapter {
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
        Ok(ChainHeight::block(100).with_finality(FinalityLevel::Safe))
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100).with_finality(FinalityLevel::Finalized))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let call = match request.dataset_key.as_str() {
            "evm.blocks" => EvmSourceCall::Blocks(request.range.clone()),
            "evm.transactions" => EvmSourceCall::Transactions(request.range.clone()),
            "evm.receipts" => EvmSourceCall::Receipts(request.range.clone()),
            "evm.logs" => EvmSourceCall::Logs(request.range.clone()),
            _ => {
                return Err(DatalensError::unsupported(
                    "fixture dataset is not supported",
                ));
            }
        };
        let call_count = {
            let mut calls = self.calls.lock().expect("calls");
            calls.push(call);
            calls.len()
        };
        if let Some(fail_after) = *self.fail_after_calls.lock().expect("fail after")
            && call_count > fail_after
        {
            return Err(DatalensError::provider_timeout("injected failure"));
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
            _ => unreachable!("unsupported dataset rejected above"),
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

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-full-indexing-e2e-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}
