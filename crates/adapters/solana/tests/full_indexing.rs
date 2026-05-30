use std::sync::{Arc, Mutex};

use datalens_chain::{ChainAdapter, DatasetSelector};
use datalens_core::{DatasetKey, LedgerRange, QueryRows};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::ApplicationIdentity;
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput};
use datalens_runtime_indexer::{
    InMemoryIndexCursorStore, IndexDatasetRequest, IndexDatasetSelection, IndexFinalityRequirement,
    IndexJob, IndexJobId, IndexRetryPolicy, IndexRunMode, IndexRunStatus, IndexRuntime,
    IndexRuntimeConfig,
};
use datalens_solana::{
    SolanaAdapter, SolanaBlock, SolanaCommitment, SolanaInnerInstructionGroup, SolanaInstruction,
    SolanaRpc, SolanaTokenBalance, SolanaTransaction, solana_address_selector,
};
use datalens_storage::LocalStorage;
use datalens_writer::DurableWriterConfig;
use serde_json::{Value, json};

#[test]
fn test_full_indexing_backfills_solana_durable_datasets_once_per_slot_chunk() {
    let root = temp_storage_root("full-indexing");
    let storage = LocalStorage::new(&root);
    let provider = CountingSolanaRpc::default();
    let adapter =
        SolanaAdapter::with_provider(solana_chain(), provider.clone()).with_max_slot_range_len(3);
    let runtime = solana_runtime(adapter.clone(), storage.clone());

    let result = runtime
        .run(full_solana_job(10, 12, IndexRunMode::Backfill))
        .expect("backfill Solana datasets");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(result.accounting.chunks_planned, 5);
    assert_eq!(result.accounting.rows_written, 12);
    assert_eq!(provider.blocks_with_limit_calls(), 1);
    assert_eq!(provider.block_calls(), vec![10, 11, 12]);

    assert_json_row_slots(
        &storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::solana_slots(),
                &DatasetSelector::all(),
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read indexed slots"),
        &[10, 12],
    );
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::solana_blocks(),
                &DatasetSelector::all(),
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read indexed blocks")
            .row_count(),
        2
    );
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::solana_transactions(),
                &DatasetSelector::all(),
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read indexed transactions")
            .row_count(),
        2
    );
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::solana_instructions(),
                &DatasetSelector::all(),
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read indexed instructions")
            .row_count(),
        2
    );
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::solana_account_updates(),
                &DatasetSelector::all(),
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read indexed account updates")
            .row_count(),
        4
    );
}

#[test]
fn test_resume_after_partial_solana_indexing_fetches_remaining_dataset_same_slot_range() {
    let storage = LocalStorage::new(temp_storage_root("resume-partial"));
    let cursor_store = InMemoryIndexCursorStore::default();
    let first_provider = CountingSolanaRpc::default();
    let first_adapter =
        SolanaAdapter::with_provider(solana_chain(), first_provider).with_max_slot_range_len(3);
    let first = IndexRuntime::new(
        first_adapter,
        storage.clone(),
        cursor_store.clone(),
        writer_config(),
    );

    first
        .run(solana_slots_job(10, 12, IndexRunMode::Backfill))
        .expect("seed partial slot indexing");

    let resumed_provider = CountingSolanaRpc::default();
    let resumed_adapter = SolanaAdapter::with_provider(solana_chain(), resumed_provider.clone())
        .with_max_slot_range_len(3);
    let resumed = IndexRuntime::new(
        resumed_adapter.clone(),
        storage.clone(),
        cursor_store,
        writer_config(),
    );
    let result = resumed
        .run(full_solana_job(10, 12, IndexRunMode::Resume))
        .expect("resume remaining datasets");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(result.accounting.chunks_skipped, 1);
    assert_eq!(resumed_provider.blocks_with_limit_calls(), 1);
    assert_eq!(
        storage
            .read_rows(
                resumed_adapter.capabilities().chain(),
                &DatasetKey::solana_blocks(),
                &DatasetSelector::all(),
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read blocks after resume")
            .row_count(),
        2
    );
}

#[test]
fn test_durable_query_reads_indexed_solana_data_without_provider_fill() {
    let storage = LocalStorage::new(temp_storage_root("durable-query"));
    let indexing_provider = CountingSolanaRpc::default();
    let indexing_adapter =
        SolanaAdapter::with_provider(solana_chain(), indexing_provider).with_max_slot_range_len(3);
    solana_runtime(indexing_adapter.clone(), storage.clone())
        .run(full_solana_job(10, 12, IndexRunMode::Backfill))
        .expect("index slots");

    let query_provider = CountingSolanaRpc::default();
    let query_adapter = SolanaAdapter::with_provider(solana_chain(), query_provider.clone())
        .with_max_slot_range_len(3);
    let executor = NativeQueryExecutor::new(
        storage,
        query_adapter.clone(),
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
            chain: query_adapter.capabilities().chain().clone(),
            dataset_key: DatasetKey::solana_transactions(),
            ledger_range: LedgerRange::slots(10, 12).expect("range"),
            selector: DatasetSelector::all(),
            field_selection: FieldSelection::All,
            finality: datalens_core::QueryFinalityRequirement::DurableOnly,
        })
        .expect("query durable transactions");

    assert_eq!(result.cache.missing_ranges, Vec::<LedgerRange>::new());
    assert_eq!(result.rows.row_count(), 2);
    assert_eq!(query_provider.blocks_with_limit_calls(), 0);
    assert_eq!(query_provider.block_calls(), Vec::<u64>::new());
}

#[test]
fn test_selector_specific_solana_indexing_reads_back_without_all_selector_collision() {
    let storage = LocalStorage::new(temp_storage_root("selector-specific-readback"));
    let provider = CountingSolanaRpc::default();
    let adapter =
        SolanaAdapter::with_provider(solana_chain(), provider.clone()).with_max_slot_range_len(3);
    let selector =
        solana_address_selector("Account111111111111111111111111111111111").expect("selector");
    let runtime = solana_runtime(adapter.clone(), storage.clone());

    let result = runtime
        .run(selector_solana_transactions_job(
            10,
            12,
            selector.clone(),
            IndexRunMode::Backfill,
        ))
        .expect("backfill selected transactions");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::solana_transactions(),
                &selector,
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read selector-specific transactions")
            .row_count(),
        2
    );
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::solana_transactions(),
                &DatasetSelector::all(),
                LedgerRange::slots(10, 12).expect("range"),
            )
            .expect("read all selector transactions")
            .row_count(),
        0
    );

    let query_provider = CountingSolanaRpc::default();
    let query_adapter = SolanaAdapter::with_provider(solana_chain(), query_provider.clone())
        .with_max_slot_range_len(3);
    let executor = NativeQueryExecutor::new(
        storage,
        query_adapter.clone(),
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
            chain: query_adapter.capabilities().chain().clone(),
            dataset_key: DatasetKey::solana_transactions(),
            ledger_range: LedgerRange::slots(10, 12).expect("range"),
            selector,
            field_selection: FieldSelection::All,
            finality: datalens_core::QueryFinalityRequirement::DurableOnly,
        })
        .expect("query durable selector transactions");

    assert_eq!(result.cache.missing_ranges, Vec::<LedgerRange>::new());
    assert_eq!(result.rows.row_count(), 2);
    assert_eq!(query_provider.blocks_with_limit_calls(), 0);
}

#[test]
fn test_solana_account_updates_are_supported_from_finalized_block_metadata() {
    let adapter = SolanaAdapter::with_provider(solana_chain(), CountingSolanaRpc::default());
    assert!(
        adapter
            .capabilities()
            .dataset(&DatasetKey::solana_account_updates())
            .is_some()
    );
}

fn solana_runtime(
    adapter: SolanaAdapter<CountingSolanaRpc>,
    storage: LocalStorage,
) -> IndexRuntime<SolanaAdapter<CountingSolanaRpc>, LocalStorage, InMemoryIndexCursorStore> {
    IndexRuntime::new(
        adapter,
        storage,
        InMemoryIndexCursorStore::default(),
        writer_config(),
    )
}

fn full_solana_job(start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    IndexJob {
        id: IndexJobId::new("solana-full-indexing").expect("job id"),
        application: ApplicationIdentity::named("indexer"),
        chain: solana_chain(),
        range: LedgerRange::slots(start, end).expect("range"),
        dataset_selection: IndexDatasetSelection::AllSupported,
        finality_requirement: IndexFinalityRequirement::Finalized,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 3 },
        run_mode,
        retry_policy: IndexRetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
    }
}

fn solana_slots_job(start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    IndexJob {
        dataset_selection: IndexDatasetSelection::Selected(vec![IndexDatasetRequest {
            dataset_key: DatasetKey::solana_slots(),
            selector: DatasetSelector::all(),
        }]),
        ..full_solana_job(start, end, run_mode)
    }
}

fn selector_solana_transactions_job(
    start: u64,
    end: u64,
    selector: DatasetSelector,
    run_mode: IndexRunMode,
) -> IndexJob {
    IndexJob {
        dataset_selection: IndexDatasetSelection::Selected(vec![IndexDatasetRequest {
            dataset_key: DatasetKey::solana_transactions(),
            selector,
        }]),
        ..full_solana_job(start, end, run_mode)
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

fn assert_json_row_slots(rows: &datalens_core::DatasetRows, expected: &[u64]) {
    let QueryRows::AdapterJson { rows, .. } = rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["slot"].as_u64().expect("slot"))
            .collect::<Vec<_>>(),
        expected
    );
}

fn solana_chain() -> datalens_core::ChainIdentity {
    datalens_core::ChainIdentity::try_new(
        datalens_core::ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(datalens_core::NetworkId::textual("mainnet-beta").expect("network id")),
    )
    .expect("chain")
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-solana-full-indexing-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

#[derive(Clone, Default)]
struct CountingSolanaRpc {
    state: Arc<Mutex<CountingState>>,
}

#[derive(Default)]
struct CountingState {
    blocks_with_limit_calls: u64,
    block_calls: Vec<u64>,
}

impl CountingSolanaRpc {
    fn blocks_with_limit_calls(&self) -> u64 {
        self.state.lock().expect("state").blocks_with_limit_calls
    }

    fn block_calls(&self) -> Vec<u64> {
        self.state.lock().expect("state").block_calls.clone()
    }
}

impl SolanaRpc for CountingSolanaRpc {
    fn get_slot(&self, commitment: SolanaCommitment) -> Result<u64, datalens_core::DatalensError> {
        Ok(match commitment {
            SolanaCommitment::Processed | SolanaCommitment::Confirmed => 14,
            SolanaCommitment::Finalized => 12,
        })
    }

    fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        _commitment: SolanaCommitment,
    ) -> Result<Vec<u64>, datalens_core::DatalensError> {
        let mut state = self.state.lock().expect("state");
        state.blocks_with_limit_calls += 1;
        Ok([10, 11, 12]
            .into_iter()
            .filter(|slot| *slot >= start_slot && *slot < start_slot.saturating_add(limit))
            .collect())
    }

    fn get_block(
        &self,
        slot: u64,
        _commitment: SolanaCommitment,
    ) -> Result<Option<SolanaBlock>, datalens_core::DatalensError> {
        self.state.lock().expect("state").block_calls.push(slot);
        Ok(match slot {
            10 | 12 => Some(solana_block(slot)),
            _ => None,
        })
    }

    fn provider_name(&self) -> &'static str {
        "counting-solana-fixture"
    }
}

fn solana_block(slot: u64) -> SolanaBlock {
    SolanaBlock {
        slot,
        block_height: Some(1_000 + slot),
        blockhash: format!("slot-{slot}-hash"),
        previous_blockhash: format!("slot-{}-hash", slot.saturating_sub(1)),
        parent_slot: slot.saturating_sub(1),
        block_time: Some(1_700_000_000 + slot),
        transactions: vec![solana_transaction(slot)],
        raw: json!({ "slot": slot, "fixture": true }),
    }
}

fn solana_transaction(slot: u64) -> SolanaTransaction {
    let program_id = "program1111111111111111111111111111111111".to_owned();
    SolanaTransaction {
        signature: format!("sig-slot-{slot}"),
        fee: 5_000,
        err: None,
        account_keys: vec![
            "Account111111111111111111111111111111111".to_owned(),
            program_id.clone(),
        ],
        loaded_addresses: vec!["Loaded1111111111111111111111111111111111".to_owned()],
        pre_balances: vec![1_000_000, 1, 100_000],
        post_balances: vec![900_000, 1, 100_000],
        pre_token_balances: vec![SolanaTokenBalance {
            account_index: 0,
            mint: "TokenMint11111111111111111111111111111111".to_owned(),
            owner: Some("Owner1111111111111111111111111111111111".to_owned()),
            program_id: Some("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()),
            amount: "10".to_owned(),
            decimals: Some(0),
            ui_amount_string: Some("10".to_owned()),
            raw: json!({ "fixture": "pre-token-balance" }),
        }],
        post_token_balances: vec![SolanaTokenBalance {
            account_index: 0,
            mint: "TokenMint11111111111111111111111111111111".to_owned(),
            owner: Some("Owner1111111111111111111111111111111111".to_owned()),
            program_id: Some("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()),
            amount: "7".to_owned(),
            decimals: Some(0),
            ui_amount_string: Some("7".to_owned()),
            raw: json!({ "fixture": "post-token-balance" }),
        }],
        instructions: vec![SolanaInstruction {
            program_id: program_id.clone(),
            accounts: vec!["Account111111111111111111111111111111111".to_owned()],
            data: Some("3Bxs".to_owned()),
            parsed: None,
        }],
        inner_instructions: vec![SolanaInnerInstructionGroup {
            index: 0,
            instructions: Vec::new(),
        }],
        raw: Value::Object(serde_json::Map::new()),
    }
}
