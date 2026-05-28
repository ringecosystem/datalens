use datalens_chain::ChainAdapter;
use datalens_core::{DatasetKey, LedgerRange, QueryFinalityRequirement, QueryRows};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput, ResponseShape};
use datalens_solana::{SolanaAdapter, solana_all_selector};
use datalens_storage::LocalStorage;
use datalens_writer::DurableWriterConfig;

#[test]
fn test_solana_slots_complete_fetch_query_cache_flow() {
    let root = temp_storage_root("solana-slots-query-cache");
    let storage = LocalStorage::new(&root);
    let adapter = SolanaAdapter::with_fixture_defaults();
    let executor = NativeQueryExecutor::new(
        storage.clone(),
        adapter.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 10,
                default_chunk_range_len: 10,
            },
            writer: DurableWriterConfig {
                target_object_bytes: 4096,
                min_object_rows: 1,
                record_empty_coverage: true,
            },
        },
    );
    let input = NativeQueryInput {
        chain: adapter.capabilities().chain().clone(),
        dataset_key: DatasetKey::solana_slots(),
        ledger_range: LedgerRange::slots(10, 12).expect("valid range"),
        selector: solana_all_selector().expect("selector"),
        response_shape: ResponseShape::NativeRows,
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    };

    let first = executor.execute(input.clone()).expect("first query");
    let second = executor.execute(input).expect("second query");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::slots(10, 12).expect("valid range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::slots(10, 12).expect("valid range")]
    );
    let QueryRows::AdapterJson { rows, .. } = second.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["slot"].as_u64().expect("slot"))
            .collect::<Vec<_>>(),
        vec![10, 12]
    );
    assert!(
        root.join("chains/solana/solana-mainnet-beta/mainnet-beta/manifest.json")
            .exists()
    );
    assert!(
        root.join("chains/solana/solana-mainnet-beta/mainnet-beta/datasets")
            .exists()
    );
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("datalens-{name}-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove old temp root");
    }
    root
}
