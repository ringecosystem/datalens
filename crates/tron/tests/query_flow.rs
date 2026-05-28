use datalens_chain::ChainAdapter;
use datalens_core::{DatasetKey, LedgerRange, QueryFinalityRequirement, QueryRows};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput, ResponseShape};
use datalens_storage::LocalStorage;
use datalens_tron::{TronAdapter, tron_all_selector};
use datalens_writer::DurableWriterConfig;

#[test]
fn test_tron_blocks_complete_fetch_query_cache_flow() {
    let root = temp_storage_root("tron-blocks-query-cache");
    let storage = LocalStorage::new(&root);
    let adapter = TronAdapter::with_fixture_defaults();
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
                staging: Default::default(),
            },
        },
    );
    let input = NativeQueryInput {
        chain: adapter.capabilities().chain().clone(),
        dataset_key: DatasetKey::tron_blocks(),
        ledger_range: LedgerRange::blocks(10, 12).expect("valid range"),
        selector: tron_all_selector().expect("selector"),
        response_shape: ResponseShape::NativeRows,
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    };

    let first = executor.execute(input.clone()).expect("first query");
    let second = executor.execute(input).expect("second query");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(10, 12).expect("valid range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(10, 12).expect("valid range")]
    );
    let QueryRows::AdapterJson { rows, .. } = second.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["number"].as_u64().expect("number"))
            .collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
    assert!(
        root.join("chains/tron/tron-mainnet/mainnet/datasets")
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
