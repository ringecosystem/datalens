use std::path::PathBuf;

use datalens_chain::DatasetSelector;
use datalens_chain::FinalityLevel;
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, DatasetRows,
    LedgerRange, LedgerRangeKind, LogFilter, LogRecord, NetworkId, QueryRows,
};

use datalens_storage::*;

#[test]
fn test_log_filter_object_key_uses_compact_storage_safe_segment() {
    let storage = LocalStorage::new(temp_storage_root("compact-log-filter"));
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let filter = LogFilter {
        addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
        topics: vec![None],
    };
    let chain = test_chain();
    let selector = DatasetSelector::try_evm_logs(filter).expect("valid selector");
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![
            LogRecord::try_new(
                1,
                "0xblock".to_owned(),
                "0xtx".to_owned(),
                0,
                0,
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                Vec::new(),
                "0x".to_owned(),
                false,
            )
            .unwrap(),
        ]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range,
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert!(entry.selector_fingerprint.contains("evm-logs/addr-topic-"));
    assert!(!entry.selector_fingerprint.contains("0xaaaaaaaa"));
    assert!(
        entry
            .object_key
            .as_deref()
            .expect("object key")
            .contains("evm-logs/addr-topic-")
    );
}

#[test]
fn test_manifest_deserialization_rejects_invalid_coverage_semantics() {
    let row_count_without_object = r#"{
        "entries":[{
            "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
            "dataset_key":{"family":"Evm","name":"logs"},
            "range":{"kind":{"kind":"block"},"start":1,"end":2},
            "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
            "selector_canonical_key":"evm-logs/addr=*",
            "finality_level":"safe",
            "object_key":null,
            "row_count":1
        }]
    }"#;
    assert!(serde_json::from_str::<Manifest>(row_count_without_object).is_err());

    let object_without_rows = r#"{
        "entries":[{
            "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
            "dataset_key":{"family":"Evm","name":"logs"},
            "range":{"kind":{"kind":"block"},"start":1,"end":2},
            "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
            "selector_canonical_key":"evm-logs/addr=*",
            "finality_level":"safe",
            "object_key":"objects/logs/key/1-2.json",
            "row_count":0
        }]
    }"#;
    assert!(serde_json::from_str::<Manifest>(object_without_rows).is_err());
}

#[test]
fn test_manifest_deserialization_accepts_valid_coverage_semantics() {
    let empty = r#"{
        "entries":[{
            "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
            "dataset_key":{"family":"Evm","name":"logs"},
            "range":{"kind":{"kind":"block"},"start":1,"end":2},
            "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
            "selector_canonical_key":"evm-logs/addr=*",
            "finality_level":"safe",
            "object_key":null,
            "row_count":0
        }]
    }"#;
    assert!(serde_json::from_str::<Manifest>(empty).is_ok());

    let data_object = r#"{
        "entries":[{
            "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
            "dataset_key":{"family":"Evm","name":"logs"},
            "range":{"kind":{"kind":"block"},"start":1,"end":2},
            "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
            "selector_canonical_key":"evm-logs/addr=*",
            "finality_level":"finalized",
            "object_key":"objects/logs/key/1-2.json",
            "row_count":1
        }]
    }"#;
    assert!(serde_json::from_str::<Manifest>(data_object).is_ok());
}

#[test]
fn test_covered_ranges_rejects_malformed_manifest_entries() {
    let storage = LocalStorage::new(temp_storage_root("malformed-manifest"));
    std::fs::write(
        storage.manifest_path(),
        r#"{
            "entries":[{
                "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                "dataset_key":{"family":"Evm","name":"logs"},
                "range":{"kind":{"kind":"block"},"start":1,"end":2},
                "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
                "selector_canonical_key":"evm-logs/addr=*",
                "finality_level":"safe",
                "object_key":null,
                "row_count":1
            }]
        }"#,
    )
    .expect("write manifest");

    let error = storage
        .covered_ranges(
            &test_chain(),
            &DatasetKey::evm_logs(),
            &DatasetSelector::all(),
            LedgerRange::blocks(1, 2).expect("valid range"),
        )
        .expect_err("malformed manifest");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
}

#[test]
fn test_read_rows_rejects_invalid_cached_log_record() {
    let storage = LocalStorage::new(temp_storage_root("invalid-cached-log"));
    let filter_key = coverage_key(
        &test_chain(),
        &DatasetKey::evm_logs(),
        LedgerRangeKind::Block,
        &DatasetSelector::all(),
    );
    let object_key = format!("objects/{filter_key}/1-1.json");
    let object_path = storage.root().join(&object_key);
    std::fs::create_dir_all(object_path.parent().expect("object parent"))
        .expect("create object dir");
    std::fs::write(
        &object_path,
        r#"{
            "dataset":"logs",
            "rows":[{
                "block_number":1,
                "block_hash":"0xblock",
                "transaction_hash":"0xtx",
                "transaction_index":0,
                "log_index":0,
                "address":"0xabc",
                "topics":[],
                "data":"0x",
                "removed":false
            }]
        }"#,
    )
    .expect("write object");
    std::fs::write(
        storage.manifest_path(),
        format!(
            r#"{{
                "entries":[{{
                    "dataset":"logs",
                    "chain":{{"family":"Evm","configured_name":"ethereum","network_id":{{"kind":"numeric","value":1}}}},
                    "dataset_key":{{"family":"Evm","name":"logs"}},
                    "range":{{"kind":{{"kind":"block"}},"start":1,"end":1}},
                    "selector_fingerprint":"all",
                    "selector_canonical_key":"all",
                    "finality_level":"safe",
                    "object_key":"{object_key}",
                    "row_count":1
                }}]
            }}"#
        ),
    )
    .expect("write manifest");

    let error = storage
        .read_rows(
            &test_chain(),
            &DatasetKey::evm_logs(),
            &DatasetSelector::all(),
            LedgerRange::blocks(1, 1).expect("valid range"),
        )
        .expect_err("invalid cached log");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-storage-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}

#[test]
fn test_selector_coverage_key_includes_chain_dataset_and_stable_fingerprint() {
    let chain =
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1));
    let selector = DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
        topics: vec![None],
    })
    .expect("valid selector");

    let key = coverage_key(
        &chain,
        &DatasetKey::evm_logs(),
        LedgerRangeKind::Block,
        &selector,
    );

    assert!(key.starts_with("chains/evm/ethereum/1/datasets/evm.logs/ranges/block/selectors/"));
    assert!(key.contains("/evm-logs/addr-topic-"));
    assert!(!key.contains("0xaaaaaaaa"));
}

#[test]
fn test_manifest_entry_records_chain_neutral_coverage_identity() {
    let storage = LocalStorage::new(temp_storage_root("chain-neutral-manifest"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 12).expect("valid range");
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number: 11,
            hash: "0xblock".to_owned(),
            parent_hash: "0xparent".to_owned(),
            timestamp: 1,
        }]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: range.clone(),
            rows: &rows,
            finality_level: FinalityLevel::Finalized,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert_eq!(entry.chain, chain);
    assert_eq!(entry.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(entry.range, range);
    assert_eq!(entry.selector_fingerprint, "all");
    assert_eq!(entry.selector_canonical_key, "all");
    assert_eq!(entry.finality_level, ManifestFinalityLevel::Finalized);
    assert!(
        entry
            .object_key
            .as_deref()
            .expect("object key")
            .starts_with(
                "objects/chains/evm/ethereum/1/datasets/evm.blocks/ranges/block/selectors/all/"
            )
    );
}

#[test]
fn test_write_rows_rejects_latest_finality_for_durable_manifest() {
    let storage = LocalStorage::new(temp_storage_root("latest-finality"));
    let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");

    let error = storage
        .write_rows(StorageWriteRequest {
            chain: &test_chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(1, 2).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Latest,
            record_empty_coverage: true,
        })
        .expect_err("latest coverage rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
}

#[test]
fn test_empty_coverage_uses_chain_neutral_missing_ranges() {
    let storage = LocalStorage::new(temp_storage_root("chain-neutral-empty"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: LedgerRange::blocks(5, 7).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    let covered = storage
        .covered_ranges(
            &chain,
            &DatasetKey::evm_logs(),
            &selector,
            LedgerRange::blocks(4, 8).expect("valid range"),
        )
        .expect("covered ranges");
    assert_eq!(
        covered,
        vec![LedgerRange::blocks(5, 7).expect("valid range")]
    );
    assert_eq!(
        missing_ranges(LedgerRange::blocks(4, 8).expect("valid range"), &covered),
        vec![
            LedgerRange::blocks(4, 4).expect("valid range"),
            LedgerRange::blocks(8, 8).expect("valid range"),
        ]
    );

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert_eq!(entry.object_key, None);
    assert_eq!(entry.row_count, 0);
    assert_eq!(entry.finality_level, ManifestFinalityLevel::Safe);
}
