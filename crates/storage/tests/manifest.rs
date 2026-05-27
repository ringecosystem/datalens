use std::path::PathBuf;

use datalens_chain::DatasetSelector;
use datalens_chain::FinalityLevel;
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey,
    DatasetRows, LedgerRange, LedgerRangeKind, LogFilter, LogRecord, NetworkId, QueryRows,
};

use datalens_storage::*;

#[derive(Clone, Debug)]
struct FailingPutObjectStore {
    inner: LocalObjectStore,
}

impl FailingPutObjectStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
    }
}

impl ObjectStore for FailingPutObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, _key: &str, _bytes: &[u8]) -> Result<(), DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected object write failure",
        ))
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

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

    let encoding_key_mismatch = r#"{
        "entries":[{
            "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
            "dataset_key":{"family":"Evm","name":"logs"},
            "range":{"kind":{"kind":"block"},"start":1,"end":2},
            "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
            "selector_canonical_key":"evm-logs/addr=*",
            "finality_level":"safe",
            "object_key":"objects/logs/key/1-2.json",
            "object_encoding":"parquet-v1",
            "row_count":1
        }]
    }"#;
    assert!(serde_json::from_str::<Manifest>(encoding_key_mismatch).is_err());
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
    std::fs::create_dir_all(storage.root().join("chains/evm/ethereum/1"))
        .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&test_chain()),
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
    let object_key = format!("{filter_key}/00000000000000000001-00000000000000000001.json");
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
    std::fs::create_dir_all(storage.root().join("chains/evm/ethereum/1"))
        .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&test_chain()),
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

    assert!(key.starts_with("chains/evm/ethereum/1/datasets/evm.logs/parquet-v1/block/"));
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
    assert_eq!(entry.object_encoding, Some(ObjectEncoding::ParquetV1));
    assert!(entry.object_size_bytes.expect("object size") > 0);
    assert_eq!(entry.checksum_algorithm.as_deref(), Some("sha256"));
    assert_eq!(entry.checksum.as_deref().expect("checksum").len(), 64);
    assert!(entry.written_at_unix_seconds.is_some());
    assert!(
        entry
            .object_key
            .as_deref()
            .expect("object key")
            .starts_with("chains/evm/ethereum/1/datasets/evm.blocks/parquet-v1/block/all/")
    );
}

#[test]
fn test_write_rows_returns_data_object_metadata() {
    let storage = LocalStorage::new(temp_storage_root("write-outcome-metadata"));
    let chain = test_chain();
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number: 1,
            hash: "0xblock".to_owned(),
            parent_hash: "0xparent".to_owned(),
            timestamp: 1,
        }]),
    )
    .expect("dataset rows");

    let outcome = storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let object = outcome.data_object.expect("data object metadata");
    assert!(object.object_key.ends_with(".parquet"));
    assert_eq!(object.object_encoding, ObjectEncoding::ParquetV1);
    assert_eq!(object.row_count, 1);
    assert!(object.object_size_bytes > 0);
    assert_eq!(object.checksum_algorithm, "sha256");
    assert_eq!(object.checksum.len(), 64);
    assert!(!outcome.recorded_empty_coverage);
}

#[test]
fn test_evm_blocks_rows_write_parquet_and_read_back() {
    let storage = LocalStorage::new(temp_storage_root("blocks-parquet-roundtrip"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 12).expect("valid range");
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![
            BlockHeader {
                number: 10,
                hash: "0xblock10".to_owned(),
                parent_hash: "0xparent09".to_owned(),
                timestamp: 100,
            },
            BlockHeader {
                number: 12,
                hash: "0xblock12".to_owned(),
                parent_hash: "0xparent11".to_owned(),
                timestamp: 120,
            },
        ]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: range.clone(),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let manifest_bytes =
        std::fs::read(storage.manifest_path(&chain)).expect("manifest bytes after write");
    let manifest_json: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("manifest json");
    let entry = &manifest_json["entries"][0];
    let object_key = entry["object_key"].as_str().expect("object key");
    assert_eq!(entry["object_encoding"], "parquet-v1");
    assert!(object_key.contains("/parquet-v1/"));
    assert!(object_key.ends_with(".parquet"));

    let object_bytes = std::fs::read(storage.root().join(object_key)).expect("object bytes");
    assert_eq!(&object_bytes[..4], b"PAR1");

    let read = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect("read rows");
    assert_eq!(read, rows);
}

#[test]
fn test_evm_logs_rows_write_parquet_and_read_back() {
    let storage = LocalStorage::new(temp_storage_root("logs-parquet-roundtrip"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 12).expect("valid range");
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![
            LogRecord::try_new(
                10,
                "0xblock10".to_owned(),
                "0xtx10".to_owned(),
                1,
                0,
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                vec![
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ],
                "0x1234".to_owned(),
                false,
            )
            .expect("log row"),
            LogRecord::try_new(
                12,
                "0xblock12".to_owned(),
                "0xtx12".to_owned(),
                2,
                1,
                "0xcccccccccccccccccccccccccccccccccccccccc",
                Vec::new(),
                "0x".to_owned(),
                true,
            )
            .expect("log row"),
        ]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: range.clone(),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let manifest_bytes =
        std::fs::read(storage.manifest_path(&chain)).expect("manifest bytes after write");
    let manifest_json: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("manifest json");
    let entry = &manifest_json["entries"][0];
    let object_key = entry["object_key"].as_str().expect("object key");
    assert_eq!(entry["object_encoding"], "parquet-v1");
    assert!(object_key.contains("/parquet-v1/"));
    assert!(object_key.ends_with(".parquet"));

    let object_bytes = std::fs::read(storage.root().join(object_key)).expect("object bytes");
    assert_eq!(&object_bytes[..4], b"PAR1");

    let read = storage
        .read_rows(&chain, &DatasetKey::evm_logs(), &selector, range)
        .expect("read rows");
    assert_eq!(read, rows);
}

#[test]
fn test_parquet_read_rows_keeps_range_filtering() {
    let storage = LocalStorage::new(temp_storage_root("parquet-range-filter"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![
            BlockHeader {
                number: 10,
                hash: "0xblock10".to_owned(),
                parent_hash: "0xparent09".to_owned(),
                timestamp: 100,
            },
            BlockHeader {
                number: 11,
                hash: "0xblock11".to_owned(),
                parent_hash: "0xparent10".to_owned(),
                timestamp: 110,
            },
        ]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(10, 11).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(11, 11).expect("valid range"),
        )
        .expect("read rows");

    assert_eq!(
        read,
        DatasetRows::new(
            DatasetKey::evm_blocks(),
            QueryRows::EvmBlocks(vec![BlockHeader {
                number: 11,
                hash: "0xblock11".to_owned(),
                parent_hash: "0xparent10".to_owned(),
                timestamp: 110,
            }])
        )
        .expect("dataset rows")
    );
}

#[test]
fn test_manifest_is_chain_namespaced() {
    let storage = LocalStorage::new(temp_storage_root("chain-manifest"));
    let chain = test_chain();

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: &DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
                .expect("dataset rows"),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    assert!(
        storage
            .root()
            .join("chains/evm/ethereum/1/manifest.json")
            .exists()
    );
}

#[test]
fn test_write_rows_is_idempotent_for_same_logical_shard() {
    let storage = LocalStorage::new(temp_storage_root("idempotent-write"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 2).expect("valid range");
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number: 1,
            hash: "0xblock".to_owned(),
            parent_hash: "0xparent".to_owned(),
            timestamp: 1,
        }]),
    )
    .expect("dataset rows");

    for _ in 0..2 {
        storage
            .write_rows(StorageWriteRequest {
                chain: &chain,
                dataset_key: DatasetKey::evm_blocks(),
                selector: &selector,
                range: range.clone(),
                rows: &rows,
                finality_level: FinalityLevel::Safe,
                record_empty_coverage: true,
            })
            .expect("write rows");
    }

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
}

#[test]
fn test_object_write_failure_does_not_update_manifest() {
    let root = temp_storage_root("object-write-failure");
    let storage = DurableStorage::from_object_store(FailingPutObjectStore::new(root.clone()));
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number: 1,
            hash: "0xblock".to_owned(),
            parent_hash: "0xparent".to_owned(),
            timestamp: 1,
        }]),
    )
    .expect("dataset rows");

    let error = storage
        .write_rows(StorageWriteRequest {
            chain: &test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect_err("write failure");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
}

#[test]
fn test_read_rows_rejects_manifest_entry_with_missing_object() {
    let storage = LocalStorage::new(temp_storage_root("missing-object"));
    std::fs::create_dir_all(storage.root().join("chains/evm/ethereum/1"))
        .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&test_chain()),
        r#"{
            "entries":[{
                "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                "dataset_key":{"family":"Evm","name":"blocks"},
                "range":{"kind":{"kind":"block"},"start":1,"end":1},
                "selector_fingerprint":"all",
                "selector_canonical_key":"all",
                "finality_level":"safe",
                "object_key":"chains/evm/ethereum/1/datasets/evm.blocks/json/block/all/00000000000000000001-00000000000000000001.json",
                "row_count":1
            }]
        }"#,
    )
    .expect("write manifest");

    let error = storage
        .read_rows(
            &test_chain(),
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(1, 1).expect("valid range"),
        )
        .expect_err("missing object");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    assert!(error.message.contains("object not found"));
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
    assert_eq!(entry.object_encoding, None);
    assert_eq!(entry.row_count, 0);
    assert_eq!(entry.finality_level, ManifestFinalityLevel::Safe);
}
