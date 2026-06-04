use std::{
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
};
use datalens_chain::DatasetSelector;
use datalens_chain::FinalityLevel;
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey,
    DatasetRows, EvmReceipt, EvmTransaction, LedgerRange, LedgerRangeKind, LogFilter, LogRecord,
    NetworkId, QueryRows, missing_ranges,
};

use datalens_storage::*;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::reader::{FileReader, SerializedFileReader},
};
use sha2::{Digest, Sha256};

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

#[derive(Clone, Debug)]
struct StaleManifestObjectStore {
    inner: LocalObjectStore,
    manifest_key: String,
    concurrent_manifest_bytes: Arc<Vec<u8>>,
    manifest_get_count: Arc<AtomicUsize>,
}

impl StaleManifestObjectStore {
    fn new(root: PathBuf, manifest_key: String, concurrent_manifest: Manifest) -> Self {
        let inner = LocalObjectStore::new(root);
        inner
            .put(&manifest_key, br#"{"entries":[]}"#)
            .expect("put initial manifest");
        Self {
            inner,
            manifest_key,
            concurrent_manifest_bytes: Arc::new(
                serde_json::to_vec_pretty(&concurrent_manifest).expect("manifest bytes"),
            ),
            manifest_get_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ObjectStore for StaleManifestObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        if key == self.manifest_key {
            let count = self.manifest_get_count.fetch_add(1, Ordering::SeqCst);
            if count == 1 {
                self.inner.put(key, &self.concurrent_manifest_bytes)?;
            }
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
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

#[derive(Debug)]
struct ConcurrentManifestState {
    initial_manifest_reads: Mutex<usize>,
    initial_manifest_reads_ready: Condvar,
}

#[derive(Clone, Debug)]
struct ConcurrentManifestObjectStore {
    inner: LocalObjectStore,
    manifest_key: String,
    manifest_get_count: Arc<AtomicUsize>,
    state: Arc<ConcurrentManifestState>,
}

impl ConcurrentManifestObjectStore {
    fn new(root: PathBuf, manifest_key: String) -> Self {
        let inner = LocalObjectStore::new(root);
        inner
            .put(&manifest_key, br#"{"entries":[]}"#)
            .expect("put initial manifest");
        Self {
            inner,
            manifest_key,
            manifest_get_count: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(ConcurrentManifestState {
                initial_manifest_reads: Mutex::new(0),
                initial_manifest_reads_ready: Condvar::new(),
            }),
        }
    }
}

impl ObjectStore for ConcurrentManifestObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        if key == self.manifest_key {
            let count = self.manifest_get_count.fetch_add(1, Ordering::SeqCst) + 1;
            if count <= 2 {
                let mut initial_manifest_reads = self
                    .state
                    .initial_manifest_reads
                    .lock()
                    .expect("lock reads");
                *initial_manifest_reads += 1;
                self.state.initial_manifest_reads_ready.notify_all();
                while *initial_manifest_reads < 2 {
                    initial_manifest_reads = self
                        .state
                        .initial_manifest_reads_ready
                        .wait(initial_manifest_reads)
                        .expect("wait reads");
                }
            }
            let bytes = self.inner.get(key)?;
            if count == 3 {
                std::thread::sleep(Duration::from_millis(100));
            }
            return Ok(bytes);
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
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
fn test_manifest_deserialization_rejects_data_object_missing_required_metadata() {
    for field in [
        "object_encoding",
        "object_size_bytes",
        "checksum",
        "checksum_algorithm",
        "written_at_unix_seconds",
    ] {
        let mut manifest = serde_json::json!({
            "entries":[{
                "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                "dataset_key":{"family":"Evm","name":"logs"},
                "range":{"kind":{"kind":"block"},"start":1,"end":2},
                "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
                "selector_canonical_key":"evm-logs/addr=*",
                "finality_level":"safe",
                "object_key":"chains/evm/ethereum/1/datasets/evm.logs/parquet-v1/block/evm-logs/addr-topic-deadbeef/00000000000000000001-00000000000000000002.parquet",
                "object_encoding":"parquet-v1",
                "row_count":1,
                "object_size_bytes":128,
                "checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "checksum_algorithm":"sha256",
                "written_at_unix_seconds":1
            }]
        });
        manifest["entries"][0]
            .as_object_mut()
            .expect("manifest entry")
            .remove(field);

        assert!(
            serde_json::from_value::<Manifest>(manifest).is_err(),
            "missing {field} must be rejected"
        );
    }
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
            "object_key":"chains/evm/ethereum/1/datasets/evm.logs/parquet-v1/block/evm-logs/addr-topic-deadbeef/00000000000000000001-00000000000000000002.parquet",
            "object_encoding":"parquet-v1",
            "row_count":1,
            "object_size_bytes":128,
            "checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "checksum_algorithm":"sha256",
            "written_at_unix_seconds":1
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

const ADDRESS_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ADDRESS_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ADDRESS_C: &str = "0xcccccccccccccccccccccccccccccccccccccccc";
const TOPIC_1: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const TOPIC_2: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
const TOPIC_3: &str = "0x3333333333333333333333333333333333333333333333333333333333333333";

fn evm_log_selector(addresses: Vec<&str>, topics: Vec<Option<Vec<&str>>>) -> DatasetSelector {
    DatasetSelector::try_evm_logs(LogFilter {
        addresses: addresses.into_iter().map(str::to_owned).collect(),
        topics: topics
            .into_iter()
            .map(|slot| slot.map(|values| values.into_iter().map(str::to_owned).collect()))
            .collect(),
    })
    .expect("valid selector")
}

fn log_record(block_number: u64, log_index: u64, address: &str, topics: Vec<&str>) -> LogRecord {
    LogRecord::try_new(
        block_number,
        format!("0xblock{block_number}"),
        format!("0xtx{block_number}{log_index}"),
        0,
        log_index,
        address,
        topics.into_iter().map(str::to_owned).collect(),
        "0x".to_owned(),
        false,
    )
    .expect("valid log record")
}

fn single_block_rows(number: u64) -> DatasetRows {
    DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number,
            hash: format!("0xblock{number}"),
            parent_hash: "0xparent".to_owned(),
            timestamp: 1,
        }]),
    )
    .expect("dataset rows")
}

fn first_manifest_object_key(storage: &LocalStorage, chain: &ChainIdentity) -> String {
    let manifest = read_manifest_json(storage, chain);
    manifest["entries"][0]["object_key"]
        .as_str()
        .expect("object key")
        .to_owned()
}

fn read_manifest_json(storage: &LocalStorage, chain: &ChainIdentity) -> serde_json::Value {
    let bytes = std::fs::read(storage.manifest_path(chain)).expect("manifest bytes");
    serde_json::from_slice(&bytes).expect("manifest json")
}

fn write_manifest_json(storage: &LocalStorage, chain: &ChainIdentity, manifest: serde_json::Value) {
    let bytes = serde_json::to_vec_pretty(&manifest).expect("manifest bytes");
    std::fs::write(storage.manifest_path(chain), bytes).expect("write manifest");
}

fn legacy_evm_logs_parquet_bytes() -> Vec<u8> {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("topics", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("removed", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(UInt64Array::from_iter_values([10])) as ArrayRef,
            std::sync::Arc::new(StringArray::from_iter_values(["0xblock10"])),
            std::sync::Arc::new(StringArray::from_iter_values(["0xtx10"])),
            std::sync::Arc::new(UInt64Array::from_iter_values([1])),
            std::sync::Arc::new(UInt64Array::from_iter_values([0])),
            std::sync::Arc::new(StringArray::from_iter_values([
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ])),
            std::sync::Arc::new(StringArray::from_iter_values(["[]"])),
            std::sync::Arc::new(StringArray::from_iter_values(["0x"])),
            std::sync::Arc::new(BooleanArray::from_iter([false])),
        ],
    )
    .expect("legacy batch");
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).expect("legacy writer");
    writer.write(&batch).expect("legacy row write");
    writer.close().expect("legacy writer close");
    bytes
}

fn assert_parquet_compression(bytes: &[u8], expected: Compression) {
    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(bytes))
        .expect("parquet file reader");
    for row_group in reader.metadata().row_groups() {
        for column in row_group.columns() {
            assert_eq!(column.compression(), expected);
        }
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
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
    assert_eq!(entry.object_compression, Some(ParquetCompression::None));
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
fn test_evm_blocks_rows_write_zstd_parquet_and_read_back() {
    let storage = LocalStorage::new_with_config(
        temp_storage_root("blocks-zstd-roundtrip"),
        DurableStorageConfig {
            parquet_compression: ParquetCompression::Zstd,
        },
    );
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

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert_eq!(entry.object_compression, Some(ParquetCompression::Zstd));
    let object_key = entry.object_key.as_deref().expect("object key");
    let object_bytes = std::fs::read(storage.root().join(object_key)).expect("object bytes");
    assert_parquet_compression(&object_bytes, Compression::ZSTD(Default::default()));

    let read = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect("read rows");
    assert_eq!(read, rows);
}

#[test]
fn test_evm_logs_rows_write_snappy_parquet_and_read_back() {
    let storage = LocalStorage::new_with_config(
        temp_storage_root("logs-snappy-roundtrip"),
        DurableStorageConfig {
            parquet_compression: ParquetCompression::Snappy,
        },
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 10).expect("valid range");
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![LogRecord {
            parent_hash: Some("0xparent09".to_owned()),
            block_timestamp: Some(1_700_000_010),
            ..LogRecord::try_new(
                10,
                "0xblock10".to_owned(),
                "0xtx10".to_owned(),
                1,
                0,
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                vec![
                    "0x0000000000000000000000000000000000000000000000000000000000000001".to_owned(),
                ],
                "0xdeadbeef".to_owned(),
                false,
            )
            .unwrap()
        }]),
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

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert_eq!(entry.object_compression, Some(ParquetCompression::Snappy));
    let object_key = entry.object_key.as_deref().expect("object key");
    let object_bytes = std::fs::read(storage.root().join(object_key)).expect("object bytes");
    assert_parquet_compression(&object_bytes, Compression::SNAPPY);

    let read = storage
        .read_rows(&chain, &DatasetKey::evm_logs(), &selector, range)
        .expect("read rows");
    assert_eq!(read, rows);
}

#[test]
fn test_manifest_without_compression_metadata_still_decodes_and_reads() {
    let storage = LocalStorage::new(temp_storage_root("legacy-no-compression-metadata"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 10).expect("valid range");
    let object_key = "chains/evm/ethereum/1/datasets/evm.logs/parquet-v1/block/all/00000000000000000010-00000000000000000010.parquet";
    let object_bytes = legacy_evm_logs_parquet_bytes();
    let object_path = storage.root().join(object_key);
    std::fs::create_dir_all(object_path.parent().expect("object parent"))
        .expect("create object parent");
    std::fs::write(&object_path, &object_bytes).expect("write object");
    std::fs::create_dir_all(storage.root().join("chains/evm/ethereum/1"))
        .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        format!(
            r#"{{
                "entries":[{{
                    "chain":{{"family":"Evm","configured_name":"ethereum","network_id":{{"kind":"numeric","value":1}}}},
                    "dataset_key":{{"family":"Evm","name":"logs"}},
                    "range":{{"kind":{{"kind":"block"}},"start":10,"end":10}},
                    "selector_fingerprint":"all",
                    "selector_canonical_key":"all",
                    "finality_level":"safe",
                    "object_key":"{object_key}",
                    "object_encoding":"parquet-v1",
                    "row_count":1,
                    "object_size_bytes":{},
                    "checksum":"{}",
                    "checksum_algorithm":"sha256",
                    "written_at_unix_seconds":1
                }}]
            }}"#,
            object_bytes.len(),
            hex_sha256(&object_bytes)
        ),
    )
    .expect("write manifest");

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries[0].object_compression, None);

    let read = storage
        .read_rows(&chain, &DatasetKey::evm_logs(), &selector, range)
        .expect("read legacy rows");
    assert_eq!(read.row_count(), 1);
}

#[test]
fn test_empty_coverage_does_not_record_object_compression() {
    let storage = LocalStorage::new_with_config(
        temp_storage_root("empty-no-compression"),
        DurableStorageConfig {
            parquet_compression: ParquetCompression::Zstd,
        },
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("empty log rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: LedgerRange::blocks(100, 101).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert_eq!(entry.object_key, None);
    assert_eq!(entry.object_encoding, None);
    assert_eq!(entry.object_compression, None);
    assert!(
        storage
            .object_store()
            .list("chains")
            .expect("list")
            .iter()
            .all(|object| !object.key.ends_with(".parquet"))
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
fn test_write_rows_outcome_uses_current_range_when_manifest_tail_sorts_elsewhere() {
    let storage = LocalStorage::new(temp_storage_root("write-outcome-sorted-tail"));
    let chain = test_chain();
    let unrelated_range = LedgerRange::blocks(3998752, 4000000).expect("valid range");
    let unrelated_selector = evm_log_selector(vec![ADDRESS_A], vec![None]);
    let unrelated_rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![log_record(3998752, 0, ADDRESS_A, Vec::new())]),
    )
    .expect("dataset rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &unrelated_selector,
            range: unrelated_range.clone(),
            rows: &unrelated_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write unrelated rows");

    let current_range = LedgerRange::blocks(5000000, 5000128).expect("valid range");
    let current_selector = DatasetSelector::all();
    let current_rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");
    let outcome = storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &current_selector,
            range: current_range.clone(),
            rows: &current_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write current empty coverage");

    let manifest = storage.manifest().expect("manifest");
    let sorted_last = manifest.entries.last().expect("manifest entry");
    assert_eq!(sorted_last.range, unrelated_range);
    assert_eq!(outcome.range, current_range);
    assert_eq!(outcome.row_count, 0);
    assert!(outcome.recorded_empty_coverage);
    assert_ne!(sorted_last.range, outcome.range);
    assert!(manifest.entries.iter().any(|entry| {
        entry.dataset_key == DatasetKey::evm_logs()
            && entry.selector_fingerprint == current_selector.fingerprint()
            && entry.range == outcome.range
            && entry.row_count == 0
            && entry.object_key.is_none()
    }));
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
fn test_read_rows_accepts_matching_object_metadata() {
    let storage = LocalStorage::new(temp_storage_root("matching-object-metadata"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = single_block_rows(1);

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

    let read = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect("read rows");

    assert_eq!(read, rows);
}

#[test]
fn test_read_rows_rejects_object_size_mismatch() {
    let storage = LocalStorage::new(temp_storage_root("object-size-mismatch"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = single_block_rows(1);

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
    let object_key = first_manifest_object_key(&storage, &chain);
    let object_path = storage.root().join(&object_key);
    let mut bytes = std::fs::read(&object_path).expect("object bytes");
    bytes.push(0);
    std::fs::write(&object_path, bytes).expect("tamper object");

    let error = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect_err("size mismatch");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    assert!(error.message.contains(&object_key));
    assert!(error.message.contains("size mismatch"));
}

#[test]
fn test_read_rows_rejects_object_checksum_mismatch() {
    let storage = LocalStorage::new(temp_storage_root("object-checksum-mismatch"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = single_block_rows(1);

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
    let object_key = first_manifest_object_key(&storage, &chain);
    let object_path = storage.root().join(&object_key);
    let mut bytes = std::fs::read(&object_path).expect("object bytes");
    let index = bytes.len() / 2;
    bytes[index] ^= 0xff;
    std::fs::write(&object_path, bytes).expect("tamper object");

    let error = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect_err("checksum mismatch");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    assert!(error.message.contains(&object_key));
    assert!(error.message.contains("checksum mismatch"));
}

#[test]
fn test_read_rows_rejects_unknown_checksum_algorithm() {
    let storage = LocalStorage::new(temp_storage_root("unknown-checksum-algorithm"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = single_block_rows(1);

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
    let mut manifest = read_manifest_json(&storage, &chain);
    manifest["entries"][0]["checksum_algorithm"] = serde_json::Value::String("md5".to_owned());
    write_manifest_json(&storage, &chain, manifest);
    let object_key = first_manifest_object_key(&storage, &chain);

    let error = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect_err("unknown checksum algorithm");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    assert!(error.message.contains(&object_key));
    assert!(error.message.contains("unknown checksum algorithm"));
}

#[test]
fn test_read_rows_rejects_manifest_without_required_object_metadata() {
    let storage = LocalStorage::new(temp_storage_root("missing-object-metadata"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = single_block_rows(1);

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
    let mut manifest = read_manifest_json(&storage, &chain);
    let entry = manifest["entries"][0]
        .as_object_mut()
        .expect("manifest entry object");
    entry.remove("object_size_bytes");
    entry.remove("checksum");
    entry.remove("checksum_algorithm");
    entry.remove("written_at_unix_seconds");
    write_manifest_json(&storage, &chain, manifest);

    let error = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect_err("manifest without required object metadata");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
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
            LogRecord {
                parent_hash: Some("0xparent09".to_owned()),
                block_timestamp: Some(1_700_000_010),
                ..LogRecord::try_new(
                    10,
                    "0xblock10".to_owned(),
                    "0xtx10".to_owned(),
                    1,
                    0,
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    vec![
                        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                            .to_owned(),
                    ],
                    "0x1234".to_owned(),
                    false,
                )
                .expect("log row")
            },
            LogRecord {
                parent_hash: Some("0xparent11".to_owned()),
                block_timestamp: Some(1_700_000_012),
                ..LogRecord::try_new(
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
                .expect("log row")
            },
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
fn test_broad_evm_log_address_wildcard_topics_coverage_serves_narrow_query() {
    let storage = LocalStorage::new(temp_storage_root("semantic-address-wildcard-topics"));
    let chain = test_chain();
    let stored_selector = evm_log_selector(vec![ADDRESS_A, ADDRESS_B, ADDRESS_C], vec![]);
    let query_selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let stored_rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![
            log_record(10, 0, ADDRESS_A, vec![TOPIC_1]),
            log_record(10, 1, ADDRESS_B, vec![TOPIC_2]),
            log_record(11, 0, ADDRESS_C, vec![TOPIC_3]),
        ]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &stored_selector,
            range: LedgerRange::blocks(10, 11).expect("valid range"),
            rows: &stored_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write broad rows");

    let covered = storage
        .covered_ranges(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(10, 11).expect("valid range"),
        )
        .expect("covered ranges");
    assert_eq!(
        covered,
        vec![LedgerRange::blocks(10, 11).expect("valid range")]
    );

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(10, 11).expect("valid range"),
        )
        .expect("read rows");
    assert_eq!(
        read,
        DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(vec![log_record(10, 0, ADDRESS_A, vec![TOPIC_1])])
        )
        .expect("dataset rows")
    );
}

#[test]
fn test_broad_evm_log_topic_values_coverage_serves_narrow_query() {
    let storage = LocalStorage::new(temp_storage_root("semantic-topic-values"));
    let chain = test_chain();
    let stored_selector = evm_log_selector(
        vec![ADDRESS_A, ADDRESS_B, ADDRESS_C],
        vec![Some(vec![TOPIC_1, TOPIC_2])],
    );
    let query_selector = evm_log_selector(vec![ADDRESS_B], vec![Some(vec![TOPIC_2])]);
    let stored_rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![
            log_record(10, 0, ADDRESS_A, vec![TOPIC_1]),
            log_record(10, 1, ADDRESS_B, vec![TOPIC_2]),
            log_record(10, 2, ADDRESS_C, vec![TOPIC_3]),
        ]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &stored_selector,
            range: LedgerRange::blocks(10, 10).expect("valid range"),
            rows: &stored_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write broad rows");

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(10, 10).expect("valid range"),
        )
        .expect("read rows");
    assert_eq!(
        read,
        DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(vec![log_record(10, 1, ADDRESS_B, vec![TOPIC_2])])
        )
        .expect("dataset rows")
    );
}

#[test]
fn test_exact_evm_log_coverage_prevents_overlapping_semantic_read() {
    let storage = LocalStorage::new(temp_storage_root("semantic-overlap-exact-preferred"));
    let chain = test_chain();
    let query_selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let broad_selector = evm_log_selector(vec![ADDRESS_A, ADDRESS_B, ADDRESS_C], vec![]);
    let exact_rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![log_record(12, 0, ADDRESS_A, vec![TOPIC_1])]),
    )
    .expect("exact rows");
    let broad_rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![
            log_record(12, 1, ADDRESS_A, vec![TOPIC_1]),
            log_record(12, 2, ADDRESS_B, vec![TOPIC_2]),
        ]),
    )
    .expect("broad rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &query_selector,
            range: LedgerRange::blocks(12, 12).expect("valid range"),
            rows: &exact_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write exact rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &broad_selector,
            range: LedgerRange::blocks(12, 12).expect("valid range"),
            rows: &broad_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write broad rows");

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(12, 12).expect("valid range"),
        )
        .expect("read rows");

    assert_eq!(read, exact_rows);
}

#[test]
fn test_broad_evm_log_empty_coverage_satisfies_narrow_query() {
    let storage = LocalStorage::new(temp_storage_root("semantic-empty-coverage"));
    let chain = test_chain();
    let stored_selector = evm_log_selector(vec![ADDRESS_A, ADDRESS_B, ADDRESS_C], vec![]);
    let query_selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("empty rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &stored_selector,
            range: LedgerRange::blocks(20, 22).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    let covered = storage
        .covered_ranges(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(19, 23).expect("valid range"),
        )
        .expect("covered ranges");
    assert_eq!(
        covered,
        vec![LedgerRange::blocks(20, 22).expect("valid range")]
    );
    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(20, 22).expect("valid range"),
        )
        .expect("read rows");
    assert_eq!(read.row_count(), 0);
}

#[test]
fn test_narrow_evm_log_address_coverage_does_not_cover_broader_query() {
    let storage = LocalStorage::new(temp_storage_root("semantic-narrow-address"));
    let chain = test_chain();
    let stored_selector = evm_log_selector(vec![ADDRESS_A], vec![]);
    let query_selector = evm_log_selector(vec![ADDRESS_A, ADDRESS_B], vec![]);
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![log_record(30, 0, ADDRESS_A, vec![TOPIC_1])]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &stored_selector,
            range: LedgerRange::blocks(30, 30).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let covered = storage
        .covered_ranges(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(30, 30).expect("valid range"),
        )
        .expect("covered ranges");
    assert!(covered.is_empty());
}

#[test]
fn test_narrow_evm_log_topic_coverage_does_not_cover_wildcard_query() {
    let storage = LocalStorage::new(temp_storage_root("semantic-narrow-topic"));
    let chain = test_chain();
    let stored_selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let query_selector = evm_log_selector(vec![ADDRESS_A], vec![]);
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![log_record(40, 0, ADDRESS_A, vec![TOPIC_1])]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &stored_selector,
            range: LedgerRange::blocks(40, 40).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let covered = storage
        .covered_ranges(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(40, 40).expect("valid range"),
        )
        .expect("covered ranges");
    assert!(covered.is_empty());
}

#[test]
fn test_invalid_evm_log_canonical_key_keeps_exact_fingerprint_reads() {
    let storage = LocalStorage::new(temp_storage_root("semantic-invalid-canonical-exact"));
    let chain = test_chain();
    let selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![log_record(50, 0, ADDRESS_A, vec![TOPIC_1])]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: LedgerRange::blocks(50, 50).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");
    let mut manifest = read_manifest_json(&storage, &chain);
    manifest["entries"][0]["selector_canonical_key"] =
        serde_json::Value::String("evm-logs/not-addr=*".to_owned());
    write_manifest_json(&storage, &chain, manifest);

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &selector,
            LedgerRange::blocks(50, 50).expect("valid range"),
        )
        .expect("read rows");
    assert_eq!(read, rows);
}

#[test]
fn test_exact_evm_log_selector_read_behavior_remains_unchanged() {
    let storage = LocalStorage::new(temp_storage_root("semantic-exact-unchanged"));
    let chain = test_chain();
    let selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![log_record(60, 0, ADDRESS_A, vec![TOPIC_1])]),
    )
    .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: LedgerRange::blocks(60, 60).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &selector,
            LedgerRange::blocks(60, 60).expect("valid range"),
        )
        .expect("read rows");
    assert_eq!(read, rows);
}

#[test]
fn test_evm_logs_legacy_parquet_without_block_metadata_reads_null_metadata() {
    let storage = LocalStorage::new(temp_storage_root("logs-legacy-parquet"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 10).expect("valid range");
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
                Vec::new(),
                "0x".to_owned(),
                false,
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

    let mut manifest = read_manifest_json(&storage, &chain);
    let object_key = manifest["entries"][0]["object_key"]
        .as_str()
        .expect("object key")
        .to_owned();
    let legacy_bytes = legacy_evm_logs_parquet_bytes();
    std::fs::write(storage.root().join(&object_key), &legacy_bytes).expect("legacy parquet write");
    let entry = manifest["entries"][0]
        .as_object_mut()
        .expect("manifest entry object");
    entry.insert(
        "object_size_bytes".to_owned(),
        serde_json::json!(legacy_bytes.len() as u64),
    );
    entry.insert(
        "checksum".to_owned(),
        serde_json::json!(hex_sha256(&legacy_bytes)),
    );
    write_manifest_json(&storage, &chain, manifest);

    let read = storage
        .read_rows(&chain, &DatasetKey::evm_logs(), &selector, range)
        .expect("read legacy rows");
    let QueryRows::EvmLogs(logs) = read.rows() else {
        panic!("expected evm logs");
    };

    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].parent_hash, None);
    assert_eq!(logs[0].block_timestamp, None);
}

#[test]
fn test_evm_transactions_and_receipts_write_parquet_and_read_back() {
    let storage = LocalStorage::new(temp_storage_root("evm-durable-dataset-roundtrip"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 10).expect("valid range");

    let transactions = DatasetRows::new(
        DatasetKey::evm_transactions(),
        QueryRows::EvmTransactions(vec![EvmTransaction {
            hash: "0xtx10".to_owned(),
            block_number: 10,
            block_hash: "0xblock10".to_owned(),
            transaction_index: 0,
            from: "0x1111111111111111111111111111111111111111".to_owned(),
            to: Some("0x2222222222222222222222222222222222222222".to_owned()),
            value: "0x1".to_owned(),
            input: "0x".to_owned(),
            nonce: 1,
            gas: 21_000,
            gas_price: Some("0x3b9aca00".to_owned()),
            max_fee_per_gas: Some("0x77359400".to_owned()),
            max_priority_fee_per_gas: Some("0x59682f00".to_owned()),
            transaction_type: Some("0x2".to_owned()),
        }]),
    )
    .expect("transaction rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_transactions(),
            selector: &selector,
            range: range.clone(),
            rows: &transactions,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write transactions");
    assert_eq!(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_transactions(),
                &selector,
                range.clone()
            )
            .expect("read transactions"),
        transactions
    );

    let receipts = DatasetRows::new(
        DatasetKey::evm_receipts(),
        QueryRows::EvmReceipts(vec![EvmReceipt {
            transaction_hash: "0xtx10".to_owned(),
            block_number: 10,
            block_hash: "0xblock10".to_owned(),
            transaction_index: 0,
            status: Some(1),
            gas_used: 21_000,
            cumulative_gas_used: 21_000,
            effective_gas_price: Some("0x3b9aca00".to_owned()),
            contract_address: None,
            logs_bloom: Some(format!("0x{}", "0".repeat(512))),
        }]),
    )
    .expect("receipt rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_receipts(),
            selector: &selector,
            range: range.clone(),
            rows: &receipts,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write receipts");
    assert_eq!(
        storage
            .read_rows(&chain, &DatasetKey::evm_receipts(), &selector, range)
            .expect("read receipts"),
        receipts
    );
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
fn test_write_rows_merges_latest_manifest_before_persisting_stale_snapshot() {
    let root = temp_storage_root("stale-manifest-merge");
    let chain = test_chain();
    let manifest_key = "chains/evm/ethereum/1/manifest.json".to_owned();
    let concurrent_empty_range = LedgerRange::blocks(2, 2).expect("valid range");
    let concurrent_manifest = Manifest {
        entries: vec![ManifestEntry {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_logs(),
            range: concurrent_empty_range.clone(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality_level: ManifestFinalityLevel::Safe,
            object_key: None,
            object_encoding: None,
            object_compression: None,
            row_count: 0,
            object_size_bytes: None,
            checksum: None,
            checksum_algorithm: None,
            written_at_unix_seconds: None,
        }],
    };
    let storage = DurableStorage::from_object_store(StaleManifestObjectStore::new(
        root,
        manifest_key,
        concurrent_manifest,
    ));
    let data_range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = single_block_rows(1);

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: data_range.clone(),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 2);
    assert!(manifest.entries.iter().any(|entry| {
        entry.dataset_key == DatasetKey::evm_blocks()
            && entry.range == data_range
            && entry.object_key.is_some()
            && entry.row_count == 1
    }));
    assert!(manifest.entries.iter().any(|entry| {
        entry.dataset_key == DatasetKey::evm_logs()
            && entry.range == concurrent_empty_range
            && entry.object_key.is_none()
            && entry.row_count == 0
    }));
}

#[test]
fn test_cloned_storage_serializes_concurrent_manifest_updates() {
    let root = temp_storage_root("concurrent-manifest-writes");
    let chain = test_chain();
    let manifest_key = "chains/evm/ethereum/1/manifest.json".to_owned();
    let storage =
        DurableStorage::from_object_store(ConcurrentManifestObjectStore::new(root, manifest_key));

    let first_storage = storage.clone();
    let first_chain = chain.clone();
    let first = std::thread::spawn(move || {
        let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("dataset rows");
        first_storage
            .write_rows(StorageWriteRequest {
                chain: &first_chain,
                dataset_key: DatasetKey::evm_logs(),
                selector: &DatasetSelector::all(),
                range: LedgerRange::blocks(10, 10).expect("valid range"),
                rows: &rows,
                finality_level: FinalityLevel::Safe,
                record_empty_coverage: true,
            })
            .expect("write first coverage");
    });

    let second_storage = storage.clone();
    let second_chain = chain.clone();
    let second = std::thread::spawn(move || {
        let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("dataset rows");
        second_storage
            .write_rows(StorageWriteRequest {
                chain: &second_chain,
                dataset_key: DatasetKey::evm_logs(),
                selector: &DatasetSelector::all(),
                range: LedgerRange::blocks(11, 11).expect("valid range"),
                rows: &rows,
                finality_level: FinalityLevel::Safe,
                record_empty_coverage: true,
            })
            .expect("write second coverage");
    });

    first.join().expect("first writer");
    second.join().expect("second writer");

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 2);
    assert!(manifest.entries.iter().any(|entry| {
        entry.range == LedgerRange::blocks(10, 10).expect("valid range")
            && entry.object_key.is_none()
            && entry.row_count == 0
    }));
    assert!(manifest.entries.iter().any(|entry| {
        entry.range == LedgerRange::blocks(11, 11).expect("valid range")
            && entry.object_key.is_none()
            && entry.row_count == 0
    }));
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
                "object_encoding":"json",
                "row_count":1,
                "object_size_bytes":128,
                "checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "checksum_algorithm":"sha256",
                "written_at_unix_seconds":1
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
