use std::{
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
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

#[derive(Clone, Debug)]
struct CountingDataObjectExistsStore {
    inner: LocalObjectStore,
    data_object_exists_count: Arc<AtomicUsize>,
}

impl CountingDataObjectExistsStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            data_object_exists_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn put_inner(&self, key: &str, bytes: &[u8]) {
        self.inner.put(key, bytes).expect("put inner object");
    }

    fn data_object_exists_count(&self) -> usize {
        self.data_object_exists_count.load(Ordering::SeqCst)
    }
}

impl ObjectStore for CountingDataObjectExistsStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        if key.contains("/datasets/") {
            self.data_object_exists_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

#[derive(Clone, Debug)]
struct MissingDataObjectExistsStore {
    inner: LocalObjectStore,
}

impl MissingDataObjectExistsStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
    }
}

impl ObjectStore for MissingDataObjectExistsStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        if key.contains("/datasets/") {
            return Ok(false);
        }
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

#[derive(Clone, Debug)]
struct ManifestGetCountingStore {
    inner: LocalObjectStore,
    manifest_key: String,
    manifest_get_count: Arc<AtomicUsize>,
}

impl ManifestGetCountingStore {
    fn new(root: PathBuf, chain: &ChainIdentity) -> Self {
        let inner = LocalObjectStore::new(root);
        let manifest_key = format!("chains/{}/manifest.json", chain.key_prefix());
        inner
            .put(&manifest_key, br#"{"entries":[]}"#)
            .expect("put initial manifest");
        Self {
            inner,
            manifest_key,
            manifest_get_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn manifest_get_count(&self) -> usize {
        self.manifest_get_count.load(Ordering::SeqCst)
    }
}

impl ObjectStore for ManifestGetCountingStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        if key == self.manifest_key {
            self.manifest_get_count.fetch_add(1, Ordering::SeqCst);
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

#[derive(Clone, Debug)]
struct ManifestAccessCountingStore {
    inner: LocalObjectStore,
    manifest_access_count: Arc<AtomicUsize>,
    coverage_index_list_count: Arc<AtomicUsize>,
}

impl ManifestAccessCountingStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            manifest_access_count: Arc::new(AtomicUsize::new(0)),
            coverage_index_list_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn manifest_access_count(&self) -> usize {
        self.manifest_access_count.load(Ordering::SeqCst)
    }

    fn reset_manifest_access_count(&self) {
        self.manifest_access_count.store(0, Ordering::SeqCst);
    }

    fn coverage_index_list_count(&self) -> usize {
        self.coverage_index_list_count.load(Ordering::SeqCst)
    }

    fn reset_coverage_index_list_count(&self) {
        self.coverage_index_list_count.store(0, Ordering::SeqCst);
    }

    fn put_inner(&self, key: &str, bytes: &[u8]) {
        self.inner.put(key, bytes).expect("put inner object");
    }

    fn is_manifest_access(key: &str) -> bool {
        key.ends_with("/manifest.json")
            || key.ends_with("/manifest.version")
            || key.contains("/manifest-segments")
    }
}

impl ObjectStore for ManifestAccessCountingStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        if Self::is_manifest_access(key) {
            self.manifest_access_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        if Self::is_manifest_access(key) {
            self.manifest_access_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        if Self::is_manifest_access(prefix) {
            self.manifest_access_count.fetch_add(1, Ordering::SeqCst);
        }
        if prefix.contains("/coverage-index") {
            self.coverage_index_list_count
                .fetch_add(1, Ordering::SeqCst);
        }
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

#[derive(Debug)]
struct PausedManifestPutState {
    put_started: Mutex<bool>,
    release_put: Mutex<bool>,
    put_started_ready: Condvar,
    release_put_ready: Condvar,
}

#[derive(Clone, Debug)]
struct PausedManifestPutStore {
    inner: LocalObjectStore,
    paused_put_prefix: String,
    state: Arc<PausedManifestPutState>,
}

impl PausedManifestPutStore {
    fn new(root: PathBuf, paused_put_prefix: String) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            paused_put_prefix,
            state: Arc::new(PausedManifestPutState {
                put_started: Mutex::new(false),
                release_put: Mutex::new(false),
                put_started_ready: Condvar::new(),
                release_put_ready: Condvar::new(),
            }),
        }
    }

    fn wait_for_paused_put(&self) {
        let mut put_started = self.state.put_started.lock().expect("lock put started");
        while !*put_started {
            put_started = self
                .state
                .put_started_ready
                .wait(put_started)
                .expect("wait put started");
        }
    }

    fn release_paused_put(&self) {
        let mut release_put = self.state.release_put.lock().expect("lock release put");
        *release_put = true;
        self.state.release_put_ready.notify_all();
    }
}

impl ObjectStore for PausedManifestPutStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        if key.starts_with(&self.paused_put_prefix) {
            {
                let mut put_started = self.state.put_started.lock().expect("lock put started");
                *put_started = true;
                self.state.put_started_ready.notify_all();
            }
            let mut release_put = self.state.release_put.lock().expect("lock release put");
            while !*release_put {
                release_put = self
                    .state
                    .release_put_ready
                    .wait(release_put)
                    .expect("wait release put");
            }
        }
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
fn test_manifest_rejects_malformed_manifest_entries() {
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

    let error = storage.manifest().expect_err("malformed manifest");

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
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let manifest = serde_json::from_str::<serde_json::Value>(&format!(
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
    ))
    .expect("manifest json");
    std::fs::write(
        storage.manifest_path(&test_chain()),
        serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
    )
    .expect("write manifest");
    write_coverage_index_json(
        &storage,
        &test_chain(),
        &DatasetKey::evm_logs(),
        "block",
        &DatasetSelector::all(),
        ManifestFinalityLevel::Safe,
        &range,
        manifest,
    );

    let error = storage
        .read_rows(
            &test_chain(),
            &DatasetKey::evm_logs(),
            &DatasetSelector::all(),
            range,
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

fn lisk_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "lisk", NetworkId::numeric(1135))
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
    first_manifest_object_key_from_merged(storage, chain)
}

fn read_manifest_json(storage: &LocalStorage, chain: &ChainIdentity) -> serde_json::Value {
    let bytes = std::fs::read(manifest_json_path_for_test(storage, chain)).expect("manifest bytes");
    serde_json::from_slice(&bytes).expect("manifest json")
}

fn write_manifest_json(storage: &LocalStorage, chain: &ChainIdentity, manifest: serde_json::Value) {
    let bytes = serde_json::to_vec_pretty(&manifest).expect("manifest bytes");
    std::fs::write(manifest_json_path_for_test(storage, chain), bytes).expect("write manifest");
    clear_coverage_index(storage, chain);
}

#[allow(clippy::too_many_arguments)]
fn write_coverage_index_json<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range_kind: &str,
    selector: &DatasetSelector,
    finality_level: ManifestFinalityLevel,
    range: &LedgerRange,
    index: serde_json::Value,
) {
    let bucket_size = 100_000;
    let bucket_start = (range.start() / bucket_size) * bucket_size;
    let bucket_end = bucket_start + bucket_size - 1;
    let key = format!(
        "chains/{}/coverage-index/{}/{}/{}/{}/{:020}-{:020}.json",
        chain.key_prefix(),
        dataset_key.as_str(),
        range_kind,
        selector.fingerprint(),
        finality_level.as_str(),
        bucket_start,
        bucket_end
    );
    let bytes = serde_json::to_vec_pretty(&index).expect("coverage index bytes");
    storage
        .object_store()
        .put(&key, &bytes)
        .expect("write coverage index");
}

fn manifest_json_path_for_test(storage: &LocalStorage, chain: &ChainIdentity) -> PathBuf {
    let manifest_path = storage.manifest_path(chain);
    if manifest_path.exists() {
        return manifest_path;
    }
    let segment_key = manifest_segment_keys(storage, chain)
        .into_iter()
        .next()
        .expect("manifest segment");
    storage.root().join(segment_key)
}

fn manifest_segment_keys(storage: &LocalStorage, chain: &ChainIdentity) -> Vec<String> {
    storage
        .object_store()
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segment list")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

fn coverage_index_keys<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range_kind: &str,
    selector: &DatasetSelector,
    finality_level: ManifestFinalityLevel,
) -> Vec<String> {
    storage
        .object_store()
        .list(&format!(
            "chains/{}/coverage-index/{}/{}/{}/{}",
            chain.key_prefix(),
            dataset_key.as_str(),
            range_kind,
            selector.fingerprint(),
            finality_level.as_str(),
        ))
        .expect("coverage index list")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

fn clear_coverage_index<S: ObjectStore>(storage: &DurableStorage<S>, chain: &ChainIdentity) {
    for object in storage
        .object_store()
        .list(&format!("chains/{}/coverage-index", chain.key_prefix()))
        .expect("coverage index list")
    {
        storage
            .object_store()
            .delete(&object.key)
            .expect("delete coverage index object");
    }
}

fn first_coverage_index_json(
    storage: &LocalStorage,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range_kind: &str,
    selector: &DatasetSelector,
    finality_level: ManifestFinalityLevel,
) -> serde_json::Value {
    let key = coverage_index_keys(
        storage,
        chain,
        dataset_key,
        range_kind,
        selector,
        finality_level,
    )
    .into_iter()
    .next()
    .expect("coverage index key");
    let bytes = std::fs::read(storage.root().join(key)).expect("coverage index bytes");
    serde_json::from_slice(&bytes).expect("coverage index json")
}

fn first_manifest_object_key_from_merged(storage: &LocalStorage, chain: &ChainIdentity) -> String {
    let manifest = storage.manifest().expect("manifest");
    manifest
        .entries
        .iter()
        .find(|entry| entry.chain == *chain)
        .expect("manifest entry")
        .object_key
        .as_deref()
        .expect("object key")
        .to_owned()
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
    let manifest = serde_json::from_str::<serde_json::Value>(&format!(
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
    ))
    .expect("manifest json");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
    )
    .expect("write manifest");
    write_coverage_index_json(
        &storage,
        &chain,
        &DatasetKey::evm_logs(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
        &range,
        manifest.clone(),
    );

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

    let manifest_json = read_manifest_json(&storage, &chain);
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
    write_manifest_json(&storage, &chain, manifest.clone());
    write_coverage_index_json(
        &storage,
        &chain,
        &DatasetKey::evm_blocks(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
        &range,
        manifest,
    );
    let object_key = first_manifest_object_key(&storage, &chain);
    let reader = LocalStorage::new(storage.root().to_path_buf());

    let error = reader
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
    write_manifest_json(&storage, &chain, manifest.clone());
    write_coverage_index_json(
        &storage,
        &chain,
        &DatasetKey::evm_blocks(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
        &range,
        manifest,
    );
    let reader = LocalStorage::new(storage.root().to_path_buf());

    let error = reader
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

    let manifest_json = read_manifest_json(&storage, &chain);
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
fn test_broad_evm_log_address_wildcard_topics_index_does_not_serve_narrow_query() {
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
    assert!(covered.is_empty());

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(10, 11).expect("valid range"),
        )
        .expect("read rows");
    assert_eq!(read.row_count(), 0);
}

#[test]
fn test_broad_evm_log_topic_values_index_does_not_serve_narrow_query() {
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
    assert_eq!(read.row_count(), 0);
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
fn test_partial_exact_coverage_does_not_use_semantic_fallback_for_missing_ranges() {
    let storage = LocalStorage::new(temp_storage_root("semantic-partial-exact-fallback"));
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
            log_record(13, 0, ADDRESS_A, vec![TOPIC_1]),
            log_record(13, 1, ADDRESS_B, vec![TOPIC_2]),
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
            range: LedgerRange::blocks(13, 13).expect("valid range"),
            rows: &broad_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write broad rows");

    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_logs(),
                &query_selector,
                LedgerRange::blocks(12, 13).expect("valid range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(12, 12).expect("valid range")]
    );

    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_logs(),
            &query_selector,
            LedgerRange::blocks(12, 13).expect("valid range"),
        )
        .expect("read rows");

    assert_eq!(
        read,
        DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(vec![log_record(12, 0, ADDRESS_A, vec![TOPIC_1]),])
        )
        .expect("dataset rows")
    );
}

#[test]
fn test_broad_evm_log_empty_coverage_index_does_not_satisfy_narrow_query() {
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
    assert!(covered.is_empty());
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
    write_manifest_json(&storage, &chain, manifest.clone());
    write_coverage_index_json(
        &storage,
        &chain,
        &DatasetKey::evm_logs(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
        &LedgerRange::blocks(50, 50).expect("valid range"),
        manifest,
    );
    let reader = LocalStorage::new(storage.root().to_path_buf());

    let read = reader
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
    write_manifest_json(&storage, &chain, manifest.clone());
    write_coverage_index_json(
        &storage,
        &chain,
        &DatasetKey::evm_logs(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
        &range,
        manifest,
    );
    let reader = LocalStorage::new(storage.root().to_path_buf());

    let read = reader
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

    assert_eq!(manifest_segment_keys(&storage, &chain).len(), 1);
}

#[test]
fn test_empty_coverage_write_publishes_segment_without_rewriting_full_manifest() {
    let root = temp_storage_root("empty-segment-no-full-rewrite");
    let chain = test_chain();
    let store = ManifestGetCountingStore::new(root.clone(), &chain);
    let storage = DurableStorage::from_object_store(store.clone());
    let manifest_path = root.join(format!("chains/{}/manifest.json", chain.key_prefix()));
    let original_manifest_bytes = std::fs::read(&manifest_path).expect("manifest bytes");
    let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(10, 10).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    assert_eq!(store.manifest_get_count(), 0);
    assert_eq!(
        std::fs::read(&manifest_path).expect("manifest bytes"),
        original_manifest_bytes
    );
    let segments = LocalStorage::new(root)
        .object_store()
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segments");
    assert_eq!(segments.len(), 1);
    assert!(segments[0].size < 2048);
}

#[test]
fn test_legacy_full_manifest_and_segments_merge() {
    let storage = LocalStorage::new(temp_storage_root("legacy-full-plus-segments"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let legacy_range = LedgerRange::blocks(1, 1).expect("valid range");
    let segment_range = LedgerRange::blocks(2, 2).expect("valid range");
    let legacy_rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");
    let segment_rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");
    let legacy_manifest = Manifest {
        entries: vec![ManifestEntry {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_logs(),
            range: legacy_range.clone(),
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
    std::fs::create_dir_all(
        storage
            .root()
            .join(format!("chains/{}", chain.key_prefix())),
    )
    .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&legacy_manifest).expect("manifest bytes"),
    )
    .expect("write legacy manifest");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: segment_range.clone(),
            rows: &segment_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write segment coverage");

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 2);
    assert!(
        manifest
            .entries
            .iter()
            .any(|entry| entry.range == legacy_range)
    );
    assert!(
        manifest
            .entries
            .iter()
            .any(|entry| entry.range == segment_range)
    );
    assert_eq!(legacy_rows.row_count(), 0);
}

#[test]
fn test_segmented_entries_are_visible_to_covered_ranges_and_read_rows() {
    let storage = LocalStorage::new(temp_storage_root("segmented-read-coverage"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let data_range = LedgerRange::blocks(1, 1).expect("valid range");
    let empty_range = LedgerRange::blocks(2, 2).expect("valid range");
    let rows = single_block_rows(1);
    let empty_rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: data_range.clone(),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write data rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: empty_range.clone(),
            rows: &empty_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty rows");

    assert_eq!(manifest_segment_keys(&storage, &chain).len(), 2);
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(1, 2).expect("valid range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(1, 2).expect("valid range")]
    );
    assert_eq!(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(1, 2).expect("valid range"),
            )
            .expect("read rows"),
        rows
    );
}

#[test]
fn test_empty_coverage_write_creates_bucket_index_and_serves_covered_ranges() {
    let storage = LocalStorage::new(temp_storage_root("coverage-index-empty"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(10, 12).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    let index_keys = coverage_index_keys(
        &storage,
        &chain,
        &DatasetKey::evm_blocks(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
    );
    assert_eq!(index_keys.len(), 1);
    assert!(index_keys[0].contains("/coverage-index/evm.blocks/block/all/safe/"));

    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(10, 12).expect("valid range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(10, 12).expect("valid range")]
    );
}

#[test]
fn test_adjacent_empty_coverage_coalesces_in_bucket_index() {
    let storage = LocalStorage::new(temp_storage_root("coverage-index-empty-coalesce"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        .expect("dataset rows");

    for range in [
        LedgerRange::blocks(10, 11).expect("valid range"),
        LedgerRange::blocks(12, 14).expect("valid range"),
    ] {
        storage
            .write_rows(StorageWriteRequest {
                chain: &chain,
                dataset_key: DatasetKey::evm_blocks(),
                selector: &selector,
                range,
                rows: &rows,
                finality_level: FinalityLevel::Safe,
                record_empty_coverage: true,
            })
            .expect("write empty coverage");
    }

    let index = first_coverage_index_json(
        &storage,
        &chain,
        &DatasetKey::evm_blocks(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
    );
    let entries = index["entries"].as_array().expect("index entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["range"]["start"], 10);
    assert_eq!(entries[0]["range"]["end"], 14);
    assert!(entries[0]["object_key"].is_null());

    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(10, 14).expect("valid range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(10, 14).expect("valid range")]
    );
}

#[test]
fn test_data_object_coverage_can_be_read_through_coverage_index() {
    let store = ManifestAccessCountingStore::new(temp_storage_root("coverage-index-data-read"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = single_block_rows(10);

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(10, 10).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write data rows");

    store.reset_manifest_access_count();
    let read = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(10, 10).expect("valid range"),
        )
        .expect("read rows");

    assert_eq!(read, rows);
    assert_eq!(store.manifest_access_count(), 0);
}

#[test]
fn test_old_manifest_data_still_works_through_explicit_manifest_path() {
    let storage = LocalStorage::new(temp_storage_root("coverage-index-legacy-fallback"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(20, 20).expect("valid range");
    let rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        .expect("dataset rows");
    let legacy_manifest = Manifest {
        entries: vec![ManifestEntry {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            range: range.clone(),
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
    std::fs::create_dir_all(
        storage
            .root()
            .join(format!("chains/{}", chain.key_prefix())),
    )
    .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&legacy_manifest).expect("manifest bytes"),
    )
    .expect("write legacy manifest");

    assert!(
        coverage_index_keys(
            &storage,
            &chain,
            &DatasetKey::evm_blocks(),
            "block",
            &selector,
            ManifestFinalityLevel::Safe,
        )
        .is_empty()
    );
    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].range, range);
    assert_eq!(manifest.entries[0].row_count, rows.row_count());
}

#[test]
fn test_covered_ranges_does_not_load_legacy_manifest_when_coverage_index_is_absent() {
    let store = ManifestAccessCountingStore::new(temp_storage_root("coverage-index-absent-cover"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = single_block_rows(20);
    let range = LedgerRange::blocks(20, 20).expect("valid range");

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
        .expect("write data rows");
    clear_coverage_index(&storage, &chain);

    store.reset_manifest_access_count();
    let covered = storage
        .covered_ranges(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect("covered ranges");

    assert!(covered.is_empty());
    assert_eq!(store.manifest_access_count(), 0);
}

#[test]
fn test_read_rows_does_not_load_legacy_manifest_when_coverage_index_is_absent() {
    let store = ManifestAccessCountingStore::new(temp_storage_root("coverage-index-absent-read"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = single_block_rows(20);
    let range = LedgerRange::blocks(20, 20).expect("valid range");

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
        .expect("write data rows");
    clear_coverage_index(&storage, &chain);

    store.reset_manifest_access_count();
    let read = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect("read rows");

    assert_eq!(read.row_count(), 0);
    assert_eq!(store.manifest_access_count(), 0);
}

#[test]
fn test_partial_coverage_index_does_not_load_legacy_manifest() {
    let store = ManifestAccessCountingStore::new(temp_storage_root("coverage-index-partial"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let first_range = LedgerRange::blocks(20, 20).expect("valid range");
    let second_range = LedgerRange::blocks(21, 21).expect("valid range");
    let query_range = LedgerRange::blocks(20, 21).expect("valid range");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: first_range.clone(),
            rows: &single_block_rows(20),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write first range");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: second_range,
            rows: &single_block_rows(21),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write second range");

    let manifest = storage.manifest().expect("manifest");
    let index = serde_json::json!({
        "entries": manifest
            .entries
            .iter()
            .filter(|entry| entry.range == first_range)
            .cloned()
            .collect::<Vec<_>>()
    });
    write_coverage_index_json(
        &storage,
        &chain,
        &DatasetKey::evm_blocks(),
        "block",
        &selector,
        ManifestFinalityLevel::Safe,
        &query_range,
        index,
    );

    store.reset_manifest_access_count();
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                query_range.clone(),
            )
            .expect("covered ranges"),
        vec![first_range]
    );
    assert_eq!(store.manifest_access_count(), 0);

    store.reset_manifest_access_count();
    assert_eq!(
        storage
            .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, query_range)
            .expect("read rows"),
        single_block_rows(20)
    );
    assert_eq!(store.manifest_access_count(), 0);
}

#[test]
fn test_manifest_cross_bucket_partial_index_returns_present_bucket_without_manifest_or_list() {
    let store =
        ManifestAccessCountingStore::new(temp_storage_root("coverage-index-cross-bucket-partial"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let covered_range = LedgerRange::blocks(99_999, 99_999).expect("valid range");
    let query_range = LedgerRange::blocks(99_999, 100_000).expect("valid range");
    let rows = single_block_rows(99_999);

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: covered_range.clone(),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write first bucket rows");

    store.reset_manifest_access_count();
    store.reset_coverage_index_list_count();
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                query_range.clone(),
            )
            .expect("covered ranges"),
        vec![covered_range.clone()]
    );
    assert_eq!(store.manifest_access_count(), 0);
    assert_eq!(store.coverage_index_list_count(), 0);

    store.reset_manifest_access_count();
    store.reset_coverage_index_list_count();
    assert_eq!(
        storage
            .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, query_range)
            .expect("read rows"),
        rows
    );
    assert_eq!(store.manifest_access_count(), 0);
    assert_eq!(store.coverage_index_list_count(), 0);
}

#[test]
fn test_exact_indexed_query_uses_deterministic_keys_without_listing_coverage_index() {
    let store = ManifestAccessCountingStore::new(temp_storage_root("coverage-index-deterministic"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = single_block_rows(30);
    let range = LedgerRange::blocks(30, 30).expect("valid range");

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
        .expect("write data rows");

    store.reset_manifest_access_count();
    store.reset_coverage_index_list_count();
    assert_eq!(
        storage
            .covered_ranges(&chain, &DatasetKey::evm_blocks(), &selector, range.clone())
            .expect("covered ranges"),
        vec![range.clone()]
    );
    assert_eq!(store.manifest_access_count(), 0);
    assert_eq!(store.coverage_index_list_count(), 0);

    store.reset_manifest_access_count();
    store.reset_coverage_index_list_count();
    assert_eq!(
        storage
            .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
            .expect("read rows"),
        rows
    );
    assert_eq!(store.manifest_access_count(), 0);
    assert_eq!(store.coverage_index_list_count(), 0);
}

#[test]
fn test_exact_indexed_query_does_not_load_unrelated_manifest_segments() {
    let store = ManifestAccessCountingStore::new(temp_storage_root("coverage-index-no-manifest"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let unrelated_selector = evm_log_selector(vec![ADDRESS_A], vec![]);
    let rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        .expect("dataset rows");
    let unrelated_manifest_key = format!(
        "chains/{}/manifest-segments/evm.logs/block/{}/safe/00000000000000000001-00000000000000000001.json",
        chain.key_prefix(),
        unrelated_selector.fingerprint()
    );
    store.put_inner(&unrelated_manifest_key, br#"{"entries":[]}"#);

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(30, 31).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    store.reset_manifest_access_count();
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(30, 31).expect("valid range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(30, 31).expect("valid range")]
    );
    assert_eq!(store.manifest_access_count(), 0);
}

#[test]
fn test_write_rows_is_idempotent_for_same_logical_shard() {
    let storage = LocalStorage::new(temp_storage_root("idempotent-write"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 2).expect("valid range");
    let rows = single_block_rows(1);

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
    assert_eq!(manifest_segment_keys(&storage, &chain).len(), 1);
}

#[test]
fn test_full_manifest_shadows_stale_compacted_segments() {
    let root = temp_storage_root("full-manifest-shadows-stale-segments");
    let storage = LocalStorage::new(&root);
    let chain = test_chain();
    let selector = DatasetSelector::all();

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: &single_block_rows(1),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write stale small segment");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(1, 2).expect("valid range"),
            rows: &DatasetRows::new(
                DatasetKey::evm_blocks(),
                QueryRows::EvmBlocks(vec![
                    BlockHeader {
                        number: 1,
                        hash: "0xblock1".to_owned(),
                        parent_hash: "0xparent".to_owned(),
                        timestamp: 1,
                    },
                    BlockHeader {
                        number: 2,
                        hash: "0xblock2".to_owned(),
                        parent_hash: "0xblock1".to_owned(),
                        timestamp: 2,
                    },
                ]),
            )
            .expect("dataset rows"),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write compacted segment object");

    let compacted_entry = storage
        .manifest()
        .expect("manifest")
        .entries
        .into_iter()
        .find(|entry| entry.range == LedgerRange::blocks(1, 2).expect("valid range"))
        .expect("compacted entry");
    let full_manifest = Manifest {
        entries: vec![compacted_entry],
    };
    std::fs::create_dir_all(
        storage
            .root()
            .join(format!("chains/{}", chain.key_prefix())),
    )
    .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&full_manifest).expect("manifest bytes"),
    )
    .expect("write full manifest");
    assert_eq!(manifest_segment_keys(&storage, &chain).len(), 2);

    let restarted = LocalStorage::new(root);
    let manifest = restarted.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(
        manifest.entries[0].range,
        LedgerRange::blocks(1, 2).expect("valid range")
    );
    assert_eq!(
        restarted
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(1, 2).expect("valid range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(1, 2).expect("valid range")]
    );
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
    let root = temp_storage_root("concurrent-segment-writes");
    let chain = test_chain();
    let storage = LocalStorage::new(root);

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
    assert_eq!(manifest_segment_keys(&storage, &chain).len(), 2);
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
fn test_single_entry_empty_coverage_publish_does_not_revalidate_existing_data_objects() {
    let root = temp_storage_root("single-entry-no-historical-exists");
    let store = CountingDataObjectExistsStore::new(root);
    let chain = test_chain();
    let historical_entry_count = 32;
    let mut manifest = Manifest::default();

    for index in 0..historical_entry_count {
        let object_key = format!(
            "chains/{}/datasets/evm.blocks/parquet-v1/block/all/{index:020}-{index:020}.parquet",
            chain.key_prefix()
        );
        store.put_inner(&object_key, b"x");
        manifest.entries.push(ManifestEntry {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            range: LedgerRange::blocks(index as u64, index as u64).expect("valid range"),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality_level: ManifestFinalityLevel::Safe,
            object_key: Some(object_key),
            object_encoding: Some(ObjectEncoding::ParquetV1),
            object_compression: Some(ParquetCompression::None),
            row_count: 1,
            object_size_bytes: Some(1),
            checksum: Some("0".repeat(64)),
            checksum_algorithm: Some("sha256".to_owned()),
            written_at_unix_seconds: Some(1),
        });
    }
    store.put_inner(
        &format!("chains/{}/manifest.json", chain.key_prefix()),
        &serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
    );

    let storage = DurableStorage::from_object_store(store.clone());
    let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(10_000, 10_000).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    assert_eq!(store.data_object_exists_count(), 0);
    assert_eq!(
        storage.manifest().expect("manifest").entries.len(),
        historical_entry_count + 1
    );
}

#[test]
fn test_single_entry_data_object_publish_validates_new_object_exists() {
    let root = temp_storage_root("single-entry-new-object-exists");
    let storage =
        DurableStorage::from_object_store(MissingDataObjectExistsStore::new(root.clone()));
    let chain = test_chain();
    let selector = evm_log_selector(vec![ADDRESS_A], vec![Some(vec![TOPIC_1])]);
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![log_record(10_000, 0, ADDRESS_A, vec![TOPIC_1])]),
    )
    .expect("dataset rows");

    let error = storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: &selector,
            range: LedgerRange::blocks(10_000, 10_000).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect_err("missing new object validation should fail");

    assert_eq!(error.kind, DatalensErrorKind::ManifestUpdateFailure);
    assert!(
        storage.manifest().expect("manifest").entries.is_empty(),
        "failed data-object manifest publish must not leave durable coverage"
    );
    let segment_prefix = root.join(format!("chains/{}/manifest-segments", chain.key_prefix()));
    assert!(
        !segment_prefix.exists(),
        "failed data-object manifest publish must not leave a segment"
    );
}

#[test]
fn test_manifest_updates_for_different_chains_do_not_share_lock() {
    let root = temp_storage_root("per-chain-manifest-locks");
    let ethereum = test_chain();
    let lisk = lisk_chain();
    let store = PausedManifestPutStore::new(
        root,
        format!("chains/{}/manifest-segments/", ethereum.key_prefix()),
    );
    let storage = DurableStorage::from_object_store(store.clone());

    let ethereum_storage = storage.clone();
    let ethereum_writer = std::thread::spawn(move || {
        let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("dataset rows");
        ethereum_storage.write_rows(StorageWriteRequest {
            chain: &ethereum,
            dataset_key: DatasetKey::evm_logs(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
    });
    store.wait_for_paused_put();

    let (sender, receiver) = mpsc::channel();
    let lisk_storage = storage.clone();
    let lisk_writer = std::thread::spawn(move || {
        let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("dataset rows");
        let result = lisk_storage.write_rows(StorageWriteRequest {
            chain: &lisk,
            dataset_key: DatasetKey::evm_logs(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(2, 2).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        });
        sender.send(result).expect("send lisk result");
    });

    let quick_lisk_result = receiver.recv_timeout(Duration::from_millis(100));
    let lisk_completed_quickly =
        !matches!(&quick_lisk_result, Err(mpsc::RecvTimeoutError::Timeout));
    store.release_paused_put();
    ethereum_writer
        .join()
        .expect("ethereum writer")
        .expect("ethereum write");
    let lisk_result = match quick_lisk_result {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("lisk result after releasing ethereum put"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("lisk writer disconnected"),
    };
    lisk_writer.join().expect("lisk writer");
    lisk_result.expect("lisk write");

    assert!(
        lisk_completed_quickly,
        "lisk manifest write waited behind ethereum manifest put"
    );
}

#[test]
fn test_object_and_empty_coverage_under_same_selector_remain_visible() {
    let storage = LocalStorage::new(temp_storage_root("mixed-object-empty-coverage"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let empty_rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        .expect("dataset rows");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: &single_block_rows(1),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write object coverage");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(2, 2).expect("valid range"),
            rows: &empty_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(3, 3).expect("valid range"),
            rows: &single_block_rows(3),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write adjacent object coverage");

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 3);
    assert_eq!(
        storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(1, 3).expect("valid range"),
            )
            .expect("covered ranges"),
        vec![LedgerRange::blocks(1, 3).expect("valid range")]
    );
    assert!(manifest.entries.iter().any(|entry| {
        entry.range == LedgerRange::blocks(1, 1).expect("valid range")
            && entry.object_key.is_some()
            && entry.row_count == 1
    }));
    assert!(manifest.entries.iter().any(|entry| {
        entry.range == LedgerRange::blocks(2, 2).expect("valid range")
            && entry.object_key.is_none()
            && entry.row_count == 0
    }));
    assert!(manifest.entries.iter().any(|entry| {
        entry.range == LedgerRange::blocks(3, 3).expect("valid range")
            && entry.object_key.is_some()
            && entry.row_count == 1
    }));
}

#[test]
fn test_cold_narrow_coverage_lookup_does_not_scan_all_empty_manifest_segments() {
    let root = temp_storage_root("narrow-empty-coverage-lookup");
    let writer = LocalStorage::new(&root);
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let empty_rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        .expect("dataset rows");
    let segment_count = 128;

    for block in 1..=segment_count {
        writer
            .write_rows(StorageWriteRequest {
                chain: &chain,
                dataset_key: DatasetKey::evm_logs(),
                selector: &selector,
                range: LedgerRange::blocks(block, block).expect("valid range"),
                rows: &empty_rows,
                finality_level: FinalityLevel::Safe,
                record_empty_coverage: true,
            })
            .expect("write empty coverage");
    }
    assert_eq!(
        manifest_segment_keys(&writer, &chain).len(),
        segment_count as usize
    );

    let store = ManifestAccessCountingStore::new(root);
    let storage = DurableStorage::from_object_store(store.clone());
    let covered = storage
        .covered_ranges(
            &chain,
            &DatasetKey::evm_logs(),
            &selector,
            LedgerRange::blocks(segment_count, segment_count).expect("valid range"),
        )
        .expect("covered ranges");

    assert_eq!(
        covered,
        vec![LedgerRange::blocks(segment_count, segment_count).expect("valid range")]
    );
    assert!(
        store.manifest_access_count() <= 4,
        "narrow coverage lookup used {} manifest accesses",
        store.manifest_access_count()
    );
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
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let manifest = serde_json::json!({
        "entries": [{
            "chain": {"family": "Evm", "configured_name": "ethereum", "network_id": {"kind": "numeric", "value": 1}},
            "dataset_key": {"family": "Evm", "name": "blocks"},
            "range": {"kind": {"kind": "block"}, "start": 1, "end": 1},
            "selector_fingerprint": "all",
            "selector_canonical_key": "all",
            "finality_level": "safe",
            "object_key": "chains/evm/ethereum/1/datasets/evm.blocks/json/block/all/00000000000000000001-00000000000000000001.json",
            "object_encoding": "json",
            "row_count": 1,
            "object_size_bytes": 128,
            "checksum": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "checksum_algorithm": "sha256",
            "written_at_unix_seconds": 1
        }]
    });
    std::fs::create_dir_all(storage.root().join("chains/evm/ethereum/1"))
        .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&test_chain()),
        serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
    )
    .expect("write manifest");
    write_coverage_index_json(
        &storage,
        &test_chain(),
        &DatasetKey::evm_blocks(),
        "block",
        &DatasetSelector::all(),
        ManifestFinalityLevel::Safe,
        &range,
        manifest,
    );

    let error = storage
        .read_rows(
            &test_chain(),
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            range,
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
