use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatasetKey, DatasetRows, LedgerRange, NetworkId,
    QueryRows,
};
use datalens_storage::{
    DurableStorage, LocalObjectStore, Manifest, ObjectListPage, ObjectMetadata, ObjectStore,
    ReadThroughCacheConfig, StorageWriteRequest,
};

#[derive(Clone, Debug)]
struct CountingObjectStore {
    inner: LocalObjectStore,
    reads: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl CountingObjectStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            reads: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn read_count(&self, key: &str) -> usize {
        *self
            .reads
            .lock()
            .expect("read counts")
            .get(key)
            .unwrap_or(&0)
    }

    fn data_object_read_count(&self) -> usize {
        self.reads
            .lock()
            .expect("read counts")
            .iter()
            .filter(|(key, _)| key.contains("/datasets/"))
            .map(|(_, count)| *count)
            .sum()
    }
}

impl ObjectStore for CountingObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, datalens_core::DatalensError> {
        *self
            .reads
            .lock()
            .expect("read counts")
            .entry(key.to_owned())
            .or_default() += 1;
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), datalens_core::DatalensError> {
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, datalens_core::DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, datalens_core::DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, datalens_core::DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), datalens_core::DatalensError> {
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

#[test]
fn test_read_through_cache_second_read_skips_object_fetch() {
    let root = temp_storage_root("hit");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::enabled(16),
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 11).expect("valid range");
    let rows = block_rows(&[10, 11]);

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
    let object_key = first_data_object_key(&storage);

    assert_eq!(
        storage
            .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range.clone())
            .expect("first read"),
        rows
    );
    assert_eq!(store.read_count(&object_key), 1);

    assert_eq!(
        storage
            .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
            .expect("second read"),
        rows
    );
    assert_eq!(store.read_count(&object_key), 1);
}

#[test]
fn test_read_through_cache_checksum_change_invalidates_entry() {
    let root = temp_storage_root("checksum-change");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::enabled(16),
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = block_rows(&[1]);

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
    let object_key = first_data_object_key(&storage);
    storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range.clone())
        .expect("warm cache");
    assert_eq!(store.read_count(&object_key), 1);

    let mut manifest = storage.manifest().expect("manifest");
    manifest.entries[0].checksum = Some("changed-checksum".to_owned());
    let segment_key = store
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segments")
        .into_iter()
        .next()
        .expect("manifest segment")
        .key;
    store
        .put(
            &segment_key,
            &serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
        )
        .expect("write manifest segment");
    store
        .put(
            &format!("chains/{}/manifest.version", chain.key_prefix()),
            b"changed",
        )
        .expect("write manifest version");
    write_coverage_index(
        &store,
        &chain,
        &DatasetKey::evm_blocks(),
        "block",
        &selector,
        &range,
        &manifest,
    );

    storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect_err("changed checksum should not use stale cached rows");
    assert_eq!(store.read_count(&object_key), 2);
}

#[test]
fn test_read_through_cache_still_applies_requested_range_filter() {
    let root = temp_storage_root("range-filter");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::enabled(16),
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = block_rows(&[10, 11]);

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
    let object_key = first_data_object_key(&storage);

    storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(10, 11).expect("valid range"),
        )
        .expect("warm cache");
    let filtered = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(11, 11).expect("valid range"),
        )
        .expect("filtered cache read");

    assert_eq!(filtered, block_rows(&[11]));
    assert_eq!(store.read_count(&object_key), 1);
}

#[test]
fn test_read_through_cache_does_not_create_manifest_coverage() {
    let root = temp_storage_root("no-coverage");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store,
        ReadThroughCacheConfig::enabled(16),
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let rows = block_rows(&[1]);

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write rows");
    storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(1, 1).expect("valid range"),
        )
        .expect("warm cache");

    let uncovered = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(2, 2).expect("valid range"),
        )
        .expect("uncovered read");

    assert_eq!(uncovered, block_rows(&[]));
}

#[test]
fn test_read_through_cache_disabled_preserves_object_fetches() {
    let root = temp_storage_root("disabled");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::disabled(),
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(1, 1).expect("valid range");
    let rows = block_rows(&[1]);

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
    let object_key = first_data_object_key(&storage);

    storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range.clone())
        .expect("first read");
    storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect("second read");

    assert_eq!(store.read_count(&object_key), 2);
}

#[test]
fn test_storage_read_plan_skips_empty_coverage_object_fetches() {
    let root = temp_storage_root("empty-coverage-no-get");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::enabled(16),
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(20, 21).expect("valid range");

    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: range.clone(),
            rows: &block_rows(&[]),
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");

    let rows = storage
        .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, range)
        .expect("read empty coverage");

    assert_eq!(rows, block_rows(&[]));
    assert_eq!(store.data_object_read_count(), 0);
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-read-through-cache-{name}-{}",
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

fn block_rows(numbers: &[u64]) -> DatasetRows {
    DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(
            numbers
                .iter()
                .map(|number| BlockHeader {
                    number: *number,
                    hash: format!("0xblock{number}"),
                    parent_hash: "0xparent".to_owned(),
                    timestamp: *number,
                })
                .collect(),
        ),
    )
    .expect("dataset rows")
}

fn first_data_object_key<S>(storage: &DurableStorage<S>) -> String
where
    S: ObjectStore,
{
    storage
        .manifest()
        .expect("manifest")
        .entries
        .into_iter()
        .find_map(|entry| entry.object_key)
        .expect("object key")
}

fn write_coverage_index<S>(
    store: &S,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range_kind: &str,
    selector: &DatasetSelector,
    range: &LedgerRange,
    manifest: &Manifest,
) where
    S: ObjectStore,
{
    let bucket_size = 100_000;
    let bucket_start = (range.start() / bucket_size) * bucket_size;
    let bucket_end = bucket_start + bucket_size - 1;
    let key = format!(
        "chains/{}/coverage-index/{}/{}/{}/safe/{:020}-{:020}.json",
        chain.key_prefix(),
        dataset_key.as_str(),
        range_kind,
        selector.fingerprint(),
        bucket_start,
        bucket_end
    );
    store
        .put(
            &key,
            &serde_json::to_vec_pretty(manifest).expect("coverage index bytes"),
        )
        .expect("write coverage index");
}
