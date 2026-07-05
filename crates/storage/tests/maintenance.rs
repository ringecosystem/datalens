use std::path::PathBuf;

use datalens_chain::{AdapterKey, DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey,
    DatasetRows, LedgerRange, NetworkId, QueryRows,
};
use datalens_storage::{
    DurableStorage, DurableStorageConfig, LocalObjectStore, LocalStorage,
    MaintenanceCompactionConfig, MaintenanceCompactionPressure, MaintenanceCompactionTickStatus,
    MaintenanceIssueKind, MaintenanceOperationMode, Manifest, ObjectListPage, ObjectLockLease,
    ObjectMetadata, ObjectPutIfAbsentResult, ObjectStore, ParquetCompression, StorageWriteRequest,
};
use parquet::{
    basic::Compression,
    file::reader::{FileReader, SerializedFileReader},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

mod support;

use support::CountingObjectStore;

#[derive(Clone, Debug)]
struct FailingDataObjectPutStore {
    inner: LocalObjectStore,
}

#[derive(Clone, Debug)]
struct FailingDataObjectDeleteStore {
    inner: LocalObjectStore,
}

#[derive(Clone, Debug)]
struct FailingCoverageIndexV2DeltaDeleteStore {
    inner: LocalObjectStore,
}

#[derive(Clone, Debug)]
struct FailingManifestSegmentPutStore<S = LocalObjectStore> {
    inner: S,
}

#[derive(Clone, Debug)]
struct CountingListStore {
    inner: LocalObjectStore,
    list_prefixes: Arc<Mutex<Vec<String>>>,
    list_page_prefixes: Arc<Mutex<Vec<String>>>,
}

#[derive(Clone, Debug)]
struct CountingOperationStore {
    inner: LocalObjectStore,
    gets: Arc<Mutex<Vec<String>>>,
    puts: Arc<Mutex<Vec<String>>>,
    deletes: Arc<Mutex<Vec<String>>>,
}

#[derive(Clone, Debug)]
struct FailingManifestSegmentListPageStore {
    inner: LocalObjectStore,
}

#[derive(Clone, Debug)]
struct OverlappingWriteDuringCompactionStore {
    inner: LocalObjectStore,
    injected: Arc<AtomicBool>,
    chain: ChainIdentity,
}

impl FailingDataObjectPutStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
    }
}

impl FailingDataObjectDeleteStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
    }
}

impl FailingCoverageIndexV2DeltaDeleteStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
    }
}

impl FailingManifestSegmentPutStore<LocalObjectStore> {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
    }
}

impl<S> FailingManifestSegmentPutStore<S> {
    fn from_inner(inner: S) -> Self {
        Self { inner }
    }
}

impl CountingListStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            list_prefixes: Arc::new(Mutex::new(Vec::new())),
            list_page_prefixes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn list_count_for_prefix(&self, prefix: &str) -> usize {
        self.list_prefixes
            .lock()
            .expect("list prefixes")
            .iter()
            .filter(|listed_prefix| listed_prefix.as_str() == prefix)
            .count()
    }

    fn list_page_count_for_prefix(&self, prefix: &str) -> usize {
        self.list_page_prefixes
            .lock()
            .expect("list page prefixes")
            .iter()
            .filter(|listed_prefix| listed_prefix.as_str() == prefix)
            .count()
    }
}

impl CountingOperationStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            gets: Arc::new(Mutex::new(Vec::new())),
            puts: Arc::new(Mutex::new(Vec::new())),
            deletes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn delete_count(&self) -> usize {
        self.deletes.lock().expect("deletes").len()
    }

    fn get_keys(&self) -> Vec<String> {
        self.gets.lock().expect("gets").clone()
    }

    fn data_object_get_keys(&self) -> Vec<String> {
        self.gets
            .lock()
            .expect("gets")
            .iter()
            .filter(|key| key.contains("/datasets/"))
            .cloned()
            .collect()
    }

    fn reset_gets(&self) {
        self.gets.lock().expect("gets").clear();
    }
}

impl FailingManifestSegmentListPageStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
    }
}

impl OverlappingWriteDuringCompactionStore {
    fn new(root: PathBuf, chain: ChainIdentity) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            injected: Arc::new(AtomicBool::new(false)),
            chain,
        }
    }

    fn injected(&self) -> bool {
        self.injected.load(Ordering::SeqCst)
    }
}

impl ObjectStore for FailingDataObjectPutStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        if key.contains("/datasets/") && (key.ends_with(".json") || key.ends_with(".parquet")) {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "injected data object write failure",
            ));
        }
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        if key.contains("/datasets/") && (key.ends_with(".json") || key.ends_with(".parquet")) {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "injected data object write failure",
            ));
        }
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

impl ObjectStore for FailingDataObjectDeleteStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        if key.contains("/datasets/") && (key.ends_with(".json") || key.ends_with(".parquet")) {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "injected data object delete failure",
            ));
        }
        self.inner.delete(key)
    }
}

impl ObjectStore for FailingCoverageIndexV2DeltaDeleteStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        if key.contains("/coverage-index-v2/deltas/") {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "injected coverage index v2 delta delete failure",
            ));
        }
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

impl<S: ObjectStore> ObjectStore for FailingManifestSegmentPutStore<S> {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        if key.contains("/manifest-segments/") && key.ends_with(".json") {
            return Err(DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                "injected manifest segment write failure",
            ));
        }
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

impl ObjectStore for CountingListStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.list_prefixes
            .lock()
            .expect("list prefixes")
            .push(prefix.to_owned());
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.list_page_prefixes
            .lock()
            .expect("list page prefixes")
            .push(prefix.to_owned());
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

impl ObjectStore for CountingOperationStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.gets.lock().expect("gets").push(key.to_owned());
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.puts.lock().expect("puts").push(key.to_owned());
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        self.puts.lock().expect("puts").push(key.to_owned());
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.deletes.lock().expect("deletes").push(key.to_owned());
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

impl ObjectStore for FailingManifestSegmentListPageStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        if prefix.contains("/manifest-segments") {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "injected manifest segment list timeout",
            ));
        }
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

impl ObjectStore for OverlappingWriteDuringCompactionStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)?;
        if key.contains("/compacted/")
            && self
                .injected
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let rows = DatasetRows::new(
                DatasetKey::evm_blocks(),
                QueryRows::EvmBlocks(vec![BlockHeader {
                    number: 50,
                    hash: "0xreplacement50".to_owned(),
                    parent_hash: "0xparent".to_owned(),
                    timestamp: 50,
                }]),
            )
            .expect("replacement rows");
            DurableStorage::from_object_store(self.inner.clone()).write_rows_replacing_existing(
                StorageWriteRequest {
                    chain: &self.chain,
                    dataset_key: DatasetKey::evm_blocks(),
                    selector: &DatasetSelector::all(),
                    range: LedgerRange::blocks(50, 50).expect("range"),
                    rows: &rows,
                    finality_level: FinalityLevel::Safe,
                    record_empty_coverage: true,
                },
            )?;
        }
        Ok(())
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, DatalensError> {
        let result = self.inner.put_if_absent(key, bytes)?;
        if result == ObjectPutIfAbsentResult::Created
            && key.contains("/compacted/")
            && self
                .injected
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let rows = DatasetRows::new(
                DatasetKey::evm_blocks(),
                QueryRows::EvmBlocks(vec![BlockHeader {
                    number: 50,
                    hash: "0xreplacement50".to_owned(),
                    parent_hash: "0xparent".to_owned(),
                    timestamp: 50,
                }]),
            )
            .expect("replacement rows");
            DurableStorage::from_object_store(self.inner.clone()).write_rows_replacing_existing(
                StorageWriteRequest {
                    chain: &self.chain,
                    dataset_key: DatasetKey::evm_blocks(),
                    selector: &DatasetSelector::all(),
                    range: LedgerRange::blocks(50, 50).expect("range"),
                    rows: &rows,
                    finality_level: FinalityLevel::Safe,
                    record_empty_coverage: true,
                },
            )?;
        }
        Ok(result)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

#[test]
fn test_maintenance_check_reports_missing_object() {
    let storage = LocalStorage::new(temp_storage_root("missing-object"));
    let chain = test_chain();
    let written = write_block_object(&storage, &chain, 1, FinalityLevel::Safe);
    storage
        .object_store()
        .delete(&written)
        .expect("delete object");

    let report = storage.maintenance_report().expect("maintenance report");

    assert!(report.read_only);
    assert_eq!(report.check.issues.len(), 1);
    assert_eq!(
        report.check.issues[0].issue_kind,
        MaintenanceIssueKind::MissingObject
    );
    assert_eq!(
        report.check.issues[0].object_key.as_deref(),
        Some(written.as_str())
    );
}

#[test]
fn test_maintenance_check_reports_manifest_decode_failure() {
    let storage = LocalStorage::new(temp_storage_root("bad-manifest"));
    std::fs::create_dir_all(storage.root().join("chains/evm/ethereum/1"))
        .expect("create manifest parent");
    std::fs::write(storage.manifest_path(&test_chain()), b"{not-json").expect("write bad manifest");

    let report = storage.maintenance_report().expect("maintenance report");

    assert_eq!(report.check.issues.len(), 1);
    assert_eq!(
        report.check.issues[0].issue_kind,
        MaintenanceIssueKind::ManifestDecodeFailure
    );
    assert_eq!(
        report.check.issues[0].object_key.as_deref(),
        Some("chains/evm/ethereum/1/manifest.json")
    );
}

#[test]
fn test_maintenance_check_reports_size_checksum_and_decode_failures() {
    let storage = LocalStorage::new(temp_storage_root("bad-object"));
    let chain = test_chain();
    let object_key = write_block_object(&storage, &chain, 2, FinalityLevel::Safe);
    storage
        .object_store()
        .put(&object_key, b"not parquet")
        .expect("corrupt object");

    let report = storage.maintenance_report().expect("maintenance report");
    let issue_kinds = report
        .check
        .issues
        .iter()
        .map(|issue| issue.issue_kind)
        .collect::<Vec<_>>();

    assert!(issue_kinds.contains(&MaintenanceIssueKind::ObjectSizeMismatch));
    assert!(issue_kinds.contains(&MaintenanceIssueKind::ObjectChecksumMismatch));
    assert!(issue_kinds.contains(&MaintenanceIssueKind::ObjectDecodeFailure));
}

#[test]
fn test_compaction_candidates_only_include_compatible_adjacent_data_entries() {
    let storage = LocalStorage::new(temp_storage_root("compaction-candidates"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 10, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 11, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 12, FinalityLevel::Finalized);
    write_empty_coverage(&storage, &chain, 13, FinalityLevel::Safe);

    let report = storage.maintenance_report().expect("maintenance report");

    assert_eq!(report.compaction.candidates.len(), 1);
    let candidate = &report.compaction.candidates[0];
    assert_eq!(candidate.entry_count, 2);
    assert_eq!(candidate.range.start(), 10);
    assert_eq!(candidate.range.end(), 11);
    assert_eq!(candidate.finality_level.as_str(), "safe");
}

#[test]
fn test_compaction_candidates_do_not_mix_selector_canonical_keys() {
    let storage = LocalStorage::new(temp_storage_root("compaction-selector-canonical"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 14, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 15, FinalityLevel::Safe);

    let mut manifest = read_manifest_json(&storage, &chain);
    manifest["entries"][1]["selector_canonical_key"] =
        serde_json::Value::String("selector-alias".to_owned());
    write_manifest_json(&storage, &chain, manifest);

    let report = storage.maintenance_report().expect("maintenance report");

    assert!(report.compaction.candidates.is_empty());
}

#[test]
fn test_compaction_candidate_builder_targets_object_size_instead_of_fixed_range_count() {
    let storage = LocalStorage::new(temp_storage_root("compaction-target-size-candidates"));
    let chain = test_chain();
    for number in 100..220 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    let manifest = storage.manifest().expect("manifest");
    let source_bytes = manifest
        .entries
        .iter()
        .filter_map(|entry| entry.object_size_bytes)
        .collect::<Vec<_>>();
    let per_object_bytes = source_bytes
        .iter()
        .copied()
        .max()
        .expect("source object bytes");
    let target_object_bytes = per_object_bytes.saturating_mul(80);
    let max_output_object_bytes = target_object_bytes.saturating_add(per_object_bytes);

    let report = storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            target_object_bytes,
            max_output_object_bytes,
            max_input_objects_per_candidate: 512,
            max_input_bytes_per_candidate: u64::MAX,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_concurrent_candidates: 8,
            max_manifest_entries_per_tick: 20_000,
            max_gets_per_tick: 512,
            cleanup_enabled: false,
            delete_source_objects: false,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    assert!(
        report.candidate_count <= 2,
        "target-size builder should reduce 120 one-block objects to a small candidate set"
    );
    assert!(
        report
            .candidates
            .iter()
            .any(|candidate| candidate.entry_count > 32)
    );
    assert_eq!(report.processed_candidates, report.candidate_count);
    let manifest = storage.manifest().expect("compacted manifest");
    assert!(manifest.entries.len() <= 2);
    for entry in &manifest.entries {
        assert!(
            entry.object_size_bytes.expect("compacted size") <= max_output_object_bytes,
            "compacted object should stay within the configured output cap"
        );
    }
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(100, 219).expect("range"),
        )
        .expect("read compacted rows");
    assert_eq!(rows.row_count(), 120);
}

#[test]
fn test_maintenance_check_reports_contradictory_coverage() {
    let storage = LocalStorage::new(temp_storage_root("contradictory-coverage"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 30, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 30, FinalityLevel::Finalized);

    let report = storage.maintenance_report().expect("maintenance report");

    assert!(
        report
            .check
            .issues
            .iter()
            .any(|issue| issue.issue_kind == MaintenanceIssueKind::ContradictoryCoverage)
    );
}

#[test]
fn test_retention_dry_run_protects_current_manifest_objects() {
    let storage = LocalStorage::new(temp_storage_root("retention"));
    let chain = test_chain();
    let object_key = write_block_object(&storage, &chain, 20, FinalityLevel::Safe);
    let coverage_index_v2_key = format!(
        "chains/{}/coverage-index-v2/deltas/exact/evm_blocks/block/all/safe/00000000000000000000-00000000000000099999/0001.json",
        chain.key_prefix()
    );
    storage
        .object_store()
        .put(&coverage_index_v2_key, b"{}")
        .expect("write coverage index v2 object");
    let before = storage.object_store().list("chains").expect("before list");

    let report = storage.maintenance_report().expect("maintenance report");
    let after = storage.object_store().list("chains").expect("after list");

    assert_eq!(report.mode, MaintenanceOperationMode::DryRun);
    assert!(
        report
            .retention
            .protected_current_objects
            .contains(&object_key)
    );
    assert!(
        !report
            .retention
            .delete_candidates
            .contains(&coverage_index_v2_key)
    );
    assert!(report.retention.delete_candidates.is_empty());
    assert_eq!(before, after);
}

#[test]
fn test_compaction_merges_adjacent_small_objects_and_retains_old_objects() {
    let storage = LocalStorage::new(temp_storage_root("execute-compaction"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 50, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 51, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: false,
            delete_source_objects: false,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    assert!(!report.read_only);
    assert_eq!(
        report.tick_status,
        MaintenanceCompactionTickStatus::Completed
    );
    assert_eq!(report.candidates.len(), 1);
    assert_eq!(report.candidate_count, 1);
    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
    assert_eq!(report.compacted_rows, 2);

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    let compacted_entry = manifest
        .entries
        .iter()
        .find(|entry| entry.range == LedgerRange::blocks(50, 51).expect("range"))
        .expect("compacted entry");
    assert_eq!(compacted_entry.row_count, 2);
    let compacted_object = compacted_entry
        .object_key
        .as_ref()
        .expect("compacted object key");
    assert!(compacted_object.contains("/compacted/"));
    assert_ne!(compacted_object, &first_object);
    assert_ne!(compacted_object, &second_object);

    assert!(
        storage
            .object_store()
            .exists(&first_object)
            .expect("first exists")
    );
    assert!(
        storage
            .object_store()
            .exists(&second_object)
            .expect("second exists")
    );
    assert!(
        storage
            .object_store()
            .exists(compacted_object)
            .expect("compacted exists")
    );
    let manifest_segments = manifest_segment_keys(&storage, &chain);
    assert_eq!(
        manifest_segments.len(),
        1,
        "compaction should replace covered manifest segments"
    );

    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(50, 51).expect("range"),
        )
        .expect("read compacted rows");
    assert_eq!(rows.row_count(), 2);
    assert!(
        storage
            .maintenance_report()
            .expect("maintenance")
            .check
            .issues
            .is_empty()
    );
}

#[test]
fn test_compaction_checkpoint_failure_stops_before_publish() {
    let storage = LocalStorage::new(temp_storage_root("compaction-checkpoint-failure"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 50, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 51, FinalityLevel::Safe);
    let checkpoint_called = AtomicBool::new(false);

    let error = storage
        .compact_small_objects_for_chain_with_checkpoint(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 8,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: false,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
            || {
                checkpoint_called.store(true, Ordering::Relaxed);
                Err(DatalensError::new(
                    DatalensErrorKind::StorageWriteFailure,
                    "leader lock renewal failed",
                ))
            },
        )
        .expect_err("checkpoint failure stops compaction");

    assert!(checkpoint_called.load(Ordering::Relaxed));
    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert!(
        storage
            .object_store()
            .exists(&first_object)
            .expect("first source exists")
    );
    assert!(
        storage
            .object_store()
            .exists(&second_object)
            .expect("second source exists")
    );
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 2);
}

#[test]
fn test_compaction_checkpoint_failure_inside_manifest_publish_keeps_sources_current() {
    let storage = LocalStorage::new(temp_storage_root("compaction-checkpoint-publish-failure"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 52, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 53, FinalityLevel::Safe);
    let old_manifest = storage.manifest().expect("old manifest");
    let old_segments = manifest_segment_keys(&storage, &chain);
    let checkpoint_calls = AtomicUsize::new(0);

    let error = storage
        .compact_small_objects_for_chain_with_checkpoint(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 8,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: true,
                delete_source_objects: true,
                source_delete_grace_ms: 0,
                ..MaintenanceCompactionConfig::default()
            },
            || {
                let call = checkpoint_calls.fetch_add(1, Ordering::Relaxed);
                if call >= 2 {
                    return Err(DatalensError::new(
                        DatalensErrorKind::StorageWriteFailure,
                        "leader lock renewal failed",
                    ));
                }
                Ok(())
            },
        )
        .expect_err("checkpoint failure inside manifest publish stops compaction");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert!(checkpoint_calls.load(Ordering::Relaxed) >= 3);
    assert!(
        storage
            .object_store()
            .list(&format!("chains/{}/datasets", chain.key_prefix()))
            .expect("compacted objects")
            .iter()
            .any(|object| object.key.contains("/compacted/")),
        "compacted object should be created before the helper checkpoint fails"
    );
    assert_eq!(storage.manifest().expect("manifest"), old_manifest);
    assert_eq!(manifest_segment_keys(&storage, &chain), old_segments);
    assert!(
        storage
            .object_store()
            .exists(&first_object)
            .expect("first source exists")
    );
    assert!(
        storage
            .object_store()
            .exists(&second_object)
            .expect("second source exists")
    );
}

#[test]
fn test_compaction_skips_candidate_when_overlapping_write_publishes_before_manifest_update() {
    let storage = LocalStorage::new(temp_storage_root("compaction-overlap-write"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 50, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 51, FinalityLevel::Safe);
    let injecting_store =
        OverlappingWriteDuringCompactionStore::new(storage.root().into(), chain.clone());
    let compacting_storage = DurableStorage::from_object_store(injecting_store.clone());

    let report = compacting_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: false,
            delete_source_objects: false,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    assert!(injecting_store.injected());
    assert_eq!(report.candidate_count, 1);
    assert_eq!(report.processed_candidates, 0);
    assert_eq!(report.compacted_objects, 0);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(50, 51).expect("range"),
        )
        .expect("read rows after skipped compaction");
    assert_eq!(rows.row_count(), 2);
    let replacement = storage
        .manifest()
        .expect("manifest")
        .entries
        .into_iter()
        .find(|entry| entry.range == LedgerRange::blocks(50, 50).expect("range"))
        .expect("replacement entry");
    assert_eq!(replacement.row_count, 1);
    assert!(
        replacement
            .object_key
            .as_deref()
            .is_some_and(|object_key| !object_key.contains("/compacted/"))
    );
}

#[test]
fn test_maintenance_report_summarizes_compaction_backlog_by_chain_dataset_selector() {
    let storage = LocalStorage::new(temp_storage_root("compaction-backlog-report"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 10, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 11, FinalityLevel::Safe);
    let other_selector = DatasetSelector::try_other(
        AdapterKey::try_new("test").expect("adapter key"),
        "selector-b",
        "selector-b",
    )
    .expect("selector");
    write_block_object_with_selector(&storage, &chain, &other_selector, 12, FinalityLevel::Safe);

    let report = storage.maintenance_report().expect("maintenance");

    assert_eq!(report.compaction_backlog.small_object_count, 3);
    assert!(report.compaction_backlog.small_object_bytes > 0);
    assert_eq!(report.compaction_backlog.chains.len(), 1);
    let chain_backlog = &report.compaction_backlog.chains[0];
    assert_eq!(chain_backlog.chain, chain);
    assert_eq!(chain_backlog.small_object_count, 3);
    assert!(
        chain_backlog.manifest_segment_count >= 3,
        "each small write should leave an inspectable manifest segment before compaction"
    );
    assert_eq!(chain_backlog.datasets.len(), 1);
    let dataset_backlog = &chain_backlog.datasets[0];
    assert_eq!(dataset_backlog.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(dataset_backlog.small_object_count, 3);
    assert_eq!(dataset_backlog.selectors.len(), 2);
    assert!(
        dataset_backlog
            .selectors
            .iter()
            .any(|selector| selector.selector_canonical_key == "all"
                && selector.small_object_count == 2)
    );
    assert!(
        dataset_backlog
            .selectors
            .iter()
            .any(|selector| selector.selector_canonical_key == "selector-b"
                && selector.small_object_count == 1)
    );
}

#[test]
fn test_maintenance_report_populates_fragmentation_fields() {
    let storage = LocalStorage::new(temp_storage_root("fragmentation-report"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 10, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 11, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 12, FinalityLevel::Safe);
    let delta_keys = list_prefix(
        &storage,
        &format!("chains/{}/coverage-index-v2/deltas", chain.key_prefix()),
    );
    let snapshot_key =
        write_coverage_index_v2_snapshot(&storage, &chain, "snapshot-a", 1, delta_keys.clone());
    write_coverage_index_v2_snapshot_head(&storage, &chain, "head-a", 1, &snapshot_key);
    write_coverage_index_v2_cleanup_record(
        &storage,
        &chain,
        "cleanup-a",
        &snapshot_key,
        delta_keys.clone(),
    );

    let report = storage.maintenance_report().expect("maintenance");

    assert_eq!(report.fragmentation.data_object_small_object_count, 3);
    assert!(report.fragmentation.data_object_small_object_bytes > 0);
    assert!(
        report.fragmentation.manifest_segment_count >= 3,
        "small writes should leave manifest segment fragments"
    );
    assert_eq!(report.fragmentation.coverage_delta_count, delta_keys.len());
    assert!(report.fragmentation.coverage_delta_bytes > 0);
    assert_eq!(report.fragmentation.coverage_snapshot_count, 1);
    assert!(report.fragmentation.coverage_snapshot_age_ms_max > 0);
    assert_eq!(report.fragmentation.coverage_cleanup_record_count, 1);
    assert_eq!(report.fragmentation.coverage_delta_backlog_top.len(), 1);
    let backlog = &report.fragmentation.coverage_delta_backlog_top[0];
    assert_eq!(backlog.chain, chain);
    assert_eq!(backlog.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(backlog.scope_kind, "exact");
    assert_eq!(backlog.scope_class, "all");
    assert_eq!(backlog.selector_fingerprint.as_deref(), Some("all"));
    assert_eq!(backlog.bucket_start, 0);
    assert_eq!(backlog.bucket_end, 99_999);
    assert_eq!(backlog.object_count, delta_keys.len());
    assert_eq!(backlog.bytes, report.fragmentation.coverage_delta_bytes);
}

#[test]
fn test_maintenance_report_sanitizes_semantic_coverage_delta_backlog_scope() {
    let storage = LocalStorage::new(temp_storage_root("fragmentation-semantic-scope"));
    let chain = test_chain();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let topic = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let address_key = format!(
        "chains/{}/coverage-index-v2/deltas/semantic/evm.logs/block/safe/v1/addr/{}/00000000000000000000-00000000000000099999/address.json",
        chain.key_prefix(),
        address
    );
    let topic_key = format!(
        "chains/{}/coverage-index-v2/deltas/semantic/evm.logs/block/safe/v1/topic/0/{}/00000000000000000000-00000000000000099999/topic.json",
        chain.key_prefix(),
        topic
    );
    storage
        .object_store()
        .put(&address_key, b"address delta")
        .expect("write address semantic delta");
    storage
        .object_store()
        .put(&topic_key, b"topic delta")
        .expect("write topic semantic delta");

    let report = storage.maintenance_report().expect("maintenance");
    let encoded = serde_json::to_string(&report).expect("encode report");

    assert_eq!(report.fragmentation.coverage_delta_backlog_top.len(), 2);
    assert!(encoded.contains(r#""scope_kind":"semantic""#));
    assert!(encoded.contains(r#""scope_class":"addr_value""#));
    assert!(encoded.contains(r#""scope_class":"topic_value""#));
    assert!(!encoded.contains(address));
    assert!(!encoded.contains(topic));
    assert!(!encoded.contains("semantic_scope"));
}

#[test]
fn test_maintenance_report_does_not_expose_exact_canonical_selector_key() {
    let storage = LocalStorage::new(temp_storage_root("fragmentation-exact-canonical-scope"));
    let chain = test_chain();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let topic = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let delta_key = format!(
        "chains/{}/coverage-index-v2/deltas/exact/evm.logs/block/addresses={}/topics={}/safe/00000000000000000000-00000000000000099999/exact.json",
        chain.key_prefix(),
        address,
        topic
    );
    storage
        .object_store()
        .put(&delta_key, b"exact canonical delta")
        .expect("write exact canonical delta");

    let report = storage.maintenance_report().expect("maintenance");
    let encoded = serde_json::to_string(&report).expect("encode report");

    assert_eq!(report.fragmentation.coverage_delta_backlog_top.len(), 1);
    assert_eq!(
        report.fragmentation.coverage_delta_backlog_top[0].selector_fingerprint,
        None
    );
    assert!(encoded.contains(r#""scope_kind":"exact""#));
    assert!(encoded.contains(r#""scope_class":"selector""#));
    assert!(!encoded.contains(address));
    assert!(!encoded.contains(topic));
    assert!(!encoded.contains("addresses="));
    assert!(!encoded.contains("topics="));
}

#[test]
fn test_maintenance_report_orders_coverage_delta_backlog_by_bytes_desc() {
    let storage = LocalStorage::new(temp_storage_root("fragmentation-backlog-order"));
    let chain = test_chain();
    let small_key = format!(
        "chains/{}/coverage-index-v2/deltas/exact/evm.blocks/block/all/safe/00000000000000000000-00000000000000099999/small.json",
        chain.key_prefix()
    );
    let large_key = format!(
        "chains/{}/coverage-index-v2/deltas/exact/evm.blocks/block/all/safe/00000000000100000000-00000000000199999/large.json",
        chain.key_prefix()
    );
    storage
        .object_store()
        .put(&small_key, b"small")
        .expect("write small delta");
    storage
        .object_store()
        .put(&large_key, b"larger coverage delta bytes")
        .expect("write large delta");

    let report = storage.maintenance_report().expect("maintenance");

    assert_eq!(report.fragmentation.coverage_delta_backlog_top.len(), 2);
    assert_eq!(
        report.fragmentation.coverage_delta_backlog_top[0].bucket_start,
        100_000_000
    );
    assert_eq!(
        report.fragmentation.coverage_delta_backlog_top[1].bucket_start,
        0
    );
    assert!(
        report.fragmentation.coverage_delta_backlog_top[0].bytes
            > report.fragmentation.coverage_delta_backlog_top[1].bytes
    );
}

#[test]
fn test_compaction_uses_configured_parquet_compression() {
    let storage = LocalStorage::new_with_config(
        temp_storage_root("compaction-zstd"),
        DurableStorageConfig {
            parquet_compression: ParquetCompression::Zstd,
        },
    );
    let chain = test_chain();
    write_block_object(&storage, &chain, 60, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 61, FinalityLevel::Safe);

    storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: false,
            delete_source_objects: false,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    let manifest = storage.manifest().expect("manifest");
    let compacted_entry = manifest
        .entries
        .iter()
        .find(|entry| entry.range == LedgerRange::blocks(60, 61).expect("range"))
        .expect("compacted entry");
    assert_eq!(
        compacted_entry.object_compression,
        Some(ParquetCompression::Zstd)
    );
    let compacted_object = compacted_entry
        .object_key
        .as_ref()
        .expect("compacted object key");
    let compacted_bytes = storage
        .object_store()
        .get(compacted_object)
        .expect("compacted object bytes");
    assert_parquet_compression(&compacted_bytes, Compression::ZSTD(Default::default()));
}

#[test]
fn test_compaction_records_superseded_source_objects_without_deleting_during_grace() {
    let storage = LocalStorage::new(temp_storage_root("compaction-superseded-grace"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 52, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 53, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 60_000,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
    assert_eq!(report.deleted_source_objects, 0);
    assert_eq!(report.source_delete_failures, 0);
    assert!(
        storage
            .object_store()
            .exists(&first_object)
            .expect("first exists")
    );
    assert!(
        storage
            .object_store()
            .exists(&second_object)
            .expect("second exists")
    );
    let reconciliation = storage
        .reconcile_compaction_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                cleanup_enabled: true,
                delete_source_objects: true,
                source_delete_grace_ms: 60_000,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("reconcile during grace");
    assert_eq!(
        reconciliation.stale_source_objects,
        vec![first_object.clone(), second_object.clone()]
    );
    assert_eq!(reconciliation.deleted_stale_source_objects, 0);

    let manifest = storage.manifest().expect("manifest");
    let compacted_entry = manifest
        .entries
        .iter()
        .find(|entry| entry.range == LedgerRange::blocks(52, 53).expect("range"))
        .expect("compacted entry");
    let compacted_object = compacted_entry
        .object_key
        .as_ref()
        .expect("compacted object key");
    assert!(
        storage
            .object_store()
            .exists(compacted_object)
            .expect("compacted exists")
    );

    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(52, 53).expect("range"),
        )
        .expect("read compacted rows");
    assert_eq!(rows.row_count(), 2);
    assert!(
        storage
            .maintenance_report()
            .expect("maintenance")
            .check
            .issues
            .is_empty()
    );
}

#[test]
fn test_compaction_preserves_source_objects_when_cleanup_disabled() {
    let storage = LocalStorage::new(temp_storage_root("execute-compaction-cleanup-disabled"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 54, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 55, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: false,
            delete_source_objects: true,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
    assert_eq!(report.deleted_source_objects, 0);
    assert!(
        storage
            .object_store()
            .exists(&first_object)
            .expect("first exists")
    );
    assert!(
        storage
            .object_store()
            .exists(&second_object)
            .expect("second exists")
    );
}

#[test]
fn test_compaction_preserves_source_objects_for_old_coverage_plan_after_replacement() {
    let store = CountingOperationStore::new(temp_storage_root("old-plan-after-compaction"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let first_object = write_block_object_to_storage(&storage, &chain, 80, FinalityLevel::Safe);
    let second_object = write_block_object_to_storage(&storage, &chain, 81, FinalityLevel::Safe);
    let query_range = LedgerRange::blocks(80, 81).expect("range");
    let old_plan = storage
        .coverage_plan(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            query_range.clone(),
        )
        .expect("old coverage plan");

    storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 60_000,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    assert!(
        storage
            .object_store()
            .exists(&first_object)
            .expect("first source exists")
    );
    assert!(
        storage
            .object_store()
            .exists(&second_object)
            .expect("second source exists")
    );
    store.reset_gets();

    let rows = storage
        .read_rows_with_coverage_plan(
            &old_plan,
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            query_range,
        )
        .expect("read rows through old coverage plan");

    assert_block_numbers(rows, &[80, 81]);
    let gets = store.get_keys();
    assert!(
        gets.contains(&first_object),
        "old coverage plan should still read the first source object"
    );
    assert!(
        gets.contains(&second_object),
        "old coverage plan should still read the second source object"
    );
}

#[test]
fn test_new_query_reads_compacted_object_after_replacement_publish() {
    let store = CountingOperationStore::new(temp_storage_root("new-query-compacted-object"));
    let storage = DurableStorage::from_object_store(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();
    write_block_object_to_storage(&storage, &chain, 82, FinalityLevel::Safe);
    write_block_object_to_storage(&storage, &chain, 83, FinalityLevel::Safe);

    storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: false,
            delete_source_objects: false,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");
    let compacted_object = storage.manifest().expect("manifest").entries[0]
        .object_key
        .clone()
        .expect("compacted object");
    assert!(
        compacted_object.contains("/compacted/"),
        "published replacement should point at the compacted object"
    );
    store.reset_gets();

    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(82, 83).expect("range"),
        )
        .expect("read compacted rows");

    assert_block_numbers(rows, &[82, 83]);
    assert_eq!(
        store.data_object_get_keys(),
        vec![compacted_object],
        "new reads should use the compacted replacement object"
    );
}

#[test]
fn test_compaction_cleanup_deletes_superseded_sources_after_grace_with_tick_limit() {
    let storage = LocalStorage::new(temp_storage_root("compaction-superseded-cleanup-limit"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 54, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 55, FinalityLevel::Safe);

    storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");

    let first = storage
        .reconcile_compaction_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                cleanup_enabled: true,
                delete_source_objects: true,
                source_delete_grace_ms: 0,
                max_deletes_per_tick: 1,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("first cleanup tick");

    assert_eq!(first.stale_source_objects.len(), 2);
    assert_eq!(first.deleted_stale_source_objects, 1);
    assert_eq!(first.deleted_stale_cleanup_records, 1);
    let remaining_exists = storage
        .object_store()
        .exists(&first_object)
        .expect("first exists")
        || storage
            .object_store()
            .exists(&second_object)
            .expect("second exists");
    assert!(remaining_exists);

    let second = storage
        .reconcile_compaction_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                cleanup_enabled: true,
                delete_source_objects: true,
                source_delete_grace_ms: 0,
                max_deletes_per_tick: 1,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("second cleanup tick");

    assert_eq!(second.deleted_stale_source_objects, 1);
    assert!(
        !storage
            .object_store()
            .exists(&first_object)
            .expect("first exists")
    );
    assert!(
        !storage
            .object_store()
            .exists(&second_object)
            .expect("second exists")
    );
}

#[test]
fn test_compaction_publish_failure_keeps_old_manifest_index_and_objects_queryable() {
    let storage = LocalStorage::new(temp_storage_root("failed-compaction-publish-safe"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 68, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 69, FinalityLevel::Safe);
    let old_manifest = storage.manifest().expect("old manifest");
    let old_segments = manifest_segment_keys(&storage, &chain);
    let old_plan = storage
        .coverage_plan(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(68, 69).expect("range"),
        )
        .expect("old coverage plan");
    let failing_storage = DurableStorage::from_object_store(FailingManifestSegmentPutStore::new(
        storage.root().into(),
    ));

    let error = failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect_err("manifest publish failure should not replace coverage");

    assert_eq!(error.kind, DatalensErrorKind::ManifestUpdateFailure);
    assert_eq!(storage.manifest().expect("manifest"), old_manifest);
    assert_eq!(manifest_segment_keys(&storage, &chain), old_segments);
    assert!(
        storage
            .object_store()
            .exists(&first_object)
            .expect("first source exists")
    );
    assert!(
        storage
            .object_store()
            .exists(&second_object)
            .expect("second source exists")
    );
    assert_eq!(
        storage
            .coverage_plan(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(68, 69).expect("range"),
            )
            .expect("current coverage plan"),
        old_plan
    );

    let rows = storage
        .read_rows_with_coverage_plan(
            &old_plan,
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(68, 69).expect("range"),
        )
        .expect("old coverage plan remains readable");
    assert_block_numbers(rows, &[68, 69]);
}

#[test]
fn test_compaction_replacement_write_failure_leaves_old_manifest_readable() {
    let storage = LocalStorage::new(temp_storage_root("failed-compaction-write"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 70, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 71, FinalityLevel::Safe);
    let failing_storage =
        DurableStorage::from_object_store(FailingDataObjectPutStore::new(storage.root().into()));

    let error = failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect_err("compaction replacement write failure");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 2);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(70, 71).expect("range"),
        )
        .expect("old manifest entries remain readable");
    assert_eq!(rows.row_count(), 2);
}

#[test]
fn test_compaction_retry_reuses_existing_identical_compacted_object_without_second_put() {
    let storage = LocalStorage::new(temp_storage_root("compaction-retry-existing-identical"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 74, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 75, FinalityLevel::Safe);
    let counting_store = CountingObjectStore::new(LocalObjectStore::new(storage.root()));
    let failing_storage = DurableStorage::from_object_store(
        FailingManifestSegmentPutStore::from_inner(counting_store.clone()),
    );

    let error = failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect_err("manifest publish failure should leave compacted object orphaned");

    assert_eq!(error.kind, DatalensErrorKind::ManifestUpdateFailure);
    let compacted_keys = compacted_object_keys(&storage, &chain);
    assert_eq!(compacted_keys.len(), 1);
    let compacted_key = &compacted_keys[0];
    assert_eq!(counting_store.put_count(compacted_key), 0);
    assert_eq!(counting_store.put_if_absent_count(compacted_key), 1);

    let retry_storage = DurableStorage::from_object_store(counting_store.clone());
    let report = retry_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("retry compaction reuses existing compacted object");

    assert_eq!(report.compacted_objects, 1);
    assert_eq!(counting_store.put_count(compacted_key), 0);
    assert_eq!(counting_store.put_if_absent_count(compacted_key), 1);
    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(
        manifest.entries[0].object_key.as_deref(),
        Some(compacted_key.as_str())
    );
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(74, 75).expect("range"),
        )
        .expect("read reused compacted rows");
    assert_block_numbers(rows, &[74, 75]);
}

#[test]
fn test_compaction_existing_corrupt_compacted_object_fails_before_manifest_publish() {
    let storage = LocalStorage::new(temp_storage_root("compaction-existing-corrupt"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 76, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 77, FinalityLevel::Safe);
    let old_manifest = storage.manifest().expect("old manifest");
    let counting_store = CountingObjectStore::new(LocalObjectStore::new(storage.root()));
    let failing_storage = DurableStorage::from_object_store(
        FailingManifestSegmentPutStore::from_inner(counting_store.clone()),
    );

    failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect_err("manifest publish failure should leave compacted object orphaned");

    let compacted_keys = compacted_object_keys(&storage, &chain);
    assert_eq!(compacted_keys.len(), 1);
    let compacted_key = &compacted_keys[0];
    storage
        .object_store()
        .put(compacted_key, b"corrupt compacted bytes")
        .expect("corrupt existing compacted object");

    let error = DurableStorage::from_object_store(counting_store.clone())
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect_err("corrupt existing compacted object should fail");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert_eq!(storage.manifest().expect("manifest"), old_manifest);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(76, 77).expect("range"),
        )
        .expect("old rows remain readable");
    assert_block_numbers(rows, &[76, 77]);
}

#[test]
fn test_compaction_source_delete_failure_leaves_reads_working() {
    let storage = LocalStorage::new(temp_storage_root("failed-compaction-delete"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 72, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 73, FinalityLevel::Safe);
    let failing_storage =
        DurableStorage::from_object_store(FailingDataObjectDeleteStore::new(storage.root().into()));

    let report = failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compaction reports source delete failure");

    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
    assert_eq!(report.deleted_source_objects, 0);
    assert_eq!(report.source_delete_failures, 0);
    let reconciliation = failing_storage
        .reconcile_compaction_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                cleanup_enabled: true,
                delete_source_objects: true,
                source_delete_grace_ms: 0,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("cleanup reports source delete failure");
    assert_eq!(reconciliation.deleted_stale_source_objects, 0);
    assert_eq!(reconciliation.delete_failures, 2);
    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(72, 73).expect("range"),
        )
        .expect("compacted manifest remains readable");
    assert_eq!(rows.row_count(), 2);

    let new_rows = block_rows(&[74]);
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(74, 74).expect("range"),
            rows: &new_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write after cleanup failure");
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(72, 74).expect("range"),
        )
        .expect("read after cleanup failure");
    assert_block_numbers(rows, &[72, 73, 74]);
}

#[test]
fn test_compaction_reconciliation_deletes_unpublished_compacted_orphan() {
    let storage = LocalStorage::new(temp_storage_root("orphan-compacted-recovery"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 74, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 75, FinalityLevel::Safe);
    let failing_storage = DurableStorage::from_object_store(FailingManifestSegmentPutStore::new(
        storage.root().into(),
    ));

    let error = failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect_err("manifest publish crash leaves compacted object");

    assert_eq!(error.kind, DatalensErrorKind::ManifestUpdateFailure);
    let orphan_key = compacted_object_keys(&storage, &chain)
        .into_iter()
        .next()
        .expect("orphan compacted object");
    let report = storage.maintenance_report().expect("maintenance report");
    assert_eq!(
        report.compaction_reconciliation.orphan_compacted_objects,
        vec![orphan_key.clone()]
    );

    let reconciliation = storage
        .reconcile_compaction_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                cleanup_enabled: true,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("reconcile compaction");

    assert_eq!(reconciliation.deleted_orphan_compacted_objects, 1);
    assert!(
        !storage
            .object_store()
            .exists(&orphan_key)
            .expect("orphan exists")
    );
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(74, 75).expect("range"),
        )
        .expect("source manifest entries remain readable");
    assert_eq!(rows.row_count(), 2);
}

#[test]
fn test_compaction_reconciliation_preserves_unpublished_orphan_when_cleanup_disabled() {
    let storage = LocalStorage::new(temp_storage_root("orphan-compacted-cleanup-disabled"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 74, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 75, FinalityLevel::Safe);
    let failing_storage = DurableStorage::from_object_store(FailingManifestSegmentPutStore::new(
        storage.root().into(),
    ));

    failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_concurrent_candidates: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: false,
            delete_source_objects: true,
            ..MaintenanceCompactionConfig::default()
        })
        .expect_err("manifest publish crash leaves compacted object");

    let orphan_key = compacted_object_keys(&storage, &chain)
        .into_iter()
        .next()
        .expect("orphan compacted object");
    let reconciliation = storage
        .reconcile_compaction_for_chain(&chain, MaintenanceCompactionConfig::default())
        .expect("reconcile compaction");

    assert_eq!(
        reconciliation.orphan_compacted_objects,
        vec![orphan_key.clone()]
    );
    assert_eq!(reconciliation.deleted_orphan_compacted_objects, 0);
    assert!(
        storage
            .object_store()
            .exists(&orphan_key)
            .expect("orphan exists")
    );
}

#[test]
fn test_compaction_reconciliation_retries_stale_source_cleanup_after_restart() {
    let storage = LocalStorage::new(temp_storage_root("stale-source-cleanup-recovery"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 76, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 77, FinalityLevel::Safe);
    let failing_storage =
        DurableStorage::from_object_store(FailingDataObjectDeleteStore::new(storage.root().into()));

    failing_storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            source_delete_grace_ms: 0,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compaction with cleanup failures");

    let report = storage.maintenance_report().expect("maintenance report");
    assert_eq!(
        report.compaction_reconciliation.stale_source_objects,
        vec![first_object.clone(), second_object.clone()]
    );

    let reconciliation = storage
        .reconcile_compaction_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                cleanup_enabled: true,
                delete_source_objects: true,
                source_delete_grace_ms: 0,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("reconcile compaction");

    assert_eq!(reconciliation.deleted_stale_source_objects, 2);
    assert!(
        !storage
            .object_store()
            .exists(&first_object)
            .expect("first exists")
    );
    assert!(
        !storage
            .object_store()
            .exists(&second_object)
            .expect("second exists")
    );
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(76, 77).expect("range"),
        )
        .expect("compacted manifest remains readable");
    assert_eq!(rows.row_count(), 2);
}

#[test]
fn test_compaction_reconciliation_never_deletes_current_manifest_objects() {
    let storage = LocalStorage::new(temp_storage_root("protect-current-reconciliation"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 78, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 79, FinalityLevel::Safe);
    storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_input_objects_per_candidate: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            cleanup_enabled: true,
            delete_source_objects: true,
            ..MaintenanceCompactionConfig::default()
        })
        .expect("compact small objects");
    let current_object = storage.manifest().expect("manifest").entries[0]
        .object_key
        .clone()
        .expect("current object");

    let reconciliation = storage
        .reconcile_compaction_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                cleanup_enabled: true,
                delete_source_objects: true,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("reconcile compaction");

    assert_eq!(reconciliation.deleted_orphan_compacted_objects, 0);
    assert_eq!(reconciliation.deleted_stale_source_objects, 0);
    assert!(
        storage
            .object_store()
            .exists(&current_object)
            .expect("current object exists")
    );
}

#[test]
fn test_compaction_tick_stops_after_candidate_budget_and_reports_partial() {
    let storage = LocalStorage::new(temp_storage_root("candidate-budget"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 80, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 81, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 90, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 91, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 1,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: false,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("bounded compaction");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(report.candidate_count, 2);
    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
}

#[test]
fn test_compaction_report_exposes_backlog_estimates_and_tick_summary() {
    let storage = LocalStorage::new(temp_storage_root("compaction-observability"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 180, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 181, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 190, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 191, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 1,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: true,
                delete_source_objects: true,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("observable compaction");

    assert_eq!(report.tick_summary.input_objects, 2);
    assert_eq!(report.tick_summary.output_objects, 1);
    assert_eq!(report.tick_summary.deleted_source_objects, 0);
    assert!(report.tick_summary.deleted_manifest_segments > 0);
    assert!(report.tick_summary.duration_ms > 0);
    assert_eq!(report.pause_reason.as_deref(), None);
    assert_eq!(report.candidate_backlog, 1);
    assert_eq!(report.backlog.len(), 1);
    assert!(report.backlog.iter().any(|scope| scope.chain == chain
        && scope.dataset_key == DatasetKey::evm_blocks()
        && scope.selector_fingerprint == "all"
        && scope.candidate_backlog == 1
        && scope.small_objects == 2));
}

#[test]
fn test_compaction_pause_report_exposes_backpressure_reason() {
    let storage = LocalStorage::new(temp_storage_root("compaction-pause-reason"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 184, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 185, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                query_latency_pause_threshold_ms: 100,
                pressure: MaintenanceCompactionPressure {
                    query_latency_ms: Some(250),
                    write_latency_ms: None,
                },
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("pressure-paused compaction");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Paused);
    assert_eq!(report.pause_reason.as_deref(), Some("query_latency"));
    assert_eq!(report.candidate_backlog, 1);
    assert_eq!(report.tick_summary.input_objects, 0);
    assert_eq!(report.tick_summary.output_objects, 0);
}

#[test]
fn test_compaction_tick_stops_before_exceeding_object_store_operation_budgets() {
    let storage = LocalStorage::new(temp_storage_root("operation-budgets"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 82, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 83, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 92, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 93, FinalityLevel::Safe);
    let counting_store = CountingOperationStore::new(storage.root().into());
    let counting_storage = DurableStorage::from_object_store(counting_store.clone());

    let report = counting_storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_concurrent_candidates: 1,
                max_manifest_entries_per_tick: 20_000,
                max_gets_per_tick: 4,
                max_puts_per_tick: 2,
                max_deletes_per_tick: 0,
                delete_source_objects: true,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("budgeted compaction");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(report.candidate_count, 2);
    assert_eq!(report.processed_candidates, 0);
    assert_eq!(report.compacted_objects, 0);
    assert_eq!(report.deleted_source_objects, 0);
    assert_eq!(report.get_operations, 0);
    assert_eq!(report.put_operations, 0);
    assert_eq!(report.delete_operations, 0);
    assert_eq!(counting_store.delete_count(), 0);
}

#[test]
fn test_compaction_pauses_when_query_latency_exceeds_threshold_and_reads_still_work() {
    let storage = LocalStorage::new(temp_storage_root("query-pressure-pause"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 84, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 85, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                query_latency_pause_threshold_ms: 100,
                pressure: MaintenanceCompactionPressure {
                    query_latency_ms: Some(250),
                    write_latency_ms: None,
                },
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("pressure-paused compaction");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Paused);
    assert_eq!(report.processed_candidates, 0);
    assert_eq!(report.compacted_objects, 0);
    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(84, 85).expect("range"),
        )
        .expect("query path still works");
    assert_eq!(rows.row_count(), 2);
}

#[test]
fn test_compaction_pauses_when_write_latency_exceeds_threshold() {
    let storage = LocalStorage::new(temp_storage_root("write-pressure-pause"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 86, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 87, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                write_latency_pause_threshold_ms: 100,
                pressure: MaintenanceCompactionPressure {
                    query_latency_ms: None,
                    write_latency_ms: Some(250),
                },
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("write-pressure-paused compaction");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Paused);
    assert_eq!(report.pause_reason.as_deref(), Some("write_latency"));
    assert_eq!(report.processed_candidates, 0);
    assert_eq!(
        storage
            .manifest()
            .expect("manifest")
            .entries
            .iter()
            .filter(|entry| entry.object_key.is_some())
            .count(),
        2,
        "write pressure pause must not publish a compacted replacement"
    );
}

#[test]
fn test_compaction_tick_does_not_reload_manifest_per_candidate() {
    let storage = LocalStorage::new(temp_storage_root("no-repeated-reload"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 100, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 101, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 110, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 111, FinalityLevel::Safe);
    let counting_store = CountingListStore::new(storage.root().into());
    let counting_storage = DurableStorage::from_object_store(counting_store.clone());
    let manifest_prefix = format!("chains/{}/manifest-segments", chain.key_prefix());

    let report = counting_storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_concurrent_candidates: 8,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: false,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("bounded compaction");

    assert_eq!(report.candidate_count, 2);
    assert_eq!(report.processed_candidates, 2);
    assert_eq!(counting_store.list_count_for_prefix(&manifest_prefix), 0);
    assert_eq!(
        counting_store.list_page_count_for_prefix(&manifest_prefix),
        0
    );
    assert_eq!(
        counting_store.list_page_count_for_prefix(&format!(
            "chains/{}/metadata/compaction-queue",
            chain.key_prefix()
        )),
        1
    );
}

#[test]
fn test_compaction_tick_uses_queue_when_manifest_segment_list_times_out() {
    let storage = LocalStorage::new(temp_storage_root("queue-avoids-list-timeout"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 104, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 105, FinalityLevel::Safe);
    let failing_store = FailingManifestSegmentListPageStore::new(storage.root().into());
    let failing_storage = DurableStorage::from_object_store(failing_store);

    let report = failing_storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: false,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("queue-backed compaction");

    assert_eq!(report.candidate_count, 1);
    assert_eq!(report.processed_candidates, 1);
}

#[test]
fn test_compaction_tick_cleans_consumed_queue_entries() {
    let storage = LocalStorage::new(temp_storage_root("queue-cleanup"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 106, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 107, FinalityLevel::Safe);
    let queue_prefix = format!("chains/{}/metadata/compaction-queue", chain.key_prefix());

    assert_eq!(
        storage
            .object_store()
            .list(&queue_prefix)
            .expect("queue entries before compaction")
            .len(),
        2
    );
    let before_queue_keys = storage
        .object_store()
        .list(&queue_prefix)
        .expect("queue entries before compaction")
        .into_iter()
        .map(|object| object.key)
        .collect::<Vec<_>>();

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: true,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("queue cleanup compaction");

    assert_eq!(report.processed_candidates, 1);
    let after_queue_keys = storage
        .object_store()
        .list(&queue_prefix)
        .expect("queue entries after compaction")
        .into_iter()
        .map(|object| object.key)
        .collect::<Vec<_>>();
    assert_eq!(after_queue_keys.len(), 1);
    assert!(after_queue_keys[0].contains("00000000000000000106-00000000000000000107"));
    for key in before_queue_keys {
        assert!(!after_queue_keys.contains(&key));
    }
}

#[test]
fn test_compaction_queue_advances_after_non_candidate_scope() {
    let storage = LocalStorage::new(temp_storage_root("queue-non-candidate-scope"));
    let chain = test_chain();
    let selector_a = DatasetSelector::try_other(
        AdapterKey::try_new("test").expect("adapter key"),
        "selector-a",
        "selector-a",
    )
    .expect("selector a");
    let selector_b = DatasetSelector::try_other(
        AdapterKey::try_new("test").expect("adapter key"),
        "selector-b",
        "selector-b",
    )
    .expect("selector b");
    write_block_object_with_selector(&storage, &chain, &selector_a, 108, FinalityLevel::Safe);
    write_block_object_with_selector(&storage, &chain, &selector_b, 109, FinalityLevel::Safe);
    write_block_object_with_selector(&storage, &chain, &selector_b, 110, FinalityLevel::Safe);
    let config = MaintenanceCompactionConfig {
        min_object_bytes: u64::MAX,
        max_input_objects_per_candidate: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 1,
        max_manifest_entries_per_tick: 20_000,
        cleanup_enabled: false,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    let first = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("first tick");
    assert_eq!(first.candidate_count, 0);
    assert_eq!(first.processed_candidates, 0);

    let second = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("second tick");
    assert_eq!(second.candidate_count, 1);
    assert_eq!(second.processed_candidates, 1);
    assert_eq!(second.compacted_objects, 1);
}

#[test]
fn test_compaction_coverage_index_v2_snapshot_without_head_keeps_deltas_queryable() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-snapshot-no-head"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 10, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 11, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 12, FinalityLevel::Safe);
    clear_coverage_index_v1(&storage, &chain);
    let checkpoints = Arc::new(AtomicUsize::new(0));
    let checkpoint_state = checkpoints.clone();

    let error = storage
        .compact_small_objects_for_chain_with_checkpoint(
            &chain,
            coverage_index_v2_compaction_config(false),
            move || {
                let count = checkpoint_state.fetch_add(1, Ordering::SeqCst);
                if count == 1 {
                    Err(DatalensError::internal("injected checkpoint failure"))
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("checkpoint failure");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert_eq!(coverage_index_v2_snapshot_count(&storage, &chain), 1);
    assert_eq!(coverage_index_v2_snapshot_head_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 3);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(10, 12).expect("range"),
            )
            .expect("read rows from deltas"),
        &[10, 11, 12],
    );

    let report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("retry coverage index v2 compaction");
    assert_eq!(report.coverage_index_v2_compacted_buckets, 1);
    assert_eq!(report.coverage_index_v2_deleted_deltas, 3);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
}

#[test]
fn test_compaction_coverage_index_v2_head_without_cleanup_is_retried() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-head-no-cleanup"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 20, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 21, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 22, FinalityLevel::Safe);
    clear_coverage_index_v1(&storage, &chain);
    let checkpoints = Arc::new(AtomicUsize::new(0));
    let checkpoint_state = checkpoints.clone();

    let error = storage
        .compact_small_objects_for_chain_with_checkpoint(
            &chain,
            coverage_index_v2_compaction_config(true),
            move || {
                let count = checkpoint_state.fetch_add(1, Ordering::SeqCst);
                if count == 2 {
                    Err(DatalensError::internal("injected checkpoint failure"))
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("checkpoint failure");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert_eq!(coverage_index_v2_snapshot_count(&storage, &chain), 1);
    assert_eq!(coverage_index_v2_snapshot_head_count(&storage, &chain), 1);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 3);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(20, 22).expect("range"),
            )
            .expect("read rows from snapshot head"),
        &[20, 21, 22],
    );

    let report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("retry coverage index v2 cleanup");
    assert_eq!(report.coverage_index_v2_cleanup_records, 1);
    assert_eq!(report.coverage_index_v2_deleted_deltas, 3);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 0);
}

#[test]
fn test_compaction_coverage_index_v2_recovers_older_head_cleanup_after_new_head() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-older-head-cleanup"));
    let chain = test_chain();
    for number in 40..=42 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);
    let checkpoints = Arc::new(AtomicUsize::new(0));
    let checkpoint_state = checkpoints.clone();

    let error = storage
        .compact_small_objects_for_chain_with_checkpoint(
            &chain,
            coverage_index_v2_compaction_config(true),
            move || {
                let count = checkpoint_state.fetch_add(1, Ordering::SeqCst);
                if count == 2 {
                    Err(DatalensError::internal("injected checkpoint failure"))
                } else {
                    Ok(())
                }
            },
        )
        .expect_err("checkpoint failure");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert_eq!(coverage_index_v2_snapshot_head_count(&storage, &chain), 1);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 0);
    for number in 43..=45 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);

    let report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("recover old cleanup and compact new deltas");

    assert!(report.coverage_index_v2_cleanup_records >= 1);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 0);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(40, 45).expect("range"),
            )
            .expect("read rows after cleanup recovery"),
        &[40, 41, 42, 43, 44, 45],
    );

    let noop_report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("no-op coverage index v2 compaction");
    assert_eq!(noop_report.coverage_index_v2_cleanup_records, 0);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 0);
}

#[test]
fn test_compaction_coverage_index_v2_cleanup_delete_failure_retries() {
    let root = temp_storage_root("coverage-v2-delete-retry");
    let chain = test_chain();
    let storage = LocalStorage::new(root.clone());
    write_block_object(&storage, &chain, 30, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 31, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 32, FinalityLevel::Safe);
    clear_coverage_index_v1(&storage, &chain);
    let failing_storage = DurableStorage::from_object_store(
        FailingCoverageIndexV2DeltaDeleteStore::new(root.clone()),
    );

    let failed_cleanup_report = failing_storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("coverage index v2 compaction with delete failure");

    assert_eq!(failed_cleanup_report.coverage_index_v2_compacted_buckets, 1);
    assert_eq!(
        failed_cleanup_report.coverage_index_v2_delta_delete_failures,
        3
    );
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 3);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 1);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(30, 32).expect("range"),
            )
            .expect("read rows after failed cleanup"),
        &[30, 31, 32],
    );

    let retry_report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("retry coverage index v2 cleanup");
    assert_eq!(retry_report.coverage_index_v2_deleted_deltas, 3);
    assert_eq!(retry_report.coverage_index_v2_delta_delete_failures, 0);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 0);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(30, 32).expect("range"),
            )
            .expect("read rows after cleanup retry"),
        &[30, 31, 32],
    );
}

#[test]
fn test_compaction_coverage_index_v2_cleanup_checkpoint_failure_stops_delta_delete_until_retry() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-cleanup-checkpoint-retry"));
    let chain = test_chain();
    for number in 33..=35 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);
    let checkpoints = Arc::new(AtomicUsize::new(0));
    let checkpoint_state = checkpoints.clone();

    let failed_cleanup_report = storage
        .compact_small_objects_for_chain_with_checkpoint(
            &chain,
            coverage_index_v2_compaction_config(true),
            move || {
                let count = checkpoint_state.fetch_add(1, Ordering::SeqCst);
                if count == 3 {
                    Err(DatalensError::internal(
                        "injected cleanup checkpoint failure",
                    ))
                } else {
                    Ok(())
                }
            },
        )
        .expect("checkpoint failure is reported as deferred cleanup work");

    assert_eq!(failed_cleanup_report.coverage_index_v2_compacted_buckets, 1);
    assert_eq!(
        failed_cleanup_report.coverage_index_v2_delta_delete_failures, 1,
        "leader checkpoint failure must stop coverage delta deletes before any key is removed"
    );
    assert_eq!(failed_cleanup_report.coverage_index_v2_deleted_deltas, 0);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 3);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 1);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(33, 35).expect("range"),
            )
            .expect("read rows while cleanup is deferred"),
        &[33, 34, 35],
    );

    let retry_report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("retry deferred cleanup");
    assert_eq!(retry_report.coverage_index_v2_deleted_deltas, 3);
    assert_eq!(retry_report.coverage_index_v2_delta_delete_failures, 0);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_cleanup_count(&storage, &chain), 0);
}

#[test]
fn test_compaction_coverage_index_v2_malformed_cleanup_record_does_not_delete_arbitrary_key() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-malformed-cleanup"));
    let chain = test_chain();
    let victim_key = format!("chains/{}/objects/not-a-delta.json", chain.key_prefix());
    storage
        .object_store()
        .put(&victim_key, br#"{"keep":true}"#)
        .expect("write victim object");
    write_coverage_index_v2_cleanup_record(
        &storage,
        &chain,
        "malformed",
        "chains/not-a-real-snapshot.json",
        vec![victim_key.clone()],
    );

    storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("cleanup tick");

    assert!(
        storage
            .object_store()
            .exists(&victim_key)
            .expect("victim exists"),
        "cleanup must not delete keys outside the record bucket delta prefix"
    );
}

#[test]
fn test_compaction_coverage_index_v2_older_head_cleanup_preserves_delta_missing_from_latest_head() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-older-head-missing-delta"));
    let chain = test_chain();
    for number in 47..=49 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);
    let delta_key = first_coverage_index_v2_delta_key(&storage, &chain);

    storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(false))
        .expect("write first coverage index v2 snapshot head");
    let first_snapshot_key = list_prefix(
        &storage,
        &format!("chains/{}/coverage-index-v2/snapshots", chain.key_prefix()),
    )
    .into_iter()
    .next()
    .expect("first snapshot key");
    write_coverage_index_v2_cleanup_record(
        &storage,
        &chain,
        "older-head",
        &first_snapshot_key,
        vec![delta_key.clone()],
    );
    let second_snapshot_key = write_coverage_index_v2_snapshot(
        &storage,
        &chain,
        "newer-missing-delta",
        9_999_999_999_999,
        Vec::new(),
    );
    write_coverage_index_v2_snapshot_head(
        &storage,
        &chain,
        "newer-missing-delta",
        9_999_999_999_999,
        &second_snapshot_key,
    );

    storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                coverage_index_v2_delta_count_threshold: 999,
                ..coverage_index_v2_compaction_config(true)
            },
        )
        .expect("cleanup tick");

    assert!(
        storage
            .object_store()
            .exists(&delta_key)
            .expect("delta exists"),
        "cleanup must not delete a delta missing from the latest snapshot head"
    );
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(47, 49).expect("range"),
            )
            .expect("read rows after stale cleanup tick"),
        &[47, 48, 49],
    );
}

#[test]
fn test_compaction_coverage_index_v2_stale_cleanup_record_does_not_delete_delta() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-stale-cleanup"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 50, FinalityLevel::Safe);
    clear_coverage_index_v1(&storage, &chain);
    let delta_key = first_coverage_index_v2_delta_key(&storage, &chain);
    write_coverage_index_v2_cleanup_record(
        &storage,
        &chain,
        "stale",
        "chains/test/coverage-index-v2/snapshots/missing.json",
        vec![delta_key.clone()],
    );

    storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                coverage_index_v2_delta_count_threshold: 999,
                ..coverage_index_v2_compaction_config(true)
            },
        )
        .expect("cleanup tick");

    assert!(
        storage
            .object_store()
            .exists(&delta_key)
            .expect("delta exists"),
        "cleanup must prove the snapshot before deleting its compacted delta keys"
    );
}

#[test]
fn test_compaction_coverage_index_v2_delta_count_threshold_triggers_compaction() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-delta-threshold"));
    let chain = test_chain();
    for number in 100..=101 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);
    let config = MaintenanceCompactionConfig {
        coverage_index_v2_delta_count_threshold: 3,
        ..coverage_index_v2_compaction_config(false)
    };

    let below_threshold = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("below threshold compaction");
    assert_eq!(below_threshold.coverage_index_v2_compacted_buckets, 0);
    assert_eq!(coverage_index_v2_snapshot_head_count(&storage, &chain), 0);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 2);

    write_block_object(&storage, &chain, 102, FinalityLevel::Safe);
    clear_coverage_index_v1(&storage, &chain);
    let at_threshold = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("at threshold compaction");

    assert_eq!(
        at_threshold.coverage_index_v2_compacted_buckets, 1,
        "delta_count threshold must trigger coverage-index-v2 compaction exactly at budget"
    );
    assert_eq!(at_threshold.coverage_index_v2_compacted_deltas, 3);
    assert_eq!(coverage_index_v2_snapshot_head_count(&storage, &chain), 1);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(100, 102).expect("range"),
            )
            .expect("read rows after threshold compaction"),
        &[100, 101, 102],
    );
}

#[test]
fn test_compaction_coverage_index_v2_snapshot_head_writes_stay_within_overwrite_budget() {
    let object_store = CountingObjectStore::new(LocalObjectStore::new(temp_storage_root(
        "coverage-v2-head-overwrite-budget",
    )));
    let storage = DurableStorage::from_object_store(object_store.clone());
    let chain = test_chain();
    for number in 110..=112 {
        write_block_object_to_storage(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);

    storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(false))
        .expect("first coverage index v2 compaction");
    for number in 113..=115 {
        write_block_object_to_storage(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);
    storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(false))
        .expect("second coverage index v2 compaction");

    let prefix = format!("chains/{}/coverage-index-v2", chain.key_prefix());
    object_store.assert_no_overwrite(&format!("{prefix}/snapshots/"));
    object_store.assert_no_overwrite(&format!("{prefix}/snapshot-heads/"));
    assert_eq!(
        object_store.overwrite_budget_violations_for_prefix(&prefix),
        Vec::<(String, usize)>::new(),
        "coverage-index-v2 compaction should use immutable records instead of same-key PUT retries"
    );
    assert_eq!(coverage_index_v2_snapshot_head_count(&storage, &chain), 2);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(110, 115).expect("range"),
            )
            .expect("read rows after immutable head writes"),
        &[110, 111, 112, 113, 114, 115],
    );
}

#[test]
fn test_compaction_coverage_index_v2_default_config_compacts_at_get_budget() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-default-budget"));
    let chain = test_chain();
    for number in 0..64 {
        write_empty_coverage(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);

    let report = storage
        .compact_small_objects_for_chain(&chain, MaintenanceCompactionConfig::default())
        .expect("default coverage index v2 compaction");

    assert_eq!(report.coverage_index_v2_compacted_buckets, 1);
    assert_eq!(report.coverage_index_v2_compacted_deltas, 64);
    assert_eq!(report.get_operations, 64);
    assert_eq!(coverage_index_v2_snapshot_head_count(&storage, &chain), 1);
}

#[test]
fn test_compaction_coverage_index_v2_skips_corrupt_cleanup_record_and_cleans_valid_work() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-corrupt-cleanup"));
    let chain = test_chain();
    for number in 90..=92 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);
    let corrupt_key = format!(
        "chains/{}/coverage-index-v2/cleanup/exact/evm.blocks/block/all/safe/00000000000000000000-00000000000000099999/corrupt.json",
        chain.key_prefix()
    );
    storage
        .object_store()
        .put(&corrupt_key, b"{not-json")
        .expect("write corrupt cleanup record");

    let report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("compaction skips corrupt cleanup record");

    assert_eq!(report.coverage_index_v2_compacted_buckets, 1);
    assert_eq!(report.coverage_index_v2_deleted_deltas, 3);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
    assert!(
        storage
            .object_store()
            .exists(&corrupt_key)
            .expect("corrupt cleanup record exists"),
        "invalid cleanup record should be skipped rather than blocking the tick"
    );
}

#[test]
fn test_compaction_coverage_index_v2_ignores_sibling_prefixes() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-sibling-prefix"));
    let chain = test_chain();
    for number in 60..=62 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);
    let sibling_key = format!(
        "chains/{}/coverage-index-v2/deltas-old/exact/evm.blocks/block/all/safe/not-a-number-00000000000000000000/0001.json",
        chain.key_prefix()
    );
    storage
        .object_store()
        .put(&sibling_key, br#"{"not":"a v2 delta"}"#)
        .expect("write sibling object");

    let report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(false))
        .expect("compact real v2 deltas");

    assert_eq!(report.coverage_index_v2_compacted_buckets, 1);
}

#[test]
fn test_compaction_coverage_index_v2_respects_get_budget() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-get-budget"));
    let chain = test_chain();
    for number in 70..=73 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    clear_coverage_index_v1(&storage, &chain);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                max_gets_per_tick: 3,
                cleanup_enabled: false,
                coverage_index_v2_delta_count_threshold: 3,
                ..coverage_index_v2_compaction_config(false)
            },
        )
        .expect("budgeted coverage index v2 compaction");

    assert_eq!(report.coverage_index_v2_compacted_deltas, 3);
    assert_eq!(report.get_operations, 3);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &DatasetSelector::all(),
                LedgerRange::blocks(70, 73).expect("range"),
            )
            .expect("read rows after budgeted compaction"),
        &[70, 71, 72, 73],
    );
}

#[test]
fn test_compaction_coverage_index_v2_preserves_replacement_tombstone_snapshot() {
    let storage = LocalStorage::new(temp_storage_root("coverage-v2-replacement-snapshot"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    for number in 80..=82 {
        write_block_object(&storage, &chain, number, FinalityLevel::Safe);
    }
    let empty_rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        .expect("empty rows");
    storage
        .write_rows_replacing_existing(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(81, 81).expect("range"),
            rows: &empty_rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write empty replacement");
    clear_coverage_index_v1(&storage, &chain);

    let report = storage
        .compact_small_objects_for_chain(&chain, coverage_index_v2_compaction_config(true))
        .expect("compact replacement deltas");

    assert_eq!(report.coverage_index_v2_compacted_buckets, 1);
    assert_eq!(coverage_index_v2_delta_count(&storage, &chain), 0);
    assert_block_numbers(
        storage
            .read_rows(
                &chain,
                &DatasetKey::evm_blocks(),
                &selector,
                LedgerRange::blocks(80, 82).expect("range"),
            )
            .expect("read replacement snapshot"),
        &[80, 82],
    );
}

#[test]
fn test_compaction_tick_scans_one_manifest_segment_prefix_per_tick() {
    let storage = LocalStorage::new(temp_storage_root("prefix-scope"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 112, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 113, FinalityLevel::Safe);
    let other_selector = DatasetSelector::try_other(
        AdapterKey::try_new("test").expect("adapter key"),
        "selector-b",
        "selector-b",
    )
    .expect("selector");
    write_block_object_with_selector(&storage, &chain, &other_selector, 114, FinalityLevel::Safe);
    write_block_object_with_selector(&storage, &chain, &other_selector, 115, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: false,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("prefix-scoped compaction");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(report.candidate_count, 1);
    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.candidates[0].selector_fingerprint, "all");
}

#[test]
fn test_compaction_cursor_resumes_and_loss_recovers_without_affecting_reads() {
    let storage = LocalStorage::new(temp_storage_root("cursor-resume"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 120, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 121, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 130, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 131, FinalityLevel::Safe);
    let config = MaintenanceCompactionConfig {
        min_object_bytes: u64::MAX,
        max_input_objects_per_candidate: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 8,
        max_manifest_entries_per_tick: 2,
        cleanup_enabled: false,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    let first = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("first tick");
    assert_eq!(first.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(first.processed_candidates, 1);

    let second = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("second tick");
    assert_eq!(second.processed_candidates, 1);

    storage
        .object_store()
        .delete(&format!(
            "chains/{}/metadata/compaction-cursor.json",
            chain.key_prefix()
        ))
        .expect("delete compaction cursor");
    let recovery = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("recovery tick");
    assert!(!recovery.read_only);

    let rows = storage
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            LedgerRange::blocks(120, 131).expect("range"),
        )
        .expect("reads remain correct after cursor loss");
    assert_eq!(rows.row_count(), 4);
}

#[test]
fn test_compaction_scope_cursor_is_isolated_per_selector_scope() {
    let storage = LocalStorage::new(temp_storage_root("scope-cursor-isolated"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 120, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 121, FinalityLevel::Safe);
    let other_selector = DatasetSelector::try_other(
        AdapterKey::try_new("test").expect("adapter key"),
        "selector-b",
        "selector-b",
    )
    .expect("selector");
    write_block_object_with_selector(&storage, &chain, &other_selector, 130, FinalityLevel::Safe);
    write_block_object_with_selector(&storage, &chain, &other_selector, 131, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 2,
                cleanup_enabled: false,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("first scoped tick");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(report.candidates[0].selector_fingerprint, "all");
    assert!(
        !storage
            .object_store()
            .exists(&format!(
                "chains/{}/metadata/compaction-cursor.json",
                chain.key_prefix()
            ))
            .expect("legacy chain cursor exists check"),
        "new manifest segment scans must not write a chain-wide compaction cursor"
    );
    assert!(
        storage
            .object_store()
            .exists(&format!(
                "chains/{}/manifest-segments/evm.blocks/block/all/safe/_metadata/compaction-cursor.json",
                chain.key_prefix()
            ))
            .expect("scope cursor exists check"),
        "the active selector scope should own its own cursor"
    );
    assert!(
        !storage
            .object_store()
            .exists(&format!(
                "chains/{}/manifest-segments/evm.blocks/block/{}/safe/_metadata/compaction-cursor.json",
                chain.key_prefix(),
                other_selector.fingerprint()
            ))
            .expect("other scope cursor exists check"),
        "unprocessed selector scopes should not inherit another scope cursor"
    );
}

#[test]
fn test_object_store_lock_lease_prevents_duplicate_leaders_until_released() {
    let store = LocalObjectStore::new(temp_storage_root("object-lock-lease"));
    let first = store
        .try_acquire_lock("locks/compaction/chain-a/scope-a.json", b"first")
        .expect("first lock acquire")
        .expect("first lock holder");

    let second = store
        .try_acquire_lock("locks/compaction/chain-a/scope-a.json", b"second")
        .expect("second lock acquire");
    assert!(second.is_none());

    store.release_lock(first).expect("release first lock");
    let third = store
        .try_acquire_lock("locks/compaction/chain-a/scope-a.json", b"third")
        .expect("third lock acquire")
        .expect("third lock holder");
    assert_eq!(
        third,
        ObjectLockLease {
            key: "locks/compaction/chain-a/scope-a.json".to_owned(),
            owner: b"third".to_vec(),
        }
    );
}

#[test]
fn test_object_store_lock_lease_recovers_expired_leader() {
    let store = LocalObjectStore::new(temp_storage_root("object-lock-expired"));
    let key = "locks/compaction/chain-a/scope-a.json";
    store
        .put(key, br#"{"owner_id":"old","acquired_at_unix_seconds":1}"#)
        .expect("write expired lock");

    let lease = store
        .try_acquire_lock_with_ttl(
            key,
            br#"{"owner_id":"new","acquired_at_unix_seconds":9999999999}"#,
            std::time::Duration::from_secs(1),
        )
        .expect("recover expired lock")
        .expect("new leader lease");

    assert_eq!(lease.key, key);
    assert_eq!(
        store.get(key).expect("current lock owner"),
        br#"{"owner_id":"new","acquired_at_unix_seconds":9999999999}"#
    );
}

#[test]
fn test_compaction_legacy_full_manifest_partial_tick_persists_offset_cursor() {
    let storage = LocalStorage::new(temp_storage_root("legacy-cursor"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 132, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 133, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 134, FinalityLevel::Safe);
    let manifest = storage.manifest().expect("manifest");
    for object in storage
        .object_store()
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segments")
    {
        storage
            .object_store()
            .delete(&object.key)
            .expect("delete segment");
    }
    std::fs::create_dir_all(
        storage
            .manifest_path(&chain)
            .parent()
            .expect("manifest parent"),
    )
    .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
    )
    .expect("write full manifest");
    let config = MaintenanceCompactionConfig {
        min_object_bytes: u64::MAX,
        max_input_objects_per_candidate: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 8,
        max_manifest_entries_per_tick: 1,
        cleanup_enabled: false,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    let first = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("first legacy tick");
    assert_eq!(first.tick_status, MaintenanceCompactionTickStatus::Partial);
    let cursor_key = format!(
        "chains/{}/metadata/compaction-cursor.json",
        chain.key_prefix()
    );
    let cursor = serde_json::from_slice::<serde_json::Value>(
        &storage.object_store().get(&cursor_key).expect("cursor"),
    )
    .expect("cursor json");
    assert_eq!(cursor["legacy_entry_offset"], 1);

    let second = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("second legacy tick");
    assert_eq!(second.tick_status, MaintenanceCompactionTickStatus::Partial);
    let cursor = serde_json::from_slice::<serde_json::Value>(
        &storage.object_store().get(&cursor_key).expect("cursor"),
    )
    .expect("cursor json");
    assert_eq!(cursor["legacy_entry_offset"], 2);
}

#[test]
fn test_compaction_legacy_cursor_continues_after_partial_tick_writes_segment() {
    let storage = LocalStorage::new(temp_storage_root("legacy-cursor-after-segment"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 1, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 2, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 100, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 101, FinalityLevel::Safe);
    let manifest = storage.manifest().expect("manifest");
    for object in storage
        .object_store()
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segments")
    {
        storage
            .object_store()
            .delete(&object.key)
            .expect("delete segment");
    }
    std::fs::create_dir_all(
        storage
            .manifest_path(&chain)
            .parent()
            .expect("manifest parent"),
    )
    .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
    )
    .expect("write full manifest");
    let config = MaintenanceCompactionConfig {
        min_object_bytes: u64::MAX,
        max_input_objects_per_candidate: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 8,
        max_manifest_entries_per_tick: 2,
        cleanup_enabled: false,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    let first = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("first legacy tick");
    assert_eq!(first.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(first.compacted_objects, 1);
    assert_eq!(
        first.candidates[0].range,
        LedgerRange::blocks(1, 2).expect("range")
    );
    assert!(
        !storage
            .object_store()
            .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
            .expect("manifest segments")
            .is_empty()
    );

    let second = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("second legacy tick");

    assert_eq!(
        second.tick_status,
        MaintenanceCompactionTickStatus::Completed
    );
    assert_eq!(second.compacted_objects, 1);
    assert_eq!(
        second.candidates[0].range,
        LedgerRange::blocks(100, 101).expect("range")
    );
}

#[test]
fn test_compaction_legacy_cursor_survives_candidate_budget_partial() {
    let storage = LocalStorage::new(temp_storage_root("legacy-cursor-candidate-budget"));
    let chain = test_chain();
    write_block_object(&storage, &chain, 3, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 4, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 200, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 201, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 300, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 301, FinalityLevel::Safe);
    let manifest = storage.manifest().expect("manifest");
    for object in storage
        .object_store()
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segments")
    {
        storage
            .object_store()
            .delete(&object.key)
            .expect("delete segment");
    }
    std::fs::create_dir_all(
        storage
            .manifest_path(&chain)
            .parent()
            .expect("manifest parent"),
    )
    .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&manifest).expect("manifest bytes"),
    )
    .expect("write full manifest");
    let config = MaintenanceCompactionConfig {
        min_object_bytes: u64::MAX,
        max_input_objects_per_candidate: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 1,
        max_manifest_entries_per_tick: 4,
        cleanup_enabled: false,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    };

    let first = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("first legacy tick");
    assert_eq!(first.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(first.compacted_objects, 1);
    let cursor_key = format!(
        "chains/{}/metadata/compaction-cursor.json",
        chain.key_prefix()
    );
    let cursor = serde_json::from_slice::<serde_json::Value>(
        &storage.object_store().get(&cursor_key).expect("cursor"),
    )
    .expect("cursor json");
    assert_eq!(cursor["legacy_entry_offset"], 0);

    let second = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("second legacy tick");

    assert_eq!(
        second.candidates[0].range,
        LedgerRange::blocks(200, 201).expect("range")
    );
    assert_eq!(second.compacted_objects, 1);
}

#[test]
fn test_compaction_ignores_segments_shadowed_by_full_manifest() {
    let storage = LocalStorage::new(temp_storage_root("shadowed-segments"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    write_block_object(&storage, &chain, 140, FinalityLevel::Safe);
    write_block_object(&storage, &chain, 141, FinalityLevel::Safe);
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![
            BlockHeader {
                number: 140,
                hash: "0xreplacement140".to_owned(),
                parent_hash: "0xparent".to_owned(),
                timestamp: 140,
            },
            BlockHeader {
                number: 141,
                hash: "0xreplacement141".to_owned(),
                parent_hash: "0xreplacement140".to_owned(),
                timestamp: 141,
            },
        ]),
    )
    .expect("replacement rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: LedgerRange::blocks(140, 141).expect("range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write replacement rows");
    let full_entry = storage
        .manifest()
        .expect("manifest")
        .entries
        .into_iter()
        .find(|entry| entry.range == LedgerRange::blocks(140, 141).expect("range"))
        .expect("full manifest entry");
    std::fs::create_dir_all(
        storage
            .manifest_path(&chain)
            .parent()
            .expect("manifest parent"),
    )
    .expect("create manifest parent");
    std::fs::write(
        storage.manifest_path(&chain),
        serde_json::to_vec_pretty(&Manifest {
            entries: vec![full_entry],
        })
        .expect("manifest bytes"),
    )
    .expect("write full manifest");

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                min_object_bytes: u64::MAX,
                max_input_objects_per_candidate: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                cleanup_enabled: false,
                delete_source_objects: false,
                ..MaintenanceCompactionConfig::default()
            },
        )
        .expect("compaction");

    assert_eq!(report.candidate_count, 0);
    assert_eq!(report.compacted_objects, 0);
}

fn write_block_object(
    storage: &LocalStorage,
    chain: &ChainIdentity,
    number: u64,
    finality: FinalityLevel,
) -> String {
    write_block_object_with_selector(storage, chain, &DatasetSelector::all(), number, finality)
}

fn write_block_object_with_selector(
    storage: &LocalStorage,
    chain: &ChainIdentity,
    selector: &DatasetSelector,
    number: u64,
    finality: FinalityLevel,
) -> String {
    write_block_object_to_storage_with_selector(storage, chain, selector, number, finality)
}

fn write_block_object_to_storage<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    number: u64,
    finality: FinalityLevel,
) -> String {
    write_block_object_to_storage_with_selector(
        storage,
        chain,
        &DatasetSelector::all(),
        number,
        finality,
    )
}

fn write_block_object_to_storage_with_selector<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    selector: &DatasetSelector,
    number: u64,
    finality: FinalityLevel,
) -> String {
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number,
            hash: format!("0xblock{number}"),
            parent_hash: "0xparent".to_owned(),
            timestamp: number,
        }]),
    )
    .expect("rows");
    let outcome = storage
        .write_rows(StorageWriteRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: LedgerRange::blocks(number, number).expect("range"),
            rows: &rows,
            finality_level: finality,
            record_empty_coverage: true,
        })
        .expect("write rows");
    outcome.data_object.expect("data object").object_key
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
    .expect("rows")
}

fn assert_block_numbers(rows: DatasetRows, expected: &[u64]) {
    match rows.into_rows() {
        QueryRows::EvmBlocks(blocks) => {
            assert_eq!(
                blocks
                    .into_iter()
                    .map(|block| block.number)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        rows => panic!("expected evm block rows, got {rows:?}"),
    }
}

fn write_empty_coverage(
    storage: &LocalStorage,
    chain: &ChainIdentity,
    number: u64,
    finality: FinalityLevel,
) {
    let rows =
        DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new())).expect("rows");
    storage
        .write_rows(StorageWriteRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(number, number).expect("range"),
            rows: &rows,
            finality_level: finality,
            record_empty_coverage: true,
        })
        .expect("write empty coverage");
}

fn coverage_index_v2_compaction_config(cleanup_enabled: bool) -> MaintenanceCompactionConfig {
    MaintenanceCompactionConfig {
        min_object_bytes: 0,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 8,
        max_manifest_entries_per_tick: 20_000,
        max_gets_per_tick: 128,
        max_puts_per_tick: 16,
        max_deletes_per_tick: 128,
        cleanup_enabled,
        delete_source_objects: false,
        coverage_index_v2_delta_count_threshold: 3,
        coverage_index_v2_delete_grace_ms: 0,
        ..MaintenanceCompactionConfig::default()
    }
}

fn clear_coverage_index_v1<S: ObjectStore>(storage: &DurableStorage<S>, chain: &ChainIdentity) {
    delete_prefix(
        storage,
        &format!("chains/{}/coverage-index", chain.key_prefix()),
    );
}

fn coverage_index_v2_delta_count<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
) -> usize {
    list_prefix(
        storage,
        &format!("chains/{}/coverage-index-v2/deltas", chain.key_prefix()),
    )
    .len()
}

fn coverage_index_v2_snapshot_count<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
) -> usize {
    list_prefix(
        storage,
        &format!("chains/{}/coverage-index-v2/snapshots", chain.key_prefix()),
    )
    .len()
}

fn coverage_index_v2_snapshot_head_count<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
) -> usize {
    list_prefix(
        storage,
        &format!(
            "chains/{}/coverage-index-v2/snapshot-heads",
            chain.key_prefix()
        ),
    )
    .len()
}

fn coverage_index_v2_cleanup_count<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
) -> usize {
    list_prefix(
        storage,
        &format!("chains/{}/coverage-index-v2/cleanup", chain.key_prefix()),
    )
    .len()
}

fn first_coverage_index_v2_delta_key<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
) -> String {
    let delta_prefix = format!("chains/{}/coverage-index-v2/deltas/", chain.key_prefix());
    list_prefix(
        storage,
        &format!("chains/{}/coverage-index-v2/deltas", chain.key_prefix()),
    )
    .into_iter()
    .find(|key| key.starts_with(&delta_prefix))
    .expect("coverage index v2 delta key")
}

fn write_coverage_index_v2_cleanup_record<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    id: &str,
    snapshot_key: &str,
    compacted_delta_keys: Vec<String>,
) -> String {
    let key = format!(
        "chains/{}/coverage-index-v2/cleanup/exact/evm.blocks/block/all/safe/00000000000000000000-00000000000000099999/{id}.json",
        chain.key_prefix()
    );
    let bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "schema_version": 1,
        "created_at_unix_ms": 1,
        "scope": "exact/evm.blocks/block/all/safe",
        "bucket_start": 0,
        "bucket_end": 99_999,
        "compaction_id": id,
        "snapshot_key": snapshot_key,
        "compacted_delta_keys": compacted_delta_keys,
    }))
    .expect("cleanup record bytes");
    storage
        .object_store()
        .put(&key, &bytes)
        .expect("write cleanup record");
    key
}

fn write_coverage_index_v2_snapshot<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    id: &str,
    created_at_unix_ms: u64,
    compacted_delta_keys: Vec<String>,
) -> String {
    let key = format!(
        "chains/{}/coverage-index-v2/snapshots/exact/evm.blocks/block/all/safe/00000000000000000000-00000000000000099999/{id}.json",
        chain.key_prefix()
    );
    let bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "schema_version": 1,
        "created_at_unix_ms": created_at_unix_ms,
        "scope": "exact/evm.blocks/block/all/safe",
        "bucket_start": 0,
        "bucket_end": 99_999,
        "entries": [],
        "compacted_delta_keys": compacted_delta_keys,
    }))
    .expect("snapshot bytes");
    storage
        .object_store()
        .put(&key, &bytes)
        .expect("write snapshot");
    key
}

fn write_coverage_index_v2_snapshot_head<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    id: &str,
    created_at_unix_ms: u64,
    snapshot_key: &str,
) -> String {
    let key = format!(
        "chains/{}/coverage-index-v2/snapshot-heads/exact/evm.blocks/block/all/safe/00000000000000000000-00000000000000099999/{id}.json",
        chain.key_prefix()
    );
    let bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "schema_version": 1,
        "created_at_unix_ms": created_at_unix_ms,
        "scope": "exact/evm.blocks/block/all/safe",
        "bucket_start": 0,
        "bucket_end": 99_999,
        "snapshot_key": snapshot_key,
        "included_delta_high_watermark": "",
    }))
    .expect("snapshot head bytes");
    storage
        .object_store()
        .put(&key, &bytes)
        .expect("write snapshot head");
    key
}

fn list_prefix<S: ObjectStore>(storage: &DurableStorage<S>, prefix: &str) -> Vec<String> {
    storage
        .object_store()
        .list(prefix)
        .expect("list prefix")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

fn delete_prefix<S: ObjectStore>(storage: &DurableStorage<S>, prefix: &str) {
    for key in list_prefix(storage, prefix) {
        storage.object_store().delete(&key).expect("delete object");
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-storage-maintenance-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
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

fn read_manifest_json(storage: &LocalStorage, chain: &ChainIdentity) -> serde_json::Value {
    let manifest = storage.manifest().expect("manifest");
    let entries = manifest
        .entries
        .into_iter()
        .filter(|entry| entry.chain == *chain)
        .collect::<Vec<_>>();
    serde_json::to_value(datalens_storage::Manifest { entries }).expect("manifest json")
}

fn write_manifest_json(storage: &LocalStorage, chain: &ChainIdentity, manifest: serde_json::Value) {
    for object in storage
        .object_store()
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segments")
    {
        storage
            .object_store()
            .delete(&object.key)
            .expect("delete manifest segment");
    }
    let bytes = serde_json::to_vec_pretty(&manifest).expect("manifest bytes");
    std::fs::create_dir_all(
        storage
            .manifest_path(chain)
            .parent()
            .expect("manifest parent"),
    )
    .expect("create manifest parent");
    std::fs::write(storage.manifest_path(chain), bytes).expect("write manifest");
}

fn compacted_object_keys(storage: &LocalStorage, chain: &ChainIdentity) -> Vec<String> {
    storage
        .object_store()
        .list(&format!("chains/{}/datasets", chain.key_prefix()))
        .expect("list data objects")
        .into_iter()
        .map(|object| object.key)
        .filter(|key| key.contains("/compacted/"))
        .collect()
}

fn manifest_segment_keys(storage: &LocalStorage, chain: &ChainIdentity) -> Vec<String> {
    storage
        .object_store()
        .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
        .expect("manifest segments")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}
