use std::path::PathBuf;

use datalens_chain::{AdapterKey, DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey,
    DatasetRows, LedgerRange, NetworkId, QueryRows,
};
use datalens_storage::{
    DurableStorage, DurableStorageConfig, LocalObjectStore, LocalStorage,
    MaintenanceCompactionConfig, MaintenanceCompactionTickStatus, MaintenanceIssueKind,
    MaintenanceOperationMode, Manifest, ObjectListPage, ObjectMetadata, ObjectStore,
    ParquetCompression, StorageWriteRequest,
};
use parquet::{
    basic::Compression,
    file::reader::{FileReader, SerializedFileReader},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Debug)]
struct FailingDataObjectPutStore {
    inner: LocalObjectStore,
}

#[derive(Clone, Debug)]
struct FailingDataObjectDeleteStore {
    inner: LocalObjectStore,
}

#[derive(Clone, Debug)]
struct FailingManifestSegmentPutStore {
    inner: LocalObjectStore,
}

#[derive(Clone, Debug)]
struct CountingListStore {
    inner: LocalObjectStore,
    list_prefixes: Arc<Mutex<Vec<String>>>,
    list_page_prefixes: Arc<Mutex<Vec<String>>>,
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

impl FailingManifestSegmentPutStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
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

impl ObjectStore for FailingManifestSegmentPutStore {
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: false,
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
    assert!(
        storage
            .object_store()
            .list(&format!("chains/{}/manifest-segments", chain.key_prefix()))
            .expect("manifest segments")
            .len()
            >= 3,
        "compaction should publish an additional segment without deleting old segments"
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: false,
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: false,
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
fn test_compaction_deletes_source_objects_when_enabled() {
    let storage = LocalStorage::new(temp_storage_root("execute-compaction-delete-sources"));
    let chain = test_chain();
    let first_object = write_block_object(&storage, &chain, 52, FinalityLevel::Safe);
    let second_object = write_block_object(&storage, &chain, 53, FinalityLevel::Safe);

    let report = storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: true,
        })
        .expect("compact small objects");

    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
    assert_eq!(report.deleted_source_objects, 2);
    assert_eq!(report.source_delete_failures, 0);
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: true,
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: true,
        })
        .expect("compaction reports source delete failure");

    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
    assert_eq!(report.deleted_source_objects, 0);
    assert_eq!(report.source_delete_failures, 2);
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: true,
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
        .reconcile_compaction_for_chain(&chain, MaintenanceCompactionConfig::default())
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: true,
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
                delete_source_objects: true,
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
            max_merge_ranges: 8,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 8,
            max_manifest_entries_per_tick: 20_000,
            delete_source_objects: true,
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
                max_merge_ranges: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 1,
                max_manifest_entries_per_tick: 20_000,
                delete_source_objects: false,
            },
        )
        .expect("bounded compaction");

    assert_eq!(report.tick_status, MaintenanceCompactionTickStatus::Partial);
    assert_eq!(report.candidate_count, 2);
    assert_eq!(report.processed_candidates, 1);
    assert_eq!(report.compacted_objects, 1);
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
                max_merge_ranges: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                delete_source_objects: false,
            },
        )
        .expect("bounded compaction");

    assert_eq!(report.candidate_count, 2);
    assert_eq!(report.processed_candidates, 2);
    assert_eq!(counting_store.list_count_for_prefix(&manifest_prefix), 0);
    assert_eq!(
        counting_store.list_page_count_for_prefix(&manifest_prefix),
        1
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
                max_merge_ranges: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                delete_source_objects: false,
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
        max_merge_ranges: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 8,
        max_manifest_entries_per_tick: 2,
        delete_source_objects: false,
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
        max_merge_ranges: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 8,
        max_manifest_entries_per_tick: 1,
        delete_source_objects: false,
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
        max_merge_ranges: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 8,
        max_manifest_entries_per_tick: 2,
        delete_source_objects: false,
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
        max_merge_ranges: 2,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 1,
        max_manifest_entries_per_tick: 4,
        delete_source_objects: false,
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
    assert_eq!(cursor["legacy_entry_offset"], 4);

    let second = storage
        .compact_small_objects_for_chain(&chain, config)
        .expect("second legacy tick");

    assert_eq!(
        second.candidates[0].range,
        LedgerRange::blocks(300, 301).expect("range")
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
                max_merge_ranges: 2,
                max_tick_duration_ms: 30_000,
                max_candidates_per_tick: 8,
                max_manifest_entries_per_tick: 20_000,
                delete_source_objects: false,
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

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}
