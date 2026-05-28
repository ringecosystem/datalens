use std::path::PathBuf;

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey,
    DatasetRows, LedgerRange, NetworkId, QueryRows,
};
use datalens_storage::{
    DurableStorage, LocalObjectStore, LocalStorage, MaintenanceCompactionConfig,
    MaintenanceIssueKind, MaintenanceOperationMode, ObjectMetadata, ObjectStore,
    StorageWriteRequest,
};

#[derive(Clone, Debug)]
struct FailingDataObjectPutStore {
    inner: LocalObjectStore,
}

impl FailingDataObjectPutStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
        }
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

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
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
    let before = storage.object_store().list("chains").expect("before list");

    let report = storage
        .compact_small_objects(MaintenanceCompactionConfig {
            min_object_bytes: u64::MAX,
            max_merge_ranges: 8,
        })
        .expect("compact small objects");

    assert!(!report.read_only);
    assert_eq!(report.candidates.len(), 1);
    assert_eq!(report.compacted_objects, 1);
    assert_eq!(report.compacted_rows, 2);

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(
        manifest.entries[0].range,
        LedgerRange::blocks(50, 51).expect("range")
    );
    assert_eq!(manifest.entries[0].row_count, 2);
    assert_ne!(
        manifest.entries[0].object_key.as_deref(),
        Some(first_object.as_str())
    );
    assert_ne!(
        manifest.entries[0].object_key.as_deref(),
        Some(second_object.as_str())
    );

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
            .list("chains")
            .expect("after list")
            .len()
            > before.len()
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

fn write_block_object(
    storage: &LocalStorage,
    chain: &ChainIdentity,
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
            selector: &DatasetSelector::all(),
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

fn read_manifest_json(storage: &LocalStorage, chain: &ChainIdentity) -> serde_json::Value {
    let bytes = std::fs::read(storage.manifest_path(chain)).expect("manifest bytes");
    serde_json::from_slice(&bytes).expect("manifest json")
}

fn write_manifest_json(storage: &LocalStorage, chain: &ChainIdentity, manifest: serde_json::Value) {
    let bytes = serde_json::to_vec_pretty(&manifest).expect("manifest bytes");
    std::fs::write(storage.manifest_path(chain), bytes).expect("write manifest");
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}
