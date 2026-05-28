use std::path::PathBuf;

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatasetKey, DatasetRows, LedgerRange, NetworkId,
    QueryRows,
};
use datalens_storage::{
    LocalStorage, MaintenanceIssueKind, MaintenanceOperationMode, ObjectStore, StorageWriteRequest,
};

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

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}
