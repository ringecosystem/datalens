use std::path::PathBuf;

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatasetKey, DatasetRows, LedgerRange, NetworkId,
    QueryDataFinality, QueryRows,
};
use datalens_storage::{
    HotCacheCandidateStatus, HotCacheEntryMetadata, HotCacheFinalityStatus,
    HotCacheRetentionPolicy, HotCacheStorage, HotCacheWriteRequest, LocalHotCacheStorage,
    LocalStorage, ObjectStore, StorageWriteRequest,
};

#[test]
fn test_hot_object_key_is_separate_from_durable_object_key() {
    let hot = LocalHotCacheStorage::new(temp_storage_root("hot-namespace"));
    let durable = LocalStorage::new(temp_storage_root("durable-namespace"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 10).expect("valid range");
    let rows = block_rows(10, "0xaaa", "0xparent");

    let hot_outcome = hot
        .write_rows(HotCacheWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: range.clone(),
            rows: &rows,
            metadata: hot_metadata(10, "0xaaa", "0xparent", HotCacheCandidateStatus::Active),
        })
        .expect("write hot rows");

    let durable_outcome = durable
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range,
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write durable rows");

    assert!(hot_outcome.object_key.starts_with("hot-cache/"));
    assert!(hot_outcome.metadata_key.starts_with("hot-cache/"));
    assert!(
        durable_outcome
            .data_object
            .expect("durable object")
            .object_key
            .starts_with("chains/")
    );
    assert!(!hot_outcome.object_key.contains("/manifest.json"));
}

#[test]
fn test_same_height_different_hash_candidates_are_expressed() {
    let hot = LocalHotCacheStorage::new(temp_storage_root("hot-candidates"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(20, 20).expect("valid range");

    hot.write_rows(HotCacheWriteRequest {
        chain: &chain,
        dataset_key: DatasetKey::evm_blocks(),
        selector: &selector,
        range: range.clone(),
        rows: &block_rows(20, "0xaaa", "0xparent"),
        metadata: hot_metadata(20, "0xaaa", "0xparent", HotCacheCandidateStatus::Active),
    })
    .expect("write first candidate");

    hot.write_rows(HotCacheWriteRequest {
        chain: &chain,
        dataset_key: DatasetKey::evm_blocks(),
        selector: &selector,
        range: range.clone(),
        rows: &block_rows(20, "0xbbb", "0xother-parent"),
        metadata: hot_metadata(
            20,
            "0xbbb",
            "0xother-parent",
            HotCacheCandidateStatus::Active,
        ),
    })
    .expect("write replacement candidate");

    let candidates = hot
        .list_entries(&chain, range.kind(), 20, 20)
        .expect("list hot entries");
    let hashes = candidates
        .iter()
        .map(|entry| entry.block_hash.as_str())
        .collect::<Vec<_>>();

    assert_eq!(hashes, vec!["0xaaa", "0xbbb"]);
    assert_eq!(
        candidates
            .iter()
            .filter(|entry| entry.candidate_status == HotCacheCandidateStatus::Active)
            .count(),
        1
    );
    assert_eq!(
        candidates
            .iter()
            .find(|entry| entry.block_hash == "0xbbb")
            .expect("new active candidate")
            .candidate_status,
        HotCacheCandidateStatus::Active
    );
}

#[test]
fn test_hot_metadata_contains_reorg_and_promotion_fields() {
    let hot = LocalHotCacheStorage::new(temp_storage_root("hot-metadata"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(30, 30).expect("valid range");

    let outcome = hot
        .write_rows(HotCacheWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: range.clone(),
            rows: &block_rows(30, "0xccc", "0xbbb"),
            metadata: hot_metadata(30, "0xccc", "0xbbb", HotCacheCandidateStatus::Active),
        })
        .expect("write hot rows");

    let metadata = hot
        .read_metadata(&outcome.metadata_key)
        .expect("read metadata");

    assert_eq!(metadata.block_hash, "0xccc");
    assert_eq!(metadata.parent_hash, "0xbbb");
    assert_eq!(metadata.height, 30);
    assert_eq!(metadata.observed_at_unix_seconds, 1_700_000_030);
    assert_eq!(metadata.source_provider, "test-provider");
    assert_eq!(metadata.finality_status, HotCacheFinalityStatus::Unsafe);
    assert_eq!(metadata.row_count, 1);
    assert_eq!(metadata.checksum_algorithm, "sha256");
    assert!(metadata.object_size_bytes > 0);
    assert_eq!(metadata.candidate_status, HotCacheCandidateStatus::Active);
    assert_eq!(metadata.active_branch.as_deref(), Some("branch-a"));
    assert!(!metadata.eligible_for_promotion);
    assert_eq!(metadata.schema_version, "hot-cache-v1");
    assert_eq!(metadata.source, datalens_core::QuerySegmentSource::HotCache);
    assert_eq!(metadata.query_finality, QueryDataFinality::Unsafe);
}

#[test]
fn test_hot_read_write_does_not_change_durable_manifest() {
    let root = temp_storage_root("hot-manifest-isolation");
    let hot = HotCacheStorage::from_object_store(datalens_storage::LocalObjectStore::new(&root));
    let durable = LocalStorage::new(&root);
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(40, 40).expect("valid range");

    assert!(
        durable
            .manifest()
            .expect("empty manifest")
            .entries
            .is_empty()
    );

    hot.write_rows(HotCacheWriteRequest {
        chain: &chain,
        dataset_key: DatasetKey::evm_blocks(),
        selector: &selector,
        range: range.clone(),
        rows: &block_rows(40, "0xddd", "0xccc"),
        metadata: hot_metadata(40, "0xddd", "0xccc", HotCacheCandidateStatus::Active),
    })
    .expect("write hot rows");
    let read = hot
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            range.kind(),
            40,
            40,
        )
        .expect("read hot rows");

    assert_eq!(read.rows.row_count(), 1);
    assert_eq!(read.metadata.len(), 1);
    assert_eq!(read.metadata[0].source_provider, "test-provider");
    assert!(
        durable
            .manifest()
            .expect("manifest unchanged")
            .entries
            .is_empty()
    );
    assert!(
        !durable
            .object_store()
            .exists("hot-cache")
            .expect("exists check")
    );
}

#[test]
fn test_retention_cleanup_prunes_expired_non_active_candidates() {
    let hot = LocalHotCacheStorage::new(temp_storage_root("hot-retention"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(50, 50).expect("valid range");

    let expired = hot
        .write_rows(HotCacheWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: range.clone(),
            rows: &block_rows(50, "0xeee", "0xddd"),
            metadata: HotCacheEntryMetadata {
                observed_at_unix_seconds: 1_000,
                candidate_status: HotCacheCandidateStatus::Candidate,
                ..hot_metadata(50, "0xeee", "0xddd", HotCacheCandidateStatus::Candidate)
            },
        })
        .expect("write expired candidate");
    let active = hot
        .write_rows(HotCacheWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &selector,
            range: range.clone(),
            rows: &block_rows(50, "0xfff", "0xddd"),
            metadata: hot_metadata(50, "0xfff", "0xddd", HotCacheCandidateStatus::Active),
        })
        .expect("write active candidate");

    let report = hot
        .cleanup(
            2_000,
            HotCacheRetentionPolicy {
                max_age_seconds: 100,
                retain_active_candidates: true,
            },
        )
        .expect("cleanup");

    assert_eq!(report.deleted_entries, 1);
    assert!(
        !hot.object_store()
            .exists(&expired.object_key)
            .expect("expired")
    );
    assert!(
        !hot.object_store()
            .exists(&expired.metadata_key)
            .expect("expired metadata")
    );
    assert!(
        hot.object_store()
            .exists(&active.object_key)
            .expect("active")
    );
    assert!(
        hot.object_store()
            .exists(&active.metadata_key)
            .expect("active metadata")
    );
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-hot-cache-{name}-{}",
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

fn block_rows(number: u64, hash: &str, parent_hash: &str) -> DatasetRows {
    DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number,
            hash: hash.to_owned(),
            parent_hash: parent_hash.to_owned(),
            timestamp: number,
        }]),
    )
    .expect("dataset rows")
}

fn hot_metadata(
    height: u64,
    block_hash: &str,
    parent_hash: &str,
    candidate_status: HotCacheCandidateStatus,
) -> HotCacheEntryMetadata {
    HotCacheEntryMetadata {
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.to_owned(),
        height,
        observed_at_unix_seconds: 1_700_000_000 + height,
        source_provider: "test-provider".to_owned(),
        finality_status: HotCacheFinalityStatus::Unsafe,
        row_count: 0,
        object_size_bytes: 0,
        checksum: String::new(),
        checksum_algorithm: String::new(),
        candidate_status,
        active_branch: Some("branch-a".to_owned()),
        eligible_for_promotion: false,
        schema_version: String::new(),
        object_encoding: None,
        object_key: String::new(),
        metadata_key: String::new(),
        chain: None,
        dataset_key: None,
        selector_fingerprint: String::new(),
        selector_canonical_key: String::new(),
        range: None,
        source: datalens_core::QuerySegmentSource::HotCache,
        query_finality: QueryDataFinality::Unsafe,
    }
}
