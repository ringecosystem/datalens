use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, DatasetRows,
    LedgerRange, NetworkId, QueryRows,
};
use datalens_storage::{
    DurableStorage, LocalObjectStore, MaintenanceCompactionConfig, ObjectLockLease,
    ObjectPutIfAbsentResult, ObjectStore, S3ObjectStore, S3ObjectStoreConfig, StorageWriteRequest,
    validate_object_key,
};

mod support;

use support::CountingObjectStore;

#[derive(Clone, Debug)]
struct ReplaceAfterLockReadStore {
    inner: LocalObjectStore,
    trigger_owner: Vec<u8>,
    replacement_owner: Vec<u8>,
    replaced: Arc<AtomicBool>,
}

impl ReplaceAfterLockReadStore {
    fn new(root: PathBuf, trigger_owner: &[u8], replacement_owner: &[u8]) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            trigger_owner: trigger_owner.to_vec(),
            replacement_owner: replacement_owner.to_vec(),
            replaced: Arc::new(AtomicBool::new(false)),
        }
    }

    fn maybe_replace_after_read(
        &self,
        key: &str,
        owner: &[u8],
    ) -> Result<(), datalens_core::DatalensError> {
        if owner == self.trigger_owner.as_slice()
            && self
                .replaced
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.inner.put(key, &self.replacement_owner)?;
        }
        Ok(())
    }
}

impl ObjectStore for ReplaceAfterLockReadStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, datalens_core::DatalensError> {
        self.inner.get(key)
    }

    fn get_optional(&self, key: &str) -> Result<Option<Vec<u8>>, datalens_core::DatalensError> {
        let owner = self.inner.get_optional(key)?;
        if let Some(owner) = owner.as_deref() {
            self.maybe_replace_after_read(key, owner)?;
        }
        Ok(owner)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), datalens_core::DatalensError> {
        self.inner.put(key, bytes)
    }

    fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ObjectPutIfAbsentResult, datalens_core::DatalensError> {
        self.inner.put_if_absent(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, datalens_core::DatalensError> {
        self.inner.exists(key)
    }

    fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<datalens_storage::ObjectMetadata>, datalens_core::DatalensError> {
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<datalens_storage::ObjectListPage, datalens_core::DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), datalens_core::DatalensError> {
        self.inner.delete(key)
    }
}

#[test]
fn test_object_store_key_validation_rejects_unsafe_relative_paths() {
    for key in [
        "",
        "/absolute",
        "a//b",
        "a/./b",
        "a/../b",
        "a\\b",
        ".datalens-tmp/object.tmp",
        "chains/.datalens-tmp/object.tmp",
    ] {
        let error = validate_object_key(key).expect_err("unsafe key rejected");
        assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    }

    validate_object_key("chains/ethereum/manifest.json").expect("safe key");
}

#[test]
fn test_local_object_store_put_get_exists_list_delete() {
    let store = LocalObjectStore::new(temp_storage_root("local-object-store"));

    assert!(
        !store
            .exists("chains/ethereum/manifest.json")
            .expect("exists")
    );
    store
        .put("chains/ethereum/manifest.json", br#"{"entries":[]}"#)
        .expect("put object");

    assert!(
        store
            .exists("chains/ethereum/manifest.json")
            .expect("exists")
    );
    assert_eq!(
        store
            .get("chains/ethereum/manifest.json")
            .expect("get object"),
        br#"{"entries":[]}"#
    );

    let listed = store.list("chains/ethereum").expect("list objects");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, "chains/ethereum/manifest.json");
    assert_eq!(listed[0].size, br#"{"entries":[]}"#.len() as u64);

    store
        .delete("chains/ethereum/manifest.json")
        .expect("delete object");
    assert!(
        !store
            .exists("chains/ethereum/manifest.json")
            .expect("exists")
    );
}

#[test]
fn test_local_object_store_put_if_absent_creates_once_without_overwrite() {
    let store = LocalObjectStore::new(temp_storage_root("local-put-if-absent"));
    let key = "chains/ethereum/compacted/object.parquet";

    assert_eq!(
        store
            .put_if_absent(key, b"first")
            .expect("create absent object"),
        ObjectPutIfAbsentResult::Created
    );
    assert_eq!(
        store
            .put_if_absent(key, b"second")
            .expect("keep existing object"),
        ObjectPutIfAbsentResult::AlreadyExists
    );

    assert_eq!(store.get(key).expect("read existing object"), b"first");
}

#[test]
fn test_object_store_lock_renew_rejects_owner_mismatch() {
    let store = LocalObjectStore::new(temp_storage_root("local-lock-renew-mismatch"));
    let key = "locks/compaction/chain-a/scope-a.json";
    let mut lease = ObjectLockLease {
        key: key.to_owned(),
        owner: br#"{"owner_id":"leader-a","acquired_at_unix_seconds":1}"#.to_vec(),
    };
    store
        .put(
            key,
            br#"{"owner_id":"leader-b","acquired_at_unix_seconds":1}"#,
        )
        .expect("write competing lock");

    let renewed = store
        .renew_lock(&mut lease, std::time::Duration::from_secs(30))
        .expect("renew lock");

    assert!(!renewed);
    assert_eq!(
        store.get(key).expect("current owner"),
        br#"{"owner_id":"leader-b","acquired_at_unix_seconds":1}"#
    );
}

#[test]
fn test_object_store_lock_renew_extends_lease_before_reacquire() {
    let store = LocalObjectStore::new(temp_storage_root("local-lock-renew-extends"));
    let key = "locks/compaction/chain-a/scope-a.json";
    let mut lease = ObjectLockLease {
        key: key.to_owned(),
        owner: br#"{"owner_id":"leader-a","acquired_at_unix_seconds":1}"#.to_vec(),
    };
    store.put(key, &lease.owner).expect("write old lock");

    assert!(
        store
            .renew_lock(&mut lease, std::time::Duration::from_secs(60))
            .expect("renew lock")
    );

    let contender = store
        .try_acquire_lock_with_ttl(
            key,
            br#"{"owner_id":"leader-b","acquired_at_unix_seconds":9999999999}"#,
            std::time::Duration::from_secs(1),
        )
        .expect("contender acquire");
    assert!(contender.is_none());
}

#[test]
fn test_object_store_lock_release_keeps_replaced_renewed_owner() {
    let store = LocalObjectStore::new(temp_storage_root("local-lock-renew-release"));
    let key = "locks/compaction/chain-a/scope-a.json";
    let mut lease = ObjectLockLease {
        key: key.to_owned(),
        owner: br#"{"owner_id":"leader-a","acquired_at_unix_seconds":1}"#.to_vec(),
    };
    store.put(key, &lease.owner).expect("write old lock");
    assert!(
        store
            .renew_lock(&mut lease, std::time::Duration::from_secs(60))
            .expect("renew lock")
    );
    store
        .put(
            key,
            br#"{"owner_id":"leader-b","acquired_at_unix_seconds":9999999999}"#,
        )
        .expect("replace renewed lock");

    store
        .release_lock(lease)
        .expect("release stale renewed lock");

    assert_eq!(
        store.get(key).expect("current owner"),
        br#"{"owner_id":"leader-b","acquired_at_unix_seconds":9999999999}"#
    );
}

#[test]
fn test_local_lock_release_fails_closed_when_cross_process_guard_is_held() {
    let root = temp_storage_root("local-lock-release-cross-process-guard");
    let stale_process = LocalObjectStore::new(&root);
    let other_process = LocalObjectStore::new(&root);
    let key = "locks/compaction/chain-a/scope-a.json";
    let owner = br#"{"owner_id":"leader-a","acquired_at_unix_seconds":1}"#;
    other_process.put(key, owner).expect("write lock");
    let guard = create_local_lock_guard(&root, key);

    stale_process
        .release_lock(ObjectLockLease {
            key: key.to_owned(),
            owner: owner.to_vec(),
        })
        .expect("stale release while cross-process guard is held");

    assert_eq!(other_process.get(key).expect("current owner"), owner);
    std::fs::remove_dir(guard).expect("remove local lock guard");
}

#[test]
fn test_local_expired_lock_takeover_fails_closed_when_cross_process_guard_is_held() {
    let root = temp_storage_root("local-lock-takeover-cross-process-guard");
    let contender = LocalObjectStore::new(&root);
    let other_process = LocalObjectStore::new(&root);
    let key = "locks/compaction/chain-a/scope-a.json";
    let expired_owner = br#"{"owner_id":"leader-a","acquired_at_unix_seconds":1}"#;
    other_process
        .put(key, expired_owner)
        .expect("write expired lock");
    let guard = create_local_lock_guard(&root, key);

    let lease = contender
        .try_acquire_lock_with_ttl(
            key,
            br#"{"owner_id":"leader-b","acquired_at_unix_seconds":9999999999}"#,
            Duration::from_secs(1),
        )
        .expect("contender acquire while cross-process guard is held");

    assert!(lease.is_none());
    assert_eq!(
        other_process.get(key).expect("current owner"),
        expired_owner
    );
    std::fs::remove_dir(guard).expect("remove local lock guard");
}

#[test]
fn test_default_lock_release_fails_closed_after_owner_changes_between_read_and_delete() {
    let old_owner = br#"{"owner_id":"leader-a","acquired_at_unix_seconds":1}"#;
    let renewed_owner = br#"{"owner_id":"leader-a","acquired_at_unix_seconds":9999999999,"expires_at_unix_seconds":9999999999}"#;
    let store = ReplaceAfterLockReadStore::new(
        temp_storage_root("default-lock-release-fails-closed"),
        old_owner,
        renewed_owner,
    );
    let key = "locks/compaction/chain-a/scope-a.json";
    store.put(key, old_owner).expect("write old lock");

    let error = store
        .release_lock(ObjectLockLease {
            key: key.to_owned(),
            owner: old_owner.to_vec(),
        })
        .expect_err("generic release must fail without conditional delete");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert_eq!(store.get(key).expect("current owner"), renewed_owner);
}

#[test]
fn test_default_expired_lock_takeover_fails_closed_after_owner_changes_before_delete() {
    let expired_owner = br#"{"owner_id":"leader-a","acquired_at_unix_seconds":1}"#;
    let renewed_owner = br#"{"owner_id":"leader-a","acquired_at_unix_seconds":9999999999,"expires_at_unix_seconds":9999999999}"#;
    let store = ReplaceAfterLockReadStore::new(
        temp_storage_root("default-expired-lock-takeover-fails-closed"),
        expired_owner,
        renewed_owner,
    );
    let key = "locks/compaction/chain-a/scope-a.json";
    store.put(key, expired_owner).expect("write expired lock");

    let error = store
        .try_acquire_lock_with_ttl(
            key,
            br#"{"owner_id":"leader-b","acquired_at_unix_seconds":9999999999}"#,
            Duration::from_secs(1),
        )
        .expect_err("generic expired takeover must fail without conditional delete");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert_eq!(store.get(key).expect("current owner"), renewed_owner);
}

#[test]
fn test_local_object_store_lists_valid_tmp_extension_objects() {
    let root = temp_storage_root("local-list-tmp-extension");
    let store = LocalObjectStore::new(&root);
    store
        .put("usage/applications/app/chunks/000000", b"stable")
        .expect("put object");
    store
        .put("usage/applications/app/chunks/000001.tmp-123", b"valid")
        .expect("put valid tmp-extension object");

    let objects = store
        .list("usage/applications/app/chunks")
        .expect("list objects");

    assert_eq!(
        objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "usage/applications/app/chunks/000000",
            "usage/applications/app/chunks/000001.tmp-123",
        ]
    );
}

#[test]
fn test_local_object_store_list_page_respects_limit_and_start_after() {
    let store = LocalObjectStore::new(temp_storage_root("local-list-page"));
    for key in [
        "chains/ethereum/manifest-segments/a/000.json",
        "chains/ethereum/manifest-segments/a/001.json",
        "chains/ethereum/manifest-segments/a/002.json",
    ] {
        store.put(key, b"{}").expect("put object");
    }

    let first = store
        .list_page("chains/ethereum/manifest-segments", None, 2)
        .expect("first page");
    assert_eq!(
        first
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "chains/ethereum/manifest-segments/a/000.json",
            "chains/ethereum/manifest-segments/a/001.json",
        ]
    );
    assert!(first.has_more);

    let second = store
        .list_page(
            "chains/ethereum/manifest-segments",
            Some("chains/ethereum/manifest-segments/a/001.json"),
            2,
        )
        .expect("second page");
    assert_eq!(
        second
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["chains/ethereum/manifest-segments/a/002.json"]
    );
    assert!(!second.has_more);
}

#[test]
fn test_counting_object_store_records_operation_counts() {
    let store = CountingObjectStore::new(LocalObjectStore::new(temp_storage_root(
        "counting-object-store",
    )));
    let key = "coverage-index-v2/delta/chain/000001.json";

    store.put(key, b"first").expect("put object");
    store.put(key, b"second").expect("put object again");
    assert_eq!(
        store
            .put_if_absent(key, b"third")
            .expect("conditional create existing object"),
        ObjectPutIfAbsentResult::AlreadyExists
    );
    store.get(key).expect("get object");
    store.list("coverage-index-v2/delta").expect("list objects");
    store
        .list_page("coverage-index-v2/delta", None, 10)
        .expect("list object page");
    store.delete(key).expect("delete object");

    assert_eq!(store.put_count(key), 2);
    assert_eq!(store.put_if_absent_count(key), 1);
    assert_eq!(store.get_count(key), 1);
    assert_eq!(store.list_count("coverage-index-v2/delta"), 1);
    assert_eq!(store.list_page_count("coverage-index-v2/delta"), 1);
    assert_eq!(store.delete_count(key), 1);
    store.assert_put_count_at_most(key, 2);
}

#[test]
fn test_counting_object_store_reports_overwrite_violations() {
    let store = CountingObjectStore::new(LocalObjectStore::new(temp_storage_root(
        "counting-overwrites",
    )));

    store
        .put("coverage-index-v2/delta/chain/000001.json", b"first")
        .expect("put first delta");
    store
        .put("coverage-index-v2/delta/chain/000001.json", b"second")
        .expect("overwrite first delta");
    store
        .put("coverage-index-v2/delta/chain/000002.json", b"third")
        .expect("put second delta");

    assert_eq!(
        store.overwrite_budget_violations(),
        vec![("coverage-index-v2/delta/chain/000001.json".to_owned(), 2)]
    );
}

#[test]
fn test_counting_object_store_filters_overwrite_assertions_by_prefix() {
    let store = CountingObjectStore::new(LocalObjectStore::new(temp_storage_root(
        "counting-prefix-filter",
    )));

    store
        .put("coverage-index/legacy/head.json", b"first")
        .expect("put legacy head");
    store
        .put("coverage-index/legacy/head.json", b"second")
        .expect("overwrite legacy head");
    store
        .put("coverage-index-v2/delta/chain/000001.json", b"delta")
        .expect("put v2 delta");

    store.assert_no_overwrite("coverage-index-v2/delta");
    assert_eq!(
        store.overwrite_budget_violations_for_prefix("coverage-index/legacy"),
        vec![("coverage-index/legacy/head.json".to_owned(), 2)]
    );
}

#[test]
fn test_s3_object_store_builds_from_compatible_backend_config() {
    let store = S3ObjectStore::from_config(S3ObjectStoreConfig {
        bucket: "datalens".to_owned(),
        prefix: Some("dev/cache".to_owned()),
        region: "auto".to_owned(),
        endpoint_url: Some("http://localhost:9000".to_owned()),
        force_path_style: true,
        runtime_worker_threads: 4,
        max_concurrent_operations: 16,
    })
    .expect("build S3 object store");

    assert_eq!(store.bucket(), "datalens");
    assert_eq!(store.prefix(), Some("dev/cache"));
}

#[tokio::test]
async fn test_s3_object_store_builds_inside_existing_tokio_runtime() {
    let store = S3ObjectStore::from_config(S3ObjectStoreConfig {
        bucket: "datalens".to_owned(),
        prefix: Some("dev/cache".to_owned()),
        region: "auto".to_owned(),
        endpoint_url: Some("http://localhost:9000".to_owned()),
        force_path_style: true,
        runtime_worker_threads: 4,
        max_concurrent_operations: 16,
    })
    .expect("build S3 object store");

    assert_eq!(store.bucket(), "datalens");
    assert_eq!(store.prefix(), Some("dev/cache"));
}

#[test]
fn test_s3_object_store_put_get_exists_list_delete_with_prefix() {
    let Some(config) = s3_test_config() else {
        return;
    };
    let prefix = config.prefix.clone().expect("test prefix");
    let store = S3ObjectStore::from_config(config).expect("build S3 object store");
    let manifest_key = "chains/ethereum/manifest.json";
    let block_key = "chains/ethereum/blocks/1-2.json";

    assert!(!store.exists(manifest_key).expect("manifest absent"));
    store
        .put(manifest_key, br#"{"entries":[]}"#)
        .expect("put manifest");
    store
        .put(block_key, br#"[{"block_number":1}]"#)
        .expect("put rows");

    assert!(store.exists(manifest_key).expect("manifest exists"));
    assert_eq!(
        store.get(manifest_key).expect("get manifest"),
        br#"{"entries":[]}"#
    );

    let listed = store.list("chains/ethereum").expect("list objects");
    let keys = listed
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert!(keys.contains(&manifest_key));
    assert!(keys.contains(&block_key));
    assert!(keys.iter().all(|key| !key.starts_with(&prefix)));

    store.delete(manifest_key).expect("delete manifest");
    assert!(!store.exists(manifest_key).expect("manifest deleted"));
    store.delete(block_key).expect("delete rows");
}

#[test]
fn test_s3_object_store_put_if_absent_creates_once_without_overwrite() {
    let Some(config) = s3_test_config() else {
        return;
    };
    let store = S3ObjectStore::from_config(config).expect("build S3 object store");
    let key = "chains/ethereum/compacted/conditional-create.parquet";

    assert_eq!(
        store
            .put_if_absent(key, b"first")
            .expect("create absent object"),
        ObjectPutIfAbsentResult::Created
    );
    assert_eq!(
        store
            .put_if_absent(key, b"second")
            .expect("keep existing object"),
        ObjectPutIfAbsentResult::AlreadyExists
    );
    assert_eq!(store.get(key).expect("read existing object"), b"first");

    store.delete(key).expect("delete object");
}

#[test]
fn test_s3_compaction_cleans_fragmentation_without_read_regression() {
    let Some(config) = s3_test_config() else {
        return;
    };
    let chain = s3_compaction_test_chain();
    let selector = DatasetSelector::all();
    let store = S3ObjectStore::from_config(config).expect("build S3 object store");
    let mut cleanup = S3PrefixCleanup::new(store.clone(), ["chains"]);
    let storage = DurableStorage::from_object_store(store);
    let manifest_segment_prefix = format!("chains/{}/manifest-segments", chain.key_prefix());
    let queue_prefix = format!("chains/{}/metadata/compaction-queue", chain.key_prefix());
    let data_prefix = format!("chains/{}/datasets", chain.key_prefix());
    let source_blocks = (10_000..10_008).collect::<Vec<_>>();
    let expected_blocks = expected_block_headers(&source_blocks);

    for block in &expected_blocks {
        write_block_object_to_storage(&storage, &chain, &selector, block, FinalityLevel::Safe);
    }

    let before_manifest_segments = list_prefix(&storage, &manifest_segment_prefix).len();
    let before_queue_entries = list_prefix(&storage, &queue_prefix).len();
    let before_compacted_objects = compacted_object_keys(&storage, &data_prefix).len();
    assert_eq!(before_manifest_segments, source_blocks.len());
    assert_eq!(before_queue_entries, source_blocks.len());
    assert_eq!(before_compacted_objects, 0);
    assert_read_block_headers(&storage, &chain, &selector, &expected_blocks);

    let first_report = storage
        .compact_small_objects_for_chain(&chain, s3_compaction_config(false))
        .expect("first s3 compaction");
    assert_eq!(first_report.compacted_objects, 1);
    assert_eq!(first_report.compacted_rows, source_blocks.len());
    assert_eq!(first_report.deleted_source_objects, 0);
    assert_read_block_headers(&storage, &chain, &selector, &expected_blocks);

    let after_first_manifest_segments = list_prefix(&storage, &manifest_segment_prefix).len();
    let after_first_queue_entries = list_prefix(&storage, &queue_prefix).len();
    let after_first_compacted_objects = compacted_object_keys(&storage, &data_prefix).len();
    assert!(
        after_first_manifest_segments < before_manifest_segments,
        "compaction should replace fragmented manifest segments"
    );
    assert!(
        after_first_compacted_objects > before_compacted_objects,
        "compaction should create a compacted data object"
    );
    assert!(
        after_first_queue_entries > before_queue_entries,
        "cleanup disabled should leave consumed queue entries plus the compacted entry"
    );

    let second_report = storage
        .compact_small_objects_for_chain(&chain, s3_compaction_config(true))
        .expect("second s3 compaction cleanup");
    assert_eq!(second_report.compacted_objects, 0);
    assert_eq!(second_report.deleted_source_objects, 0);
    assert_read_block_headers(&storage, &chain, &selector, &expected_blocks);

    let after_second_manifest_segments = list_prefix(&storage, &manifest_segment_prefix).len();
    let after_second_queue_entries = list_prefix(&storage, &queue_prefix).len();
    let after_second_compacted_objects = compacted_object_keys(&storage, &data_prefix).len();
    assert_eq!(
        after_second_manifest_segments,
        after_first_manifest_segments
    );
    assert_eq!(
        after_second_compacted_objects,
        after_first_compacted_objects
    );
    assert!(
        after_second_queue_entries < after_first_queue_entries,
        "cleanup enabled should remove consumed or stale queue entries"
    );
    assert_eq!(
        after_second_queue_entries, after_first_compacted_objects,
        "cleanup should leave only the live compacted queue entry"
    );

    println!(
        "{}",
        serde_json::json!({
            "test": "test_s3_compaction_cleans_fragmentation_without_read_regression",
            "chain": chain.key_prefix(),
            "blocks": source_blocks.len(),
            "manifest_segments": {
                "before": before_manifest_segments,
                "after_first": after_first_manifest_segments,
                "after_second": after_second_manifest_segments,
            },
            "compaction_queue": {
                "before": before_queue_entries,
                "after_first": after_first_queue_entries,
                "after_second": after_second_queue_entries,
            },
            "compacted_objects": {
                "before": before_compacted_objects,
                "after_first": after_first_compacted_objects,
                "after_second": after_second_compacted_objects,
            },
            "first_report": {
                "compacted_objects": first_report.compacted_objects,
                "compacted_rows": first_report.compacted_rows,
                "processed_candidates": first_report.processed_candidates,
                "deleted_source_objects": first_report.deleted_source_objects,
            },
            "second_report": {
                "compacted_objects": second_report.compacted_objects,
                "compacted_rows": second_report.compacted_rows,
                "processed_candidates": second_report.processed_candidates,
                "deleted_source_objects": second_report.deleted_source_objects,
                "source_delete_failures": second_report.source_delete_failures,
            },
        })
    );
    cleanup.cleanup();
}

#[test]
fn test_s3_coverage_index_v2_compaction_handles_hot_bucket() {
    let Some(config) = s3_test_config() else {
        return;
    };
    let chain = s3_compaction_test_chain();
    let selector = DatasetSelector::all();
    let store = S3ObjectStore::from_config(config).expect("build S3 object store");
    let mut cleanup = S3PrefixCleanup::new(store.clone(), ["chains"]);
    let storage = DurableStorage::from_object_store(store);
    let blocks = (20_000..20_129).collect::<Vec<_>>();
    let expected_blocks = expected_block_headers(&blocks);

    for block in &expected_blocks {
        write_block_object_to_storage(&storage, &chain, &selector, block, FinalityLevel::Safe);
    }
    assert_read_block_headers(&storage, &chain, &selector, &expected_blocks);

    let report = storage
        .compact_small_objects_for_chain(
            &chain,
            MaintenanceCompactionConfig {
                coverage_index_v2_delta_count_threshold: 128,
                max_gets_per_tick: 256,
                ..s3_compaction_config(false)
            },
        )
        .expect("s3 coverage index v2 compaction");
    assert_eq!(report.coverage_index_v2_compacted_buckets, 1);
    assert_eq!(report.coverage_index_v2_compacted_deltas, blocks.len());
    assert_eq!(
        list_prefix(
            &storage,
            &format!(
                "chains/{}/coverage-index-v2/snapshot-heads",
                chain.key_prefix()
            )
        )
        .len(),
        1
    );
    assert_read_block_headers(&storage, &chain, &selector, &expected_blocks);

    println!(
        "{}",
        serde_json::json!({
            "test": "test_s3_coverage_index_v2_compaction_handles_hot_bucket",
            "chain": chain.key_prefix(),
            "blocks": blocks.len(),
            "coverage_index_v2_compacted_buckets": report.coverage_index_v2_compacted_buckets,
            "coverage_index_v2_compacted_deltas": report.coverage_index_v2_compacted_deltas,
        })
    );
    cleanup.cleanup();
}

fn s3_compaction_config(cleanup_enabled: bool) -> MaintenanceCompactionConfig {
    MaintenanceCompactionConfig {
        min_object_bytes: u64::MAX,
        max_input_objects_per_candidate: 16,
        max_tick_duration_ms: 30_000,
        max_candidates_per_tick: 4,
        max_concurrent_candidates: 4,
        max_manifest_entries_per_tick: 20_000,
        max_gets_per_tick: 128,
        max_puts_per_tick: 16,
        max_deletes_per_tick: 128,
        cleanup_enabled,
        delete_source_objects: false,
        ..MaintenanceCompactionConfig::default()
    }
}

fn write_block_object_to_storage<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    selector: &DatasetSelector,
    block: &BlockHeader,
    finality: FinalityLevel,
) {
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![block.clone()]),
    )
    .expect("rows");
    storage
        .write_rows(StorageWriteRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: LedgerRange::blocks(block.number, block.number).expect("range"),
            rows: &rows,
            finality_level: finality,
            record_empty_coverage: true,
        })
        .expect("write rows");
}

fn assert_read_block_headers<S: ObjectStore>(
    storage: &DurableStorage<S>,
    chain: &ChainIdentity,
    selector: &DatasetSelector,
    expected: &[BlockHeader],
) {
    let rows = storage
        .read_rows(
            chain,
            &DatasetKey::evm_blocks(),
            selector,
            LedgerRange::blocks(
                expected.first().expect("first expected block").number,
                expected.last().expect("last expected block").number,
            )
            .expect("read range"),
        )
        .expect("read rows");
    match rows.into_rows() {
        QueryRows::EvmBlocks(blocks) => assert_eq!(blocks.as_slice(), expected),
        rows => panic!("expected evm block rows, got {rows:?}"),
    }
}

fn expected_block_headers(numbers: &[u64]) -> Vec<BlockHeader> {
    numbers
        .iter()
        .map(|number| BlockHeader {
            number: *number,
            hash: format!("0xblock{number}"),
            parent_hash: format!("0xparent{number}"),
            timestamp: number.saturating_mul(12),
        })
        .collect()
}

fn list_prefix<S: ObjectStore>(storage: &DurableStorage<S>, prefix: &str) -> Vec<String> {
    let mut keys = storage
        .object_store()
        .list(prefix)
        .expect("list prefix")
        .into_iter()
        .map(|object| object.key)
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

fn compacted_object_keys<S: ObjectStore>(
    storage: &DurableStorage<S>,
    data_prefix: &str,
) -> Vec<String> {
    list_prefix(storage, data_prefix)
        .into_iter()
        .filter(|key| key.contains("/compacted/"))
        .collect()
}

fn s3_compaction_test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}

struct S3PrefixCleanup {
    store: S3ObjectStore,
    prefixes: Vec<String>,
    cleaned: bool,
}

impl S3PrefixCleanup {
    fn new<const N: usize>(store: S3ObjectStore, prefixes: [&str; N]) -> Self {
        Self {
            store,
            prefixes: prefixes.into_iter().map(ToOwned::to_owned).collect(),
            cleaned: false,
        }
    }

    fn cleanup(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        for prefix in &self.prefixes {
            let objects = match self.store.list(prefix) {
                Ok(objects) => objects,
                Err(error) => {
                    eprintln!(
                        "warning: S3 test cleanup failed to list prefix {prefix}: {:?}: {}",
                        error.kind, error.message
                    );
                    continue;
                }
            };
            for object in objects {
                if let Err(error) = self.store.delete(&object.key) {
                    eprintln!(
                        "warning: S3 test cleanup failed to delete {}: {:?}: {}",
                        object.key, error.kind, error.message
                    );
                }
            }
        }
    }
}

impl Drop for S3PrefixCleanup {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-object-store-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn create_local_lock_guard(root: &std::path::Path, key: &str) -> PathBuf {
    let guard = root.join(".datalens-tmp").join("locks").join(key);
    std::fs::create_dir_all(guard.parent().expect("guard parent")).expect("create guard parent");
    std::fs::create_dir(&guard).expect("create local lock guard");
    guard
}

fn s3_test_config() -> Option<S3ObjectStoreConfig> {
    if std::env::var("DATALENS_RUN_S3_TESTS").ok().as_deref() != Some("1") {
        return None;
    }
    let bucket = std::env::var("DATALENS_S3_BUCKET")
        .expect("DATALENS_S3_BUCKET must be set when DATALENS_RUN_S3_TESTS=1");
    let base_prefix =
        std::env::var("DATALENS_S3_PREFIX").unwrap_or_else(|_| "datalens-tests".to_owned());
    let test_prefix = format!(
        "object-store-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    );
    let base_prefix = base_prefix.trim().trim_matches('/');
    let prefix = if base_prefix.is_empty() {
        test_prefix
    } else {
        format!("{base_prefix}/{test_prefix}")
    };
    Some(S3ObjectStoreConfig {
        bucket,
        prefix: Some(prefix),
        region: std::env::var("DATALENS_S3_REGION").unwrap_or_else(|_| "auto".to_owned()),
        endpoint_url: std::env::var("DATALENS_S3_ENDPOINT_URL").ok(),
        force_path_style: std::env::var("DATALENS_S3_FORCE_PATH_STYLE")
            .map(|value| value != "0" && value != "false")
            .unwrap_or(true),
        runtime_worker_threads: 4,
        max_concurrent_operations: 16,
    })
}
