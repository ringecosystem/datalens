use std::path::PathBuf;

use datalens_core::DatalensErrorKind;
use datalens_storage::{
    LocalObjectStore, ObjectPutIfAbsentResult, ObjectStore, S3ObjectStore, S3ObjectStoreConfig,
    validate_object_key,
};

mod support;

use support::CountingObjectStore;

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
    })
}
