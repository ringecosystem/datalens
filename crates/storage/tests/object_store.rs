use std::path::PathBuf;

use datalens_core::DatalensErrorKind;
use datalens_storage::{
    LocalObjectStore, ObjectStore, S3ObjectStore, S3ObjectStoreConfig, validate_object_key,
};

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
