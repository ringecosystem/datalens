use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange,
    NetworkId,
};
use datalens_storage::{
    CacheOutcome, FillOutcome, LocalObjectStore, ObjectMetadata, ObjectStore, QueryOutcome,
    UsageLedgerEntry, UsageLedgerRepository, UsageLedgerStore,
};

#[test]
fn test_usage_ledger_appends_and_reads_application_events() {
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(temp_storage_root("append-read")));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let entry = UsageLedgerEntry::query_event(
        "analytics-api",
        chain.clone(),
        DatasetKey::evm_blocks(),
        &selector,
        LedgerRange::blocks(10, 12).expect("valid range"),
        FinalityLevel::Safe,
        QueryOutcome::Filled,
        CacheOutcome::Miss,
        FillOutcome::Written,
        3,
    )
    .with_request_id("request-1");

    ledger.append(&entry).expect("append usage event");

    let events = ledger
        .read_application("analytics-api")
        .expect("read application usage");
    assert_eq!(events, vec![entry]);
    assert!(
        ledger
            .read_application("other-api")
            .expect("read other application")
            .is_empty()
    );
}

#[test]
fn test_usage_ledger_partitions_by_application_chain_and_day() {
    let store = LocalObjectStore::new(temp_storage_root("partition"));
    let ledger = UsageLedgerStore::new(store.clone());
    let chain = test_chain();
    let selector = DatasetSelector::all();

    ledger
        .append(&UsageLedgerEntry::query_event(
            "api/with unsafe chars",
            chain.clone(),
            DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(1, 1).expect("valid range"),
            FinalityLevel::Safe,
            QueryOutcome::Hit,
            CacheOutcome::Hit,
            FillOutcome::NotAttempted,
            1,
        ))
        .expect("append usage event");

    let objects = store.list("usage").expect("list usage objects");
    assert_eq!(objects.len(), 1);
    assert!(objects[0].key.contains("/chains/evm/ethereum/1/"));
    assert!(objects[0].key.ends_with(".jsonl"));
    assert!(!objects[0].key.contains("api/with unsafe chars"));
}

#[test]
fn test_usage_ledger_rotates_bounded_chunks_for_repeated_hits() {
    let store = CountingObjectStore::new(temp_storage_root("repeated-hits"));
    let ledger = UsageLedgerStore::new(store.clone());
    let selector = DatasetSelector::all();
    let entry = UsageLedgerEntry::query_event(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        LedgerRange::blocks(1, 1).expect("valid range"),
        FinalityLevel::Safe,
        QueryOutcome::Hit,
        CacheOutcome::Hit,
        FillOutcome::NotAttempted,
        1,
    );

    for request in 0..1_000 {
        ledger
            .append(&entry.clone().with_request_id(format!("request-{request}")))
            .expect("append repeated hit");
    }

    let objects = store.list("usage").expect("list usage objects");
    let durable_bytes: u64 = objects.iter().map(|object| object.size).sum();
    assert!(objects.len() <= 16, "usage objects: {objects:#?}");
    assert!(
        objects.iter().all(|object| object.size <= 96 * 1024),
        "usage objects: {objects:#?}"
    );
    assert!(
        store.total_put_bytes() <= durable_bytes * 128,
        "total put bytes {} durable bytes {durable_bytes}",
        store.total_put_bytes()
    );

    let events = ledger
        .read_application("analytics-api")
        .expect("read application usage");
    assert_eq!(events.len(), 1_000);
    assert!(
        events
            .iter()
            .all(|event| event.query_outcome == QueryOutcome::Hit)
    );
}

#[test]
fn test_usage_ledger_write_failure_is_reported() {
    let ledger = UsageLedgerStore::new(FailingPutObjectStore);
    let selector = DatasetSelector::all();
    let entry = UsageLedgerEntry::query_event(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        LedgerRange::blocks(1, 1).expect("valid range"),
        FinalityLevel::Safe,
        QueryOutcome::Hit,
        CacheOutcome::Hit,
        FillOutcome::NotAttempted,
        1,
    );

    let error = ledger.append(&entry).expect_err("write failure returned");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
}

#[test]
fn test_usage_ledger_serializes_hot_cache_reorg_and_promotion_outcomes() {
    let selector = DatasetSelector::all();
    let entry = UsageLedgerEntry::query_event(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        LedgerRange::blocks(11, 12).expect("valid range"),
        FinalityLevel::Latest,
        QueryOutcome::HotHit,
        CacheOutcome::HotHit,
        FillOutcome::LiveFetch,
        2,
    );

    let encoded = serde_json::to_string(&entry).expect("encode usage entry");

    assert!(encoded.contains(r#""finality":"latest""#));
    assert!(encoded.contains(r#""requested_hot":true"#));
    assert!(encoded.contains(r#""query_outcome":"hot_hit""#));
    assert!(encoded.contains(r#""cache_outcome":"hot_hit""#));
    assert!(encoded.contains(r#""fill_outcome":"live_fetch""#));
    assert_eq!(QueryOutcome::ReorgRollback, QueryOutcome::ReorgRollback);
    assert_eq!(FillOutcome::PromotionWritten, FillOutcome::PromotionWritten);
}

#[test]
fn test_usage_ledger_serializes_default_durable_only_request_contract() {
    let selector = DatasetSelector::all();
    let entry = UsageLedgerEntry::query_event(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        LedgerRange::blocks(1, 1).expect("valid range"),
        FinalityLevel::Safe,
        QueryOutcome::Hit,
        CacheOutcome::Hit,
        FillOutcome::NotAttempted,
        1,
    );

    let encoded = serde_json::to_string(&entry).expect("encode usage entry");

    assert!(encoded.contains(r#""requested_hot":false"#));
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-usage-ledger-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

#[derive(Clone, Debug)]
struct CountingObjectStore {
    inner: LocalObjectStore,
    total_put_bytes: Arc<AtomicU64>,
}

impl CountingObjectStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            total_put_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    fn total_put_bytes(&self) -> u64 {
        self.total_put_bytes.load(Ordering::SeqCst)
    }
}

impl ObjectStore for CountingObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.total_put_bytes
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
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
struct FailingPutObjectStore;

impl ObjectStore for FailingPutObjectStore {
    fn get(&self, _key: &str) -> Result<Vec<u8>, DatalensError> {
        Ok(Vec::new())
    }

    fn put(&self, _key: &str, _bytes: &[u8]) -> Result<(), DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected ledger write failure",
        ))
    }

    fn exists(&self, _key: &str) -> Result<bool, DatalensError> {
        Ok(false)
    }

    fn list(&self, _prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        Ok(Vec::new())
    }

    fn delete(&self, _key: &str) -> Result<(), DatalensError> {
        Ok(())
    }
}
