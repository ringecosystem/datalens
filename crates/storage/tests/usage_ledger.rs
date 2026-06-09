use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange,
    NetworkId,
};
use datalens_storage::{
    CacheOutcome, FillOutcome, LocalObjectStore, ObjectListPage, ObjectMetadata, ObjectStore,
    QueryActivity, QueryActivityKey, QueryActivityRepository, QueryActivityStore, QueryOutcome,
    QueryWatermark, QueryWatermarkKey, QueryWatermarkRepository, QueryWatermarkStore,
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

#[test]
fn test_query_watermark_records_highest_selector_progress_without_rewinding() {
    let store = QueryWatermarkStore::new(LocalObjectStore::new(temp_storage_root("watermark")));
    let selector = DatasetSelector::all();
    let key = QueryWatermarkKey::new(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        datalens_core::LedgerRangeKind::Block,
    );

    store
        .update(&QueryWatermark {
            key: key.clone(),
            latest_block: 25,
            updated_at_unix_seconds: 100,
        })
        .expect("write watermark");
    store
        .update(&QueryWatermark {
            key: key.clone(),
            latest_block: 20,
            updated_at_unix_seconds: 200,
        })
        .expect("stale update does not rewind");

    let watermark = store
        .read(&key)
        .expect("read watermark")
        .expect("watermark");
    assert_eq!(watermark.latest_block, 25);
    assert_eq!(watermark.updated_at_unix_seconds, 100);
    assert_eq!(watermark.key.selector_fingerprint, selector.fingerprint());
    assert_eq!(
        watermark.key.selector_canonical_key,
        selector.canonical_key()
    );
}

#[test]
fn test_query_activity_can_rewind_without_rewinding_query_watermark() {
    let root = temp_storage_root("query-activity-rewinds");
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let activities = QueryActivityStore::new(LocalObjectStore::new(&root));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let watermark_key = QueryWatermarkKey::new(
        "analytics-api",
        chain.clone(),
        DatasetKey::evm_blocks(),
        &selector,
        LedgerRange::blocks(1, 1).unwrap().kind(),
    );
    let activity_key = QueryActivityKey::new(
        "analytics-api",
        chain.clone(),
        DatasetKey::evm_blocks(),
        &selector,
        LedgerRange::blocks(1, 1).unwrap().kind(),
    );

    watermarks
        .update(&QueryWatermark {
            key: watermark_key.clone(),
            latest_block: 100,
            updated_at_unix_seconds: 10,
        })
        .expect("write high watermark");
    activities
        .update(&QueryActivity {
            key: activity_key.clone(),
            latest_range: LedgerRange::blocks(90, 100).unwrap(),
            updated_at_unix_seconds: 10,
            request_id: Some("query-high".to_owned()),
        })
        .expect("write high activity");

    watermarks
        .update(&QueryWatermark {
            key: watermark_key.clone(),
            latest_block: 20,
            updated_at_unix_seconds: 20,
        })
        .expect("lower watermark update is ignored");
    activities
        .update(&QueryActivity {
            key: activity_key.clone(),
            latest_range: LedgerRange::blocks(10, 20).unwrap(),
            updated_at_unix_seconds: 20,
            request_id: Some("query-low".to_owned()),
        })
        .expect("activity rewinds");

    let watermark = watermarks
        .read(&watermark_key)
        .expect("read watermark")
        .expect("watermark");
    let activity = activities
        .read(&activity_key)
        .expect("read activity")
        .expect("activity");
    assert_eq!(watermark.latest_block, 100);
    assert_eq!(watermark.updated_at_unix_seconds, 10);
    assert_eq!(activity.latest_range, LedgerRange::blocks(10, 20).unwrap());
    assert_eq!(activity.updated_at_unix_seconds, 20);
    assert_eq!(activity.request_id.as_deref(), Some("query-low"));
    assert_eq!(
        activity.key.selector_canonical_key,
        selector.canonical_key()
    );
}

#[test]
fn test_query_activity_skips_older_out_of_order_update() {
    let store = QueryActivityStore::new(LocalObjectStore::new(temp_storage_root(
        "query-activity-out-of-order",
    )));
    let selector = DatasetSelector::all();
    let key = QueryActivityKey::new(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        datalens_core::LedgerRangeKind::Block,
    );

    store
        .update(&QueryActivity {
            key: key.clone(),
            latest_range: LedgerRange::blocks(90, 100).unwrap(),
            updated_at_unix_seconds: 200,
            request_id: Some("q-newer".to_owned()),
        })
        .expect("write newer activity");
    store
        .update(&QueryActivity {
            key: key.clone(),
            latest_range: LedgerRange::blocks(10, 20).unwrap(),
            updated_at_unix_seconds: 100,
            request_id: Some("q-older".to_owned()),
        })
        .expect("older out-of-order update is ignored");

    let activity = store.read(&key).expect("read activity").expect("activity");
    assert_eq!(activity.latest_range, LedgerRange::blocks(90, 100).unwrap());
    assert_eq!(activity.updated_at_unix_seconds, 200);
    assert_eq!(activity.request_id.as_deref(), Some("q-newer"));
}

#[test]
fn test_query_activity_allows_newer_lower_block_update() {
    let store = QueryActivityStore::new(LocalObjectStore::new(temp_storage_root(
        "query-activity-newer-lower-block",
    )));
    let selector = DatasetSelector::all();
    let key = QueryActivityKey::new(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        datalens_core::LedgerRangeKind::Block,
    );

    store
        .update(&QueryActivity {
            key: key.clone(),
            latest_range: LedgerRange::blocks(90, 100).unwrap(),
            updated_at_unix_seconds: 100,
            request_id: Some("q-high".to_owned()),
        })
        .expect("write older high activity");
    store
        .update(&QueryActivity {
            key: key.clone(),
            latest_range: LedgerRange::blocks(10, 20).unwrap(),
            updated_at_unix_seconds: 200,
            request_id: Some("q-low".to_owned()),
        })
        .expect("newer lower activity rewinds range");

    let activity = store.read(&key).expect("read activity").expect("activity");
    assert_eq!(activity.latest_range, LedgerRange::blocks(10, 20).unwrap());
    assert_eq!(activity.updated_at_unix_seconds, 200);
    assert_eq!(activity.request_id.as_deref(), Some("q-low"));
}

#[test]
fn test_query_activity_uses_request_id_tie_breaker_for_equal_timestamps() {
    let store = QueryActivityStore::new(LocalObjectStore::new(temp_storage_root(
        "query-activity-request-id-tie",
    )));
    let selector = DatasetSelector::all();
    let key = QueryActivityKey::new(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        datalens_core::LedgerRangeKind::Block,
    );

    store
        .update(&QueryActivity {
            key: key.clone(),
            latest_range: LedgerRange::blocks(90, 100).unwrap(),
            updated_at_unix_seconds: 200,
            request_id: Some("q002".to_owned()),
        })
        .expect("write newer request id");
    store
        .update(&QueryActivity {
            key: key.clone(),
            latest_range: LedgerRange::blocks(10, 20).unwrap(),
            updated_at_unix_seconds: 200,
            request_id: Some("q001".to_owned()),
        })
        .expect("older request id update is ignored");

    let activity = store.read(&key).expect("read activity").expect("activity");
    assert_eq!(activity.latest_range, LedgerRange::blocks(90, 100).unwrap());
    assert_eq!(activity.updated_at_unix_seconds, 200);
    assert_eq!(activity.request_id.as_deref(), Some("q002"));
}

#[test]
fn test_query_watermark_cloned_store_updates_do_not_race_rewind() {
    let object_store =
        PausingFirstPutObjectStore::new(LocalObjectStore::new(temp_storage_root("watermark-race")));
    let first_put_started = object_store.first_put_started();
    let release_first_put = object_store.release_first_put();
    let store = QueryWatermarkStore::new(object_store);
    let lower_store = store.clone();
    let higher_store = store.clone();
    let selector = DatasetSelector::all();
    let key = QueryWatermarkKey::new(
        "analytics-api",
        test_chain(),
        DatasetKey::evm_blocks(),
        &selector,
        datalens_core::LedgerRangeKind::Block,
    );
    let lower_key = key.clone();
    let lower = thread::spawn(move || {
        lower_store
            .update(&QueryWatermark {
                key: lower_key,
                latest_block: 20,
                updated_at_unix_seconds: 100,
            })
            .expect("lower update")
    });
    first_put_started
        .recv()
        .expect("lower update reached first put");

    let higher_key = key.clone();
    let higher = thread::spawn(move || {
        higher_store
            .update(&QueryWatermark {
                key: higher_key,
                latest_block: 25,
                updated_at_unix_seconds: 200,
            })
            .expect("higher update")
    });
    release_first_put.send(()).expect("release lower update");
    lower.join().expect("lower update joins");
    higher.join().expect("higher update joins");

    let watermark = store
        .read(&key)
        .expect("read watermark")
        .expect("watermark");
    assert_eq!(watermark.latest_block, 25);
    assert_eq!(watermark.updated_at_unix_seconds, 200);
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

#[derive(Clone, Debug)]
struct PausingFirstPutObjectStore {
    inner: LocalObjectStore,
    paused: Arc<AtomicBool>,
    started: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    release_sender: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

impl PausingFirstPutObjectStore {
    fn new(inner: LocalObjectStore) -> Self {
        let (started_tx, _started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        Self {
            inner,
            paused: Arc::new(AtomicBool::new(false)),
            started: Arc::new(Mutex::new(Some(started_tx))),
            release: Arc::new(Mutex::new(release_rx)),
            release_sender: Arc::new(Mutex::new(Some(release_tx))),
        }
    }

    fn first_put_started(&self) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel();
        *self.started.lock().expect("started sender") = Some(tx);
        rx
    }

    fn release_first_put(&self) -> mpsc::Sender<()> {
        self.release_sender
            .lock()
            .expect("release sender")
            .as_ref()
            .expect("release sender")
            .clone()
    }
}

impl ObjectStore for PausingFirstPutObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        if self
            .paused
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            if let Some(started) = self.started.lock().expect("started sender").take() {
                started.send(()).expect("send first put started");
            }
            self.release
                .lock()
                .expect("release receiver")
                .recv()
                .expect("release first put");
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

    fn list_page(
        &self,
        _prefix: &str,
        _start_after: Option<&str>,
        _limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        Ok(ObjectListPage {
            objects: Vec::new(),
            has_more: false,
        })
    }

    fn delete(&self, _key: &str) -> Result<(), DatalensError> {
        Ok(())
    }
}
