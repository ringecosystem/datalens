use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, ProviderDiagnostics,
    SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, Dataset, DatasetKey,
    LedgerRange, LogFilter, NetworkId, QueryFinalityRequirement, QueryRows, TopicFilter,
};
use datalens_edge::{
    QueryService, QueryServiceRegistry,
    config::{DatalensConfig, WriterConfig},
    router,
};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_planner::{FieldSelection, NativeQueryInput};
use datalens_storage::{
    DurableStorage, LocalObjectStore, LocalStorage, ObjectMetadata, ObjectPutIfAbsentResult,
    ObjectStore, QueryWatermarkKey, QueryWatermarkRepository, QueryWatermarkStore,
    ReadThroughCacheConfig,
};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupChunkPolicy, WarmupRetryPolicy, WarmupRuntime, WarmupRuntimeConfig,
    WarmupSchedulerConfig, WarmupSubmitRequest, WarmupTaskMode, WarmupTaskPool,
};
use tower::ServiceExt;

#[tokio::test]
async fn test_production_readiness_validates_service_staging_warmup_metrics_and_hot_boundary() {
    let root = temp_storage_root("production-readiness");
    let config_path = write_config(&root);
    let config = DatalensConfig::from_file(&config_path).expect("config boundary loads");
    let chain = config
        .chains
        .get("ethereum")
        .expect("configured ethereum chain")
        .clone();
    let store = CountingObjectStore::new(&root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::enabled(8),
    );
    let source = MockSource::default().with_blocks(vec![
        block(10),
        block(12),
        block(13),
        block(14),
        block(90),
    ]);
    let service = QueryService::new_with_metrics_config(
        storage.clone(),
        source.clone(),
        config.planner.clone(),
        config.writer.clone(),
        "ethereum",
        chain,
        config.metrics.clone(),
    )
    .expect("service boundary builds from config");
    let metrics = service.metrics_recorder();
    let service = service.with_warmup_pool(warmup_pool(&root, &config.writer, metrics));

    let staged = service
        .query_native(blocks_request(10, 10))
        .expect("staging boundary returns provider rows");
    assert_eq!(block_numbers(&staged), vec![10]);
    assert_eq!(
        staged.cache.missing_ranges,
        vec![LedgerRange::blocks(10, 10).expect("range")]
    );
    service
        .wait_for_durable_promotions()
        .expect("promotion drain");
    let manifest = storage.manifest().expect("manifest after promotion drain");
    assert_eq!(
        manifest.entries.len(),
        1,
        "background promotion should flush sub-threshold query rows into durable coverage"
    );
    let shutdown_flush = service
        .flush_staged_writes_for_shutdown()
        .expect("writer boundary has no remaining staged rows");
    assert!(shutdown_flush.data_objects.is_empty());
    let object_key = manifest.entries[0]
        .object_key
        .clone()
        .expect("flushed data object key");

    let first_hit = service
        .query_native(blocks_request(10, 10))
        .expect("durable boundary serves flushed rows");
    let second_hit = service
        .query_native(blocks_request(10, 10))
        .expect("read-through boundary serves cached object bytes");
    assert_eq!(
        first_hit.cache.durable_hit_ranges,
        vec![LedgerRange::blocks(10, 10).expect("range")]
    );
    assert_eq!(
        second_hit.cache.durable_hit_ranges,
        vec![LedgerRange::blocks(10, 10).expect("range")]
    );
    assert_eq!(store.read_count(&object_key), 1);

    let flushed = service
        .query_native(blocks_request(12, 14))
        .expect("writer boundary flushes when staged rows reach threshold");
    assert_eq!(block_numbers(&flushed), vec![12, 13, 14]);
    service
        .wait_for_durable_promotions()
        .expect("threshold promotion drain");
    assert!(
        storage
            .manifest()
            .expect("manifest after threshold flush")
            .entries
            .len()
            >= 2,
        "writer boundary should create durable manifest coverage after threshold flush"
    );

    let hot = service
        .query_native(NativeQueryInput {
            finality: QueryFinalityRequirement::LatestOnly,
            ..blocks_request(90, 90)
        })
        .expect("hot boundary serves explicit latest query without durable write");
    assert_eq!(block_numbers(&hot), vec![90]);
    assert_eq!(
        hot.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(90, 90).expect("range")]
    );
    assert!(hot.cache.durable_hit_ranges.is_empty());

    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry boundary");
    let app = router(registry);

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "production-app")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(20, 21))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 20,
                        "end": 21,
                        "mode": "fixed_range",
                        "chunk_policy": {
                            "max_range_len": 2
                        }
                    }))
                    .expect("warmup request json"),
                ))
                .expect("warmup submit request"),
        )
        .await
        .expect("warmup submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);

    let run_once = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .body(Body::empty())
                .expect("warmup run-once request"),
        )
        .await
        .expect("warmup run-once response");
    assert_eq!(run_once.status(), StatusCode::OK);

    source.clear_calls();
    let warmed = app
        .clone()
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body(logs_request(20, 21))))
                .expect("logs query request"),
        )
        .await
        .expect("warmed query response");
    assert_eq!(warmed.status(), StatusCode::OK);
    assert_eq!(
        source.calls(),
        Vec::<SourceCall>::new(),
        "service boundary should hit warmup-created durable coverage"
    );

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics = body_text(metrics.into_body()).await;
    assert!(metrics.contains(r#"application="prod-readiness""#));
    assert!(metrics.contains(r#"chain="ethereum""#));
    assert!(metrics.contains(r#"chain_kind="evm""#));
    assert!(metrics.contains(r#"dataset="evm.blocks""#));
    assert!(metrics.contains(r#"datalens_query_total"#));
    assert!(metrics.contains(r#"outcome="hit""#));
    assert!(metrics.contains(r#"outcome="hot_miss""#));
    assert!(metrics.contains(r#"datalens_fill_total"#));
    assert!(metrics.contains(r#"outcome="live_fetch""#));
    assert!(metrics.contains(r#"datalens_durable_write_total"#));
    assert!(metrics.contains(r#"outcome="flushed""#));
    assert!(metrics.contains(r#"datalens_warmup_task_total"#));
    assert!(metrics.contains(r#"datalens_warmup_fetch_total"#));
    assert!(metrics.contains(r#"datalens_warmup_write_total"#));
    assert!(metrics.contains(r#"application="production-app""#));
}

#[tokio::test]
async fn test_service_wires_query_watermarks_into_follow_query_warmup() {
    let root = temp_storage_root("production-follow-query-watermarks");
    let config_path = write_config(&root);
    let config = DatalensConfig::from_file(&config_path).expect("config boundary loads");
    let chain = config
        .chains
        .get("ethereum")
        .expect("configured ethereum chain")
        .clone();
    let storage: Arc<dyn datalens_storage::StorageRepository> = Arc::new(LocalStorage::new(&root));
    let source = MockSource::default();
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(&root));
    let service = QueryService::new_with_metrics_config(
        storage.clone(),
        source.clone(),
        config.planner.clone(),
        config.writer.clone(),
        "ethereum",
        chain,
        config.metrics.clone(),
    )
    .expect("service boundary builds from config")
    .with_query_watermarks(
        watermarks.clone(),
        ApplicationIdentity::named("prod-readiness"),
    );
    let registry = LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry")));
    let runtime = WarmupRuntime::new(
        source.clone(),
        storage,
        registry.clone(),
        datalens_writer::DurableWriterConfig {
            target_object_bytes: config.writer.target_object_bytes,
            min_object_rows: config.writer.min_object_rows,
            record_empty_coverage: config.writer.record_empty_coverage,
            staging: datalens_writer::WriteStagingConfig::default(),
        },
    )
    .with_durable_writer(service.durable_writer())
    .with_query_watermarks(watermarks.clone())
    .with_follow_query_lookahead_blocks(config.warmup.follow_query_lookahead_blocks)
    .with_runtime_config(WarmupRuntimeConfig {
        max_fetches_per_task_loop: 1,
    });
    let query = logs_request(20, 21);

    service
        .query_native(query.clone())
        .expect("query records watermark");

    let watermark_key = QueryWatermarkKey::new(
        "prod-readiness",
        ethereum_identity(),
        DatasetKey::evm_logs(),
        &query.selector,
        datalens_core::LedgerRangeKind::Block,
    );
    assert_eq!(wait_for_query_watermark(&watermarks, &watermark_key), 21);
    source.clear_calls();

    let task_id = registry
        .submit(WarmupSubmitRequest {
            application_id: "prod-readiness".to_owned(),
            chain: ethereum_identity(),
            dataset_key: DatasetKey::evm_logs(),
            selector: query.selector,
            range_kind: datalens_core::LedgerRangeKind::Block,
            start: 1,
            end: None,
            mode: WarmupTaskMode::FollowQuery,
            chunk_policy: WarmupChunkPolicy {
                max_range_len: 4,
                target_rows_hint: None,
            },
            retry_policy: WarmupRetryPolicy::default(),
        })
        .expect("submit follow-query task")
        .task_id;

    runtime.run_task_once(&task_id).expect("run follow-query");

    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(71, 74))]
    );
    let cursor = registry
        .load_cursor(&task_id)
        .expect("load cursor")
        .expect("cursor");
    assert_eq!(cursor.next, 75);
}

#[derive(Clone, Debug)]
struct CountingObjectStore {
    inner: LocalObjectStore,
    reads: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl CountingObjectStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            reads: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn read_count(&self, key: &str) -> usize {
        *self
            .reads
            .lock()
            .expect("read counts")
            .get(key)
            .unwrap_or(&0)
    }
}

impl ObjectStore for CountingObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        *self
            .reads
            .lock()
            .expect("read counts")
            .entry(key.to_owned())
            .or_default() += 1;
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
    ) -> Result<datalens_storage::ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone)]
struct MockSource {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
}

impl Default for MockSource {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl MockSource {
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks") = blocks;
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls").clone()
    }

    fn clear_calls(&self) {
        self.calls.lock().expect("calls").clear();
    }
}

impl ChainAdapter for MockSource {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(ethereum_identity())
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Blocks)
                    .with_selector(SelectorKind::All)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(4)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Logs)
                    .with_selector(SelectorKind::EvmLogs)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(4)
                    .with_max_addresses_per_query(2)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100).with_finality(FinalityKind::Safe))
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100).with_finality(FinalityKind::Finalized))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = request.range.block_range().expect("block range");
        if request.dataset_key == DatasetKey::evm_blocks() {
            self.calls
                .lock()
                .expect("calls")
                .push(SourceCall::Blocks(range));
            let rows = self
                .blocks
                .lock()
                .expect("blocks")
                .iter()
                .filter(|block| range.contains(block.number))
                .cloned()
                .collect();
            response(request, QueryRows::EvmBlocks(rows))
        } else if request.dataset_key == DatasetKey::evm_logs() {
            self.calls
                .lock()
                .expect("calls")
                .push(SourceCall::Logs(range));
            response(request, QueryRows::EvmLogs(Vec::new()))
        } else {
            unreachable!("production-readiness fixture only serves EVM blocks and logs")
        }
    }
}

fn response(
    request: ChainFetchRequest,
    rows: QueryRows,
) -> Result<ChainFetchResponse, DatalensError> {
    ChainFetchResponse::try_new(
        request.chain,
        request.dataset_key,
        request.range,
        request.selector,
        rows,
    )
    .map(|response| {
        response.with_provider_diagnostics(ProviderDiagnostics {
            calls: 1,
            rows_scanned: 0,
            warnings: Vec::new(),
        })
    })
}

async fn body_text(body: Body) -> String {
    String::from_utf8(
        to_bytes(body, usize::MAX)
            .await
            .expect("body bytes")
            .to_vec(),
    )
    .expect("utf8 body")
}

fn warmup_pool(
    root: &Path,
    writer: &WriterConfig,
    metrics: Option<MetricsRecorder>,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    let registry = LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry")));
    let mut runtime = WarmupRuntime::new(
        MockSource::default(),
        LocalStorage::new(root),
        registry,
        datalens_writer::DurableWriterConfig {
            target_object_bytes: writer.target_object_bytes,
            min_object_rows: writer.min_object_rows,
            record_empty_coverage: writer.record_empty_coverage,
            staging: datalens_writer::WriteStagingConfig {
                enabled: writer.staging.enabled,
                min_rows: writer.staging.min_rows,
                target_object_bytes: writer.staging.target_object_bytes,
                max_staged_ranges: writer.staging.max_staged_ranges,
                max_staged_rows: writer.staging.max_staged_rows,
                max_staged_age_ms: writer.staging.max_staged_age_ms,
                flush_on_shutdown: writer.staging.flush_on_shutdown,
                max_staged_bytes: writer.staging.max_staged_bytes,
            },
        },
    )
    .with_runtime_config(WarmupRuntimeConfig {
        max_fetches_per_task_loop: 4,
    });
    if let Some(metrics) = metrics {
        runtime = runtime.with_metrics(metrics);
    }
    WarmupTaskPool::new(
        runtime,
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    )
}

fn blocks_request(from_block: u64, to_block: u64) -> NativeQueryInput {
    NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        ledger_range: LedgerRange::blocks(from_block, to_block).expect("valid range"),
        selector: DatasetSelector::all(),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

fn logs_request(from_block: u64, to_block: u64) -> NativeQueryInput {
    NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_logs(),
        ledger_range: LedgerRange::blocks(from_block, to_block).expect("valid range"),
        selector: DatasetSelector::try_evm_logs(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: Vec::new(),
        })
        .expect("valid logs selector"),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

fn query_body(request: NativeQueryInput) -> Vec<u8> {
    let selector = match &request.selector {
        DatasetSelector::All => serde_json::json!({ "kind": "all" }),
        DatasetSelector::EvmLogs(filter) => serde_json::json!({
            "kind": "evm_logs",
            "value": evm_log_filter_value(filter)
        }),
        DatasetSelector::Other {
            kind,
            fingerprint,
            canonical_key,
        } => serde_json::json!({
            "kind": "other",
            "value": {
                "kind": kind.as_str(),
                "fingerprint": fingerprint,
                "canonical_key": canonical_key
            }
        }),
    };
    serde_json::to_vec(&serde_json::json!({
        "chain": request.chain,
        "dataset_key": request.dataset_key.as_str(),
        "selector": selector,
        "range": {
            "kind": "block",
            "start": request.ledger_range.start(),
            "end": request.ledger_range.end()
        },
        "finality": request.finality,
        "fields": "all"
    }))
    .expect("query request json")
}

fn evm_logs_selector_value(request: &NativeQueryInput) -> serde_json::Value {
    match &request.selector {
        DatasetSelector::EvmLogs(filter) => evm_log_filter_value(filter),
        _ => panic!("expected evm logs selector"),
    }
}

fn evm_log_filter_value(filter: &datalens_core::EvmLogFilter) -> serde_json::Value {
    serde_json::json!({
        "addresses": filter.addresses(),
        "topics": filter
            .topics()
            .iter()
            .map(|topic| match topic {
                TopicFilter::Wildcard => serde_json::Value::Null,
                TopicFilter::AnyOf(values) => serde_json::json!(values),
            })
            .collect::<Vec<_>>()
    })
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn block(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        hash: format!("0x{number:064x}"),
        parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
        timestamp: number * 10,
    }
}

fn block_numbers(response: &datalens_edge::NativeQueryResponse) -> Vec<u64> {
    match response.rows.rows() {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected block rows"),
    }
}

fn write_config(root: &Path) -> PathBuf {
    let path = root.with_file_name(format!(
        "datalens-production-readiness-{}.toml",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::write(
        &path,
        format!(
            r#"
            [server]
            bind = "127.0.0.1:0"

            [storage]
            backend = "local"

            [storage.local]
            root = "{}"

            [planner]
            max_query_range_blocks = 8
            default_chunk_range_blocks = 4

            [writer]
            target_object_bytes = 1048576
            min_object_rows = 3
            record_empty_coverage = true

            [writer.staging]
            enabled = true
            min_rows = 3
            target_object_bytes = 1048576
            max_staged_ranges = 8
            max_staged_rows = 16
            max_staged_age_ms = 60000
            flush_on_shutdown = true
            max_staged_bytes = 1048576

            [metrics]
            enabled = true
            default_application = "prod-readiness"

            [warmup]
            enabled = true
            registry_path = "{}"
            scheduler_interval_ms = 1000
            max_global_tasks = 1
            max_per_chain_tasks = 1
            max_fetches_per_loop = 4
            follow_query_lookahead_blocks = 2048
            flush_on_shutdown = true

            [index]
            default_chunk_range = 2
            max_concurrency = 1
            default_finality = "finalized"
            cursor_path = "{}"

            [chains.ethereum]
            kind = "evm"
            chain_id = 1
            rpc_urls = ["http://example.invalid"]

            [chains.ethereum.finality]
            mode = "lag"
            safe_lag_blocks = 4
            finalized_lag_blocks = 8

            [chains.ethereum.datasets.blocks]
            enabled = true
            max_batch_blocks = 4

            [chains.ethereum.datasets.logs]
            enabled = true
            max_get_logs_range_blocks = 4
            max_addresses_per_query = 2
            "#,
            root.display(),
            root.join("warmup").display(),
            root.join("cursors").display()
        ),
    )
    .expect("write production-readiness config");
    path
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

fn wait_for_query_watermark<R>(watermarks: &R, key: &QueryWatermarkKey) -> u64
where
    R: QueryWatermarkRepository,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(watermark) = watermarks.read(key).expect("read watermark") {
            return watermark.latest_block;
        }
        if Instant::now() >= deadline {
            panic!("watermark was not recorded");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
