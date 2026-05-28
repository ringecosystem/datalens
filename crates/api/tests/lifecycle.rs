use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use datalens_api::config::{
    BlocksDatasetConfig, ChainConfig, DatasetsConfig, LogsDatasetConfig, PlannerConfig,
    WriterConfig,
};
use datalens_api::{LegacyEvmQueryRequest, QueryService, QueryServiceRegistry, router};
use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, FinalityKind, HeightRangeKind, ProviderDiagnostics, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, LedgerRange, LogFilter, NetworkId, QueryFinalityRequirement, QueryRows,
};
use datalens_metrics::MetricsRecorder;
use datalens_storage::{
    DurableStorage, LocalObjectStore, LocalStorage, Manifest, ObjectMetadata, ObjectStore,
    ReadThroughCacheConfig, S3ObjectStore, S3ObjectStoreConfig, StorageRepository,
    StorageWriteOutcome, StorageWriteRequest,
};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};
use tower::ServiceExt;

#[test]
fn test_local_lifecycle_records_metrics_for_miss_fill_hit_and_provider_error() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let source = MockSource::default().with_blocks(vec![block(10), block(11)]);
    let lifecycle_service = service(
        LocalStorage::new(temp_storage_root("metrics-lifecycle")),
        source.clone(),
    )
    .with_metrics(recorder.clone());
    let request = blocks_request(10, 11);

    lifecycle_service
        .query(request.clone())
        .expect("miss fills cache");
    lifecycle_service.query(request).expect("hit reads cache");

    let metrics = recorder.encode().expect("metrics text");
    assert!(metrics.contains(
        r#"datalens_query_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="filled"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_query_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="hit"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_cache_coverage_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="miss"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_cache_coverage_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="hit"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_fill_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="filled"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_application_chain_latest_requested_block{application="unknown",chain="ethereum",chain_kind="evm",dataset="blocks"} 11"#
    ));
    assert!(metrics.contains(
        r#"datalens_application_chain_latest_filled_block{application="unknown",chain="ethereum",chain_kind="evm",dataset="blocks"} 11"#
    ));
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 11))]
    );

    let error_source = MockSource::default().with_error(DatalensErrorKind::ProviderTimeout);
    let error_service = service(
        LocalStorage::new(temp_storage_root("metrics-error")),
        error_source,
    )
    .with_metrics(recorder.clone());
    let error = error_service
        .query(blocks_request(20, 20))
        .expect_err("provider error");
    assert_eq!(error.kind, DatalensErrorKind::ProviderTimeout);

    let metrics = recorder.encode().expect("metrics text");
    assert!(metrics.contains(
        r#"datalens_provider_error_total{chain="ethereum",chain_kind="evm",dataset="blocks",error_kind="provider_timeout"} 1"#
    ));
}

#[tokio::test]
async fn test_api_lifecycle_routes_expose_health_chains_query_and_metrics() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let source = MockSource::default().with_blocks(vec![block(30)]);
    let service = service(
        LocalStorage::new(temp_storage_root("api-lifecycle")),
        source.clone(),
    )
    .with_metrics(recorder);
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let health = app
        .clone()
        .oneshot(
            Request::get("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("health response");
    assert_eq!(health.status(), StatusCode::OK);

    let chains = app
        .clone()
        .oneshot(
            Request::get("/v1/chains")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("chains response");
    assert_eq!(chains.status(), StatusCode::OK);
    let chains_body = body_json(chains.into_body()).await;
    assert_eq!(chains_body["chains"], serde_json::json!(["ethereum"]));

    let query = app
        .clone()
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&blocks_request(30, 30)).expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("query response");
    assert_eq!(query.status(), StatusCode::OK);

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let body = body_text(metrics.into_body()).await;
    assert!(body.contains("# HELP datalens_query_total"));
    assert!(body.contains(r#"chain="ethereum""#));
    assert!(body.contains(r#"chain_kind="evm""#));
    assert!(body.contains(r#"dataset="blocks""#));
}

#[tokio::test]
async fn test_api_warmup_routes_manage_application_scoped_tasks() {
    let root = temp_storage_root("api-warmup-routes");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let application_registry = datalens_api::config::ApplicationRegistryConfig {
        required: true,
        applications: vec![
            application_config("app-a", "token-a"),
            application_config("app-b", "token-b"),
        ],
    };
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry)
        .expect("application registry")
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset": "logs",
                        "range": BlockRange::expect_new(10, 12),
                        "filter": logs_request(10, 12).filter,
                        "mode": "fixed_range",
                        "chunk_policy": {
                            "max_range_len": 2
                        }
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);
    let submit_body = body_json(submit.into_body()).await;
    let task_id = submit_body["task_id"].as_str().expect("task id").to_owned();
    assert_eq!(submit_body["created"], true);

    let list = app
        .clone()
        .oneshot(
            Request::get("/v1/warmup/tasks")
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("list response");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = body_json(list.into_body()).await;
    assert_eq!(list_body["tasks"].as_array().expect("tasks").len(), 1);

    let forbidden = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/warmup/tasks/{task_id}/cancel"))
                .header("x-datalens-application", "app-b")
                .header("authorization", "Bearer token-b")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("forbidden response");
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let cancel = app
        .oneshot(
            Request::post(format!("/v1/warmup/tasks/{task_id}/cancel"))
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("cancel response");
    assert_eq!(cancel.status(), StatusCode::OK);
    let cancel_body = body_json(cancel.into_body()).await;
    assert_eq!(cancel_body["task"]["state"], "cancelled");
}

#[tokio::test]
async fn test_api_warmup_run_once_writes_durable_coverage_that_query_hits() {
    let root = temp_storage_root("api-warmup-run-once");
    let source = MockSource::default();
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let service = service(LocalStorage::new(&root), source.clone())
        .with_metrics(recorder.clone())
        .with_warmup_pool(warmup_pool_with_metrics(&root, recorder));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset": "logs",
                        "range": BlockRange::expect_new(20, 21),
                        "filter": logs_request(20, 21).filter,
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);

    let run_once = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("run once response");
    assert_eq!(run_once.status(), StatusCode::OK);

    source.calls.lock().expect("calls").clear();
    let query = app
        .clone()
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&logs_request(20, 21)).expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("query response");
    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(
        source.calls(),
        Vec::<SourceCall>::new(),
        "query should hit warmup-created durable coverage"
    );

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let body = body_text(metrics.into_body()).await;
    assert!(body.contains("datalens_warmup_task_total"));
    assert!(body.contains("datalens_warmup_fetch_total"));
    assert!(body.contains("datalens_warmup_write_total"));
}

#[test]
fn test_local_lifecycle_covers_multichain_storage_isolation_and_unknown_chain() {
    let root = temp_storage_root("multi-chain");
    let ethereum_source = MockSource::default()
        .with_chain(ethereum_identity())
        .with_blocks(vec![block(40)]);
    let polygon_source = MockSource::default()
        .with_chain(polygon_identity())
        .with_blocks(vec![BlockHeader {
            hash: "0xpolygon".to_owned(),
            ..block(40)
        }]);
    let ethereum = service_named(
        LocalStorage::new(&root),
        ethereum_source.clone(),
        "ethereum",
        chain_config(1),
    );
    let polygon = service_named(
        LocalStorage::new(&root),
        polygon_source.clone(),
        "polygon",
        chain_config(137),
    );

    ethereum
        .query(LegacyEvmQueryRequest {
            chain: ethereum_identity(),
            ..blocks_request(40, 40)
        })
        .expect("ethereum query");
    polygon
        .query(LegacyEvmQueryRequest {
            chain: polygon_identity(),
            ..blocks_request(40, 40)
        })
        .expect("polygon query");

    assert!(root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(root.join("chains/evm/polygon/137/manifest.json").exists());
    assert_eq!(
        ethereum_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(40, 40))]
    );
    assert_eq!(
        polygon_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(40, 40))]
    );

    let error = ethereum
        .query(LegacyEvmQueryRequest {
            chain: polygon_identity(),
            ..blocks_request(40, 40)
        })
        .expect_err("unknown chain for route");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_empty_logs_lifecycle_records_empty_coverage_without_data_object_and_hits_cache() {
    let root = temp_storage_root("empty-logs");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source.clone());
    let request = logs_request(50, 51);

    let first = service.query(request.clone()).expect("empty logs miss");
    let second = service.query(request).expect("empty logs hit");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(50, 51)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(50, 51)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(50, 51))]
    );
    assert!(root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_local_lifecycle_returns_provider_rows_when_durable_write_fails_without_coverage() {
    let root = temp_storage_root("write-failure-provider-rows");
    let source = MockSource::default().with_blocks(vec![block(65)]);
    let service = QueryService::new_named(
        FailingWriteStorage::new(LocalStorage::new(&root)),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "ethereum",
        chain_config(1),
    );

    let response = service
        .query(blocks_request(65, 65))
        .expect("provider rows are returned despite durable write failure");

    assert_eq!(block_numbers(&response), vec![65]);
    assert_eq!(
        response.cache.missing_ranges,
        vec![BlockRange::expect_new(65, 65)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(65, 65))]
    );
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_local_lifecycle_durable_hit_reads_through_cache_after_manifest_coverage() {
    let root = temp_storage_root("read-through-lifecycle");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::enabled(16),
    );
    let source = MockSource::default().with_blocks(vec![block(70), block(71)]);
    let service = QueryService::new_named(
        storage.clone(),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "ethereum",
        chain_config(1),
    );
    let request = blocks_request(70, 71);

    let first = service.query(request.clone()).expect("miss fills cache");
    let object_key = storage
        .manifest()
        .expect("manifest")
        .entries
        .into_iter()
        .find_map(|entry| entry.object_key)
        .expect("object key");
    let second = service.query(request.clone()).expect("first durable hit");
    let third = service.query(request).expect("second durable hit");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(70, 71)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(70, 71)]
    );
    assert_eq!(third.cache.hit_ranges, vec![BlockRange::expect_new(70, 71)]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(70, 71))]
    );
    assert_eq!(store.read_count(&object_key), 1);
}

#[test]
fn test_s3_lifecycle_is_gated_and_uses_dedicated_prefix() {
    let Some(config) = s3_test_config() else {
        return;
    };
    let store = S3ObjectStore::from_config(config).expect("build S3 object store");
    cleanup_s3_prefix(&store);
    let storage = DurableStorage::from_object_store(store.clone());
    let source = MockSource::default().with_blocks(vec![block(60)]);
    let service = QueryService::new_named(
        storage,
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "ethereum",
        chain_config(1),
    );
    let request = blocks_request(60, 60);

    let first = service.query(request.clone()).expect("S3 miss fills cache");
    let second = service.query(request).expect("S3 hit reads cache");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(60, 60)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(60, 60)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(60, 60))]
    );

    let manifest = DurableStorage::from_object_store(store.clone())
        .manifest()
        .expect("S3 manifest");
    assert_eq!(manifest.entries.len(), 1);
    let entry = &manifest.entries[0];
    assert_eq!(entry.chain, ethereum_identity());
    assert_eq!(entry.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(entry.row_count, 1);
    assert!(entry.object_key.is_some());
    assert!(entry.object_size_bytes.is_some_and(|size| size > 0));
    assert_eq!(entry.checksum_algorithm.as_deref(), Some("sha256"));

    cleanup_s3_prefix(&store);
}

#[derive(Clone, Debug)]
struct CountingObjectStore {
    inner: LocalObjectStore,
    reads: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl CountingObjectStore {
    fn new(root: PathBuf) -> Self {
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

#[derive(Clone)]
struct FailingWriteStorage {
    inner: LocalStorage,
}

impl FailingWriteStorage {
    fn new(inner: LocalStorage) -> Self {
        Self { inner }
    }
}

impl StorageRepository for FailingWriteStorage {
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        self.inner.manifest()
    }

    fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &datalens_chain::DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        self.inner
            .covered_ranges(chain, dataset_key, selector, range)
    }

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &datalens_chain::DatasetSelector,
        range: LedgerRange,
    ) -> Result<datalens_core::DatasetRows, DatalensError> {
        self.inner.read_rows(chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        _request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected durable write failure",
        ))
    }
}

async fn body_json(body: Body) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(body, usize::MAX).await.expect("body bytes"))
        .expect("json body")
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

fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
    service_named(storage, source, "ethereum", chain_config(1))
}

fn service_named(
    storage: LocalStorage,
    source: MockSource,
    chain_name: &str,
    chain: ChainConfig,
) -> QueryService<MockSource> {
    QueryService::new_named(
        storage,
        source,
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        chain_name,
        chain,
    )
}

fn chain_config(chain_id: u64) -> ChainConfig {
    ChainConfig {
        kind: "evm".to_owned(),
        chain_id,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        finality: datalens_api::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: BlocksDatasetConfig {
                enabled: true,
                max_batch_blocks: 4,
            },
            logs: LogsDatasetConfig {
                enabled: true,
                max_get_logs_range_blocks: 4,
                max_addresses_per_query: 2,
            },
        },
    }
}

fn blocks_request(from_block: u64, to_block: u64) -> LegacyEvmQueryRequest {
    LegacyEvmQueryRequest {
        chain: ethereum_identity(),
        dataset: Dataset::Blocks,
        range: BlockRange::expect_new(from_block, to_block),
        filter: None,
        include_block: false,
        allow_hot: false,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

fn logs_request(from_block: u64, to_block: u64) -> LegacyEvmQueryRequest {
    LegacyEvmQueryRequest {
        chain: ethereum_identity(),
        dataset: Dataset::Logs,
        range: BlockRange::expect_new(from_block, to_block),
        filter: Some(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: Vec::new(),
        }),
        include_block: false,
        allow_hot: false,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

fn warmup_pool(
    root: &std::path::Path,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    warmup_pool_inner(root, None)
}

fn warmup_pool_with_metrics(
    root: &std::path::Path,
    recorder: MetricsRecorder,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    warmup_pool_inner(root, Some(recorder))
}

fn warmup_pool_inner(
    root: &std::path::Path,
    recorder: Option<MetricsRecorder>,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    let storage = LocalStorage::new(root);
    let registry = LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry")));
    let mut runtime = WarmupRuntime::new(
        MockSource::default(),
        storage,
        registry,
        datalens_writer::DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    )
    .with_runtime_config(WarmupRuntimeConfig {
        max_fetches_per_task_loop: 4,
    });
    if let Some(recorder) = recorder {
        runtime = runtime.with_metrics(recorder);
    }
    WarmupTaskPool::new(
        runtime,
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    )
}

fn application_config(id: &str, token: &str) -> datalens_api::config::ApplicationConfig {
    datalens_api::config::ApplicationConfig {
        id: id.to_owned(),
        name: id.to_owned(),
        enabled: true,
        display_name: None,
        token: token.to_owned(),
        chains: vec!["ethereum".to_owned()],
        datasets: vec!["logs".to_owned()],
        quota: None,
    }
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn polygon_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
        .expect("valid chain")
}

fn block(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        hash: format!("0x{number:02x}"),
        parent_hash: format!("0x{number:02x}-parent"),
        timestamp: number * 10,
    }
}

fn block_numbers(response: &datalens_api::LegacyEvmQueryResponse) -> Vec<u64> {
    match &response.rows {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected block rows"),
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-lifecycle-{name}-{}",
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
        "lifecycle-{}",
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

fn cleanup_s3_prefix(store: &S3ObjectStore) {
    if let Ok(objects) = store.list("chains") {
        for object in objects {
            store
                .delete(&object.key)
                .expect("delete S3 lifecycle object");
        }
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
    error: Arc<Mutex<Option<DatalensErrorKind>>>,
    chain: Arc<Mutex<ChainIdentity>>,
}

impl Default for MockSource {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            error: Arc::new(Mutex::new(None)),
            chain: Arc::new(Mutex::new(ethereum_identity())),
        }
    }
}

impl MockSource {
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    fn with_chain(self, chain: ChainIdentity) -> Self {
        *self.chain.lock().expect("chain lock") = chain;
        self
    }

    fn with_error(self, kind: DatalensErrorKind) -> Self {
        *self.error.lock().expect("error lock") = Some(kind);
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
    }
}

impl ChainAdapter for MockSource {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.lock().expect("chain lock").clone())
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

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = request.range.block_range().expect("block range");
        if let Some(kind) = self.error.lock().expect("error lock").clone() {
            return Err(DatalensError::new(kind, "mock provider error"));
        }
        match request.dataset_key.legacy_dataset() {
            Some(Dataset::Blocks) => {
                self.calls
                    .lock()
                    .expect("calls lock")
                    .push(SourceCall::Blocks(range));
                let rows = self
                    .blocks
                    .lock()
                    .expect("blocks lock")
                    .iter()
                    .filter(|block| range.contains(block.number))
                    .cloned()
                    .collect();
                Ok(ChainFetchResponse::try_new(
                    request.chain,
                    DatasetKey::evm_blocks(),
                    LedgerRange::from_block_range(range),
                    request.selector,
                    QueryRows::EvmBlocks(rows),
                )?
                .with_provider_diagnostics(ProviderDiagnostics {
                    calls: 1,
                    rows_scanned: 0,
                    warnings: Vec::new(),
                }))
            }
            Some(Dataset::Logs) => {
                self.calls
                    .lock()
                    .expect("calls lock")
                    .push(SourceCall::Logs(range));
                Ok(ChainFetchResponse::try_new(
                    request.chain,
                    DatasetKey::evm_logs(),
                    LedgerRange::from_block_range(range),
                    request.selector,
                    QueryRows::EvmLogs(Vec::new()),
                )?
                .with_provider_diagnostics(ProviderDiagnostics {
                    calls: 1,
                    rows_scanned: 0,
                    warnings: Vec::new(),
                }))
            }
            Some(Dataset::Transactions) | Some(Dataset::Receipts) => {
                unreachable!("lifecycle fixtures only request blocks or logs")
            }
            None => unreachable!("only EVM datasets are used in lifecycle tests"),
        }
    }
}
