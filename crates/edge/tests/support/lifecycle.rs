#![allow(dead_code, unused_imports)]

pub(crate) use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
pub(crate) use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, ProviderDiagnostics,
    SelectorKind,
};
pub(crate) use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, LedgerRange, LogFilter, NetworkId, QueryFinalityRequirement, QueryRows,
    TopicFilter,
};
pub(crate) use datalens_edge::config::{
    ApplicationOperationConfig, BlocksDatasetConfig, ChainConfig, DatasetsConfig,
    LogsDatasetConfig, PlannerConfig, WriterConfig, WriterStagingConfig,
};
pub(crate) use datalens_edge::{QueryService, QueryServiceRegistry, ServiceLifecycle, router};
pub(crate) use datalens_metrics::MetricsRecorder;
pub(crate) use datalens_planner::{FieldSelection, NativeQueryInput};
pub(crate) use datalens_solana::{
    SolanaAdapter, SolanaBlock, SolanaCommitment, SolanaFixtureRpc, SolanaRpc, solana_all_selector,
};
pub(crate) use datalens_storage::{
    DurableStorage, LocalObjectStore, LocalStorage, Manifest, ObjectListPage, ObjectMetadata,
    ObjectPutIfAbsentResult, ObjectStore, ReadThroughCacheConfig, S3ObjectStore,
    S3ObjectStoreConfig, StorageRepository, StorageWriteOutcome, StorageWriteRequest,
};
pub(crate) use datalens_tron::{
    TronAdapter, TronBlock, TronFinality, TronFixtureProviderRpc, TronProvider, tron_all_selector,
};
pub(crate) use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};
pub(crate) use serde_json::Value;
pub(crate) use tower::ServiceExt;

#[derive(Clone, Debug)]
pub(crate) struct CountingObjectStore {
    inner: LocalObjectStore,
    reads: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl CountingObjectStore {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            reads: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn read_count(&self, key: &str) -> usize {
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
    ) -> Result<ObjectListPage, DatalensError> {
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

#[derive(Clone)]
pub(crate) struct FailingWriteStorage {
    inner: LocalStorage,
}

impl FailingWriteStorage {
    pub(crate) fn new(inner: LocalStorage) -> Self {
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

pub(crate) struct ControlledShutdownScheduler {
    pub(crate) registry: QueryServiceRegistry,
    pub(crate) events: Arc<Mutex<Vec<String>>>,
}

impl datalens_edge::LifecycleShutdown for ControlledShutdownScheduler {
    fn shutdown(self) {
        self.events
            .lock()
            .expect("events")
            .push("scheduler_shutdown".to_owned());
        self.registry
            .query_native(blocks_request(10, 10))
            .expect("scheduler stages rows before final flush");
    }
}

pub(crate) async fn body_json(body: Body) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(body, usize::MAX).await.expect("body bytes"))
        .expect("json body")
}

pub(crate) async fn body_text(body: Body) -> String {
    String::from_utf8(
        to_bytes(body, usize::MAX)
            .await
            .expect("body bytes")
            .to_vec(),
    )
    .expect("utf8 body")
}

pub(crate) fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
    service_named(storage, source, "ethereum", chain_config(1))
}

pub(crate) fn service_named(
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

pub(crate) fn planner_config() -> PlannerConfig {
    PlannerConfig {
        max_query_range_blocks: 8,
        default_chunk_range_blocks: 4,
    }
}

pub(crate) fn writer_config() -> WriterConfig {
    WriterConfig {
        target_object_bytes: 1024,
        min_object_rows: 1,
        record_empty_coverage: true,
        staging: Default::default(),
    }
}

pub(crate) fn chain_config(chain_id: u64) -> ChainConfig {
    ChainConfig {
        kind: "evm".to_owned(),
        chain_id,
        rpc_url: None,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        rpc: None,
        warmup: Default::default(),
        trongrid: Default::default(),
        finality: datalens_edge::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: BlocksDatasetConfig {
                enabled: true,
                max_batch_blocks: 4,
            },
            logs: LogsDatasetConfig {
                enabled: true,
                reliability_enabled: true,
                receipt_fallback_enabled: true,
                query_strategy: Default::default(),
                max_get_logs_range_blocks: 4,
                max_block_scan_range_blocks: 4,
                max_addresses_per_query: 2,
                header_fetch_mode: "batch".to_owned(),
                header_fetch_concurrency: 8,
                header_fetch_batch_size: 20,
                header_cache_max_entries: 50_000,
                header_durable_chunk_size_blocks: 1_000,
            },
        },
    }
}

pub(crate) fn non_evm_chain_config(kind: &str) -> ChainConfig {
    ChainConfig {
        kind: kind.to_owned(),
        chain_id: 0,
        rpc_url: None,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        rpc: None,
        warmup: Default::default(),
        trongrid: Default::default(),
        finality: datalens_edge::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: BlocksDatasetConfig {
                enabled: false,
                max_batch_blocks: 2,
            },
            logs: LogsDatasetConfig {
                enabled: false,
                reliability_enabled: true,
                receipt_fallback_enabled: true,
                query_strategy: Default::default(),
                max_get_logs_range_blocks: 2,
                max_block_scan_range_blocks: 2,
                max_addresses_per_query: 2,
                header_fetch_mode: "batch".to_owned(),
                header_fetch_concurrency: 8,
                header_fetch_batch_size: 20,
                header_cache_max_entries: 50_000,
                header_durable_chunk_size_blocks: 1_000,
            },
        },
    }
}

pub(crate) fn blocks_request(from_block: u64, to_block: u64) -> NativeQueryInput {
    NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        ledger_range: LedgerRange::blocks(from_block, to_block).expect("valid range"),
        selector: DatasetSelector::all(),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

pub(crate) fn logs_request(from_block: u64, to_block: u64) -> NativeQueryInput {
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

pub(crate) fn query_body(request: NativeQueryInput) -> Vec<u8> {
    let selector = query_selector_json(&request);
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

pub(crate) fn query_selector_json(request: &NativeQueryInput) -> serde_json::Value {
    match &request.selector {
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
    }
}

pub(crate) fn evm_logs_selector_value(request: &NativeQueryInput) -> serde_json::Value {
    match &request.selector {
        DatasetSelector::EvmLogs(filter) => evm_log_filter_value(filter),
        _ => panic!("expected evm logs selector"),
    }
}

pub(crate) fn evm_log_filter_value(filter: &datalens_core::EvmLogFilter) -> serde_json::Value {
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

pub(crate) fn warmup_pool(
    root: &std::path::Path,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    warmup_pool_inner(root, None, 4)
}

pub(crate) fn warmup_pool_with_metrics(
    root: &std::path::Path,
    recorder: MetricsRecorder,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    warmup_pool_inner(root, Some(recorder), 4)
}

pub(crate) fn warmup_pool_with_max_fetches(
    root: &std::path::Path,
    max_fetches_per_task_loop: u64,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    warmup_pool_inner(root, None, max_fetches_per_task_loop)
}

fn warmup_pool_inner(
    root: &std::path::Path,
    recorder: Option<MetricsRecorder>,
    max_fetches_per_task_loop: u64,
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
        max_fetches_per_task_loop,
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

pub(crate) fn warmup_pool_for<A>(
    root: &std::path::Path,
    adapter: A,
) -> WarmupTaskPool<A, LocalStorage, LocalWarmupRegistry<LocalObjectStore>>
where
    A: ChainAdapter,
{
    let storage = LocalStorage::new(root);
    let registry = LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry")));
    let runtime = WarmupRuntime::new(
        adapter,
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
    WarmupTaskPool::new(
        runtime,
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    )
}

pub(crate) fn application_config(
    id: &str,
    token: &str,
) -> datalens_edge::config::ApplicationConfig {
    datalens_edge::config::ApplicationConfig {
        id: id.to_owned(),
        name: id.to_owned(),
        enabled: true,
        display_name: None,
        token: token.to_owned(),
        chains: vec!["ethereum".to_owned()],
        datasets: vec!["evm.logs".to_owned()],
        operations: vec![
            ApplicationOperationConfig::Query,
            ApplicationOperationConfig::Discovery,
            ApplicationOperationConfig::WarmupSubmit,
            ApplicationOperationConfig::WarmupRead,
            ApplicationOperationConfig::WarmupMutate,
            ApplicationOperationConfig::WarmupRun,
            ApplicationOperationConfig::CacheRepairSubmit,
            ApplicationOperationConfig::CacheRepairRead,
            ApplicationOperationConfig::CacheRepairMutate,
            ApplicationOperationConfig::CacheRepairRun,
        ],
        quota: None,
    }
}

pub(crate) fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

pub(crate) fn polygon_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
        .expect("valid chain")
}

pub(crate) fn solana_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(NetworkId::textual("mainnet-beta").expect("valid network")),
    )
    .expect("valid chain")
}

pub(crate) fn tron_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("tron".to_owned()),
        "tron",
        Some(NetworkId::numeric(728126428)),
    )
    .expect("valid chain")
}

pub(crate) fn native_warmup_request(body: serde_json::Value) -> Request<Body> {
    Request::post("/v1/warmup/tasks")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).expect("request json")))
        .expect("request")
}

pub(crate) fn native_warmup_ensure_request(body: serde_json::Value) -> Request<Body> {
    Request::post("/v1/warmup/tasks/ensure")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).expect("request json")))
        .expect("request")
}

pub(crate) fn block(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        hash: format!("0x{number:02x}"),
        parent_hash: format!("0x{number:02x}-parent"),
        timestamp: number * 10,
    }
}

pub(crate) fn block_numbers(response: &datalens_edge::NativeQueryResponse) -> Vec<u64> {
    match response.rows.rows() {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected block rows"),
    }
}

pub(crate) fn temp_storage_root(name: &str) -> PathBuf {
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

pub(crate) fn s3_test_config() -> Option<S3ObjectStoreConfig> {
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

pub(crate) fn cleanup_s3_prefix(store: &S3ObjectStore) {
    if let Ok(objects) = store.list("chains") {
        for object in objects {
            store
                .delete(&object.key)
                .expect("delete S3 lifecycle object");
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone, Default)]
pub(crate) struct CountingSolanaRpc {
    inner: SolanaFixtureRpc,
    fail_data_fetches: Arc<Mutex<bool>>,
    data_fetches: Arc<Mutex<usize>>,
}

impl CountingSolanaRpc {
    pub(crate) fn fail_data_fetches(&self) {
        *self.fail_data_fetches.lock().expect("fail lock") = true;
    }

    pub(crate) fn data_fetch_count(&self) -> usize {
        *self.data_fetches.lock().expect("fetch lock")
    }
}

impl SolanaRpc for CountingSolanaRpc {
    fn get_slot(&self, commitment: SolanaCommitment) -> Result<u64, DatalensError> {
        self.inner.get_slot(commitment)
    }

    fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        commitment: SolanaCommitment,
    ) -> Result<Vec<u64>, DatalensError> {
        if *self.fail_data_fetches.lock().expect("fail lock") {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "unexpected Solana provider fetch",
            ));
        }
        *self.data_fetches.lock().expect("fetch lock") += 1;
        self.inner
            .get_blocks_with_limit(start_slot, limit, commitment)
    }

    fn get_block(
        &self,
        slot: u64,
        commitment: SolanaCommitment,
    ) -> Result<Option<SolanaBlock>, DatalensError> {
        if *self.fail_data_fetches.lock().expect("fail lock") {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "unexpected Solana provider fetch",
            ));
        }
        *self.data_fetches.lock().expect("fetch lock") += 1;
        self.inner.get_block(slot, commitment)
    }

    fn provider_name(&self) -> &'static str {
        "counting-solana-fixture"
    }
}

#[derive(Clone, Default)]
pub(crate) struct CountingTronProvider {
    inner: TronFixtureProviderRpc,
    fail_data_fetches: Arc<Mutex<bool>>,
}

impl CountingTronProvider {
    pub(crate) fn fail_data_fetches(&self) {
        *self.fail_data_fetches.lock().expect("fail lock") = true;
    }
}

impl TronProvider for CountingTronProvider {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError> {
        self.inner.latest_block(finality)
    }

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError> {
        if *self.fail_data_fetches.lock().expect("fail lock") {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "unexpected Tron provider fetch",
            ));
        }
        self.inner.get_block_by_number(number, finality)
    }

    fn get_transaction_info_by_id(&self, tx_id: &str) -> Result<Option<Value>, DatalensError> {
        if *self.fail_data_fetches.lock().expect("fail lock") {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "unexpected Tron provider fetch",
            ));
        }
        self.inner.get_transaction_info_by_id(tx_id)
    }

    fn provider_name(&self) -> &'static str {
        "counting-tron-fixture"
    }
}

#[derive(Clone)]
pub(crate) struct MockSource {
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
    pub(crate) fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    pub(crate) fn with_chain(self, chain: ChainIdentity) -> Self {
        *self.chain.lock().expect("chain lock") = chain;
        self
    }

    pub(crate) fn with_error(self, kind: DatalensErrorKind) -> Self {
        *self.error.lock().expect("error lock") = Some(kind);
        self
    }

    pub(crate) fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
    }

    pub(crate) fn clear_calls(&self) {
        self.calls.lock().expect("calls lock").clear();
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
        if request.dataset_key == DatasetKey::evm_blocks() {
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
        } else if request.dataset_key == DatasetKey::evm_logs() {
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
        } else {
            unreachable!("lifecycle fixtures only request blocks or logs")
        }
    }
}
