#![allow(dead_code, unused_imports)]

pub(crate) use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
pub(crate) use datalens_chain::{
    AdapterCapabilities, AdapterKey, ChainAdapter, ChainFetchRequest, ChainFetchResponse,
    ChainHeight, DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, SelectorKind,
};
pub(crate) use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, LedgerRange, LogFilter, LogRecord, NetworkId, QueryFinalityRequirement, QueryRows,
    TopicFilter,
};
pub(crate) use datalens_edge::config::{
    ApplicationConfig, ApplicationOperationConfig, ApplicationQuotaConfig,
    ApplicationRegistryConfig, ChainConfig, DatasetsConfig, LogsDatasetConfig, MetricsConfig,
    PlannerConfig, WriterConfig,
};
pub(crate) use datalens_edge::{NativeQueryResponse, QueryService, QueryServiceRegistry, router};
pub(crate) use datalens_planner::{FieldSelection, NativeQueryInput};
pub(crate) use datalens_solana::{SolanaAdapter, solana_all_selector};
pub(crate) use datalens_storage::{LocalStorage, StorageRepository};
pub(crate) use datalens_tron::TronAdapter;
pub(crate) use tower::ServiceExt;

pub(crate) fn application_registry(
    applications: Vec<ApplicationConfig>,
) -> ApplicationRegistryConfig {
    ApplicationRegistryConfig {
        required: true,
        applications,
    }
}

pub(crate) fn application(
    id: &str,
    enabled: bool,
    token: &str,
    chains: Vec<&str>,
    datasets: Vec<&str>,
    quota: Option<ApplicationQuotaConfig>,
) -> ApplicationConfig {
    ApplicationConfig {
        id: id.to_owned(),
        name: id.to_owned(),
        enabled,
        display_name: None,
        token: token.to_owned(),
        chains: chains.into_iter().map(str::to_owned).collect(),
        datasets: datasets.into_iter().map(str::to_owned).collect(),
        operations: vec![
            ApplicationOperationConfig::Query,
            ApplicationOperationConfig::Discovery,
            ApplicationOperationConfig::WarmupSubmit,
            ApplicationOperationConfig::WarmupRead,
            ApplicationOperationConfig::WarmupMutate,
            ApplicationOperationConfig::WarmupRun,
        ],
        quota,
    }
}

pub(crate) fn query_http_request(
    request: NativeQueryInput,
    application: Option<&str>,
    authorization: Option<&str>,
) -> Request<Body> {
    let selector = match request.selector {
        DatasetSelector::All => serde_json::json!({ "kind": "all" }),
        DatasetSelector::EvmLogs(filter) => serde_json::json!({
            "kind": "evm_logs",
            "value": evm_log_filter_value(&filter)
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
    let range = match request.ledger_range.kind() {
        datalens_core::LedgerRangeKind::Block => serde_json::json!({
            "kind": "block",
            "start": request.ledger_range.start(),
            "end": request.ledger_range.end()
        }),
        datalens_core::LedgerRangeKind::Slot => serde_json::json!({
            "kind": "slot",
            "start": request.ledger_range.start(),
            "end": request.ledger_range.end()
        }),
        datalens_core::LedgerRangeKind::Height => serde_json::json!({
            "kind": "height",
            "start": request.ledger_range.start(),
            "end": request.ledger_range.end()
        }),
        datalens_core::LedgerRangeKind::Other(kind) => {
            panic!("query API test helper does not support {kind} ranges")
        }
    };
    let body = serde_json::json!({
        "chain": request.chain,
        "dataset_key": request.dataset_key.as_str(),
        "selector": selector,
        "range": range,
        "finality": request.finality,
        "fields": "all"
    });
    let mut builder = Request::builder()
        .method(axum::http::Method::POST)
        .uri("/v1/query")
        .header(axum::http::header::CONTENT_TYPE, "application/json");
    if let Some(application) = application {
        builder = builder.header("x-datalens-application", application);
    }
    if let Some(authorization) = authorization {
        builder = builder.header(axum::http::header::AUTHORIZATION, authorization);
    }
    builder
        .body(Body::from(serde_json::to_vec(&body).expect("request body")))
        .expect("query request")
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

pub(crate) fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
    service_named(storage, source, "ethereum", 1)
}

pub(crate) fn service_named(
    storage: impl StorageRepository + 'static,
    source: MockSource,
    chain_name: &str,
    chain_id: u64,
) -> QueryService<MockSource> {
    service_named_with_datasets(storage, source, chain_name, chain_id, true, true)
}

pub(crate) fn service_named_with_datasets(
    storage: impl StorageRepository + 'static,
    source: MockSource,
    chain_name: &str,
    chain_id: u64,
    blocks_enabled: bool,
    logs_enabled: bool,
) -> QueryService<MockSource> {
    QueryService::new_named(
        storage,
        source,
        PlannerConfig {
            max_query_range_blocks: 4,
            default_chunk_range_blocks: 2,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        chain_name,
        ChainConfig {
            kind: "evm".to_owned(),
            chain_id,
            rpc_urls: vec!["http://example.invalid".to_owned()],
            trongrid: Default::default(),
            finality: datalens_edge::config::FinalityConfig::Auto,
            datasets: DatasetsConfig {
                blocks: datalens_edge::config::BlocksDatasetConfig {
                    enabled: blocks_enabled,
                    max_batch_blocks: 2,
                },
                logs: LogsDatasetConfig {
                    enabled: logs_enabled,
                    max_get_logs_range_blocks: 2,
                    max_addresses_per_query: 2,
                },
            },
        },
    )
}

pub(crate) fn chain_config(chain_id: u64) -> ChainConfig {
    ChainConfig {
        kind: "evm".to_owned(),
        chain_id,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        trongrid: Default::default(),
        finality: datalens_edge::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: datalens_edge::config::BlocksDatasetConfig {
                enabled: true,
                max_batch_blocks: 2,
            },
            logs: LogsDatasetConfig {
                enabled: true,
                max_get_logs_range_blocks: 2,
                max_addresses_per_query: 2,
            },
        },
    }
}

pub(crate) fn planner_config() -> PlannerConfig {
    PlannerConfig {
        max_query_range_blocks: 4,
        default_chunk_range_blocks: 2,
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

pub(crate) fn solana_chain_config() -> ChainConfig {
    ChainConfig {
        kind: "solana".to_owned(),
        chain_id: 0,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        trongrid: Default::default(),
        finality: datalens_edge::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: datalens_edge::config::BlocksDatasetConfig {
                enabled: false,
                max_batch_blocks: 2,
            },
            logs: LogsDatasetConfig {
                enabled: false,
                max_get_logs_range_blocks: 2,
                max_addresses_per_query: 2,
            },
        },
    }
}

pub(crate) fn blocks_request(from_block: u64, to_block: u64) -> NativeQueryInput {
    blocks_request_for(ethereum_identity(), from_block, to_block)
}

pub(crate) fn blocks_request_for(
    chain: ChainIdentity,
    from_block: u64,
    to_block: u64,
) -> NativeQueryInput {
    NativeQueryInput {
        chain,
        dataset_key: DatasetKey::evm_blocks(),
        ledger_range: LedgerRange::blocks(from_block, to_block).expect("valid range"),
        selector: DatasetSelector::all(),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

pub(crate) fn logs_request(
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
) -> NativeQueryInput {
    logs_request_for(ethereum_identity(), from_block, to_block, addresses)
}

pub(crate) fn logs_request_for(
    chain: ChainIdentity,
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
) -> NativeQueryInput {
    logs_request_with_topics_for(
        chain,
        from_block,
        to_block,
        addresses,
        vec![None, None, None, None],
    )
}

pub(crate) fn logs_request_with_topics(
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
    topics: Vec<Option<Vec<&str>>>,
) -> NativeQueryInput {
    logs_request_with_topics_for(ethereum_identity(), from_block, to_block, addresses, topics)
}

pub(crate) fn logs_request_with_topics_for(
    chain: ChainIdentity,
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
    topics: Vec<Option<Vec<&str>>>,
) -> NativeQueryInput {
    NativeQueryInput {
        chain,
        dataset_key: DatasetKey::evm_logs(),
        ledger_range: LedgerRange::blocks(from_block, to_block).expect("valid range"),
        selector: DatasetSelector::try_evm_logs(LogFilter {
            addresses: addresses.into_iter().map(str::to_owned).collect(),
            topics: topics
                .into_iter()
                .map(|topic| topic.map(|values| values.into_iter().map(str::to_owned).collect()))
                .collect(),
        })
        .expect("valid logs selector"),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

pub(crate) const TOPIC_A: &str =
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub(crate) const TOPIC_B: &str =
    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

pub(crate) fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

pub(crate) fn polygon_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
        .expect("valid chain identity")
}

pub(crate) fn solana_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(NetworkId::textual("mainnet-beta").expect("valid network")),
    )
    .expect("valid chain identity")
}

pub(crate) fn tron_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("tron".to_owned()),
        "tron",
        Some(NetworkId::numeric(1)),
    )
    .expect("valid chain identity")
}

pub(crate) fn tron_mainnet_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("tron".to_owned()),
        "tron-mainnet",
        Some(NetworkId::textual("mainnet").expect("valid network")),
    )
    .expect("valid chain identity")
}

pub(crate) fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number * 10,
    }
}

pub(crate) fn log(
    block_number: u64,
    log_index: u64,
    address: &str,
    topics: Vec<&str>,
) -> LogRecord {
    LogRecord {
        block_number,
        block_hash: format!("0xblock-{block_number}"),
        transaction_hash: format!("0xtx-{block_number}-{log_index}"),
        transaction_index: 0,
        log_index,
        address: address.to_owned(),
        topics: topics.into_iter().map(str::to_owned).collect(),
        data: "0x".to_owned(),
        removed: false,
    }
}

pub(crate) fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

pub(crate) fn block_numbers(response: &NativeQueryResponse) -> Vec<u64> {
    match response.rows.rows() {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected blocks"),
    }
}

pub(crate) fn log_indexes(response: &NativeQueryResponse) -> Vec<u64> {
    match response.rows.rows() {
        QueryRows::EvmLogs(rows) => rows.iter().map(|row| row.log_index).collect(),
        _ => panic!("expected logs"),
    }
}

pub(crate) fn log_addresses(response: &NativeQueryResponse) -> Vec<String> {
    match response.rows.rows() {
        QueryRows::EvmLogs(rows) => rows.iter().map(|row| row.address.clone()).collect(),
        _ => panic!("expected logs"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
    Native(DatasetKey, LedgerRange),
}

#[derive(Clone)]
pub(crate) struct MockSource {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    logs: Arc<Mutex<Vec<LogRecord>>>,
    native_rows: Arc<Mutex<Vec<serde_json::Value>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    error: Arc<Mutex<Option<DatalensErrorKind>>>,
    blocks_max_range_len: Arc<Mutex<u64>>,
    logs_max_range_len: Arc<Mutex<u64>>,
    max_addresses_per_query: Arc<Mutex<usize>>,
    safe_height: Arc<Mutex<ChainHeight>>,
    chain: Arc<Mutex<ChainIdentity>>,
    fetch_delay: Arc<Mutex<Option<Duration>>>,
}

impl Default for MockSource {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
            native_rows: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            error: Arc::new(Mutex::new(None)),
            blocks_max_range_len: Arc::new(Mutex::new(2)),
            logs_max_range_len: Arc::new(Mutex::new(2)),
            max_addresses_per_query: Arc::new(Mutex::new(2)),
            safe_height: Arc::new(Mutex::new(
                ChainHeight::block(100).with_finality(FinalityKind::Safe),
            )),
            chain: Arc::new(Mutex::new(ethereum_identity())),
            fetch_delay: Arc::new(Mutex::new(None)),
        }
    }
}

impl MockSource {
    pub(crate) fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    pub(crate) fn with_logs(self, logs: Vec<LogRecord>) -> Self {
        *self.logs.lock().expect("logs lock") = logs;
        self
    }

    pub(crate) fn with_native_rows(self, rows: Vec<serde_json::Value>) -> Self {
        *self.native_rows.lock().expect("native rows lock") = rows;
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

    pub(crate) fn with_safe_height(self, value: u64, finality: FinalityKind) -> Self {
        *self.safe_height.lock().expect("safe height lock") =
            ChainHeight::block(value).with_finality(finality);
        self
    }

    pub(crate) fn with_fetch_delay(self, delay: Duration) -> Self {
        *self.fetch_delay.lock().expect("fetch delay lock") = Some(delay);
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
        let chain = self.chain.lock().expect("chain lock").clone();
        let capabilities = AdapterCapabilities::new(chain.clone())
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Blocks)
                    .with_selector(SelectorKind::All)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(
                        *self
                            .blocks_max_range_len
                            .lock()
                            .expect("blocks max range len lock"),
                    )
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Logs)
                    .with_selector(SelectorKind::EvmLogs)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(
                        *self
                            .logs_max_range_len
                            .lock()
                            .expect("logs max range len lock"),
                    )
                    .with_max_addresses_per_query(
                        *self
                            .max_addresses_per_query
                            .lock()
                            .expect("max addresses per query lock"),
                    )
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            );
        if chain.family() == ChainFamily::Evm {
            capabilities
        } else {
            capabilities.with_dataset_capability(
                DatasetCapability::new(DatasetKey::tron_events())
                    .with_selector(SelectorKind::Other(
                        AdapterKey::try_new("tron-events").expect("valid selector kind"),
                    ))
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(2)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            )
        }
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(self.safe_height.lock().expect("safe height lock").clone())
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = request.range.block_range().expect("expected block range");
        let response = if request.dataset_key == DatasetKey::evm_blocks() {
            self.fetch_blocks(range).and_then(|rows| {
                Ok(ChainFetchResponse::try_new(
                    request.chain,
                    DatasetKey::evm_blocks(),
                    LedgerRange::from_block_range(range),
                    request.selector,
                    QueryRows::EvmBlocks(rows),
                )?
                .with_provider_diagnostics(datalens_chain::ProviderDiagnostics {
                    calls: range.len().min(usize::MAX as u128) as usize,
                    rows_scanned: 0,
                    warnings: Vec::new(),
                }))
            })
        } else if request.dataset_key == DatasetKey::evm_logs() {
            let filter = match &request.selector {
                DatasetSelector::EvmLogs(filter) => filter,
                selector => panic!("expected EVM logs selector, got {selector:?}"),
            };
            self.fetch_logs(range, filter).and_then(|rows| {
                Ok(ChainFetchResponse::try_new(
                    request.chain,
                    DatasetKey::evm_logs(),
                    LedgerRange::from_block_range(range),
                    request.selector,
                    QueryRows::EvmLogs(rows),
                )?
                .with_provider_diagnostics(datalens_chain::ProviderDiagnostics {
                    calls: 1,
                    rows_scanned: 0,
                    warnings: Vec::new(),
                }))
            })
        } else {
            self.calls
                .lock()
                .expect("calls lock")
                .push(SourceCall::Native(
                    request.dataset_key.clone(),
                    request.range.clone(),
                ));
            Ok(ChainFetchResponse::try_new(
                request.chain,
                request.dataset_key.clone(),
                request.range,
                request.selector,
                QueryRows::AdapterJson {
                    dataset_key: request.dataset_key,
                    rows: self.native_rows.lock().expect("native rows lock").clone(),
                },
            )?
            .with_provider_diagnostics(datalens_chain::ProviderDiagnostics {
                calls: 1,
                rows_scanned: 0,
                warnings: Vec::new(),
            }))
        }?;

        Ok(response)
    }
}

impl MockSource {
    fn fetch_blocks(&self, range: BlockRange) -> Result<Vec<BlockHeader>, DatalensError> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(SourceCall::Blocks(range));
        if let Some(delay) = *self.fetch_delay.lock().expect("fetch delay lock") {
            std::thread::sleep(delay);
        }
        if let Some(kind) = self.error.lock().expect("error lock").clone() {
            return Err(DatalensError::new(kind, "mock provider error"));
        }
        Ok(self
            .blocks
            .lock()
            .expect("blocks lock")
            .iter()
            .filter(|block| range.contains(block.number))
            .cloned()
            .collect())
    }

    fn fetch_logs(
        &self,
        range: BlockRange,
        filter: &datalens_core::EvmLogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(SourceCall::Logs(range));
        if let Some(delay) = *self.fetch_delay.lock().expect("fetch delay lock") {
            std::thread::sleep(delay);
        }
        if let Some(kind) = self.error.lock().expect("error lock").clone() {
            return Err(DatalensError::new(kind, "mock provider error"));
        }
        Ok(self
            .logs
            .lock()
            .expect("logs lock")
            .iter()
            .filter(|log| range.contains(log.block_number))
            .filter(|log| log_matches_filter(log, filter))
            .cloned()
            .collect())
    }
}

fn log_matches_filter(log: &LogRecord, filter: &datalens_core::EvmLogFilter) -> bool {
    if !filter.addresses().is_empty()
        && !filter
            .addresses()
            .iter()
            .any(|address| address == &log.address)
    {
        return false;
    }

    filter
        .topics()
        .iter()
        .enumerate()
        .all(|(index, expected)| match expected {
            datalens_core::TopicFilter::AnyOf(values) => log
                .topics
                .get(index)
                .is_some_and(|topic| values.iter().any(|value| value == topic)),
            datalens_core::TopicFilter::Wildcard => true,
        })
}
