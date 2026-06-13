#![allow(dead_code, unused_imports)]

pub(crate) use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, SelectorKind,
};
pub(crate) use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, LedgerRange, LogFilter, LogRecord, NetworkId, QueryDataFinality,
    QueryFinalityRequirement, QueryRows, QuerySegmentSource,
};
pub(crate) use datalens_edge::config::{
    ChainConfig, DatasetsConfig, LogsDatasetConfig, PlannerConfig, QueryDurableIntentConfig,
    QueryMetadataConfig, WriterConfig,
};
pub(crate) use datalens_edge::{
    FieldSelectionApi, NativeQueryResponse, QueryApiRequest, QueryApiResponse, QueryRangeApi,
    QuerySelectorApi, QueryService, api_error_body, api_error_status,
};
pub(crate) use datalens_planner::{FieldSelection, NativeQueryInput};
pub(crate) use datalens_storage::{
    CreateDurablePromotionIntent, DurablePromotionIntent, DurablePromotionIntentCreateOutcome,
    DurablePromotionIntentRepository, LocalStorage,
};

pub(crate) fn assert_contract_violation_not_cached(name: &str, mutation: ResponseMutation) {
    let root = temp_storage_root(name);
    let source = MockSource::default()
        .with_blocks(vec![block(1, "0x01"), block(2, "0x02")])
        .with_response_mutation(mutation);
    let service = service(LocalStorage::new(&root), source);

    let error = service
        .query_native(blocks_request(1, 2))
        .expect_err("contract violation");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

pub(crate) fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
    QueryService::new(
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
        chain_config(2),
    )
}

pub(crate) fn service_with_query_durable_intents_config(
    storage: LocalStorage,
    source: MockSource,
    durable_intents_config: QueryDurableIntentConfig,
    repository: impl DurablePromotionIntentRepository + 'static,
) -> QueryService<MockSource> {
    QueryService::new_with_query_worker_config(
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
        "ethereum",
        chain_config(2),
        Default::default(),
        QueryMetadataConfig::default(),
        durable_intents_config,
    )
    .expect("query service")
    .with_durable_intents_configured(
        repository,
        durable_intents_config,
        Arc::new(std::sync::Once::new()),
    )
}

pub(crate) fn chain_config(max_addresses_per_query: usize) -> ChainConfig {
    ChainConfig {
        kind: "evm".to_owned(),
        chain_id: 1,
        rpc_url: None,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        rpc: None,
        warmup: Default::default(),
        trongrid: Default::default(),
        finality: datalens_edge::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: datalens_edge::config::BlocksDatasetConfig {
                enabled: true,
                max_batch_blocks: 2,
            },
            logs: LogsDatasetConfig {
                enabled: true,
                reliability_enabled: true,
                receipt_fallback_enabled: true,
                query_strategy: Default::default(),
                max_get_logs_range_blocks: 2,
                max_block_scan_range_blocks: 2,
                max_addresses_per_query,
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

pub(crate) fn logs_request(
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
) -> NativeQueryInput {
    logs_request_with_topics(
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
    NativeQueryInput {
        chain: ethereum_identity(),
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
pub(crate) fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
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
        parent_hash: None,
        block_timestamp: None,
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
    query_row_block_numbers(response.rows.rows())
}

pub(crate) fn query_row_block_numbers(rows: &QueryRows) -> Vec<u64> {
    match rows {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected blocks"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone)]
pub(crate) struct MockSource {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    logs: Arc<Mutex<Vec<LogRecord>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    error: Arc<Mutex<Option<DatalensErrorKind>>>,
    response_mutation: Arc<Mutex<Option<ResponseMutation>>>,
    blocks_max_range_len: Arc<Mutex<u64>>,
    logs_max_range_len: Arc<Mutex<u64>>,
    max_addresses_per_query: Arc<Mutex<usize>>,
    safe_height: Arc<Mutex<ChainHeight>>,
}

impl Default for MockSource {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            error: Arc::new(Mutex::new(None)),
            response_mutation: Arc::new(Mutex::new(None)),
            blocks_max_range_len: Arc::new(Mutex::new(2)),
            logs_max_range_len: Arc::new(Mutex::new(2)),
            max_addresses_per_query: Arc::new(Mutex::new(2)),
            safe_height: Arc::new(Mutex::new(
                ChainHeight::block(100).with_finality(FinalityKind::Safe),
            )),
        }
    }
}

impl MockSource {
    pub(crate) fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    pub(crate) fn with_response_mutation(self, mutation: ResponseMutation) -> Self {
        *self
            .response_mutation
            .lock()
            .expect("response mutation lock") = Some(mutation);
        self
    }

    pub(crate) fn with_blocks_max_range_len(self, max_range_len: u64) -> Self {
        *self
            .blocks_max_range_len
            .lock()
            .expect("blocks max range len lock") = max_range_len;
        self
    }

    pub(crate) fn with_logs_max_range_len(self, max_range_len: u64) -> Self {
        *self
            .logs_max_range_len
            .lock()
            .expect("logs max range len lock") = max_range_len;
        self
    }

    pub(crate) fn with_max_addresses_per_query(self, max_addresses_per_query: usize) -> Self {
        *self
            .max_addresses_per_query
            .lock()
            .expect("max addresses per query lock") = max_addresses_per_query;
        self
    }

    pub(crate) fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
    }
}

#[derive(Clone, Default)]
pub(crate) struct RecordingIntentRepository {
    recorded: Arc<Mutex<Vec<CreateDurablePromotionIntent>>>,
}

impl RecordingIntentRepository {
    pub(crate) fn recorded(&self) -> Arc<Mutex<Vec<CreateDurablePromotionIntent>>> {
        self.recorded.clone()
    }
}

impl DurablePromotionIntentRepository for RecordingIntentRepository {
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        self.recorded
            .lock()
            .expect("recorded intent lock")
            .push(request);
        panic!("durable intent should not be submitted")
    }

    fn get(&self, _intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn list_pending(
        &self,
        _now_unix_seconds: u64,
        _limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        Ok(Vec::new())
    }

    fn list_pending_for_chain(
        &self,
        _chain: &ChainIdentity,
        _now_unix_seconds: u64,
        _limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        Ok(Vec::new())
    }

    fn mark_running(
        &self,
        _intent_id: &str,
        _now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn mark_completed(
        &self,
        _intent_id: &str,
        _now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn mark_retryable_failure(
        &self,
        _intent_id: &str,
        _error: &str,
        _now_unix_seconds: u64,
        _next_retry_at_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn mark_terminal_failure(
        &self,
        _intent_id: &str,
        _error: &str,
        _now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        Ok(None)
    }

    fn reset_stale_running(
        &self,
        _stale_before_unix_seconds: u64,
        _now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResponseMutation {
    Chain(ChainIdentity),
    Dataset(DatasetKey),
    Range(LedgerRange),
    Selector(DatasetSelector),
    Rows(QueryRows),
}

impl ChainAdapter for MockSource {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(ethereum_identity())
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
            )
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
            panic!("query contract fixtures only request blocks or logs")
        }?;

        Ok(self.mutate_response(response))
    }
}

impl MockSource {
    fn mutate_response(&self, mut response: ChainFetchResponse) -> ChainFetchResponse {
        match self
            .response_mutation
            .lock()
            .expect("response mutation lock")
            .clone()
        {
            Some(ResponseMutation::Chain(chain)) => response.chain = chain,
            Some(ResponseMutation::Dataset(dataset)) => response.dataset_key = dataset,
            Some(ResponseMutation::Range(range)) => response.range = range,
            Some(ResponseMutation::Selector(selector)) => response.coverage_selector = selector,
            Some(ResponseMutation::Rows(rows)) => {
                response.rows = datalens_core::DatasetRows::new(rows.dataset_key(), rows)
                    .expect("matching rows")
            }
            None => {}
        }
        response
    }

    fn fetch_blocks(&self, range: BlockRange) -> Result<Vec<BlockHeader>, DatalensError> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(SourceCall::Blocks(range));
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
