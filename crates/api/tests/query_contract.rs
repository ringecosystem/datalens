use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_api::config::{
    ChainConfig, DatasetsConfig, LogsDatasetConfig, PlannerConfig, WriterConfig,
};
use datalens_api::{
    LegacyEvmQueryRequest, LegacyEvmQueryResponse, QueryService, api_error_body, api_error_status,
    legacy_evm_to_native_input,
};
use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, LedgerRange, LogFilter, LogRecord, NetworkId, QueryDataFinality,
    QueryFinalityRequirement, QueryRows, QuerySegmentSource,
};
use datalens_planner::{FieldSelection, ResponseShape};
use datalens_storage::LocalStorage;

#[test]
fn test_client_query_request_json_matches_api_request_contract() {
    let request = datalens_client::QueryRequest {
        chain: ethereum_identity(),
        dataset: Dataset::Logs,
        range: BlockRange::expect_new(20, 21),
        filter: Some(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None, Some(vec![TOPIC_A.to_owned()])],
        }),
        include_block: false,
        allow_hot: false,
        finality: QueryFinalityRequirement::DurableOnly,
    };

    let api_request: LegacyEvmQueryRequest =
        serde_json::from_value(serde_json::to_value(request).expect("client request json"))
            .expect("api request json");

    assert_eq!(
        api_request,
        logs_request_with_topics(
            20,
            21,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            vec![None, Some(vec![TOPIC_A])]
        )
    );
}

#[test]
fn test_client_query_response_json_decodes_api_response_contract() {
    let api_response = LegacyEvmQueryResponse {
        chain: ethereum_identity(),
        range: BlockRange::expect_new(10, 10),
        cache: datalens_api::CacheSummary {
            hit_ranges: vec![BlockRange::expect_new(10, 10)],
            missing_ranges: Vec::new(),
            durable_hit_ranges: vec![BlockRange::expect_new(10, 10)],
            hot_hit_ranges: Vec::new(),
            provider_fill_ranges: Vec::new(),
            promotion_pending_ranges: Vec::new(),
            segments: vec![datalens_api::QuerySegment {
                range: BlockRange::expect_new(10, 10),
                source: QuerySegmentSource::Durable,
                finality: QueryDataFinality::Safe,
            }],
        },
        rows: QueryRows::EvmBlocks(vec![block(10, "0x10")]),
    };

    let client_response: datalens_client::QueryResponse =
        serde_json::from_value(serde_json::to_value(api_response).expect("api response json"))
            .expect("client response json");

    assert_eq!(
        client_response.cache.outcome(),
        datalens_client::CacheOutcome::FullHit
    );
    assert_eq!(query_row_block_numbers(&client_response.rows), vec![10]);
}

#[test]
fn test_legacy_blocks_request_maps_to_native_domain_input() {
    let input = legacy_evm_to_native_input(blocks_request(10, 12)).expect("blocks request maps");

    assert_eq!(input.chain, ethereum_identity());
    assert_eq!(input.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(
        input.ledger_range,
        LedgerRange::from_block_range(BlockRange::expect_new(10, 12))
    );
    assert_eq!(input.selector, DatasetSelector::all());
    assert_eq!(input.response_shape, ResponseShape::LegacyEvmBlocks);
    assert_eq!(input.field_selection, FieldSelection::All);
    assert_eq!(input.finality, QueryFinalityRequirement::DurableOnly);
}

#[test]
fn test_legacy_logs_request_maps_to_native_domain_input() {
    let input = legacy_evm_to_native_input(logs_request_with_topics(
        20,
        21,
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        vec![Some(vec![TOPIC_A])],
    ))
    .expect("logs request maps");

    assert_eq!(input.chain, ethereum_identity());
    assert_eq!(input.dataset_key, DatasetKey::evm_logs());
    assert_eq!(
        input.ledger_range,
        LedgerRange::from_block_range(BlockRange::expect_new(20, 21))
    );
    assert!(matches!(input.selector, DatasetSelector::EvmLogs(_)));
    assert_eq!(input.response_shape, ResponseShape::LegacyEvmLogs);
    assert_eq!(input.field_selection, FieldSelection::All);
    assert_eq!(input.finality, QueryFinalityRequirement::DurableOnly);
}

#[test]
fn test_legacy_request_rejects_latest_finality_without_allow_hot() {
    let mut request = blocks_request(100, 101);
    request.finality = QueryFinalityRequirement::SafeToLatest;

    let error = legacy_evm_to_native_input(request).expect_err("allow_hot is required");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("allow_hot"));
}

#[test]
fn test_legacy_request_maps_explicit_hot_contract_to_native_input() {
    let mut request = blocks_request(100, 101);
    request.allow_hot = true;
    request.finality = QueryFinalityRequirement::SafeToLatest;

    let input = legacy_evm_to_native_input(request).expect("hot request maps");

    assert_eq!(input.finality, QueryFinalityRequirement::SafeToLatest);
}

#[test]
fn test_query_service_supports_latest_only_read_through_without_durable_cache_write() {
    let root = temp_storage_root("hot-read-through");
    let source = MockSource::default().with_blocks(vec![block(100, "0x64")]);
    let service = service(LocalStorage::new(&root), source.clone());
    let mut request = blocks_request(100, 100);
    request.allow_hot = true;
    request.finality = QueryFinalityRequirement::LatestOnly;

    let response = service.query(request).expect("hot query succeeds");

    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![BlockRange::expect_new(100, 100)]
    );
    assert_eq!(
        response.cache.missing_ranges,
        vec![BlockRange::expect_new(100, 100)]
    );
    assert_eq!(response.cache.durable_hit_ranges, Vec::<BlockRange>::new());
    assert_eq!(response.cache.hot_hit_ranges, Vec::<BlockRange>::new());
    assert_eq!(response.cache.segments.len(), 1);
    assert_eq!(
        response.cache.segments[0].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(
        response.cache.segments[0].finality,
        QueryDataFinality::Latest
    );
    assert_eq!(query_row_block_numbers(&response.rows), vec![100]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(100, 100))]
    );
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_api_error_mapping_uses_stable_response_codes() {
    let body = api_error_body(DatalensError::new(
        DatalensErrorKind::ProviderLimit,
        "mock provider limit",
    ));

    assert_eq!(
        api_error_status(&DatalensErrorKind::ProviderLimit),
        axum::http::StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(body.error.kind, "provider_limit");
    assert_eq!(body.error.message, "mock provider limit");

    let hot_body = api_error_body(DatalensError::new(
        DatalensErrorKind::UnsupportedHotQuery,
        "adapter cannot safely serve the requested hot/latest contract",
    ));
    assert_eq!(
        api_error_status(&DatalensErrorKind::UnsupportedHotQuery),
        axum::http::StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(hot_body.error.kind, "unsupported_hot_query");
}

#[test]
fn test_query_response_marks_provider_fill_source_and_finality() {
    let source = MockSource::default().with_blocks(vec![block(1, "0x01"), block(2, "0x02")]);
    let service = service(
        LocalStorage::new(temp_storage_root("response-provider-segment")),
        source,
    );

    let response = service.query(blocks_request(1, 2)).expect("query succeeds");

    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![BlockRange::expect_new(1, 2)]
    );
    assert_eq!(response.cache.hot_hit_ranges, Vec::<BlockRange>::new());
    assert_eq!(response.cache.segments.len(), 1);
    assert_eq!(
        response.cache.segments[0].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(response.cache.segments[0].finality, QueryDataFinality::Safe);
}

#[test]
fn test_query_rejects_fetch_response_chain_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-chain",
        ResponseMutation::Chain(
            ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
                .expect("valid chain"),
        ),
    );
}

#[test]
fn test_query_rejects_fetch_response_dataset_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-dataset",
        ResponseMutation::Dataset(DatasetKey::evm_logs()),
    );
}

#[test]
fn test_query_rejects_fetch_response_range_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-range",
        ResponseMutation::Range(LedgerRange::from_block_range(BlockRange::expect_new(2, 3))),
    );
}

#[test]
fn test_query_rejects_fetch_response_selector_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-selector",
        ResponseMutation::Selector(
            DatasetSelector::try_evm_logs(LogFilter {
                addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
                topics: Vec::new(),
            })
            .expect("valid selector"),
        ),
    );
}

#[test]
fn test_query_rejects_fetch_response_rows_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-rows",
        ResponseMutation::Rows(QueryRows::EvmLogs(vec![log(
            1,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        )])),
    );
}

#[test]
fn test_query_uses_adapter_range_limit_when_smaller_than_config() {
    let source = MockSource::default()
        .with_blocks(vec![
            block(1, "0x01"),
            block(2, "0x02"),
            block(3, "0x03"),
            block(4, "0x04"),
        ])
        .with_blocks_max_range_len(1);
    let service = service(
        LocalStorage::new(temp_storage_root("adapter-range-limit")),
        source.clone(),
    );

    let response = service.query(blocks_request(1, 4)).expect("query succeeds");

    assert_eq!(block_numbers(&response), vec![1, 2, 3, 4]);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 1)),
            SourceCall::Blocks(BlockRange::expect_new(2, 2)),
            SourceCall::Blocks(BlockRange::expect_new(3, 3)),
            SourceCall::Blocks(BlockRange::expect_new(4, 4)),
        ]
    );
}

#[test]
fn test_query_uses_adapter_log_range_limit_when_smaller_than_config() {
    let source = MockSource::default().with_logs_max_range_len(1);
    let service = service(
        LocalStorage::new(temp_storage_root("adapter-log-range-limit")),
        source.clone(),
    );

    service
        .query(logs_request(
            1,
            3,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect("query succeeds");

    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Logs(BlockRange::expect_new(1, 1)),
            SourceCall::Logs(BlockRange::expect_new(2, 2)),
            SourceCall::Logs(BlockRange::expect_new(3, 3)),
        ]
    );
}

#[test]
fn test_query_rejects_log_addresses_above_adapter_capability() {
    let source = MockSource::default().with_max_addresses_per_query(1);
    let service = service(
        LocalStorage::new(temp_storage_root("adapter-address-limit")),
        source,
    );

    let error = service
        .query(logs_request(
            1,
            1,
            vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ],
        ))
        .expect_err("too many addresses for adapter");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
}

#[test]
fn test_query_delegates_log_address_limit_to_planner_capabilities() {
    let source = MockSource::default().with_max_addresses_per_query(2);
    let service = QueryService::new(
        LocalStorage::new(temp_storage_root("planner-address-limit")),
        source.clone(),
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
        chain_config(1),
    );

    service
        .query(logs_request(
            1,
            1,
            vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ],
        ))
        .expect("planner capability allows two addresses");

    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(1, 1))]
    );
}

fn assert_contract_violation_not_cached(name: &str, mutation: ResponseMutation) {
    let root = temp_storage_root(name);
    let source = MockSource::default()
        .with_blocks(vec![block(1, "0x01"), block(2, "0x02")])
        .with_response_mutation(mutation);
    let service = service(LocalStorage::new(&root), source);

    let error = service
        .query(blocks_request(1, 2))
        .expect_err("contract violation");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
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

fn chain_config(max_addresses_per_query: usize) -> ChainConfig {
    ChainConfig {
        kind: "evm".to_owned(),
        chain_id: 1,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        finality: datalens_api::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: datalens_api::config::BlocksDatasetConfig {
                enabled: true,
                max_batch_blocks: 2,
            },
            logs: LogsDatasetConfig {
                enabled: true,
                max_get_logs_range_blocks: 2,
                max_addresses_per_query,
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

fn logs_request(from_block: u64, to_block: u64, addresses: Vec<&str>) -> LegacyEvmQueryRequest {
    logs_request_with_topics(
        from_block,
        to_block,
        addresses,
        vec![None, None, None, None],
    )
}

fn logs_request_with_topics(
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
    topics: Vec<Option<Vec<&str>>>,
) -> LegacyEvmQueryRequest {
    LegacyEvmQueryRequest {
        chain: ethereum_identity(),
        dataset: Dataset::Logs,
        range: BlockRange::expect_new(from_block, to_block),
        filter: Some(LogFilter {
            addresses: addresses.into_iter().map(str::to_owned).collect(),
            topics: topics
                .into_iter()
                .map(|topic| topic.map(|values| values.into_iter().map(str::to_owned).collect()))
                .collect(),
        }),
        include_block: false,
        allow_hot: false,
        finality: QueryFinalityRequirement::DurableOnly,
    }
}

const TOPIC_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number * 10,
    }
}

fn log(block_number: u64, log_index: u64, address: &str, topics: Vec<&str>) -> LogRecord {
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

fn temp_storage_root(name: &str) -> PathBuf {
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

fn block_numbers(response: &LegacyEvmQueryResponse) -> Vec<u64> {
    query_row_block_numbers(&response.rows)
}

fn query_row_block_numbers(rows: &QueryRows) -> Vec<u64> {
    match rows {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected blocks"),
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
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    fn with_response_mutation(self, mutation: ResponseMutation) -> Self {
        *self
            .response_mutation
            .lock()
            .expect("response mutation lock") = Some(mutation);
        self
    }

    fn with_blocks_max_range_len(self, max_range_len: u64) -> Self {
        *self
            .blocks_max_range_len
            .lock()
            .expect("blocks max range len lock") = max_range_len;
        self
    }

    fn with_logs_max_range_len(self, max_range_len: u64) -> Self {
        *self
            .logs_max_range_len
            .lock()
            .expect("logs max range len lock") = max_range_len;
        self
    }

    fn with_max_addresses_per_query(self, max_addresses_per_query: usize) -> Self {
        *self
            .max_addresses_per_query
            .lock()
            .expect("max addresses per query lock") = max_addresses_per_query;
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResponseMutation {
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
        let response = match request
            .dataset_key
            .legacy_dataset()
            .expect("legacy dataset")
        {
            Dataset::Blocks => self.fetch_blocks(range).and_then(|rows| {
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
            }),
            Dataset::Transactions | Dataset::Receipts => {
                unreachable!("legacy query contract fixtures only request blocks or logs")
            }
            Dataset::Logs => {
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
                    .with_provider_diagnostics(
                        datalens_chain::ProviderDiagnostics {
                            calls: 1,
                            rows_scanned: 0,
                            warnings: Vec::new(),
                        },
                    ))
                })
            }
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
