use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use datalens_api::config::{
    ChainConfig, DatasetsConfig, LogsDatasetConfig, PlannerConfig, WriterConfig,
};
use datalens_api::{
    LegacyEvmQueryRequest, LegacyEvmQueryResponse, QueryService, QueryServiceRegistry, router,
};
use datalens_chain::{
    AdapterCapabilities, AdapterKey, ChainAdapter, ChainFetchRequest, ChainFetchResponse,
    ChainHeight, DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, LedgerRange, LogFilter, LogRecord, NetworkId, QueryRows,
};
use datalens_planner::{FieldSelection, NativeQueryInput, ResponseShape};
use datalens_storage::{LocalStorage, StorageRepository};
use tower::ServiceExt;

#[test]
fn test_query_blocks_miss_persists_then_equivalent_hit_uses_cache() {
    let storage = LocalStorage::new(temp_storage_root("blocks-miss-hit"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let service = service(storage, source.clone());
    let request = blocks_request(10, 11);

    let first = service
        .query(request.clone())
        .expect("first query succeeds");
    let second = service.query(request).expect("second query succeeds");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(10, 11)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(10, 11)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 11))]
    );
    assert_eq!(block_numbers(&second), vec![10, 11]);
}

#[test]
fn test_query_blocks_partial_hit_fetches_only_missing_range() {
    let storage = LocalStorage::new(temp_storage_root("blocks-partial"));
    let source = MockSource::default().with_blocks(vec![
        block(1, "0x01"),
        block(2, "0x02"),
        block(3, "0x03"),
        block(4, "0x04"),
    ]);
    let service = service(storage, source.clone());

    service.query(blocks_request(1, 2)).expect("seed cache");
    source.clear_calls();
    let response = service.query(blocks_request(1, 4)).expect("partial query");

    assert_eq!(
        response.cache.hit_ranges,
        vec![BlockRange::expect_new(1, 2)]
    );
    assert_eq!(
        response.cache.missing_ranges,
        vec![BlockRange::expect_new(3, 4)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(3, 4))]
    );
    assert_eq!(block_numbers(&response), vec![1, 2, 3, 4]);
}

#[test]
fn test_query_empty_logs_records_empty_coverage_without_data_object() {
    let storage = LocalStorage::new(temp_storage_root("logs-empty"));
    let root = storage.root().to_path_buf();
    let source = MockSource::default();
    let service = service(storage, source.clone());
    let request = logs_request(50, 52, vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]);

    let first = service
        .query(request.clone())
        .expect("empty log query succeeds");
    let second = service.query(request).expect("empty log query hits cache");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(50, 52)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(50, 52)]
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Logs(BlockRange::expect_new(50, 51)),
            SourceCall::Logs(BlockRange::expect_new(52, 52)),
        ]
    );
    assert_eq!(log_indexes(&second), Vec::<u64>::new());
    assert!(root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_logs_miss_persists_then_equivalent_hit_uses_cache() {
    let storage = LocalStorage::new(temp_storage_root("logs-miss-hit"));
    let source = MockSource::default().with_logs(vec![
        log(
            20,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        ),
        log(
            20,
            1,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            vec![TOPIC_A],
        ),
        log(
            21,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_B],
        ),
    ]);
    let service = service(storage, source.clone());
    let request = logs_request_with_topics(
        20,
        21,
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        vec![Some(vec![TOPIC_A])],
    );

    let first = service
        .query(request.clone())
        .expect("first log query succeeds");
    let second = service.query(request).expect("second log query succeeds");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(20, 21)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(20, 21)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(20, 21))]
    );
    assert_eq!(
        log_addresses(&second),
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    );
    assert_eq!(log_indexes(&second), vec![0]);
}

#[test]
fn test_query_range_limit_rejection_returns_invalid_input() {
    let service = service(
        LocalStorage::new(temp_storage_root("range-limit")),
        MockSource::default(),
    );
    let error = service
        .query(blocks_request(1, 5))
        .expect_err("range is too large");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
}

#[test]
fn test_query_rejects_range_above_safe_height_without_fetch_or_cache_write() {
    let root = temp_storage_root("unsafe-range");
    let source = MockSource::default()
        .with_blocks(vec![block(99, "0x63"), block(100, "0x64")])
        .with_safe_height(99, FinalityKind::Safe);
    let service = service(LocalStorage::new(&root), source.clone());

    let error = service
        .query(blocks_request(99, 100))
        .expect_err("unsafe range is rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("safe/finalized height"));
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_allows_range_at_safe_height_and_writes_cache() {
    let root = temp_storage_root("safe-range");
    let source = MockSource::default()
        .with_blocks(vec![block(98, "0x62"), block(99, "0x63")])
        .with_safe_height(99, FinalityKind::Safe);
    let service = service(LocalStorage::new(&root), source.clone());

    let response = service
        .query(blocks_request(98, 99))
        .expect("safe range succeeds");

    assert_eq!(block_numbers(&response), vec![98, 99]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(98, 99))]
    );
    assert!(root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_rejects_empty_unsafe_range_without_empty_coverage() {
    let root = temp_storage_root("unsafe-empty-coverage");
    let source = MockSource::default().with_safe_height(49, FinalityKind::Safe);
    let service = service(LocalStorage::new(&root), source.clone());

    let error = service
        .query(logs_request(
            50,
            50,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect_err("unsafe empty range is rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_accepts_finalized_adapter_height_without_evm_specific_assumption() {
    let root = temp_storage_root("finalized-range");
    let source = MockSource::default()
        .with_blocks(vec![block(7, "0x07")])
        .with_safe_height(7, FinalityKind::Finalized);
    let service = service(LocalStorage::new(&root), source.clone());

    let response = service
        .query(blocks_request(7, 7))
        .expect("finalized range succeeds");

    assert_eq!(block_numbers(&response), vec![7]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(7, 7))]
    );
}

#[test]
fn test_registry_routes_legacy_query_to_requested_chain_service() {
    let root = temp_storage_root("registry-route");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let ethereum_source = MockSource::default()
        .with_blocks(vec![block(10, "0x0a")])
        .with_chain(ethereum_identity());
    let polygon_source = MockSource::default()
        .with_blocks(vec![block(20, "0x14")])
        .with_chain(polygon_identity());
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            storage.clone(),
            ethereum_source.clone(),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            storage.clone(),
            polygon_source.clone(),
            "polygon",
            137,
        ))
        .expect("register polygon");

    let ethereum = registry
        .query(blocks_request_for(ethereum_identity(), 10, 10))
        .expect("ethereum query succeeds");
    let polygon = registry
        .query(blocks_request_for(polygon_identity(), 20, 20))
        .expect("polygon query succeeds");

    assert_eq!(block_numbers(&ethereum), vec![10]);
    assert_eq!(block_numbers(&polygon), vec![20]);
    assert_eq!(
        ethereum_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
    assert_eq!(
        polygon_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(20, 20))]
    );
}

#[test]
fn test_registry_lists_only_registered_chains() {
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("registry-list-ethereum")),
            MockSource::default().with_chain(ethereum_identity()),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("registry-list-polygon")),
            MockSource::default().with_chain(polygon_identity()),
            "polygon",
            137,
        ))
        .expect("register polygon");

    assert_eq!(
        registry.chain_names(),
        vec!["ethereum".to_owned(), "polygon".to_owned()]
    );
}

#[tokio::test]
async fn test_chains_route_lists_registered_chains() {
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("chains-route-ethereum")),
            MockSource::default().with_chain(ethereum_identity()),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("chains-route-polygon")),
            MockSource::default().with_chain(polygon_identity()),
            "polygon",
            137,
        ))
        .expect("register polygon");

    let response = router(registry)
        .oneshot(
            Request::builder()
                .uri("/v1/chains")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    assert_eq!(
        value,
        serde_json::json!({ "chains": ["ethereum", "polygon"] })
    );
}

#[test]
fn test_registry_rejects_unknown_chain_without_falling_back_to_first_service() {
    let ethereum_source = MockSource::default()
        .with_blocks(vec![block(10, "0x0a")])
        .with_chain(ethereum_identity());
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("registry-unknown")),
            ethereum_source.clone(),
            "ethereum",
            1,
        ))
        .expect("register ethereum");

    let error = registry
        .query(blocks_request_for(polygon_identity(), 10, 10))
        .expect_err("polygon is not registered");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
    assert!(error.message.contains("polygon"));
    assert_eq!(ethereum_source.calls(), Vec::<SourceCall>::new());
}

#[test]
fn test_registry_dataset_disabled_is_scoped_to_target_chain() {
    let root = temp_storage_root("registry-disabled-dataset");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let ethereum_source = MockSource::default()
        .with_logs(vec![log(
            10,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        )])
        .with_chain(ethereum_identity());
    let polygon_source = MockSource::default()
        .with_logs(vec![log(
            10,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        )])
        .with_chain(polygon_identity());
    let registry = QueryServiceRegistry::new()
        .with_service(service_named_with_datasets(
            storage.clone(),
            ethereum_source.clone(),
            "ethereum",
            1,
            true,
            false,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            storage.clone(),
            polygon_source.clone(),
            "polygon",
            137,
        ))
        .expect("register polygon");

    let ethereum_error = registry
        .query(logs_request_for(
            ethereum_identity(),
            10,
            10,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect_err("ethereum logs are disabled");
    let polygon = registry
        .query(logs_request_for(
            polygon_identity(),
            10,
            10,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect("polygon logs are enabled");

    assert_eq!(ethereum_error.kind, DatalensErrorKind::UnsupportedDataset);
    assert_eq!(ethereum_source.calls(), Vec::<SourceCall>::new());
    assert_eq!(log_indexes(&polygon), vec![0]);
    assert_eq!(
        polygon_source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(10, 10))]
    );
}

#[test]
fn test_registry_shared_storage_keeps_manifest_isolated_by_chain_identity() {
    let root = temp_storage_root("registry-manifest-isolation");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            storage.clone(),
            MockSource::default()
                .with_blocks(vec![block(1, "0x01")])
                .with_chain(ethereum_identity()),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            storage.clone(),
            MockSource::default()
                .with_blocks(vec![block(1, "0x89")])
                .with_chain(polygon_identity()),
            "polygon",
            137,
        ))
        .expect("register polygon");

    registry
        .query(blocks_request_for(ethereum_identity(), 1, 1))
        .expect("ethereum query succeeds");
    registry
        .query(blocks_request_for(polygon_identity(), 1, 1))
        .expect("polygon query succeeds");

    assert!(root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(root.join("chains/evm/polygon/137/manifest.json").exists());
    assert!(root.join("chains/evm/ethereum/1/datasets").exists());
    assert!(root.join("chains/evm/polygon/137/datasets").exists());
}

#[test]
fn test_provider_limit_error_is_classified() {
    let source = MockSource::default().with_error(DatalensErrorKind::ProviderLimit);
    let root = temp_storage_root("provider-limit");
    let service = service(LocalStorage::new(&root), source);
    let error = service
        .query(logs_request(
            1,
            2,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect_err("provider limit");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
}

#[test]
fn test_query_native_executes_non_evm_plan_without_legacy_route_validation() {
    let root = temp_storage_root("native-non-evm");
    let source = MockSource::default()
        .with_chain(tron_identity())
        .with_native_rows(vec![serde_json::json!({"event": "transfer"})]);
    let service = QueryService::new_named(
        LocalStorage::new(&root),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 4,
            default_chunk_range_blocks: 2,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
        },
        "tron",
        ChainConfig {
            kind: "tron".to_owned(),
            chain_id: 1,
            rpc_urls: vec!["http://example.invalid".to_owned()],
            finality: datalens_api::config::FinalityConfig::Auto,
            datasets: DatasetsConfig {
                blocks: datalens_api::config::BlocksDatasetConfig {
                    enabled: false,
                    max_batch_blocks: 2,
                },
                logs: LogsDatasetConfig {
                    enabled: false,
                    max_get_logs_range_blocks: 2,
                    max_addresses_per_query: 2,
                },
            },
        },
    );
    let input = NativeQueryInput {
        chain: tron_identity(),
        dataset_key: DatasetKey::tron_events(),
        ledger_range: LedgerRange::blocks(1, 1).expect("valid range"),
        selector: DatasetSelector::try_other(
            AdapterKey::try_new("tron-events").expect("valid selector kind"),
            "tron-events/all",
            "tron-events/all",
        )
        .expect("valid selector"),
        response_shape: ResponseShape::NativeRows,
        field_selection: FieldSelection::All,
    };

    let response = service.query_native(input).expect("native query succeeds");

    assert_eq!(response.chain, tron_identity());
    assert_eq!(response.dataset_key, DatasetKey::tron_events());
    assert_eq!(
        response.cache.missing_ranges,
        vec![LedgerRange::blocks(1, 1).expect("valid range")]
    );
    assert_eq!(response.rows.dataset_key(), &DatasetKey::tron_events());
    assert_eq!(response.rows.row_count(), 1);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Native(
            DatasetKey::tron_events(),
            LedgerRange::blocks(1, 1).expect("valid range")
        )]
    );
}

fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
    service_named(storage, source, "ethereum", 1)
}

fn service_named(
    storage: impl StorageRepository + 'static,
    source: MockSource,
    chain_name: &str,
    chain_id: u64,
) -> QueryService<MockSource> {
    service_named_with_datasets(storage, source, chain_name, chain_id, true, true)
}

fn service_named_with_datasets(
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
        },
        chain_name,
        ChainConfig {
            kind: "evm".to_owned(),
            chain_id,
            rpc_urls: vec!["http://example.invalid".to_owned()],
            finality: datalens_api::config::FinalityConfig::Auto,
            datasets: DatasetsConfig {
                blocks: datalens_api::config::BlocksDatasetConfig {
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

fn blocks_request(from_block: u64, to_block: u64) -> LegacyEvmQueryRequest {
    blocks_request_for(ethereum_identity(), from_block, to_block)
}

fn blocks_request_for(
    chain: ChainIdentity,
    from_block: u64,
    to_block: u64,
) -> LegacyEvmQueryRequest {
    LegacyEvmQueryRequest {
        chain,
        dataset: Dataset::Blocks,
        range: BlockRange::expect_new(from_block, to_block),
        filter: None,
        include_block: false,
    }
}

fn logs_request(from_block: u64, to_block: u64, addresses: Vec<&str>) -> LegacyEvmQueryRequest {
    logs_request_for(ethereum_identity(), from_block, to_block, addresses)
}

fn logs_request_for(
    chain: ChainIdentity,
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
) -> LegacyEvmQueryRequest {
    logs_request_with_topics_for(
        chain,
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
    logs_request_with_topics_for(ethereum_identity(), from_block, to_block, addresses, topics)
}

fn logs_request_with_topics_for(
    chain: ChainIdentity,
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
    topics: Vec<Option<Vec<&str>>>,
) -> LegacyEvmQueryRequest {
    LegacyEvmQueryRequest {
        chain,
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
    }
}

const TOPIC_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TOPIC_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

fn polygon_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
        .expect("valid chain identity")
}

fn tron_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("tron".to_owned()),
        "tron",
        Some(NetworkId::numeric(1)),
    )
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
    match &response.rows {
        QueryRows::EvmBlocks(rows) => rows.iter().map(|row| row.number).collect(),
        _ => panic!("expected blocks"),
    }
}

fn log_indexes(response: &LegacyEvmQueryResponse) -> Vec<u64> {
    match &response.rows {
        QueryRows::EvmLogs(rows) => rows.iter().map(|row| row.log_index).collect(),
        _ => panic!("expected logs"),
    }
}

fn log_addresses(response: &LegacyEvmQueryResponse) -> Vec<String> {
    match &response.rows {
        QueryRows::EvmLogs(rows) => rows.iter().map(|row| row.address.clone()).collect(),
        _ => panic!("expected logs"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
    Native(DatasetKey, LedgerRange),
}

#[derive(Clone)]
struct MockSource {
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
        }
    }
}

impl MockSource {
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    fn with_logs(self, logs: Vec<LogRecord>) -> Self {
        *self.logs.lock().expect("logs lock") = logs;
        self
    }

    fn with_native_rows(self, rows: Vec<serde_json::Value>) -> Self {
        *self.native_rows.lock().expect("native rows lock") = rows;
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

    fn with_safe_height(self, value: u64, finality: FinalityKind) -> Self {
        *self.safe_height.lock().expect("safe height lock") =
            ChainHeight::block(value).with_finality(finality);
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn clear_calls(&self) {
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
        let response = match request.dataset_key.legacy_dataset() {
            Some(Dataset::Blocks) => self.fetch_blocks(range).and_then(|rows| {
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
            Some(Dataset::Logs) => {
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
            None => {
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
            }
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
