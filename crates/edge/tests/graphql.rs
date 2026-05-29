use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, HeightRangeKind, ProviderDiagnostics, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, NetworkId, QueryRows,
};
use datalens_edge::config::{
    ApiConfig, BlocksDatasetConfig, ChainConfig, DatasetsConfig, GraphqlConfig, LogsDatasetConfig,
    PlannerConfig, WriterConfig,
};
use datalens_edge::{QueryService, QueryServiceRegistry, router, router_with_api_config};
use datalens_solana::SolanaAdapter;
use datalens_storage::{LocalObjectStore, LocalStorage};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};
use tower::ServiceExt;

#[tokio::test]
async fn test_graphql_discovery_lists_registered_chains() {
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("discovery-ethereum")),
            MockSource::default().with_chain(ethereum_identity()),
            "ethereum",
            chain_config(1),
        ))
        .expect("register ethereum")
        .with_service(QueryService::new_named(
            LocalStorage::new(temp_storage_root("discovery-solana")),
            SolanaAdapter::with_fixture_defaults(),
            planner_config(),
            writer_config(),
            "solana-mainnet-beta",
            non_evm_chain_config("solana"),
        ))
        .expect("register solana");
    let app = router(registry);

    let body = graphql_json(
        app.clone(),
        r#"
        query {
          discovery {
            chains {
              identity
              datasets
            }
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    let chains = body["data"]["discovery"]["chains"]
        .as_array()
        .expect("chains");
    assert_eq!(chains.len(), 2);
    assert!(chains.iter().any(|chain| {
        chain["identity"]["configured_name"] == "ethereum"
            && chain["datasets"] == serde_json::json!(["blocks", "logs"])
    }));
    assert!(chains.iter().any(|chain| {
        chain["identity"]["configured_name"] == "solana-mainnet-beta"
            && chain["datasets"] == serde_json::json!([])
    }));
}

#[tokio::test]
async fn test_graphql_native_evm_blocks_and_logs_query_match_rest_contract() {
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-evm")),
            source.clone(),
        ))
        .expect("register service");
    let app = router(registry);

    let body = graphql_json(
        app.clone(),
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
            range
            rows
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_identity(),
                "datasetKey": "evm.blocks",
                "selector": { "kind": "all" },
                "range": { "kind": "block", "start": 10, "end": 10 },
                "finality": "durable_only",
                "fields": "all"
            }
        }),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_eq!(body["data"]["query"]["datasetKey"], "evm.blocks");
    assert_eq!(
        body["data"]["query"]["range"],
        serde_json::json!({ "kind": "block", "start": 10, "end": 10 })
    );
    assert_eq!(
        body["data"]["query"]["rows"]["rows"]["rows"][0]["number"],
        10
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );

    let body = graphql_json(
        app,
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
            range
            rows
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_identity(),
                "datasetKey": "evm.logs",
                "selector": {
                    "kind": "evm_logs",
                    "value": {
                        "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "topics": []
                    }
                },
                "range": { "kind": "block", "start": 20, "end": 21 },
                "finality": "durable_only",
                "fields": "all"
            }
        }),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_eq!(body["data"]["query"]["datasetKey"], "evm.logs");
    assert_eq!(
        body["data"]["query"]["range"],
        serde_json::json!({ "kind": "block", "start": 20, "end": 21 })
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(10, 10)),
            SourceCall::Logs(BlockRange::expect_new(20, 21))
        ]
    );
}

#[tokio::test]
async fn test_graphql_native_solana_query_uses_native_contract() {
    let root = temp_storage_root("gql-solana");
    let solana = SolanaAdapter::with_fixture_defaults();
    let registry = QueryServiceRegistry::new()
        .with_service(QueryService::new_named(
            LocalStorage::new(&root),
            solana,
            planner_config(),
            writer_config(),
            "solana-mainnet-beta",
            non_evm_chain_config("solana"),
        ))
        .expect("register solana");
    let app = router(registry);

    let body = graphql_json(
        app,
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
            range
            rows
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": solana_identity(),
                "datasetKey": "solana.slots",
                "selector": {
                    "kind": "other",
                    "value": {
                        "kind": "solana_all",
                        "fingerprint": "solana-all/all",
                        "canonical_key": "all"
                    }
                },
                "range": { "kind": "slot", "start": 10, "end": 12 },
                "finality": "durable_only",
                "fields": "all"
            }
        }),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_eq!(body["data"]["query"]["datasetKey"], "solana.slots");
    assert_eq!(
        body["data"]["query"]["range"],
        serde_json::json!({ "kind": "slot", "start": 10, "end": 12 })
    );
    assert_eq!(
        body["data"]["query"]["rows"]["rows"]["rows"]["rows"][0]["slot"],
        10
    );
}

#[tokio::test]
async fn test_graphql_warmup_submit_list_and_cancel_task() {
    let root = temp_storage_root("gql-warmup");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("register service");
    let app = router(registry);

    let submit = graphql_json(
        app.clone(),
        r#"
        mutation($input: WarmupSubmitInput!) {
          submitWarmupTask(input: $input) {
            taskId
            created
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_identity(),
                "datasetKey": "evm.logs",
                "selector": {
                    "kind": "evm_logs",
                    "value": {
                        "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "topics": []
                    }
                },
                "rangeKind": { "kind": "block" },
                "start": 20,
                "end": 21,
                "mode": "fixed_range",
                "chunkPolicy": { "max_range_len": 2 }
            }
        }),
    )
    .await;
    assert_eq!(submit["errors"], serde_json::Value::Null);
    let task_id = submit["data"]["submitWarmupTask"]["taskId"]
        .as_str()
        .expect("task id")
        .to_owned();

    let listed = graphql_json(
        app.clone(),
        r#"
        query {
          warmupTasks {
            taskId
            state
            datasetKey
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(listed["errors"], serde_json::Value::Null);
    assert_eq!(
        listed["data"]["warmupTasks"]
            .as_array()
            .expect("tasks")
            .len(),
        1
    );
    assert_eq!(listed["data"]["warmupTasks"][0]["datasetKey"], "evm.logs");

    let cancelled = graphql_json(
        app,
        r#"
        mutation($id: ID!) {
          cancelWarmupTask(id: $id) {
            taskId
            state
          }
        }
        "#,
        serde_json::json!({ "id": task_id }),
    )
    .await;
    assert_eq!(cancelled["errors"], serde_json::Value::Null);
    assert_eq!(cancelled["data"]["cancelWarmupTask"]["state"], "cancelled");
}

#[tokio::test]
async fn test_graphql_playground_respects_config() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-playground")),
            MockSource::default(),
        ))
        .expect("register service");
    let enabled = router_with_api_config(
        registry.clone(),
        ApiConfig {
            graphql: GraphqlConfig {
                enabled: true,
                playground_enabled: true,
            },
        },
    );
    let disabled = router_with_api_config(
        registry,
        ApiConfig {
            graphql: GraphqlConfig {
                enabled: true,
                playground_enabled: false,
            },
        },
    );

    let response = enabled
        .oneshot(
            Request::get("/graphql/playground")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("enabled response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(body_text(response.into_body()).await.contains("/graphql"));

    let response = disabled
        .oneshot(
            Request::get("/graphql/playground")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("disabled response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_graphql_can_be_disabled_independently() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-disabled")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router_with_api_config(
        registry,
        ApiConfig {
            graphql: GraphqlConfig {
                enabled: false,
                playground_enabled: true,
            },
        },
    );

    let response = app
        .oneshot(
            Request::post("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"{ chains }"}"#))
                .expect("request"),
        )
        .await
        .expect("disabled response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_graphql_query_records_metrics_with_application_header() {
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-metrics")),
            source,
        ))
        .expect("register service");
    let app = router(registry);

    let response = app
        .clone()
        .oneshot(
            Request::post("/graphql")
                .header("content-type", "application/json")
                .header("x-datalens-application", "wallet-search")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "query": r#"
                            query($input: QueryInput!) {
                              query(input: $input) {
                                datasetKey
                              }
                            }
                        "#,
                        "variables": {
                            "input": {
                                "chain": ethereum_identity(),
                                "datasetKey": "evm.blocks",
                                "selector": { "kind": "all" },
                                "range": { "kind": "block", "start": 10, "end": 10 }
                            }
                        }
                    }))
                    .expect("graphql request"),
                ))
                .expect("request"),
        )
        .await
        .expect("graphql response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response.into_body()).await;
    assert_eq!(body["errors"], serde_json::Value::Null);

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let text = body_text(metrics.into_body()).await;
    assert!(text.contains(
        r#"datalens_query_total{application="wallet-search",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="filled"} 1"#
    ));
}

#[tokio::test]
async fn test_graphql_errors_include_stable_extensions() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-errors")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router(registry);

    let body = graphql_json(
        app,
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_identity(),
                "datasetKey": "bad-key",
                "selector": { "kind": "all" },
                "range": { "kind": "block", "start": 1, "end": 1 }
            }
        }),
    )
    .await;

    assert_eq!(body["errors"][0]["extensions"]["kind"], "invalid_input");
    assert_eq!(body["errors"][0]["extensions"]["status"], 400);
}

async fn graphql_json(
    app: axum::Router,
    query: &str,
    variables: serde_json::Value,
) -> serde_json::Value {
    let response = app
        .oneshot(
            Request::post("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "query": query,
                        "variables": variables
                    }))
                    .expect("graphql request"),
                ))
                .expect("request"),
        )
        .await
        .expect("graphql response");
    assert_eq!(response.status(), StatusCode::OK);
    body_json(response.into_body()).await
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
        planner_config(),
        writer_config(),
        chain_name,
        chain,
    )
}

fn planner_config() -> PlannerConfig {
    PlannerConfig {
        max_query_range_blocks: 8,
        default_chunk_range_blocks: 4,
    }
}

fn writer_config() -> WriterConfig {
    WriterConfig {
        target_object_bytes: 1024,
        min_object_rows: 1,
        record_empty_coverage: true,
        staging: Default::default(),
    }
}

fn chain_config(chain_id: u64) -> ChainConfig {
    ChainConfig {
        kind: "evm".to_owned(),
        chain_id,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        finality: datalens_edge::config::FinalityConfig::Auto,
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

fn non_evm_chain_config(kind: &str) -> ChainConfig {
    ChainConfig {
        kind: kind.to_owned(),
        chain_id: 0,
        rpc_urls: vec!["http://example.invalid".to_owned()],
        finality: datalens_edge::config::FinalityConfig::Auto,
        datasets: DatasetsConfig {
            blocks: BlocksDatasetConfig {
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

fn warmup_pool(
    root: &std::path::Path,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    let storage = LocalStorage::new(root);
    let registry = LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry")));
    let runtime = WarmupRuntime::new(
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
    WarmupTaskPool::new(
        runtime,
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    )
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn solana_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(NetworkId::textual("mainnet-beta").expect("valid network")),
    )
    .expect("valid chain")
}

fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number * 10,
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-graphql-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone, Default)]
struct MockSource {
    chain: Option<ChainIdentity>,
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
}

impl MockSource {
    fn with_chain(mut self, chain: ChainIdentity) -> Self {
        self.chain = Some(chain);
        self
    }

    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks") = blocks;
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls").clone()
    }
}

impl ChainAdapter for MockSource {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone().unwrap_or_else(ethereum_identity))
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
        Ok(ChainHeight::block(1_000))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(1_000).with_finality(datalens_chain::FinalityLevel::Safe))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        match request.dataset_key {
            ref key if key == &DatasetKey::evm_blocks() => {
                let range = request.range.block_range().expect("block range");
                self.calls
                    .lock()
                    .expect("calls")
                    .push(SourceCall::Blocks(range));
                let rows = self
                    .blocks
                    .lock()
                    .expect("blocks")
                    .iter()
                    .filter(|block| {
                        block.number >= range.from_block && block.number <= range.to_block
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                ChainFetchResponse::try_new(
                    request.chain,
                    request.dataset_key,
                    request.range,
                    request.selector,
                    QueryRows::EvmBlocks(rows),
                )
            }
            ref key if key == &DatasetKey::evm_logs() => {
                let range = request.range.block_range().expect("block range");
                self.calls
                    .lock()
                    .expect("calls")
                    .push(SourceCall::Logs(range));
                Ok(ChainFetchResponse::try_new(
                    request.chain,
                    request.dataset_key,
                    request.range,
                    request.selector,
                    QueryRows::EvmLogs(Vec::new()),
                )?
                .with_provider_diagnostics(ProviderDiagnostics {
                    calls: 1,
                    rows_scanned: 0,
                    warnings: Vec::new(),
                }))
            }
            _ => Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "unsupported dataset",
            )),
        }
    }
}
