#![allow(dead_code, unused_imports)]

pub(crate) use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
pub(crate) use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, HeightRangeKind, ProviderDiagnostics, SelectorKind,
};
pub(crate) use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, LedgerRange, NetworkId, QueryRows,
};
pub(crate) use datalens_edge::config::{
    BlocksDatasetConfig, ChainConfig, DatasetsConfig, EdgeConfig, GraphqlSurfaceConfig,
    LogsDatasetConfig, PlannerConfig, QueryConfig, WriterConfig,
};
pub(crate) use datalens_edge::{
    QueryService, QueryServiceRegistry, router, router_with_edge_config,
};
pub(crate) use datalens_solana::SolanaAdapter;
pub(crate) use datalens_storage::{LocalObjectStore, LocalStorage};
pub(crate) use datalens_tron::TronAdapter;
pub(crate) use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};
pub(crate) use tower::ServiceExt;

pub(crate) async fn graphql_json(
    app: axum::Router,
    query: &str,
    variables: serde_json::Value,
) -> serde_json::Value {
    let response = app
        .oneshot(
            Request::post("/native/graphql")
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
    let body = response.into_body();
    serde_json::from_slice(&to_bytes(body, usize::MAX).await.expect("body bytes"))
        .expect("json body")
}

pub(crate) fn graphql_router(registry: QueryServiceRegistry) -> axum::Router {
    router_with_edge_config(
        registry,
        EdgeConfig {
            query: QueryConfig {
                native: GraphqlSurfaceConfig {
                    graphql_enabled: true,
                    playground_enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        },
    )
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
        planner_config(),
        writer_config(),
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
        rpc_urls: vec!["http://example.invalid".to_owned()],
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
                query_strategy: Default::default(),
                max_get_logs_range_blocks: 4,
                max_block_scan_range_blocks: 4,
                max_addresses_per_query: 2,
            },
        },
    }
}

pub(crate) fn non_evm_chain_config(kind: &str) -> ChainConfig {
    ChainConfig {
        kind: kind.to_owned(),
        chain_id: 0,
        rpc_urls: vec!["http://example.invalid".to_owned()],
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
                query_strategy: Default::default(),
                max_get_logs_range_blocks: 2,
                max_block_scan_range_blocks: 2,
                max_addresses_per_query: 2,
            },
        },
    }
}

pub(crate) fn warmup_pool(
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

pub(crate) fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

pub(crate) fn ethereum_chain_input() -> serde_json::Value {
    serde_json::json!({
        "family": { "kind": "evm" },
        "configuredName": "ethereum",
        "networkId": { "numeric": 1 }
    })
}

pub(crate) fn solana_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(NetworkId::textual("mainnet-beta").expect("valid network")),
    )
    .expect("valid chain")
}

pub(crate) fn solana_chain_input() -> serde_json::Value {
    serde_json::json!({
        "family": { "kind": "other", "other": "solana" },
        "configuredName": "solana-mainnet-beta",
        "networkId": { "textual": "mainnet-beta" }
    })
}

pub(crate) fn tron_chain_input() -> serde_json::Value {
    serde_json::json!({
        "family": { "kind": "other", "other": "tron" },
        "configuredName": "tron-mainnet",
        "networkId": { "textual": "mainnet" }
    })
}

pub(crate) fn dataset_key_input(family: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "family": family,
        "name": name
    })
}

pub(crate) fn assert_input_field_type(
    parent: &serde_json::Value,
    field_name: &str,
    expected_type: &str,
) {
    let field = parent["inputFields"]
        .as_array()
        .expect("input fields")
        .iter()
        .find(|field| field["name"] == field_name)
        .unwrap_or_else(|| panic!("missing input field {field_name}"));
    let field_type = &field["type"];
    let type_name = field_type["name"]
        .as_str()
        .or_else(|| field_type["ofType"]["name"].as_str())
        .expect("field type name");
    assert_eq!(type_name, expected_type, "{field_name} type");
}

pub(crate) fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number * 10,
    }
}

pub(crate) fn temp_storage_root(name: &str) -> PathBuf {
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
pub(crate) enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone, Default)]
pub(crate) struct MockSource {
    chain: Option<ChainIdentity>,
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
}

impl MockSource {
    pub(crate) fn with_chain(mut self, chain: ChainIdentity) -> Self {
        self.chain = Some(chain);
        self
    }

    pub(crate) fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks") = blocks;
        self
    }

    pub(crate) fn calls(&self) -> Vec<SourceCall> {
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
