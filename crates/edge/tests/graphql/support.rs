use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, HeightRangeKind, ProviderDiagnostics, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, NetworkId, QueryRows,
};
use datalens_edge::QueryService;
use datalens_edge::config::{
    BlocksDatasetConfig, ChainConfig, DatasetsConfig, LogsDatasetConfig, PlannerConfig,
    WriterConfig,
};
use datalens_storage::{LocalObjectStore, LocalStorage};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};

pub(super) fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
    service_named(storage, source, "ethereum", chain_config(1))
}

pub(super) fn service_named(
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

pub(super) fn planner_config() -> PlannerConfig {
    PlannerConfig {
        max_query_range_blocks: 8,
        default_chunk_range_blocks: 4,
    }
}

pub(super) fn writer_config() -> WriterConfig {
    WriterConfig {
        target_object_bytes: 1024,
        min_object_rows: 1,
        record_empty_coverage: true,
        staging: Default::default(),
    }
}

pub(super) fn chain_config(chain_id: u64) -> ChainConfig {
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

pub(super) fn non_evm_chain_config(kind: &str) -> ChainConfig {
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

pub(super) fn warmup_pool(
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

pub(super) fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

pub(super) fn ethereum_chain_input() -> serde_json::Value {
    serde_json::json!({
        "family": { "kind": "evm" },
        "configuredName": "ethereum",
        "networkId": { "numeric": 1 }
    })
}

pub(super) fn solana_chain_input() -> serde_json::Value {
    serde_json::json!({
        "family": { "kind": "other", "other": "solana" },
        "configuredName": "solana-mainnet-beta",
        "networkId": { "textual": "mainnet-beta" }
    })
}

pub(super) fn tron_chain_input() -> serde_json::Value {
    serde_json::json!({
        "family": { "kind": "other", "other": "tron" },
        "configuredName": "tron-mainnet",
        "networkId": { "textual": "mainnet" }
    })
}

pub(super) fn dataset_key_input(family: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "family": family,
        "name": name
    })
}

pub(super) fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number * 10,
    }
}

pub(super) fn temp_storage_root(name: &str) -> PathBuf {
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
pub(super) enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone, Default)]
pub(super) struct MockSource {
    chain: Option<ChainIdentity>,
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
}

impl MockSource {
    pub(super) fn with_chain(mut self, chain: ChainIdentity) -> Self {
        self.chain = Some(chain);
        self
    }

    pub(super) fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks") = blocks;
        self
    }

    pub(super) fn calls(&self) -> Vec<SourceCall> {
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
