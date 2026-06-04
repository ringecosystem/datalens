use std::sync::Arc;

use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_edge::config::{ChainConfig, DatalensConfig, FinalityConfig};
use datalens_edge::{QueryService, QueryServiceRegistry};
use datalens_evm::{EvmFinalityPolicy, EvmRpcClient};
use datalens_metrics::ApplicationIdentity;
use datalens_solana::{SolanaAdapter, SolanaHttpRpc};
use datalens_storage::{
    DurableStorage, LocalObjectStore, LocalStorage, ObjectMetadata, ObjectStore,
    QueryWatermarkRepository, QueryWatermarkStore, S3ObjectStore, UsageLedgerRepository,
    UsageLedgerStore,
};
use datalens_tron::{TronAdapter, TronHttpProvider};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};

use crate::chain_identity;

pub(crate) fn build_service(
    config: &DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
) -> Result<QueryService<EvmRpcClient>, DatalensError> {
    if chain.kind != "evm" {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "CLI query commands only support evm chains",
        ));
    }
    let storage: Arc<dyn datalens_storage::StorageRepository> = Arc::from(build_storage(config)?);
    let usage_ledger: Arc<dyn UsageLedgerRepository> = Arc::from(build_usage_ledger(config)?);
    let query_watermarks: Arc<dyn QueryWatermarkRepository> =
        Arc::from(build_query_watermarks(config)?);
    build_evm_service_with_storage(
        storage,
        usage_ledger,
        query_watermarks,
        config,
        chain_name,
        chain,
    )
}

pub(crate) fn build_service_registry(
    config: &DatalensConfig,
) -> Result<QueryServiceRegistry, DatalensError> {
    let storage: Arc<dyn datalens_storage::StorageRepository> = Arc::from(build_storage(config)?);
    let usage_ledger: Arc<dyn UsageLedgerRepository> = Arc::from(build_usage_ledger(config)?);
    let query_watermarks: Arc<dyn QueryWatermarkRepository> =
        Arc::from(build_query_watermarks(config)?);
    let mut registry =
        QueryServiceRegistry::new().with_application_registry(config.applications.clone())?;
    for (chain_name, chain) in &config.chains {
        match chain.kind.as_str() {
            "evm" => {
                let service = build_evm_service_with_storage(
                    storage.clone(),
                    usage_ledger.clone(),
                    query_watermarks.clone(),
                    config,
                    chain_name.as_str(),
                    chain,
                )?;
                registry = registry.with_service(service)?;
            }
            "solana" => {
                let service = build_solana_service_with_storage(
                    storage.clone(),
                    usage_ledger.clone(),
                    query_watermarks.clone(),
                    config,
                    chain_name.as_str(),
                    chain,
                )?;
                registry = registry.with_service(service)?;
            }
            "tron" => {
                let service = build_tron_service_with_storage(
                    storage.clone(),
                    usage_ledger.clone(),
                    query_watermarks.clone(),
                    config,
                    chain_name.as_str(),
                    chain,
                )?;
                registry = registry.with_service(service)?;
            }
            _ => unreachable!("chain kind validated"),
        }
    }
    Ok(registry)
}

fn build_evm_service_with_storage(
    storage: Arc<dyn datalens_storage::StorageRepository>,
    usage_ledger: Arc<dyn UsageLedgerRepository>,
    query_watermarks: Arc<dyn QueryWatermarkRepository>,
    config: &DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
) -> Result<QueryService<EvmRpcClient>, DatalensError> {
    log::info!(
        "using chain {chain_name} kind={} chain_id={}",
        chain.kind,
        chain.chain_id
    );
    let source = EvmRpcClient::with_chain(
        chain.rpc_urls.clone(),
        chain_identity(chain_name, chain).expect("validated chain identity"),
        evm_finality_policy(&chain.finality),
        chain.datasets.blocks.max_batch_blocks,
        chain.datasets.logs.max_get_logs_range_blocks,
        chain.datasets.logs.max_block_scan_range_blocks,
        chain.datasets.logs.max_addresses_per_query,
    )
    .with_logs_query_strategy(chain.datasets.logs.query_strategy);
    let mut service = datalens_edge::QueryService::new_with_metrics_config(
        storage.clone(),
        source.clone(),
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
        config.metrics.clone(),
    )?
    .with_usage_ledger(
        usage_ledger.clone(),
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_watermarks(
        query_watermarks.clone(),
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    );
    if config.warmup.enabled {
        let mut runtime = WarmupRuntime::new(
            source,
            storage,
            build_warmup_registry(config)?,
            durable_writer_config(&config.writer),
        )
        .with_durable_writer(service.durable_writer())
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: config.warmup.max_fetches_per_loop,
        })
        .with_follow_query_lookahead_blocks(config.warmup.follow_query_lookahead_blocks)
        .with_follow_query_start_offset_blocks(config.warmup.follow_query_start_offset_blocks)
        .with_usage_ledger(usage_ledger)
        .with_query_watermarks(query_watermarks);
        if let Some(recorder) = service.metrics_recorder() {
            runtime = runtime.with_metrics(recorder);
        }
        service = service.with_warmup_pool(WarmupTaskPool::new(
            runtime,
            WarmupSchedulerConfig {
                max_global_concurrent_tasks: config.warmup.max_global_tasks,
                max_concurrent_tasks_per_chain: config.warmup.max_per_chain_tasks,
            },
        ));
    }
    Ok(service)
}

fn build_solana_service_with_storage(
    storage: Arc<dyn datalens_storage::StorageRepository>,
    usage_ledger: Arc<dyn UsageLedgerRepository>,
    query_watermarks: Arc<dyn QueryWatermarkRepository>,
    config: &DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
) -> Result<QueryService<SolanaAdapter<SolanaHttpRpc>>, DatalensError> {
    log::info!(
        "using chain {chain_name} kind={} chain_id={}",
        chain.kind,
        chain.chain_id
    );
    let url = chain.rpc_urls.first().ok_or_else(|| {
        DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {chain_name} must define at least one rpc URL"),
        )
    })?;
    let source = SolanaAdapter::with_provider(
        chain_identity(chain_name, chain).expect("validated chain identity"),
        SolanaHttpRpc::new(url.clone()),
    )
    .with_max_slot_range_len(chain.datasets.blocks.max_batch_blocks.max(1))
    .with_query_strategy(chain.datasets.logs.query_strategy);
    Ok(datalens_edge::QueryService::new_with_metrics_config(
        storage,
        source,
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
        config.metrics.clone(),
    )?
    .with_usage_ledger(
        usage_ledger,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_watermarks(
        query_watermarks,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    ))
}

fn build_tron_service_with_storage(
    storage: Arc<dyn datalens_storage::StorageRepository>,
    usage_ledger: Arc<dyn UsageLedgerRepository>,
    query_watermarks: Arc<dyn QueryWatermarkRepository>,
    config: &DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
) -> Result<QueryService<TronAdapter<TronHttpProvider>>, DatalensError> {
    log::info!(
        "using chain {chain_name} kind={} chain_id={}",
        chain.kind,
        chain.chain_id
    );
    let url = chain.rpc_urls.first().ok_or_else(|| {
        DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {chain_name} must define at least one rpc URL"),
        )
    })?;
    let source = TronAdapter::with_provider(
        chain_identity(chain_name, chain).expect("validated chain identity"),
        tron_provider(url.clone(), chain),
    )
    .with_max_block_range_len(chain.datasets.blocks.max_batch_blocks.max(1))
    .with_events_query_strategy(chain.datasets.logs.query_strategy);
    Ok(datalens_edge::QueryService::new_with_metrics_config(
        storage,
        source,
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
        config.metrics.clone(),
    )?
    .with_usage_ledger(
        usage_ledger,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_watermarks(
        query_watermarks,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    ))
}

pub(crate) fn tron_provider(url: String, chain: &ChainConfig) -> TronHttpProvider {
    let provider = TronHttpProvider::new(url);
    if chain.trongrid.enabled {
        provider.with_trongrid(
            chain
                .trongrid
                .base_url
                .clone()
                .unwrap_or_else(|| "https://api.trongrid.io".to_owned()),
            chain.trongrid.api_key.clone(),
        )
    } else {
        provider
    }
}

pub(crate) fn build_storage(
    config: &DatalensConfig,
) -> Result<Box<dyn datalens_storage::StorageRepository>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            Ok(Box::new(LocalStorage::new_with_config(
                &local.root,
                config.storage.parquet.into(),
            )))
        }
        "s3" => {
            let s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            let store = S3ObjectStore::from_config(s3)?;
            Ok(Box::new(DurableStorage::from_object_store_with_config(
                store,
                config.storage.parquet.into(),
            )))
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        )),
    }
}

pub(crate) fn maintenance_report(
    config: &DatalensConfig,
) -> Result<datalens_storage::MaintenanceReport, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            LocalStorage::new(&local.root).maintenance_report()
        }
        "s3" => {
            let s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            DurableStorage::from_object_store(S3ObjectStore::from_config(s3)?).maintenance_report()
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        )),
    }
}

pub(crate) fn build_usage_ledger(
    config: &DatalensConfig,
) -> Result<Box<dyn UsageLedgerRepository>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            Ok(Box::new(UsageLedgerStore::new(LocalObjectStore::new(
                &local.root,
            ))))
        }
        "s3" => {
            let s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            Ok(Box::new(UsageLedgerStore::new(S3ObjectStore::from_config(
                s3,
            )?)))
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        )),
    }
}

pub fn build_query_watermarks(
    config: &DatalensConfig,
) -> Result<Box<dyn QueryWatermarkRepository>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            Ok(Box::new(QueryWatermarkStore::new(LocalObjectStore::new(
                &local.root,
            ))))
        }
        "s3" => {
            let s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            Ok(Box::new(QueryWatermarkStore::new(
                S3ObjectStore::from_config(s3)?,
            )))
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        )),
    }
}

#[derive(Clone)]
enum WarmupRegistryObjectStore {
    Local(LocalObjectStore),
    S3(S3ObjectStore),
}

impl ObjectStore for WarmupRegistryObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        match self {
            Self::Local(store) => store.get(key),
            Self::S3(store) => store.get(key),
        }
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        match self {
            Self::Local(store) => store.put(key, bytes),
            Self::S3(store) => store.put(key, bytes),
        }
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        match self {
            Self::Local(store) => store.exists(key),
            Self::S3(store) => store.exists(key),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        match self {
            Self::Local(store) => store.list(prefix),
            Self::S3(store) => store.list(prefix),
        }
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        match self {
            Self::Local(store) => store.delete(key),
            Self::S3(store) => store.delete(key),
        }
    }
}

fn build_warmup_registry(
    config: &DatalensConfig,
) -> Result<LocalWarmupRegistry<WarmupRegistryObjectStore>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => Ok(LocalWarmupRegistry::new(WarmupRegistryObjectStore::Local(
            LocalObjectStore::new(&config.warmup.registry_path),
        ))),
        "s3" => {
            let mut s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            let registry_path = config.warmup.registry_path.trim().trim_matches('/');
            s3.prefix = match (s3.prefix.as_deref(), registry_path.is_empty()) {
                (Some(prefix), false) if !prefix.trim().is_empty() => Some(format!(
                    "{}/{registry_path}",
                    prefix.trim().trim_matches('/')
                )),
                (_, false) => Some(registry_path.to_owned()),
                (Some(prefix), true) => Some(prefix.to_owned()),
                (None, true) => None,
            };
            Ok(LocalWarmupRegistry::new(WarmupRegistryObjectStore::S3(
                S3ObjectStore::from_config(s3)?,
            )))
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        )),
    }
}

fn durable_writer_config(
    config: &datalens_edge::config::WriterConfig,
) -> datalens_writer::DurableWriterConfig {
    datalens_writer::DurableWriterConfig {
        target_object_bytes: config.target_object_bytes,
        min_object_rows: config.min_object_rows,
        record_empty_coverage: config.record_empty_coverage,
        staging: datalens_writer::WriteStagingConfig {
            enabled: config.staging.enabled,
            min_rows: config.staging.min_rows,
            target_object_bytes: config.staging.target_object_bytes,
            max_staged_ranges: config.staging.max_staged_ranges,
            max_staged_rows: config.staging.max_staged_rows,
            max_staged_age_ms: config.staging.max_staged_age_ms,
            flush_on_shutdown: config.staging.flush_on_shutdown,
            max_staged_bytes: config.staging.max_staged_bytes,
        },
    }
}

pub(crate) fn evm_finality_policy(finality: &FinalityConfig) -> EvmFinalityPolicy {
    match finality {
        FinalityConfig::Auto => EvmFinalityPolicy::Auto,
        FinalityConfig::Lag {
            safe_lag_blocks,
            finalized_lag_blocks,
        } => EvmFinalityPolicy::Lag {
            safe_lag_blocks: *safe_lag_blocks,
            finalized_lag_blocks: *finalized_lag_blocks,
        },
        FinalityConfig::RpcTags {
            safe_tag,
            finalized_tag,
        } => EvmFinalityPolicy::RpcTags {
            safe_tag: safe_tag.clone(),
            finalized_tag: finalized_tag.clone(),
        },
    }
}

pub(crate) fn finality_summary(chain: &ChainConfig) -> serde_json::Value {
    match &chain.finality {
        FinalityConfig::Auto => serde_json::json!({
            "mode": "auto",
        }),
        FinalityConfig::Lag {
            safe_lag_blocks,
            finalized_lag_blocks,
        } => serde_json::json!({
            "mode": "lag",
            "safe_lag_blocks": safe_lag_blocks,
            "finalized_lag_blocks": finalized_lag_blocks,
        }),
        FinalityConfig::RpcTags {
            safe_tag,
            finalized_tag,
        } => serde_json::json!({
            "mode": "rpc_tags",
            "safe_tag": safe_tag,
            "finalized_tag": finalized_tag,
        }),
    }
}
