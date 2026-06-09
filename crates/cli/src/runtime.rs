use std::{
    sync::{
        Arc, Once,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind};
use datalens_edge::config::{ChainConfig, DatalensConfig, FinalityConfig};
use datalens_edge::{QueryService, QueryServiceRegistry};
use datalens_evm::{
    EvmBlockHeaderFetchMode, EvmBlockHeaderMetadataConfig, EvmFinalityPolicy, EvmRpcClient,
};
use datalens_metrics::ApplicationIdentity;
use datalens_solana::{SolanaAdapter, SolanaHttpRpc};
use datalens_storage::{
    DurablePromotionIntentRepository, DurablePromotionIntentStore, DurableStorage,
    LocalObjectStore, LocalStorage, MaintenanceCompactionConfig, ObjectMetadata, ObjectStore,
    QueryActivityRepository, QueryActivityStore, QueryWatermarkRepository, QueryWatermarkStore,
    S3ObjectStore, UsageLedgerRepository, UsageLedgerStore,
};
use datalens_tron::{TronAdapter, TronHttpProvider};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};

use crate::chain_identity;

#[derive(Clone)]
struct QueryRuntimeStores {
    storage: Arc<dyn datalens_storage::StorageRepository>,
    usage_ledger: Arc<dyn UsageLedgerRepository>,
    query_watermarks: Arc<dyn QueryWatermarkRepository>,
    query_activity: Arc<dyn QueryActivityRepository>,
    durable_intents: Arc<dyn DurablePromotionIntentRepository>,
    warmup_registry: Option<LocalWarmupRegistry<WarmupRegistryObjectStore>>,
    durable_intent_startup_maintenance: Arc<Once>,
}

impl QueryRuntimeStores {
    fn build(config: &DatalensConfig) -> Result<Self, DatalensError> {
        let warmup_registry = if config.warmup.enabled {
            Some(build_warmup_registry(config)?)
        } else {
            None
        };
        Ok(Self {
            storage: Arc::from(build_storage(config)?),
            usage_ledger: Arc::from(build_usage_ledger(config)?),
            query_watermarks: Arc::from(build_query_watermarks(config)?),
            query_activity: Arc::from(build_query_activity(config)?),
            durable_intents: Arc::from(build_durable_intents(config)?),
            warmup_registry,
            durable_intent_startup_maintenance: Arc::new(Once::new()),
        })
    }
}

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
    let stores = QueryRuntimeStores::build(config)?;
    build_evm_service_with_storage(stores, config, chain_name, chain)
}

pub(crate) fn build_service_registry(
    config: &DatalensConfig,
) -> Result<QueryServiceRegistry, DatalensError> {
    let stores = QueryRuntimeStores::build(config)?;
    let mut registry =
        QueryServiceRegistry::new().with_application_registry(config.applications.clone())?;
    for (chain_name, chain) in &config.chains {
        match chain.kind.as_str() {
            "evm" => {
                let service = build_evm_service_with_storage(
                    stores.clone(),
                    config,
                    chain_name.as_str(),
                    chain,
                )?;
                registry = registry.with_service(service)?;
            }
            "solana" => {
                let service = build_solana_service_with_storage(
                    stores.clone(),
                    config,
                    chain_name.as_str(),
                    chain,
                )?;
                registry = registry.with_service(service)?;
            }
            "tron" => {
                let service = build_tron_service_with_storage(
                    stores.clone(),
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

pub(crate) fn start_storage_compaction_worker(
    config: &DatalensConfig,
) -> Result<Option<StorageCompactionWorker>, DatalensError> {
    if !config.storage.compaction.enabled {
        log::info!("storage compaction worker disabled");
        return Ok(None);
    }
    let chains = configured_compaction_chains(config)?;
    if chains.is_empty() {
        log::info!("storage compaction worker disabled because no chains are configured");
        return Ok(None);
    }
    let interval = Duration::from_millis(config.storage.compaction.interval_ms.max(1));
    let compaction = MaintenanceCompactionConfig {
        min_object_bytes: config.storage.compaction.min_object_bytes,
        max_merge_ranges: config.storage.compaction.max_merge_ranges.max(2),
        max_tick_duration_ms: config.storage.compaction.max_tick_duration_ms,
        max_candidates_per_tick: config.storage.compaction.max_candidates_per_tick,
        max_manifest_entries_per_tick: config.storage.compaction.max_manifest_entries_per_tick,
    };
    let storage = match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            CompactionStorage::Local(LocalStorage::new_with_config(
                &local.root,
                config.storage.parquet.into(),
            ))
        }
        "s3" => {
            let s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            CompactionStorage::S3(DurableStorage::from_object_store_with_config(
                S3ObjectStore::from_config(s3)?,
                config.storage.parquet.into(),
            ))
        }
        _ => {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "storage.backend must be local or s3",
            ));
        }
    };
    Ok(Some(StorageCompactionWorker::start(
        storage, chains, compaction, interval,
    )?))
}

pub(crate) struct StorageCompactionWorker {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

#[derive(Clone)]
enum CompactionStorage {
    Local(LocalStorage),
    S3(DurableStorage<S3ObjectStore>),
}

impl StorageCompactionWorker {
    fn start(
        storage: CompactionStorage,
        chains: Vec<ChainIdentity>,
        config: MaintenanceCompactionConfig,
        interval: Duration,
    ) -> Result<Self, DatalensError> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let handle = thread::Builder::new()
            .name("datalens-storage-compaction".to_owned())
            .spawn(move || {
                log::info!(
                    "storage compaction worker started interval_ms={} min_object_bytes={} max_merge_ranges={} max_tick_duration_ms={} max_candidates_per_tick={} max_manifest_entries_per_tick={} chain_count={}",
                    interval.as_millis(),
                    config.min_object_bytes,
                    config.max_merge_ranges,
                    config.max_tick_duration_ms,
                    config.max_candidates_per_tick,
                    config.max_manifest_entries_per_tick,
                    chains.len()
                );
                let mut consecutive_failures = 0u32;
                let mut next_chain_index = 0usize;
                while !worker_stop.load(Ordering::Relaxed) {
                    thread::park_timeout(compaction_sleep_duration(interval, consecutive_failures));
                    if worker_stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let chain = chains[next_chain_index % chains.len()].clone();
                    next_chain_index = next_chain_index.saturating_add(1);
                    let started = Instant::now();
                    let result = match &storage {
                        CompactionStorage::Local(storage) => {
                            storage.compact_small_objects_for_chain(&chain, config)
                        }
                        CompactionStorage::S3(storage) => {
                            storage.compact_small_objects_for_chain(&chain, config)
                        }
                    };
                    match result {
                        Ok(report) => {
                            consecutive_failures = 0;
                            log::info!(
                                "storage compaction tick completed chain_key={} candidate_count={} processed_candidates={} compacted_objects={} compacted_rows={} tick_status={} duration_ms={}",
                                chain.key_prefix(),
                                report.candidate_count,
                                report.processed_candidates,
                                report.compacted_objects,
                                report.compacted_rows,
                                report.tick_status.as_str(),
                                started.elapsed().as_millis()
                            );
                        }
                        Err(error) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            log::warn!(
                                "storage compaction tick failed chain_key={} tick_status=failed kind={:?} message={} consecutive_failures={} duration_ms={}",
                                chain.key_prefix(),
                                error.kind,
                                error.message,
                                consecutive_failures,
                                started.elapsed().as_millis()
                            );
                        }
                    }
                }
                log::info!("storage compaction worker stopped");
            })
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("start storage compaction worker: {error}"),
                )
            })?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }
}

impl Drop for StorageCompactionWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = &self.handle {
            handle.thread().unpark();
        }
        if let Some(handle) = self.handle.take()
            && let Err(error) = handle.join()
        {
            log::warn!("storage compaction worker join failed error={error:?}");
        }
    }
}

fn configured_compaction_chains(
    config: &DatalensConfig,
) -> Result<Vec<ChainIdentity>, DatalensError> {
    config
        .chains
        .iter()
        .map(|(name, chain)| chain_identity(name, chain))
        .collect()
}

fn compaction_sleep_duration(interval: Duration, consecutive_failures: u32) -> Duration {
    let multiplier = 1_u32 << consecutive_failures.min(4);
    interval.saturating_mul(multiplier)
}

fn build_evm_service_with_storage(
    stores: QueryRuntimeStores,
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
    .with_logs_query_strategy(chain.datasets.logs.query_strategy)
    .with_block_header_metadata_config(evm_block_header_metadata_config(chain)?);
    let mut service = datalens_edge::QueryService::new_with_metrics_config(
        stores.storage.clone(),
        source.clone(),
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
        config.metrics.clone(),
    )?
    .with_usage_ledger(
        stores.usage_ledger.clone(),
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_watermarks(
        stores.query_watermarks.clone(),
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_activity(
        stores.query_activity.clone(),
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_durable_intents_startup_maintenance_once(
        stores.durable_intents.clone(),
        stores.durable_intent_startup_maintenance.clone(),
    );
    if config.warmup.enabled {
        let mut runtime = WarmupRuntime::new(
            source,
            stores.storage,
            stores
                .warmup_registry
                .clone()
                .ok_or_else(|| DatalensError::internal("warmup registry was not initialized"))?,
            durable_writer_config(&config.writer),
        )
        .with_durable_writer(service.durable_writer())
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: config.warmup.max_fetches_per_loop,
        })
        .with_follow_query_lookahead_blocks(config.warmup.follow_query_lookahead_blocks)
        .with_follow_query_start_offset_blocks(
            chain
                .warmup
                .follow_query_start_offset_blocks
                .or(config.warmup.follow_query_start_offset_blocks),
        )
        .with_follow_query_start_offset_tiers_blocks(
            chain
                .warmup
                .follow_query_start_offset_tiers_blocks
                .clone()
                .or(config.warmup.follow_query_start_offset_tiers_blocks.clone()),
        )
        .with_follow_query_catchup_threshold_blocks(
            chain
                .warmup
                .follow_query_catchup_threshold_blocks
                .unwrap_or(config.warmup.follow_query_catchup_threshold_blocks),
        )
        .with_usage_ledger(stores.usage_ledger)
        .with_query_activity_ttl_seconds(config.warmup.query_activity_ttl_seconds)
        .with_query_activity(stores.query_activity)
        .with_query_watermarks(stores.query_watermarks)
        .with_durable_intents(stores.durable_intents);
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
    stores: QueryRuntimeStores,
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
        stores.storage,
        source,
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
        config.metrics.clone(),
    )?
    .with_usage_ledger(
        stores.usage_ledger,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_watermarks(
        stores.query_watermarks,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_activity(
        stores.query_activity,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_durable_intents_startup_maintenance_once(
        stores.durable_intents,
        stores.durable_intent_startup_maintenance,
    ))
}

fn build_tron_service_with_storage(
    stores: QueryRuntimeStores,
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
        stores.storage,
        source,
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
        config.metrics.clone(),
    )?
    .with_usage_ledger(
        stores.usage_ledger,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_watermarks(
        stores.query_watermarks,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_query_activity(
        stores.query_activity,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    )
    .with_durable_intents_startup_maintenance_once(
        stores.durable_intents,
        stores.durable_intent_startup_maintenance,
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

pub fn build_query_activity(
    config: &DatalensConfig,
) -> Result<Box<dyn QueryActivityRepository>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            Ok(Box::new(QueryActivityStore::new(LocalObjectStore::new(
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
            Ok(Box::new(QueryActivityStore::new(
                S3ObjectStore::from_config(s3)?,
            )))
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        )),
    }
}

pub fn build_durable_intents(
    config: &DatalensConfig,
) -> Result<Box<dyn DurablePromotionIntentRepository>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            Ok(Box::new(DurablePromotionIntentStore::new(
                LocalObjectStore::new(&local.root),
            )))
        }
        "s3" => {
            let s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            Ok(Box::new(DurablePromotionIntentStore::new(
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

pub(crate) fn evm_block_header_metadata_config(
    chain: &ChainConfig,
) -> Result<EvmBlockHeaderMetadataConfig, DatalensError> {
    let fetch_mode = match chain.datasets.logs.header_fetch_mode.as_str() {
        "concurrent" => EvmBlockHeaderFetchMode::Concurrent,
        "batch" => EvmBlockHeaderFetchMode::Batch,
        _ => {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "logs header_fetch_mode must be concurrent or batch",
            ));
        }
    };
    Ok(EvmBlockHeaderMetadataConfig::default()
        .with_cache_max_entries(chain.datasets.logs.header_cache_max_entries)
        .with_fetch_concurrency(chain.datasets.logs.header_fetch_concurrency)
        .with_fetch_mode(fetch_mode)
        .with_batch_size(chain.datasets.logs.header_fetch_batch_size))
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
