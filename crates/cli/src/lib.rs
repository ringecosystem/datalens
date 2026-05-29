use std::{net::SocketAddr, sync::Arc};

use clap::{Args, Parser, Subcommand};
pub use datalens_api::config::{ChainConfig, DatalensConfig, FinalityConfig};
use datalens_api::{
    LegacyEvmQueryRequest, QueryService, QueryServiceRegistry,
    auth::{ApplicationRegistry, normalize_application_id},
};
use datalens_chain::ChainAdapter;
pub use datalens_core::DatalensErrorKind;
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensError, Dataset, EvmLogFilter, LedgerRangeKind,
    LogFilter, NetworkId,
};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata, EvmFinalityPolicy, EvmRpcClient};
use datalens_metrics::ApplicationIdentity;
use datalens_solana::{SolanaAdapter, SolanaHttpRpc};
use datalens_storage::{
    DurableStorage, LocalObjectStore, LocalStorage, ManifestEntry, ManifestFinalityLevel,
    ObjectMetadata, ObjectStore, S3ObjectStore, UsageLedgerRepository, UsageLedgerStore,
};
use datalens_tron::{TronAdapter, TronHttpProvider};
use datalens_warmup::{
    LocalWarmupRegistry, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig, WarmupTaskPool,
};
use tracing_subscriber::EnvFilter;

mod index;

pub use index::*;

#[derive(Debug, Parser)]
#[command(name = "datalens", arg_required_else_help = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve(ConfigCommand),
    Doctor(ConfigCommand),
    Query(QueryCommand),
    Inspect(InspectCommand),
    Index(IndexCommand),
}

#[derive(Debug, Args)]
pub struct ConfigCommand {
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,
}

#[derive(Debug, Args)]
pub struct QueryCommand {
    #[command(subcommand)]
    pub command: QuerySubcommand,
}

#[derive(Debug, Subcommand)]
pub enum QuerySubcommand {
    Blocks(QueryBlocksCommand),
    Logs(QueryLogsCommand),
}

#[derive(Debug, Args)]
pub struct QueryBlocksCommand {
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,

    #[arg(long)]
    pub chain: String,

    #[arg(long)]
    pub from_block: u64,

    #[arg(long)]
    pub to_block: u64,
}

#[derive(Debug, Args)]
pub struct QueryLogsCommand {
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,

    #[arg(long)]
    pub chain: String,

    #[arg(long)]
    pub from_block: u64,

    #[arg(long)]
    pub to_block: u64,

    #[arg(long = "address")]
    pub addresses: Vec<String>,

    #[arg(long = "topic")]
    pub topics: Vec<String>,
}

#[derive(Debug, Args)]
pub struct InspectCommand {
    #[command(subcommand)]
    pub command: InspectSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum InspectSubcommand {
    Manifest(ConfigCommand),
    Coverage(ConfigCommand),
    Maintenance(ConfigCommand),
    Usage(InspectUsageCommand),
}

#[derive(Debug, Args)]
pub struct InspectUsageCommand {
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,

    #[arg(long)]
    pub application: String,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve(command) => serve_command(command).await?,
        Command::Doctor(command) => doctor_command(command)?,
        Command::Query(command) => query_command(command)?,
        Command::Inspect(command) => inspect_command(command)?,
        Command::Index(command) => index_command(command)?,
    }
    Ok(())
}

async fn serve_command(
    command: ConfigCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging()?;
    log::info!("starting datalens");
    let config = load_config(&command.config)?;
    validate_config(&config)?;
    let bind = parse_bind(&config.server.bind)?;
    let registry = build_service_registry(&config)?;
    let warmup_scheduler = if config.warmup.enabled {
        Some(
            registry.start_warmup_scheduler(std::time::Duration::from_millis(
                config.warmup.scheduler_interval_ms,
            )),
        )
    } else {
        None
    };

    let adapter = EvmAdapter::new(EvmAdapterMetadata::default());
    let _capabilities = adapter.capabilities();

    log::info!("serving datalens API on {bind}");
    let lifecycle = datalens_api::ServiceLifecycle::new(registry);
    if let Some(scheduler) = warmup_scheduler {
        datalens_api::serve_lifecycle(bind, lifecycle.with_warmup_scheduler(scheduler)).await?;
    } else {
        datalens_api::serve_lifecycle(bind, lifecycle).await?;
    }
    Ok(())
}

fn doctor_command(command: ConfigCommand) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = load_config(&command.config)?;
    validate_config(&config)?;
    let chains = config
        .chains
        .iter()
        .map(|(name, chain)| doctor_chain_summary(name, chain))
        .collect::<Result<Vec<_>, _>>()?;
    let summary = serde_json::json!({
        "status": "ok",
        "config": command.config,
        "server": {
            "bind": config.server.bind,
        },
        "storage": {
            "backend": config.storage.backend,
            "local": config.storage.local,
            "s3": config.storage.s3,
        },
        "planner": config.planner,
        "writer": config.writer,
        "index": config.index,
        "warmup": config.warmup,
        "metrics": {
            "enabled": config.metrics.enabled,
            "default_application": config.metrics.default_application,
        },
        "chains": chains,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn query_command(command: QueryCommand) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = load_config(query_config(&command.command))?;
    validate_config(&config)?;
    let chain_name = query_chain(&command.command);
    let (chain_name, chain) = configured_chain(&config, &chain_name)?;
    let service = build_service(&config, chain_name, chain)?;
    let request = match command.command {
        QuerySubcommand::Blocks(command) => LegacyEvmQueryRequest {
            chain: chain_identity(chain_name, chain)?,
            dataset: Dataset::Blocks,
            range: BlockRange::try_new(command.from_block, command.to_block)?,
            filter: None,
            include_block: false,
            allow_hot: false,
            finality: datalens_core::QueryFinalityRequirement::DurableOnly,
        },
        QuerySubcommand::Logs(command) => {
            let filter = LogFilter {
                addresses: command.addresses,
                topics: command
                    .topics
                    .into_iter()
                    .map(|topic| Some(vec![topic]))
                    .collect(),
            };
            EvmLogFilter::try_from(&filter)?;
            LegacyEvmQueryRequest {
                chain: chain_identity(chain_name, chain)?,
                dataset: Dataset::Logs,
                range: BlockRange::try_new(command.from_block, command.to_block)?,
                filter: Some(filter),
                include_block: false,
                allow_hot: false,
                finality: datalens_core::QueryFinalityRequirement::DurableOnly,
            }
        }
    };
    let response = service.query(request)?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}

fn inspect_command(
    command: InspectCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let summary = inspect_summary(command)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

pub fn inspect_summary(command: InspectCommand) -> Result<serde_json::Value, DatalensError> {
    let config_path = inspect_config(&command.command).to_owned();
    let config = load_config(&config_path)?;
    validate_config(&config)?;
    let storage_summary = inspect_storage_summary(&config);
    let summary = match command.command {
        InspectSubcommand::Manifest(_) => {
            let manifest = build_storage(&config)?.manifest()?;
            let entries = manifest
                .entries
                .iter()
                .map(inspect_manifest_entry)
                .collect::<Vec<_>>();
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "manifest": {
                    "entry_count": entries.len(),
                    "entries": entries,
                },
            })
        }
        InspectSubcommand::Coverage(_) => {
            let manifest = build_storage(&config)?.manifest()?;
            let entries = manifest
                .entries
                .iter()
                .map(inspect_manifest_entry)
                .collect::<Vec<_>>();
            let data_object_count = manifest
                .entries
                .iter()
                .filter(|entry| entry.object_key.is_some())
                .count();
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "coverage": {
                    "entry_count": entries.len(),
                    "data_object_count": data_object_count,
                    "empty_coverage_count": entries.len() - data_object_count,
                    "entries": entries,
                },
            })
        }
        InspectSubcommand::Maintenance(_) => {
            let maintenance = maintenance_report(&config)?;
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "maintenance": maintenance,
            })
        }
        InspectSubcommand::Usage(command) => {
            let usage_ledger = build_usage_ledger(&config)?;
            let events = usage_ledger.read_application(&command.application)?;
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "usage": {
                    "application": command.application,
                    "event_count": events.len(),
                    "events": events,
                },
            })
        }
    };
    Ok(summary)
}

fn inspect_config(command: &InspectSubcommand) -> &str {
    match command {
        InspectSubcommand::Manifest(command) => &command.config,
        InspectSubcommand::Coverage(command) => &command.config,
        InspectSubcommand::Maintenance(command) => &command.config,
        InspectSubcommand::Usage(command) => &command.config,
    }
}

fn inspect_storage_summary(config: &DatalensConfig) -> serde_json::Value {
    serde_json::json!({
        "backend": config.storage.backend,
        "local": config.storage.local,
        "s3": config.storage.s3,
    })
}

fn inspect_manifest_entry(entry: &ManifestEntry) -> serde_json::Value {
    serde_json::json!({
        "chain": {
            "key": entry.chain.key_prefix(),
            "identity": entry.chain,
        },
        "dataset": {
            "key": entry.dataset_key.as_str(),
            "identity": entry.dataset_key,
        },
        "selector": {
            "fingerprint": entry.selector_fingerprint,
            "canonical_key": entry.selector_canonical_key,
        },
        "range": {
            "kind": range_kind_name(entry.range.kind()),
            "start": entry.range.start(),
            "end": entry.range.end(),
        },
        "finality": manifest_finality_name(entry.finality_level),
        "coverage_type": if entry.object_key.is_some() { "data_object" } else { "empty" },
        "row_count": entry.row_count,
        "object": inspect_object(entry),
    })
}

fn inspect_object(entry: &ManifestEntry) -> serde_json::Value {
    let Some(object_key) = entry.object_key.as_deref() else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "key": object_key,
        "encoding": entry.object_encoding,
        "size_bytes": entry.object_size_bytes,
        "checksum": entry.checksum,
        "checksum_algorithm": entry.checksum_algorithm,
        "written_at_unix_seconds": entry.written_at_unix_seconds,
    })
}

fn range_kind_name(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

fn manifest_finality_name(finality: ManifestFinalityLevel) -> &'static str {
    match finality {
        ManifestFinalityLevel::Safe => "safe",
        ManifestFinalityLevel::Finalized => "finalized",
    }
}

fn load_config(path: &str) -> Result<DatalensConfig, DatalensError> {
    let config = DatalensConfig::from_file(path)?;
    log::info!(
        "loaded config from {} with {} configured chain(s)",
        path,
        config.chains.len()
    );
    Ok(config)
}

pub fn validate_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    if config.chains.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "config must define at least one chain",
        ));
    }
    parse_bind(&config.server.bind)?;
    if !matches!(config.storage.backend.as_str(), "local" | "s3") {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        ));
    }
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root or legacy storage.root must be set",
                )
            })?;
            if local.root.trim().is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must not be empty",
                ));
            }
        }
        "s3" => {
            let s3 = config.storage.s3.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            if s3.bucket.trim().is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3.bucket must not be empty",
                ));
            }
        }
        _ => unreachable!("storage backend validated"),
    }
    if config.planner.max_query_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "planner.max_query_range_blocks must be greater than zero",
        ));
    }
    if config.planner.default_chunk_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "planner.default_chunk_range_blocks must be greater than zero",
        ));
    }
    if config.writer.target_object_bytes == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "writer.target_object_bytes must be greater than zero",
        ));
    }
    if config.writer.min_object_rows == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "writer.min_object_rows must be greater than zero",
        ));
    }
    if config.metrics.enabled && config.metrics.default_application.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "metrics.default_application must not be empty when metrics are enabled",
        ));
    }
    validate_index_config(config)?;
    validate_warmup_config(config)?;
    validate_applications(config)?;
    for (name, chain) in &config.chains {
        validate_chain(name, chain)?;
    }
    Ok(())
}

fn validate_index_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    if config.index.default_chunk_range == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.default_chunk_range must be greater than zero",
        ));
    }
    if config.index.max_concurrency == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.max_concurrency must be greater than zero",
        ));
    }
    if config.index.retry.max_attempts == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.retry.max_attempts must be greater than zero",
        ));
    }
    if config.index.retry.max_backoff_ms < config.index.retry.initial_backoff_ms {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.retry.max_backoff_ms must be greater than or equal to index.retry.initial_backoff_ms",
        ));
    }
    if !matches!(config.index.default_finality.as_str(), "safe" | "finalized") {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.default_finality must be safe or finalized",
        ));
    }
    if config.index.cursor_path.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.cursor_path must not be empty",
        ));
    }
    Ok(())
}

fn validate_warmup_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    if !config.warmup.enabled {
        return Ok(());
    }
    if config.warmup.registry_path.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.registry_path must not be empty when warmup is enabled",
        ));
    }
    if config.warmup.scheduler_interval_ms == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.scheduler_interval_ms must be greater than zero",
        ));
    }
    if config.warmup.max_global_tasks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.max_global_tasks must be greater than zero",
        ));
    }
    if config.warmup.max_per_chain_tasks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.max_per_chain_tasks must be greater than zero",
        ));
    }
    if config.warmup.max_fetches_per_loop == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.max_fetches_per_loop must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_applications(config: &DatalensConfig) -> Result<(), DatalensError> {
    ApplicationRegistry::from_config(config.applications.clone())?;
    for application in &config.applications.applications {
        let application_id = normalize_application_id(&application.id)?;
        if application.chains.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("application {application_id} must allow at least one chain"),
            ));
        }
        for chain in &application.chains {
            if !config.chains.contains_key(chain) {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("application {application_id} references unknown chain {chain}"),
                ));
            }
        }
        if application.datasets.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("application {application_id} must allow at least one dataset"),
            ));
        }
        for dataset in &application.datasets {
            if !matches!(
                dataset.as_str(),
                "blocks" | "transactions" | "receipts" | "logs"
            ) {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("application {application_id} references unknown dataset {dataset}"),
                ));
            }
        }
        if let Some(quota) = &application.quota
            && (matches!(quota.max_query_range_blocks, Some(0))
                || matches!(quota.max_requests_per_minute, Some(0))
                || matches!(quota.max_concurrent_requests, Some(0)))
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("application {application_id} quota limits must be greater than zero"),
            ));
        }
    }
    Ok(())
}

fn validate_chain(name: &str, chain: &ChainConfig) -> Result<(), DatalensError> {
    let _identity = chain_identity(name, chain)?;
    if !matches!(chain.kind.as_str(), "evm" | "solana" | "tron") {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "only evm, solana, and tron chains are supported",
        ));
    }
    if chain.rpc_urls.is_empty() || chain.rpc_urls.iter().any(|url| url.trim().is_empty()) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} must define at least one rpc URL"),
        ));
    }
    if matches!(chain.kind.as_str(), "solana" | "tron") {
        return Ok(());
    }
    if chain.datasets.blocks.max_batch_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} blocks max_batch_blocks must be greater than zero"),
        ));
    }
    if chain.datasets.logs.max_get_logs_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs max_get_logs_range_blocks must be greater than zero"),
        ));
    }
    if chain.datasets.logs.max_addresses_per_query == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs max_addresses_per_query must be greater than zero"),
        ));
    }
    validate_finality(name, &chain.finality)?;
    Ok(())
}

pub fn doctor_chain_summary(
    name: &str,
    chain: &ChainConfig,
) -> Result<serde_json::Value, DatalensError> {
    if chain.kind == "solana" {
        let source = SolanaAdapter::with_provider(
            chain_identity(name, chain)?,
            SolanaHttpRpc::new(chain.rpc_urls.first().cloned().unwrap_or_default()),
        )
        .with_max_slot_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
        let safe_height = source.cache_safe_height().map_err(|error| {
            DatalensError::new(
                error.kind,
                format!(
                    "chain {name} cannot determine finalized slot for durable cache writes: {}",
                    error.message
                ),
            )
        })?;
        return Ok(serde_json::json!({
            "name": name,
            "kind": chain.kind,
            "chain_id": chain.chain_id,
            "rpc_urls": chain.rpc_urls.iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
            "finality": {
                "config": "solana_finalized",
                "detected_height": safe_height.value,
                "detected_kind": format!("{:?}", safe_height.finality),
            },
            "datasets": {
                "slots": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                },
                "blocks": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                },
                "transactions": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                },
                "instructions": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                }
            }
        }));
    }
    if chain.kind == "tron" {
        let source = TronAdapter::with_provider(
            chain_identity(name, chain)?,
            TronHttpProvider::new(chain.rpc_urls.first().cloned().unwrap_or_default()),
        )
        .with_max_block_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
        let safe_height = source.cache_safe_height().map_err(|error| {
            DatalensError::new(
                error.kind,
                format!(
                    "chain {name} cannot determine finalized Tron block for durable cache writes: {}",
                    error.message
                ),
            )
        })?;
        return Ok(serde_json::json!({
            "name": name,
            "kind": chain.kind,
            "chain_id": chain.chain_id,
            "rpc_urls": chain.rpc_urls.iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
            "finality": {
                "config": "tron_solidity_finalized",
                "detected_height": safe_height.value,
                "detected_kind": format!("{:?}", safe_height.finality),
            },
            "datasets": {
                "blocks": {
                    "enabled": chain.datasets.blocks.enabled,
                    "max_batch_blocks": chain.datasets.blocks.max_batch_blocks,
                },
                "events": {
                    "enabled": false,
                    "reason": "unsupported by Tron MVP",
                }
            }
        }));
    }
    let source = EvmRpcClient::with_chain(
        chain.rpc_urls.clone(),
        chain_identity(name, chain)?,
        evm_finality_policy(&chain.finality),
        chain.datasets.blocks.max_batch_blocks,
        chain.datasets.logs.max_get_logs_range_blocks,
        chain.datasets.logs.max_addresses_per_query,
    );
    let safe_height = source.cache_safe_height().map_err(|error| {
        DatalensError::new(
            error.kind,
            format!(
                "chain {name} cannot determine safe/finalized height for durable cache writes: {}",
                error.message
            ),
        )
    })?;
    Ok(serde_json::json!({
        "name": name,
        "kind": chain.kind,
        "chain_id": chain.chain_id,
        "rpc_urls": chain.rpc_urls.iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
        "finality": {
            "config": finality_summary(chain),
            "detected_height": safe_height.value,
            "detected_kind": format!("{:?}", safe_height.finality),
        },
        "datasets": {
            "blocks": {
                "enabled": chain.datasets.blocks.enabled,
                "max_batch_blocks": chain.datasets.blocks.max_batch_blocks,
            },
            "logs": {
                "enabled": chain.datasets.logs.enabled,
                "max_get_logs_range_blocks": chain.datasets.logs.max_get_logs_range_blocks,
                "max_addresses_per_query": chain.datasets.logs.max_addresses_per_query,
            }
        }
    }))
}

fn validate_finality(name: &str, finality: &FinalityConfig) -> Result<(), DatalensError> {
    match finality {
        FinalityConfig::Auto => Ok(()),
        FinalityConfig::Lag {
            safe_lag_blocks,
            finalized_lag_blocks,
        } => {
            if matches!(safe_lag_blocks, Some(0)) || matches!(finalized_lag_blocks, Some(0)) {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("chain {name} lag finality values must be greater than zero"),
                ));
            }
            if safe_lag_blocks.is_none() && finalized_lag_blocks.is_none() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!(
                        "chain {name} lag finality must define safe_lag_blocks or finalized_lag_blocks"
                    ),
                ));
            }
            Ok(())
        }
        FinalityConfig::RpcTags {
            safe_tag,
            finalized_tag,
        } => {
            if safe_tag.trim().is_empty() || finalized_tag.trim().is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("chain {name} RPC finality tags must not be empty"),
                ));
            }
            Ok(())
        }
    }
}

fn build_service(
    config: &DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
) -> Result<QueryService<EvmRpcClient>, DatalensError> {
    if chain.kind != "evm" {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "CLI legacy query commands only support evm chains",
        ));
    }
    let storage: Arc<dyn datalens_storage::StorageRepository> = Arc::from(build_storage(config)?);
    let usage_ledger: Arc<dyn UsageLedgerRepository> = Arc::from(build_usage_ledger(config)?);
    build_evm_service_with_storage(storage, usage_ledger, config, chain_name, chain)
}

fn build_service_registry(config: &DatalensConfig) -> Result<QueryServiceRegistry, DatalensError> {
    let storage: Arc<dyn datalens_storage::StorageRepository> = Arc::from(build_storage(config)?);
    let usage_ledger: Arc<dyn UsageLedgerRepository> = Arc::from(build_usage_ledger(config)?);
    let mut registry =
        QueryServiceRegistry::new().with_application_registry(config.applications.clone())?;
    for (chain_name, chain) in &config.chains {
        match chain.kind.as_str() {
            "evm" => {
                let service = build_evm_service_with_storage(
                    storage.clone(),
                    usage_ledger.clone(),
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
    storage: impl datalens_storage::StorageRepository + Clone + 'static,
    usage_ledger: impl UsageLedgerRepository + Clone + 'static,
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
        chain.datasets.logs.max_addresses_per_query,
    );
    let mut service = datalens_api::QueryService::new_with_metrics_config(
        storage.clone(),
        source.clone(),
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
        config.metrics.clone(),
    )?
    .with_usage_ledger(
        usage_ledger,
        ApplicationIdentity::named(config.metrics.default_application.clone()),
    );
    if config.warmup.enabled {
        let mut runtime = WarmupRuntime::new(
            source,
            storage,
            build_warmup_registry(config)?,
            durable_writer_config(&config.writer),
        )
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: config.warmup.max_fetches_per_loop,
        })
        .with_usage_ledger(build_usage_ledger(config)?);
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
    storage: impl datalens_storage::StorageRepository + Clone + 'static,
    usage_ledger: impl UsageLedgerRepository + Clone + 'static,
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
    .with_max_slot_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
    Ok(datalens_api::QueryService::new_with_metrics_config(
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
    ))
}

fn build_tron_service_with_storage(
    storage: impl datalens_storage::StorageRepository + Clone + 'static,
    usage_ledger: impl UsageLedgerRepository + Clone + 'static,
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
        TronHttpProvider::new(url.clone()),
    )
    .with_max_block_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
    Ok(datalens_api::QueryService::new_with_metrics_config(
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
    ))
}

fn build_storage(
    config: &DatalensConfig,
) -> Result<Box<dyn datalens_storage::StorageRepository>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root or legacy storage.root must be set",
                )
            })?;
            Ok(Box::new(LocalStorage::new(&local.root)))
        }
        "s3" => {
            let s3 = config.storage.s3.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            let store = S3ObjectStore::from_config(s3)?;
            Ok(Box::new(DurableStorage::from_object_store(store)))
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        )),
    }
}

fn maintenance_report(
    config: &DatalensConfig,
) -> Result<datalens_storage::MaintenanceReport, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root or legacy storage.root must be set",
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

fn build_usage_ledger(
    config: &DatalensConfig,
) -> Result<Box<dyn UsageLedgerRepository>, DatalensError> {
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root or legacy storage.root must be set",
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
    config: &datalens_api::config::WriterConfig,
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

fn evm_finality_policy(finality: &FinalityConfig) -> EvmFinalityPolicy {
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

fn finality_summary(chain: &ChainConfig) -> serde_json::Value {
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

fn configured_chain<'a>(
    config: &'a DatalensConfig,
    name: &str,
) -> Result<(&'a str, &'a ChainConfig), DatalensError> {
    config
        .chains
        .get_key_value(name)
        .map(|(name, chain)| (name.as_str(), chain))
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("chain {name} is not configured"),
            )
        })
}

fn query_chain(command: &QuerySubcommand) -> String {
    match command {
        QuerySubcommand::Blocks(command) => command.chain.clone(),
        QuerySubcommand::Logs(command) => command.chain.clone(),
    }
}

fn query_config(command: &QuerySubcommand) -> &str {
    match command {
        QuerySubcommand::Blocks(command) => &command.config,
        QuerySubcommand::Logs(command) => &command.config,
    }
}

fn chain_identity(name: &str, chain: &ChainConfig) -> Result<ChainIdentity, DatalensError> {
    let family = match chain.kind.as_str() {
        "evm" => ChainFamily::Evm,
        value => ChainFamily::try_other(value.to_owned())?,
    };
    ChainIdentity::try_new(family, name, Some(NetworkId::numeric(chain.chain_id)))
}

fn parse_bind(value: &str) -> Result<SocketAddr, DatalensError> {
    value.parse().map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("server.bind must be a socket address: {error}"),
        )
    })
}

pub fn redact_url(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return "<redacted>".to_owned();
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = authority.rsplit('@').next().unwrap_or(authority);
    format!("{scheme}://{host}/<redacted>")
}

fn init_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_log::LogTracer::init()?;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("datalens=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()?;
    Ok(())
}
