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
use datalens_storage::{
    DurableStorage, LocalObjectStore, LocalStorage, ManifestEntry, ManifestFinalityLevel,
    S3ObjectStore, UsageLedgerRepository, UsageLedgerStore,
};
use tracing_subscriber::EnvFilter;

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

    let adapter = EvmAdapter::new(EvmAdapterMetadata::default());
    let _capabilities = adapter.capabilities();

    log::info!("serving datalens API on {bind}");
    datalens_api::serve(bind, registry).await?;
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
    let storage = build_storage(&config)?;
    let usage_ledger = build_usage_ledger(&config)?;
    let manifest = storage.manifest()?;
    let entries = manifest
        .entries
        .iter()
        .map(inspect_manifest_entry)
        .collect::<Vec<_>>();
    let storage = inspect_storage_summary(&config);
    let summary = match command.command {
        InspectSubcommand::Manifest(_) => serde_json::json!({
            "status": "ok",
            "read_only": true,
            "config": config_path,
            "storage": storage,
            "manifest": {
                "entry_count": entries.len(),
                "entries": entries,
            },
        }),
        InspectSubcommand::Coverage(_) => {
            let data_object_count = manifest
                .entries
                .iter()
                .filter(|entry| entry.object_key.is_some())
                .count();
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage,
                "coverage": {
                    "entry_count": entries.len(),
                    "data_object_count": data_object_count,
                    "empty_coverage_count": entries.len() - data_object_count,
                    "entries": entries,
                },
            })
        }
        InspectSubcommand::Usage(command) => {
            let events = usage_ledger.read_application(&command.application)?;
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage,
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
    validate_applications(config)?;
    for (name, chain) in &config.chains {
        validate_chain(name, chain)?;
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
            if !matches!(dataset.as_str(), "blocks" | "logs") {
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
    if chain.kind != "evm" {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "only evm chains are supported",
        ));
    }
    if chain.rpc_urls.is_empty() || chain.rpc_urls.iter().any(|url| url.trim().is_empty()) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} must define at least one rpc URL"),
        ));
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
    build_service_with_storage(
        build_storage(config)?,
        build_usage_ledger(config)?,
        config,
        chain_name,
        chain,
    )
}

fn build_service_registry(config: &DatalensConfig) -> Result<QueryServiceRegistry, DatalensError> {
    let storage: Arc<dyn datalens_storage::StorageRepository> = Arc::from(build_storage(config)?);
    let usage_ledger: Arc<dyn UsageLedgerRepository> = Arc::from(build_usage_ledger(config)?);
    let mut registry =
        QueryServiceRegistry::new().with_application_registry(config.applications.clone())?;
    for (chain_name, chain) in &config.chains {
        let service = build_service_with_storage(
            storage.clone(),
            usage_ledger.clone(),
            config,
            chain_name.as_str(),
            chain,
        )?;
        registry = registry.with_service(service)?;
    }
    Ok(registry)
}

fn build_service_with_storage(
    storage: impl datalens_storage::StorageRepository + 'static,
    usage_ledger: impl UsageLedgerRepository + 'static,
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
    ChainIdentity::try_new(
        ChainFamily::Evm,
        name,
        Some(NetworkId::numeric(chain.chain_id)),
    )
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
