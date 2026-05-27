use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use datalens_api::QueryService;
pub use datalens_api::config::{ChainConfig, DatalensConfig, FinalityConfig};
use datalens_chain::ChainAdapter;
pub use datalens_core::DatalensErrorKind;
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensError, Dataset, EvmLogFilter, LogFilter,
    NetworkId, QueryRequest,
};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata, EvmFinalityPolicy, EvmRpcClient};
use datalens_storage::LocalStorage;
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
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,

    #[command(subcommand)]
    pub command: InspectSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum InspectSubcommand {
    Manifest,
    Coverage,
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
    let (chain_name, chain) = first_chain(&config)?;
    let service = build_service(&config, chain_name, chain);

    let adapter = EvmAdapter::new(EvmAdapterMetadata::default());
    let _capabilities = adapter.capabilities();

    log::info!("serving datalens API on {bind}");
    datalens_api::serve(bind, service, config.chains.keys().cloned().collect()).await?;
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
            "root": config.storage.root,
        },
        "planner": config.planner,
        "writer": config.writer,
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
    let service = build_service(&config, chain_name, chain);
    let request = match command.command {
        QuerySubcommand::Blocks(command) => QueryRequest {
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
            QueryRequest {
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
    let config = load_config(&command.config)?;
    validate_config(&config)?;
    let summary = match command.command {
        InspectSubcommand::Manifest => serde_json::json!({
            "status": "not_implemented",
            "read_only": true,
            "storage": {
                "backend": config.storage.backend,
                "root": config.storage.root,
            },
            "message": "inspect manifest is reserved for the storage inspection API",
        }),
        InspectSubcommand::Coverage => serde_json::json!({
            "status": "not_implemented",
            "read_only": true,
            "storage": {
                "backend": config.storage.backend,
                "root": config.storage.root,
            },
            "message": "inspect coverage is reserved for the storage inspection API",
        }),
    };
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
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
    if config.storage.backend != "local" {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "only local storage backend is supported",
        ));
    }
    if config.storage.root.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "storage.root must not be empty",
        ));
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
    for (name, chain) in &config.chains {
        validate_chain(name, chain)?;
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
) -> QueryService<EvmRpcClient> {
    log::info!(
        "using chain {chain_name} kind={} chain_id={}",
        chain.kind,
        chain.chain_id
    );
    let storage = LocalStorage::new(&config.storage.root);
    let source = EvmRpcClient::with_chain(
        chain.rpc_urls.clone(),
        chain_identity(chain_name, chain).expect("validated chain identity"),
        evm_finality_policy(&chain.finality),
        chain.datasets.blocks.max_batch_blocks,
        chain.datasets.logs.max_get_logs_range_blocks,
        chain.datasets.logs.max_addresses_per_query,
    );
    datalens_api::QueryService::new_named(
        storage,
        source,
        config.planner.clone(),
        config.writer.clone(),
        chain_name.to_owned(),
        chain.clone(),
    )
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

fn first_chain(config: &DatalensConfig) -> Result<(&str, &ChainConfig), DatalensError> {
    config
        .chains
        .iter()
        .next()
        .map(|(name, chain)| (name.as_str(), chain))
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "config must define at least one chain",
            )
        })
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
