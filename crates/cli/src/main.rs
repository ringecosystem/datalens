use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use datalens_api::{
    QueryService,
    config::{ChainConfig, DatalensConfig},
};
use datalens_chain::ChainAdapter;
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    EvmLogFilter, LogFilter, NetworkId, QueryRequest,
};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata, EvmRpcClient};
use datalens_storage::LocalStorage;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "datalens", arg_required_else_help = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve(ConfigCommand),
    Doctor(ConfigCommand),
    Query(QueryCommand),
    Inspect(InspectCommand),
}

#[derive(Debug, Args)]
struct ConfigCommand {
    #[arg(long, default_value = "datalens.toml")]
    config: String,
}

#[derive(Debug, Args)]
struct QueryCommand {
    #[command(subcommand)]
    command: QuerySubcommand,
}

#[derive(Debug, Subcommand)]
enum QuerySubcommand {
    Blocks(QueryBlocksCommand),
    Logs(QueryLogsCommand),
}

#[derive(Debug, Args)]
struct QueryBlocksCommand {
    #[arg(long, default_value = "datalens.toml")]
    config: String,

    #[arg(long)]
    chain: String,

    #[arg(long)]
    from_block: u64,

    #[arg(long)]
    to_block: u64,
}

#[derive(Debug, Args)]
struct QueryLogsCommand {
    #[arg(long, default_value = "datalens.toml")]
    config: String,

    #[arg(long)]
    chain: String,

    #[arg(long)]
    from_block: u64,

    #[arg(long)]
    to_block: u64,

    #[arg(long = "address")]
    addresses: Vec<String>,

    #[arg(long = "topic")]
    topics: Vec<String>,
}

#[derive(Debug, Args)]
struct InspectCommand {
    #[arg(long, default_value = "datalens.toml")]
    config: String,

    #[command(subcommand)]
    command: InspectSubcommand,
}

#[derive(Debug, Subcommand)]
enum InspectSubcommand {
    Manifest,
    Coverage,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        .map(|(name, chain)| {
            serde_json::json!({
                "name": name,
                "kind": chain.kind,
                "chain_id": chain.chain_id,
                "rpc_urls": chain.rpc_urls.iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
                "safe_height_lag_blocks": chain.safe_height_lag_blocks,
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
            })
        })
        .collect::<Vec<_>>();
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

fn validate_config(config: &DatalensConfig) -> Result<(), DatalensError> {
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
    Ok(())
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
        chain.safe_height_lag_blocks,
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

fn redact_url(value: &str) -> String {
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

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn test_bare_cli_requires_subcommand_instead_of_serve_default() {
        assert!(Cli::try_parse_from(["datalens"]).is_err());
    }

    #[test]
    fn test_serve_accepts_config_path() {
        let cli = Cli::parse_from(["datalens", "serve", "--config", "custom.toml"]);

        match cli.command {
            Command::Serve(command) => assert_eq!(command.config, "custom.toml"),
            command => panic!("expected serve command, got {command:?}"),
        }
    }

    #[test]
    fn test_doctor_accepts_config_path() {
        let cli = Cli::parse_from(["datalens", "doctor", "--config", "custom.toml"]);

        match cli.command {
            Command::Doctor(command) => assert_eq!(command.config, "custom.toml"),
            command => panic!("expected doctor command, got {command:?}"),
        }
    }

    #[test]
    fn test_query_blocks_accepts_chain_and_range() {
        let cli = Cli::parse_from([
            "datalens",
            "query",
            "blocks",
            "--config",
            "custom.toml",
            "--chain",
            "ethereum",
            "--from-block",
            "10",
            "--to-block",
            "12",
        ]);

        match cli.command {
            Command::Query(QueryCommand {
                command: QuerySubcommand::Blocks(command),
                ..
            }) => {
                assert_eq!(command.config, "custom.toml");
                assert_eq!(command.chain, "ethereum");
                assert_eq!(command.from_block, 10);
                assert_eq!(command.to_block, 12);
            }
            command => panic!("expected query blocks command, got {command:?}"),
        }
    }

    #[test]
    fn test_query_logs_accepts_address_and_topic_filters() {
        let cli = Cli::parse_from([
            "datalens",
            "query",
            "logs",
            "--config",
            "custom.toml",
            "--chain",
            "ethereum",
            "--from-block",
            "10",
            "--to-block",
            "12",
            "--address",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--topic",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ]);

        match cli.command {
            Command::Query(QueryCommand {
                command: QuerySubcommand::Logs(command),
                ..
            }) => {
                assert_eq!(command.config, "custom.toml");
                assert_eq!(command.addresses.len(), 1);
                assert_eq!(command.topics.len(), 1);
            }
            command => panic!("expected query logs command, got {command:?}"),
        }
    }

    #[test]
    fn test_redact_url_hides_credentials_path_and_query() {
        assert_eq!(
            redact_url("https://user:secret@example.invalid/path?token=secret"),
            "https://example.invalid/<redacted>"
        );
    }
}
