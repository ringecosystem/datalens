use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use datalens_chain::ChainAdapter;
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, EvmLogFilter,
    LedgerRange, LedgerRangeKind, LogFilter, NetworkId,
};
use datalens_edge::QueryApiResponse;
use datalens_edge::config::{ChainConfig, DatalensConfig};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata};
use datalens_planner::{FieldSelection, NativeQueryInput};
use datalens_storage::{ManifestEntry, ManifestFinalityLevel};
use tracing_subscriber::EnvFilter;

use crate::config::{doctor_chain_summary, load_config, validate_config};
use crate::index::{IndexCommand, index_command};
use crate::runtime::{
    build_service, build_service_registry, build_storage, build_usage_ledger, maintenance_report,
};

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
    Index(Box<IndexCommand>),
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

pub fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve(command) => serve_command(command)?,
        Command::Doctor(command) => doctor_command(command)?,
        Command::Query(command) => query_command(command)?,
        Command::Inspect(command) => inspect_command(command)?,
        Command::Index(command) => index_command(*command)?,
    }
    Ok(())
}

fn serve_command(command: ConfigCommand) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    log::info!("serving datalens edge on {bind}");
    let lifecycle = datalens_edge::ServiceLifecycle::new_with_edge_config(registry, config.edge);
    let runtime = tokio::runtime::Runtime::new()?;
    if let Some(scheduler) = warmup_scheduler {
        runtime.block_on(datalens_edge::serve_lifecycle(
            bind,
            lifecycle.with_warmup_scheduler(scheduler),
        ))?;
    } else {
        runtime.block_on(datalens_edge::serve_lifecycle(bind, lifecycle))?;
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
        QuerySubcommand::Blocks(command) => NativeQueryInput {
            chain: chain_identity(chain_name, chain)?,
            dataset_key: DatasetKey::evm_blocks(),
            ledger_range: LedgerRange::blocks(command.from_block, command.to_block)?,
            selector: datalens_chain::DatasetSelector::all(),
            field_selection: FieldSelection::All,
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
            NativeQueryInput {
                chain: chain_identity(chain_name, chain)?,
                dataset_key: DatasetKey::evm_logs(),
                ledger_range: LedgerRange::blocks(command.from_block, command.to_block)?,
                selector: datalens_chain::DatasetSelector::try_evm_logs(filter)?,
                field_selection: FieldSelection::All,
                finality: datalens_core::QueryFinalityRequirement::DurableOnly,
            }
        }
    };
    let response = QueryApiResponse::try_from_native_response(service.query_native(request)?)?;
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

pub(crate) fn configured_chain<'a>(
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

pub(crate) fn chain_identity(
    name: &str,
    chain: &ChainConfig,
) -> Result<ChainIdentity, DatalensError> {
    let family = match chain.kind.as_str() {
        "evm" => ChainFamily::Evm,
        value => ChainFamily::try_other(value.to_owned())?,
    };
    ChainIdentity::try_new(family, name, Some(NetworkId::numeric(chain.chain_id)))
}

pub(crate) fn parse_bind(value: &str) -> Result<SocketAddr, DatalensError> {
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
