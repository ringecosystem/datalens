use clap::{Args, Subcommand};
use datalens_core::{DatasetKey, EvmLogFilter, LedgerRange, LogFilter};
use datalens_edge::QueryApiResponse;
use datalens_planner::{FieldSelection, NativeQueryInput};

use crate::config::{load_config, validate_config};
use crate::runtime::build_service;

use super::{DEFAULT_SERVER_CONFIG, chain_identity, configured_chain};

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
    #[arg(long, default_value = DEFAULT_SERVER_CONFIG, help = "Datalens server config path")]
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
    #[arg(long, default_value = DEFAULT_SERVER_CONFIG, help = "Datalens server config path")]
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

    #[arg(long = "topic0-any-of")]
    pub topic0_any_of: Vec<String>,

    #[arg(long = "selector-json")]
    pub selector_json: Option<String>,
}

pub fn query_command(
    command: QueryCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            let filter = evm_log_filter(
                command.addresses,
                command.topics,
                command.topic0_any_of,
                command.selector_json.as_deref(),
            )?;
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

fn query_chain(command: &QuerySubcommand) -> String {
    match command {
        QuerySubcommand::Blocks(command) => command.chain.clone(),
        QuerySubcommand::Logs(command) => command.chain.clone(),
    }
}

fn evm_log_filter(
    addresses: Vec<String>,
    topics: Vec<String>,
    topic0_any_of: Vec<String>,
    selector_json: Option<&str>,
) -> Result<LogFilter, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(path) = selector_json {
        if !addresses.is_empty() || !topics.is_empty() || !topic0_any_of.is_empty() {
            return Err(
                "query logs --selector-json cannot be combined with address or topic filters"
                    .into(),
            );
        }
        let content = std::fs::read_to_string(path)?;
        return Ok(serde_json::from_str(&content)?);
    }

    let mut topic_slots = topics
        .into_iter()
        .map(|topic| Some(vec![topic]))
        .collect::<Vec<_>>();
    if !topic0_any_of.is_empty() {
        if topic_slots.first().and_then(|slot| slot.as_ref()).is_some() {
            return Err(
                "query logs --topic0-any-of cannot be combined with the first --topic slot".into(),
            );
        }
        if topic_slots.is_empty() {
            topic_slots.push(Some(topic0_any_of));
        } else {
            topic_slots[0] = Some(topic0_any_of);
        }
    }

    Ok(LogFilter {
        addresses,
        topics: topic_slots,
    })
}

fn query_config(command: &QuerySubcommand) -> &str {
    match command {
        QuerySubcommand::Blocks(command) => &command.config,
        QuerySubcommand::Logs(command) => &command.config,
    }
}
