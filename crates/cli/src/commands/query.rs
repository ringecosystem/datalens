use clap::{Args, Subcommand};
use datalens_core::{DatasetKey, EvmLogFilter, LedgerRange, LogFilter};
use datalens_edge::QueryApiResponse;
use datalens_planner::{FieldSelection, NativeQueryInput};

use crate::config::{load_config, validate_config};
use crate::runtime::build_service;

use super::{chain_identity, configured_chain};

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
