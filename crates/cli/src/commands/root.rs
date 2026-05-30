use clap::{Args, Parser, Subcommand};

use super::{
    IndexCommand, InspectCommand, QueryCommand, doctor_command, index_command, inspect_command,
    query_command, serve_command,
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
