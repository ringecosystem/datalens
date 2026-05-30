use clap::{Args, Parser, Subcommand};

use super::{
    BenchmarkCommand, CacheCommand, IndexCommand, InspectCommand, QueryCommand, ServeCommand,
    benchmark_command, cache_command, doctor_command, index_command, inspect_command, plan_command,
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
    Serve(ServeCommand),
    Doctor(ConfigCommand),
    Plan(ConfigCommand),
    Query(QueryCommand),
    Inspect(InspectCommand),
    Index(Box<IndexCommand>),
    Cache(Box<CacheCommand>),
    Benchmark(BenchmarkCommand),
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
        Command::Plan(command) => plan_command(command)?,
        Command::Query(command) => query_command(command)?,
        Command::Inspect(command) => inspect_command(command)?,
        Command::Index(command) => index_command(*command)?,
        Command::Cache(command) => cache_command(*command)?,
        Command::Benchmark(command) => benchmark_command(command)?,
    }
    Ok(())
}
