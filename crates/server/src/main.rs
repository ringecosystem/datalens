use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use datalens_server::config::Config;
use datalens_server::run;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "datalens",
    version,
    about = "Query-driven EVM data cache on PostgreSQL"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the configured roles until SIGINT or SIGTERM.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Apply database migrations and exit.
    Migrate {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let result = match Cli::parse().command {
        Command::Serve { config } => match Config::load(&config) {
            Ok(config) => run::serve(config).await,
            Err(error) => Err(error.into()),
        },
        Command::Migrate { config } => match Config::load(&config) {
            Ok(config) => run::migrate(config).await,
            Err(error) => Err(error.into()),
        },
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "datalens failed");
            ExitCode::FAILURE
        }
    }
}
