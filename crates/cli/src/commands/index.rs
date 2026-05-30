use clap::{Args, Subcommand};

use super::index_declarative::{index_doctor_summary, index_plan, index_run};

#[derive(Debug, Args)]
#[command(about = "Run application config-first indexing workflows")]
pub struct IndexCommand {
    #[command(subcommand)]
    pub command: IndexWorkflowCommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum IndexWorkflowCommand {
    /// Plan application index tasks from an app.index.toml config.
    Plan(IndexPlanCommand),
    /// Run application index tasks from an app.index.toml config.
    Run(IndexRunCommand),
    /// Validate an application index config.
    Doctor(IndexDoctorCommand),
}

pub type IndexSubcommand = IndexWorkflowCommand;

#[derive(Debug, Clone, Args)]
pub struct IndexPlanCommand {
    #[arg(long, default_value = "app.index.toml")]
    pub config: String,
}

#[derive(Debug, Clone, Args)]
/// Execute declarative EVM log index tasks. Stops on the first task failure.
pub struct IndexRunCommand {
    #[arg(long, default_value = "app.index.toml")]
    pub config: String,

    #[arg(long)]
    pub from_start: bool,

    #[arg(long)]
    pub no_checkpoint: bool,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct IndexDoctorCommand {
    #[arg(long)]
    pub config: String,
}

pub fn index_command(
    command: IndexCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command.command {
        IndexWorkflowCommand::Plan(command) => {
            let plan = index_plan(command)?;
            println!("{}", serde_json::to_string_pretty(&plan)?);
        }
        IndexWorkflowCommand::Doctor(command) => {
            let summary = index_doctor_summary(&command)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        IndexWorkflowCommand::Run(command) => {
            let summary = index_run(command)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
    }
    Ok(())
}
