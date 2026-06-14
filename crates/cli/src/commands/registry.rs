use clap::{Args, Subcommand};

use crate::{
    config::{load_config, validate_config},
    runtime::migrate_runtime_registry_paths,
};

use super::ConfigCommand;

#[derive(Debug, Args)]
pub struct RegistryCommand {
    #[command(subcommand)]
    pub command: RegistrySubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RegistrySubcommand {
    Migrate(ConfigCommand),
}

pub fn registry_command(
    command: RegistryCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command.command {
        RegistrySubcommand::Migrate(command) => registry_migrate_command(command)?,
    }
    Ok(())
}

fn registry_migrate_command(
    command: ConfigCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = load_config(&command.config)?;
    validate_config(&config)?;
    let report = migrate_runtime_registry_paths(&config)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.total_problems() > 0 {
        return Err(datalens_core::DatalensError::internal(
            "registry migration finished with conflicts or failed object copies",
        )
        .into());
    }
    Ok(())
}
