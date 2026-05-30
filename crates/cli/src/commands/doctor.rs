use crate::config::{doctor_chain_summary, load_config, validate_config};

use super::ConfigCommand;

pub fn doctor_command(
    command: ConfigCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
