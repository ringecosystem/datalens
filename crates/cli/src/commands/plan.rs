use crate::config::{application_index_config, load_config, validate_config};

use super::ConfigCommand;

pub fn plan_command(
    command: ConfigCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = load_config(&command.config)?;
    validate_config(&config)?;
    let application_index = application_index_config(&config)?;
    let application_index_plan = application_index
        .as_ref()
        .map(|config| datalens_indexer::IndexPlanBuilder::new().build(config))
        .transpose()?;
    let summary = serde_json::json!({
        "status": "planned",
        "config": command.config,
        "service": {
            "bind": config.server.bind,
            "cache": {
                "storage_backend": config.storage.backend,
            },
            "index": {
                "application_configured": application_index.is_some(),
                "application_tasks": application_index_plan
                    .as_ref()
                    .map(|plan| plan.tasks().len())
                    .unwrap_or_default(),
            },
            "query": {
                "native": {
                    "graphql_enabled": config.query.native.graphql_enabled,
                    "path": config.query.native.path,
                    "playground_enabled": config.query.native.playground_enabled,
                    "playground_path": config.query.native.playground_path,
                },
                "index": {
                    "graphql_enabled": config.query.index.graphql_enabled,
                    "path": config.query.index.path,
                    "playground_enabled": config.query.index.playground_enabled,
                    "playground_path": config.query.index.playground_path,
                },
            },
            "metrics": {
                "enabled": config.metrics.enabled,
            },
            "auth": {
                "applications_required": config.applications.required,
                "applications": config.applications.applications.len(),
            },
            "warmup": {
                "enabled": config.warmup.enabled,
            },
        },
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
