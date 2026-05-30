use std::fs;

use datalens_client::DatalensClient;
use datalens_indexer::{
    CheckpointPolicy, DatabaseDriver, DatalensIndexConfig, FinalityRequirement, IndexDaemon,
    IndexDataset, IndexPlanBuilder, IndexRunner, IndexRunnerOptions, OutputConfig,
    OutputSinkConfig, SourceConfig, WebhookHeaderConfig,
};

use crate::redact_url;

use super::{IndexDaemonCommand, IndexDoctorCommand, IndexPlanCommand, IndexRunCommand};

pub(super) fn index_plan(
    command: IndexPlanCommand,
) -> Result<datalens_indexer::IndexPlan, Box<dyn std::error::Error + Send + Sync>> {
    let input = fs::read_to_string(&command.config)?;
    let config = DatalensIndexConfig::from_toml_str(&input)?;
    Ok(IndexPlanBuilder::new().build(&config)?)
}

pub(super) fn index_run(
    command: IndexRunCommand,
) -> Result<datalens_indexer::IndexRunReport, Box<dyn std::error::Error + Send + Sync>> {
    let input = fs::read_to_string(&command.config)?;
    let config = DatalensIndexConfig::from_toml_str(&input)?;
    let plan = IndexPlanBuilder::new().build(&config)?;
    let client = DatalensClient::new(config.client.to_datalens_client_config())?;
    let options = IndexRunnerOptions::default()
        .with_checkpoint_policy(config.checkpoint.clone())
        .with_no_checkpoint(command.no_checkpoint)
        .with_from_start(command.from_start)
        .with_dry_run(command.dry_run);
    let runner = IndexRunner::new(plan, output_sink_config(&config.output)).with_options(options);
    Ok(runner.run(&client)?)
}

pub(super) fn index_daemon(
    command: IndexDaemonCommand,
) -> Result<datalens_indexer::IndexDaemonReport, Box<dyn std::error::Error + Send + Sync>> {
    let input = fs::read_to_string(&command.config)?;
    let config = DatalensIndexConfig::from_toml_str(&input)?;
    datalens_indexer::validate_daemon_config(&config)?;
    let client = DatalensClient::new(config.client.to_datalens_client_config())?;
    let daemon = IndexDaemon::new(config, client);
    let runtime = tokio::runtime::Runtime::new()?;
    Ok(runtime.block_on(daemon.run_until_shutdown(async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            log::error!("failed to listen for index daemon shutdown signal: {error}");
        }
    }))?)
}

fn output_sink_config(output: &OutputConfig) -> OutputSinkConfig {
    match output {
        OutputConfig::Jsonl { path } => OutputSinkConfig::FileJson { path: path.clone() },
        OutputConfig::Parquet { parquet } => OutputSinkConfig::Parquet {
            config: parquet.clone(),
        },
        OutputConfig::Database { database } => match database.driver {
            DatabaseDriver::Sqlite => OutputSinkConfig::DatabaseSqlite {
                url: database.url.clone(),
            },
            DatabaseDriver::Postgres => OutputSinkConfig::DatabasePostgres {
                url: database.url.clone(),
            },
        },
        OutputConfig::Webhook { webhook } => OutputSinkConfig::Webhook {
            webhook: webhook.clone(),
        },
    }
}

pub fn index_doctor_summary(
    command: &IndexDoctorCommand,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let input = fs::read_to_string(&command.config)?;
    let config = DatalensIndexConfig::from_toml_str(&input)?;
    Ok(declarative_index_summary(&config))
}

fn declarative_index_summary(config: &DatalensIndexConfig) -> serde_json::Value {
    serde_json::json!({
        "status": "ok",
        "index": config.index.name,
        "client": {
            "endpoint": config.client.endpoint,
            "application": config.client.application,
        },
        "dataset": index_dataset_name(config.index.dataset),
        "finality": declarative_finality_name(config.index.finality),
        "chunk_blocks": config.index.chunk_blocks,
        "source_count": config.sources.len(),
        "sources": config.sources.iter().map(declarative_source_summary).collect::<Vec<_>>(),
        "output": declarative_output_summary(&config.output),
        "checkpoint": declarative_checkpoint_summary(&config.checkpoint),
    })
}

fn declarative_source_summary(source: &SourceConfig) -> serde_json::Value {
    match source {
        SourceConfig::Evm(source) => serde_json::json!({
            "chain": source.chain,
            "family": "evm",
            "chain_id": source.chain_id,
            "from_block": source.from_block,
            "to_block": source.to_block,
            "addresses": source.addresses.len(),
            "topics": source.topics.len(),
        }),
    }
}

fn declarative_output_summary(output: &OutputConfig) -> serde_json::Value {
    match output {
        OutputConfig::Jsonl { path } => {
            let capability = output.capability();
            serde_json::json!({
                "kind": capability.kind.as_str(),
                "path": path.to_string_lossy(),
                "capability": {
                    "write": capability.supports_write,
                    "query": capability.supports_query,
                    "graphql": capability.supports_graphql,
                    "write_mode": capability.write_mode,
                },
            })
        }
        OutputConfig::Database { database } => {
            let capability = output.capability();
            serde_json::json!({
                "kind": capability.kind.as_str(),
                "database": {
                    "driver": database_driver_name(database.driver),
                    "url": redact_url(&database.url),
                },
                "capability": {
                    "write": capability.supports_write,
                    "query": capability.supports_query,
                    "graphql": capability.supports_graphql,
                    "write_mode": capability.write_mode,
                },
            })
        }
        OutputConfig::Parquet { parquet } => {
            let capability = output.capability();
            serde_json::json!({
                "kind": capability.kind.as_str(),
                "path": parquet.path.to_string_lossy(),
                "parquet": {
                    "max_rows_per_file": parquet.max_rows_per_file,
                    "max_bytes_per_file": parquet.max_bytes_per_file,
                    "partition_by": parquet.partition_by,
                    "compression": parquet.compression,
                },
                "capability": {
                    "write": capability.supports_write,
                    "query": capability.supports_query,
                    "graphql": capability.supports_graphql,
                    "write_mode": capability.write_mode,
                },
            })
        }
        OutputConfig::Webhook { webhook } => {
            let capability = output.capability();
            serde_json::json!({
                "kind": capability.kind.as_str(),
                "webhook": {
                    "url": redact_url(&webhook.url),
                    "headers": webhook.headers.iter().map(webhook_header_summary).collect::<Vec<_>>(),
                    "timeout_ms": webhook.timeout_ms,
                    "max_rows_per_request": webhook.max_rows_per_request,
                    "max_bytes_per_request": webhook.max_bytes_per_request,
                    "idempotency_key_header": webhook.idempotency_key_header,
                    "outbox": {
                        "enabled": webhook.outbox.enabled,
                        "path": webhook.outbox.path.as_ref().map(|path| path.display().to_string()),
                        "max_attempts": webhook.outbox.max_attempts,
                    },
                },
                "capability": {
                    "write": capability.supports_write,
                    "query": capability.supports_query,
                    "graphql": capability.supports_graphql,
                    "write_mode": capability.write_mode,
                },
            })
        }
    }
}

fn webhook_header_summary(header: &WebhookHeaderConfig) -> serde_json::Value {
    serde_json::json!({
        "name": header.name,
        "secret": header.secret,
    })
}

fn database_driver_name(driver: DatabaseDriver) -> &'static str {
    match driver {
        DatabaseDriver::Sqlite => "sqlite",
        DatabaseDriver::Postgres => "postgres",
    }
}

fn declarative_checkpoint_summary(checkpoint: &CheckpointPolicy) -> serde_json::Value {
    match checkpoint {
        CheckpointPolicy::File { path } => serde_json::json!({
            "path": path.to_string_lossy(),
        }),
        CheckpointPolicy::Disabled => serde_json::json!({
            "path": null,
        }),
    }
}

fn index_dataset_name(dataset: IndexDataset) -> &'static str {
    match dataset {
        IndexDataset::EvmLogs => "evm.logs",
    }
}

fn declarative_finality_name(finality: FinalityRequirement) -> &'static str {
    match finality {
        FinalityRequirement::Durable => "durable",
    }
}
