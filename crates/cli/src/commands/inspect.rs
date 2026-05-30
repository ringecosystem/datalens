use clap::{Args, Subcommand};
use datalens_core::{DatalensError, LedgerRangeKind};
use datalens_edge::config::DatalensConfig;
use datalens_storage::{ManifestEntry, ManifestFinalityLevel};

use crate::config::{load_config, validate_config};
use crate::runtime::{build_storage, build_usage_ledger, maintenance_report};

use super::ConfigCommand;

#[derive(Debug, Args)]
pub struct InspectCommand {
    #[command(subcommand)]
    pub command: InspectSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum InspectSubcommand {
    Manifest(ConfigCommand),
    Coverage(ConfigCommand),
    Maintenance(ConfigCommand),
    Usage(InspectUsageCommand),
}

#[derive(Debug, Args)]
pub struct InspectUsageCommand {
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,

    #[arg(long)]
    pub application: String,
}

pub fn inspect_command(
    command: InspectCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let summary = inspect_summary(command)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

pub fn inspect_summary(command: InspectCommand) -> Result<serde_json::Value, DatalensError> {
    let config_path = inspect_config(&command.command).to_owned();
    let config = load_config(&config_path)?;
    validate_config(&config)?;
    let storage_summary = inspect_storage_summary(&config);
    let summary = match command.command {
        InspectSubcommand::Manifest(_) => {
            let manifest = build_storage(&config)?.manifest()?;
            let entries = manifest
                .entries
                .iter()
                .map(inspect_manifest_entry)
                .collect::<Vec<_>>();
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "manifest": {
                    "entry_count": entries.len(),
                    "entries": entries,
                },
            })
        }
        InspectSubcommand::Coverage(_) => {
            let manifest = build_storage(&config)?.manifest()?;
            let entries = manifest
                .entries
                .iter()
                .map(inspect_manifest_entry)
                .collect::<Vec<_>>();
            let data_object_count = manifest
                .entries
                .iter()
                .filter(|entry| entry.object_key.is_some())
                .count();
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "coverage": {
                    "entry_count": entries.len(),
                    "data_object_count": data_object_count,
                    "empty_coverage_count": entries.len() - data_object_count,
                    "entries": entries,
                },
            })
        }
        InspectSubcommand::Maintenance(_) => {
            let maintenance = maintenance_report(&config)?;
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "maintenance": maintenance,
            })
        }
        InspectSubcommand::Usage(command) => {
            let usage_ledger = build_usage_ledger(&config)?;
            let events = usage_ledger.read_application(&command.application)?;
            serde_json::json!({
                "status": "ok",
                "read_only": true,
                "config": config_path,
                "storage": storage_summary,
                "usage": {
                    "application": command.application,
                    "event_count": events.len(),
                    "events": events,
                },
            })
        }
    };
    Ok(summary)
}

fn inspect_config(command: &InspectSubcommand) -> &str {
    match command {
        InspectSubcommand::Manifest(command) => &command.config,
        InspectSubcommand::Coverage(command) => &command.config,
        InspectSubcommand::Maintenance(command) => &command.config,
        InspectSubcommand::Usage(command) => &command.config,
    }
}

fn inspect_storage_summary(config: &DatalensConfig) -> serde_json::Value {
    serde_json::json!({
        "backend": config.storage.backend,
        "local": config.storage.local,
        "s3": config.storage.s3,
    })
}

fn inspect_manifest_entry(entry: &ManifestEntry) -> serde_json::Value {
    serde_json::json!({
        "chain": {
            "key": entry.chain.key_prefix(),
            "identity": entry.chain,
        },
        "dataset": {
            "key": entry.dataset_key.as_str(),
            "identity": entry.dataset_key,
        },
        "selector": {
            "fingerprint": entry.selector_fingerprint,
            "canonical_key": entry.selector_canonical_key,
        },
        "range": {
            "kind": range_kind_name(entry.range.kind()),
            "start": entry.range.start(),
            "end": entry.range.end(),
        },
        "finality": manifest_finality_name(entry.finality_level),
        "coverage_type": if entry.object_key.is_some() { "data_object" } else { "empty" },
        "row_count": entry.row_count,
        "object": inspect_object(entry),
    })
}

fn inspect_object(entry: &ManifestEntry) -> serde_json::Value {
    let Some(object_key) = entry.object_key.as_deref() else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "key": object_key,
        "encoding": entry.object_encoding,
        "size_bytes": entry.object_size_bytes,
        "checksum": entry.checksum,
        "checksum_algorithm": entry.checksum_algorithm,
        "written_at_unix_seconds": entry.written_at_unix_seconds,
    })
}

fn range_kind_name(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

fn manifest_finality_name(finality: ManifestFinalityLevel) -> &'static str {
    match finality {
        ManifestFinalityLevel::Safe => "safe",
        ManifestFinalityLevel::Finalized => "finalized",
    }
}
