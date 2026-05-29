use datalens_chain::ChainAdapter;
use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_edge::auth::{ApplicationRegistry, normalize_application_id};
use datalens_edge::config::{ChainConfig, DatalensConfig, FinalityConfig};
use datalens_evm::EvmRpcClient;
use datalens_solana::{SolanaAdapter, SolanaHttpRpc};
use datalens_tron::TronAdapter;

use crate::runtime::{evm_finality_policy, finality_summary, tron_provider};
use crate::{chain_identity, parse_bind, redact_url};

pub(crate) fn load_config(path: &str) -> Result<DatalensConfig, DatalensError> {
    let config = DatalensConfig::from_file(path)?;
    log::info!(
        "loaded config from {} with {} configured chain(s)",
        path,
        config.chains.len()
    );
    Ok(config)
}

pub fn validate_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    if config.chains.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "config must define at least one chain",
        ));
    }
    parse_bind(&config.server.bind)?;
    if !matches!(config.storage.backend.as_str(), "local" | "s3") {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "storage.backend must be local or s3",
        ));
    }
    match config.storage.backend.as_str() {
        "local" => {
            let local = config.storage.local.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must be set",
                )
            })?;
            if local.root.trim().is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.local.root must not be empty",
                ));
            }
        }
        "s3" => {
            let s3 = config.storage.s3.as_ref().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3 must be set when storage.backend is s3",
                )
            })?;
            if s3.bucket.trim().is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "storage.s3.bucket must not be empty",
                ));
            }
        }
        _ => unreachable!("storage backend validated"),
    }
    if config.planner.max_query_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "planner.max_query_range_blocks must be greater than zero",
        ));
    }
    if config.planner.default_chunk_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "planner.default_chunk_range_blocks must be greater than zero",
        ));
    }
    if config.writer.target_object_bytes == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "writer.target_object_bytes must be greater than zero",
        ));
    }
    if config.writer.min_object_rows == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "writer.min_object_rows must be greater than zero",
        ));
    }
    if config.metrics.enabled && config.metrics.default_application.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "metrics.default_application must not be empty when metrics are enabled",
        ));
    }
    if config.applications.required
        && config.metrics.enabled
        && !config.edge.metrics.public
        && config
            .edge
            .metrics
            .bearer_token
            .as_deref()
            .is_none_or(|token| token.trim().is_empty())
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "edge.metrics.bearer_token must be set when application auth and metrics are enabled unless edge.metrics.public is true",
        ));
    }
    validate_index_config(config)?;
    validate_warmup_config(config)?;
    validate_applications(config)?;
    for (name, chain) in &config.chains {
        validate_chain(name, chain)?;
    }
    Ok(())
}

fn validate_index_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    if config.index.default_chunk_range == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.default_chunk_range must be greater than zero",
        ));
    }
    if config.index.max_concurrency == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.max_concurrency must be greater than zero",
        ));
    }
    if config.index.retry.max_attempts == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.retry.max_attempts must be greater than zero",
        ));
    }
    if config.index.retry.max_backoff_ms < config.index.retry.initial_backoff_ms {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.retry.max_backoff_ms must be greater than or equal to index.retry.initial_backoff_ms",
        ));
    }
    if !matches!(config.index.default_finality.as_str(), "safe" | "finalized") {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.default_finality must be safe or finalized",
        ));
    }
    if config.index.cursor_path.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index.cursor_path must not be empty",
        ));
    }
    Ok(())
}

fn validate_warmup_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    if !config.warmup.enabled {
        return Ok(());
    }
    if config.warmup.registry_path.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.registry_path must not be empty when warmup is enabled",
        ));
    }
    if config.warmup.scheduler_interval_ms == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.scheduler_interval_ms must be greater than zero",
        ));
    }
    if config.warmup.max_global_tasks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.max_global_tasks must be greater than zero",
        ));
    }
    if config.warmup.max_per_chain_tasks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.max_per_chain_tasks must be greater than zero",
        ));
    }
    if config.warmup.max_fetches_per_loop == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.max_fetches_per_loop must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_applications(config: &DatalensConfig) -> Result<(), DatalensError> {
    ApplicationRegistry::from_config(config.applications.clone())?;
    for application in &config.applications.applications {
        let application_id = normalize_application_id(&application.id)?;
        if application.chains.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("application {application_id} must allow at least one chain"),
            ));
        }
        for chain in &application.chains {
            if !config.chains.contains_key(chain) {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("application {application_id} references unknown chain {chain}"),
                ));
            }
        }
        if application.datasets.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("application {application_id} must allow at least one dataset"),
            ));
        }
        if config.applications.required && application.operations.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!(
                    "application {application_id} must declare at least one operation when application auth is required"
                ),
            ));
        }
        for dataset in &application.datasets {
            if !matches!(
                dataset.as_str(),
                "blocks"
                    | "transactions"
                    | "receipts"
                    | "logs"
                    | "evm.blocks"
                    | "evm.logs"
                    | "solana.slots"
                    | "tron.blocks"
                    | "tron.events"
            ) {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("application {application_id} references unknown dataset {dataset}"),
                ));
            }
        }
        if let Some(quota) = &application.quota
            && (matches!(quota.max_query_range_blocks, Some(0))
                || matches!(quota.max_requests_per_minute, Some(0))
                || matches!(quota.max_concurrent_requests, Some(0)))
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("application {application_id} quota limits must be greater than zero"),
            ));
        }
    }
    Ok(())
}

fn validate_chain(name: &str, chain: &ChainConfig) -> Result<(), DatalensError> {
    let _identity = chain_identity(name, chain)?;
    if !matches!(chain.kind.as_str(), "evm" | "solana" | "tron") {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "only evm, solana, and tron chains are supported",
        ));
    }
    if chain.rpc_urls.is_empty() || chain.rpc_urls.iter().any(|url| url.trim().is_empty()) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} must define at least one rpc URL"),
        ));
    }
    if matches!(chain.kind.as_str(), "solana" | "tron") {
        return Ok(());
    }
    if chain.datasets.blocks.max_batch_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} blocks max_batch_blocks must be greater than zero"),
        ));
    }
    if chain.datasets.logs.max_get_logs_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs max_get_logs_range_blocks must be greater than zero"),
        ));
    }
    if chain.datasets.logs.max_addresses_per_query == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs max_addresses_per_query must be greater than zero"),
        ));
    }
    validate_finality(name, &chain.finality)?;
    Ok(())
}

pub fn doctor_chain_summary(
    name: &str,
    chain: &ChainConfig,
) -> Result<serde_json::Value, DatalensError> {
    if chain.kind == "solana" {
        let source = SolanaAdapter::with_provider(
            chain_identity(name, chain)?,
            SolanaHttpRpc::new(chain.rpc_urls.first().cloned().unwrap_or_default()),
        )
        .with_max_slot_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
        let safe_height = source.cache_safe_height().map_err(|error| {
            DatalensError::new(
                error.kind,
                format!(
                    "chain {name} cannot determine finalized slot for durable cache writes: {}",
                    error.message
                ),
            )
        })?;
        return Ok(serde_json::json!({
            "name": name,
            "kind": chain.kind,
            "chain_id": chain.chain_id,
            "rpc_urls": chain.rpc_urls.iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
            "finality": {
                "config": "solana_finalized",
                "detected_height": safe_height.value,
                "detected_kind": format!("{:?}", safe_height.finality),
            },
            "datasets": {
                "slots": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                },
                "blocks": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                },
                "transactions": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                },
                "instructions": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                }
            }
        }));
    }
    if chain.kind == "tron" {
        let source = TronAdapter::with_provider(
            chain_identity(name, chain)?,
            tron_provider(chain.rpc_urls.first().cloned().unwrap_or_default(), chain),
        )
        .with_max_block_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
        let safe_height = source.cache_safe_height().map_err(|error| {
            DatalensError::new(
                error.kind,
                format!(
                    "chain {name} cannot determine finalized Tron block for durable cache writes: {}",
                    error.message
                ),
            )
        })?;
        return Ok(serde_json::json!({
            "name": name,
            "kind": chain.kind,
            "chain_id": chain.chain_id,
            "rpc_urls": chain.rpc_urls.iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
            "finality": {
                "config": "tron_solidity_finalized",
                "detected_height": safe_height.value,
                "detected_kind": format!("{:?}", safe_height.finality),
            },
            "datasets": {
                "blocks": {
                    "enabled": chain.datasets.blocks.enabled,
                    "max_batch_blocks": chain.datasets.blocks.max_batch_blocks,
                },
                "events": {
                    "enabled": true,
                    "selector": "tron_events",
                    "trongrid": {
                        "enabled": chain.trongrid.enabled,
                        "base_url": chain.trongrid.base_url.as_deref().unwrap_or("https://api.trongrid.io"),
                        "api_key_configured": chain.trongrid.api_key.as_ref().is_some_and(|value| !value.trim().is_empty()),
                    },
                }
            }
        }));
    }
    let source = EvmRpcClient::with_chain(
        chain.rpc_urls.clone(),
        chain_identity(name, chain)?,
        evm_finality_policy(&chain.finality),
        chain.datasets.blocks.max_batch_blocks,
        chain.datasets.logs.max_get_logs_range_blocks,
        chain.datasets.logs.max_addresses_per_query,
    );
    let safe_height = source.cache_safe_height().map_err(|error| {
        DatalensError::new(
            error.kind,
            format!(
                "chain {name} cannot determine safe/finalized height for durable cache writes: {}",
                error.message
            ),
        )
    })?;
    Ok(serde_json::json!({
        "name": name,
        "kind": chain.kind,
        "chain_id": chain.chain_id,
        "rpc_urls": chain.rpc_urls.iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
        "finality": {
            "config": finality_summary(chain),
            "detected_height": safe_height.value,
            "detected_kind": format!("{:?}", safe_height.finality),
        },
        "datasets": {
            "blocks": {
                "enabled": chain.datasets.blocks.enabled,
                "max_batch_blocks": chain.datasets.blocks.max_batch_blocks,
            },
            "logs": {
                "enabled": chain.datasets.logs.enabled,
                "max_get_logs_range_blocks": chain.datasets.logs.max_get_logs_range_blocks,
                "max_addresses_per_query": chain.datasets.logs.max_addresses_per_query,
            }
        }
    }))
}

fn validate_finality(name: &str, finality: &FinalityConfig) -> Result<(), DatalensError> {
    match finality {
        FinalityConfig::Auto => Ok(()),
        FinalityConfig::Lag {
            safe_lag_blocks,
            finalized_lag_blocks,
        } => {
            if matches!(safe_lag_blocks, Some(0)) || matches!(finalized_lag_blocks, Some(0)) {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("chain {name} lag finality values must be greater than zero"),
                ));
            }
            if safe_lag_blocks.is_none() && finalized_lag_blocks.is_none() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!(
                        "chain {name} lag finality must define safe_lag_blocks or finalized_lag_blocks"
                    ),
                ));
            }
            Ok(())
        }
        FinalityConfig::RpcTags {
            safe_tag,
            finalized_tag,
        } => {
            if safe_tag.trim().is_empty() || finalized_tag.trim().is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("chain {name} RPC finality tags must not be empty"),
                ));
            }
            Ok(())
        }
    }
}
