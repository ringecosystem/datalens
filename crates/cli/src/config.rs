use datalens_chain::ChainAdapter;
use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_edge::auth::{
    ApplicationRegistry, normalize_application_dataset_key, normalize_application_id,
};
use datalens_edge::config::{ChainConfig, DatalensConfig, FinalityConfig};
use datalens_evm::EvmRpcClient;
use datalens_solana::{SolanaAdapter, SolanaHttpRpc};
use datalens_tron::TronAdapter;

use crate::runtime::{
    evm_block_header_metadata_config, evm_finality_policy, evm_log_reliability_config,
    finality_summary, tron_provider,
};
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
    if config.storage.compaction.enabled {
        if config.storage.compaction.interval_ms == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "storage.compaction.interval_ms must be greater than zero when compaction is enabled",
            ));
        }
        if config.storage.compaction.max_merge_ranges < 2 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "storage.compaction.max_merge_ranges must be at least two when compaction is enabled",
            ));
        }
        if config.storage.compaction.max_tick_duration_ms == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "storage.compaction.max_tick_duration_ms must be greater than zero when compaction is enabled",
            ));
        }
        if config.storage.compaction.max_candidates_per_tick == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "storage.compaction.max_candidates_per_tick must be greater than zero when compaction is enabled",
            ));
        }
        if config.storage.compaction.max_manifest_entries_per_tick == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "storage.compaction.max_manifest_entries_per_tick must be greater than zero when compaction is enabled",
            ));
        }
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
    validate_query_config(config)?;
    validate_warmup_config(config)?;
    validate_cache_repair_config(config)?;
    validate_applications(config)?;
    for (name, chain) in &config.chains {
        validate_chain(name, chain)?;
    }
    Ok(())
}

fn validate_query_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    validate_query_surface("query.native", &config.query.native)?;
    validate_query_surface("query.index", &config.query.index)?;
    if config.query.index.graphql_enabled {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "query.index.graphql_enabled is not a Datalens server surface; run application index GraphQL as an external application service",
        ));
    }
    if config.query.metadata.queue_capacity == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "query.metadata.queue_capacity must be greater than zero",
        ));
    }
    if config.query.metadata.worker_threads == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "query.metadata.worker_threads must be greater than zero",
        ));
    }
    if config.query.metadata.coalesced_capacity == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "query.metadata.coalesced_capacity must be greater than zero",
        ));
    }
    if config.query.durable_intents.worker_threads == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "query.durable_intents.worker_threads must be greater than zero",
        ));
    }
    if config.query.durable_intents.claim_batch_size == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "query.durable_intents.claim_batch_size must be greater than zero",
        ));
    }
    let paths = [
        ("query.native.path", &config.query.native.path),
        (
            "query.native.playground_path",
            &config.query.native.playground_path,
        ),
        ("query.index.path", &config.query.index.path),
        (
            "query.index.playground_path",
            &config.query.index.playground_path,
        ),
    ];
    for (index, (left_name, left_path)) in paths.iter().enumerate() {
        for (right_name, right_path) in paths.iter().skip(index + 1) {
            if left_path == right_path {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("{left_name} must not equal {right_name}"),
                ));
            }
        }
    }
    Ok(())
}

fn validate_query_surface(
    name: &str,
    surface: &datalens_edge::config::GraphqlSurfaceConfig,
) -> Result<(), DatalensError> {
    if !surface.path.starts_with('/') {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{name}.path must start with /"),
        ));
    }
    if !surface.playground_path.starts_with('/') {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{name}.playground_path must start with /"),
        ));
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
    if config.warmup.query_activity_ttl_seconds == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.query_activity_ttl_seconds must be greater than zero",
        ));
    }
    if config
        .warmup
        .follow_query_start_offset_blocks
        .is_some_and(|offset| offset == 0)
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup.follow_query_start_offset_blocks must be greater than zero when set",
        ));
    }
    validate_follow_query_start_offset_tiers(
        "warmup.follow_query_start_offset_tiers_blocks",
        config
            .warmup
            .follow_query_start_offset_tiers_blocks
            .as_deref(),
    )?;
    Ok(())
}

fn validate_cache_repair_config(config: &DatalensConfig) -> Result<(), DatalensError> {
    if !config.cache_repair.enabled {
        return Ok(());
    }
    if config.cache_repair.registry_path.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache_repair.registry_path must not be empty when cache repair is enabled",
        ));
    }
    if config.cache_repair.fetch_timeout_ms == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache_repair.fetch_timeout_ms must be greater than 0 when cache repair is enabled",
        ));
    }
    if config.cache_repair.lease_ttl_ms == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache_repair.lease_ttl_ms must be greater than 0 when cache repair is enabled",
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
            if normalize_application_dataset_key(dataset).is_err() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("application {application_id} references unknown dataset {dataset}"),
                ));
            }
        }
        if let Some(quota) = &application.quota
            && (matches!(quota.max_query_range_blocks, Some(0))
                || matches!(quota.max_hot_query_range_blocks, Some(0))
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
    if chain
        .primary_rpc_url()
        .is_none_or(|url| url.trim().is_empty())
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} must define a primary RPC URL"),
        ));
    }
    if chain
        .secondary_rpc_urls()
        .iter()
        .any(|url| url.trim().is_empty())
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} secondary RPC URLs must not be empty"),
        ));
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
    if chain.datasets.logs.max_block_scan_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs max_block_scan_range_blocks must be greater than zero"),
        ));
    }
    if chain.datasets.logs.max_addresses_per_query == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs max_addresses_per_query must be greater than zero"),
        ));
    }
    if !matches!(
        chain.datasets.logs.header_fetch_mode.as_str(),
        "concurrent" | "batch"
    ) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs header_fetch_mode must be concurrent or batch"),
        ));
    }
    if chain.datasets.logs.header_fetch_concurrency == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs header_fetch_concurrency must be greater than zero"),
        ));
    }
    if chain.datasets.logs.header_fetch_batch_size == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs header_fetch_batch_size must be greater than zero"),
        ));
    }
    if chain.datasets.logs.header_cache_max_entries == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs header_cache_max_entries must be greater than zero"),
        ));
    }
    if chain.datasets.logs.header_durable_chunk_size_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} logs header_durable_chunk_size_blocks must be greater than zero"),
        ));
    }
    if chain
        .warmup
        .follow_query_start_offset_blocks
        .is_some_and(|offset| offset == 0)
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!(
                "chain {name} warmup.follow_query_start_offset_blocks must be greater than zero when set"
            ),
        ));
    }
    validate_follow_query_start_offset_tiers(
        &format!("chain {name} warmup.follow_query_start_offset_tiers_blocks"),
        chain
            .warmup
            .follow_query_start_offset_tiers_blocks
            .as_deref(),
    )?;
    if chain.trongrid.contract_events_min_interval_ms == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!(
                "chain {name} trongrid.contract_events_min_interval_ms must be greater than zero"
            ),
        ));
    }
    if chain.trongrid.contract_events_backoff_ms == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} trongrid.contract_events_backoff_ms must be greater than zero"),
        ));
    }
    if chain.trongrid.contract_events_max_attempts == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("chain {name} trongrid.contract_events_max_attempts must be greater than zero"),
        ));
    }
    if chain.trongrid.contract_events_max_range_blocks == 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!(
                "chain {name} trongrid.contract_events_max_range_blocks must be greater than zero"
            ),
        ));
    }
    if matches!(chain.kind.as_str(), "solana" | "tron") {
        return Ok(());
    }
    validate_finality(name, &chain.finality)?;
    Ok(())
}

fn validate_follow_query_start_offset_tiers(
    name: &str,
    tiers: Option<&[u64]>,
) -> Result<(), DatalensError> {
    let Some(tiers) = tiers else {
        return Ok(());
    };
    if tiers.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{name} must not be empty when set"),
        ));
    }
    if tiers.contains(&0) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{name} values must be greater than zero"),
        ));
    }
    Ok(())
}

pub fn doctor_chain_summary(
    name: &str,
    chain: &ChainConfig,
) -> Result<serde_json::Value, DatalensError> {
    if chain.kind == "solana" {
        let source = SolanaAdapter::with_provider(
            chain_identity(name, chain)?,
            SolanaHttpRpc::new(chain.primary_rpc_url().unwrap_or_default().to_owned()),
        )
        .with_max_slot_range_len(chain.datasets.blocks.max_batch_blocks.max(1))
        .with_query_strategy(chain.datasets.logs.query_strategy);
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
            "rpc_urls": chain.rpc_provider_urls().iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
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
                    "query_strategy": chain.datasets.logs.query_strategy,
                },
                "instructions": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                    "query_strategy": chain.datasets.logs.query_strategy,
                },
                "account_updates": {
                    "enabled": true,
                    "max_slot_range_len": chain.datasets.blocks.max_batch_blocks,
                    "query_strategy": chain.datasets.logs.query_strategy,
                }
            }
        }));
    }
    if chain.kind == "tron" {
        let source = TronAdapter::with_provider(
            chain_identity(name, chain)?,
            tron_provider(
                chain.primary_rpc_url().unwrap_or_default().to_owned(),
                chain,
            ),
        )
        .with_max_block_range_len(chain.datasets.blocks.max_batch_blocks.max(1))
        .with_max_event_range_len(chain.trongrid.contract_events_max_range_blocks.max(1))
        .with_events_query_strategy(chain.datasets.logs.query_strategy);
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
            "rpc_urls": chain.rpc_provider_urls().iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
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
                    "query_strategy": chain.datasets.logs.query_strategy,
                    "selector": "tron_events",
                    "trongrid": {
                        "enabled": chain.trongrid.enabled,
                        "base_url": chain.trongrid.base_url.as_deref().unwrap_or("https://api.trongrid.io"),
                        "api_key_configured": chain.trongrid.api_key.as_ref().is_some_and(|value| !value.trim().is_empty()),
                        "contract_events_min_interval_ms": chain.trongrid.contract_events_min_interval_ms,
                        "contract_events_backoff_ms": chain.trongrid.contract_events_backoff_ms,
                        "contract_events_max_attempts": chain.trongrid.contract_events_max_attempts,
                        "contract_events_max_range_blocks": chain.trongrid.contract_events_max_range_blocks,
                    },
                }
            }
        }));
    }
    let source = EvmRpcClient::with_chain(
        chain.rpc_provider_urls(),
        chain_identity(name, chain)?,
        evm_finality_policy(&chain.finality),
        chain.datasets.blocks.max_batch_blocks,
        chain.datasets.logs.max_get_logs_range_blocks,
        chain.datasets.logs.max_block_scan_range_blocks,
        chain.datasets.logs.max_addresses_per_query,
    )
    .with_logs_query_strategy(chain.datasets.logs.query_strategy)
    .with_log_reliability_config(evm_log_reliability_config(chain))
    .with_block_header_metadata_config(evm_block_header_metadata_config(chain)?);
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
        "rpc_urls": chain.rpc_provider_urls().iter().map(|url| redact_url(url)).collect::<Vec<_>>(),
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
                "reliability_enabled": chain.datasets.logs.reliability_enabled,
                "receipt_fallback_enabled": chain.datasets.logs.receipt_fallback_enabled,
                "query_strategy": chain.datasets.logs.query_strategy,
                "max_get_logs_range_blocks": chain.datasets.logs.max_get_logs_range_blocks,
                "max_block_scan_range_blocks": chain.datasets.logs.max_block_scan_range_blocks,
                "max_addresses_per_query": chain.datasets.logs.max_addresses_per_query,
                "header_fetch_mode": chain.datasets.logs.header_fetch_mode,
                "header_fetch_concurrency": chain.datasets.logs.header_fetch_concurrency,
                "header_fetch_batch_size": chain.datasets.logs.header_fetch_batch_size,
                "header_cache_max_entries": chain.datasets.logs.header_cache_max_entries,
                "header_durable_chunk_size_blocks": chain.datasets.logs.header_durable_chunk_size_blocks,
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
