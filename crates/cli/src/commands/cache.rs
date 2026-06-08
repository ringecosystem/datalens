use std::sync::Arc;

use clap::{Args, Subcommand};
use datalens_chain::{ChainAdapter, DatasetSelector, HeightRangeKind};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange, LedgerRangeKind,
    LogFilter,
};
use datalens_edge::auth::normalize_application_id;
use datalens_evm::EvmRpcClient;
use datalens_metrics::ApplicationIdentity;
use datalens_runtime_indexer::{
    FileIndexCursorStore, IndexDatasetProviderLimit, IndexDatasetRequest, IndexDatasetSelection,
    IndexFinalityRequirement, IndexJob, IndexJobId, IndexPlan, IndexRetryPolicy, IndexRunMode,
    IndexRuntime, IndexRuntimeConfig,
};
use datalens_solana::{
    SolanaAdapter, SolanaHttpRpc, solana_address_selector, solana_program_selector,
    solana_signature_selector,
};
use datalens_storage::StorageRepository;
use datalens_tron::{
    TronAdapter, TronEventFilter, TronHttpProvider, tron_all_selector, tron_event_selector,
};
use datalens_writer::DurableWriterConfig;

use crate::{
    ChainConfig, DatalensConfig, build_storage, build_usage_ledger, chain_identity,
    configured_chain, evm_block_header_metadata_config, evm_finality_policy, load_config,
    validate_config,
};

use super::index_report::{plan_summary, run_summary};

#[derive(Debug, Args)]
#[command(about = "Operate durable cache and runtime indexing storage")]
pub struct CacheCommand {
    #[command(subcommand)]
    pub command: CacheWorkflowCommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum CacheWorkflowCommand {
    /// Backfill durable cache storage for a selected chain range.
    Backfill(CacheBackfillCommand),
    /// Resume durable cache indexing from persisted cursor state.
    Resume(CacheResumeCommand),
    /// Repair durable cache coverage gaps.
    Repair(CacheRepairCommand),
    /// Verify durable cache coverage without writing data.
    Verify(CacheVerifyCommand),
}

#[derive(Debug, Clone, Args)]
pub struct CacheBackfillCommand {
    #[command(flatten)]
    pub common: CacheCommonCommand,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct CacheResumeCommand {
    #[command(flatten)]
    pub common: CacheCommonCommand,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct CacheRepairCommand {
    #[command(flatten)]
    pub common: CacheCommonCommand,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct CacheVerifyCommand {
    #[command(flatten)]
    pub common: CacheCommonCommand,

    #[arg(long)]
    pub verify_only: bool,
}

#[derive(Debug, Clone, Args)]
pub struct CacheCommonCommand {
    #[arg(long, default_value = super::DEFAULT_SERVER_CONFIG, help = "Datalens server config path")]
    pub config: String,

    #[arg(long)]
    pub chain: String,

    #[arg(long = "dataset")]
    pub datasets: Vec<String>,

    #[arg(long)]
    pub range_kind: String,

    #[arg(long)]
    pub range_start: u64,

    #[arg(long)]
    pub range_end: u64,

    #[arg(long)]
    pub application: String,

    #[arg(long)]
    pub finality: Option<String>,

    #[arg(long)]
    pub json: bool,

    #[arg(long = "address")]
    pub addresses: Vec<String>,

    #[arg(long = "account")]
    pub account: Option<String>,

    #[arg(long = "program-id")]
    pub program_id: Option<String>,

    #[arg(long = "signature")]
    pub signature: Option<String>,

    #[arg(long = "topic")]
    pub topics: Vec<String>,

    #[arg(long = "topic0-any-of")]
    pub topic0_any_of: Vec<String>,

    #[arg(long = "selector-json")]
    pub selector_json: Option<String>,

    #[arg(long = "event-name")]
    pub event_names: Vec<String>,
}

pub fn cache_command(
    command: CacheCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = cache_common(&command.command).json;
    let summary = cache_summary(command.command)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!(
            "cache {} {}: chunks planned={}, written={}, rows={}",
            summary["mode"].as_str().unwrap_or("run"),
            summary["status"].as_str().unwrap_or("unknown"),
            summary["accounting"]["chunks_planned"]
                .as_u64()
                .unwrap_or(0),
            summary["accounting"]["chunks_written"]
                .as_u64()
                .unwrap_or(0),
            summary["accounting"]["rows_written"].as_u64().unwrap_or(0),
        );
    }
    Ok(())
}

pub fn cache_summary(command: CacheWorkflowCommand) -> Result<serde_json::Value, DatalensError> {
    let common = cache_common(&command);
    let config = load_config(&common.config)?;
    validate_config(&config)?;
    let (chain_name, chain) = configured_chain(&config, &common.chain)?;
    let chain_name = chain_name.to_owned();
    let chain = chain.clone();
    match chain.kind.as_str() {
        "evm" => {
            let adapter = EvmRpcClient::with_chain(
                chain.rpc_urls.clone(),
                chain_identity(&chain_name, &chain)?,
                evm_finality_policy(&chain.finality),
                chain.datasets.blocks.max_batch_blocks,
                chain.datasets.logs.max_get_logs_range_blocks,
                chain.datasets.logs.max_block_scan_range_blocks,
                chain.datasets.logs.max_addresses_per_query,
            )
            .with_logs_query_strategy(chain.datasets.logs.query_strategy)
            .with_block_header_metadata_config(evm_block_header_metadata_config(&chain)?);
            cache_summary_with_context(command, config, &chain_name, &chain, adapter)
        }
        "solana" => {
            let url = chain.rpc_urls.first().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("chain {chain_name} must define at least one rpc URL"),
                )
            })?;
            let adapter = SolanaAdapter::with_provider(
                chain_identity(&chain_name, &chain)?,
                SolanaHttpRpc::new(url.clone()),
            )
            .with_max_slot_range_len(chain.datasets.blocks.max_batch_blocks.max(1))
            .with_query_strategy(chain.datasets.logs.query_strategy);
            cache_summary_with_context(command, config, &chain_name, &chain, adapter)
        }
        "tron" => {
            let url = chain.rpc_urls.first().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("chain {chain_name} must define at least one rpc URL"),
                )
            })?;
            let adapter = TronAdapter::with_provider(
                chain_identity(&chain_name, &chain)?,
                tron_provider(url.clone(), &chain),
            )
            .with_max_block_range_len(chain.datasets.blocks.max_batch_blocks.max(1))
            .with_events_query_strategy(chain.datasets.logs.query_strategy);
            cache_summary_with_context(command, config, &chain_name, &chain, adapter)
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "only evm, solana, and tron chains are supported",
        )),
    }
}
pub fn cache_summary_with_adapter<A>(
    command: CacheWorkflowCommand,
    adapter: A,
) -> Result<serde_json::Value, DatalensError>
where
    A: ChainAdapter,
{
    let common = cache_common(&command);
    let config = load_config(&common.config)?;
    validate_config(&config)?;
    let (chain_name, chain) = configured_chain(&config, &common.chain)?;
    let chain_name = chain_name.to_owned();
    let chain = chain.clone();
    cache_summary_with_context(command, config, &chain_name, &chain, adapter)
}

fn cache_summary_with_context<A>(
    command: CacheWorkflowCommand,
    config: DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
    adapter: A,
) -> Result<serde_json::Value, DatalensError>
where
    A: ChainAdapter,
{
    let common = cache_common(&command);
    let dry_run = cache_dry_run(&command);
    let job = cache_job(&config, chain_name, chain, &command)?;
    let storage: Arc<dyn StorageRepository> = Arc::from(build_storage(&config)?);
    let covered_ranges = covered_ranges(storage.clone(), &job)?;
    let finality_boundary = match job.finality_requirement {
        IndexFinalityRequirement::Safe => adapter.cache_safe_height()?,
        IndexFinalityRequirement::Finalized => adapter.finalized_height()?,
    };
    let selected_datasets = selected_datasets(&job)?;
    let plan = IndexPlan::try_new(
        job.clone(),
        finality_boundary,
        provider_limits(
            &adapter,
            selected_datasets,
            job.runtime_config.max_chunk_len,
        ),
        covered_ranges,
    )?;

    if dry_run {
        return Ok(plan_summary(
            &common.config,
            chain_name,
            &job,
            &plan,
            &config,
            true,
        ));
    }

    let runtime = IndexRuntime::new(
        adapter,
        storage,
        FileIndexCursorStore::new(&config.index.cursor_path),
        DurableWriterConfig {
            target_object_bytes: config.writer.target_object_bytes,
            min_object_rows: config.writer.min_object_rows,
            record_empty_coverage: config.writer.record_empty_coverage,
            staging: datalens_writer::WriteStagingConfig {
                enabled: config.writer.staging.enabled,
                min_rows: config.writer.staging.min_rows,
                target_object_bytes: config.writer.staging.target_object_bytes,
                max_staged_ranges: config.writer.staging.max_staged_ranges,
                max_staged_rows: config.writer.staging.max_staged_rows,
                max_staged_age_ms: config.writer.staging.max_staged_age_ms,
                flush_on_shutdown: config.writer.staging.flush_on_shutdown,
                max_staged_bytes: config.writer.staging.max_staged_bytes,
            },
        },
    )
    .with_usage_ledger(build_usage_ledger(&config)?);
    let result = runtime.run(job)?;
    Ok(run_summary(
        &common.config,
        chain_name,
        &config,
        &result,
        matches!(command, CacheWorkflowCommand::Verify(_)),
    ))
}

fn cache_job(
    config: &DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
    command: &CacheWorkflowCommand,
) -> Result<IndexJob, DatalensError> {
    let common = cache_common(command);
    let chain_identity = chain_identity(chain_name, chain)?;
    let datasets = if common.datasets.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache requires at least one --dataset",
        ));
    } else {
        common
            .datasets
            .iter()
            .map(|dataset| {
                Ok(IndexDatasetRequest {
                    dataset_key: dataset_key(chain, dataset)?,
                    selector: dataset_selector(chain, dataset, common)?,
                })
            })
            .collect::<Result<Vec<_>, DatalensError>>()?
    };
    let finality = common
        .finality
        .as_deref()
        .unwrap_or(&config.index.default_finality);
    let finality_requirement = match finality {
        "safe" => IndexFinalityRequirement::Safe,
        "finalized" => IndexFinalityRequirement::Finalized,
        _ => {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "cache finality must be safe or finalized",
            ));
        }
    };
    let mode = cache_run_mode(command);
    let range = LedgerRange::try_new(
        range_kind(&common.range_kind)?,
        common.range_start,
        common.range_end,
    )?;
    Ok(IndexJob {
        id: cache_job_id(&chain_identity, &datasets, &range, mode)?,
        application: ApplicationIdentity::named(normalize_application_id(&common.application)?),
        chain: chain_identity,
        range,
        dataset_selection: IndexDatasetSelection::Selected(datasets),
        finality_requirement,
        runtime_config: IndexRuntimeConfig {
            max_chunk_len: config.index.default_chunk_range,
        },
        run_mode: mode,
        retry_policy: IndexRetryPolicy {
            max_attempts: config.index.retry.max_attempts,
            initial_backoff_ms: config.index.retry.initial_backoff_ms,
            max_backoff_ms: config.index.retry.max_backoff_ms,
        },
    })
}

fn cache_job_id(
    chain: &ChainIdentity,
    datasets: &[IndexDatasetRequest],
    range: &LedgerRange,
    mode: IndexRunMode,
) -> Result<IndexJobId, DatalensError> {
    let datasets = datasets
        .iter()
        .map(|dataset| dataset.dataset_key.as_str().replace('.', "-"))
        .collect::<Vec<_>>()
        .join("-");
    IndexJobId::new(format!(
        "index-{}-{}-{}-{}-{}",
        cache_mode_name(mode),
        chain.configured_name(),
        datasets,
        range.start(),
        range.end()
    ))
}

fn dataset_key(chain: &ChainConfig, dataset: &str) -> Result<DatasetKey, DatalensError> {
    match (chain.kind.as_str(), dataset) {
        ("evm", "blocks") => Ok(DatasetKey::evm_blocks()),
        ("evm", "logs") => Ok(DatasetKey::evm_logs()),
        ("solana", "slots") => Ok(DatasetKey::solana_slots()),
        ("solana", "blocks") => Ok(DatasetKey::solana_blocks()),
        ("solana", "transactions") => Ok(DatasetKey::solana_transactions()),
        ("solana", "instructions") => Ok(DatasetKey::solana_instructions()),
        ("solana", "account_updates") => Ok(DatasetKey::solana_account_updates()),
        ("tron", "blocks") => Ok(DatasetKey::tron_blocks()),
        ("tron", "events") => Ok(DatasetKey::tron_events()),
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            format!(
                "dataset {dataset} is not supported for {} chains",
                chain.kind
            ),
        )),
    }
}

fn dataset_selector(
    chain: &ChainConfig,
    dataset: &str,
    common: &CacheCommonCommand,
) -> Result<DatasetSelector, DatalensError> {
    if chain.kind == "evm" && dataset == "logs" {
        return DatasetSelector::try_evm_logs(evm_log_filter(common)?);
    }
    if chain.kind == "tron" && dataset == "events" && !common.addresses.is_empty() {
        return tron_event_selector(TronEventFilter {
            contract_addresses: common.addresses.clone(),
            event_names: common.event_names.clone(),
        });
    }
    if chain.kind == "tron" {
        return tron_all_selector();
    }
    if chain.kind == "solana" {
        return solana_dataset_selector(dataset, common);
    }
    Ok(DatasetSelector::all())
}

fn evm_log_filter(common: &CacheCommonCommand) -> Result<LogFilter, DatalensError> {
    if let Some(path) = common.selector_json.as_deref() {
        if !common.addresses.is_empty()
            || !common.topics.is_empty()
            || !common.topic0_any_of.is_empty()
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "cache --selector-json cannot be combined with address or topic filters",
            ));
        }
        let content = std::fs::read_to_string(path).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("read selector json {path}: {error}"),
            )
        })?;
        return serde_json::from_str(&content).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("parse selector json {path}: {error}"),
            )
        });
    }

    let mut topic_slots = common
        .topics
        .iter()
        .cloned()
        .map(|topic| Some(vec![topic]))
        .collect::<Vec<_>>();
    if !common.topic0_any_of.is_empty() {
        if topic_slots.first().and_then(|slot| slot.as_ref()).is_some() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "cache --topic0-any-of cannot be combined with the first --topic slot",
            ));
        }
        if topic_slots.is_empty() {
            topic_slots.push(Some(common.topic0_any_of.clone()));
        } else {
            topic_slots[0] = Some(common.topic0_any_of.clone());
        }
    }

    Ok(LogFilter {
        addresses: common.addresses.clone(),
        topics: topic_slots,
    })
}

fn solana_dataset_selector(
    dataset: &str,
    common: &CacheCommonCommand,
) -> Result<DatasetSelector, DatalensError> {
    let address = common.account.as_ref().or_else(|| common.addresses.first());
    if common.addresses.len() > 1 || (common.account.is_some() && !common.addresses.is_empty()) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Solana index accepts only one account/address selector",
        ));
    }
    let selector_count = usize::from(common.program_id.is_some())
        + usize::from(address.is_some())
        + usize::from(common.signature.is_some());
    if selector_count > 1 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Solana index accepts only one of --program-id, --address/--account, or --signature",
        ));
    }

    match dataset {
        "slots" | "blocks" => {
            if selector_count == 0 {
                Ok(DatasetSelector::all())
            } else {
                Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "Solana blocks and slots only support all selectors",
                ))
            }
        }
        "instructions" => {
            if let Some(program_id) = &common.program_id {
                solana_program_selector(program_id)
            } else if selector_count == 0 {
                Ok(DatasetSelector::all())
            } else {
                Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "Solana instructions only support --program-id selectors",
                ))
            }
        }
        "transactions" | "account_updates" => {
            if let Some(program_id) = &common.program_id {
                solana_program_selector(program_id)
            } else if let Some(address) = address {
                solana_address_selector(address)
            } else if let Some(signature) = &common.signature {
                solana_signature_selector(signature)
            } else {
                Ok(DatasetSelector::all())
            }
        }
        _ => Ok(DatasetSelector::all()),
    }
}

fn range_kind(value: &str) -> Result<HeightRangeKind, DatalensError> {
    match value {
        "block" => Ok(LedgerRangeKind::Block),
        "slot" => Ok(LedgerRangeKind::Slot),
        "height" => Ok(LedgerRangeKind::Height),
        value if !value.trim().is_empty() => Ok(LedgerRangeKind::Other(value.to_owned())),
        _ => Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "range kind must not be empty",
        )),
    }
}

fn covered_ranges(
    storage: Arc<dyn StorageRepository>,
    job: &IndexJob,
) -> Result<Vec<LedgerRange>, DatalensError> {
    let mut covered = Vec::new();
    for dataset in selected_datasets(job)? {
        covered.extend(storage.covered_ranges(
            &job.chain,
            &dataset.dataset_key,
            &dataset.selector,
            job.range.clone(),
        )?);
    }
    Ok(covered)
}

fn provider_limits<A>(
    adapter: &A,
    datasets: &[IndexDatasetRequest],
    runtime_max_chunk_len: u64,
) -> Vec<IndexDatasetProviderLimit>
where
    A: ChainAdapter,
{
    let capabilities = adapter.capabilities();
    datasets
        .iter()
        .filter_map(|dataset| {
            capabilities
                .dataset(&dataset.dataset_key)
                .map(|capability| {
                    capability
                        .ranges()
                        .iter()
                        .map(move |range_kind| IndexDatasetProviderLimit {
                            dataset_key: dataset.dataset_key.clone(),
                            range_kind: range_kind.clone(),
                            max_range_len: capability
                                .max_range_len()
                                .unwrap_or(u64::MAX)
                                .min(runtime_max_chunk_len.max(1)),
                        })
                })
        })
        .flatten()
        .collect()
}

fn selected_datasets(job: &IndexJob) -> Result<&[IndexDatasetRequest], DatalensError> {
    match &job.dataset_selection {
        IndexDatasetSelection::Selected(datasets) if !datasets.is_empty() => Ok(datasets),
        _ => Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index dataset selection must not be empty",
        )),
    }
}

fn tron_provider(url: String, chain: &ChainConfig) -> TronHttpProvider {
    let provider = TronHttpProvider::new(url);
    if chain.trongrid.enabled {
        provider.with_trongrid(
            chain
                .trongrid
                .base_url
                .clone()
                .unwrap_or_else(|| "https://api.trongrid.io".to_owned()),
            chain.trongrid.api_key.clone(),
        )
    } else {
        provider
    }
}

fn cache_common(command: &CacheWorkflowCommand) -> &CacheCommonCommand {
    match command {
        CacheWorkflowCommand::Backfill(command) => &command.common,
        CacheWorkflowCommand::Resume(command) => &command.common,
        CacheWorkflowCommand::Repair(command) => &command.common,
        CacheWorkflowCommand::Verify(command) => &command.common,
    }
}

fn cache_dry_run(command: &CacheWorkflowCommand) -> bool {
    match command {
        CacheWorkflowCommand::Backfill(command) => command.dry_run,
        CacheWorkflowCommand::Resume(command) => command.dry_run,
        CacheWorkflowCommand::Repair(command) => command.dry_run,
        CacheWorkflowCommand::Verify(_) => false,
    }
}

fn cache_run_mode(command: &CacheWorkflowCommand) -> IndexRunMode {
    match command {
        CacheWorkflowCommand::Backfill(_) => IndexRunMode::Backfill,
        CacheWorkflowCommand::Resume(_) => IndexRunMode::Resume,
        CacheWorkflowCommand::Repair(_) => IndexRunMode::Repair,
        CacheWorkflowCommand::Verify(_) => IndexRunMode::Verify,
    }
}

fn cache_mode_name(mode: IndexRunMode) -> &'static str {
    match mode {
        IndexRunMode::Backfill => "backfill",
        IndexRunMode::Resume => "resume",
        IndexRunMode::Repair => "repair",
        IndexRunMode::Verify => "verify",
    }
}
