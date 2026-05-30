use std::{fs, sync::Arc};

use clap::{Args, Subcommand};
use datalens_chain::{ChainAdapter, DatasetSelector, FinalityLevel, HeightRangeKind, SelectorKind};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange, LedgerRangeKind,
    LogFilter,
};
use datalens_edge::auth::normalize_application_id;
use datalens_evm::EvmRpcClient;
use datalens_indexer::{
    CheckpointPolicy, DatalensIndexConfig, FinalityRequirement, IndexDataset, IndexPlanBuilder,
    OutputConfig, SourceConfig,
};
use datalens_metrics::ApplicationIdentity;
use datalens_runtime_indexer::{
    FileIndexCursorStore, IndexAccounting, IndexChunk, IndexDatasetProviderLimit,
    IndexDatasetRequest, IndexDatasetSelection, IndexFinalityRequirement, IndexJob, IndexJobId,
    IndexPlan, IndexRetryPolicy, IndexRunMode, IndexRunResult, IndexRuntime, IndexRuntimeConfig,
    IndexSkippedRange, IndexVerificationRange,
};
use datalens_solana::{
    SolanaAdapter, SolanaHttpRpc, solana_address_selector, solana_program_selector,
    solana_signature_selector,
};
use datalens_storage::StorageRepository;
use datalens_tron::{TronAdapter, TronEventFilter, TronHttpProvider, tron_event_selector};
use datalens_writer::DurableWriterConfig;

use crate::{
    ChainConfig, DatalensConfig, build_storage, build_usage_ledger, chain_identity,
    configured_chain, evm_finality_policy, load_config, validate_config,
};

#[derive(Debug, Args)]
pub struct IndexCommand {
    #[command(subcommand)]
    pub command: IndexWorkflowCommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum IndexWorkflowCommand {
    Plan(IndexPlanCommand),
    Backfill(IndexBackfillCommand),
    Resume(IndexResumeCommand),
    Repair(IndexRepairCommand),
    Verify(IndexVerifyCommand),
    Doctor(IndexDoctorCommand),
}

pub type IndexSubcommand = IndexWorkflowCommand;

#[derive(Debug, Clone, Args)]
pub struct IndexPlanCommand {
    #[arg(long, default_value = "app.index.toml")]
    pub config: String,
}

#[derive(Debug, Clone, Args)]
pub struct IndexBackfillCommand {
    #[command(flatten)]
    pub common: IndexCommonCommand,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct IndexResumeCommand {
    #[command(flatten)]
    pub common: IndexCommonCommand,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct IndexRepairCommand {
    #[command(flatten)]
    pub common: IndexCommonCommand,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Args)]
pub struct IndexVerifyCommand {
    #[command(flatten)]
    pub common: IndexCommonCommand,

    #[arg(long)]
    pub verify_only: bool,
}

#[derive(Debug, Clone, Args)]
pub struct IndexDoctorCommand {
    #[arg(long)]
    pub config: String,
}

#[derive(Debug, Clone, Args)]
pub struct IndexCommonCommand {
    #[arg(long, default_value = "datalens.toml")]
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

    #[arg(long = "event-name")]
    pub event_names: Vec<String>,
}

pub fn index_command(
    command: IndexCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let IndexWorkflowCommand::Plan(command) = command.command {
        let plan = index_plan(command)?;
        println!("{}", serde_json::to_string_pretty(&plan)?);
        return Ok(());
    }

    if let IndexWorkflowCommand::Doctor(command) = command.command {
        let summary = index_doctor_summary(&command)?;
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }

    let json = index_common(&command.command).json;
    let summary = index_summary(command.command)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!(
            "index {} {}: chunks planned={}, written={}, rows={}",
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

fn index_plan(
    command: IndexPlanCommand,
) -> Result<datalens_indexer::IndexPlan, Box<dyn std::error::Error + Send + Sync>> {
    let input = fs::read_to_string(&command.config)?;
    let config = DatalensIndexConfig::from_toml_str(&input)?;
    Ok(IndexPlanBuilder::new().build(&config)?)
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
        OutputConfig::Jsonl { path } => serde_json::json!({
            "kind": "jsonl",
            "path": path.to_string_lossy(),
        }),
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

pub fn index_summary(command: IndexWorkflowCommand) -> Result<serde_json::Value, DatalensError> {
    let common = index_common(&command);
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
                chain.datasets.logs.max_addresses_per_query,
            );
            index_summary_with_context(command, config, &chain_name, &chain, adapter)
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
            .with_max_slot_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
            index_summary_with_context(command, config, &chain_name, &chain, adapter)
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
            .with_max_block_range_len(chain.datasets.blocks.max_batch_blocks.max(1));
            index_summary_with_context(command, config, &chain_name, &chain, adapter)
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "only evm, solana, and tron chains are supported",
        )),
    }
}

pub fn index_summary_with_adapter<A>(
    command: IndexWorkflowCommand,
    adapter: A,
) -> Result<serde_json::Value, DatalensError>
where
    A: ChainAdapter,
{
    let common = index_common(&command);
    let config = load_config(&common.config)?;
    validate_config(&config)?;
    let (chain_name, chain) = configured_chain(&config, &common.chain)?;
    let chain_name = chain_name.to_owned();
    let chain = chain.clone();
    index_summary_with_context(command, config, &chain_name, &chain, adapter)
}

fn index_summary_with_context<A>(
    command: IndexWorkflowCommand,
    config: DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
    adapter: A,
) -> Result<serde_json::Value, DatalensError>
where
    A: ChainAdapter,
{
    let common = index_common(&command);
    let dry_run = index_dry_run(&command);
    let job = index_job(&config, chain_name, chain, &command)?;
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
        matches!(command, IndexWorkflowCommand::Verify(_)),
    ))
}

fn index_job(
    config: &DatalensConfig,
    chain_name: &str,
    chain: &ChainConfig,
    command: &IndexWorkflowCommand,
) -> Result<IndexJob, DatalensError> {
    let common = index_common(command);
    let chain_identity = chain_identity(chain_name, chain)?;
    let datasets = if common.datasets.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index requires at least one --dataset",
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
                "index finality must be safe or finalized",
            ));
        }
    };
    let mode = index_run_mode(command);
    let range = LedgerRange::try_new(
        range_kind(&common.range_kind)?,
        common.range_start,
        common.range_end,
    )?;
    Ok(IndexJob {
        id: index_job_id(&chain_identity, &datasets, &range, mode)?,
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

fn index_job_id(
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
        index_mode_name(mode),
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
    common: &IndexCommonCommand,
) -> Result<DatasetSelector, DatalensError> {
    if chain.kind == "evm" && dataset == "logs" {
        return DatasetSelector::try_evm_logs(LogFilter {
            addresses: common.addresses.clone(),
            topics: common
                .topics
                .iter()
                .cloned()
                .map(|topic| Some(vec![topic]))
                .collect(),
        });
    }
    if chain.kind == "tron" && dataset == "events" && !common.addresses.is_empty() {
        return tron_event_selector(TronEventFilter {
            contract_addresses: common.addresses.clone(),
            event_names: common.event_names.clone(),
        });
    }
    if chain.kind == "solana" {
        return solana_dataset_selector(dataset, common);
    }
    Ok(DatasetSelector::all())
}

fn solana_dataset_selector(
    dataset: &str,
    common: &IndexCommonCommand,
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

fn plan_summary(
    config_path: &str,
    chain_name: &str,
    job: &IndexJob,
    plan: &IndexPlan,
    config: &DatalensConfig,
    dry_run: bool,
) -> serde_json::Value {
    serde_json::json!({
        "status": "planned",
        "mode": index_mode_name(job.run_mode),
        "dry_run": dry_run,
        "read_only": dry_run || job.run_mode == IndexRunMode::Verify,
        "config": config_path,
        "chain": chain_name,
        "application": job.application.as_str(),
        "cursor_path": config.index.cursor_path,
        "concurrency": config.index.max_concurrency,
        "finality": {
            "requirement": finality_requirement_name(job.finality_requirement),
            "durable": finality_level_name(plan.durable_finality),
            "boundary": {
                "kind": range_kind_name(plan.finality_boundary.range_kind.clone()),
                "height": plan.finality_boundary.value,
            },
        },
        "range": range_summary(&job.range),
        "datasets": selected_dataset_summary(job),
        "plan": {
            "planned_range": range_summary(&plan.planned_range),
            "chunk_count": plan.chunks.len(),
            "chunks": plan.chunks.iter().map(chunk_summary).collect::<Vec<_>>(),
            "skipped_ranges": plan.skipped_ranges.iter().map(skipped_summary).collect::<Vec<_>>(),
            "verification_ranges": plan.verification_ranges.iter().map(verification_summary).collect::<Vec<_>>(),
        },
        "accounting": {
            "chunks_planned": plan.chunks.len(),
            "chunks_fetched": 0,
            "chunks_skipped": plan.skipped_ranges.len(),
            "chunks_written": 0,
            "rows_written": 0,
        },
    })
}

fn run_summary(
    config_path: &str,
    chain_name: &str,
    config: &DatalensConfig,
    result: &IndexRunResult,
    read_only: bool,
) -> serde_json::Value {
    serde_json::json!({
        "status": index_status_name(result.status),
        "mode": index_mode_name(result.mode),
        "dry_run": false,
        "read_only": read_only,
        "config": config_path,
        "chain": chain_name,
        "cursor_path": config.index.cursor_path,
        "concurrency": config.index.max_concurrency,
        "job_id": result.job_id.as_str(),
        "accounting": accounting_summary(&result.accounting),
        "checkpoints": result.checkpoints.iter().map(|checkpoint| {
            serde_json::json!({
                "dataset": checkpoint.chunk.dataset_key.as_str(),
                "range": range_summary(&checkpoint.chunk.range),
                "provider_calls": checkpoint.provider_calls,
                "attempts": checkpoint.attempts,
                "durable_write": checkpoint.durable_write.as_ref().map(|write| {
                    serde_json::json!({
                        "finality": finality_level_name(write.finality_level),
                        "data_objects": write.data_objects,
                        "empty_coverages": write.empty_coverages,
                        "rows_written": write.rows_written,
                    })
                }),
            })
        }).collect::<Vec<_>>(),
    })
}

fn accounting_summary(accounting: &IndexAccounting) -> serde_json::Value {
    serde_json::json!({
        "chunks_planned": accounting.chunks_planned,
        "chunks_fetched": accounting.chunks_fetched,
        "chunks_skipped": accounting.chunks_skipped,
        "chunks_written": accounting.chunks_written,
        "chunks_failed": accounting.chunks_failed,
        "provider_limit_splits": accounting.provider_limit_splits,
        "finality_capped_ranges": accounting.finality_capped_ranges,
        "provider_calls": accounting.provider_calls,
        "rows_written": accounting.rows_written,
        "skipped_ranges": accounting.skipped_ranges,
        "retries": accounting.retries,
        "failures": accounting.failures,
    })
}

fn chunk_summary(chunk: &IndexChunk) -> serde_json::Value {
    serde_json::json!({
        "ordinal": chunk.ordinal,
        "dataset": chunk.dataset_key.as_str(),
        "range": range_summary(&chunk.range),
    })
}

fn skipped_summary(skipped: &IndexSkippedRange) -> serde_json::Value {
    serde_json::json!({
        "dataset": skipped.dataset_key.as_str(),
        "range": range_summary(&skipped.range),
        "reason": "covered_by_manifest",
    })
}

fn verification_summary(range: &IndexVerificationRange) -> serde_json::Value {
    serde_json::json!({
        "dataset": range.dataset_key.as_str(),
        "range": range_summary(&range.range),
    })
}

fn selected_dataset_summary(job: &IndexJob) -> Vec<serde_json::Value> {
    selected_datasets(job)
        .unwrap_or(&[])
        .iter()
        .map(|dataset| {
            serde_json::json!({
                "key": dataset.dataset_key.as_str(),
                "selector_kind": selector_kind_name(dataset.selector.kind()),
                "selector_fingerprint": dataset.selector.fingerprint(),
                "selector_canonical_key": dataset.selector.canonical_key(),
            })
        })
        .collect()
}

fn range_summary(range: &LedgerRange) -> serde_json::Value {
    serde_json::json!({
        "kind": range_kind_name(range.kind()),
        "start": range.start(),
        "end": range.end(),
    })
}

fn index_common(command: &IndexWorkflowCommand) -> &IndexCommonCommand {
    match command {
        IndexWorkflowCommand::Backfill(command) => &command.common,
        IndexWorkflowCommand::Resume(command) => &command.common,
        IndexWorkflowCommand::Repair(command) => &command.common,
        IndexWorkflowCommand::Verify(command) => &command.common,
        IndexWorkflowCommand::Plan(_) => {
            unreachable!("index plan does not use runtime index common options")
        }
        IndexWorkflowCommand::Doctor(_) => {
            unreachable!("index doctor does not use runtime index common options")
        }
    }
}

fn index_dry_run(command: &IndexWorkflowCommand) -> bool {
    match command {
        IndexWorkflowCommand::Backfill(command) => command.dry_run,
        IndexWorkflowCommand::Resume(command) => command.dry_run,
        IndexWorkflowCommand::Repair(command) => command.dry_run,
        IndexWorkflowCommand::Verify(_) => false,
        IndexWorkflowCommand::Plan(_) => {
            unreachable!("index plan does not use runtime index dry-run options")
        }
        IndexWorkflowCommand::Doctor(_) => {
            unreachable!("index doctor does not use runtime index dry-run options")
        }
    }
}

fn index_run_mode(command: &IndexWorkflowCommand) -> IndexRunMode {
    match command {
        IndexWorkflowCommand::Backfill(_) => IndexRunMode::Backfill,
        IndexWorkflowCommand::Resume(_) => IndexRunMode::Resume,
        IndexWorkflowCommand::Repair(_) => IndexRunMode::Repair,
        IndexWorkflowCommand::Verify(_) => IndexRunMode::Verify,
        IndexWorkflowCommand::Plan(_) => {
            unreachable!("index plan does not use runtime index run modes")
        }
        IndexWorkflowCommand::Doctor(_) => {
            unreachable!("index doctor does not use runtime index run modes")
        }
    }
}

fn index_mode_name(mode: IndexRunMode) -> &'static str {
    match mode {
        IndexRunMode::Backfill => "backfill",
        IndexRunMode::Resume => "resume",
        IndexRunMode::Repair => "repair",
        IndexRunMode::Verify => "verify",
    }
}

fn index_status_name(status: datalens_runtime_indexer::IndexRunStatus) -> &'static str {
    match status {
        datalens_runtime_indexer::IndexRunStatus::Completed => "completed",
        datalens_runtime_indexer::IndexRunStatus::Partial => "partial",
        datalens_runtime_indexer::IndexRunStatus::Failed => "failed",
    }
}

fn finality_requirement_name(finality: IndexFinalityRequirement) -> &'static str {
    match finality {
        IndexFinalityRequirement::Safe => "safe",
        IndexFinalityRequirement::Finalized => "finalized",
    }
}

fn finality_level_name(finality: FinalityLevel) -> &'static str {
    match finality {
        FinalityLevel::Latest => "latest",
        FinalityLevel::Safe => "safe",
        FinalityLevel::Finalized => "finalized",
        FinalityLevel::ChainSpecific(value) => value,
    }
}

fn range_kind_name(kind: HeightRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

fn selector_kind_name(kind: SelectorKind) -> String {
    match kind {
        SelectorKind::All => "all".to_owned(),
        SelectorKind::EvmLogs => "evm_logs".to_owned(),
        SelectorKind::Other(value) => value.as_str().to_owned(),
    }
}
