use datalens_chain::{FinalityLevel, HeightRangeKind, SelectorKind};
use datalens_core::{LedgerRange, LedgerRangeKind};
use datalens_runtime_indexer::{
    IndexAccounting, IndexChunk, IndexFinalityRequirement, IndexJob, IndexRunMode, IndexRunResult,
    IndexSkippedRange, IndexVerificationRange,
};

use crate::DatalensConfig;

pub(super) fn plan_summary(
    config_path: &str,
    chain_name: &str,
    job: &IndexJob,
    plan: &datalens_runtime_indexer::IndexPlan,
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

pub(super) fn run_summary(
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
    match &job.dataset_selection {
        datalens_runtime_indexer::IndexDatasetSelection::Selected(datasets) => datasets
            .iter()
            .map(|dataset| {
                serde_json::json!({
                    "key": dataset.dataset_key.as_str(),
                    "selector_kind": selector_kind_name(dataset.selector.kind()),
                    "selector_fingerprint": dataset.selector.fingerprint(),
                    "selector_canonical_key": dataset.selector.canonical_key(),
                })
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn range_summary(range: &LedgerRange) -> serde_json::Value {
    serde_json::json!({
        "kind": range_kind_name(range.kind()),
        "start": range.start(),
        "end": range.end(),
    })
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
