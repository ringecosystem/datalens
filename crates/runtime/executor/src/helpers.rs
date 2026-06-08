use datalens_chain::{ChainHeight, FinalityLevel};
use datalens_core::{DatalensError, Dataset, DatasetKey, LedgerRange, QueryRows};
use datalens_metrics::{
    CacheCoverageOutcome, DurableWriteOutcome as MetricsDurableWriteOutcome, QueryOutcome,
};
use datalens_storage::{
    CacheOutcome as LedgerCacheOutcome, FillOutcome as LedgerFillOutcome, HotCacheCandidateStatus,
    HotCacheEntryMetadata, HotCacheFinalityStatus, QueryOutcome as LedgerQueryOutcome,
};
use datalens_writer::DurableWriteResult;

use crate::{HotPromotionRequest, NativeQueryExecutionResult};

pub(crate) fn eligible_for_promotion(
    entry: &HotCacheEntryMetadata,
    request: &HotPromotionRequest,
    boundary: &ChainHeight,
) -> bool {
    if entry.candidate_status != HotCacheCandidateStatus::Active
        || !entry.eligible_for_promotion
        || entry.promoted_at_unix_seconds.is_some()
    {
        return false;
    }
    if !matches!(
        entry.finality_status,
        HotCacheFinalityStatus::Safe | HotCacheFinalityStatus::Finalized
    ) {
        return false;
    }
    let Some(entry_range) = entry.range.as_ref() else {
        return false;
    };
    entry.chain.as_ref() == Some(&request.chain)
        && entry.dataset_key.as_ref() == Some(&request.dataset_key)
        && entry_range.kind() == boundary.range_kind
        && entry_range.end() <= boundary.value
        && entry_range.intersection(&request.range).is_some()
}

pub(crate) fn promoted_row_count(result: &DurableWriteResult) -> usize {
    result
        .data_objects
        .iter()
        .map(|object| object.row_count)
        .sum()
}

pub(crate) fn unix_seconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::internal(format!("system clock before unix epoch: {error}"))
        })
}

pub(crate) fn ledger_cache_outcome(outcome: CacheCoverageOutcome) -> LedgerCacheOutcome {
    match outcome {
        CacheCoverageOutcome::Hit => LedgerCacheOutcome::Hit,
        CacheCoverageOutcome::HotHit => LedgerCacheOutcome::HotHit,
        CacheCoverageOutcome::Miss => LedgerCacheOutcome::Miss,
        CacheCoverageOutcome::HotMiss => LedgerCacheOutcome::HotMiss,
        CacheCoverageOutcome::PartialHit => LedgerCacheOutcome::PartialHit,
        CacheCoverageOutcome::Mixed => LedgerCacheOutcome::Mixed,
        CacheCoverageOutcome::Empty => LedgerCacheOutcome::Empty,
        CacheCoverageOutcome::Error => LedgerCacheOutcome::Error,
    }
}

pub(crate) fn ledger_query_outcome(outcome: QueryOutcome) -> LedgerQueryOutcome {
    match outcome {
        QueryOutcome::Hit => LedgerQueryOutcome::Hit,
        QueryOutcome::HotHit => LedgerQueryOutcome::HotHit,
        QueryOutcome::Miss => LedgerQueryOutcome::Miss,
        QueryOutcome::HotMiss => LedgerQueryOutcome::HotMiss,
        QueryOutcome::PartialHit => LedgerQueryOutcome::PartialHit,
        QueryOutcome::Mixed => LedgerQueryOutcome::Mixed,
        QueryOutcome::Filled => LedgerQueryOutcome::Filled,
        QueryOutcome::Empty => LedgerQueryOutcome::Empty,
        QueryOutcome::ReorgRollback => LedgerQueryOutcome::ReorgRollback,
        QueryOutcome::PromotionCompleted => LedgerQueryOutcome::PromotionCompleted,
        QueryOutcome::PromotionSkipped => LedgerQueryOutcome::PromotionSkipped,
        QueryOutcome::Error => LedgerQueryOutcome::Error,
    }
}

pub(crate) fn ledger_query_error(error: &DatalensError) -> LedgerQueryOutcome {
    if is_provider_error(&error.kind) {
        LedgerQueryOutcome::ProviderError
    } else if is_storage_error(&error.kind) {
        LedgerQueryOutcome::StorageError
    } else {
        LedgerQueryOutcome::Error
    }
}

pub(crate) fn ledger_fill_error(error: &DatalensError) -> LedgerFillOutcome {
    if is_provider_error(&error.kind) {
        LedgerFillOutcome::ProviderError
    } else if is_storage_error(&error.kind) {
        LedgerFillOutcome::StorageError
    } else {
        LedgerFillOutcome::Error
    }
}

pub(crate) fn ledger_fill_outcome(
    provider_fetch_attempted: bool,
    _fill_row_count: usize,
) -> LedgerFillOutcome {
    if !provider_fetch_attempted {
        LedgerFillOutcome::NotAttempted
    } else {
        LedgerFillOutcome::LiveFetch
    }
}

pub(crate) fn metrics_durable_write_outcome(
    result: &DurableWriteResult,
) -> MetricsDurableWriteOutcome {
    if !result.data_objects.is_empty() {
        MetricsDurableWriteOutcome::Flushed
    } else if !result.empty_coverages.is_empty() {
        MetricsDurableWriteOutcome::EmptyCoverageRecorded
    } else if !result.staged_ranges.is_empty() {
        MetricsDurableWriteOutcome::Staged
    } else if !result.skipped_ranges.is_empty() {
        MetricsDurableWriteOutcome::Skipped
    } else {
        MetricsDurableWriteOutcome::NotAttempted
    }
}

pub(crate) fn coverage_outcome(
    hit_ranges: &[LedgerRange],
    miss_ranges: &[LedgerRange],
) -> CacheCoverageOutcome {
    match (hit_ranges.is_empty(), miss_ranges.is_empty()) {
        (false, true) => CacheCoverageOutcome::Hit,
        (true, false) => CacheCoverageOutcome::Miss,
        (false, false) => CacheCoverageOutcome::PartialHit,
        (true, true) => CacheCoverageOutcome::Empty,
    }
}

pub(crate) fn plan_coverage_outcome(
    plan: &datalens_planner::NativeQueryPlan,
) -> CacheCoverageOutcome {
    let has_hot_fetch = plan.fetch_tasks.iter().any(|task| !task.cache_write);
    let has_durable_work = !plan.coverage.durable_hit_ranges.is_empty()
        || plan.fetch_tasks.iter().any(|task| task.cache_write);
    if matches!(
        plan.requested_finality,
        datalens_core::QueryFinalityRequirement::SafeToLatest
    ) && has_hot_fetch
    {
        if has_durable_work {
            CacheCoverageOutcome::Mixed
        } else {
            CacheCoverageOutcome::HotMiss
        }
    } else {
        coverage_outcome(&plan.coverage.hit_ranges, &plan.coverage.missing_ranges)
    }
}

pub(crate) fn query_outcome(
    coverage_outcome: CacheCoverageOutcome,
    filled_cache: bool,
    result: &NativeQueryExecutionResult,
) -> QueryOutcome {
    match coverage_outcome {
        CacheCoverageOutcome::Hit if result.rows.row_count() == 0 => QueryOutcome::Empty,
        CacheCoverageOutcome::Hit => QueryOutcome::Hit,
        CacheCoverageOutcome::HotHit => QueryOutcome::HotHit,
        CacheCoverageOutcome::PartialHit => QueryOutcome::PartialHit,
        CacheCoverageOutcome::Mixed => QueryOutcome::Mixed,
        CacheCoverageOutcome::Miss if result.rows.row_count() == 0 => QueryOutcome::Empty,
        CacheCoverageOutcome::Miss if filled_cache => QueryOutcome::Filled,
        CacheCoverageOutcome::Miss => QueryOutcome::Miss,
        CacheCoverageOutcome::HotMiss => QueryOutcome::HotMiss,
        CacheCoverageOutcome::Empty => QueryOutcome::Empty,
        CacheCoverageOutcome::Error => QueryOutcome::Error,
    }
}

pub(crate) fn is_provider_error(kind: &datalens_core::DatalensErrorKind) -> bool {
    matches!(
        kind,
        datalens_core::DatalensErrorKind::ProviderFailure
            | datalens_core::DatalensErrorKind::ProviderLimit
            | datalens_core::DatalensErrorKind::ProviderTimeout
            | datalens_core::DatalensErrorKind::RateLimited
    )
}

pub(crate) fn is_storage_error(kind: &datalens_core::DatalensErrorKind) -> bool {
    matches!(
        kind,
        datalens_core::DatalensErrorKind::StorageReadFailure
            | datalens_core::DatalensErrorKind::StorageWriteFailure
            | datalens_core::DatalensErrorKind::ManifestUpdateFailure
    )
}

pub(crate) fn boundary_for_cached_hit(range: &LedgerRange) -> ChainHeight {
    ChainHeight {
        range_kind: range.kind(),
        value: range.end(),
        finality: FinalityLevel::Safe,
    }
}

pub(crate) fn empty_query_rows(dataset_key: &DatasetKey) -> QueryRows {
    match dataset_key.evm_dataset() {
        Some(Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(Dataset::Transactions) => QueryRows::EvmTransactions(Vec::new()),
        Some(Dataset::Receipts) => QueryRows::EvmReceipts(Vec::new()),
        Some(Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: Vec::new(),
        },
    }
}
