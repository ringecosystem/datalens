use datalens_chain::{AdapterCapabilities, ChainFetchRequest, ChainHeight, FinalityLevel};
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

pub(crate) fn dataset_capability_max_range_len(
    capabilities: &AdapterCapabilities,
    dataset_key: &DatasetKey,
) -> Option<u64> {
    capabilities
        .dataset(dataset_key)
        .and_then(|capability| capability.max_range_len())
}

pub(crate) fn chunk_ledger_range_by_max_len(
    range: &LedgerRange,
    max_len: u64,
) -> Result<Vec<LedgerRange>, DatalensError> {
    if max_len == 0 {
        return Err(DatalensError::invalid_input(
            "provider limit max_len must be greater than zero",
        ));
    }
    range.split(max_len)
}

pub(crate) fn split_fetch_request_by_max_len(
    fetch_request: &ChainFetchRequest,
    max_len: Option<u64>,
) -> Result<Vec<ChainFetchRequest>, DatalensError> {
    let Some(max_len) = max_len else {
        return Ok(vec![fetch_request.clone()]);
    };
    chunk_ledger_range_by_max_len(&fetch_request.range, max_len).map(|ranges| {
        ranges
            .into_iter()
            .map(|range| ChainFetchRequest {
                range,
                ..fetch_request.clone()
            })
            .collect()
    })
}

pub(crate) fn provider_limit_split_target(
    configured_max_len: Option<u64>,
    hint_max_len: Option<u64>,
) -> Option<u64> {
    match (configured_max_len, hint_max_len) {
        (Some(configured), Some(hint)) => Some(configured.min(hint)),
        (Some(configured), None) => Some(configured),
        (None, Some(hint)) => Some(hint),
        (None, None) => None,
    }
}

pub(crate) fn parse_provider_limit_hint(message: &str) -> Option<u64> {
    let normalized = message.to_ascii_lowercase();
    parse_number_after_marker(&normalized, "filter:")
        .or_else(|| parse_number_after_marker(&normalized, "limit:"))
        .or_else(|| parse_number_after_limit_word(&normalized))
}

pub(crate) fn split_provider_limit_range(
    range: &LedgerRange,
    target_max_len: Option<u64>,
) -> Result<Vec<LedgerRange>, DatalensError> {
    if let Some(max_len) = target_max_len
        && (max_len == 0 || range.len() > u128::from(max_len))
    {
        return chunk_ledger_range_by_max_len(range, max_len);
    }
    bisect_provider_limit_range(range)
}

fn bisect_provider_limit_range(range: &LedgerRange) -> Result<Vec<LedgerRange>, DatalensError> {
    let first_len = u64::try_from(range.len() / 2).unwrap_or(u64::MAX).max(1);
    let first_end = range.start().saturating_add(first_len - 1);
    Ok(vec![
        LedgerRange::try_new(range.kind(), range.start(), first_end)?,
        LedgerRange::try_new(range.kind(), first_end + 1, range.end())?,
    ])
}

fn parse_number_after_marker(message: &str, marker: &str) -> Option<u64> {
    let start = message.find(marker)? + marker.len();
    parse_positive_u64_at(message, start)
}

fn parse_number_after_limit_word(message: &str) -> Option<u64> {
    let mut search_start = 0;
    while let Some(relative) = message[search_start..].find("limit") {
        let start = search_start + relative;
        let end = start + "limit".len();
        if is_word_boundary(message, start, end)
            && message[end..]
                .chars()
                .next()
                .is_none_or(|ch| ch.is_ascii_whitespace() || ch == ':' || ch == ',')
        {
            let scan_end = message.len().min(end + 48);
            if let Some(offset) = message[end..scan_end]
                .char_indices()
                .find_map(|(index, ch)| ch.is_ascii_digit().then_some(end + index))
                && let Some(value) = parse_positive_u64_at(message, offset)
            {
                return Some(value);
            }
        }
        search_start = end;
    }
    None
}

fn parse_positive_u64_at(message: &str, mut start: usize) -> Option<u64> {
    while let Some(ch) = message[start..].chars().next() {
        if !ch.is_ascii_whitespace() {
            break;
        }
        start += ch.len_utf8();
    }
    let mut end = start;
    while let Some(ch) = message[end..].chars().next() {
        if !ch.is_ascii_digit() {
            break;
        }
        end += ch.len_utf8();
    }
    if end == start {
        return None;
    }
    message[start..end]
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
}

fn is_word_boundary(message: &str, start: usize, end: usize) -> bool {
    let before = message[..start].chars().next_back();
    let after = message[end..].chars().next();
    !before.is_some_and(is_word_char) && !after.is_some_and(is_word_char)
}

fn is_word_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalens_core::DatalensErrorKind;

    #[test]
    fn test_chunk_ledger_range_by_max_len_splits_5000_into_1000_chunks() {
        let range = LedgerRange::blocks(1, 5_000).expect("valid range");

        let chunks = chunk_ledger_range_by_max_len(&range, 1_000).expect("chunk range");

        assert_eq!(chunks.len(), 5);
        assert!(chunks.iter().all(|chunk| chunk.len() <= 1_000));
        assert_eq!(chunks[0], LedgerRange::blocks(1, 1_000).unwrap());
        assert_eq!(chunks[4], LedgerRange::blocks(4_001, 5_000).unwrap());
    }

    #[test]
    fn test_chunk_ledger_range_by_max_len_rejects_zero() {
        let range = LedgerRange::blocks(1, 1).expect("valid range");

        let error = chunk_ledger_range_by_max_len(&range, 0).expect_err("zero is invalid");

        assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    }

    #[test]
    fn test_parse_provider_limit_hint_uses_conservative_patterns() {
        assert_eq!(
            parse_provider_limit_hint(
                "query block range exceeds server limit, narrow your filter: 1000"
            ),
            Some(1_000)
        );
        assert_eq!(
            parse_provider_limit_hint("provider limit: 2500"),
            Some(2_500)
        );
        assert_eq!(
            parse_provider_limit_hint("provider limit 1250 blocks"),
            Some(1_250)
        );
        assert_eq!(
            parse_provider_limit_hint("upstream url https://example.test/limit/999 hash 0xabc1000"),
            None
        );
    }
}
