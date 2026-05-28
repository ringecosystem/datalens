//! Durable full-indexing runtime contract.

use datalens_chain::{ChainHeight, DatasetSelector, FinalityLevel, HeightRangeKind};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange, missing_ranges,
};
use datalens_metrics::ApplicationIdentity;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexJobId(String);

impl IndexJobId {
    pub fn new(value: impl Into<String>) -> Result<Self, DatalensError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "index job id must not be empty",
            ));
        }
        if value.contains('/') || value.contains('\\') {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "index job id must not contain path separators",
            ));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexRunMode {
    Backfill,
    Resume,
    Repair,
    Verify,
}

impl IndexRunMode {
    pub fn writes_durable_data(self) -> bool {
        matches!(self, Self::Backfill | Self::Resume | Self::Repair)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexJob {
    pub id: IndexJobId,
    pub application: ApplicationIdentity,
    pub chain: ChainIdentity,
    pub range: LedgerRange,
    pub dataset_selection: IndexDatasetSelection,
    pub run_mode: IndexRunMode,
    pub retry_policy: IndexRetryPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexDatasetSelection {
    AllSupported,
    Selected(Vec<IndexDatasetRequest>),
}

impl IndexDatasetSelection {
    fn selected(&self) -> Result<&[IndexDatasetRequest], DatalensError> {
        match self {
            Self::AllSupported => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "all-supported dataset selection must be resolved before planning",
            )),
            Self::Selected(datasets) if datasets.is_empty() => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "index dataset selection must not be empty",
            )),
            Self::Selected(datasets) => Ok(datasets),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDatasetRequest {
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDatasetProviderLimit {
    pub dataset_key: DatasetKey,
    pub range_kind: HeightRangeKind,
    pub max_range_len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for IndexRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 250,
            max_backoff_ms: 30_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexPlan {
    pub job: IndexJob,
    pub finality_boundary: ChainHeight,
    pub durable_finality: FinalityLevel,
    pub retry_policy: IndexRetryPolicy,
    pub chunks: Vec<IndexChunk>,
    pub skipped_ranges: Vec<IndexSkippedRange>,
    pub verification_ranges: Vec<IndexVerificationRange>,
}

impl IndexPlan {
    pub fn try_new(
        job: IndexJob,
        finality_boundary: ChainHeight,
        provider_limits: Vec<IndexDatasetProviderLimit>,
        covered_ranges: Vec<LedgerRange>,
    ) -> Result<Self, DatalensError> {
        finality_boundary.validate_durable_writable()?;
        validate_boundary_range(&job.range, &finality_boundary)?;

        let datasets = job.dataset_selection.selected()?;
        let mut chunks = Vec::new();
        let mut skipped_ranges = Vec::new();
        let mut verification_ranges = Vec::new();

        for dataset in datasets {
            let relevant_coverage = covered_ranges
                .iter()
                .filter_map(|range| range.intersection(&job.range))
                .collect::<Vec<_>>();

            if job.run_mode == IndexRunMode::Verify {
                verification_ranges.extend(relevant_coverage.into_iter().map(|range| {
                    IndexVerificationRange {
                        dataset_key: dataset.dataset_key.clone(),
                        range,
                    }
                }));
                continue;
            }

            skipped_ranges.extend(relevant_coverage.iter().cloned().map(|range| {
                IndexSkippedRange {
                    dataset_key: dataset.dataset_key.clone(),
                    range,
                    reason: IndexSkipReason::CoveredByManifest,
                }
            }));

            let missing = missing_ranges(job.range.clone(), &relevant_coverage);
            let max_range_len = provider_limits
                .iter()
                .find(|limit| {
                    limit.dataset_key == dataset.dataset_key && limit.range_kind == job.range.kind()
                })
                .map(|limit| limit.max_range_len)
                .unwrap_or(u64::MAX)
                .max(1);
            for range in missing {
                for range in range.split(max_range_len)? {
                    chunks.push(IndexChunk {
                        ordinal: chunks.len() as u64,
                        dataset_key: dataset.dataset_key.clone(),
                        selector: dataset.selector.clone(),
                        range,
                        retry_policy: job.retry_policy.clone(),
                    });
                }
            }
        }

        Ok(Self {
            durable_finality: finality_boundary.finality,
            retry_policy: job.retry_policy.clone(),
            job,
            finality_boundary,
            chunks,
            skipped_ranges,
            verification_ranges,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexChunk {
    pub ordinal: u64,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub range: LedgerRange,
    pub retry_policy: IndexRetryPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSkippedRange {
    pub dataset_key: DatasetKey,
    pub range: LedgerRange,
    pub reason: IndexSkipReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexSkipReason {
    CoveredByManifest,
    OutsideFinalityBoundary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexVerificationRange {
    pub dataset_key: DatasetKey,
    pub range: LedgerRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexCursor {
    pub job_id: IndexJobId,
    pub next_chunk_ordinal: u64,
    pub last_checkpointed_range: Option<LedgerRange>,
}

impl IndexCursor {
    pub fn from_checkpoint(checkpoint: &IndexCheckpoint) -> Self {
        Self {
            job_id: checkpoint.job_id.clone(),
            next_chunk_ordinal: checkpoint.chunk.ordinal + 1,
            last_checkpointed_range: Some(checkpoint.chunk.range.clone()),
        }
    }

    pub fn is_durable_coverage(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexCheckpoint {
    pub job_id: IndexJobId,
    pub chunk: IndexChunk,
    pub durable_write: Option<IndexDurableWriteSummary>,
    pub provider_calls: u64,
    pub attempts: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDurableWriteSummary {
    pub finality_level: FinalityLevel,
    pub data_objects: usize,
    pub empty_coverages: usize,
    pub rows_written: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRunResult {
    pub job_id: IndexJobId,
    pub mode: IndexRunMode,
    pub status: IndexRunStatus,
    pub checkpoints: Vec<IndexCheckpoint>,
    pub accounting: IndexAccounting,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexRunStatus {
    Completed,
    Partial,
    Failed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexAccounting {
    pub provider_calls: u64,
    pub rows_written: usize,
    pub skipped_ranges: usize,
    pub retries: u64,
    pub failures: u64,
}

fn validate_boundary_range(
    range: &LedgerRange,
    finality_boundary: &ChainHeight,
) -> Result<(), DatalensError> {
    if range.kind() != finality_boundary.range_kind {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index range kind must match finality boundary range kind",
        ));
    }
    if range.end() > finality_boundary.value {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "index range exceeds safe/finalized finality boundary",
        ));
    }
    Ok(())
}
