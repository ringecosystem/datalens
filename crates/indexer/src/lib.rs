//! Durable full-indexing runtime contract.

mod runtime;

pub use runtime::{InMemoryIndexCursorStore, IndexCursorRepository, IndexRuntime};

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
    pub finality_requirement: IndexFinalityRequirement,
    pub runtime_config: IndexRuntimeConfig,
    pub run_mode: IndexRunMode,
    pub retry_policy: IndexRetryPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexFinalityRequirement {
    Safe,
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRuntimeConfig {
    pub max_chunk_len: u64,
}

impl Default for IndexRuntimeConfig {
    fn default() -> Self {
        Self {
            max_chunk_len: 1_000,
        }
    }
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
    pub planned_range: LedgerRange,
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
        let covered_ranges = job
            .dataset_selection
            .selected()?
            .iter()
            .flat_map(|dataset| {
                covered_ranges
                    .iter()
                    .cloned()
                    .map(|range| IndexDatasetCoverage {
                        dataset_key: dataset.dataset_key.clone(),
                        selector: dataset.selector.clone(),
                        range,
                    })
            })
            .collect();
        Self::try_new_with_dataset_coverage(job, finality_boundary, provider_limits, covered_ranges)
    }

    pub fn try_new_with_dataset_coverage(
        job: IndexJob,
        finality_boundary: ChainHeight,
        provider_limits: Vec<IndexDatasetProviderLimit>,
        covered_ranges: Vec<IndexDatasetCoverage>,
    ) -> Result<Self, DatalensError> {
        finality_boundary.validate_durable_writable()?;
        if job.range.kind() != finality_boundary.range_kind {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "index range kind must match finality boundary range kind",
            ));
        }
        if job.range.start() > finality_boundary.value {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "index range starts after safe/finalized finality boundary",
            ));
        }
        let planned_range = LedgerRange::try_new(
            job.range.kind(),
            job.range.start(),
            job.range.end().min(finality_boundary.value),
        )?;

        let datasets = job.dataset_selection.selected()?;
        let mut chunks = Vec::new();
        let mut skipped_ranges = Vec::new();
        let mut verification_ranges = Vec::new();

        for dataset in datasets {
            let relevant_coverage = covered_ranges
                .iter()
                .filter(|coverage| {
                    coverage.dataset_key == dataset.dataset_key
                        && coverage.selector == dataset.selector
                })
                .map(|coverage| &coverage.range)
                .filter_map(|range| range.intersection(&planned_range))
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

            let missing = missing_ranges(planned_range.clone(), &relevant_coverage);
            let max_range_len = provider_limits
                .iter()
                .find(|limit| {
                    limit.dataset_key == dataset.dataset_key && limit.range_kind == job.range.kind()
                })
                .map(|limit| limit.max_range_len)
                .unwrap_or(u64::MAX)
                .min(job.runtime_config.max_chunk_len.max(1))
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
            planned_range,
            finality_boundary,
            chunks,
            skipped_ranges,
            verification_ranges,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDatasetCoverage {
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub range: LedgerRange,
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
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub range_kind: HeightRangeKind,
    pub next_height: u64,
    pub completed_chunks: Vec<u64>,
    pub completed_ranges: Vec<LedgerRange>,
    pub failure_state: Option<IndexFailureState>,
    pub next_chunk_ordinal: u64,
    pub last_checkpointed_range: Option<LedgerRange>,
}

impl IndexCursor {
    pub fn from_checkpoint(checkpoint: &IndexCheckpoint) -> Self {
        Self {
            job_id: checkpoint.job_id.clone(),
            chain: checkpoint.chain.clone(),
            dataset_key: checkpoint.chunk.dataset_key.clone(),
            selector: checkpoint.chunk.selector.clone(),
            range_kind: checkpoint.chunk.range.kind(),
            next_height: checkpoint.chunk.range.end().saturating_add(1),
            completed_chunks: vec![checkpoint.chunk.ordinal],
            completed_ranges: vec![checkpoint.chunk.range.clone()],
            failure_state: None,
            next_chunk_ordinal: checkpoint.chunk.ordinal + 1,
            last_checkpointed_range: Some(checkpoint.chunk.range.clone()),
        }
    }

    pub fn is_durable_coverage(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexFailureState {
    pub chunk: IndexChunk,
    pub error_kind: DatalensErrorKind,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexCheckpoint {
    pub job_id: IndexJobId,
    pub chain: ChainIdentity,
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
    pub chunks_planned: u64,
    pub chunks_fetched: u64,
    pub chunks_skipped: u64,
    pub chunks_written: u64,
    pub chunks_failed: u64,
    pub provider_limit_splits: u64,
    pub finality_capped_ranges: u64,
    pub provider_calls: u64,
    pub rows_written: usize,
    pub skipped_ranges: usize,
    pub retries: u64,
    pub failures: u64,
}
