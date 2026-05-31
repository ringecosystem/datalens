use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use datalens_chain::{
    ChainAdapter, ChainFetchRequest, ChainHeight, DatasetSelector, FinalityLevel, SelectorKind,
    validate_durable_range,
};
use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_metrics::{
    CacheCoverageOutcome as MetricsCacheCoverageOutcome, FillOutcome as MetricsFillOutcome,
    MetricsLabels, MetricsRecorder,
};
use datalens_storage::{
    CacheOutcome, FillOutcome, QueryOutcome, StorageRepository, UsageLedgerEntry,
    UsageLedgerRepository,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig,
};

use crate::{
    IndexAccounting, IndexCheckpoint, IndexChunk, IndexCursor, IndexDatasetCoverage,
    IndexDatasetProviderLimit, IndexDatasetRequest, IndexDurableWriteSummary, IndexFailureState,
    IndexFinalityRequirement, IndexJob, IndexJobId, IndexPlan, IndexRetryPolicy, IndexRunMode,
    IndexRunResult, IndexRunStatus,
};

pub trait IndexCursorRepository: Clone + Send + Sync + 'static {
    /// Load resumable progress for an index job. Cursors are operational
    /// checkpoints only; durable data authority still comes from manifest
    /// coverage.
    fn load(&self, job_id: &IndexJobId) -> Result<Option<IndexCursor>, DatalensError>;
    fn save(&self, cursor: &IndexCursor) -> Result<(), DatalensError>;
}

#[derive(Clone, Default)]
pub struct InMemoryIndexCursorStore {
    cursors: Arc<Mutex<HashMap<String, IndexCursor>>>,
}

impl IndexCursorRepository for InMemoryIndexCursorStore {
    fn load(&self, job_id: &IndexJobId) -> Result<Option<IndexCursor>, DatalensError> {
        Ok(self
            .cursors
            .lock()
            .expect("index cursor lock")
            .get(job_id.as_str())
            .cloned())
    }

    fn save(&self, cursor: &IndexCursor) -> Result<(), DatalensError> {
        self.cursors
            .lock()
            .expect("index cursor lock")
            .insert(cursor.job_id.as_str().to_owned(), cursor.clone());
        Ok(())
    }
}

#[derive(Clone)]
/// Durable index runtime for backfill, repair, retry, and verify workflows.
/// It uses cursor checkpoints to resume work, but always consults manifest
/// coverage before planning writes.
pub struct IndexRuntime<A, S, C> {
    adapter: A,
    storage: S,
    cursor_store: C,
    writer_config: DurableWriterConfig,
    metrics: Option<MetricsRecorder>,
    usage_ledger: Option<Arc<dyn UsageLedgerRepository>>,
}

impl<A, S, C> IndexRuntime<A, S, C>
where
    A: ChainAdapter,
    S: StorageRepository + Clone + 'static,
    C: IndexCursorRepository,
{
    pub fn new(
        adapter: A,
        storage: S,
        cursor_store: C,
        writer_config: DurableWriterConfig,
    ) -> Self {
        Self {
            adapter,
            storage,
            cursor_store,
            writer_config,
            metrics: None,
            usage_ledger: None,
        }
    }

    pub fn with_metrics(mut self, recorder: MetricsRecorder) -> Self {
        self.metrics = Some(recorder);
        self
    }

    pub fn with_usage_ledger(mut self, repository: impl UsageLedgerRepository + 'static) -> Self {
        self.usage_ledger = Some(Arc::new(repository));
        self
    }

    pub fn run(&self, job: IndexJob) -> Result<IndexRunResult, DatalensError> {
        let capabilities = self.adapter.capabilities();
        if capabilities.chain() != &job.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "index job chain does not match adapter capabilities",
            ));
        }
        let datasets = resolve_datasets(&job, &capabilities)?;
        let plan_job = IndexJob {
            dataset_selection: crate::IndexDatasetSelection::Selected(datasets.clone()),
            ..job.clone()
        };
        let mut covered_ranges = Vec::new();
        for dataset in &datasets {
            // Planning is anchored to durable manifest coverage, not cursor
            // progress, so a stale or lost cursor cannot claim data exists.
            match self.storage.covered_ranges(
                &job.chain,
                &dataset.dataset_key,
                &dataset.selector,
                job.range.clone(),
            ) {
                Ok(ranges) => {
                    covered_ranges.extend(ranges.into_iter().map(|range| IndexDatasetCoverage {
                        dataset_key: dataset.dataset_key.clone(),
                        selector: dataset.selector.clone(),
                        range,
                    }))
                }
                Err(error) if job.run_mode == IndexRunMode::Verify => {
                    return Err(verify_storage_error(
                        &job.chain,
                        &dataset.dataset_key,
                        &job.range,
                        "manifest coverage",
                        error,
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        let finality_boundary = if job.run_mode == IndexRunMode::Verify {
            verify_finality_boundary(&job)
        } else {
            match job.finality_requirement {
                IndexFinalityRequirement::Safe => self.adapter.cache_safe_height()?,
                IndexFinalityRequirement::Finalized => self.adapter.finalized_height()?,
            }
        };
        let provider_limits =
            provider_limits(&capabilities, &datasets, job.runtime_config.max_chunk_len);
        let plan = IndexPlan::try_new_with_dataset_coverage(
            plan_job,
            finality_boundary,
            provider_limits,
            covered_ranges,
        )?;
        let mut accounting = IndexAccounting {
            chunks_planned: plan.chunks.len() as u64,
            chunks_skipped: plan.skipped_ranges.len() as u64,
            skipped_ranges: plan.skipped_ranges.len(),
            finality_capped_ranges: u64::from(plan.planned_range != job.range),
            ..IndexAccounting::default()
        };

        if job.run_mode == IndexRunMode::Verify {
            return self.verify_plan(&plan, accounting);
        }

        let writer = DurableWriter::new(self.storage.clone(), self.writer_config.clone());
        let mut checkpoints = Vec::new();
        let mut queue = plan.chunks.iter().cloned().collect::<VecDeque<_>>();

        while let Some(chunk) = queue.pop_front() {
            let checkpoint = match self.execute_chunk(&job, &plan, &writer, chunk.clone()) {
                Ok(checkpoint) => checkpoint,
                Err(error)
                    if error.kind == DatalensErrorKind::ProviderLimit && chunk.range.len() > 1 =>
                {
                    // Provider limits are repaired by splitting the current
                    // chunk; durable validation still runs on each child range.
                    accounting.provider_limit_splits += 1;
                    let mut split = chunk.range.split(split_len(chunk.range.len()))?;
                    split.reverse();
                    for range in split {
                        queue.push_front(IndexChunk {
                            range,
                            ..chunk.clone()
                        });
                    }
                    continue;
                }
                Err(error) => {
                    self.save_failure_cursor(&job, &chunk, &error)?;
                    return Err(if error.kind == DatalensErrorKind::ProviderLimit {
                        DatalensError::new(
                            error.kind,
                            format!(
                                "{} for dataset {} range {}-{}",
                                error.message,
                                chunk.dataset_key.as_str(),
                                chunk.range.start(),
                                chunk.range.end()
                            ),
                        )
                    } else {
                        error
                    });
                }
            };

            accounting.provider_calls += checkpoint.provider_calls;
            accounting.retries += u64::from(checkpoint.attempts.saturating_sub(1));
            accounting.chunks_fetched += 1;
            accounting.chunks_written += u64::from(checkpoint.durable_write.is_some());
            accounting.rows_written += checkpoint
                .durable_write
                .as_ref()
                .map(|write| write.rows_written)
                .unwrap_or_default();
            checkpoints.push(checkpoint);
        }

        writer.flush_for_shutdown()?;

        Ok(IndexRunResult {
            job_id: job.id,
            mode: job.run_mode,
            status: IndexRunStatus::Completed,
            checkpoints,
            accounting,
        })
    }

    fn execute_chunk(
        &self,
        job: &IndexJob,
        plan: &IndexPlan,
        writer: &DurableWriter<S>,
        chunk: IndexChunk,
    ) -> Result<IndexCheckpoint, DatalensError> {
        validate_durable_range(&chunk.range, &plan.finality_boundary)?;
        let mut attempts = 0;
        let mut retries = 0;
        let response = loop {
            attempts += 1;
            let request = ChainFetchRequest::new(
                job.chain.clone(),
                chunk.dataset_key.clone(),
                chunk.range.clone(),
                chunk.selector.clone(),
            );
            match self.adapter.fetch(request.clone()) {
                Ok(response) => {
                    response.validate_for_request(&request)?;
                    break response;
                }
                Err(error)
                    if error.is_retryable() && attempts < chunk.retry_policy.max_attempts =>
                {
                    retries += 1;
                    sleep_backoff(&chunk.retry_policy, attempts);
                }
                Err(error) => return Err(error),
            }
        };

        validate_durable_range(&response.range, &plan.finality_boundary)?;
        let provider_calls = response.provider_diagnostics.calls.max(1) as u64
            + u64::from(attempts.saturating_sub(1));
        let rows_written = response.rows.row_count();
        let write_result = writer.write(DurableWriteRequest {
            chain: job.chain.clone(),
            dataset_key: chunk.dataset_key.clone(),
            selector: chunk.selector.clone(),
            finality_level: plan.durable_finality,
            segments: vec![DurableWriteSegment {
                range: response.range.clone(),
                rows: response.rows,
            }],
        })?;
        let durable_write = IndexDurableWriteSummary {
            finality_level: plan.durable_finality,
            data_objects: write_result.data_objects.len(),
            empty_coverages: write_result.empty_coverages.len(),
            rows_written,
        };
        self.record_chunk_metrics(job, &chunk, rows_written);
        self.append_usage(job, &chunk, plan.durable_finality, rows_written)?;
        let checkpoint = IndexCheckpoint {
            job_id: job.id.clone(),
            chain: job.chain.clone(),
            chunk: chunk.clone(),
            durable_write: Some(durable_write),
            provider_calls,
            attempts,
        };
        self.save_success_cursor(job, &checkpoint)?;
        if retries > 0 {
            let mut cursor = self
                .cursor_store
                .load(&job.id)?
                .unwrap_or_else(|| IndexCursor::from_checkpoint(&checkpoint));
            cursor.failure_state = None;
            self.cursor_store.save(&cursor)?;
        }
        Ok(checkpoint)
    }

    fn verify_plan(
        &self,
        plan: &IndexPlan,
        mut accounting: IndexAccounting,
    ) -> Result<IndexRunResult, DatalensError> {
        // Verify mode reads planned coverage and never writes or advances
        // cursors; it checks durable objects behind the manifest.
        for range in &plan.verification_ranges {
            let dataset = plan
                .job
                .dataset_selection
                .selected()?
                .iter()
                .find(|dataset| dataset.dataset_key == range.dataset_key)
                .ok_or_else(|| DatalensError::internal("verification dataset disappeared"))?;
            self.storage
                .read_rows(
                    &plan.job.chain,
                    &range.dataset_key,
                    &dataset.selector,
                    range.range.clone(),
                )
                .map_err(|error| {
                    verify_storage_error(
                        &plan.job.chain,
                        &range.dataset_key,
                        &range.range,
                        "read",
                        error,
                    )
                })?;
            accounting.chunks_fetched += 1;
        }
        Ok(IndexRunResult {
            job_id: plan.job.id.clone(),
            mode: plan.job.run_mode,
            status: IndexRunStatus::Completed,
            checkpoints: Vec::new(),
            accounting,
        })
    }

    fn save_success_cursor(
        &self,
        job: &IndexJob,
        checkpoint: &IndexCheckpoint,
    ) -> Result<(), DatalensError> {
        let mut cursor = self
            .cursor_store
            .load(&job.id)?
            .unwrap_or_else(|| IndexCursor::from_checkpoint(checkpoint));
        cursor.job_id = job.id.clone();
        cursor.chain = job.chain.clone();
        cursor.dataset_key = checkpoint.chunk.dataset_key.clone();
        cursor.selector = checkpoint.chunk.selector.clone();
        cursor.range_kind = checkpoint.chunk.range.kind();
        cursor.next_height = checkpoint.chunk.range.end().saturating_add(1);
        cursor.next_chunk_ordinal = checkpoint.chunk.ordinal + 1;
        cursor.last_checkpointed_range = Some(checkpoint.chunk.range.clone());
        if !cursor.completed_chunks.contains(&checkpoint.chunk.ordinal) {
            cursor.completed_chunks.push(checkpoint.chunk.ordinal);
        }
        if !cursor.completed_ranges.contains(&checkpoint.chunk.range) {
            cursor.completed_ranges.push(checkpoint.chunk.range.clone());
        }
        cursor.failure_state = None;
        self.cursor_store.save(&cursor)
    }

    fn save_failure_cursor(
        &self,
        job: &IndexJob,
        chunk: &IndexChunk,
        error: &DatalensError,
    ) -> Result<(), DatalensError> {
        let mut cursor = self.cursor_store.load(&job.id)?.unwrap_or(IndexCursor {
            job_id: job.id.clone(),
            chain: job.chain.clone(),
            dataset_key: chunk.dataset_key.clone(),
            selector: chunk.selector.clone(),
            range_kind: chunk.range.kind(),
            next_height: chunk.range.start(),
            completed_chunks: Vec::new(),
            completed_ranges: Vec::new(),
            failure_state: None,
            next_chunk_ordinal: chunk.ordinal,
            last_checkpointed_range: None,
        });
        cursor.failure_state = Some(IndexFailureState {
            chunk: chunk.clone(),
            error_kind: error.kind.clone(),
            message: error.message.clone(),
        });
        self.cursor_store.save(&cursor)
    }

    fn record_chunk_metrics(&self, job: &IndexJob, chunk: &IndexChunk, rows_written: usize) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let labels = MetricsLabels::from_dataset_key(
            job.application.clone(),
            job.chain.clone(),
            chunk.dataset_key.clone(),
        );
        metrics.record_cache_coverage(&labels, MetricsCacheCoverageOutcome::Miss);
        metrics.record_fill(
            &labels,
            if rows_written == 0 {
                MetricsFillOutcome::Empty
            } else {
                MetricsFillOutcome::Filled
            },
        );
        if chunk.range.kind() == datalens_chain::HeightRangeKind::Block {
            metrics.set_latest_filled_block(&labels, chunk.range.end());
        }
    }

    fn append_usage(
        &self,
        job: &IndexJob,
        chunk: &IndexChunk,
        finality: FinalityLevel,
        rows_written: usize,
    ) -> Result<(), DatalensError> {
        let Some(ledger) = &self.usage_ledger else {
            return Ok(());
        };
        ledger.append(&UsageLedgerEntry::query_event(
            job.application.as_str(),
            job.chain.clone(),
            chunk.dataset_key.clone(),
            &chunk.selector,
            chunk.range.clone(),
            finality,
            QueryOutcome::Filled,
            CacheOutcome::Miss,
            if rows_written == 0 {
                FillOutcome::EmptyCoverageRecorded
            } else {
                FillOutcome::Written
            },
            rows_written,
        ))
    }
}

fn verify_finality_boundary(job: &IndexJob) -> ChainHeight {
    // Verify mode checks existing durable manifest coverage only. It must not
    // call the provider to authorize writes because it never writes.
    ChainHeight {
        range_kind: job.range.kind(),
        value: job.range.end(),
        finality: match job.finality_requirement {
            IndexFinalityRequirement::Safe => FinalityLevel::Safe,
            IndexFinalityRequirement::Finalized => FinalityLevel::Finalized,
        },
    }
}

fn verify_storage_error(
    chain: &datalens_core::ChainIdentity,
    dataset_key: &datalens_core::DatasetKey,
    range: &datalens_core::LedgerRange,
    subsystem: &str,
    error: DatalensError,
) -> DatalensError {
    let kind = match error.kind {
        DatalensErrorKind::ProviderFailure
        | DatalensErrorKind::ProviderTimeout
        | DatalensErrorKind::RateLimited => DatalensErrorKind::StorageReadFailure,
        kind => kind,
    };
    let retryable = kind.is_retryable();
    DatalensError::new(
        kind,
        format!(
            "verify storage {subsystem} failed for chain {} dataset {} range {}-{} retryable={retryable}: {}",
            chain.configured_name(),
            dataset_key.as_str(),
            range.start(),
            range.end(),
            error.message,
        ),
    )
}

fn provider_limits(
    capabilities: &datalens_chain::AdapterCapabilities,
    datasets: &[IndexDatasetRequest],
    runtime_max_chunk_len: u64,
) -> Vec<IndexDatasetProviderLimit> {
    datasets
        .iter()
        .filter_map(|dataset| {
            capabilities
                .dataset(&dataset.dataset_key)
                .and_then(|capability| {
                    capability.max_range_len().map(|max_range_len| {
                        capability.ranges().iter().map(move |range_kind| {
                            IndexDatasetProviderLimit {
                                dataset_key: dataset.dataset_key.clone(),
                                range_kind: range_kind.clone(),
                                max_range_len: max_range_len.min(runtime_max_chunk_len.max(1)),
                            }
                        })
                    })
                })
        })
        .flatten()
        .collect()
}

fn resolve_datasets(
    job: &IndexJob,
    capabilities: &datalens_chain::AdapterCapabilities,
) -> Result<Vec<IndexDatasetRequest>, DatalensError> {
    match &job.dataset_selection {
        crate::IndexDatasetSelection::Selected(datasets) if datasets.is_empty() => {
            Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "index dataset selection must not be empty",
            ))
        }
        crate::IndexDatasetSelection::Selected(datasets) => Ok(datasets.clone()),
        crate::IndexDatasetSelection::AllSupported => capabilities
            .dataset_capabilities()
            .iter()
            .map(|capability| {
                let selector = if capability.supports_selector(SelectorKind::All) {
                    DatasetSelector::all()
                } else {
                    return Err(DatalensError::new(
                        DatalensErrorKind::UnsupportedDataset,
                        format!(
                            "dataset {} cannot be selected by all-supported indexing",
                            capability.dataset().as_str()
                        ),
                    ));
                };
                Ok(IndexDatasetRequest {
                    dataset_key: capability.dataset().clone(),
                    selector,
                })
            })
            .collect(),
    }
}

fn split_len(len: u128) -> u64 {
    u64::try_from((len / 2).max(1)).unwrap_or(u64::MAX)
}

fn sleep_backoff(policy: &IndexRetryPolicy, attempts: u32) {
    let capped = policy
        .initial_backoff_ms
        .saturating_mul(2u64.saturating_pow(attempts.saturating_sub(1)))
        .min(policy.max_backoff_ms);
    if capped > 0 {
        thread::sleep(Duration::from_millis(capped));
    }
}
