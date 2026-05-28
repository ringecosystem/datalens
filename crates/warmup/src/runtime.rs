use std::{collections::HashMap, sync::Arc, thread, time::Duration};

use datalens_chain::{ChainAdapter, ChainFetchRequest, FinalityLevel, validate_durable_range};
use datalens_core::{
    DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange, missing_ranges,
};
use datalens_metrics::{
    ApplicationIdentity, MetricsLabels, MetricsRecorder, WarmupFetchOutcome, WarmupTaskOutcome,
    WarmupWriteOutcome,
};
use datalens_storage::{
    CacheOutcome, FillOutcome, QueryOutcome, StorageRepository, UsageLedgerEntry,
    UsageLedgerRepository,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig,
};
use serde::{Deserialize, Serialize};

use crate::{
    WarmupCheckpoint, WarmupCursor, WarmupRegistry, WarmupTask, WarmupTaskId, WarmupTaskMode,
    WarmupTaskState, registry::unix_seconds_now,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupRuntimeConfig {
    pub max_fetches_per_task_loop: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupSchedulerConfig {
    pub max_global_concurrent_tasks: usize,
    pub max_concurrent_tasks_per_chain: usize,
}

impl Default for WarmupSchedulerConfig {
    fn default() -> Self {
        Self {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        }
    }
}

impl Default for WarmupRuntimeConfig {
    fn default() -> Self {
        Self {
            max_fetches_per_task_loop: u64::MAX,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupRunStatus {
    Completed,
    Partial,
    #[default]
    Stopped,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarmupRunResult {
    pub status: WarmupRunStatus,
    pub fetched_ranges: u64,
    pub written_ranges: u64,
    pub empty_ranges: u64,
    pub provider_calls: u64,
    pub rows_fetched: usize,
    pub checkpoints: Vec<WarmupCheckpoint>,
}

#[derive(Clone)]
pub struct WarmupRuntime<A, S, R> {
    adapter: A,
    storage: S,
    registry: R,
    writer_config: DurableWriterConfig,
    runtime_config: WarmupRuntimeConfig,
    metrics: Option<MetricsRecorder>,
    usage_ledger: Option<Arc<dyn UsageLedgerRepository>>,
}

#[derive(Clone)]
pub struct WarmupTaskPool<A, S, R> {
    runtime: WarmupRuntime<A, S, R>,
    config: WarmupSchedulerConfig,
}

impl<A, S, R> WarmupTaskPool<A, S, R>
where
    A: ChainAdapter,
    S: StorageRepository + Clone + 'static,
    R: WarmupRegistry,
{
    pub fn new(runtime: WarmupRuntime<A, S, R>, config: WarmupSchedulerConfig) -> Self {
        Self { runtime, config }
    }

    pub fn submit(
        &self,
        request: crate::WarmupSubmitRequest,
    ) -> Result<crate::WarmupSubmitOutcome, DatalensError> {
        self.runtime.registry.submit(request)
    }

    pub fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError> {
        self.runtime.registry.get(task_id)
    }

    pub fn list(&self, filter: crate::WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError> {
        self.runtime.registry.list(filter)
    }

    pub fn pause(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        self.runtime.registry.pause(task_id)
    }

    pub fn cancel(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        self.runtime.registry.cancel(task_id)
    }

    pub fn retry_failed(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        self.runtime.registry.retry_failed(task_id)
    }

    pub fn run_available_once(&self) -> Result<Vec<WarmupRunResult>, DatalensError> {
        let mut results = Vec::new();
        let max_global = self.config.max_global_concurrent_tasks.max(1);
        let max_per_chain = self.config.max_concurrent_tasks_per_chain.max(1);
        let mut chain_counts: HashMap<String, usize> = HashMap::new();
        let tasks = self
            .runtime
            .registry
            .list(crate::WarmupTaskFilter::default())?;
        for task in tasks {
            if results.len() >= max_global {
                break;
            }
            if !matches!(
                task.state,
                WarmupTaskState::Queued | WarmupTaskState::Running
            ) {
                continue;
            }
            let chain_key = task.chain.key_prefix();
            let count = chain_counts.entry(chain_key).or_default();
            if *count >= max_per_chain {
                continue;
            }
            *count += 1;
            results.push(self.runtime.run_task_once(&task.task_id)?);
        }
        Ok(results)
    }
}

impl<A, S, R> WarmupRuntime<A, S, R>
where
    A: ChainAdapter,
    S: StorageRepository + Clone + 'static,
    R: WarmupRegistry,
{
    pub fn new(adapter: A, storage: S, registry: R, writer_config: DurableWriterConfig) -> Self {
        Self {
            adapter,
            storage,
            registry,
            writer_config,
            runtime_config: WarmupRuntimeConfig::default(),
            metrics: None,
            usage_ledger: None,
        }
    }

    pub fn with_runtime_config(mut self, config: WarmupRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    pub fn with_metrics(mut self, recorder: MetricsRecorder) -> Self {
        self.metrics = Some(recorder);
        self
    }

    pub fn with_usage_ledger(mut self, repository: impl UsageLedgerRepository + 'static) -> Self {
        self.usage_ledger = Some(Arc::new(repository));
        self
    }

    pub fn run_task_once(&self, task_id: &WarmupTaskId) -> Result<WarmupRunResult, DatalensError> {
        let mut task = self
            .registry
            .get(task_id)?
            .ok_or_else(|| missing_task(task_id))?;
        match task.state {
            WarmupTaskState::Paused | WarmupTaskState::Cancelled => {
                return Ok(WarmupRunResult {
                    status: WarmupRunStatus::Stopped,
                    ..WarmupRunResult::default()
                });
            }
            WarmupTaskState::Completed => {
                return Ok(WarmupRunResult {
                    status: WarmupRunStatus::Completed,
                    ..WarmupRunResult::default()
                });
            }
            WarmupTaskState::Queued | WarmupTaskState::Running | WarmupTaskState::Failed => {}
        }

        validate_task(&task, &self.adapter)?;
        let safe_height = self.adapter.cache_safe_height()?;
        safe_height.validate_durable_writable()?;
        if safe_height.range_kind != task.range_kind {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "warmup task range kind does not match adapter safe/finalized height",
            ));
        }

        let mut cursor = self.registry.load_cursor(task_id)?.unwrap_or_else(|| {
            WarmupCursor::new(
                task_id.clone(),
                task.start,
                unix_seconds_now().unwrap_or_default(),
            )
        });
        let target_end = target_end(&task, safe_height.value)?;
        if cursor.next > target_end {
            self.record_task_metric(&task, WarmupTaskOutcome::Completed);
            return self.finish_or_stop(task, WarmupRunResult::default());
        }

        task.state = WarmupTaskState::Running;
        task.last_error = None;
        task.touch(unix_seconds_now()?);
        self.registry.save_task(&task)?;
        self.record_task_metric(&task, WarmupTaskOutcome::Running);

        let writer = DurableWriter::new(self.storage.clone(), self.writer_config.clone());
        let mut result = WarmupRunResult::default();
        let mut next = cursor.next;

        while next <= target_end
            && result.fetched_ranges < self.runtime_config.max_fetches_per_task_loop.max(1)
        {
            if self.should_stop(task_id)? {
                writer.flush_for_shutdown()?;
                result.status = WarmupRunStatus::Stopped;
                return Ok(result);
            }

            let chunk = LedgerRange::try_new(
                task.range_kind.clone(),
                next,
                target_end.min(next.saturating_add(max_chunk_len(&task, &self.adapter)? - 1)),
            )?;
            validate_durable_range(&chunk, &safe_height)?;
            let covered = self.storage.covered_ranges(
                &task.chain,
                &task.dataset_key,
                &task.selector,
                chunk.clone(),
            )?;
            let missing = missing_ranges(chunk.clone(), &covered);
            if missing.is_empty() {
                cursor.mark_committed(chunk.clone(), unix_seconds_now()?);
                self.registry.save_cursor(&cursor)?;
                next = cursor.next;
                result.checkpoints.push(WarmupCheckpoint {
                    task_id: task.task_id.clone(),
                    range_kind: task.range_kind.clone(),
                    committed_range: chunk,
                    rows_written: 0,
                    provider_calls: 0,
                });
                continue;
            }

            let fetched = match self.fetch_missing(&task, &missing) {
                Ok(fetched) => fetched,
                Err(error) => {
                    self.record_provider_error_metric(&task, &error);
                    self.mark_failed(&mut task, &mut cursor, chunk, &error)?;
                    return Err(error);
                }
            };
            let rows_fetched = fetched
                .segments
                .iter()
                .map(|segment| segment.rows.row_count())
                .sum::<usize>();
            let write_result = match writer.write(DurableWriteRequest {
                chain: task.chain.clone(),
                dataset_key: task.dataset_key.clone(),
                selector: task.selector.clone(),
                finality_level: safe_height.finality,
                segments: fetched.segments,
            }) {
                Ok(write_result) => write_result,
                Err(error) => {
                    self.record_write_metric(&task, WarmupWriteOutcome::Error);
                    self.mark_failed(&mut task, &mut cursor, chunk, &error)?;
                    return Err(error);
                }
            };

            let written_ranges =
                (write_result.data_objects.len() + write_result.empty_coverages.len()) as u64;
            let empty_ranges = write_result.empty_coverages.len() as u64;
            self.record_write_metric(
                &task,
                if rows_fetched == 0 {
                    WarmupWriteOutcome::EmptyCoverageRecorded
                } else {
                    WarmupWriteOutcome::Written
                },
            );
            cursor.mark_committed(chunk.clone(), unix_seconds_now()?);
            self.registry.save_cursor(&cursor)?;
            self.append_usage(&task, &chunk, safe_height.finality, rows_fetched)?;
            self.record_metrics(&task, &chunk, rows_fetched);

            result.fetched_ranges += missing.len() as u64;
            result.written_ranges += written_ranges;
            result.empty_ranges += empty_ranges;
            result.provider_calls += fetched.provider_calls;
            result.rows_fetched += rows_fetched;
            result.checkpoints.push(WarmupCheckpoint {
                task_id: task.task_id.clone(),
                range_kind: task.range_kind.clone(),
                committed_range: chunk,
                rows_written: rows_fetched,
                provider_calls: fetched.provider_calls,
            });
            next = cursor.next;
        }

        task.stats.fetched_ranges += result.fetched_ranges;
        task.stats.written_ranges += result.written_ranges;
        task.stats.empty_ranges += result.empty_ranges;
        task.stats.provider_calls += result.provider_calls;
        task.stats.rows_fetched += result.rows_fetched;
        task.touch(unix_seconds_now()?);
        if next <= target_end {
            writer.flush_for_shutdown()?;
            task.state = WarmupTaskState::Queued;
            result.status = WarmupRunStatus::Partial;
            self.registry.save_task(&task)?;
            return Ok(result);
        }
        writer.flush_for_shutdown()?;
        self.finish_or_stop(task, result)
    }

    fn fetch_missing(
        &self,
        task: &WarmupTask,
        missing: &[LedgerRange],
    ) -> Result<FetchedSegments, DatalensError> {
        let mut segments = Vec::new();
        let mut provider_calls = 0u64;
        for range in missing {
            let mut attempts = 0;
            let response = loop {
                attempts += 1;
                let request = ChainFetchRequest::new(
                    task.chain.clone(),
                    task.dataset_key.clone(),
                    range.clone(),
                    task.selector.clone(),
                );
                match self.adapter.fetch(request.clone()) {
                    Ok(response) => {
                        response.validate_for_request(&request)?;
                        break response;
                    }
                    Err(error)
                        if error.is_retryable() && attempts < task.retry_policy.max_attempts =>
                    {
                        sleep_backoff(&task.retry_policy, attempts);
                    }
                    Err(error) => return Err(error),
                }
            };
            provider_calls += response.provider_diagnostics.calls.max(1) as u64
                + u64::from(attempts.saturating_sub(1));
            let dataset_key = response.rows.dataset_key().clone();
            let mut rows = response.rows.into_rows();
            rows.sort();
            segments.push(DurableWriteSegment {
                range: response.range,
                rows: DatasetRows::new(dataset_key, rows)?,
            });
        }
        Ok(FetchedSegments {
            segments,
            provider_calls,
        })
    }

    fn finish_or_stop(
        &self,
        mut task: WarmupTask,
        mut result: WarmupRunResult,
    ) -> Result<WarmupRunResult, DatalensError> {
        match task.mode {
            WarmupTaskMode::FixedRange => {
                task.state = WarmupTaskState::Completed;
                result.status = WarmupRunStatus::Completed;
                self.record_task_metric(&task, WarmupTaskOutcome::Completed);
            }
            WarmupTaskMode::FollowSafeHeight => {
                task.state = WarmupTaskState::Queued;
                result.status = if result.fetched_ranges == 0 {
                    WarmupRunStatus::Stopped
                } else {
                    WarmupRunStatus::Partial
                };
            }
        }
        task.touch(unix_seconds_now()?);
        self.registry.save_task(&task)?;
        Ok(result)
    }

    fn should_stop(&self, task_id: &WarmupTaskId) -> Result<bool, DatalensError> {
        let Some(task) = self.registry.get(task_id)? else {
            return Ok(true);
        };
        Ok(matches!(
            task.state,
            WarmupTaskState::Paused | WarmupTaskState::Cancelled | WarmupTaskState::Completed
        ))
    }

    fn mark_failed(
        &self,
        task: &mut WarmupTask,
        cursor: &mut WarmupCursor,
        range: LedgerRange,
        error: &DatalensError,
    ) -> Result<(), DatalensError> {
        let now = unix_seconds_now()?;
        cursor.mark_failure(
            range,
            cursor.current_attempt.saturating_add(1),
            error.message.clone(),
            now,
        );
        self.registry.save_cursor(cursor)?;
        task.state = WarmupTaskState::Failed;
        task.last_error = Some(error.message.clone());
        task.touch(now);
        self.record_task_metric(task, WarmupTaskOutcome::Failed);
        self.registry.save_task(task)
    }

    fn append_usage(
        &self,
        task: &WarmupTask,
        range: &LedgerRange,
        finality: FinalityLevel,
        rows_written: usize,
    ) -> Result<(), DatalensError> {
        let Some(ledger) = &self.usage_ledger else {
            return Ok(());
        };
        ledger.append(
            &UsageLedgerEntry::query_event(
                task.application_id.clone(),
                task.chain.clone(),
                task.dataset_key.clone(),
                &task.selector,
                range.clone(),
                finality,
                QueryOutcome::Filled,
                CacheOutcome::Miss,
                if rows_written == 0 {
                    FillOutcome::EmptyCoverageRecorded
                } else {
                    FillOutcome::Written
                },
                rows_written,
            )
            .with_request_id(format!("warmup:{}", task.task_id.as_str())),
        )
    }

    fn record_metrics(&self, task: &WarmupTask, range: &LedgerRange, rows_written: usize) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let labels = MetricsLabels::from_dataset_key(
            ApplicationIdentity::named(task.application_id.clone()),
            task.chain.clone(),
            task.dataset_key.clone(),
        );
        metrics.record_warmup_fetch(
            &labels,
            "evm_logs",
            if rows_written == 0 {
                WarmupFetchOutcome::Empty
            } else {
                WarmupFetchOutcome::Fetched
            },
        );
        metrics.record_warmup_rows(&labels, rows_written as u64);
        metrics.set_warmup_current_height(&labels, range.end());
    }

    fn record_task_metric(&self, task: &WarmupTask, outcome: WarmupTaskOutcome) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        metrics.record_warmup_task(&metrics_labels(task), outcome);
    }

    fn record_write_metric(&self, task: &WarmupTask, outcome: WarmupWriteOutcome) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        metrics.record_warmup_write(&metrics_labels(task), outcome);
    }

    fn record_provider_error_metric(&self, task: &WarmupTask, error: &DatalensError) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        metrics.record_warmup_provider_error(&metrics_labels(task), "evm_logs", error.kind.clone());
    }
}

struct FetchedSegments {
    segments: Vec<DurableWriteSegment>,
    provider_calls: u64,
}

fn validate_task<A>(task: &WarmupTask, adapter: &A) -> Result<(), DatalensError>
where
    A: ChainAdapter,
{
    let capabilities = adapter.capabilities();
    if capabilities.chain() != &task.chain {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup task chain does not match adapter capabilities",
        ));
    }
    if task.dataset_key != DatasetKey::evm_logs() {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "warmup MVP supports evm.logs only",
        ));
    }
    let capability = capabilities.dataset(&task.dataset_key).ok_or_else(|| {
        DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not support warmup dataset",
        )
    })?;
    if !capability.supports_selector(task.selector.kind()) {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not support warmup selector",
        ));
    }
    if !capability.ranges().contains(&task.range_kind) {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not support warmup range kind",
        ));
    }
    Ok(())
}

fn target_end(task: &WarmupTask, safe_height: u64) -> Result<u64, DatalensError> {
    match task.mode {
        WarmupTaskMode::FixedRange => Ok(task.end.unwrap_or(task.start).min(safe_height)),
        WarmupTaskMode::FollowSafeHeight => Ok(safe_height),
    }
}

fn max_chunk_len<A>(task: &WarmupTask, adapter: &A) -> Result<u64, DatalensError>
where
    A: ChainAdapter,
{
    let capability = adapter
        .capabilities()
        .dataset(&task.dataset_key)
        .and_then(|capability| capability.max_range_len())
        .unwrap_or(u64::MAX);
    Ok(task
        .chunk_policy
        .max_range_len
        .max(1)
        .min(capability.max(1)))
}

fn sleep_backoff(policy: &crate::WarmupRetryPolicy, attempts: u32) {
    let capped = policy
        .initial_backoff_ms
        .saturating_mul(2u64.saturating_pow(attempts.saturating_sub(1)))
        .min(policy.max_backoff_ms);
    if capped > 0 {
        thread::sleep(Duration::from_millis(capped));
    }
}

fn metrics_labels(task: &WarmupTask) -> MetricsLabels {
    MetricsLabels::from_dataset_key(
        ApplicationIdentity::named(task.application_id.clone()),
        task.chain.clone(),
        task.dataset_key.clone(),
    )
}

fn missing_task(task_id: &WarmupTaskId) -> DatalensError {
    DatalensError::new(
        DatalensErrorKind::InvalidInput,
        format!("warmup task {} not found", task_id.as_str()),
    )
}
