use std::{collections::HashMap, sync::Arc, thread, time::Duration};

use datalens_chain::{
    ChainAdapter, ChainFetchRequest, DatasetCapability, DatasetSelector, FinalityLevel,
    SelectorKind, validate_durable_range,
};
use datalens_core::{
    DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange, missing_ranges,
};
use datalens_metrics::{
    ApplicationIdentity, MetricsLabels, MetricsRecorder, WarmupFetchOutcome, WarmupTaskOutcome,
    WarmupWriteOutcome,
};
use datalens_storage::{
    CacheOutcome, FillOutcome, QueryOutcome, QueryWatermarkKey, QueryWatermarkRepository,
    StorageRepository, UsageLedgerEntry, UsageLedgerRepository,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig,
};
use serde::{Deserialize, Serialize};

use crate::{
    WarmupCheckpoint, WarmupCursor, WarmupRegistry, WarmupTask, WarmupTaskId, WarmupTaskMode,
    WarmupTaskState,
    pending_commit::PendingWarmupCommit,
    registry::unix_seconds_now,
    target_planner::{PlannedWarmupTarget, WarmupTargetPlanInput, WarmupTargetPlanner},
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
/// Warmup runtime that pre-fills safe/finalized durable coverage for submitted
/// application-owned tasks. It skips manifest-covered ranges, fetches only
/// missing gaps, and records cursor progress after durable write success.
pub struct WarmupRuntime<A, S, R> {
    adapter: A,
    storage: S,
    registry: R,
    writer: DurableWriter<S>,
    runtime_config: WarmupRuntimeConfig,
    follow_query_lookahead_blocks: u64,
    follow_query_start_offset_blocks: Option<u64>,
    follow_query_start_offset_tiers_blocks: Option<Vec<u64>>,
    follow_query_catchup_threshold_blocks: u64,
    metrics: Option<MetricsRecorder>,
    usage_ledger: Option<Arc<dyn UsageLedgerRepository>>,
    query_watermarks: Option<Arc<dyn QueryWatermarkRepository>>,
}

#[derive(Clone)]
/// Scheduler facade that limits concurrent warmup work globally and per chain.
/// The pool owns task selection; the runtime owns fetch/write/cursor semantics.
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
        validate_request(&request, &self.runtime.adapter)?;
        self.runtime.registry.submit(request)
    }

    pub fn ensure(
        &self,
        request: crate::WarmupSubmitRequest,
    ) -> Result<crate::WarmupEnsureOutcome, DatalensError> {
        validate_request(&request, &self.runtime.adapter)?;
        self.runtime.registry.ensure(request)
    }

    pub fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError> {
        self.runtime.registry.get(task_id)
    }

    pub fn list(
        &self,
        mut filter: crate::WarmupTaskFilter,
    ) -> Result<Vec<WarmupTask>, DatalensError> {
        let chain_key = self.runtime.adapter.capabilities().chain().key_prefix();
        if filter
            .chain_key
            .as_ref()
            .is_some_and(|filter_chain_key| filter_chain_key != &chain_key)
        {
            return Ok(Vec::new());
        }
        filter.chain_key = Some(chain_key);
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
        let tasks = self.list(crate::WarmupTaskFilter::default())?;
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
        let writer = DurableWriter::new(storage.clone(), writer_config);
        Self {
            adapter,
            storage,
            registry,
            writer,
            runtime_config: WarmupRuntimeConfig::default(),
            follow_query_lookahead_blocks: 100,
            follow_query_start_offset_blocks: None,
            follow_query_start_offset_tiers_blocks: None,
            follow_query_catchup_threshold_blocks: 200,
            metrics: None,
            usage_ledger: None,
            query_watermarks: None,
        }
    }

    pub fn with_durable_writer(mut self, writer: DurableWriter<S>) -> Self {
        self.writer = writer;
        self
    }

    pub fn with_runtime_config(mut self, config: WarmupRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    pub fn with_follow_query_lookahead_blocks(mut self, lookahead_blocks: u64) -> Self {
        self.follow_query_lookahead_blocks = lookahead_blocks;
        self
    }

    pub fn with_follow_query_start_offset_blocks(
        mut self,
        start_offset_blocks: Option<u64>,
    ) -> Self {
        self.follow_query_start_offset_blocks = start_offset_blocks;
        self
    }

    pub fn with_follow_query_start_offset_tiers_blocks(
        mut self,
        start_offset_tiers_blocks: Option<Vec<u64>>,
    ) -> Self {
        self.follow_query_start_offset_tiers_blocks = start_offset_tiers_blocks;
        self
    }

    pub fn with_follow_query_catchup_threshold_blocks(
        mut self,
        catchup_threshold_blocks: u64,
    ) -> Self {
        self.follow_query_catchup_threshold_blocks = catchup_threshold_blocks;
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

    pub fn with_query_watermarks(
        mut self,
        repository: impl QueryWatermarkRepository + 'static,
    ) -> Self {
        self.query_watermarks = Some(Arc::new(repository));
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
        let query_watermark = self.query_watermark(&task)?;
        let target_plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
            mode: task.mode,
            fixed_end: task.end,
            cursor_next: cursor.next,
            query_watermark,
            safe_head: safe_height.value,
            lookahead_blocks: self.follow_query_lookahead_blocks,
            start_offset_blocks: self.follow_query_start_offset_blocks,
            start_offset_tiers_blocks: self.follow_query_start_offset_tiers_blocks.clone(),
            catchup_threshold_blocks: self.follow_query_catchup_threshold_blocks,
        });
        log_target_plan(
            &task,
            cursor.next,
            query_watermark,
            safe_height.value,
            self.follow_query_lookahead_blocks,
            &target_plan,
        );
        let (target_start, target_end) = match target_plan {
            PlannedWarmupTarget::Range { start, end } => (start, end),
            PlannedWarmupTarget::Noop(_) => {
                if task.mode == WarmupTaskMode::FixedRange {
                    self.record_task_metric(&task, WarmupTaskOutcome::Completed);
                }
                return self.finish_or_stop(task, WarmupRunResult::default());
            }
        };
        if target_start != cursor.next {
            cursor.realign(target_start, unix_seconds_now()?);
            self.registry.save_cursor(&cursor)?;
        }
        if cursor.next > target_end {
            if task.mode == WarmupTaskMode::FixedRange {
                self.record_task_metric(&task, WarmupTaskOutcome::Completed);
            }
            return self.finish_or_stop(task, WarmupRunResult::default());
        }

        task.state = WarmupTaskState::Running;
        task.last_error = None;
        task.touch(unix_seconds_now()?);
        self.registry.save_task(&task)?;
        self.record_task_metric(&task, WarmupTaskOutcome::Running);

        let writer = self.writer.clone();
        let mut result = WarmupRunResult::default();
        let mut pending_commits = Vec::new();
        let mut next = cursor.next;
        let mut uncommitted_staged_coverage_seen = false;
        let mut external_staged_coverage_seen = false;

        while next <= target_end
            && result.fetched_ranges < self.runtime_config.max_fetches_per_task_loop.max(1)
        {
            if self.should_stop(task_id)? {
                self.flush_pending_commits(
                    &mut task,
                    &mut cursor,
                    &writer,
                    &mut pending_commits,
                    &mut result,
                    safe_height.finality,
                )?;
                result.status = WarmupRunStatus::Stopped;
                return Ok(result);
            }

            let chunk = LedgerRange::try_new(
                task.range_kind.clone(),
                next,
                target_end.min(next.saturating_add(max_chunk_len(&task, &self.adapter)? - 1)),
            )?;
            validate_durable_range(&chunk, &safe_height)?;
            let durable_covered = self.storage.covered_ranges(
                &task.chain,
                &task.dataset_key,
                &task.selector,
                chunk.clone(),
            )?;
            let durable_missing = missing_ranges(chunk.clone(), &durable_covered);
            let mut covered = durable_covered.clone();
            covered.extend(writer.staged_covered_ranges(
                &task.chain,
                &task.dataset_key,
                &task.selector,
                chunk.clone(),
            )?);
            let missing = missing_ranges(chunk.clone(), &covered);
            if missing.is_empty() {
                if durable_missing.is_empty()
                    && !uncommitted_staged_coverage_seen
                    && chunk.start() == cursor.next
                {
                    // Cursor progress follows manifest coverage here: if the
                    // chunk is already covered, advancing the cursor records
                    // skipped work without creating new durable authority.
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
                } else {
                    uncommitted_staged_coverage_seen = true;
                    external_staged_coverage_seen = true;
                    next = chunk.end().saturating_add(1);
                }
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
            let commit = PendingWarmupCommit {
                range: chunk.clone(),
                fetched_ranges: missing.len() as u64,
                written_ranges,
                empty_ranges,
                provider_calls: fetched.provider_calls,
                rows_fetched,
            };
            if write_result.staged_ranges.is_empty() {
                if !uncommitted_staged_coverage_seen
                    && chunk.start() == cursor.next
                    && self.is_durable_covered(&task, chunk.clone())?
                {
                    self.commit_visible_range(
                        &task,
                        &mut cursor,
                        commit,
                        &mut result,
                        safe_height.finality,
                    )?;
                    next = cursor.next;
                } else {
                    uncommitted_staged_coverage_seen = true;
                    next = chunk.end().saturating_add(1);
                }
            } else {
                pending_commits.push(commit);
                next = chunk.end().saturating_add(1);
            }
        }

        if !external_staged_coverage_seen {
            self.flush_pending_commits(
                &mut task,
                &mut cursor,
                &writer,
                &mut pending_commits,
                &mut result,
                safe_height.finality,
            )?;
        } else if pending_commits
            .first()
            .is_some_and(|commit| commit.range.start() == cursor.next)
        {
            self.flush_committable_pending_commits(
                &mut task,
                &mut cursor,
                &writer,
                &mut pending_commits,
                &mut result,
                safe_height.finality,
            )?;
        }
        task.stats.fetched_ranges += result.fetched_ranges;
        task.stats.written_ranges += result.written_ranges;
        task.stats.empty_ranges += result.empty_ranges;
        task.stats.provider_calls += result.provider_calls;
        task.stats.rows_fetched += result.rows_fetched;
        task.touch(unix_seconds_now()?);
        if uncommitted_staged_coverage_seen || next <= target_end {
            task.state = WarmupTaskState::Queued;
            result.status = WarmupRunStatus::Partial;
            self.registry.save_task(&task)?;
            return Ok(result);
        }
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
            WarmupTaskMode::FollowSafeHeight | WarmupTaskMode::FollowQuery => {
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

    fn is_durable_covered(
        &self,
        task: &WarmupTask,
        range: LedgerRange,
    ) -> Result<bool, DatalensError> {
        let covered = self.storage.covered_ranges(
            &task.chain,
            &task.dataset_key,
            &task.selector,
            range.clone(),
        )?;
        Ok(missing_ranges(range, &covered).is_empty())
    }

    fn flush_pending_commits(
        &self,
        task: &mut WarmupTask,
        cursor: &mut WarmupCursor,
        writer: &DurableWriter<S>,
        pending_commits: &mut Vec<PendingWarmupCommit>,
        result: &mut WarmupRunResult,
        finality: FinalityLevel,
    ) -> Result<(), DatalensError> {
        let pending_ranges = pending_commits
            .iter()
            .map(|commit| commit.range.clone())
            .collect::<Vec<_>>();
        let flush_result = match if pending_ranges.is_empty() {
            writer.flush_for_shutdown()
        } else {
            writer.flush_ranges_for_shutdown(
                &task.chain,
                &task.dataset_key,
                &task.selector,
                &pending_ranges,
            )
        } {
            Ok(flush_result) => flush_result,
            Err(error) => {
                self.record_write_metric(task, WarmupWriteOutcome::Error);
                if let Some(first_pending) = pending_commits.first() {
                    self.mark_failed(task, cursor, first_pending.range.clone(), &error)?;
                }
                return Err(error);
            }
        };
        if pending_commits.is_empty() {
            return Ok(());
        }
        if let Some(last_commit) = pending_commits.last_mut() {
            last_commit.include_flush_result(&flush_result);
        }
        for commit in std::mem::take(pending_commits) {
            self.commit_visible_range(task, cursor, commit, result, finality)?;
        }
        Ok(())
    }

    fn flush_committable_pending_commits(
        &self,
        task: &mut WarmupTask,
        cursor: &mut WarmupCursor,
        writer: &DurableWriter<S>,
        pending_commits: &mut Vec<PendingWarmupCommit>,
        result: &mut WarmupRunResult,
        finality: FinalityLevel,
    ) -> Result<(), DatalensError> {
        let mut expected_next = cursor.next;
        let committable_len = pending_commits
            .iter()
            .take_while(|commit| {
                let committable = commit.range.start() == expected_next;
                expected_next = commit.range.end().saturating_add(1);
                committable
            })
            .count();
        if committable_len == 0 {
            return Ok(());
        }

        let mut committable = pending_commits.drain(..committable_len).collect::<Vec<_>>();
        match self.flush_pending_commits(task, cursor, writer, &mut committable, result, finality) {
            Ok(()) => Ok(()),
            Err(error) => {
                committable.append(pending_commits);
                *pending_commits = committable;
                Err(error)
            }
        }
    }

    fn commit_visible_range(
        &self,
        task: &WarmupTask,
        cursor: &mut WarmupCursor,
        commit: PendingWarmupCommit,
        result: &mut WarmupRunResult,
        finality: FinalityLevel,
    ) -> Result<(), DatalensError> {
        cursor.mark_committed(commit.range.clone(), unix_seconds_now()?);
        self.registry.save_cursor(cursor)?;
        self.append_usage(task, &commit.range, finality, commit.rows_fetched)?;
        self.record_metrics(task, &commit.range, commit.rows_fetched);

        result.fetched_ranges += commit.fetched_ranges;
        result.written_ranges += commit.written_ranges;
        result.empty_ranges += commit.empty_ranges;
        result.provider_calls += commit.provider_calls;
        result.rows_fetched += commit.rows_fetched;
        result.checkpoints.push(WarmupCheckpoint {
            task_id: task.task_id.clone(),
            range_kind: task.range_kind.clone(),
            committed_range: commit.range,
            rows_written: commit.rows_fetched,
            provider_calls: commit.provider_calls,
        });
        Ok(())
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
        // Failure cursors point back at the failed range so retry/repair modes
        // restart from the first uncommitted gap rather than from the next chunk.
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

    fn query_watermark(&self, task: &WarmupTask) -> Result<Option<u64>, DatalensError> {
        if task.mode != WarmupTaskMode::FollowQuery {
            return Ok(None);
        }
        let Some(repository) = &self.query_watermarks else {
            return Ok(None);
        };
        let key = QueryWatermarkKey::new(
            task.application_id.clone(),
            task.chain.clone(),
            task.dataset_key.clone(),
            &task.selector,
            task.range_kind.clone(),
        );
        Ok(repository
            .read(&key)?
            .map(|watermark| watermark.latest_block))
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
            &selector_label(&task.selector),
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
        metrics.record_warmup_provider_error(
            &metrics_labels(task),
            &selector_label(&task.selector),
            error.kind.clone(),
        );
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
    validate_capability(
        &task.chain,
        &task.dataset_key,
        &task.selector,
        task.range_kind.clone(),
        adapter,
    )
}

fn validate_request<A>(
    request: &crate::WarmupSubmitRequest,
    adapter: &A,
) -> Result<(), DatalensError>
where
    A: ChainAdapter,
{
    validate_capability(
        &request.chain,
        &request.dataset_key,
        &request.selector,
        request.range_kind.clone(),
        adapter,
    )
}

fn validate_capability<A>(
    chain: &datalens_core::ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range_kind: datalens_core::LedgerRangeKind,
    adapter: &A,
) -> Result<(), DatalensError>
where
    A: ChainAdapter,
{
    let capabilities = adapter.capabilities();
    if capabilities.chain() != chain {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup task chain does not match adapter capabilities",
        ));
    }
    let capability = capabilities.dataset(dataset_key).ok_or_else(|| {
        DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not support warmup dataset",
        )
    })?;
    if !capability.supports_selector(selector.kind()) {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not support warmup selector",
        ));
    }
    if !capability.ranges().contains(&range_kind) {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not support warmup range kind",
        ));
    }
    if !capability.supports_safe_height() && !capability.supports_finalized_height() {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter dataset does not expose safe or finalized height for durable cache",
        ));
    }
    validate_selector_limits(selector, capability)?;
    Ok(())
}

fn validate_selector_limits(
    selector: &DatasetSelector,
    capability: &DatasetCapability,
) -> Result<(), DatalensError> {
    if let DatasetSelector::EvmLogs(filter) = selector {
        if let Some(max_addresses) = capability.max_addresses_per_query()
            && filter.addresses().len() > max_addresses
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "too many log addresses",
            ));
        }
        if let Some(max_topics) = capability.max_topics_per_query()
            && filter.topics().len() > max_topics
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "too many log topic slots",
            ));
        }
    }
    Ok(())
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

fn selector_label(selector: &DatasetSelector) -> String {
    match selector.kind() {
        SelectorKind::All => "all".to_owned(),
        SelectorKind::EvmLogs => "evm_logs".to_owned(),
        SelectorKind::Other(kind) => kind.as_str().to_owned(),
    }
}

fn log_target_plan(
    task: &WarmupTask,
    cursor_next: u64,
    query_watermark: Option<u64>,
    safe_head: u64,
    lookahead_blocks: u64,
    target_plan: &PlannedWarmupTarget,
) {
    let (planned_start, planned_end, no_op_reason) = match target_plan {
        PlannedWarmupTarget::Range { start, end } => (Some(*start), Some(*end), None),
        PlannedWarmupTarget::Noop(reason) => (None, None, Some(*reason)),
    };
    let cursor_query_distance =
        query_watermark.and_then(|watermark| cursor_next.checked_sub(watermark));
    let planned_query_distance = query_watermark
        .and_then(|watermark| planned_start.and_then(|start| start.checked_sub(watermark)));
    log::info!(
        "warmup target plan task_id={} application={} chain={} dataset={} selector_fingerprint={} selector_canonical_key={} cursor_next={} query_watermark={:?} cursor_query_distance={:?} safe_head={} lookahead_blocks={} planned_start={:?} planned_end={:?} planned_query_distance={:?} no_op_reason={:?}",
        task.task_id.as_str(),
        task.application_id,
        task.chain.key_prefix(),
        task.dataset_key.as_str(),
        task.selector.fingerprint(),
        task.selector.canonical_key(),
        cursor_next,
        query_watermark,
        cursor_query_distance,
        safe_head,
        lookahead_blocks,
        planned_start,
        planned_end,
        planned_query_distance,
        no_op_reason,
    );
}

fn missing_task(task_id: &WarmupTaskId) -> DatalensError {
    DatalensError::new(
        DatalensErrorKind::InvalidInput,
        format!("warmup task {} not found", task_id.as_str()),
    )
}
