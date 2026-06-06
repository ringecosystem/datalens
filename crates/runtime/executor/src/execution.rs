use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use datalens_chain::{
    ChainAdapter, ChainFetchRequest, ChainFetchResponse, FetchContext, FinalityLevel,
};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    QueryFinalityRequirement, missing_ranges,
};
use datalens_metrics::{
    ApplicationIdentity, CacheCoverageOutcome, DurableWriteOutcome as MetricsDurableWriteOutcome,
    ErrorLabels, FillOutcome, MetricsLabels, MetricsRecorder, QueryOutcome,
};
use datalens_planner::{
    CoverageSummary, FinalityPolicy, NativePlanner, NativePlannerConfig, NativeQueryInput,
};
use datalens_storage::{
    CacheOutcome as LedgerCacheOutcome, DurableWriteOutcome as LedgerDurableWriteOutcome,
    FillOutcome as LedgerFillOutcome, QueryOutcome as LedgerQueryOutcome, QueryWatermark,
    QueryWatermarkKey, QueryWatermarkRepository, StorageRepository, UsageLedgerEntry,
    UsageLedgerRepository,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteResult, DurableWriteSegment, DurableWriter,
    DurableWriterConfig,
};

use crate::helpers::*;
pub use crate::hot_promotion::HotCachePromoter;

static QUERY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn generate_query_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default();
    let counter = QUERY_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("q{}{}", base36(millis), base36(counter))
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Native query executor configuration shared by REST and GraphQL query flows.
/// Planner settings decide coverage and provider gaps; writer settings decide
/// how safe/finalized fills become durable objects.
pub struct NativeQueryExecutionConfig {
    pub planner: NativePlannerConfig,
    pub writer: DurableWriterConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeQueryExecutionResult {
    pub chain: datalens_core::ChainIdentity,
    pub dataset_key: DatasetKey,
    pub ledger_range: LedgerRange,
    pub cache: CoverageSummary,
    pub rows: DatasetRows,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotPromotionRequest {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: datalens_chain::DatasetSelector,
    pub range: LedgerRange,
    pub application: Option<ApplicationIdentity>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HotPromotionResult {
    pub attempted: usize,
    pub promoted: usize,
    pub skipped: usize,
}

#[derive(Clone)]
/// Executes the native datalens query contract: plan coverage, read durable
/// rows, fetch provider gaps, persist only durable-safe fills, and record metrics
/// plus usage ledger entries under the caller's application identity.
pub struct NativeQueryExecutor<R, S> {
    storage: R,
    source: S,
    planner: NativePlanner,
    writer: DurableWriter<R>,
    metrics: Option<ExecutorMetrics>,
    usage_ledger: Option<ExecutorUsageLedger>,
    query_watermarks: Option<ExecutorQueryWatermarks>,
}

#[derive(Clone)]
pub(crate) struct ExecutorMetrics {
    pub(crate) recorder: Arc<MetricsRecorder>,
    pub(crate) application: ApplicationIdentity,
}

#[derive(Clone)]
pub(crate) struct ExecutorUsageLedger {
    pub(crate) repository: Arc<dyn UsageLedgerRepository>,
    pub(crate) application: ApplicationIdentity,
}

#[derive(Clone)]
pub(crate) struct ExecutorQueryWatermarks {
    pub(crate) repository: Arc<dyn QueryWatermarkRepository>,
    pub(crate) application: ApplicationIdentity,
}

type ProviderFetchResponse = (ChainFetchRequest, ChainFetchResponse);

impl<R, S> NativeQueryExecutor<R, S>
where
    R: StorageRepository + Clone,
    S: ChainAdapter,
{
    pub fn new(storage: R, source: S, config: NativeQueryExecutionConfig) -> Self {
        let writer = DurableWriter::new(storage.clone(), config.writer);
        Self {
            storage,
            source,
            planner: NativePlanner::new(config.planner),
            writer,
            metrics: None,
            usage_ledger: None,
            query_watermarks: None,
        }
    }

    pub fn with_metrics(
        mut self,
        recorder: MetricsRecorder,
        application: ApplicationIdentity,
    ) -> Self {
        self.metrics = Some(ExecutorMetrics {
            recorder: Arc::new(recorder),
            application,
        });
        self
    }

    pub fn with_usage_ledger(
        mut self,
        repository: impl UsageLedgerRepository + 'static,
        application: ApplicationIdentity,
    ) -> Self {
        self.usage_ledger = Some(ExecutorUsageLedger {
            repository: Arc::new(repository),
            application,
        });
        self
    }

    pub fn with_query_watermarks(
        mut self,
        repository: impl QueryWatermarkRepository + 'static,
        application: ApplicationIdentity,
    ) -> Self {
        self.query_watermarks = Some(ExecutorQueryWatermarks {
            repository: Arc::new(repository),
            application,
        });
        self
    }

    pub fn execute(
        &self,
        input: NativeQueryInput,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        self.execute_with_application_and_query_id(input, None, generate_query_id())
    }

    pub fn latest_height(&self) -> Result<datalens_chain::ChainHeight, DatalensError> {
        self.source.latest_height()
    }

    pub fn cache_safe_height(&self) -> Result<datalens_chain::ChainHeight, DatalensError> {
        self.source.cache_safe_height()
    }

    pub fn finalized_height(&self) -> Result<datalens_chain::ChainHeight, DatalensError> {
        self.source.finalized_height()
    }

    pub fn flush_staged_writes(&self) -> Result<DurableWriteResult, DatalensError> {
        self.writer.flush()
    }

    pub fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError> {
        self.writer.flush_for_shutdown()
    }

    pub fn durable_writer(&self) -> DurableWriter<R> {
        self.writer.clone()
    }

    pub fn execute_with_application(
        &self,
        input: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        self.execute_with_application_and_query_id(input, application, generate_query_id())
    }

    pub fn execute_with_application_and_query_id(
        &self,
        input: NativeQueryInput,
        application: Option<ApplicationIdentity>,
        query_id: String,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        let start = Instant::now();
        let labels = self.metrics_labels(&input, application.clone());
        let ledger_application = self.ledger_application(application.clone());
        if let Some((recorder, labels)) = self.metrics_recorder(&labels) {
            recorder.set_latest_requested_block(labels, input.ledger_range.end());
        }
        if matches!(input.finality, QueryFinalityRequirement::LatestOnly) {
            return self.execute_hot_read_through(input, application, start, labels, query_id);
        }

        // Coverage is read before asking the adapter for finality so full
        // durable hits avoid unnecessary provider calls; misses still require a
        // safe/finalized boundary before they can be filled and written.
        let mut covered_ranges = match self.storage.covered_ranges(
            &input.chain,
            &input.dataset_key,
            &input.selector,
            input.ledger_range.clone(),
        ) {
            Ok(covered_ranges) => covered_ranges,
            Err(error) => {
                self.record_error(&labels, &error);
                self.record_cache_coverage(&labels, CacheCoverageOutcome::Error);
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage(
                    &query_id,
                    &ledger_application,
                    &input,
                    FinalityLevel::Safe,
                    LedgerQueryOutcome::StorageError,
                    LedgerCacheOutcome::Error,
                    LedgerFillOutcome::NotAttempted,
                    LedgerDurableWriteOutcome::NotAttempted,
                    0,
                );
                return Err(error);
            }
        };
        covered_ranges.extend(self.writer.staged_covered_ranges(
            &input.chain,
            &input.dataset_key,
            &input.selector,
            input.ledger_range.clone(),
        )?);
        let hit_ranges = covered_ranges
            .iter()
            .filter_map(|range| range.intersection(&input.ledger_range))
            .collect::<Vec<_>>();
        let miss_ranges = missing_ranges(input.ledger_range.clone(), &hit_ranges);
        let preliminary_coverage_outcome = coverage_outcome(&hit_ranges, &miss_ranges);
        let durable_boundary = if matches!(input.finality, QueryFinalityRequirement::SafeToLatest) {
            match self.source.cache_safe_height() {
                Ok(boundary) => boundary,
                Err(error) => {
                    self.record_error(&labels, &error);
                    self.record_cache_coverage(&labels, CacheCoverageOutcome::Error);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage(
                        &query_id,
                        &ledger_application,
                        &input,
                        FinalityLevel::Safe,
                        ledger_query_error(&error),
                        ledger_cache_outcome(preliminary_coverage_outcome),
                        LedgerFillOutcome::NotAttempted,
                        LedgerDurableWriteOutcome::NotAttempted,
                        0,
                    );
                    return Err(error);
                }
            }
        } else if miss_ranges.is_empty() {
            boundary_for_cached_hit(&input.ledger_range)
        } else {
            match self.source.cache_safe_height() {
                Ok(boundary) => boundary,
                Err(error) => {
                    self.record_error(&labels, &error);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage(
                        &query_id,
                        &ledger_application,
                        &input,
                        FinalityLevel::Safe,
                        ledger_query_error(&error),
                        ledger_cache_outcome(preliminary_coverage_outcome),
                        LedgerFillOutcome::NotAttempted,
                        LedgerDurableWriteOutcome::NotAttempted,
                        0,
                    );
                    return Err(error);
                }
            }
        };
        let latest = if matches!(input.finality, QueryFinalityRequirement::SafeToLatest) {
            match self.source.latest_height() {
                Ok(latest) => latest,
                Err(error) => {
                    self.record_error(&labels, &error);
                    self.record_cache_coverage(&labels, CacheCoverageOutcome::Error);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage(
                        &query_id,
                        &ledger_application,
                        &input,
                        FinalityLevel::Latest,
                        ledger_query_error(&error),
                        ledger_cache_outcome(preliminary_coverage_outcome),
                        LedgerFillOutcome::NotAttempted,
                        LedgerDurableWriteOutcome::NotAttempted,
                        0,
                    );
                    return Err(error);
                }
            }
        } else {
            durable_boundary.clone()
        };
        let plan = match self.planner.plan_with_live_coverage(
            input.clone(),
            &self.source.capabilities(),
            durable_boundary.clone(),
            latest,
            covered_ranges,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage(
                    &query_id,
                    &ledger_application,
                    &input,
                    durable_boundary.finality,
                    LedgerQueryOutcome::Error,
                    ledger_cache_outcome(preliminary_coverage_outcome),
                    LedgerFillOutcome::NotAttempted,
                    LedgerDurableWriteOutcome::NotAttempted,
                    0,
                );
                return Err(error);
            }
        };

        let coverage_outcome = plan_coverage_outcome(&plan);
        let provider_fill_ranges = plan
            .fetch_tasks
            .iter()
            .map(|task| task.range.clone())
            .collect::<Vec<_>>();
        log::info!(
            "query executor cache summary {}",
            cache_diagnostic_summary(
                &query_id,
                &plan.chain,
                &plan.dataset_key,
                &plan.ledger_range,
                &plan.selector,
                plan.coverage.status.clone(),
                coverage_outcome,
                &plan.coverage.hit_ranges,
                &plan.coverage.missing_ranges,
                &plan.coverage.durable_hit_ranges,
                &plan.coverage.hot_hit_ranges,
                &provider_fill_ranges,
            )
        );
        log::debug!(
            "query executor selector canonical query_id={} selector_canonical_key={}",
            query_id,
            plan.selector.canonical_key()
        );
        self.record_cache_coverage(&labels, coverage_outcome);

        let mut rows = empty_query_rows(&plan.dataset_key);
        for segment in &plan.read_segments {
            let cached = match self.writer.read_staged_rows(
                &plan.chain,
                &plan.dataset_key,
                &plan.selector,
                segment.range.clone(),
            ) {
                Ok(Some(cached)) => cached,
                Ok(None) => match self.storage.read_rows(
                    &plan.chain,
                    &plan.dataset_key,
                    &plan.selector,
                    segment.range.clone(),
                ) {
                    Ok(cached) => cached,
                    Err(error) => {
                        self.record_error(&labels, &error);
                        self.record_query(&labels, QueryOutcome::Error, start);
                        self.record_usage_for_plan(
                            &query_id,
                            &ledger_application,
                            &plan,
                            LedgerQueryOutcome::StorageError,
                            ledger_cache_outcome(coverage_outcome),
                            LedgerFillOutcome::NotAttempted,
                            LedgerDurableWriteOutcome::NotAttempted,
                            rows.row_count(),
                        );
                        return Err(error);
                    }
                },
                Err(error) => {
                    self.record_error(&labels, &error);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage_for_plan(
                        &query_id,
                        &ledger_application,
                        &plan,
                        LedgerQueryOutcome::StorageError,
                        ledger_cache_outcome(coverage_outcome),
                        LedgerFillOutcome::NotAttempted,
                        LedgerDurableWriteOutcome::NotAttempted,
                        rows.row_count(),
                    );
                    return Err(error);
                }
            };
            if let Err(error) = rows.try_append(cached.into_rows()) {
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage_for_plan(
                    &query_id,
                    &ledger_application,
                    &plan,
                    LedgerQueryOutcome::Error,
                    ledger_cache_outcome(coverage_outcome),
                    LedgerFillOutcome::NotAttempted,
                    LedgerDurableWriteOutcome::NotAttempted,
                    rows.row_count(),
                );
                return Err(error);
            }
        }

        let finality_level = match &plan.finality_policy {
            FinalityPolicy::DurableCache { boundary } => boundary.finality,
            FinalityPolicy::HotReadThrough { latest } => latest.finality,
            FinalityPolicy::MixedReadThrough {
                durable_boundary, ..
            } => durable_boundary.finality,
        };
        let mut fetched_segments = Vec::new();
        let mut fill_row_count = 0usize;
        let mut fill_end_block = None;
        let fill_start = Instant::now();

        for task in &plan.fetch_tasks {
            let fetch_request = ChainFetchRequest::new(
                plan.chain.clone(),
                plan.dataset_key.clone(),
                task.range.clone(),
                plan.selector.clone(),
            )
            .with_context(FetchContext {
                request_id: Some(query_id.clone()),
                cache_write: task.cache_write,
            });
            let fetched_responses =
                match self.fetch_with_provider_limit_splits(fetch_request.clone(), &query_id) {
                    Ok(responses) => responses,
                    Err(error) => {
                        log::warn!(
                            "provider fetch failed query_id={} dataset={} range={}-{} kind={:?}",
                            query_id,
                            plan.dataset_key.as_str(),
                            task.range.start(),
                            task.range.end(),
                            error.kind
                        );
                        self.record_error(&labels, &error);
                        self.record_fill(&labels, FillOutcome::Error, fill_start);
                        self.record_query(&labels, QueryOutcome::Error, start);
                        self.record_usage_for_plan(
                            &query_id,
                            &ledger_application,
                            &plan,
                            ledger_query_error(&error),
                            ledger_cache_outcome(coverage_outcome),
                            ledger_fill_error(&error),
                            LedgerDurableWriteOutcome::NotAttempted,
                            rows.row_count(),
                        );
                        return Err(error);
                    }
                };
            for (fetch_request, response) in fetched_responses {
                let fetched = {
                    if let Err(error) = response.validate_for_request(&fetch_request) {
                        self.record_fill(&labels, FillOutcome::Error, fill_start);
                        self.record_query(&labels, QueryOutcome::Error, start);
                        self.record_usage_for_plan(
                            &query_id,
                            &ledger_application,
                            &plan,
                            LedgerQueryOutcome::Error,
                            ledger_cache_outcome(coverage_outcome),
                            LedgerFillOutcome::Error,
                            LedgerDurableWriteOutcome::NotAttempted,
                            rows.row_count(),
                        );
                        return Err(error);
                    }
                    fill_row_count += response.rows.row_count();
                    response.rows
                };
                if task.cache_write {
                    // Only planner-marked durable misses are staged for durable
                    // cache. Hot/latest provider data is appended to the response
                    // but intentionally excluded from fetched_segments.
                    fill_end_block = Some(
                        fill_end_block
                            .unwrap_or(fetch_request.range.end())
                            .max(fetch_request.range.end()),
                    );
                    fetched_segments.push(DurableWriteSegment {
                        range: fetch_request.range.clone(),
                        rows: fetched.clone(),
                    });
                }
                if let Err(error) = rows.try_append(fetched.into_rows()) {
                    self.record_fill(&labels, FillOutcome::Error, fill_start);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage_for_plan(
                        &query_id,
                        &ledger_application,
                        &plan,
                        LedgerQueryOutcome::Error,
                        ledger_cache_outcome(coverage_outcome),
                        LedgerFillOutcome::Error,
                        LedgerDurableWriteOutcome::NotAttempted,
                        rows.row_count(),
                    );
                    return Err(error);
                }
            }
        }

        let provider_fetch_attempted = !plan.fetch_tasks.is_empty();
        let cache_fill_attempted = !fetched_segments.is_empty();
        let mut durable_write_outcome = LedgerDurableWriteOutcome::NotAttempted;
        if cache_fill_attempted {
            match self.writer.write(DurableWriteRequest {
                chain: plan.chain.clone(),
                dataset_key: plan.dataset_key.clone(),
                selector: plan.selector.clone(),
                finality_level,
                segments: fetched_segments,
            }) {
                Ok(mut write_result) => {
                    let mut write_failed = false;
                    if !write_result.staged_ranges.is_empty() {
                        match self.writer.flush() {
                            Ok(flush_result) => {
                                write_result = flush_result;
                            }
                            Err(error) => {
                                log::error!(
                                    "cache write flush failed dataset={} range={}-{} kind={:?}",
                                    plan.dataset_key.as_str(),
                                    plan.ledger_range.start(),
                                    plan.ledger_range.end(),
                                    error.kind
                                );
                                self.record_error(&labels, &error);
                                self.record_durable_write(
                                    &labels,
                                    MetricsDurableWriteOutcome::StorageError,
                                );
                                durable_write_outcome = LedgerDurableWriteOutcome::StorageError;
                                write_failed = true;
                            }
                        }
                    }
                    if !write_failed {
                        durable_write_outcome = ledger_durable_write_outcome(&write_result);
                        self.record_durable_write(
                            &labels,
                            metrics_durable_write_outcome(&write_result),
                        );
                    }
                }
                Err(error) => {
                    log::error!(
                        "cache write failed dataset={} range={}-{} kind={:?}",
                        plan.dataset_key.as_str(),
                        plan.ledger_range.start(),
                        plan.ledger_range.end(),
                        error.kind
                    );
                    self.record_error(&labels, &error);
                    self.record_durable_write(&labels, MetricsDurableWriteOutcome::StorageError);
                    durable_write_outcome = LedgerDurableWriteOutcome::StorageError;
                }
            };
        }
        if provider_fetch_attempted {
            let fill_outcome = if !cache_fill_attempted {
                FillOutcome::LiveFetch
            } else if fill_row_count == 0 {
                FillOutcome::Empty
            } else {
                FillOutcome::Filled
            };
            self.record_fill(&labels, fill_outcome, fill_start);
            if let Some(block) = fill_end_block
                && let Some((recorder, labels)) = self.metrics_recorder(&labels)
            {
                recorder.set_latest_filled_block(labels, block);
            }
        }

        rows.sort();
        let mut cache = plan.coverage.clone();
        cache.provider_fill_ranges = provider_fill_ranges;

        let result = NativeQueryExecutionResult {
            chain: plan.chain.clone(),
            dataset_key: plan.dataset_key.clone(),
            ledger_range: plan.ledger_range.clone(),
            cache,
            rows: DatasetRows::new(plan.dataset_key.clone(), rows)?,
        };
        let query_outcome = query_outcome(coverage_outcome, cache_fill_attempted, &result);
        self.record_query(&labels, query_outcome, start);
        self.record_usage_for_plan(
            &query_id,
            &ledger_application,
            &plan,
            ledger_query_outcome(query_outcome),
            ledger_cache_outcome(coverage_outcome),
            ledger_fill_outcome(provider_fetch_attempted, fill_row_count),
            durable_write_outcome,
            result.rows.row_count(),
        );
        self.record_query_watermark_for_plan(&query_id, &application, &plan);
        Ok(result)
    }

    fn execute_hot_read_through(
        &self,
        input: NativeQueryInput,
        application: Option<ApplicationIdentity>,
        start: Instant,
        labels: Option<MetricsLabels>,
        query_id: String,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        let ledger_application = self.ledger_application(application);
        // Latest-only queries bypass durable coverage entirely so unsafe/latest
        // provider rows cannot become manifest authority through this path.
        let latest = match self.source.latest_height() {
            Ok(latest) => latest,
            Err(error) => {
                self.record_error(&labels, &error);
                self.record_cache_coverage(&labels, CacheCoverageOutcome::Error);
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage(
                    &query_id,
                    &ledger_application,
                    &input,
                    FinalityLevel::Latest,
                    ledger_query_error(&error),
                    LedgerCacheOutcome::Error,
                    LedgerFillOutcome::NotAttempted,
                    LedgerDurableWriteOutcome::NotAttempted,
                    0,
                );
                return Err(error);
            }
        };
        let plan = match self.planner.plan_with_coverage(
            input.clone(),
            &self.source.capabilities(),
            latest,
            Vec::new(),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage(
                    &query_id,
                    &ledger_application,
                    &input,
                    FinalityLevel::Latest,
                    LedgerQueryOutcome::Error,
                    LedgerCacheOutcome::HotMiss,
                    LedgerFillOutcome::NotAttempted,
                    LedgerDurableWriteOutcome::NotAttempted,
                    0,
                );
                return Err(error);
            }
        };

        self.record_cache_coverage(&labels, CacheCoverageOutcome::HotMiss);
        let provider_fill_ranges = plan
            .fetch_tasks
            .iter()
            .map(|task| task.range.clone())
            .collect::<Vec<_>>();
        log::info!(
            "query executor cache summary {}",
            cache_diagnostic_summary(
                &query_id,
                &plan.chain,
                &plan.dataset_key,
                &plan.ledger_range,
                &plan.selector,
                plan.coverage.status.clone(),
                CacheCoverageOutcome::HotMiss,
                &plan.coverage.hit_ranges,
                &plan.coverage.missing_ranges,
                &plan.coverage.durable_hit_ranges,
                &plan.coverage.hot_hit_ranges,
                &provider_fill_ranges,
            )
        );
        log::debug!(
            "query executor selector canonical query_id={} selector_canonical_key={}",
            query_id,
            plan.selector.canonical_key()
        );
        let fill_start = Instant::now();
        let mut rows = empty_query_rows(&plan.dataset_key);
        for task in &plan.fetch_tasks {
            let fetch_request = ChainFetchRequest::new(
                plan.chain.clone(),
                plan.dataset_key.clone(),
                task.range.clone(),
                plan.selector.clone(),
            )
            .with_context(FetchContext {
                request_id: Some(query_id.clone()),
                cache_write: false,
            });
            let fetched_responses =
                match self.fetch_with_provider_limit_splits(fetch_request.clone(), &query_id) {
                    Ok(responses) => responses,
                    Err(error) => {
                        self.record_error(&labels, &error);
                        self.record_fill(&labels, FillOutcome::Error, fill_start);
                        self.record_query(&labels, QueryOutcome::Error, start);
                        self.record_usage_for_plan(
                            &query_id,
                            &ledger_application,
                            &plan,
                            ledger_query_error(&error),
                            LedgerCacheOutcome::HotMiss,
                            ledger_fill_error(&error),
                            LedgerDurableWriteOutcome::NotAttempted,
                            rows.row_count(),
                        );
                        return Err(error);
                    }
                };
            for (fetch_request, response) in fetched_responses {
                let fetched = {
                    if let Err(error) = response.validate_for_request(&fetch_request) {
                        self.record_fill(&labels, FillOutcome::Error, fill_start);
                        self.record_query(&labels, QueryOutcome::Error, start);
                        self.record_usage_for_plan(
                            &query_id,
                            &ledger_application,
                            &plan,
                            LedgerQueryOutcome::Error,
                            LedgerCacheOutcome::HotMiss,
                            LedgerFillOutcome::Error,
                            LedgerDurableWriteOutcome::NotAttempted,
                            rows.row_count(),
                        );
                        return Err(error);
                    }
                    response.rows
                };
                if let Err(error) = rows.try_append(fetched.into_rows()) {
                    self.record_fill(&labels, FillOutcome::Error, fill_start);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage_for_plan(
                        &query_id,
                        &ledger_application,
                        &plan,
                        LedgerQueryOutcome::Error,
                        LedgerCacheOutcome::HotMiss,
                        LedgerFillOutcome::Error,
                        LedgerDurableWriteOutcome::NotAttempted,
                        rows.row_count(),
                    );
                    return Err(error);
                }
            }
        }

        self.record_fill(&labels, FillOutcome::LiveFetch, fill_start);
        if let Some((recorder, labels)) = self.metrics_recorder(&labels) {
            recorder.set_latest_filled_block(labels, plan.ledger_range.end());
        }

        rows.sort();
        let mut cache = plan.coverage.clone();
        cache.provider_fill_ranges = provider_fill_ranges;

        let result = NativeQueryExecutionResult {
            chain: plan.chain.clone(),
            dataset_key: plan.dataset_key.clone(),
            ledger_range: plan.ledger_range.clone(),
            cache,
            rows: DatasetRows::new(plan.dataset_key.clone(), rows)?,
        };
        self.record_query(&labels, QueryOutcome::HotMiss, start);
        self.record_usage_for_plan(
            &query_id,
            &ledger_application,
            &plan,
            LedgerQueryOutcome::HotMiss,
            LedgerCacheOutcome::HotMiss,
            LedgerFillOutcome::LiveFetch,
            LedgerDurableWriteOutcome::NotAttempted,
            result.rows.row_count(),
        );
        Ok(result)
    }

    fn fetch_with_provider_limit_splits(
        &self,
        fetch_request: ChainFetchRequest,
        query_id: &str,
    ) -> Result<Vec<ProviderFetchResponse>, DatalensError> {
        let mut responses = Vec::new();
        let mut queue = VecDeque::from([fetch_request]);
        while let Some(fetch_request) = queue.pop_front() {
            match self.source.fetch(fetch_request.clone()) {
                Ok(response) => responses.push((fetch_request, response)),
                Err(error)
                    if error.kind == DatalensErrorKind::ProviderLimit
                        && fetch_request.range.len() > 1 =>
                {
                    log::warn!(
                        "provider limit split query_id={} dataset={} range={}-{}",
                        query_id,
                        fetch_request.dataset_key.as_str(),
                        fetch_request.range.start(),
                        fetch_request.range.end()
                    );
                    for range in split_provider_limit_range(&fetch_request.range)?
                        .into_iter()
                        .rev()
                    {
                        queue.push_front(ChainFetchRequest {
                            range,
                            ..fetch_request.clone()
                        });
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(responses)
    }

    fn ledger_application(
        &self,
        application: Option<ApplicationIdentity>,
    ) -> Option<ApplicationIdentity> {
        self.usage_ledger
            .as_ref()
            .map(|ledger| application.unwrap_or_else(|| ledger.application.clone()))
    }

    fn metrics_labels(
        &self,
        input: &NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Option<MetricsLabels> {
        self.metrics.as_ref().map(|metrics| {
            MetricsLabels::from_dataset_key(
                application.unwrap_or_else(|| metrics.application.clone()),
                input.chain.clone(),
                input.dataset_key.clone(),
            )
        })
    }

    fn watermark_application(
        &self,
        application: &Option<ApplicationIdentity>,
    ) -> Option<ApplicationIdentity> {
        self.query_watermarks.as_ref().map(|watermarks| {
            application
                .clone()
                .unwrap_or_else(|| watermarks.application.clone())
        })
    }

    fn metrics_recorder<'a>(
        &'a self,
        labels: &'a Option<MetricsLabels>,
    ) -> Option<(&'a MetricsRecorder, &'a MetricsLabels)> {
        self.metrics
            .as_ref()
            .zip(labels.as_ref())
            .map(|(metrics, labels)| (metrics.recorder.as_ref(), labels))
    }

    fn record_query(&self, labels: &Option<MetricsLabels>, outcome: QueryOutcome, start: Instant) {
        if let Some((recorder, labels)) = self.metrics_recorder(labels) {
            recorder.record_query(labels, outcome);
            recorder.observe_query_duration(labels, start.elapsed().as_secs_f64());
        }
    }

    fn record_cache_coverage(&self, labels: &Option<MetricsLabels>, outcome: CacheCoverageOutcome) {
        if let Some((recorder, labels)) = self.metrics_recorder(labels) {
            recorder.record_cache_coverage(labels, outcome);
        }
    }

    fn record_fill(&self, labels: &Option<MetricsLabels>, outcome: FillOutcome, start: Instant) {
        if let Some((recorder, labels)) = self.metrics_recorder(labels) {
            recorder.record_fill(labels, outcome);
            recorder.observe_fill_duration(labels, start.elapsed().as_secs_f64());
        }
    }

    fn record_durable_write(
        &self,
        labels: &Option<MetricsLabels>,
        outcome: MetricsDurableWriteOutcome,
    ) {
        if let Some((recorder, labels)) = self.metrics_recorder(labels) {
            recorder.record_durable_write(labels, outcome);
        }
    }

    fn record_error(&self, labels: &Option<MetricsLabels>, error: &DatalensError) {
        if let Some((recorder, labels)) = self.metrics_recorder(labels) {
            let error_labels = ErrorLabels::from_labels(labels, error.kind.clone());
            if is_provider_error(&error.kind) {
                recorder.record_provider_error(&error_labels);
            }
            if is_storage_error(&error.kind) {
                recorder.record_storage_error(&error_labels);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_usage(
        &self,
        query_id: &str,
        application: &Option<ApplicationIdentity>,
        input: &NativeQueryInput,
        finality_level: FinalityLevel,
        query_outcome: LedgerQueryOutcome,
        cache_outcome: LedgerCacheOutcome,
        fill_outcome: LedgerFillOutcome,
        durable_write_outcome: LedgerDurableWriteOutcome,
        row_count: usize,
    ) {
        let Some(ledger) = &self.usage_ledger else {
            return;
        };
        let application = application
            .as_ref()
            .unwrap_or(&ledger.application)
            .as_str()
            .to_owned();
        let entry = UsageLedgerEntry::query_event(
            application,
            input.chain.clone(),
            input.dataset_key.clone(),
            &input.selector,
            input.ledger_range.clone(),
            finality_level,
            query_outcome,
            cache_outcome,
            fill_outcome,
            row_count,
        )
        .with_requested_hot(input.finality.allows_hot())
        .with_durable_write_outcome(durable_write_outcome)
        .with_request_id(query_id.to_owned());
        spawn_usage_ledger_append(ledger.repository.clone(), entry);
    }

    #[allow(clippy::too_many_arguments)]
    fn record_usage_for_plan(
        &self,
        query_id: &str,
        application: &Option<ApplicationIdentity>,
        plan: &datalens_planner::NativeQueryPlan,
        query_outcome: LedgerQueryOutcome,
        cache_outcome: LedgerCacheOutcome,
        fill_outcome: LedgerFillOutcome,
        durable_write_outcome: LedgerDurableWriteOutcome,
        row_count: usize,
    ) {
        self.record_usage(
            query_id,
            application,
            &NativeQueryInput {
                chain: plan.chain.clone(),
                dataset_key: plan.dataset_key.clone(),
                ledger_range: plan.ledger_range.clone(),
                selector: plan.selector.clone(),
                field_selection: plan.field_selection.clone(),
                finality: plan.requested_finality,
            },
            match &plan.finality_policy {
                FinalityPolicy::DurableCache { boundary } => boundary.finality,
                FinalityPolicy::HotReadThrough { latest } => latest.finality,
                FinalityPolicy::MixedReadThrough { latest, .. } => latest.finality,
            },
            query_outcome,
            cache_outcome,
            fill_outcome,
            durable_write_outcome,
            row_count,
        )
    }

    fn record_query_watermark_for_plan(
        &self,
        query_id: &str,
        application: &Option<ApplicationIdentity>,
        plan: &datalens_planner::NativeQueryPlan,
    ) {
        let Some(watermarks) = &self.query_watermarks else {
            return;
        };
        let Some(application) = self.watermark_application(application) else {
            return;
        };
        let Some(latest_block) = durable_watermark_block(plan) else {
            return;
        };
        let updated_at_unix_seconds = match unix_seconds_now() {
            Ok(updated_at_unix_seconds) => updated_at_unix_seconds,
            Err(error) => {
                log::warn!(
                    "query metadata build failed metadata_kind=query_watermark query_id={} chain={} dataset={} range={}-{} kind={:?} message={}",
                    query_id,
                    plan.chain.configured_name(),
                    plan.dataset_key.as_str(),
                    plan.ledger_range.start(),
                    plan.ledger_range.end(),
                    error.kind,
                    error.message
                );
                return;
            }
        };
        let watermark = QueryWatermark {
            key: QueryWatermarkKey::new(
                application.as_str(),
                plan.chain.clone(),
                plan.dataset_key.clone(),
                &plan.selector,
                plan.ledger_range.kind(),
            ),
            latest_block,
            updated_at_unix_seconds,
        };
        spawn_query_watermark_update(
            watermarks.repository.clone(),
            query_id.to_owned(),
            watermark,
            plan.ledger_range.start(),
            plan.ledger_range.end(),
        );
    }
}

fn spawn_usage_ledger_append(repository: Arc<dyn UsageLedgerRepository>, entry: UsageLedgerEntry) {
    let enqueue_start = Instant::now();
    let query_id = entry
        .request_id
        .clone()
        .unwrap_or_else(|| "unknown".to_owned());
    let application_id = entry.application_id.clone();
    let chain = entry.chain.configured_name().to_owned();
    let dataset = entry.dataset_key.as_str().to_owned();
    let range_start = entry.range.start();
    let range_end = entry.range.end();
    let query_outcome = entry.query_outcome;
    let builder = thread::Builder::new().name("datalens-usage-ledger".to_owned());
    let thread_query_id = query_id.clone();
    let thread_application_id = application_id.clone();
    let thread_chain = chain.clone();
    let thread_dataset = dataset.clone();
    match builder.spawn(move || {
        let start = Instant::now();
        match repository.append(&entry) {
            Ok(()) => log::info!(
                "query metadata background write completed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} query_outcome={:?} duration_ms={}",
                thread_query_id,
                thread_application_id,
                thread_chain,
                thread_dataset,
                range_start,
                range_end,
                query_outcome,
                elapsed_ms(start)
            ),
            Err(error) => log::warn!(
                "query metadata background write failed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} query_outcome={:?} duration_ms={} kind={:?} message={}",
                thread_query_id,
                thread_application_id,
                thread_chain,
                thread_dataset,
                range_start,
                range_end,
                query_outcome,
                elapsed_ms(start),
                error.kind,
                error.message
            ),
        }
    }) {
        Ok(_handle) => log::debug!(
            "query metadata enqueue completed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} duration_ms={}",
            query_id,
            application_id,
            chain,
            dataset,
            range_start,
            range_end,
            elapsed_ms(enqueue_start)
        ),
        Err(error) => log::warn!(
            "query metadata enqueue failed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} duration_ms={} message={}",
            query_id,
            application_id,
            chain,
            dataset,
            range_start,
            range_end,
            elapsed_ms(enqueue_start),
            error
        ),
    }
}

fn spawn_query_watermark_update(
    repository: Arc<dyn QueryWatermarkRepository>,
    query_id: String,
    watermark: QueryWatermark,
    range_start: u64,
    range_end: u64,
) {
    let enqueue_start = Instant::now();
    let application_id = watermark.key.application_id.clone();
    let chain = watermark.key.chain.configured_name().to_owned();
    let dataset = watermark.key.dataset_key.as_str().to_owned();
    let latest_block = watermark.latest_block;
    let builder = thread::Builder::new().name("datalens-query-watermark".to_owned());
    let thread_query_id = query_id.clone();
    let thread_application_id = application_id.clone();
    let thread_chain = chain.clone();
    let thread_dataset = dataset.clone();
    match builder.spawn(move || {
        let start = Instant::now();
        match repository.update(&watermark) {
            Ok(()) => log::info!(
                "query metadata background write completed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={}",
                thread_query_id,
                thread_application_id,
                thread_chain,
                thread_dataset,
                range_start,
                range_end,
                latest_block,
                elapsed_ms(start)
            ),
            Err(error) => log::warn!(
                "query metadata background write failed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={} kind={:?} message={}",
                thread_query_id,
                thread_application_id,
                thread_chain,
                thread_dataset,
                range_start,
                range_end,
                latest_block,
                elapsed_ms(start),
                error.kind,
                error.message
            ),
        }
    }) {
        Ok(_handle) => log::debug!(
            "query metadata enqueue completed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={}",
            query_id,
            application_id,
            chain,
            dataset,
            range_start,
            range_end,
            latest_block,
            elapsed_ms(enqueue_start)
        ),
        Err(error) => log::warn!(
            "query metadata enqueue failed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={} message={}",
            query_id,
            application_id,
            chain,
            dataset,
            range_start,
            range_end,
            latest_block,
            elapsed_ms(enqueue_start),
            error
        ),
    }
}

fn elapsed_ms(start: Instant) -> u128 {
    start.elapsed().as_millis()
}

fn split_provider_limit_range(range: &LedgerRange) -> Result<Vec<LedgerRange>, DatalensError> {
    let first_len = u64::try_from(range.len() / 2).unwrap_or(u64::MAX).max(1);
    let first_end = range.start().saturating_add(first_len - 1);
    Ok(vec![
        LedgerRange::try_new(range.kind(), range.start(), first_end)?,
        LedgerRange::try_new(range.kind(), first_end + 1, range.end())?,
    ])
}

fn durable_watermark_block(plan: &datalens_planner::NativeQueryPlan) -> Option<u64> {
    match &plan.finality_policy {
        FinalityPolicy::DurableCache { boundary } => {
            Some(plan.ledger_range.end().min(boundary.value))
        }
        FinalityPolicy::MixedReadThrough {
            durable_boundary, ..
        } => Some(plan.ledger_range.end().min(durable_boundary.value)),
        FinalityPolicy::HotReadThrough { .. } => None,
    }
}

fn unix_seconds_now() -> Result<u64, DatalensError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::internal(format!("system clock before unix epoch: {error}"))
        })
}

fn base36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_owned();
    }
    let mut encoded = Vec::new();
    while value > 0 {
        encoded.push(DIGITS[(value % 36) as usize] as char);
        value /= 36;
    }
    encoded.iter().rev().collect()
}

#[allow(clippy::too_many_arguments)]
fn cache_diagnostic_summary(
    query_id: &str,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    ledger_range: &LedgerRange,
    selector: &datalens_chain::DatasetSelector,
    cache_status: datalens_planner::QueryPlanStatus,
    cache_outcome: CacheCoverageOutcome,
    hit_ranges: &[LedgerRange],
    missing_ranges: &[LedgerRange],
    durable_hit_ranges: &[LedgerRange],
    hot_hit_ranges: &[LedgerRange],
    provider_fill_ranges: &[LedgerRange],
) -> String {
    format!(
        "query_id={} chain={} dataset={} range={}-{} selector_kind={:?} selector_fingerprint={} cache_status={:?} cache_outcome={:?} hit_ranges={} missing_ranges={} durable_hit_ranges={} hot_hit_ranges={} provider_fill_ranges={}",
        query_id,
        chain.configured_name(),
        dataset_key.as_str(),
        ledger_range.start(),
        ledger_range.end(),
        selector.kind(),
        selector.fingerprint(),
        cache_status,
        cache_outcome,
        format_ranges(hit_ranges),
        format_ranges(missing_ranges),
        format_ranges(durable_hit_ranges),
        format_ranges(hot_hit_ranges),
        format_ranges(provider_fill_ranges),
    )
}

fn format_ranges(ranges: &[LedgerRange]) -> String {
    let formatted = ranges
        .iter()
        .map(|range| format!("{}-{}", range.start(), range.end()))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}[{}]", ranges.len(), formatted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_diagnostic_summary_includes_query_context_and_compact_ranges() {
        let summary = cache_diagnostic_summary(
            "q-abc",
            &ChainIdentity::expect_new(datalens_core::ChainFamily::Evm, "ethereum"),
            &DatasetKey::evm_blocks(),
            &LedgerRange::blocks(10, 15).expect("range"),
            &datalens_chain::DatasetSelector::all(),
            datalens_planner::QueryPlanStatus::PartialHit,
            CacheCoverageOutcome::PartialHit,
            &[
                LedgerRange::blocks(10, 11).expect("range"),
                LedgerRange::blocks(14, 14).expect("range"),
            ],
            &[LedgerRange::blocks(12, 13).expect("range")],
            &[LedgerRange::blocks(10, 11).expect("range")],
            &[],
            &[LedgerRange::blocks(12, 13).expect("range")],
        );

        assert_eq!(
            summary,
            "query_id=q-abc chain=ethereum dataset=evm.blocks range=10-15 selector_kind=All selector_fingerprint=all cache_status=PartialHit cache_outcome=PartialHit hit_ranges=2[10-11,14-14] missing_ranges=1[12-13] durable_hit_ranges=1[10-11] hot_hit_ranges=0[] provider_fill_ranges=1[12-13]"
        );
    }
}
