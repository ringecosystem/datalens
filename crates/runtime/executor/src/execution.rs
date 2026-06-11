use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, Once, OnceLock,
        atomic::{AtomicU64, Ordering},
        mpsc,
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
    ApplicationIdentity, CacheCoverageOutcome, DurableIntentOutcome as MetricsDurableIntentOutcome,
    DurableWriteOutcome as MetricsDurableWriteOutcome, ErrorLabels, FillOutcome, MetricsLabels,
    MetricsRecorder, QueryMetadataEnqueueOutcome, QueryMetadataWriteOutcome, QueryOutcome,
};
use datalens_planner::{
    CoverageSummary, FinalityPolicy, NativePlanner, NativePlannerConfig, NativeQueryInput,
};
use datalens_storage::{
    CacheOutcome as LedgerCacheOutcome, DurableIntentSubmissionOutcome,
    DurableIntentSubmissionRequest, DurableIntentSubmissionService,
    DurablePromotionIntentRepository, DurablePromotionIntentSource,
    DurableWriteOutcome as LedgerDurableWriteOutcome, FillOutcome as LedgerFillOutcome,
    QueryActivity, QueryActivityKey, QueryActivityRepository, QueryOutcome as LedgerQueryOutcome,
    QueryWatermark, QueryWatermarkKey, QueryWatermarkRepository, StorageRepository,
    UsageLedgerEntry, UsageLedgerRepository,
};
use datalens_writer::{
    DurableWriteResult, DurableWriteSegment, DurableWriter, DurableWriterConfig,
};

use crate::helpers::*;
pub use crate::hot_promotion::HotCachePromoter;
use crate::{
    durable_promotion::{
        DurablePromotionIntentWorker, DurablePromotionMetrics, DurablePromotionQueue,
        DurablePromotionRequest, PromotionEnqueueOutcome,
    },
    provider_range::{ProviderRangeController, ProviderRangeKey},
    provider_singleflight::ProviderSingleflight,
};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryMetadataWorkerConfig {
    pub queue_capacity: usize,
    pub worker_threads: usize,
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
    durable_promotions: DurablePromotionQueue<R>,
    provider_singleflight: ProviderSingleflight,
    provider_ranges: ProviderRangeController,
    metrics: Option<ExecutorMetrics>,
    usage_ledger: Option<ExecutorUsageLedger>,
    query_watermarks: Option<ExecutorQueryWatermarks>,
    query_activity: Option<ExecutorQueryActivity>,
    durable_intents: Option<Arc<dyn DurablePromotionIntentRepository>>,
    durable_intent_worker: Option<DurablePromotionIntentWorker<R, S>>,
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

#[derive(Clone)]
pub(crate) struct ExecutorQueryActivity {
    pub(crate) repository: Arc<dyn QueryActivityRepository>,
    pub(crate) application: ApplicationIdentity,
}

type ProviderFetchResponse = (ChainFetchRequest, ChainFetchResponse);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DurableIntentPlanSubmissionOutcome {
    Pending,
    Completed,
    Failed,
}

impl<R, S> NativeQueryExecutor<R, S>
where
    R: StorageRepository + Clone + 'static,
    S: ChainAdapter,
{
    pub fn new(storage: R, source: S, config: NativeQueryExecutionConfig) -> Self {
        let writer = DurableWriter::new(storage.clone(), config.writer);
        let durable_promotions =
            DurablePromotionQueue::new(writer.clone()).expect("durable promotion workers start");
        Self {
            storage,
            source,
            planner: NativePlanner::new(config.planner),
            writer,
            durable_promotions,
            provider_singleflight: ProviderSingleflight::default(),
            provider_ranges: ProviderRangeController::default(),
            metrics: None,
            usage_ledger: None,
            query_watermarks: None,
            query_activity: None,
            durable_intents: None,
            durable_intent_worker: None,
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

    pub fn with_query_activity(
        mut self,
        repository: impl QueryActivityRepository + 'static,
        application: ApplicationIdentity,
    ) -> Self {
        self.query_activity = Some(ExecutorQueryActivity {
            repository: Arc::new(repository),
            application,
        });
        self
    }

    pub fn with_durable_intents(
        mut self,
        repository: impl DurablePromotionIntentRepository + 'static,
    ) -> Self {
        self =
            self.with_durable_intents_startup_maintenance_once(repository, Arc::new(Once::new()));
        self
    }

    pub fn with_durable_intents_startup_maintenance_once(
        mut self,
        repository: impl DurablePromotionIntentRepository + 'static,
        startup_maintenance_once: Arc<Once>,
    ) -> Self {
        let repository: Arc<dyn DurablePromotionIntentRepository> = Arc::new(repository);
        self.durable_intent_worker = Some(
            DurablePromotionIntentWorker::start_with_startup_maintenance_once(
                repository.clone(),
                self.writer.clone(),
                self.source.clone(),
                self.metrics
                    .as_ref()
                    .map(|metrics| metrics.recorder.clone()),
                self.provider_ranges.clone(),
                startup_maintenance_once,
            )
            .expect("durable intent workers start"),
        );
        self.durable_intents = Some(repository);
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
        self.durable_promotions.wait_for_idle()?;
        self.writer.flush()
    }

    pub fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError> {
        self.durable_promotions.wait_for_idle()?;
        self.writer.flush_for_shutdown()
    }

    pub fn durable_writer(&self) -> DurableWriter<R> {
        self.writer.clone()
    }

    pub fn wait_for_durable_promotions(&self) -> Result<(), DatalensError> {
        self.durable_promotions.wait_for_idle()
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
        let mut promotion_pending_ranges = Vec::new();
        if cache_fill_attempted {
            let pending_ranges = fetched_segments
                .iter()
                .map(|segment| segment.range.clone())
                .collect::<Vec<_>>();
            if self.durable_intents.is_some() {
                match self.submit_durable_intent_for_plan(
                    &query_id,
                    &ledger_application,
                    &plan,
                    finality_level,
                    pending_ranges.clone(),
                ) {
                    DurableIntentPlanSubmissionOutcome::Pending => {
                        promotion_pending_ranges = pending_ranges;
                        durable_write_outcome = LedgerDurableWriteOutcome::Staged;
                        self.record_durable_write(&labels, MetricsDurableWriteOutcome::Staged);
                        log::info!(
                            "durable promotion intent pending query_id={} dataset={} range={}-{} pending_ranges={}",
                            query_id,
                            plan.dataset_key.as_str(),
                            plan.ledger_range.start(),
                            plan.ledger_range.end(),
                            promotion_pending_ranges.len()
                        );
                    }
                    DurableIntentPlanSubmissionOutcome::Completed => {}
                    DurableIntentPlanSubmissionOutcome::Failed => {
                        self.record_durable_write(
                            &labels,
                            MetricsDurableWriteOutcome::StorageError,
                        );
                        durable_write_outcome = LedgerDurableWriteOutcome::StorageError;
                    }
                }
            } else {
                let enqueue_start = Instant::now();
                match self.durable_promotions.enqueue(DurablePromotionRequest {
                    query_id: query_id.clone(),
                    chain: plan.chain.clone(),
                    dataset_key: plan.dataset_key.clone(),
                    selector: plan.selector.clone(),
                    finality_level,
                    segments: fetched_segments,
                    metrics: self.promotion_metrics(&labels),
                }) {
                    Ok(PromotionEnqueueOutcome::Queued) => {
                        promotion_pending_ranges = pending_ranges;
                        durable_write_outcome = LedgerDurableWriteOutcome::Staged;
                        self.record_durable_write(&labels, MetricsDurableWriteOutcome::Staged);
                        log::info!(
                            "durable promotion enqueued query_id={} dataset={} range={}-{} pending_ranges={} duration_ms={}",
                            query_id,
                            plan.dataset_key.as_str(),
                            plan.ledger_range.start(),
                            plan.ledger_range.end(),
                            promotion_pending_ranges.len(),
                            enqueue_start.elapsed().as_millis()
                        );
                    }
                    Ok(PromotionEnqueueOutcome::AlreadyInFlight) => {
                        promotion_pending_ranges = pending_ranges;
                        durable_write_outcome = LedgerDurableWriteOutcome::Staged;
                        self.record_durable_write(&labels, MetricsDurableWriteOutcome::Staged);
                        log::info!(
                            "durable promotion already in flight query_id={} dataset={} range={}-{} pending_ranges={} duration_ms={}",
                            query_id,
                            plan.dataset_key.as_str(),
                            plan.ledger_range.start(),
                            plan.ledger_range.end(),
                            promotion_pending_ranges.len(),
                            enqueue_start.elapsed().as_millis()
                        );
                    }
                    Ok(PromotionEnqueueOutcome::Rejected) => {
                        self.record_durable_write(
                            &labels,
                            MetricsDurableWriteOutcome::StorageError,
                        );
                        durable_write_outcome = LedgerDurableWriteOutcome::StorageError;
                        log::error!(
                            "durable promotion enqueue rejected query_id={} dataset={} range={}-{} duration_ms={}",
                            query_id,
                            plan.dataset_key.as_str(),
                            plan.ledger_range.start(),
                            plan.ledger_range.end(),
                            enqueue_start.elapsed().as_millis()
                        );
                    }
                    Err(error) => {
                        log::error!(
                            "durable promotion enqueue failed query_id={} dataset={} range={}-{} kind={:?}",
                            query_id,
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
                    }
                };
            }
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
        cache.promotion_pending_ranges = promotion_pending_ranges;

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
        self.record_query_activity_for_plan(&query_id, &application, &plan);
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
        let capability_max_len = dataset_capability_max_range_len(
            &self.source.capabilities(),
            &fetch_request.dataset_key,
        );
        let range_key = ProviderRangeKey::from_request(&fetch_request);
        let effective_max_len = self
            .provider_ranges
            .effective_limit(&range_key, capability_max_len);
        let initial_requests = split_fetch_request_by_max_len(&fetch_request, effective_max_len)?;
        if initial_requests.len() > 1 {
            log::info!(
                "provider fetch pre-split query_id={} dataset={} range={}-{} target_max_len={:?} chunks={}",
                query_id,
                fetch_request.dataset_key.as_str(),
                fetch_request.range.start(),
                fetch_request.range.end(),
                effective_max_len,
                initial_requests.len()
            );
        }
        let mut responses = Vec::new();
        let mut queue = VecDeque::from(initial_requests);
        while let Some(fetch_request) = queue.pop_front() {
            let fetch_start = Instant::now();
            match self
                .provider_singleflight
                .fetch(&fetch_request, || self.source.fetch(fetch_request.clone()))
            {
                Ok(outcome) => {
                    log::info!(
                        "provider fetch completed query_id={} dataset={} range={}-{} shared={} duration_ms={}",
                        query_id,
                        fetch_request.dataset_key.as_str(),
                        fetch_request.range.start(),
                        fetch_request.range.end(),
                        outcome.shared,
                        fetch_start.elapsed().as_millis()
                    );
                    self.provider_ranges.record_success(
                        &range_key,
                        capability_max_len,
                        fetch_request.range.len(),
                    );
                    responses.push((fetch_request, outcome.response));
                }
                Err(error)
                    if error.kind == DatalensErrorKind::ProviderLimit
                        && fetch_request.range.len() > 1 =>
                {
                    let hint_max_len = parse_provider_limit_hint(&error.message);
                    let split_target = self.provider_ranges.record_provider_limit(
                        &range_key,
                        capability_max_len,
                        fetch_request.range.len(),
                        hint_max_len,
                    );
                    let split_ranges =
                        split_provider_limit_range(&fetch_request.range, split_target)?;
                    log::warn!(
                        "provider limit split query_id={} dataset={} range={}-{} target_max_len={:?} configured_max_len={:?} hint_max_len={:?} chunks={} duration_ms={}",
                        query_id,
                        fetch_request.dataset_key.as_str(),
                        fetch_request.range.start(),
                        fetch_request.range.end(),
                        split_target,
                        capability_max_len,
                        hint_max_len,
                        split_ranges.len(),
                        fetch_start.elapsed().as_millis()
                    );
                    for range in split_ranges.into_iter().rev() {
                        queue.push_front(ChainFetchRequest {
                            range,
                            ..fetch_request.clone()
                        });
                    }
                }
                Err(error) => {
                    log::warn!(
                        "provider fetch failed query_id={} dataset={} range={}-{} kind={:?} duration_ms={}",
                        query_id,
                        fetch_request.dataset_key.as_str(),
                        fetch_request.range.start(),
                        fetch_request.range.end(),
                        error.kind,
                        fetch_start.elapsed().as_millis()
                    );
                    return Err(error);
                }
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

    fn activity_application(
        &self,
        application: &Option<ApplicationIdentity>,
    ) -> Option<ApplicationIdentity> {
        self.query_activity.as_ref().map(|activity| {
            application
                .clone()
                .unwrap_or_else(|| activity.application.clone())
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

    fn promotion_metrics(&self, labels: &Option<MetricsLabels>) -> Option<DurablePromotionMetrics> {
        self.metrics
            .as_ref()
            .zip(labels.as_ref())
            .map(|(metrics, labels)| DurablePromotionMetrics {
                recorder: metrics.recorder.clone(),
                labels: labels.clone(),
            })
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
        enqueue_usage_ledger_append(
            ledger.repository.clone(),
            entry,
            self.metrics
                .as_ref()
                .map(|metrics| metrics.recorder.clone()),
        );
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

    fn submit_durable_intent_for_plan(
        &self,
        query_id: &str,
        application: &Option<ApplicationIdentity>,
        plan: &datalens_planner::NativeQueryPlan,
        finality_level: FinalityLevel,
        ranges: Vec<LedgerRange>,
    ) -> DurableIntentPlanSubmissionOutcome {
        let Some(repository) = &self.durable_intents else {
            return DurableIntentPlanSubmissionOutcome::Failed;
        };
        let Ok(now_unix_seconds) = unix_seconds_now() else {
            log::error!(
                "durable intent scheduling skipped query_id={} dataset={} range={}-{} reason=clock",
                query_id,
                plan.dataset_key.as_str(),
                plan.ledger_range.start(),
                plan.ledger_range.end()
            );
            return DurableIntentPlanSubmissionOutcome::Failed;
        };
        let application = application
            .as_ref()
            .map(ApplicationIdentity::as_str)
            .unwrap_or("default")
            .to_owned();
        let application_label = application.clone();
        let service = DurableIntentSubmissionService::new(repository.clone());
        match service.submit(DurableIntentSubmissionRequest {
            source: DurablePromotionIntentSource::Query,
            application,
            chain: plan.chain.clone(),
            dataset_key: plan.dataset_key.clone(),
            selector: plan.selector.clone(),
            finality: finality_level,
            ranges,
            request_id: Some(query_id.to_owned()),
            task_id: None,
            now_unix_seconds,
        }) {
            DurableIntentSubmissionOutcome::Submitted(intent) => {
                self.record_durable_intent(
                    plan,
                    &intent.application,
                    "query",
                    MetricsDurableIntentOutcome::Submitted,
                );
                log::info!(
                    "durable intent submitted source=query query_id={} intent_id={} chain_key={} dataset={} selector_fingerprint={} ranges={}",
                    query_id,
                    intent.intent_id,
                    intent.chain.key_prefix(),
                    intent.dataset_key.as_str(),
                    intent.selector_fingerprint,
                    format_ranges(&intent.ranges)
                );
                DurableIntentPlanSubmissionOutcome::Pending
            }
            DurableIntentSubmissionOutcome::AlreadyPending(intent) => {
                self.record_durable_intent(
                    plan,
                    &intent.application,
                    "query",
                    MetricsDurableIntentOutcome::AlreadyPending,
                );
                log::info!(
                    "durable intent already pending source=query query_id={} intent_id={} chain_key={} dataset={} selector_fingerprint={} ranges={}",
                    query_id,
                    intent.intent_id,
                    intent.chain.key_prefix(),
                    intent.dataset_key.as_str(),
                    intent.selector_fingerprint,
                    format_ranges(&intent.ranges)
                );
                DurableIntentPlanSubmissionOutcome::Pending
            }
            DurableIntentSubmissionOutcome::AlreadyCompleted(intent) => {
                self.record_durable_intent(
                    plan,
                    &intent.application,
                    "query",
                    MetricsDurableIntentOutcome::AlreadyCompleted,
                );
                log::info!(
                    "durable intent already completed source=query query_id={} intent_id={} chain_key={} dataset={} selector_fingerprint={} ranges={}",
                    query_id,
                    intent.intent_id,
                    intent.chain.key_prefix(),
                    intent.dataset_key.as_str(),
                    intent.selector_fingerprint,
                    format_ranges(&intent.ranges)
                );
                DurableIntentPlanSubmissionOutcome::Completed
            }
            DurableIntentSubmissionOutcome::Failed(error) => {
                self.record_durable_intent(
                    plan,
                    &application_label,
                    "query",
                    MetricsDurableIntentOutcome::Error,
                );
                log::error!(
                    "durable intent scheduling failed source=query query_id={} dataset={} range={}-{} kind={:?} message={}",
                    query_id,
                    plan.dataset_key.as_str(),
                    plan.ledger_range.start(),
                    plan.ledger_range.end(),
                    error.kind,
                    error.message
                );
                DurableIntentPlanSubmissionOutcome::Failed
            }
        }
    }

    fn record_durable_intent(
        &self,
        plan: &datalens_planner::NativeQueryPlan,
        application: &str,
        source: &str,
        outcome: MetricsDurableIntentOutcome,
    ) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let labels = MetricsLabels::from_dataset_key(
            ApplicationIdentity::named(application.to_owned()),
            plan.chain.clone(),
            plan.dataset_key.clone(),
        );
        metrics
            .recorder
            .record_durable_intent(&labels, source, outcome);
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
        enqueue_query_watermark_update(
            watermarks.repository.clone(),
            query_id.to_owned(),
            watermark,
            plan.ledger_range.start(),
            plan.ledger_range.end(),
            self.metrics
                .as_ref()
                .map(|metrics| metrics.recorder.clone()),
        );
    }

    fn record_query_activity_for_plan(
        &self,
        query_id: &str,
        application: &Option<ApplicationIdentity>,
        plan: &datalens_planner::NativeQueryPlan,
    ) {
        let Some(activities) = &self.query_activity else {
            return;
        };
        let Some(application) = self.activity_application(application) else {
            return;
        };
        let Some(latest_range) = durable_activity_range(plan) else {
            return;
        };
        let updated_at_unix_seconds = match unix_seconds_now() {
            Ok(updated_at_unix_seconds) => updated_at_unix_seconds,
            Err(error) => {
                log::warn!(
                    "query metadata build failed metadata_kind=query_activity query_id={} chain={} dataset={} range={}-{} kind={:?} message={}",
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
        let activity = QueryActivity {
            key: QueryActivityKey::new(
                application.as_str(),
                plan.chain.clone(),
                plan.dataset_key.clone(),
                &plan.selector,
                plan.ledger_range.kind(),
            ),
            latest_range,
            updated_at_unix_seconds,
            request_id: Some(query_id.to_owned()),
        };
        enqueue_query_activity_update(
            activities.repository.clone(),
            query_id.to_owned(),
            activity,
            plan.ledger_range.start(),
            plan.ledger_range.end(),
            self.metrics
                .as_ref()
                .map(|metrics| metrics.recorder.clone()),
        );
    }
}

const DEFAULT_QUERY_METADATA_QUEUE_CAPACITY: usize = 8192;
const DEFAULT_QUERY_METADATA_WORKER_THREADS: usize = 4;

impl Default for QueryMetadataWorkerConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUERY_METADATA_QUEUE_CAPACITY,
            worker_threads: DEFAULT_QUERY_METADATA_WORKER_THREADS,
        }
    }
}

pub fn configure_query_metadata_worker_pool(config: QueryMetadataWorkerConfig) {
    QUERY_METADATA_WORKER_POOL.get_or_init(|| {
        MetadataWorkerPool::new(config.queue_capacity.max(1), config.worker_threads.max(1))
    });
}

static QUERY_METADATA_WORKER_POOL: OnceLock<MetadataWorkerPool> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataEnqueueOutcome {
    Enqueued,
    Coalesced,
    Full,
    Closed,
}

struct MetadataWorkerPool {
    inner: Arc<MetadataWorkerPoolInner>,
}

struct MetadataWorkerPoolInner {
    sender: mpsc::SyncSender<MetadataJob>,
    receiver: Mutex<mpsc::Receiver<MetadataJob>>,
    pending_watermarks: Mutex<BTreeMap<String, CoalescedQueryWatermark>>,
    pending_activities: Mutex<BTreeMap<String, CoalescedQueryActivity>>,
}

struct CoalescedQueryWatermark {
    repository: Arc<dyn QueryWatermarkRepository>,
    watermark: QueryWatermark,
    context: QueryWatermarkMetadataContext,
    metrics: Option<Arc<MetricsRecorder>>,
}

struct CoalescedQueryActivity {
    repository: Arc<dyn QueryActivityRepository>,
    activity: QueryActivity,
    context: QueryActivityMetadataContext,
    metrics: Option<Arc<MetricsRecorder>>,
}

impl MetadataWorkerPool {
    fn new(capacity: usize, worker_count: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let inner = Arc::new(MetadataWorkerPoolInner {
            sender,
            receiver: Mutex::new(receiver),
            pending_watermarks: Mutex::new(BTreeMap::new()),
            pending_activities: Mutex::new(BTreeMap::new()),
        });
        for worker_index in 0..worker_count {
            let inner = inner.clone();
            let name = format!("datalens-query-metadata-{worker_index}");
            if let Err(error) = thread::Builder::new().name(name).spawn(move || {
                loop {
                    let job = {
                        let receiver = match inner.receiver.lock() {
                            Ok(receiver) => receiver,
                            Err(error) => {
                                log::warn!("query metadata worker lock failed message={error}");
                                return;
                            }
                        };
                        receiver.recv()
                    };
                    match job {
                        Ok(job) => {
                            process_metadata_job(job);
                            inner.flush_coalesced();
                        }
                        Err(_) => return,
                    }
                }
            }) {
                log::warn!(
                    "query metadata worker spawn failed worker_index={} message={}",
                    worker_index,
                    error
                );
            }
        }
        Self { inner }
    }

    #[cfg(test)]
    fn new_for_test(capacity: usize, worker_count: usize) -> Self {
        Self::new(capacity, worker_count)
    }

    fn enqueue(&self, job: MetadataJob) -> MetadataEnqueueOutcome {
        self.inner.enqueue(job)
    }

    #[cfg(test)]
    fn flush_coalesced(&self) {
        self.inner.flush_coalesced();
    }

    #[cfg(test)]
    fn recv_for_test(&self) -> Option<MetadataJob> {
        self.inner
            .receiver
            .lock()
            .expect("metadata worker receiver")
            .try_recv()
            .ok()
    }

    #[cfg(test)]
    fn pending_latest_state_counts_for_test(&self) -> (usize, usize) {
        (
            self.inner
                .pending_watermarks
                .lock()
                .expect("pending watermarks")
                .len(),
            self.inner
                .pending_activities
                .lock()
                .expect("pending activities")
                .len(),
        )
    }
}

impl MetadataWorkerPoolInner {
    fn enqueue(&self, job: MetadataJob) -> MetadataEnqueueOutcome {
        match self.sender.try_send(job) {
            Ok(()) => MetadataEnqueueOutcome::Enqueued,
            Err(mpsc::TrySendError::Full(job)) => self.coalesce_or_full(job),
            Err(mpsc::TrySendError::Disconnected(_)) => MetadataEnqueueOutcome::Closed,
        }
    }

    fn coalesce_or_full(&self, job: MetadataJob) -> MetadataEnqueueOutcome {
        match job {
            MetadataJob::QueryWatermarkUpdate {
                repository,
                watermark,
                context,
                metrics,
            } => {
                self.coalesce_watermark(repository, watermark, context, metrics);
                MetadataEnqueueOutcome::Coalesced
            }
            MetadataJob::QueryActivityUpdate {
                repository,
                activity,
                context,
                metrics,
            } => {
                self.coalesce_activity(repository, activity, context, metrics);
                MetadataEnqueueOutcome::Coalesced
            }
            _ => MetadataEnqueueOutcome::Full,
        }
    }

    fn coalesce_watermark(
        &self,
        repository: Arc<dyn QueryWatermarkRepository>,
        watermark: QueryWatermark,
        context: QueryWatermarkMetadataContext,
        metrics: Option<Arc<MetricsRecorder>>,
    ) {
        let key = query_watermark_coalesce_key(&watermark.key);
        let mut pending = match self.pending_watermarks.lock() {
            Ok(pending) => pending,
            Err(error) => {
                log::warn!(
                    "query metadata coalesce lock failed metadata_kind=query_watermark message={error}"
                );
                return;
            }
        };
        let incoming = CoalescedQueryWatermark {
            repository,
            watermark,
            context,
            metrics,
        };
        match pending.get(&key) {
            Some(existing) if !watermark_is_newer(&incoming.watermark, &existing.watermark) => {}
            _ => {
                pending.insert(key, incoming);
            }
        }
    }

    fn coalesce_activity(
        &self,
        repository: Arc<dyn QueryActivityRepository>,
        activity: QueryActivity,
        context: QueryActivityMetadataContext,
        metrics: Option<Arc<MetricsRecorder>>,
    ) {
        let key = query_activity_coalesce_key(&activity.key);
        let mut pending = match self.pending_activities.lock() {
            Ok(pending) => pending,
            Err(error) => {
                log::warn!(
                    "query metadata coalesce lock failed metadata_kind=query_activity message={error}"
                );
                return;
            }
        };
        let incoming = CoalescedQueryActivity {
            repository,
            activity,
            context,
            metrics,
        };
        match pending.get(&key) {
            Some(existing) if !activity_is_newer(&incoming.activity, &existing.activity) => {}
            _ => {
                pending.insert(key, incoming);
            }
        }
    }

    fn flush_coalesced(&self) {
        loop {
            let Some(job) = self.pop_coalesced_job() else {
                return;
            };
            match self.sender.try_send(job) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(job)) => {
                    self.requeue_coalesced_job(job);
                    return;
                }
                Err(mpsc::TrySendError::Disconnected(_)) => return,
            }
        }
    }

    fn pop_coalesced_job(&self) -> Option<MetadataJob> {
        if let Some(coalesced) = pop_first(&self.pending_watermarks) {
            return Some(MetadataJob::QueryWatermarkUpdate {
                repository: coalesced.repository,
                watermark: coalesced.watermark,
                context: coalesced.context,
                metrics: coalesced.metrics,
            });
        }
        pop_first(&self.pending_activities).map(|coalesced| MetadataJob::QueryActivityUpdate {
            repository: coalesced.repository,
            activity: coalesced.activity,
            context: coalesced.context,
            metrics: coalesced.metrics,
        })
    }

    fn requeue_coalesced_job(&self, job: MetadataJob) {
        match job {
            MetadataJob::QueryWatermarkUpdate {
                repository,
                watermark,
                context,
                metrics,
            } => self.coalesce_watermark(repository, watermark, context, metrics),
            MetadataJob::QueryActivityUpdate {
                repository,
                activity,
                context,
                metrics,
            } => self.coalesce_activity(repository, activity, context, metrics),
            _ => {}
        }
    }
}

enum MetadataJob {
    UsageLedgerAppend {
        repository: Arc<dyn UsageLedgerRepository>,
        entry: UsageLedgerEntry,
        context: UsageLedgerMetadataContext,
        metrics: Option<Arc<MetricsRecorder>>,
    },
    QueryWatermarkUpdate {
        repository: Arc<dyn QueryWatermarkRepository>,
        watermark: QueryWatermark,
        context: QueryWatermarkMetadataContext,
        metrics: Option<Arc<MetricsRecorder>>,
    },
    QueryActivityUpdate {
        repository: Arc<dyn QueryActivityRepository>,
        activity: QueryActivity,
        context: QueryActivityMetadataContext,
        metrics: Option<Arc<MetricsRecorder>>,
    },
    #[cfg(test)]
    NoopForTest,
}

#[derive(Clone)]
struct QueryMetadataContext {
    query_id: String,
    application_id: String,
    chain: String,
    dataset: String,
    range_start: u64,
    range_end: u64,
}

#[derive(Clone)]
struct UsageLedgerMetadataContext {
    base: QueryMetadataContext,
    query_outcome: LedgerQueryOutcome,
}

#[derive(Clone)]
struct QueryWatermarkMetadataContext {
    base: QueryMetadataContext,
    latest_block: u64,
}

#[derive(Clone)]
struct QueryActivityMetadataContext {
    base: QueryMetadataContext,
    latest_start: u64,
    latest_end: u64,
}

fn metadata_worker_pool() -> &'static MetadataWorkerPool {
    QUERY_METADATA_WORKER_POOL.get_or_init(|| {
        let config = QueryMetadataWorkerConfig::default();
        MetadataWorkerPool::new(config.queue_capacity, config.worker_threads)
    })
}

fn pop_first<T>(pending: &Mutex<BTreeMap<String, T>>) -> Option<T> {
    let mut pending = pending.lock().ok()?;
    let key = pending.keys().next().cloned()?;
    pending.remove(&key)
}

fn query_watermark_coalesce_key(key: &QueryWatermarkKey) -> String {
    format!(
        "application={}|chain={}|dataset={}|range_kind={:?}|selector={}",
        key.application_id,
        key.chain.key_prefix(),
        key.dataset_key.as_str(),
        key.range_kind,
        key.selector_fingerprint,
    )
}

fn query_activity_coalesce_key(key: &QueryActivityKey) -> String {
    format!(
        "application={}|chain={}|dataset={}|range_kind={:?}|selector={}",
        key.application_id,
        key.chain.key_prefix(),
        key.dataset_key.as_str(),
        key.range_kind,
        key.selector_fingerprint,
    )
}

fn watermark_is_newer(incoming: &QueryWatermark, existing: &QueryWatermark) -> bool {
    incoming.latest_block > existing.latest_block
        || (incoming.latest_block == existing.latest_block
            && incoming.updated_at_unix_seconds >= existing.updated_at_unix_seconds)
}

fn activity_is_newer(incoming: &QueryActivity, existing: &QueryActivity) -> bool {
    incoming.latest_range.end() > existing.latest_range.end()
        || (incoming.latest_range.end() == existing.latest_range.end()
            && incoming.updated_at_unix_seconds >= existing.updated_at_unix_seconds)
}

fn metadata_enqueue_metric_outcome(outcome: MetadataEnqueueOutcome) -> QueryMetadataEnqueueOutcome {
    match outcome {
        MetadataEnqueueOutcome::Enqueued => QueryMetadataEnqueueOutcome::Enqueued,
        MetadataEnqueueOutcome::Coalesced => QueryMetadataEnqueueOutcome::Coalesced,
        MetadataEnqueueOutcome::Full => QueryMetadataEnqueueOutcome::Dropped,
        MetadataEnqueueOutcome::Closed => QueryMetadataEnqueueOutcome::Closed,
    }
}

fn record_query_metadata_enqueue(
    metrics: Option<&Arc<MetricsRecorder>>,
    labels: &MetricsLabels,
    metadata_kind: &str,
    outcome: QueryMetadataEnqueueOutcome,
) {
    if let Some(metrics) = metrics {
        metrics.record_query_metadata_enqueue(labels, metadata_kind, outcome);
    }
}

fn record_query_metadata_write(
    metrics: Option<&Arc<MetricsRecorder>>,
    labels: &MetricsLabels,
    metadata_kind: &str,
    outcome: QueryMetadataWriteOutcome,
    seconds: f64,
) {
    if let Some(metrics) = metrics {
        metrics.record_query_metadata_write(labels, metadata_kind, outcome);
        metrics.observe_query_metadata_write_duration(labels, metadata_kind, seconds);
    }
}

fn enqueue_usage_ledger_append(
    repository: Arc<dyn UsageLedgerRepository>,
    entry: UsageLedgerEntry,
    metrics: Option<Arc<MetricsRecorder>>,
) {
    let enqueue_start = Instant::now();
    let labels = MetricsLabels::from_dataset_key(
        ApplicationIdentity::named(entry.application_id.clone()),
        entry.chain.clone(),
        entry.dataset_key.clone(),
    );
    let context = UsageLedgerMetadataContext {
        base: QueryMetadataContext {
            query_id: entry
                .request_id
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
            application_id: entry.application_id.clone(),
            chain: entry.chain.configured_name().to_owned(),
            dataset: entry.dataset_key.as_str().to_owned(),
            range_start: entry.range.start(),
            range_end: entry.range.end(),
        },
        query_outcome: entry.query_outcome,
    };
    let outcome = metadata_worker_pool().enqueue(MetadataJob::UsageLedgerAppend {
        repository,
        entry,
        context: context.clone(),
        metrics: metrics.clone(),
    });
    record_query_metadata_enqueue(
        metrics.as_ref(),
        &labels,
        "usage_ledger",
        metadata_enqueue_metric_outcome(outcome),
    );
    match outcome {
        MetadataEnqueueOutcome::Enqueued => log::debug!(
            "query metadata enqueue completed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} duration_ms={}",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            elapsed_ms(enqueue_start)
        ),
        MetadataEnqueueOutcome::Coalesced => log::debug!(
            "query metadata enqueue coalesced metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} duration_ms={}",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            elapsed_ms(enqueue_start)
        ),
        MetadataEnqueueOutcome::Full => log::warn!(
            "query metadata enqueue dropped metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} duration_ms={} reason=queue_full",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            elapsed_ms(enqueue_start),
        ),
        MetadataEnqueueOutcome::Closed => log::warn!(
            "query metadata enqueue failed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} duration_ms={} reason=queue_closed",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            elapsed_ms(enqueue_start),
        ),
    }
}

fn enqueue_query_watermark_update(
    repository: Arc<dyn QueryWatermarkRepository>,
    query_id: String,
    watermark: QueryWatermark,
    range_start: u64,
    range_end: u64,
    metrics: Option<Arc<MetricsRecorder>>,
) {
    let enqueue_start = Instant::now();
    let labels = MetricsLabels::from_dataset_key(
        ApplicationIdentity::named(watermark.key.application_id.clone()),
        watermark.key.chain.clone(),
        watermark.key.dataset_key.clone(),
    );
    let application_id = watermark.key.application_id.clone();
    let chain = watermark.key.chain.configured_name().to_owned();
    let dataset = watermark.key.dataset_key.as_str().to_owned();
    let latest_block = watermark.latest_block;
    let context = QueryWatermarkMetadataContext {
        base: QueryMetadataContext {
            query_id,
            application_id,
            chain,
            dataset,
            range_start,
            range_end,
        },
        latest_block,
    };
    let outcome = metadata_worker_pool().enqueue(MetadataJob::QueryWatermarkUpdate {
        repository,
        watermark,
        context: context.clone(),
        metrics: metrics.clone(),
    });
    record_query_metadata_enqueue(
        metrics.as_ref(),
        &labels,
        "query_watermark",
        metadata_enqueue_metric_outcome(outcome),
    );
    match outcome {
        MetadataEnqueueOutcome::Enqueued => log::debug!(
            "query metadata enqueue completed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={}",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_block,
            elapsed_ms(enqueue_start)
        ),
        MetadataEnqueueOutcome::Coalesced => log::debug!(
            "query metadata enqueue coalesced metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={} reason=queue_full",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_block,
            elapsed_ms(enqueue_start),
        ),
        MetadataEnqueueOutcome::Full => log::warn!(
            "query metadata enqueue dropped metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={} reason=queue_full",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_block,
            elapsed_ms(enqueue_start),
        ),
        MetadataEnqueueOutcome::Closed => log::warn!(
            "query metadata enqueue failed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={} reason=queue_closed",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_block,
            elapsed_ms(enqueue_start),
        ),
    }
}

fn enqueue_query_activity_update(
    repository: Arc<dyn QueryActivityRepository>,
    query_id: String,
    activity: QueryActivity,
    range_start: u64,
    range_end: u64,
    metrics: Option<Arc<MetricsRecorder>>,
) {
    let enqueue_start = Instant::now();
    let labels = MetricsLabels::from_dataset_key(
        ApplicationIdentity::named(activity.key.application_id.clone()),
        activity.key.chain.clone(),
        activity.key.dataset_key.clone(),
    );
    let application_id = activity.key.application_id.clone();
    let chain = activity.key.chain.configured_name().to_owned();
    let dataset = activity.key.dataset_key.as_str().to_owned();
    let latest_start = activity.latest_range.start();
    let latest_end = activity.latest_range.end();
    let context = QueryActivityMetadataContext {
        base: QueryMetadataContext {
            query_id,
            application_id,
            chain,
            dataset,
            range_start,
            range_end,
        },
        latest_start,
        latest_end,
    };
    let outcome = metadata_worker_pool().enqueue(MetadataJob::QueryActivityUpdate {
        repository,
        activity,
        context: context.clone(),
        metrics: metrics.clone(),
    });
    record_query_metadata_enqueue(
        metrics.as_ref(),
        &labels,
        "query_activity",
        metadata_enqueue_metric_outcome(outcome),
    );
    match outcome {
        MetadataEnqueueOutcome::Enqueued => log::debug!(
            "query metadata enqueue completed metadata_kind=query_activity query_id={} application={} chain={} dataset={} range={}-{} latest_range={}-{} duration_ms={}",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_start,
            context.latest_end,
            elapsed_ms(enqueue_start)
        ),
        MetadataEnqueueOutcome::Coalesced => log::debug!(
            "query metadata enqueue coalesced metadata_kind=query_activity query_id={} application={} chain={} dataset={} range={}-{} latest_range={}-{} duration_ms={} reason=queue_full",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_start,
            context.latest_end,
            elapsed_ms(enqueue_start),
        ),
        MetadataEnqueueOutcome::Full => log::warn!(
            "query metadata enqueue dropped metadata_kind=query_activity query_id={} application={} chain={} dataset={} range={}-{} latest_range={}-{} duration_ms={} reason=queue_full",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_start,
            context.latest_end,
            elapsed_ms(enqueue_start),
        ),
        MetadataEnqueueOutcome::Closed => log::warn!(
            "query metadata enqueue failed metadata_kind=query_activity query_id={} application={} chain={} dataset={} range={}-{} latest_range={}-{} duration_ms={} reason=queue_closed",
            context.base.query_id,
            context.base.application_id,
            context.base.chain,
            context.base.dataset,
            context.base.range_start,
            context.base.range_end,
            context.latest_start,
            context.latest_end,
            elapsed_ms(enqueue_start),
        ),
    }
}

fn process_metadata_job(job: MetadataJob) {
    match job {
        MetadataJob::UsageLedgerAppend {
            repository,
            entry,
            context,
            metrics,
        } => {
            let start = Instant::now();
            let labels = MetricsLabels::from_dataset_key(
                ApplicationIdentity::named(context.base.application_id.clone()),
                entry.chain.clone(),
                entry.dataset_key.clone(),
            );
            match repository.append(&entry) {
                Ok(()) => {
                    let duration = start.elapsed();
                    record_query_metadata_write(
                        metrics.as_ref(),
                        &labels,
                        "usage_ledger",
                        QueryMetadataWriteOutcome::Completed,
                        duration.as_secs_f64(),
                    );
                    log::info!(
                        "query metadata background write completed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} query_outcome={:?} duration_ms={}",
                        context.base.query_id,
                        context.base.application_id,
                        context.base.chain,
                        context.base.dataset,
                        context.base.range_start,
                        context.base.range_end,
                        context.query_outcome,
                        duration.as_millis()
                    )
                }
                Err(error) => {
                    let duration = start.elapsed();
                    record_query_metadata_write(
                        metrics.as_ref(),
                        &labels,
                        "usage_ledger",
                        QueryMetadataWriteOutcome::Failed,
                        duration.as_secs_f64(),
                    );
                    log::warn!(
                        "query metadata background write failed metadata_kind=usage_ledger query_id={} application={} chain={} dataset={} range={}-{} query_outcome={:?} duration_ms={} kind={:?} message={}",
                        context.base.query_id,
                        context.base.application_id,
                        context.base.chain,
                        context.base.dataset,
                        context.base.range_start,
                        context.base.range_end,
                        context.query_outcome,
                        duration.as_millis(),
                        error.kind,
                        error.message
                    )
                }
            }
        }
        MetadataJob::QueryWatermarkUpdate {
            repository,
            watermark,
            context,
            metrics,
        } => {
            let start = Instant::now();
            let labels = MetricsLabels::from_dataset_key(
                ApplicationIdentity::named(context.base.application_id.clone()),
                watermark.key.chain.clone(),
                watermark.key.dataset_key.clone(),
            );
            match repository.update(&watermark) {
                Ok(()) => {
                    let duration = start.elapsed();
                    record_query_metadata_write(
                        metrics.as_ref(),
                        &labels,
                        "query_watermark",
                        QueryMetadataWriteOutcome::Completed,
                        duration.as_secs_f64(),
                    );
                    log::info!(
                        "query metadata background write completed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={}",
                        context.base.query_id,
                        context.base.application_id,
                        context.base.chain,
                        context.base.dataset,
                        context.base.range_start,
                        context.base.range_end,
                        context.latest_block,
                        duration.as_millis()
                    )
                }
                Err(error) => {
                    let duration = start.elapsed();
                    record_query_metadata_write(
                        metrics.as_ref(),
                        &labels,
                        "query_watermark",
                        QueryMetadataWriteOutcome::Failed,
                        duration.as_secs_f64(),
                    );
                    log::warn!(
                        "query metadata background write failed metadata_kind=query_watermark query_id={} application={} chain={} dataset={} range={}-{} latest_block={} duration_ms={} kind={:?} message={}",
                        context.base.query_id,
                        context.base.application_id,
                        context.base.chain,
                        context.base.dataset,
                        context.base.range_start,
                        context.base.range_end,
                        context.latest_block,
                        duration.as_millis(),
                        error.kind,
                        error.message
                    )
                }
            }
        }
        MetadataJob::QueryActivityUpdate {
            repository,
            activity,
            context,
            metrics,
        } => {
            let start = Instant::now();
            let labels = MetricsLabels::from_dataset_key(
                ApplicationIdentity::named(context.base.application_id.clone()),
                activity.key.chain.clone(),
                activity.key.dataset_key.clone(),
            );
            match repository.update(&activity) {
                Ok(()) => {
                    let duration = start.elapsed();
                    record_query_metadata_write(
                        metrics.as_ref(),
                        &labels,
                        "query_activity",
                        QueryMetadataWriteOutcome::Completed,
                        duration.as_secs_f64(),
                    );
                    log::info!(
                        "query metadata background write completed metadata_kind=query_activity query_id={} application={} chain={} dataset={} range={}-{} latest_range={}-{} duration_ms={}",
                        context.base.query_id,
                        context.base.application_id,
                        context.base.chain,
                        context.base.dataset,
                        context.base.range_start,
                        context.base.range_end,
                        context.latest_start,
                        context.latest_end,
                        duration.as_millis()
                    )
                }
                Err(error) => {
                    let duration = start.elapsed();
                    record_query_metadata_write(
                        metrics.as_ref(),
                        &labels,
                        "query_activity",
                        QueryMetadataWriteOutcome::Failed,
                        duration.as_secs_f64(),
                    );
                    log::warn!(
                        "query metadata background write failed metadata_kind=query_activity query_id={} application={} chain={} dataset={} range={}-{} latest_range={}-{} duration_ms={} kind={:?} message={}",
                        context.base.query_id,
                        context.base.application_id,
                        context.base.chain,
                        context.base.dataset,
                        context.base.range_start,
                        context.base.range_end,
                        context.latest_start,
                        context.latest_end,
                        duration.as_millis(),
                        error.kind,
                        error.message
                    )
                }
            }
        }
        #[cfg(test)]
        MetadataJob::NoopForTest => {}
    }
}

fn elapsed_ms(start: Instant) -> u128 {
    start.elapsed().as_millis()
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

fn durable_activity_range(plan: &datalens_planner::NativeQueryPlan) -> Option<LedgerRange> {
    let latest_block = durable_watermark_block(plan)?;
    LedgerRange::try_new(
        plan.ledger_range.kind(),
        plan.ledger_range.start(),
        latest_block,
    )
    .ok()
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

    #[test]
    fn test_metadata_worker_pool_reports_full_without_blocking() {
        let pool = MetadataWorkerPool::new_for_test(1, 0);

        assert_eq!(
            pool.enqueue(MetadataJob::NoopForTest),
            MetadataEnqueueOutcome::Enqueued
        );
        let start = Instant::now();
        assert_eq!(
            pool.enqueue(MetadataJob::NoopForTest),
            MetadataEnqueueOutcome::Full
        );
        assert!(
            start.elapsed() < std::time::Duration::from_millis(50),
            "full metadata queue enqueue should not block"
        );
    }

    #[test]
    fn test_metadata_worker_pool_coalesces_latest_state_when_queue_is_full() {
        let pool = MetadataWorkerPool::new_for_test(1, 0);
        let watermarks = RecordingQueryWatermarkRepository::default();
        let activities = RecordingQueryActivityRepository::default();

        assert_eq!(
            pool.enqueue(MetadataJob::NoopForTest),
            MetadataEnqueueOutcome::Enqueued
        );
        assert_eq!(
            pool.enqueue(MetadataJob::QueryWatermarkUpdate {
                repository: Arc::new(watermarks.clone()),
                watermark: test_watermark(10, 100),
                context: test_watermark_context("q-low", 10),
                metrics: None,
            }),
            MetadataEnqueueOutcome::Coalesced
        );
        assert_eq!(
            pool.enqueue(MetadataJob::QueryWatermarkUpdate {
                repository: Arc::new(watermarks),
                watermark: test_watermark(20, 200),
                context: test_watermark_context("q-high", 20),
                metrics: None,
            }),
            MetadataEnqueueOutcome::Coalesced
        );
        assert_eq!(
            pool.enqueue(MetadataJob::QueryActivityUpdate {
                repository: Arc::new(activities.clone()),
                activity: test_activity(10, 100),
                context: test_activity_context("q-activity-low", 10),
                metrics: None,
            }),
            MetadataEnqueueOutcome::Coalesced
        );
        assert_eq!(
            pool.enqueue(MetadataJob::QueryActivityUpdate {
                repository: Arc::new(activities),
                activity: test_activity(30, 300),
                context: test_activity_context("q-activity-high", 30),
                metrics: None,
            }),
            MetadataEnqueueOutcome::Coalesced
        );
        assert_eq!(pool.pending_latest_state_counts_for_test(), (1, 1));

        assert!(matches!(
            pool.recv_for_test(),
            Some(MetadataJob::NoopForTest)
        ));
        pool.flush_coalesced();
        match pool.recv_for_test() {
            Some(MetadataJob::QueryWatermarkUpdate {
                watermark, context, ..
            }) => {
                assert_eq!(watermark.latest_block, 20);
                assert_eq!(context.base.query_id, "q-high");
            }
            _ => panic!("expected coalesced watermark"),
        }
        pool.flush_coalesced();
        match pool.recv_for_test() {
            Some(MetadataJob::QueryActivityUpdate {
                activity, context, ..
            }) => {
                assert_eq!(activity.latest_range.end(), 30);
                assert_eq!(context.base.query_id, "q-activity-high");
            }
            _ => panic!("expected coalesced activity"),
        }
    }

    #[derive(Clone, Default)]
    struct RecordingQueryWatermarkRepository;

    impl QueryWatermarkRepository for RecordingQueryWatermarkRepository {
        fn update(&self, _watermark: &QueryWatermark) -> Result<(), DatalensError> {
            Ok(())
        }

        fn read(&self, _key: &QueryWatermarkKey) -> Result<Option<QueryWatermark>, DatalensError> {
            Ok(None)
        }
    }

    #[derive(Clone, Default)]
    struct RecordingQueryActivityRepository;

    impl QueryActivityRepository for RecordingQueryActivityRepository {
        fn update(&self, _activity: &QueryActivity) -> Result<(), DatalensError> {
            Ok(())
        }

        fn read(&self, _key: &QueryActivityKey) -> Result<Option<QueryActivity>, DatalensError> {
            Ok(None)
        }
    }

    fn test_watermark(latest_block: u64, updated_at_unix_seconds: u64) -> QueryWatermark {
        QueryWatermark {
            key: test_watermark_key(),
            latest_block,
            updated_at_unix_seconds,
        }
    }

    fn test_watermark_key() -> QueryWatermarkKey {
        QueryWatermarkKey::new(
            "app",
            ChainIdentity::expect_new(datalens_core::ChainFamily::Evm, "ethereum"),
            DatasetKey::evm_blocks(),
            &datalens_chain::DatasetSelector::all(),
            datalens_core::LedgerRangeKind::Block,
        )
    }

    fn test_activity(latest_end: u64, updated_at_unix_seconds: u64) -> QueryActivity {
        QueryActivity {
            key: test_activity_key(),
            latest_range: LedgerRange::blocks(1, latest_end).expect("range"),
            updated_at_unix_seconds,
            request_id: Some(format!("q-{latest_end}")),
        }
    }

    fn test_activity_key() -> QueryActivityKey {
        QueryActivityKey::new(
            "app",
            ChainIdentity::expect_new(datalens_core::ChainFamily::Evm, "ethereum"),
            DatasetKey::evm_blocks(),
            &datalens_chain::DatasetSelector::all(),
            datalens_core::LedgerRangeKind::Block,
        )
    }

    fn test_watermark_context(query_id: &str, latest_block: u64) -> QueryWatermarkMetadataContext {
        QueryWatermarkMetadataContext {
            base: test_metadata_context(query_id),
            latest_block,
        }
    }

    fn test_activity_context(query_id: &str, latest_end: u64) -> QueryActivityMetadataContext {
        QueryActivityMetadataContext {
            base: test_metadata_context(query_id),
            latest_start: 1,
            latest_end,
        }
    }

    fn test_metadata_context(query_id: &str) -> QueryMetadataContext {
        QueryMetadataContext {
            query_id: query_id.to_owned(),
            application_id: "app".to_owned(),
            chain: "ethereum".to_owned(),
            dataset: "evm.blocks".to_owned(),
            range_start: 1,
            range_end: 1,
        }
    }
}
