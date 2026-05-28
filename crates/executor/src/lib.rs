//! Query execution boundary for durable native datalens plans.

use std::{sync::Arc, time::Instant};

use datalens_chain::{
    CanonicalBlockRequest, ChainAdapter, ChainFetchRequest, ChainHeight, FetchContext,
    FinalityLevel,
};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows, LedgerRange,
    QueryDataFinality, QueryRows, QuerySegmentMetadata, QuerySegmentSource, missing_ranges,
};
use datalens_metrics::HotPromotionOutcome as MetricsHotPromotionOutcome;
use datalens_metrics::{
    ApplicationIdentity, CacheCoverageOutcome, ErrorLabels, FillOutcome, MetricsLabels,
    MetricsRecorder, QueryOutcome,
};
use datalens_planner::{
    CoverageSummary, FinalityPolicy, NativePlanner, NativePlannerConfig, NativeQueryInput,
};
use datalens_storage::{
    CacheOutcome as LedgerCacheOutcome, FillOutcome as LedgerFillOutcome, HotCacheCandidateStatus,
    HotCacheEntryMetadata, HotCacheFinalityStatus, HotCacheStorage,
    QueryOutcome as LedgerQueryOutcome, StorageRepository, UsageLedgerEntry, UsageLedgerRepository,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteResult, DurableWriteSegment, DurableWriter,
    DurableWriterConfig,
};

#[derive(Clone, Debug, Eq, PartialEq)]
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
pub struct HotCachePromoter<H, R, S> {
    hot: HotCacheStorage<H>,
    writer: DurableWriter<R>,
    source: S,
    metrics: Option<ExecutorMetrics>,
    usage_ledger: Option<ExecutorUsageLedger>,
}

impl<H, R, S> HotCachePromoter<H, R, S>
where
    H: datalens_storage::ObjectStore + Clone + 'static,
    R: StorageRepository + Clone,
    S: ChainAdapter,
{
    pub fn new(
        hot: HotCacheStorage<H>,
        durable: R,
        source: S,
        writer_config: DurableWriterConfig,
    ) -> Self {
        Self {
            hot,
            writer: DurableWriter::new(durable, writer_config),
            source,
            metrics: None,
            usage_ledger: None,
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

    pub fn promote(
        &self,
        request: HotPromotionRequest,
    ) -> Result<HotPromotionResult, DatalensError> {
        let labels = self.promotion_labels(&request);
        let application = self.promotion_application(&request);
        let boundary = self.source.cache_safe_height()?;
        boundary.validate_durable_writable()?;
        let entries = self.hot.list_entries(
            &request.chain,
            request.range.kind(),
            request.range.start(),
            request.range.end(),
        )?;
        let mut result = HotPromotionResult::default();

        for entry in entries {
            if entry.dataset_key.as_ref() != Some(&request.dataset_key)
                || entry.selector_fingerprint != request.selector.fingerprint()
            {
                continue;
            }
            result.attempted += 1;
            self.record_hot_promotion(&labels, MetricsHotPromotionOutcome::Attempted, 1);

            if !eligible_for_promotion(&entry, &request, &boundary) {
                result.skipped += 1;
                self.record_hot_promotion(&labels, MetricsHotPromotionOutcome::Skipped, 1);
                self.record_promotion_usage(
                    &application,
                    &request,
                    boundary.finality,
                    LedgerQueryOutcome::PromotionSkipped,
                    LedgerFillOutcome::PromotionSkipped,
                    0,
                )?;
                continue;
            }

            let entry_range = entry.range.clone().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    "hot cache promotion metadata missing range",
                )
            })?;
            let hot_rows = self.hot.read_rows(
                &request.chain,
                &request.dataset_key,
                &request.selector,
                entry_range.kind(),
                entry_range.start(),
                entry_range.end(),
            )?;
            if let Err(error) =
                self.validate_canonical_rows(&request.chain, &entry_range, &hot_rows.rows)
            {
                if is_provider_error(&error.kind) {
                    return Err(error);
                }
                result.skipped += 1;
                self.record_hot_promotion(&labels, MetricsHotPromotionOutcome::Skipped, 1);
                self.record_promotion_usage(
                    &application,
                    &request,
                    boundary.finality,
                    LedgerQueryOutcome::PromotionSkipped,
                    LedgerFillOutcome::PromotionSkipped,
                    0,
                )?;
                continue;
            }

            let write_result = match self.writer.write(DurableWriteRequest {
                chain: request.chain.clone(),
                dataset_key: request.dataset_key.clone(),
                selector: request.selector.clone(),
                finality_level: boundary.finality,
                segments: vec![DurableWriteSegment {
                    range: entry_range.clone(),
                    rows: hot_rows.rows.clone(),
                }],
            }) {
                Ok(write_result) => write_result,
                Err(error) => {
                    self.record_hot_promotion(&labels, MetricsHotPromotionOutcome::Failed, 1);
                    self.record_promotion_usage(
                        &application,
                        &request,
                        boundary.finality,
                        LedgerQueryOutcome::StorageError,
                        LedgerFillOutcome::StorageError,
                        0,
                    )?;
                    return Err(error);
                }
            };

            let row_count = promoted_row_count(&write_result);
            self.hot.mark_promoted(
                std::slice::from_ref(&entry.metadata_key),
                unix_seconds_now()?,
            )?;
            result.promoted += 1;
            self.record_hot_promotion(&labels, MetricsHotPromotionOutcome::Promoted, 1);
            self.record_promotion_usage(
                &application,
                &HotPromotionRequest {
                    range: entry_range,
                    ..request.clone()
                },
                boundary.finality,
                LedgerQueryOutcome::PromotionCompleted,
                LedgerFillOutcome::PromotionWritten,
                row_count,
            )?;
        }

        Ok(result)
    }

    fn validate_canonical_rows(
        &self,
        chain: &ChainIdentity,
        range: &LedgerRange,
        rows: &DatasetRows,
    ) -> Result<(), DatalensError> {
        match rows.rows() {
            QueryRows::EvmBlocks(blocks) => {
                let mut previous_hash = None::<&str>;
                for block in blocks {
                    if block.number < range.start() || block.number > range.end() {
                        return Err(DatalensError::new(
                            DatalensErrorKind::InvalidInput,
                            "hot cache row is outside promotion range",
                        ));
                    }
                    if let Some(previous_hash) = previous_hash
                        && block.parent_hash != previous_hash
                    {
                        return Err(DatalensError::new(
                            DatalensErrorKind::InvalidInput,
                            "hot cache block hash continuity check failed",
                        ));
                    }
                    let canonical = self.source.canonical_block(CanonicalBlockRequest {
                        chain: chain.clone(),
                        range_kind: range.kind(),
                        height: block.number,
                    })?;
                    if canonical.hash != block.hash || canonical.parent_hash != block.parent_hash {
                        return Err(DatalensError::new(
                            DatalensErrorKind::InvalidInput,
                            "hot cache block does not match canonical chain",
                        ));
                    }
                    previous_hash = Some(block.hash.as_str());
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn promotion_application(&self, request: &HotPromotionRequest) -> Option<ApplicationIdentity> {
        self.usage_ledger.as_ref().map(|ledger| {
            request
                .application
                .clone()
                .unwrap_or_else(|| ledger.application.clone())
        })
    }

    fn promotion_labels(&self, request: &HotPromotionRequest) -> Option<MetricsLabels> {
        self.metrics.as_ref().map(|metrics| {
            MetricsLabels::from_dataset_key(
                request
                    .application
                    .clone()
                    .unwrap_or_else(|| metrics.application.clone()),
                request.chain.clone(),
                request.dataset_key.clone(),
            )
        })
    }

    fn record_hot_promotion(
        &self,
        labels: &Option<MetricsLabels>,
        outcome: MetricsHotPromotionOutcome,
        count: u64,
    ) {
        if let Some((metrics, labels)) = self
            .metrics
            .as_ref()
            .zip(labels.as_ref())
            .map(|(metrics, labels)| (metrics.recorder.as_ref(), labels))
        {
            metrics.record_hot_promotion(labels, outcome, count);
        }
    }

    fn record_promotion_usage(
        &self,
        application: &Option<ApplicationIdentity>,
        request: &HotPromotionRequest,
        finality_level: FinalityLevel,
        query_outcome: LedgerQueryOutcome,
        fill_outcome: LedgerFillOutcome,
        row_count: usize,
    ) -> Result<(), DatalensError> {
        let Some(ledger) = &self.usage_ledger else {
            return Ok(());
        };
        let application = application
            .as_ref()
            .unwrap_or(&ledger.application)
            .as_str()
            .to_owned();
        ledger.repository.append(&UsageLedgerEntry::query_event(
            application,
            request.chain.clone(),
            request.dataset_key.clone(),
            &request.selector,
            request.range.clone(),
            finality_level,
            query_outcome,
            LedgerCacheOutcome::HotHit,
            fill_outcome,
            row_count,
        ))
    }
}

#[derive(Clone)]
pub struct NativeQueryExecutor<R, S> {
    storage: R,
    source: S,
    planner: NativePlanner,
    writer: DurableWriter<R>,
    metrics: Option<ExecutorMetrics>,
    usage_ledger: Option<ExecutorUsageLedger>,
}

#[derive(Clone)]
struct ExecutorMetrics {
    recorder: Arc<MetricsRecorder>,
    application: ApplicationIdentity,
}

#[derive(Clone)]
struct ExecutorUsageLedger {
    repository: Arc<dyn UsageLedgerRepository>,
    application: ApplicationIdentity,
}

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

    pub fn execute(
        &self,
        input: NativeQueryInput,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        self.execute_with_application(input, None)
    }

    pub fn execute_with_application(
        &self,
        input: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        let start = Instant::now();
        let labels = self.metrics_labels(&input, application.clone());
        let ledger_application = self.ledger_application(application);
        if let Some((recorder, labels)) = self.metrics_recorder(&labels) {
            recorder.set_latest_requested_block(labels, input.ledger_range.end());
        }

        let covered_ranges = match self.storage.covered_ranges(
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
                    &ledger_application,
                    &input,
                    FinalityLevel::Safe,
                    LedgerQueryOutcome::StorageError,
                    LedgerCacheOutcome::Error,
                    LedgerFillOutcome::NotAttempted,
                    0,
                )?;
                return Err(error);
            }
        };
        let hit_ranges = covered_ranges
            .iter()
            .filter_map(|range| range.intersection(&input.ledger_range))
            .collect::<Vec<_>>();
        let miss_ranges = missing_ranges(input.ledger_range.clone(), &hit_ranges);
        let coverage_outcome = coverage_outcome(&hit_ranges, &miss_ranges);
        self.record_cache_coverage(&labels, coverage_outcome);
        let durable_boundary = if miss_ranges.is_empty() {
            boundary_for_cached_hit(&input.ledger_range)
        } else {
            match self.source.cache_safe_height() {
                Ok(boundary) => boundary,
                Err(error) => {
                    self.record_error(&labels, &error);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage(
                        &ledger_application,
                        &input,
                        FinalityLevel::Safe,
                        ledger_query_error(&error),
                        ledger_cache_outcome(coverage_outcome),
                        LedgerFillOutcome::NotAttempted,
                        0,
                    )?;
                    return Err(error);
                }
            }
        };
        let plan = match self.planner.plan_with_coverage(
            input.clone(),
            &self.source.capabilities(),
            durable_boundary.clone(),
            covered_ranges,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage(
                    &ledger_application,
                    &input,
                    durable_boundary.finality,
                    LedgerQueryOutcome::Error,
                    ledger_cache_outcome(coverage_outcome),
                    LedgerFillOutcome::NotAttempted,
                    0,
                )?;
                return Err(error);
            }
        };

        log::info!(
            "query executor cache summary dataset={} hit_ranges={} missing_ranges={}",
            plan.dataset_key.as_str(),
            plan.coverage.hit_ranges.len(),
            plan.coverage.missing_ranges.len()
        );

        let mut rows = empty_query_rows(&plan.dataset_key);
        for segment in &plan.read_segments {
            let cached = match self.storage.read_rows(
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
                        &ledger_application,
                        &plan,
                        LedgerQueryOutcome::StorageError,
                        ledger_cache_outcome(coverage_outcome),
                        LedgerFillOutcome::NotAttempted,
                        rows.row_count(),
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = rows.try_append(cached.into_rows()) {
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage_for_plan(
                    &ledger_application,
                    &plan,
                    LedgerQueryOutcome::Error,
                    ledger_cache_outcome(coverage_outcome),
                    LedgerFillOutcome::NotAttempted,
                    rows.row_count(),
                )?;
                return Err(error);
            }
        }

        let finality_level = match &plan.finality_policy {
            FinalityPolicy::DurableCache { boundary } => boundary.finality,
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
                request_id: None,
                cache_write: task.cache_write,
            });
            let fetched = match self.source.fetch(fetch_request.clone()) {
                Ok(response) => {
                    if let Err(error) = response.validate_for_request(&fetch_request) {
                        self.record_fill(&labels, FillOutcome::Error, fill_start);
                        self.record_query(&labels, QueryOutcome::Error, start);
                        self.record_usage_for_plan(
                            &ledger_application,
                            &plan,
                            LedgerQueryOutcome::Error,
                            ledger_cache_outcome(coverage_outcome),
                            LedgerFillOutcome::Error,
                            rows.row_count(),
                        )?;
                        return Err(error);
                    }
                    fill_row_count += response.rows.row_count();
                    response.rows
                }
                Err(error) => {
                    log::warn!(
                        "provider fetch failed dataset={} range={}-{} kind={:?}",
                        plan.dataset_key.as_str(),
                        task.range.start(),
                        task.range.end(),
                        error.kind
                    );
                    self.record_error(&labels, &error);
                    self.record_fill(&labels, FillOutcome::Error, fill_start);
                    self.record_query(&labels, QueryOutcome::Error, start);
                    self.record_usage_for_plan(
                        &ledger_application,
                        &plan,
                        ledger_query_error(&error),
                        ledger_cache_outcome(coverage_outcome),
                        ledger_fill_error(&error),
                        rows.row_count(),
                    )?;
                    return Err(error);
                }
            };
            if task.cache_write {
                fill_end_block = Some(
                    fill_end_block
                        .unwrap_or(task.range.end())
                        .max(task.range.end()),
                );
                fetched_segments.push(DurableWriteSegment {
                    range: task.range.clone(),
                    rows: fetched.clone(),
                });
            }
            if let Err(error) = rows.try_append(fetched.into_rows()) {
                self.record_fill(&labels, FillOutcome::Error, fill_start);
                self.record_query(&labels, QueryOutcome::Error, start);
                self.record_usage_for_plan(
                    &ledger_application,
                    &plan,
                    LedgerQueryOutcome::Error,
                    ledger_cache_outcome(coverage_outcome),
                    LedgerFillOutcome::Error,
                    rows.row_count(),
                )?;
                return Err(error);
            }
        }

        let cache_fill_attempted = !fetched_segments.is_empty();
        if cache_fill_attempted
            && let Err(error) = self.writer.write(DurableWriteRequest {
                chain: plan.chain.clone(),
                dataset_key: plan.dataset_key.clone(),
                selector: plan.selector.clone(),
                finality_level,
                segments: fetched_segments,
            })
        {
            log::error!(
                "cache write failed dataset={} range={}-{} kind={:?}",
                plan.dataset_key.as_str(),
                plan.ledger_range.start(),
                plan.ledger_range.end(),
                error.kind
            );
            self.record_error(&labels, &error);
            self.record_fill(&labels, FillOutcome::Error, fill_start);
            self.record_query(&labels, QueryOutcome::Error, start);
            self.record_usage_for_plan(
                &ledger_application,
                &plan,
                LedgerQueryOutcome::StorageError,
                ledger_cache_outcome(coverage_outcome),
                LedgerFillOutcome::StorageError,
                rows.row_count(),
            )?;
            return Err(error);
        }
        if cache_fill_attempted {
            let fill_outcome = if fill_row_count == 0 {
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
        cache.provider_fill_ranges = plan
            .fetch_tasks
            .iter()
            .map(|task| task.range.clone())
            .collect();
        cache.segments.extend(plan.fetch_tasks.iter().map(|task| {
            QuerySegmentMetadata::new(
                task.range.clone(),
                QuerySegmentSource::Provider,
                query_data_finality(finality_level),
            )
        }));

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
            &ledger_application,
            &plan,
            ledger_query_outcome(query_outcome),
            ledger_cache_outcome(coverage_outcome),
            ledger_fill_outcome(cache_fill_attempted, fill_row_count),
            result.rows.row_count(),
        )?;
        Ok(result)
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
        application: &Option<ApplicationIdentity>,
        input: &NativeQueryInput,
        finality_level: FinalityLevel,
        query_outcome: LedgerQueryOutcome,
        cache_outcome: LedgerCacheOutcome,
        fill_outcome: LedgerFillOutcome,
        row_count: usize,
    ) -> Result<(), DatalensError> {
        let Some(ledger) = &self.usage_ledger else {
            return Ok(());
        };
        let application = application
            .as_ref()
            .unwrap_or(&ledger.application)
            .as_str()
            .to_owned();
        ledger.repository.append(
            &UsageLedgerEntry::query_event(
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
            .with_requested_hot(input.finality.allows_hot()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_usage_for_plan(
        &self,
        application: &Option<ApplicationIdentity>,
        plan: &datalens_planner::NativeQueryPlan,
        query_outcome: LedgerQueryOutcome,
        cache_outcome: LedgerCacheOutcome,
        fill_outcome: LedgerFillOutcome,
        row_count: usize,
    ) -> Result<(), DatalensError> {
        self.record_usage(
            application,
            &NativeQueryInput {
                chain: plan.chain.clone(),
                dataset_key: plan.dataset_key.clone(),
                ledger_range: plan.ledger_range.clone(),
                selector: plan.selector.clone(),
                response_shape: plan.response_shape.clone(),
                field_selection: plan.field_selection.clone(),
                finality: plan.requested_finality,
            },
            match &plan.finality_policy {
                FinalityPolicy::DurableCache { boundary } => boundary.finality,
            },
            query_outcome,
            cache_outcome,
            fill_outcome,
            row_count,
        )
    }
}

fn query_data_finality(finality: FinalityLevel) -> QueryDataFinality {
    match finality {
        FinalityLevel::Finalized => QueryDataFinality::Finalized,
        FinalityLevel::Safe | FinalityLevel::ChainSpecific(_) => QueryDataFinality::Safe,
        FinalityLevel::Latest => QueryDataFinality::Latest,
    }
}

fn eligible_for_promotion(
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

fn promoted_row_count(result: &DurableWriteResult) -> usize {
    result
        .data_objects
        .iter()
        .map(|object| object.row_count)
        .sum()
}

fn unix_seconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::internal(format!("system clock before unix epoch: {error}"))
        })
}

fn ledger_cache_outcome(outcome: CacheCoverageOutcome) -> LedgerCacheOutcome {
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

fn ledger_query_outcome(outcome: QueryOutcome) -> LedgerQueryOutcome {
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

fn ledger_query_error(error: &DatalensError) -> LedgerQueryOutcome {
    if is_provider_error(&error.kind) {
        LedgerQueryOutcome::ProviderError
    } else if is_storage_error(&error.kind) {
        LedgerQueryOutcome::StorageError
    } else {
        LedgerQueryOutcome::Error
    }
}

fn ledger_fill_error(error: &DatalensError) -> LedgerFillOutcome {
    if is_provider_error(&error.kind) {
        LedgerFillOutcome::ProviderError
    } else if is_storage_error(&error.kind) {
        LedgerFillOutcome::StorageError
    } else {
        LedgerFillOutcome::Error
    }
}

fn ledger_fill_outcome(cache_fill_attempted: bool, fill_row_count: usize) -> LedgerFillOutcome {
    if !cache_fill_attempted {
        LedgerFillOutcome::NotAttempted
    } else if fill_row_count == 0 {
        LedgerFillOutcome::EmptyCoverageRecorded
    } else {
        LedgerFillOutcome::Written
    }
}

fn coverage_outcome(
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

fn query_outcome(
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

fn is_provider_error(kind: &datalens_core::DatalensErrorKind) -> bool {
    matches!(
        kind,
        datalens_core::DatalensErrorKind::ProviderFailure
            | datalens_core::DatalensErrorKind::ProviderLimit
            | datalens_core::DatalensErrorKind::ProviderTimeout
            | datalens_core::DatalensErrorKind::RateLimited
    )
}

fn is_storage_error(kind: &datalens_core::DatalensErrorKind) -> bool {
    matches!(
        kind,
        datalens_core::DatalensErrorKind::StorageReadFailure
            | datalens_core::DatalensErrorKind::StorageWriteFailure
            | datalens_core::DatalensErrorKind::ManifestUpdateFailure
    )
}

fn boundary_for_cached_hit(range: &LedgerRange) -> ChainHeight {
    ChainHeight {
        range_kind: range.kind(),
        value: range.end(),
        finality: FinalityLevel::Safe,
    }
}

fn empty_query_rows(dataset_key: &DatasetKey) -> QueryRows {
    match dataset_key.legacy_dataset() {
        Some(Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: Vec::new(),
        },
    }
}
