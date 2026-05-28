//! Query execution boundary for durable native datalens plans.

use std::{sync::Arc, time::Instant};

use datalens_chain::{ChainAdapter, ChainFetchRequest, ChainHeight, FetchContext, FinalityLevel};
use datalens_core::{
    DatalensError, Dataset, DatasetKey, DatasetRows, LedgerRange, QueryRows, missing_ranges,
};
use datalens_metrics::{
    ApplicationIdentity, CacheCoverageOutcome, ErrorLabels, FillOutcome, MetricsLabels,
    MetricsRecorder, QueryOutcome,
};
use datalens_planner::{
    CoverageSummary, FinalityPolicy, NativePlanner, NativePlannerConfig, NativeQueryInput,
};
use datalens_storage::StorageRepository;
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig,
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

#[derive(Clone)]
pub struct NativeQueryExecutor<R, S> {
    storage: R,
    source: S,
    planner: NativePlanner,
    writer: DurableWriter<R>,
    metrics: Option<ExecutorMetrics>,
}

#[derive(Clone)]
struct ExecutorMetrics {
    recorder: Arc<MetricsRecorder>,
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

    pub fn execute(
        &self,
        input: NativeQueryInput,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        let start = Instant::now();
        let labels = self.metrics_labels(&input);
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
                    return Err(error);
                }
            }
        };
        let plan = match self.planner.plan_with_coverage(
            input,
            &self.source.capabilities(),
            durable_boundary,
            covered_ranges,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.record_query(&labels, QueryOutcome::Error, start);
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
                    return Err(error);
                }
            };
            if let Err(error) = rows.try_append(cached.into_rows()) {
                self.record_query(&labels, QueryOutcome::Error, start);
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
        let result = NativeQueryExecutionResult {
            chain: plan.chain,
            dataset_key: plan.dataset_key.clone(),
            ledger_range: plan.ledger_range,
            cache: plan.coverage,
            rows: DatasetRows::new(plan.dataset_key, rows)?,
        };
        let query_outcome = query_outcome(coverage_outcome, cache_fill_attempted, &result);
        self.record_query(&labels, query_outcome, start);
        Ok(result)
    }

    fn metrics_labels(&self, input: &NativeQueryInput) -> Option<MetricsLabels> {
        self.metrics.as_ref().map(|metrics| {
            MetricsLabels::from_dataset_key(
                metrics.application.clone(),
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
        CacheCoverageOutcome::PartialHit => QueryOutcome::PartialHit,
        CacheCoverageOutcome::Miss if result.rows.row_count() == 0 => QueryOutcome::Empty,
        CacheCoverageOutcome::Miss if filled_cache => QueryOutcome::Filled,
        CacheCoverageOutcome::Miss => QueryOutcome::Miss,
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
