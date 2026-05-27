//! Query execution boundary for durable native datalens plans.

use datalens_chain::{ChainAdapter, ChainFetchRequest, ChainHeight, FetchContext, FinalityLevel};
use datalens_core::{
    DatalensError, Dataset, DatasetKey, DatasetRows, LedgerRange, QueryRows, missing_ranges,
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
        }
    }

    pub fn execute(
        &self,
        input: NativeQueryInput,
    ) -> Result<NativeQueryExecutionResult, DatalensError> {
        let covered_ranges = self.storage.covered_ranges(
            &input.chain,
            &input.dataset_key,
            &input.selector,
            input.ledger_range.clone(),
        )?;
        let hit_ranges = covered_ranges
            .iter()
            .filter_map(|range| range.intersection(&input.ledger_range))
            .collect::<Vec<_>>();
        let miss_ranges = missing_ranges(input.ledger_range.clone(), &hit_ranges);
        let durable_boundary = if miss_ranges.is_empty() {
            boundary_for_cached_hit(&input.ledger_range)
        } else {
            self.source.cache_safe_height()?
        };
        let plan = self.planner.plan_with_coverage(
            input,
            &self.source.capabilities(),
            durable_boundary,
            covered_ranges,
        )?;

        log::info!(
            "query executor cache summary dataset={} hit_ranges={} missing_ranges={}",
            plan.dataset_key.as_str(),
            plan.coverage.hit_ranges.len(),
            plan.coverage.missing_ranges.len()
        );

        let mut rows = empty_query_rows(&plan.dataset_key);
        for segment in &plan.read_segments {
            let cached = self.storage.read_rows(
                &plan.chain,
                &plan.dataset_key,
                &plan.selector,
                segment.range.clone(),
            )?;
            rows.try_append(cached.into_rows())?;
        }

        let finality_level = match &plan.finality_policy {
            FinalityPolicy::DurableCache { boundary } => boundary.finality,
        };
        let mut fetched_segments = Vec::new();

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
                    response.validate_for_request(&fetch_request)?;
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
                    return Err(error);
                }
            };
            if task.cache_write {
                fetched_segments.push(DurableWriteSegment {
                    range: task.range.clone(),
                    rows: fetched.clone(),
                });
            }
            rows.try_append(fetched.into_rows())?;
        }

        if !fetched_segments.is_empty()
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
            return Err(error);
        }

        rows.sort();
        Ok(NativeQueryExecutionResult {
            chain: plan.chain,
            dataset_key: plan.dataset_key.clone(),
            ledger_range: plan.ledger_range,
            cache: plan.coverage,
            rows: DatasetRows::new(plan.dataset_key, rows)?,
        })
    }
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
