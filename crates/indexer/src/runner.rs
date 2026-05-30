use datalens_client::{DatalensClient, HttpTransport};
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatasetKey, LedgerRange, LedgerRangeKind, LogFilter,
    NetworkId, QueryFinalityRequirement, missing_ranges,
};
use serde::Serialize;
use std::time::Instant;

use crate::{IndexPlan, IndexerError, OutputSinkConfig};

#[derive(Clone)]
pub struct IndexRunner {
    plan: IndexPlan,
    output: OutputSinkConfig,
}

impl IndexRunner {
    pub fn new(plan: IndexPlan, output: OutputSinkConfig) -> Self {
        Self { plan, output }
    }

    pub fn plan(&self) -> &IndexPlan {
        &self.plan
    }

    pub fn output(&self) -> &OutputSinkConfig {
        &self.output
    }

    pub fn run<T>(&self, client: &DatalensClient<T>) -> Result<IndexRunReport, IndexerError>
    where
        T: HttpTransport,
    {
        let mut tasks = Vec::new();

        for task in self.plan.tasks() {
            let range = LedgerRange::blocks(task.range.start, task.range.end)
                .map_err(|error| IndexerError::Runner(error.to_string()))?;
            let block_range = BlockRange::try_new(task.range.start, task.range.end)
                .map_err(|error| IndexerError::Runner(error.to_string()))?;
            let chain = ChainIdentity::try_new(
                ChainFamily::Evm,
                task.chain.clone(),
                Some(NetworkId::numeric(task.chain_id)),
            )
            .map_err(|error| IndexerError::Runner(error.to_string()))?;
            let filter = LogFilter {
                addresses: task.selector.addresses.clone(),
                topics: task
                    .selector
                    .topics
                    .iter()
                    .cloned()
                    .map(|topic| Some(vec![topic]))
                    .collect(),
            };
            let started = Instant::now();
            let response = client
                .query(
                    datalens_client::QueryRequest::new(
                        chain,
                        DatasetKey::evm_logs(),
                        LedgerRange::from_block_range(block_range),
                    )
                    .with_selector(datalens_client::QuerySelector::EvmLogs(filter))
                    .with_finality(QueryFinalityRequirement::DurableOnly)
                    .with_fields(datalens_client::FieldSelection::All),
                )
                .map_err(|error| {
                    IndexerError::Runner(format!("task {} failed: {error}", task.label))
                })?;
            let elapsed_ms = started.elapsed().as_millis();
            let full_durable_hit =
                missing_ranges(range.clone(), &response.cache.durable_hit_ranges).is_empty()
                    && response.cache.provider_fill_ranges.is_empty()
                    && response.cache.missing_ranges.is_empty();

            tasks.push(IndexRunTaskSummary {
                label: task.label.clone(),
                chain: task.chain.clone(),
                range: ExecutedRange::from_ledger_range(&range),
                elapsed_ms,
                row_count: response.rows.row_count(),
                hit_ranges: response
                    .cache
                    .hit_ranges
                    .iter()
                    .map(ExecutedRange::from_ledger_range)
                    .collect(),
                missing_ranges: response
                    .cache
                    .missing_ranges
                    .iter()
                    .map(ExecutedRange::from_ledger_range)
                    .collect(),
                durable_hit_ranges: response
                    .cache
                    .durable_hit_ranges
                    .iter()
                    .map(ExecutedRange::from_ledger_range)
                    .collect(),
                provider_fill_ranges: response
                    .cache
                    .provider_fill_ranges
                    .iter()
                    .map(ExecutedRange::from_ledger_range)
                    .collect(),
                full_durable_hit,
            });
        }

        Ok(IndexRunReport {
            planned_queries: self.plan.tasks().len(),
            tasks,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexRunReport {
    pub planned_queries: usize,
    pub tasks: Vec<IndexRunTaskSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexRunTaskSummary {
    pub label: String,
    pub chain: String,
    pub range: ExecutedRange,
    pub elapsed_ms: u128,
    pub row_count: usize,
    pub hit_ranges: Vec<ExecutedRange>,
    pub missing_ranges: Vec<ExecutedRange>,
    pub durable_hit_ranges: Vec<ExecutedRange>,
    pub provider_fill_ranges: Vec<ExecutedRange>,
    pub full_durable_hit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutedRange {
    pub kind: String,
    pub start: u64,
    pub end: u64,
}

impl ExecutedRange {
    fn from_ledger_range(range: &LedgerRange) -> Self {
        Self {
            kind: range_kind_name(range.kind()),
            start: range.start(),
            end: range.end(),
        }
    }
}

fn range_kind_name(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}
