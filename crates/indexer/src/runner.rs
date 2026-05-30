use datalens_client::{DatalensClient, HttpTransport};
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatasetKey, LedgerRange, LedgerRangeKind, LogFilter,
    NetworkId, QueryFinalityRequirement, QueryRows, missing_ranges,
};
use serde::Serialize;
use std::{collections::BTreeMap, path::PathBuf, time::Instant};

use crate::{
    CheckpointPolicy, IndexCheckpointFile, IndexCheckpointFileStore, IndexPlan, IndexedRecord,
    IndexerError, OutputSinkConfig, OutputWriteSink, PlannedIndexTask,
};

#[derive(Clone)]
pub struct IndexRunner {
    plan: IndexPlan,
    output: OutputSinkConfig,
    options: IndexRunnerOptions,
}

impl IndexRunner {
    pub fn new(plan: IndexPlan, output: OutputSinkConfig) -> Self {
        Self {
            plan,
            output,
            options: IndexRunnerOptions::default(),
        }
    }

    pub fn with_options(mut self, options: IndexRunnerOptions) -> Self {
        self.options = options;
        self
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
        let checkpoint_store = self.options.checkpoint_store();
        let checkpoint = match &checkpoint_store {
            Some(store) if !self.options.from_start => store.load()?,
            _ => IndexCheckpointFile::empty(),
        };
        let mut tasks = Vec::new();
        let mut checkpoint_skipped_ranges = Vec::new();

        for task in self.plan.tasks() {
            let Some(range) = resume_task_range(
                task,
                &checkpoint,
                self.options.from_start,
                &mut checkpoint_skipped_ranges,
            )?
            else {
                continue;
            };
            if self.options.dry_run {
                continue;
            }

            let range = LedgerRange::blocks(range.start, range.end)
                .map_err(|error| IndexerError::Runner(error.to_string()))?;
            let block_range = BlockRange::try_new(range.start(), range.end())
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
            match response.rows.rows() {
                QueryRows::EvmLogs(rows) => {
                    let records = evm_log_records(
                        self.plan.index(),
                        &task.chain,
                        task.chain_id,
                        &task.dataset,
                        rows,
                    );
                    self.output.write_records(&records).map_err(|error| {
                        let output_kind = self.output.capability().kind.as_str();
                        IndexerError::Runner(format!(
                            "task {} failed to write {output_kind} output: {error}",
                            task.label
                        ))
                    })?;
                }
                _ => {
                    return Err(IndexerError::Runner(format!(
                        "task {} returned rows for unsupported output dataset",
                        task.label
                    )));
                }
            }

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
            if let Some(store) = &checkpoint_store {
                store.advance(task, range.end())?;
            }
        }

        let summary = IndexRunSummary::from_parts(
            self.plan.tasks().len(),
            &checkpoint_skipped_ranges,
            &tasks,
        );
        let chains = ChainRunSummary::from_tasks(&tasks);

        Ok(IndexRunReport {
            planned_queries: self.plan.tasks().len(),
            summary,
            chains,
            checkpoint_skipped_ranges,
            tasks,
        })
    }
}

fn evm_log_records(
    index: &str,
    chain: &str,
    chain_id: u64,
    dataset: &str,
    rows: &[datalens_core::LogRecord],
) -> Vec<IndexedRecord> {
    rows.iter()
        .map(|row| IndexedRecord {
            index: index.to_owned(),
            chain: chain.to_owned(),
            chain_id,
            dataset: dataset.to_owned(),
            payload: serde_json::json!({
                "block_number": row.block_number,
                "block_hash": row.block_hash,
                "transaction_hash": row.transaction_hash,
                "transaction_index": row.transaction_index,
                "log_index": row.log_index,
                "address": row.address,
                "topics": row.topics,
                "data": row.data,
                "removed": row.removed,
            }),
        })
        .collect()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexRunnerOptions {
    pub checkpoint: Option<CheckpointPolicy>,
    pub from_start: bool,
    pub dry_run: bool,
}

impl IndexRunnerOptions {
    pub fn with_checkpoint_policy(mut self, checkpoint: CheckpointPolicy) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    pub fn with_checkpoint_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.checkpoint = Some(CheckpointPolicy::File { path: path.into() });
        self
    }

    pub fn with_no_checkpoint(mut self, no_checkpoint: bool) -> Self {
        if no_checkpoint {
            self.checkpoint = Some(CheckpointPolicy::Disabled);
        }
        self
    }

    pub fn with_from_start(mut self, from_start: bool) -> Self {
        self.from_start = from_start;
        self
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    fn checkpoint_store(&self) -> Option<IndexCheckpointFileStore> {
        match &self.checkpoint {
            Some(CheckpointPolicy::File { path }) => Some(IndexCheckpointFileStore::new(path)),
            Some(CheckpointPolicy::Disabled) | None => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexRunReport {
    pub planned_queries: usize,
    pub summary: IndexRunSummary,
    pub chains: Vec<ChainRunSummary>,
    pub checkpoint_skipped_ranges: Vec<CheckpointSkippedRange>,
    pub tasks: Vec<IndexRunTaskSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexRunSummary {
    pub planned_queries: usize,
    pub executed_queries: usize,
    pub checkpoint_skipped_ranges: usize,
    pub elapsed_ms: u128,
    pub rows_written: usize,
    pub full_durable_hit_count: usize,
    pub provider_fill_range_count: usize,
}

impl IndexRunSummary {
    fn from_parts(
        planned_queries: usize,
        checkpoint_skipped_ranges: &[CheckpointSkippedRange],
        tasks: &[IndexRunTaskSummary],
    ) -> Self {
        Self {
            planned_queries,
            executed_queries: tasks.len(),
            checkpoint_skipped_ranges: checkpoint_skipped_ranges.len(),
            elapsed_ms: tasks.iter().map(|task| task.elapsed_ms).sum(),
            rows_written: tasks.iter().map(|task| task.row_count).sum(),
            full_durable_hit_count: tasks.iter().filter(|task| task.full_durable_hit).count(),
            provider_fill_range_count: tasks
                .iter()
                .map(|task| task.provider_fill_ranges.len())
                .sum(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChainRunSummary {
    pub chain: String,
    pub executed_queries: usize,
    pub elapsed_ms: u128,
    pub rows_written: usize,
    pub full_durable_hit_count: usize,
    pub provider_fill_ranges: Vec<ExecutedRange>,
}

impl ChainRunSummary {
    fn from_tasks(tasks: &[IndexRunTaskSummary]) -> Vec<Self> {
        let mut chains = BTreeMap::<String, ChainRunAccumulator>::new();
        for task in tasks {
            let chain = chains.entry(task.chain.clone()).or_default();
            chain.executed_queries += 1;
            chain.elapsed_ms += task.elapsed_ms;
            chain.rows_written += task.row_count;
            chain.full_durable_hit_count += usize::from(task.full_durable_hit);
            chain
                .provider_fill_ranges
                .extend(task.provider_fill_ranges.iter().cloned());
        }
        chains
            .into_iter()
            .map(|(chain, summary)| Self {
                chain,
                executed_queries: summary.executed_queries,
                elapsed_ms: summary.elapsed_ms,
                rows_written: summary.rows_written,
                full_durable_hit_count: summary.full_durable_hit_count,
                provider_fill_ranges: summary.provider_fill_ranges,
            })
            .collect()
    }
}

#[derive(Default)]
struct ChainRunAccumulator {
    executed_queries: usize,
    elapsed_ms: u128,
    rows_written: usize,
    full_durable_hit_count: usize,
    provider_fill_ranges: Vec<ExecutedRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CheckpointSkippedRange {
    pub label: String,
    pub chain: String,
    pub range: ExecutedRange,
    pub reason: String,
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

fn resume_task_range(
    task: &PlannedIndexTask,
    checkpoint: &IndexCheckpointFile,
    from_start: bool,
    checkpoint_skipped_ranges: &mut Vec<CheckpointSkippedRange>,
) -> Result<Option<crate::PlannedRange>, IndexerError> {
    if from_start {
        return Ok(Some(task.range.clone()));
    }

    let Some(last_completed_block) = checkpoint.last_completed_block(task) else {
        return Ok(Some(task.range.clone()));
    };

    if last_completed_block < task.range.start {
        return Ok(Some(task.range.clone()));
    }

    let skipped_end = last_completed_block.min(task.range.end);
    checkpoint_skipped_ranges.push(CheckpointSkippedRange {
        label: task.label.clone(),
        chain: task.chain.clone(),
        range: ExecutedRange {
            kind: task.range.kind.clone(),
            start: task.range.start,
            end: skipped_end,
        },
        reason: "covered_by_checkpoint".to_owned(),
    });

    if last_completed_block >= task.range.end {
        return Ok(None);
    }

    Ok(Some(crate::PlannedRange {
        kind: task.range.kind.clone(),
        start: last_completed_block.saturating_add(1),
        end: task.range.end,
    }))
}
