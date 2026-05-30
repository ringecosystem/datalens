use datalens_client::{DatalensClient, HttpTransport};
use datalens_core::{DatasetKey, LedgerRange, QueryFinalityRequirement, QueryRows, missing_ranges};
use serde::Serialize;
use std::{collections::BTreeMap, path::PathBuf, time::Instant};

use crate::{
    CheckpointPolicy, CheckpointSkippedRange, ExecutedRange, IndexCheckpointFile,
    IndexCheckpointFileStore, IndexPlan, IndexRunTaskSummary, IndexerError, PlannedIndexTask,
    sdk::{
        ApplicationProcessor, CheckpointCursor, EventBatch, EventOrderingKey, EventRecord,
        ProcessorContext, ProcessorError, TransactionalApplicationStore,
    },
    task_chain_identity, task_ledger_range, task_query_selector,
};

#[derive(Clone)]
pub struct ProcessorRuntime<P, S> {
    plan: IndexPlan,
    processor: P,
    store: S,
    options: ProcessorRuntimeOptions,
}

impl<P, S> ProcessorRuntime<P, S>
where
    P: ApplicationProcessor,
    S: TransactionalApplicationStore,
{
    pub fn new(plan: IndexPlan, processor: P, store: S) -> Self {
        Self {
            plan,
            processor,
            store,
            options: ProcessorRuntimeOptions::default(),
        }
    }

    pub fn with_options(mut self, options: ProcessorRuntimeOptions) -> Self {
        self.options = options;
        self
    }

    pub fn plan(&self) -> &IndexPlan {
        &self.plan
    }

    pub async fn run<T>(
        &self,
        client: &DatalensClient<T>,
    ) -> Result<ProcessorRunReport, IndexerError>
    where
        T: HttpTransport,
    {
        let checkpoint_store = self.options.checkpoint_store();
        let checkpoint = match &checkpoint_store {
            Some(store) if !self.options.from_start => store.load()?,
            _ => IndexCheckpointFile::empty(),
        };
        let mut tasks = Vec::new();
        let mut failures = Vec::new();
        let mut checkpoint_skipped_ranges = Vec::new();
        let mut final_checkpoint = None;

        for task in self.plan.tasks() {
            let Some(range) = crate::runner::resume_task_range(
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

            let range = task_ledger_range(task, range.start, range.end)?;
            let chain = task_chain_identity(task)?;
            let dataset_key = DatasetKey::parse(&task.dataset)
                .map_err(|error| IndexerError::Runner(error.to_string()))?;
            let selector = task_query_selector(task)?;
            let started = Instant::now();
            let response = match client.query(
                datalens_client::QueryRequest::new(
                    chain.clone(),
                    dataset_key.clone(),
                    range.clone(),
                )
                .with_selector(selector)
                .with_finality(QueryFinalityRequirement::DurableOnly)
                .with_fields(datalens_client::FieldSelection::All),
            ) {
                Ok(response) => response,
                Err(error) => {
                    failures.push(ProcessorRunFailure {
                        label: task.label.clone(),
                        chain: task.chain.clone(),
                        range: ExecutedRange::from_ledger_range(&range),
                        stage: ProcessorFailureStage::Fetch,
                        message: error.to_string(),
                        retryable: false,
                    });
                    break;
                }
            };
            let elapsed_ms = started.elapsed().as_millis();
            let full_durable_hit =
                missing_ranges(range.clone(), &response.cache.durable_hit_ranges).is_empty()
                    && response.cache.provider_fill_ranges.is_empty()
                    && response.cache.missing_ranges.is_empty();
            let row_count = response.rows.row_count();
            let batch = match event_batch_from_rows(
                task,
                chain,
                dataset_key,
                range.clone(),
                response.rows.rows(),
            ) {
                Ok(batch) => batch,
                Err(error) => {
                    failures.push(ProcessorRunFailure {
                        label: task.label.clone(),
                        chain: task.chain.clone(),
                        range: ExecutedRange::from_ledger_range(&range),
                        stage: ProcessorFailureStage::Convert,
                        message: error.to_string(),
                        retryable: false,
                    });
                    break;
                }
            };

            let tx = match self.store.begin_transaction().await {
                Ok(tx) => tx,
                Err(error) => {
                    failures.push(processor_failure(
                        task,
                        &range,
                        ProcessorFailureStage::BeginTransaction,
                        error,
                    ));
                    break;
                }
            };

            let result = {
                let mut context = ProcessorContext::new(
                    self.plan.application().to_owned(),
                    self.plan.index().to_owned(),
                    batch.chain().clone(),
                    batch.finalized_range().clone(),
                )
                .with_store(&*tx);
                self.processor.process(&mut context, &batch).await
            };

            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    rollback_transaction(&*tx).await;
                    failures.push(processor_failure(
                        task,
                        &range,
                        ProcessorFailureStage::Process,
                        error,
                    ));
                    break;
                }
            };

            if let Err(error) = tx.commit().await {
                rollback_transaction(&*tx).await;
                failures.push(processor_failure(
                    task,
                    &range,
                    ProcessorFailureStage::Commit,
                    error,
                ));
                break;
            }

            if let Some(cursor) = result.pending_checkpoint() {
                if let Some(store) = &checkpoint_store {
                    store.advance(task, range.end())?;
                }
                final_checkpoint = Some(cursor.clone());
            }

            tasks.push(IndexRunTaskSummary {
                label: task.label.clone(),
                chain: task.chain.clone(),
                range: ExecutedRange::from_ledger_range(&range),
                elapsed_ms,
                row_count,
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

        let status = if failures.is_empty() {
            ProcessorRunStatus::Succeeded
        } else {
            ProcessorRunStatus::Failed
        };
        let summary = ProcessorRunSummary::from_parts(
            self.plan.tasks().len(),
            &checkpoint_skipped_ranges,
            &tasks,
        );
        let chains = ProcessorChainRunSummary::from_tasks(&tasks);

        Ok(ProcessorRunReport {
            status,
            planned_batches: self.plan.tasks().len(),
            summary,
            chains,
            checkpoint_skipped_ranges,
            tasks,
            failures,
            final_checkpoint,
        })
    }
}

async fn rollback_transaction(tx: &(dyn crate::sdk::ApplicationStoreTransaction + Send + Sync)) {
    let _ = tx.rollback().await;
}

fn processor_failure(
    task: &PlannedIndexTask,
    range: &LedgerRange,
    stage: ProcessorFailureStage,
    error: ProcessorError,
) -> ProcessorRunFailure {
    ProcessorRunFailure {
        label: task.label.clone(),
        chain: task.chain.clone(),
        range: ExecutedRange::from_ledger_range(range),
        stage,
        message: error.to_string(),
        retryable: error.is_retryable(),
    }
}

fn event_batch_from_rows(
    task: &PlannedIndexTask,
    chain: datalens_core::ChainIdentity,
    dataset: DatasetKey,
    range: LedgerRange,
    rows: &QueryRows,
) -> Result<EventBatch, IndexerError> {
    let records = match rows {
        QueryRows::EvmLogs(rows) => rows
            .iter()
            .map(|row| EventRecord {
                source_key: format!(
                    "{}:{}:{}:{}",
                    task.chain, row.block_number, row.transaction_index, row.log_index
                ),
                ordering_key: EventOrderingKey::new(
                    row.block_number,
                    Some(row.transaction_index),
                    Some(row.log_index),
                ),
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
                decoded: None,
            })
            .collect(),
        QueryRows::AdapterJson { dataset_key, rows } if dataset_key.as_str() == task.dataset => {
            rows.iter()
                .enumerate()
                .map(|(index, row)| {
                    let ledger_position = row
                        .get("slot")
                        .or_else(|| row.get("block_number"))
                        .or_else(|| row.get("height"))
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(range.start());
                    let transaction_position = row
                        .get("transaction_index")
                        .and_then(serde_json::Value::as_u64);
                    let event_position = row
                        .get("event_index")
                        .or_else(|| row.get("log_index"))
                        .and_then(serde_json::Value::as_u64)
                        .or(Some(index as u64));
                    EventRecord {
                        source_key: format!("{}:{ledger_position}:{index}", task.chain),
                        ordering_key: EventOrderingKey::new(
                            ledger_position,
                            transaction_position,
                            event_position,
                        ),
                        payload: row.clone(),
                        decoded: None,
                    }
                })
                .collect()
        }
        _ => {
            return Err(IndexerError::Runner(format!(
                "task {} returned rows for unsupported processor dataset",
                task.label
            )));
        }
    };

    Ok(EventBatch::new(
        chain,
        dataset,
        range.clone(),
        CheckpointCursor::new(
            crate::checkpoint::checkpoint_key(task),
            range.end().to_string(),
        ),
        records,
    ))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProcessorRuntimeOptions {
    pub checkpoint: Option<CheckpointPolicy>,
    pub from_start: bool,
    pub dry_run: bool,
}

impl ProcessorRuntimeOptions {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessorRunStatus {
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProcessorRunReport {
    pub status: ProcessorRunStatus,
    pub planned_batches: usize,
    pub summary: ProcessorRunSummary,
    pub chains: Vec<ProcessorChainRunSummary>,
    pub checkpoint_skipped_ranges: Vec<CheckpointSkippedRange>,
    pub tasks: Vec<IndexRunTaskSummary>,
    pub failures: Vec<ProcessorRunFailure>,
    pub final_checkpoint: Option<CheckpointCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProcessorRunSummary {
    pub planned_batches: usize,
    pub executed_batches: usize,
    pub checkpoint_skipped_ranges: usize,
    pub elapsed_ms: u128,
    pub processed_records: usize,
    pub full_durable_hit_count: usize,
    pub provider_fill_range_count: usize,
}

impl ProcessorRunSummary {
    fn from_parts(
        planned_batches: usize,
        checkpoint_skipped_ranges: &[CheckpointSkippedRange],
        tasks: &[IndexRunTaskSummary],
    ) -> Self {
        Self {
            planned_batches,
            executed_batches: tasks.len(),
            checkpoint_skipped_ranges: checkpoint_skipped_ranges.len(),
            elapsed_ms: tasks.iter().map(|task| task.elapsed_ms).sum(),
            processed_records: tasks.iter().map(|task| task.row_count).sum(),
            full_durable_hit_count: tasks.iter().filter(|task| task.full_durable_hit).count(),
            provider_fill_range_count: tasks
                .iter()
                .map(|task| task.provider_fill_ranges.len())
                .sum(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProcessorChainRunSummary {
    pub chain: String,
    pub executed_batches: usize,
    pub elapsed_ms: u128,
    pub processed_records: usize,
    pub full_durable_hit_count: usize,
    pub provider_fill_ranges: Vec<ExecutedRange>,
}

impl ProcessorChainRunSummary {
    fn from_tasks(tasks: &[IndexRunTaskSummary]) -> Vec<Self> {
        let mut chains = BTreeMap::<String, ProcessorChainRunAccumulator>::new();
        for task in tasks {
            let chain = chains.entry(task.chain.clone()).or_default();
            chain.executed_batches += 1;
            chain.elapsed_ms += task.elapsed_ms;
            chain.processed_records += task.row_count;
            chain.full_durable_hit_count += usize::from(task.full_durable_hit);
            chain
                .provider_fill_ranges
                .extend(task.provider_fill_ranges.iter().cloned());
        }
        chains
            .into_iter()
            .map(|(chain, summary)| Self {
                chain,
                executed_batches: summary.executed_batches,
                elapsed_ms: summary.elapsed_ms,
                processed_records: summary.processed_records,
                full_durable_hit_count: summary.full_durable_hit_count,
                provider_fill_ranges: summary.provider_fill_ranges,
            })
            .collect()
    }
}

#[derive(Default)]
struct ProcessorChainRunAccumulator {
    executed_batches: usize,
    elapsed_ms: u128,
    processed_records: usize,
    full_durable_hit_count: usize,
    provider_fill_ranges: Vec<ExecutedRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProcessorRunFailure {
    pub label: String,
    pub chain: String,
    pub range: ExecutedRange,
    pub stage: ProcessorFailureStage,
    pub message: String,
    pub retryable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessorFailureStage {
    Fetch,
    Convert,
    BeginTransaction,
    Process,
    Commit,
}
