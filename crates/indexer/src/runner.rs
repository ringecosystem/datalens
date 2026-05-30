use datalens_client::{DatalensClient, HttpTransport};
use datalens_core::{
    ChainFamily, ChainIdentity, DatasetKey, LedgerRange, LedgerRangeKind, LogFilter, NetworkId,
    QueryFinalityRequirement, QueryRows, missing_ranges,
};
use serde::Serialize;
use std::{collections::BTreeMap, path::PathBuf, time::Instant};

use crate::{
    CheckpointPolicy, IndexCheckpointFile, IndexCheckpointFileStore, IndexPlan, IndexedRecord,
    IndexerError, OutputSinkConfig, OutputWriteSink, PlannedDecodeEvent, PlannedIndexTask,
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
        let mut output_sink: Option<Box<dyn OutputWriteSink>> = None;
        let mut output_buffers_records = false;
        let mut tasks = Vec::new();
        let mut pending_checkpoints = Vec::new();
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

            let range = task_ledger_range(task, range.start, range.end)?;
            let chain = task_chain_identity(task)?;
            let dataset_key = DatasetKey::parse(&task.dataset)
                .map_err(|error| IndexerError::Runner(error.to_string()))?;
            let selector = task_query_selector(task)?;
            let started = Instant::now();
            let response = client
                .query(
                    datalens_client::QueryRequest::new(chain, dataset_key, range.clone())
                        .with_selector(selector)
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
                    let records =
                        evm_log_records(self.plan.index(), self.plan.decode_events(), task, rows);
                    if output_sink.is_none() {
                        let sink = self.output.open_write_sink().map_err(|error| {
                            let output_kind = self.output.capability().kind.as_str();
                            IndexerError::Runner(format!(
                                "failed to open {output_kind} output: {error}"
                            ))
                        })?;
                        output_buffers_records = sink.buffers_records();
                        output_sink = Some(sink);
                    }
                    let output_sink = output_sink.as_ref().expect("output sink was opened");
                    output_sink.write_records(&records).map_err(|error| {
                        let output_kind = self.output.capability().kind.as_str();
                        IndexerError::Runner(format!(
                            "task {} failed to write {output_kind} output: {error}",
                            task.label
                        ))
                    })?;
                }
                QueryRows::AdapterJson { dataset_key, rows }
                    if dataset_key.as_str() == task.dataset =>
                {
                    let records = adapter_json_records(self.plan.index(), task, rows);
                    if output_sink.is_none() {
                        let sink = self.output.open_write_sink().map_err(|error| {
                            let output_kind = self.output.capability().kind.as_str();
                            IndexerError::Runner(format!(
                                "failed to open {output_kind} output: {error}"
                            ))
                        })?;
                        output_buffers_records = sink.buffers_records();
                        output_sink = Some(sink);
                    }
                    let output_sink = output_sink.as_ref().expect("output sink was opened");
                    output_sink.write_records(&records).map_err(|error| {
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
                if output_buffers_records {
                    pending_checkpoints.push((task.clone(), range.end()));
                } else {
                    store.advance(task, range.end())?;
                }
            }
        }

        if let Some(output_sink) = &output_sink {
            output_sink.flush().map_err(|error| {
                let output_kind = self.output.capability().kind.as_str();
                IndexerError::Runner(format!("failed to flush {output_kind} output: {error}"))
            })?;
        }
        if let Some(store) = &checkpoint_store {
            for (task, completed_block) in pending_checkpoints {
                store.advance(&task, completed_block)?;
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
    decode_events: &[PlannedDecodeEvent],
    task: &PlannedIndexTask,
    rows: &[datalens_core::LogRecord],
) -> Vec<IndexedRecord> {
    rows.iter()
        .map(|row| {
            let mut payload = serde_json::json!({
                "block_number": row.block_number,
                "block_hash": row.block_hash,
                "transaction_hash": row.transaction_hash,
                "transaction_index": row.transaction_index,
                "log_index": row.log_index,
                "address": row.address,
                "topics": row.topics,
                "data": row.data,
                "removed": row.removed,
            });
            if let Some(event) = decoded_event_for_log(decode_events, row)
                && let Some(object) = payload.as_object_mut()
            {
                object.insert(
                    "signature".to_owned(),
                    serde_json::Value::String(event.signature.clone()),
                );
                object.insert(
                    "event_name".to_owned(),
                    serde_json::Value::String(event.name.clone()),
                );
                object.insert("decoded".to_owned(), serde_json::json!({}));
            }
            IndexedRecord {
                index: index.to_owned(),
                chain: task.chain.to_owned(),
                chain_id: task.chain_id,
                dataset: task.dataset.to_owned(),
                payload,
            }
        })
        .collect()
}

fn decoded_event_for_log<'a>(
    decode_events: &'a [PlannedDecodeEvent],
    row: &datalens_core::LogRecord,
) -> Option<&'a PlannedDecodeEvent> {
    let topic0 = row.topics.first()?;
    decode_events
        .iter()
        .find(|event| event.topic0.eq_ignore_ascii_case(topic0))
}

fn adapter_json_records(
    index: &str,
    task: &PlannedIndexTask,
    rows: &[serde_json::Value],
) -> Vec<IndexedRecord> {
    rows.iter()
        .map(|row| IndexedRecord {
            index: index.to_owned(),
            chain: task.chain.to_owned(),
            chain_id: task.chain_id,
            dataset: task.dataset.to_owned(),
            payload: generic_payload(task, row),
        })
        .collect()
}

fn generic_payload(task: &PlannedIndexTask, row: &serde_json::Value) -> serde_json::Value {
    let position_value = row
        .get("slot")
        .or_else(|| row.get("block_number"))
        .or_else(|| row.get("height"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(task.range.start);
    let position_kind = if row.get("slot").is_some() {
        "slot"
    } else if row.get("height").is_some() {
        "height"
    } else {
        task.range.kind.as_str()
    };
    let transaction_id = row
        .get("transaction_hash")
        .or_else(|| row.get("transaction_id"))
        .or_else(|| row.get("signature"))
        .and_then(serde_json::Value::as_str);
    let event_id = row
        .get("event_index")
        .or_else(|| row.get("log_index"))
        .and_then(serde_json::Value::as_u64);

    let mut payload = serde_json::Map::new();
    payload.insert(
        "chain_family".to_owned(),
        serde_json::Value::String(task.family.clone()),
    );
    if let Some(network_id) = &task.network_id {
        payload.insert(
            "network_id".to_owned(),
            serde_json::Value::String(network_id.clone()),
        );
    }
    payload.insert(
        "position".to_owned(),
        serde_json::json!({ "kind": position_kind, "value": position_value }),
    );
    if let Some(transaction_id) = transaction_id {
        payload.insert(
            "transaction_id".to_owned(),
            serde_json::Value::String(transaction_id.to_owned()),
        );
    }
    if let Some(signature) = row.get("signature").and_then(serde_json::Value::as_str) {
        payload.insert(
            "signature".to_owned(),
            serde_json::Value::String(signature.to_owned()),
        );
    }
    if let Some(event_id) = event_id {
        payload.insert("event_id".to_owned(), serde_json::Value::from(event_id));
    }
    payload.insert("selector".to_owned(), selector_metadata(task));
    payload.insert(
        "finality".to_owned(),
        serde_json::Value::String(task.finality.clone()),
    );
    payload.insert("raw".to_owned(), row.clone());
    serde_json::Value::Object(payload)
}

fn task_ledger_range(
    task: &PlannedIndexTask,
    start: u64,
    end: u64,
) -> Result<LedgerRange, IndexerError> {
    match task.range.kind.as_str() {
        "block" => LedgerRange::blocks(start, end),
        "slot" => LedgerRange::slots(start, end),
        "height" => LedgerRange::heights(start, end),
        value => LedgerRange::try_new(LedgerRangeKind::Other(value.to_owned()), start, end),
    }
    .map_err(|error| IndexerError::Runner(error.to_string()))
}

fn task_chain_identity(task: &PlannedIndexTask) -> Result<ChainIdentity, IndexerError> {
    let family = match task.family.as_str() {
        "evm" => ChainFamily::Evm,
        value => ChainFamily::Other(value.to_owned()),
    };
    let network_id = if let Some(network_id) = &task.network_id {
        Some(
            NetworkId::textual(network_id.clone())
                .map_err(|error| IndexerError::Runner(error.to_string()))?,
        )
    } else if task.chain_id == 0 {
        None
    } else {
        Some(NetworkId::numeric(task.chain_id))
    };
    ChainIdentity::try_new(family, task.chain.clone(), network_id)
        .map_err(|error| IndexerError::Runner(error.to_string()))
}

fn task_query_selector(
    task: &PlannedIndexTask,
) -> Result<datalens_client::QuerySelector, IndexerError> {
    match task.selector.kind.as_str() {
        "evm_logs" => Ok(datalens_client::QuerySelector::EvmLogs(LogFilter {
            addresses: task.selector.addresses.clone(),
            topics: task
                .selector
                .topics
                .iter()
                .cloned()
                .map(|topic| Some(vec![topic]))
                .collect(),
        })),
        "solana_all" => Ok(datalens_client::QuerySelector::solana_all()),
        "solana_address" => datalens_client::QuerySelector::solana_address(
            task.selector
                .values
                .first()
                .map(String::as_str)
                .unwrap_or_default(),
        ),
        "solana_program" => datalens_client::QuerySelector::solana_program(
            task.selector
                .values
                .first()
                .map(String::as_str)
                .unwrap_or_default(),
        ),
        "solana_signature" => datalens_client::QuerySelector::solana_signature(
            task.selector
                .values
                .first()
                .map(String::as_str)
                .unwrap_or_default(),
        ),
        "tron_events" => {
            datalens_client::QuerySelector::tron_event(datalens_client::TronEventSelector {
                contract_addresses: task.selector.addresses.clone(),
                event_names: task.selector.topics.clone(),
            })
        }
        value => Err(datalens_client::ClientError::InvalidInput(format!(
            "unsupported selector kind {value}"
        ))),
    }
    .map_err(|error| IndexerError::Runner(error.to_string()))
}

fn selector_metadata(task: &PlannedIndexTask) -> serde_json::Value {
    serde_json::json!({
        "kind": task.selector.kind,
        "fingerprint": task.selector.fingerprint,
        "canonical_key": task.selector.canonical_key,
    })
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
