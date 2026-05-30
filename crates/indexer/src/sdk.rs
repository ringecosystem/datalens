//! Public processor SDK boundary for application-owned index processors.
//!
//! Datalens owns fetching, batching, checkpoint persistence, metrics plumbing,
//! and recovery. Applications implement [`ApplicationProcessor`] against
//! normalized event batches and the limited context traits in this module.

use std::{error::Error, fmt, future::Future, pin::Pin};

use datalens_core::{ChainIdentity, DatasetKey, LedgerRange};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type ProcessorFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ApplicationProcessor: Send + Sync {
    fn process<'a>(
        &'a self,
        context: &'a mut ProcessorContext<'a>,
        batch: &'a EventBatch,
    ) -> ProcessorFuture<'a, Result<ProcessResult, ProcessorError>>;
}

pub trait ApplicationStore: Send + Sync {
    fn upsert_json<'a>(
        &'a self,
        key: &'a str,
        value: Value,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>>;

    fn delete<'a>(&'a self, key: &'a str) -> ProcessorFuture<'a, Result<(), ProcessorError>>;
}

pub trait ApplicationStoreTransaction: ApplicationStore {
    fn commit<'a>(&'a self) -> ProcessorFuture<'a, Result<(), ProcessorError>>;

    fn rollback<'a>(&'a self) -> ProcessorFuture<'a, Result<(), ProcessorError>>;
}

pub trait TransactionalApplicationStore: Send + Sync {
    fn begin_transaction<'a>(
        &'a self,
    ) -> ProcessorFuture<
        'a,
        Result<Box<dyn ApplicationStoreTransaction + Send + Sync + 'a>, ProcessorError>,
    >;
}

pub trait ApplicationChainReader: Send + Sync {
    fn read_json<'a>(
        &'a self,
        chain: &'a ChainIdentity,
        key: &'a str,
    ) -> ProcessorFuture<'a, Result<Value, ProcessorError>>;
}

pub trait ProcessorMetrics: Send + Sync {
    fn increment_counter(&self, name: &str, value: u64);
}

#[derive(Clone)]
pub struct ProcessorContext<'a> {
    application: String,
    index: String,
    chain: ChainIdentity,
    finalized_range: LedgerRange,
    store: Option<&'a (dyn ApplicationStore + Send + Sync)>,
    chain_reader: Option<&'a (dyn ApplicationChainReader + Send + Sync)>,
    metrics: Option<&'a (dyn ProcessorMetrics + Send + Sync)>,
}

impl<'a> ProcessorContext<'a> {
    pub fn new(
        application: impl Into<String>,
        index: impl Into<String>,
        chain: ChainIdentity,
        finalized_range: LedgerRange,
    ) -> Self {
        Self {
            application: application.into(),
            index: index.into(),
            chain,
            finalized_range,
            store: None,
            chain_reader: None,
            metrics: None,
        }
    }

    pub fn with_store(mut self, store: &'a (dyn ApplicationStore + Send + Sync)) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_chain_reader(
        mut self,
        chain_reader: &'a (dyn ApplicationChainReader + Send + Sync),
    ) -> Self {
        self.chain_reader = Some(chain_reader);
        self
    }

    pub fn with_metrics(mut self, metrics: &'a (dyn ProcessorMetrics + Send + Sync)) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn application(&self) -> &str {
        &self.application
    }

    pub fn index(&self) -> &str {
        &self.index
    }

    pub fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub fn finalized_range(&self) -> &LedgerRange {
        &self.finalized_range
    }

    pub fn store(&self) -> Option<&'a (dyn ApplicationStore + Send + Sync)> {
        self.store
    }

    pub fn chain_reader(&self) -> Option<&'a (dyn ApplicationChainReader + Send + Sync)> {
        self.chain_reader
    }

    pub fn metrics(&self) -> Option<&'a (dyn ProcessorMetrics + Send + Sync)> {
        self.metrics
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointCursor {
    key: String,
    value: String,
}

impl CheckpointCursor {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventBatch {
    chain: ChainIdentity,
    dataset: DatasetKey,
    finalized_range: LedgerRange,
    checkpoint_cursor: CheckpointCursor,
    ordering: EventOrdering,
    records: Vec<EventRecord>,
}

impl EventBatch {
    pub fn new(
        chain: ChainIdentity,
        dataset: DatasetKey,
        finalized_range: LedgerRange,
        checkpoint_cursor: CheckpointCursor,
        mut records: Vec<EventRecord>,
    ) -> Self {
        records.sort_by(|left, right| {
            left.ordering_key
                .cmp(&right.ordering_key)
                .then_with(|| left.source_key.cmp(&right.source_key))
        });
        Self {
            chain,
            dataset,
            finalized_range,
            checkpoint_cursor,
            ordering: EventOrdering::Deterministic,
            records,
        }
    }

    pub fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub fn dataset(&self) -> &DatasetKey {
        &self.dataset
    }

    pub fn finalized_range(&self) -> &LedgerRange {
        &self.finalized_range
    }

    pub fn checkpoint_cursor(&self) -> &CheckpointCursor {
        &self.checkpoint_cursor
    }

    pub fn ordering(&self) -> EventOrdering {
        self.ordering
    }

    pub fn ordering_description(&self) -> &'static str {
        self.ordering.description()
    }

    pub fn records(&self) -> &[EventRecord] {
        &self.records
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOrdering {
    Deterministic,
}

impl EventOrdering {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Deterministic => {
                "records are sorted by ledger position, transaction position, event position, then source key"
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub source_key: String,
    pub ordering_key: EventOrderingKey,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decoded: Option<Value>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EventOrderingKey {
    pub ledger_position: u64,
    pub transaction_position: Option<u64>,
    pub event_position: Option<u64>,
}

impl EventOrderingKey {
    pub fn new(
        ledger_position: u64,
        transaction_position: Option<u64>,
        event_position: Option<u64>,
    ) -> Self {
        Self {
            ledger_position,
            transaction_position,
            event_position,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessResult {
    processed_records: usize,
    pending_checkpoint: Option<CheckpointCursor>,
    skipped_reason: Option<String>,
}

impl ProcessResult {
    /// Marks a processor batch as successful.
    ///
    /// The cursor is only a pending runtime commit target. Processor success
    /// must not directly advance durable checkpoints; Datalens may advance the
    /// checkpoint only after the runtime commits application side effects.
    pub fn success(pending_checkpoint: CheckpointCursor) -> Self {
        Self {
            processed_records: 0,
            pending_checkpoint: Some(pending_checkpoint),
            skipped_reason: None,
        }
    }

    pub fn skipped(reason: impl Into<String>) -> Self {
        Self {
            processed_records: 0,
            pending_checkpoint: None,
            skipped_reason: Some(reason.into()),
        }
    }

    pub fn with_processed_records(mut self, processed_records: usize) -> Self {
        self.processed_records = processed_records;
        self
    }

    pub fn processed_records(&self) -> usize {
        self.processed_records
    }

    pub fn pending_checkpoint(&self) -> Option<&CheckpointCursor> {
        self.pending_checkpoint.as_ref()
    }

    pub fn skipped_reason(&self) -> Option<&str> {
        self.skipped_reason.as_deref()
    }

    pub fn checkpoint_requires_runtime_commit(&self) -> bool {
        self.pending_checkpoint.is_some()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessorError {
    kind: ProcessorErrorKind,
    message: String,
    retryable: bool,
}

impl ProcessorError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ProcessorErrorKind::Config, message, false)
    }

    pub fn user(message: impl Into<String>) -> Self {
        Self::new(ProcessorErrorKind::UserProcessor, message, false)
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ProcessorErrorKind::TransientInfrastructure, message, true)
    }

    pub fn data(message: impl Into<String>) -> Self {
        Self::new(ProcessorErrorKind::NonRetryableData, message, false)
    }

    pub fn new(kind: ProcessorErrorKind, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            kind,
            message: redact_secrets(message.into()),
            retryable,
        }
    }

    pub fn kind(&self) -> ProcessorErrorKind {
        self.kind
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ProcessorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl Error for ProcessorError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessorErrorKind {
    Config,
    UserProcessor,
    TransientInfrastructure,
    NonRetryableData,
}

impl ProcessorErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Config => "processor config error",
            Self::UserProcessor => "user processor error",
            Self::TransientInfrastructure => "transient infrastructure error",
            Self::NonRetryableData => "non-retryable data error",
        }
    }
}

fn redact_secrets(message: String) -> String {
    message
        .split_whitespace()
        .map(|part| {
            if part.contains("://") || part.contains("token=") || part.contains("key=") {
                "<redacted>"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
