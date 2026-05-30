use std::{fmt, io, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::ParquetOutputConfig;

use super::{
    jsonl::write_records_jsonl, parquet::ParquetOutputSink, postgres::PostgresOutputStore,
    sqlite::SqliteOutputStore, webhook::WebhookOutputSink,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputSinkConfig {
    StdoutJson,
    FileJson { path: PathBuf },
    DatabaseSqlite { url: String },
    DatabasePostgres { url: String },
    Parquet { config: ParquetOutputConfig },
    Webhook { webhook: WebhookOutputConfig },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexedRecord {
    pub index: String,
    pub chain: String,
    pub chain_id: u64,
    pub dataset: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputWriteResult {
    pub written_rows: usize,
    pub receipt: Option<OutputWriteReceipt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputWriteReceipt {
    pub accepted_rows: usize,
    pub flushed_rows: usize,
    pub inserted_rows: usize,
    pub skipped_or_replaced_rows: usize,
    pub files_written: usize,
    pub batches_attempted: usize,
    pub batches_delivered: usize,
    pub highest_position: Option<String>,
    pub last_record: Option<String>,
}

pub trait OutputWriteSink {
    fn write_records(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult>;

    fn flush(&self) -> io::Result<OutputWriteResult> {
        Ok(OutputWriteResult {
            written_rows: 0,
            receipt: None,
        })
    }

    fn buffers_records(&self) -> bool {
        false
    }
}

impl OutputWriteSink for OutputSinkConfig {
    fn write_records(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult> {
        match self {
            Self::StdoutJson => Ok(OutputWriteResult {
                written_rows: 0,
                receipt: None,
            }),
            Self::FileJson { path } => write_records_jsonl(path, records),
            Self::DatabaseSqlite { url } => SqliteOutputStore::connect(url)?.write_records(records),
            Self::DatabasePostgres { url } => {
                PostgresOutputStore::connect(url)?.write_records(records)
            }
            Self::Parquet { config } => {
                let sink = ParquetOutputSink::new(config.clone());
                let mut result = sink.write_records(records)?;
                let flush = sink.flush()?;
                if let (Some(receipt), Some(flush_receipt)) =
                    (result.receipt.as_mut(), flush.receipt)
                {
                    receipt.flushed_rows += flush_receipt.flushed_rows;
                    receipt.files_written += flush_receipt.files_written;
                    receipt.batches_attempted += flush_receipt.batches_attempted;
                    receipt.batches_delivered += flush_receipt.batches_delivered;
                    if flush_receipt.highest_position.is_some() {
                        receipt.highest_position = flush_receipt.highest_position;
                        receipt.last_record = flush_receipt.last_record;
                    }
                }
                Ok(result)
            }
            Self::Webhook { webhook } => {
                WebhookOutputSink::connect(webhook.clone())?.write_records(records)
            }
        }
    }

    fn flush(&self) -> io::Result<OutputWriteResult> {
        match self {
            Self::Webhook { webhook } => WebhookOutputSink::connect(webhook.clone())?.flush(),
            _ => Ok(OutputWriteResult {
                written_rows: 0,
                receipt: None,
            }),
        }
    }
}

impl OutputSinkConfig {
    pub fn open_write_sink(&self) -> io::Result<Box<dyn OutputWriteSink>> {
        match self {
            Self::StdoutJson | Self::FileJson { .. } => Ok(Box::new(self.clone())),
            Self::Webhook { webhook } => Ok(Box::new(WebhookOutputSink::connect(webhook.clone())?)),
            Self::DatabaseSqlite { url } => Ok(Box::new(SqliteOutputStore::connect(url)?)),
            Self::DatabasePostgres { url } => Ok(Box::new(PostgresOutputStore::connect(url)?)),
            Self::Parquet { config } => Ok(Box::new(ParquetOutputSink::new(config.clone()))),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WebhookHeaderConfig {
    pub name: String,
    pub value: String,
    pub secret: bool,
}

impl fmt::Debug for WebhookHeaderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookHeaderConfig")
            .field("name", &self.name)
            .field(
                "value",
                &if self.secret {
                    "<redacted>"
                } else {
                    &self.value
                },
            )
            .field("secret", &self.secret)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WebhookRetryConfig {
    pub max_attempts: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub retry_429: bool,
}

impl Default for WebhookRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            retry_429: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WebhookOutboxConfig {
    pub enabled: bool,
    pub path: Option<PathBuf>,
    pub max_attempts: usize,
}

impl Default for WebhookOutboxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: None,
            max_attempts: 12,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WebhookOutputConfig {
    pub url: String,
    pub headers: Vec<WebhookHeaderConfig>,
    pub timeout_ms: u64,
    pub max_rows_per_request: usize,
    pub max_bytes_per_request: usize,
    pub retry: WebhookRetryConfig,
    pub idempotency_key_header: Option<String>,
    pub outbox: WebhookOutboxConfig,
}
