use std::{io, path::PathBuf};

use serde::{Deserialize, Serialize};

use super::{jsonl::write_records_jsonl, sqlite::SqliteOutputStore};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputSinkConfig {
    StdoutJson,
    FileJson { path: PathBuf },
    DatabaseSqlite { url: String },
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
    pub inserted_rows: usize,
    pub skipped_or_replaced_rows: usize,
    pub highest_position: Option<String>,
    pub last_record: Option<String>,
}

pub trait OutputWriteSink {
    fn write_records(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult>;
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
        }
    }
}
