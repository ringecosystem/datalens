use std::{io, path::PathBuf};

use serde::{Deserialize, Serialize};

use super::jsonl::write_records_jsonl;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputSinkConfig {
    StdoutJson,
    FileJson { path: PathBuf },
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
        }
    }
}
