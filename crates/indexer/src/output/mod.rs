use std::{
    fs::{self, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use datalens_core::LogRecord;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputSinkConfig {
    StdoutJson,
    FileJson { path: PathBuf },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRecord {
    pub query: String,
    pub payload: serde_json::Value,
}

#[derive(Serialize)]
struct EvmLogJsonlRow<'a> {
    index: &'a str,
    chain: &'a str,
    chain_id: u64,
    dataset: &'a str,
    block_number: u64,
    block_hash: &'a str,
    transaction_hash: &'a str,
    transaction_index: u64,
    log_index: u64,
    address: &'a str,
    topics: &'a [String],
    data: &'a str,
    removed: bool,
}

impl OutputSinkConfig {
    pub(crate) fn write_evm_logs(
        &self,
        index: &str,
        chain: &str,
        chain_id: u64,
        dataset: &str,
        rows: &[LogRecord],
    ) -> io::Result<()> {
        match self {
            Self::StdoutJson => Ok(()),
            Self::FileJson { path } => {
                write_evm_logs_jsonl(path, index, chain, chain_id, dataset, rows)
            }
        }
    }
}

fn write_evm_logs_jsonl(
    path: &Path,
    index: &str,
    chain: &str,
    chain_id: u64,
    dataset: &str,
    rows: &[LogRecord],
) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    for row in rows {
        serde_json::to_writer(
            &mut writer,
            &EvmLogJsonlRow {
                index,
                chain,
                chain_id,
                dataset,
                block_number: row.block_number,
                block_hash: &row.block_hash,
                transaction_hash: &row.transaction_hash,
                transaction_index: row.transaction_index,
                log_index: row.log_index,
                address: &row.address,
                topics: &row.topics,
                data: &row.data,
                removed: row.removed,
            },
        )?;
        writer.write_all(b"\n")?;
    }
    writer.flush()
}
