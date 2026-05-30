use std::{
    fs::{self, OpenOptions},
    io::{self, BufWriter, Write},
    path::Path,
};

use serde_json::{Map, Value};

use super::{IndexedRecord, OutputWriteReceipt, OutputWriteResult};

pub(super) fn write_records_jsonl(
    path: &Path,
    records: &[IndexedRecord],
) -> io::Result<OutputWriteResult> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    let mut last_record = None;

    for record in records {
        let row = jsonl_row(record);
        serde_json::to_writer(&mut writer, &row)?;
        writer.write_all(b"\n")?;
        last_record = Some(record_receipt_key(record));
    }
    writer.flush()?;

    Ok(OutputWriteResult {
        written_rows: records.len(),
        receipt: Some(OutputWriteReceipt {
            accepted_rows: records.len(),
            flushed_rows: records.len(),
            inserted_rows: records.len(),
            skipped_or_replaced_rows: 0,
            files_written: usize::from(!records.is_empty()),
            highest_position: last_record.clone(),
            last_record,
        }),
    })
}

fn jsonl_row(record: &IndexedRecord) -> Value {
    let mut row = Map::new();
    row.insert("index".to_owned(), Value::String(record.index.clone()));
    row.insert("chain".to_owned(), Value::String(record.chain.clone()));
    row.insert("chain_id".to_owned(), Value::from(record.chain_id));
    row.insert("dataset".to_owned(), Value::String(record.dataset.clone()));
    if let Value::Object(payload) = &record.payload {
        row.extend(payload.clone());
    } else {
        row.insert("payload".to_owned(), record.payload.clone());
    }
    Value::Object(row)
}

fn record_receipt_key(record: &IndexedRecord) -> String {
    match (
        record.payload.get("block_number"),
        record.payload.get("transaction_index"),
        record.payload.get("log_index"),
    ) {
        (Some(block), Some(transaction), Some(log)) => {
            format!("{}:{}:{}:{}", record.chain, block, transaction, log)
        }
        _ => format!("{}:{}", record.chain, record.dataset),
    }
}
