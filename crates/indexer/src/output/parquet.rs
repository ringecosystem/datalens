use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::ParquetOutputConfig;

use super::{
    IndexedRecord, OutputWriteReceipt, OutputWriteResult,
    event::{EventPosition, NormalizedIndexedEvent, max_position},
};

pub fn write_records_parquet(
    config: &ParquetOutputConfig,
    records: &[IndexedRecord],
) -> io::Result<OutputWriteResult> {
    let rows = records
        .iter()
        .map(NormalizedIndexedEvent::from_record)
        .collect::<Vec<_>>();
    let created_at = created_at_utc();
    let mut files_written = 0;
    let mut flushed_rows = 0;
    let mut highest_position = None;

    for chunk in parquet_chunks(config, &rows) {
        let chunk_highest = write_chunk(config, chunk, &created_at)?;
        files_written += 1;
        flushed_rows += chunk.len();
        highest_position = max_position(highest_position, chunk_highest);
    }

    Ok(OutputWriteResult {
        written_rows: records.len(),
        receipt: Some(OutputWriteReceipt {
            accepted_rows: records.len(),
            flushed_rows,
            inserted_rows: records.len(),
            skipped_or_replaced_rows: 0,
            files_written,
            highest_position: highest_position
                .clone()
                .map(|position| position.receipt_key),
            last_record: highest_position.map(|position| position.receipt_key),
        }),
    })
}

fn write_chunk(
    config: &ParquetOutputConfig,
    rows: &[NormalizedIndexedEvent],
    created_at: &str,
) -> io::Result<Option<EventPosition>> {
    if rows.is_empty() {
        return Ok(None);
    }

    let partition_dir = partition_dir(&config.path, &config.partition_by, &rows[0]);
    fs::create_dir_all(&partition_dir)?;
    let path = partition_dir.join(file_name(&partition_dir, rows));
    let batch = record_batch(rows, created_at)?;
    let properties = writer_properties(config)?;
    let file = fs::File::create(path)?;
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), Some(properties)).map_err(io::Error::other)?;
    writer.write(&batch).map_err(io::Error::other)?;
    writer.close().map_err(io::Error::other)?;

    Ok(rows.iter().fold(None, |current, row| {
        max_position(current, row.position.clone())
    }))
}

fn parquet_chunks<'a>(
    config: &ParquetOutputConfig,
    rows: &'a [NormalizedIndexedEvent],
) -> Vec<&'a [NormalizedIndexedEvent]> {
    if rows.is_empty() {
        return Vec::new();
    }

    let max_rows = config.max_rows_per_file.unwrap_or(50_000).max(1);
    let max_bytes = config
        .max_bytes_per_file
        .unwrap_or(128 * 1024 * 1024)
        .max(1);
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut estimated_bytes = 0;

    for (index, row) in rows.iter().enumerate() {
        estimated_bytes += estimated_row_bytes(row);
        let row_count = index + 1 - start;
        if row_count >= max_rows || (row_count > 1 && estimated_bytes >= max_bytes) {
            chunks.push(&rows[start..=index]);
            start = index + 1;
            estimated_bytes = 0;
        }
    }
    if start < rows.len() {
        chunks.push(&rows[start..]);
    }
    chunks
}

fn record_batch(rows: &[NormalizedIndexedEvent], created_at: &str) -> io::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_key", DataType::Utf8, false),
        Field::new("index_name", DataType::Utf8, false),
        Field::new("chain_family", DataType::Utf8, false),
        Field::new("chain_id", DataType::Int64, false),
        Field::new("chain_name", DataType::Utf8, false),
        Field::new("chain_identity", DataType::Utf8, false),
        Field::new("dataset", DataType::Utf8, false),
        Field::new("block_number", DataType::Int64, false),
        Field::new("block_hash", DataType::Utf8, true),
        Field::new("transaction_hash", DataType::Utf8, true),
        Field::new("transaction_index", DataType::Int64, true),
        Field::new("event_index", DataType::Int64, true),
        Field::new("selector", DataType::Utf8, true),
        Field::new("topics_json", DataType::Utf8, true),
        Field::new("signature", DataType::Utf8, true),
        Field::new("data_payload", DataType::Utf8, true),
        Field::new("raw_payload", DataType::Utf8, false),
        Field::new("removed", DataType::Boolean, true),
        Field::new("finality", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        string_values(rows.iter().map(|row| row.unique_key.as_str())),
        string_values(rows.iter().map(|row| row.index_name.as_str())),
        string_values(rows.iter().map(|row| row.chain_family.as_str())),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.chain_id),
        )),
        string_values(rows.iter().map(|row| row.chain_name.as_str())),
        string_values(rows.iter().map(|row| row.chain_identity.as_str())),
        string_values(rows.iter().map(|row| row.dataset.as_str())),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.block_number),
        )),
        optional_string_values(rows.iter().map(|row| row.block_hash.as_deref())),
        optional_string_values(rows.iter().map(|row| row.transaction_hash.as_deref())),
        Arc::new(Int64Array::from_iter(
            rows.iter().map(|row| row.transaction_index),
        )),
        Arc::new(Int64Array::from_iter(
            rows.iter().map(|row| row.event_index),
        )),
        optional_string_values(rows.iter().map(|row| row.selector.as_deref())),
        optional_string_values(rows.iter().map(|row| row.topics_json.as_deref())),
        optional_string_values(rows.iter().map(|row| row.signature.as_deref())),
        optional_string_values(rows.iter().map(|row| row.data_payload.as_deref())),
        string_values(rows.iter().map(|row| row.raw_payload.as_str())),
        Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| row.removed.map(|value| value != 0)),
        )),
        optional_string_values(rows.iter().map(|row| row.finality.as_deref())),
        string_values(rows.iter().map(|_| created_at)),
    ];

    RecordBatch::try_new(schema, columns).map_err(io::Error::other)
}

fn string_values<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn optional_string_values<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn writer_properties(config: &ParquetOutputConfig) -> io::Result<WriterProperties> {
    let compression = match config.compression.as_deref() {
        Some("uncompressed") | None => Compression::UNCOMPRESSED,
        Some("snappy") => Compression::SNAPPY,
        Some("zstd") => Compression::ZSTD(ZstdLevel::default()),
        Some(value) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported parquet compression {value}"),
            ));
        }
    };
    Ok(WriterProperties::builder()
        .set_compression(compression)
        .build())
}

fn partition_dir(root: &Path, partition_by: &[String], row: &NormalizedIndexedEvent) -> PathBuf {
    let mut path = root.to_path_buf();
    for field in partition_by {
        let value = match field.as_str() {
            "index" => row.index_name.as_str().to_owned(),
            "chain_family" => row.chain_family.as_str().to_owned(),
            "chain_id" => row.chain_id.to_string(),
            "chain" => row.chain_name.as_str().to_owned(),
            "dataset" => row.dataset.as_str().to_owned(),
            _ => continue,
        };
        path.push(format!("{field}={}", partition_value(&value)));
    }
    path
}

fn file_name(dir: &Path, rows: &[NormalizedIndexedEvent]) -> String {
    let sequence = next_sequence(dir);
    let start = rows.first().map(|row| row.block_number).unwrap_or_default();
    let end = rows.last().map(|row| row.block_number).unwrap_or_default();
    format!("part-{sequence:06}-blocks-{start}-{end}.parquet")
}

fn next_sequence(dir: &Path) -> usize {
    fs::read_dir(dir)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .filter(|entry| {
            entry.path().extension().and_then(|value| value.to_str()) == Some("parquet")
        })
        .count()
}

fn partition_value(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => character,
            _ => '_',
        })
        .collect()
}

fn estimated_row_bytes(row: &NormalizedIndexedEvent) -> usize {
    row.unique_key.len()
        + row.index_name.len()
        + row.chain_family.len()
        + row.chain_name.len()
        + row.chain_identity.len()
        + row.dataset.len()
        + row.block_hash.as_ref().map(String::len).unwrap_or_default()
        + row
            .transaction_hash
            .as_ref()
            .map(String::len)
            .unwrap_or_default()
        + row.selector.as_ref().map(String::len).unwrap_or_default()
        + row
            .topics_json
            .as_ref()
            .map(String::len)
            .unwrap_or_default()
        + row.signature.as_ref().map(String::len).unwrap_or_default()
        + row
            .data_payload
            .as_ref()
            .map(String::len)
            .unwrap_or_default()
        + row.raw_payload.len()
        + row.finality.as_ref().map(String::len).unwrap_or_default()
        + 96
}

fn created_at_utc() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}Z", duration.as_secs(), duration.subsec_nanos())
}
