use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
    sink::OutputWriteSink,
};

pub struct ParquetOutputSink {
    config: ParquetOutputConfig,
    buffers: Mutex<BTreeMap<PathBuf, ParquetPartitionBuffer>>,
}

impl ParquetOutputSink {
    pub fn new(config: ParquetOutputConfig) -> Self {
        Self {
            config,
            buffers: Mutex::new(BTreeMap::new()),
        }
    }

    fn flush_all_buffers(
        &self,
        buffers: &mut BTreeMap<PathBuf, ParquetPartitionBuffer>,
    ) -> io::Result<ParquetFlushSummary> {
        let keys = buffers.keys().cloned().collect::<Vec<_>>();
        self.flush_buffers(buffers, keys)
    }

    fn flush_buffers(
        &self,
        buffers: &mut BTreeMap<PathBuf, ParquetPartitionBuffer>,
        keys: Vec<PathBuf>,
    ) -> io::Result<ParquetFlushSummary> {
        let created_at = created_at_utc();
        let mut summary = ParquetFlushSummary::default();

        for key in keys {
            let Some(buffer) = buffers.get(&key) else {
                continue;
            };
            if buffer.rows.is_empty() {
                continue;
            }
            let rows = buffer.rows.clone();
            let chunk_highest = write_chunk(&self.config, &rows, &created_at)?;
            summary.files_written += 1;
            summary.flushed_rows += rows.len();
            summary.highest_position = max_position(summary.highest_position, chunk_highest);
            buffers.remove(&key);
        }

        Ok(summary)
    }
}

impl OutputWriteSink for ParquetOutputSink {
    fn write_records(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult> {
        let rows = records
            .iter()
            .map(NormalizedIndexedEvent::from_record)
            .collect::<Vec<_>>();
        let mut highest_position = None;
        let mut flushed_summary = ParquetFlushSummary::default();
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|error| io::Error::other(format!("parquet buffer lock poisoned: {error}")))?;

        for row in rows {
            highest_position = max_position(highest_position, row.position.clone());
            let key = partition_dir(&self.config.path, &self.config.partition_by, &row);
            let should_flush = {
                let buffer = buffers.entry(key.clone()).or_default();
                buffer.push(row);
                buffer.should_flush(&self.config)
            };
            if should_flush {
                let flush = self.flush_buffers(&mut buffers, vec![key])?;
                flushed_summary = flushed_summary.merge(flush);
            }
        }

        Ok(OutputWriteResult {
            written_rows: records.len(),
            receipt: Some(write_receipt(
                records.len(),
                records.len(),
                flushed_summary.flushed_rows,
                flushed_summary.files_written,
                highest_position.or(flushed_summary.highest_position),
            )),
        })
    }

    fn flush(&self) -> io::Result<OutputWriteResult> {
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|error| io::Error::other(format!("parquet buffer lock poisoned: {error}")))?;
        let flush = self.flush_all_buffers(&mut buffers)?;
        Ok(OutputWriteResult {
            written_rows: flush.flushed_rows,
            receipt: Some(write_receipt(
                0,
                0,
                flush.flushed_rows,
                flush.files_written,
                flush.highest_position,
            )),
        })
    }

    fn buffers_records(&self) -> bool {
        true
    }
}

#[derive(Default)]
struct ParquetPartitionBuffer {
    rows: Vec<NormalizedIndexedEvent>,
    estimated_bytes: usize,
}

impl ParquetPartitionBuffer {
    fn push(&mut self, row: NormalizedIndexedEvent) {
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_add(estimated_row_bytes(&row));
        self.rows.push(row);
    }

    fn should_flush(&self, config: &ParquetOutputConfig) -> bool {
        let max_rows = max_rows_per_file(config);
        let max_bytes = max_bytes_per_file(config);
        self.rows.len() >= max_rows || (self.rows.len() > 1 && self.estimated_bytes >= max_bytes)
    }
}

#[derive(Default)]
struct ParquetFlushSummary {
    flushed_rows: usize,
    files_written: usize,
    highest_position: Option<EventPosition>,
}

impl ParquetFlushSummary {
    fn merge(mut self, other: Self) -> Self {
        self.flushed_rows += other.flushed_rows;
        self.files_written += other.files_written;
        self.highest_position = max_position(self.highest_position, other.highest_position);
        self
    }
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
        Field::new("parent_hash", DataType::Utf8, true),
        Field::new("block_timestamp", DataType::Int64, true),
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
        optional_string_values(rows.iter().map(|row| row.parent_hash.as_deref())),
        Arc::new(Int64Array::from_iter(
            rows.iter().map(|row| row.block_timestamp),
        )),
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
            .parent_hash
            .as_ref()
            .map(String::len)
            .unwrap_or_default()
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

fn max_rows_per_file(config: &ParquetOutputConfig) -> usize {
    config.max_rows_per_file.unwrap_or(50_000).max(1)
}

fn max_bytes_per_file(config: &ParquetOutputConfig) -> usize {
    config
        .max_bytes_per_file
        .unwrap_or(128 * 1024 * 1024)
        .max(1)
}

fn write_receipt(
    accepted_rows: usize,
    inserted_rows: usize,
    flushed_rows: usize,
    files_written: usize,
    highest_position: Option<EventPosition>,
) -> OutputWriteReceipt {
    OutputWriteReceipt {
        accepted_rows,
        flushed_rows,
        inserted_rows,
        skipped_or_replaced_rows: 0,
        files_written,
        batches_attempted: files_written,
        batches_delivered: files_written,
        highest_position: highest_position
            .clone()
            .map(|position| position.receipt_key),
        last_record: highest_position.map(|position| position.receipt_key),
    }
}

fn created_at_utc() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}Z", duration.as_secs(), duration.subsec_nanos())
}
