use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use datalens_core::{
    BlockHeader, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LogRecord, QueryRows,
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

pub fn encode_rows(rows: &DatasetRows) -> Result<Vec<u8>, DatalensError> {
    let batch = match rows.rows() {
        QueryRows::EvmBlocks(rows) => evm_blocks_batch(rows)?,
        QueryRows::EvmLogs(rows) => evm_logs_batch(rows)?,
        QueryRows::AdapterJson { .. } => {
            return Err(DatalensError::new(
                DatalensErrorKind::Internal,
                "parquet-v1 encoding supports only evm.blocks and evm.logs",
            ));
        }
    };
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("create parquet writer: {error}"),
        )
    })?;
    writer.write(&batch).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("write parquet batch: {error}"),
        )
    })?;
    writer.close().map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("close parquet writer: {error}"),
        )
    })?;
    Ok(bytes)
}

pub fn decode_rows(dataset_key: DatasetKey, bytes: &[u8]) -> Result<DatasetRows, DatalensError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .map_err(|error| parquet_read_error(format!("create parquet reader: {error}")))?
        .build()
        .map_err(|error| parquet_read_error(format!("build parquet reader: {error}")))?;

    let mut rows = empty_query_rows(&dataset_key);
    for batch in reader {
        let batch =
            batch.map_err(|error| parquet_read_error(format!("read parquet batch: {error}")))?;
        let batch_rows = match dataset_key.legacy_dataset() {
            Some(datalens_core::Dataset::Blocks) => decode_evm_blocks(&batch)?,
            Some(datalens_core::Dataset::Logs) => decode_evm_logs(&batch)?,
            None => {
                return Err(parquet_read_error(
                    "parquet-v1 decoding supports only evm.blocks and evm.logs",
                ));
            }
        };
        rows.try_append(batch_rows)?;
    }
    DatasetRows::new(dataset_key, rows)
}

fn evm_blocks_batch(rows: &[BlockHeader]) -> Result<RecordBatch, DatalensError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("parent_hash", DataType::Utf8, false),
        Field::new("timestamp", DataType::UInt64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.number),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.hash.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.parent_hash.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.timestamp),
            )),
        ],
    )
    .map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("build evm.blocks parquet batch: {error}"),
        )
    })
}

fn evm_logs_batch(rows: &[LogRecord]) -> Result<RecordBatch, DatalensError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("topics", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("removed", DataType::Boolean, false),
    ]));
    let topics = rows
        .iter()
        .map(|row| serde_json::to_string(&row.topics))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode log topics: {error}"),
            )
        })?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.block_number),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.block_hash.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.transaction_hash.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.transaction_index),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.log_index),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.address.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                topics.iter().map(String::as_str),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.data.as_str()),
            )),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|row| row.removed))),
        ],
    )
    .map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("build evm.logs parquet batch: {error}"),
        )
    })
}

fn decode_evm_blocks(batch: &RecordBatch) -> Result<QueryRows, DatalensError> {
    let number = uint64_column(batch, "number")?;
    let hash = string_column(batch, "hash")?;
    let parent_hash = string_column(batch, "parent_hash")?;
    let timestamp = uint64_column(batch, "timestamp")?;

    let rows = (0..batch.num_rows())
        .map(|index| BlockHeader {
            number: number.value(index),
            hash: hash.value(index).to_owned(),
            parent_hash: parent_hash.value(index).to_owned(),
            timestamp: timestamp.value(index),
        })
        .collect();
    Ok(QueryRows::EvmBlocks(rows))
}

fn decode_evm_logs(batch: &RecordBatch) -> Result<QueryRows, DatalensError> {
    let block_number = uint64_column(batch, "block_number")?;
    let block_hash = string_column(batch, "block_hash")?;
    let transaction_hash = string_column(batch, "transaction_hash")?;
    let transaction_index = uint64_column(batch, "transaction_index")?;
    let log_index = uint64_column(batch, "log_index")?;
    let address = string_column(batch, "address")?;
    let topics = string_column(batch, "topics")?;
    let data = string_column(batch, "data")?;
    let removed = bool_column(batch, "removed")?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let topics = serde_json::from_str::<Vec<String>>(topics.value(index))
            .map_err(|error| parquet_read_error(format!("decode log topics: {error}")))?;
        rows.push(LogRecord::try_new(
            block_number.value(index),
            block_hash.value(index).to_owned(),
            transaction_hash.value(index).to_owned(),
            transaction_index.value(index),
            log_index.value(index),
            address.value(index),
            topics,
            data.value(index).to_owned(),
            removed.value(index),
        )?);
    }
    Ok(QueryRows::EvmLogs(rows))
}

fn empty_query_rows(dataset_key: &DatasetKey) -> QueryRows {
    match dataset_key.legacy_dataset() {
        Some(datalens_core::Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(datalens_core::Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: Vec::new(),
        },
    }
}

fn uint64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array, DatalensError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| parquet_read_error(format!("missing UInt64 column {name}")))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, DatalensError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| parquet_read_error(format!("missing Utf8 column {name}")))
}

fn bool_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray, DatalensError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| parquet_read_error(format!("missing Boolean column {name}")))
}

fn parquet_read_error(message: impl Into<String>) -> DatalensError {
    DatalensError::new(DatalensErrorKind::StorageReadFailure, message)
}
