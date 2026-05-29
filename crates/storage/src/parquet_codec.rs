use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use datalens_core::{
    BlockHeader, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, EvmReceipt,
    EvmTransaction, LogRecord, QueryRows,
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

pub fn encode_rows(rows: &DatasetRows) -> Result<Vec<u8>, DatalensError> {
    let batch = match rows.rows() {
        QueryRows::EvmBlocks(rows) => evm_blocks_batch(rows)?,
        QueryRows::EvmTransactions(rows) => evm_transactions_batch(rows)?,
        QueryRows::EvmReceipts(rows) => evm_receipts_batch(rows)?,
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
        let batch_rows = match dataset_key.evm_dataset() {
            Some(datalens_core::Dataset::Blocks) => decode_evm_blocks(&batch)?,
            Some(datalens_core::Dataset::Transactions) => decode_evm_transactions(&batch)?,
            Some(datalens_core::Dataset::Receipts) => decode_evm_receipts(&batch)?,
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

fn evm_transactions_batch(rows: &[EvmTransaction]) -> Result<RecordBatch, DatalensError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("hash", DataType::Utf8, false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, false),
        Field::new("input", DataType::Utf8, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("gas", DataType::UInt64, false),
        Field::new("gas_price", DataType::Utf8, true),
        Field::new("max_fee_per_gas", DataType::Utf8, true),
        Field::new("max_priority_fee_per_gas", DataType::Utf8, true),
        Field::new("transaction_type", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            string_values(rows.iter().map(|row| row.hash.as_str())) as ArrayRef,
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.block_number),
            )),
            string_values(rows.iter().map(|row| row.block_hash.as_str())),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.transaction_index),
            )),
            string_values(rows.iter().map(|row| row.from.as_str())),
            optional_string_values(rows.iter().map(|row| row.to.as_deref())),
            string_values(rows.iter().map(|row| row.value.as_str())),
            string_values(rows.iter().map(|row| row.input.as_str())),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.nonce),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.gas),
            )),
            optional_string_values(rows.iter().map(|row| row.gas_price.as_deref())),
            optional_string_values(rows.iter().map(|row| row.max_fee_per_gas.as_deref())),
            optional_string_values(
                rows.iter()
                    .map(|row| row.max_priority_fee_per_gas.as_deref()),
            ),
            optional_string_values(rows.iter().map(|row| row.transaction_type.as_deref())),
        ],
    )
    .map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("build evm.transactions parquet batch: {error}"),
        )
    })
}

fn evm_receipts_batch(rows: &[EvmReceipt]) -> Result<RecordBatch, DatalensError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("status", DataType::UInt64, true),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("cumulative_gas_used", DataType::UInt64, false),
        Field::new("effective_gas_price", DataType::Utf8, true),
        Field::new("contract_address", DataType::Utf8, true),
        Field::new("logs_bloom", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            string_values(rows.iter().map(|row| row.transaction_hash.as_str())) as ArrayRef,
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.block_number),
            )),
            string_values(rows.iter().map(|row| row.block_hash.as_str())),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.transaction_index),
            )),
            Arc::new(UInt64Array::from_iter(rows.iter().map(|row| row.status))),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.gas_used),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.cumulative_gas_used),
            )),
            optional_string_values(rows.iter().map(|row| row.effective_gas_price.as_deref())),
            optional_string_values(rows.iter().map(|row| row.contract_address.as_deref())),
            optional_string_values(rows.iter().map(|row| row.logs_bloom.as_deref())),
        ],
    )
    .map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("build evm.receipts parquet batch: {error}"),
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

fn decode_evm_transactions(batch: &RecordBatch) -> Result<QueryRows, DatalensError> {
    let hash = string_column(batch, "hash")?;
    let block_number = uint64_column(batch, "block_number")?;
    let block_hash = string_column(batch, "block_hash")?;
    let transaction_index = uint64_column(batch, "transaction_index")?;
    let from = string_column(batch, "from")?;
    let to = string_column(batch, "to")?;
    let value = string_column(batch, "value")?;
    let input = string_column(batch, "input")?;
    let nonce = uint64_column(batch, "nonce")?;
    let gas = uint64_column(batch, "gas")?;
    let gas_price = string_column(batch, "gas_price")?;
    let max_fee_per_gas = string_column(batch, "max_fee_per_gas")?;
    let max_priority_fee_per_gas = string_column(batch, "max_priority_fee_per_gas")?;
    let transaction_type = string_column(batch, "transaction_type")?;

    let rows = (0..batch.num_rows())
        .map(|index| EvmTransaction {
            hash: hash.value(index).to_owned(),
            block_number: block_number.value(index),
            block_hash: block_hash.value(index).to_owned(),
            transaction_index: transaction_index.value(index),
            from: from.value(index).to_owned(),
            to: optional_string(to, index),
            value: value.value(index).to_owned(),
            input: input.value(index).to_owned(),
            nonce: nonce.value(index),
            gas: gas.value(index),
            gas_price: optional_string(gas_price, index),
            max_fee_per_gas: optional_string(max_fee_per_gas, index),
            max_priority_fee_per_gas: optional_string(max_priority_fee_per_gas, index),
            transaction_type: optional_string(transaction_type, index),
        })
        .collect();
    Ok(QueryRows::EvmTransactions(rows))
}

fn decode_evm_receipts(batch: &RecordBatch) -> Result<QueryRows, DatalensError> {
    let transaction_hash = string_column(batch, "transaction_hash")?;
    let block_number = uint64_column(batch, "block_number")?;
    let block_hash = string_column(batch, "block_hash")?;
    let transaction_index = uint64_column(batch, "transaction_index")?;
    let status = uint64_column(batch, "status")?;
    let gas_used = uint64_column(batch, "gas_used")?;
    let cumulative_gas_used = uint64_column(batch, "cumulative_gas_used")?;
    let effective_gas_price = string_column(batch, "effective_gas_price")?;
    let contract_address = string_column(batch, "contract_address")?;
    let logs_bloom = string_column(batch, "logs_bloom")?;

    let rows = (0..batch.num_rows())
        .map(|index| EvmReceipt {
            transaction_hash: transaction_hash.value(index).to_owned(),
            block_number: block_number.value(index),
            block_hash: block_hash.value(index).to_owned(),
            transaction_index: transaction_index.value(index),
            status: optional_u64(status, index),
            gas_used: gas_used.value(index),
            cumulative_gas_used: cumulative_gas_used.value(index),
            effective_gas_price: optional_string(effective_gas_price, index),
            contract_address: optional_string(contract_address, index),
            logs_bloom: optional_string(logs_bloom, index),
        })
        .collect();
    Ok(QueryRows::EvmReceipts(rows))
}

fn empty_query_rows(dataset_key: &DatasetKey) -> QueryRows {
    match dataset_key.evm_dataset() {
        Some(datalens_core::Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(datalens_core::Dataset::Transactions) => QueryRows::EvmTransactions(Vec::new()),
        Some(datalens_core::Dataset::Receipts) => QueryRows::EvmReceipts(Vec::new()),
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

fn string_values<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn optional_string_values<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn optional_string(column: &StringArray, index: usize) -> Option<String> {
    if column.is_null(index) {
        None
    } else {
        Some(column.value(index).to_owned())
    }
}

fn optional_u64(column: &UInt64Array, index: usize) -> Option<u64> {
    if column.is_null(index) {
        None
    } else {
        Some(column.value(index))
    }
}

fn parquet_read_error(message: impl Into<String>) -> DatalensError {
    DatalensError::new(DatalensErrorKind::StorageReadFailure, message)
}
