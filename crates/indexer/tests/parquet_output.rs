use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use datalens_indexer::{IndexedRecord, OutputSinkConfig, ParquetOutputConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn record(block_number: u64, log_index: u64, address: &str) -> IndexedRecord {
    IndexedRecord {
        index: "ormp".to_owned(),
        chain: "ethereum".to_owned(),
        chain_id: 1,
        dataset: "evm.logs".to_owned(),
        payload: serde_json::json!({
            "block_number": block_number,
            "block_hash": format!("0xblock{block_number:064x}"),
            "transaction_hash": format!("0xtx{block_number:064x}"),
            "transaction_index": 2,
            "log_index": log_index,
            "address": address,
            "topics": [
                "0x0000000000000000000000000000000000000000000000000000000000000000"
            ],
            "data": "0x010203",
            "removed": false,
            "finality": "durable",
        }),
    }
}

#[test]
fn test_parquet_output_writes_partitioned_file_with_normalized_schema() {
    let root = temp_path("roundtrip");
    let config = ParquetOutputConfig {
        path: root.clone(),
        max_rows_per_file: Some(100),
        max_bytes_per_file: None,
        partition_by: vec![
            "index".to_owned(),
            "chain_family".to_owned(),
            "chain_id".to_owned(),
            "dataset".to_owned(),
        ],
        compression: Some("zstd".to_owned()),
    };
    let sink = OutputSinkConfig::Parquet { config }
        .open_write_sink()
        .expect("open sink");
    let records = vec![
        record(100, 0, "0x0000000000000000000000000000000000000001"),
        record(101, 1, "0x0000000000000000000000000000000000000002"),
    ];

    let result = sink.write_records(&records).expect("write parquet");
    sink.flush().expect("flush parquet");

    assert_eq!(result.written_rows, 2);
    let receipt = result.receipt.expect("receipt");
    assert_eq!(receipt.accepted_rows, 2);
    assert_eq!(receipt.flushed_rows, 0);
    assert_eq!(receipt.inserted_rows, 2);
    assert_eq!(receipt.skipped_or_replaced_rows, 0);
    assert_eq!(receipt.files_written, 0);
    assert_eq!(receipt.batches_attempted, 0);
    assert_eq!(receipt.batches_delivered, 0);
    assert_eq!(
        receipt.highest_position,
        Some("ethereum:101:2:1".to_owned())
    );

    let files = parquet_files(&root);
    assert_eq!(files.len(), 1);
    let file = files.first().expect("parquet file");
    assert!(file.to_string_lossy().contains("index=ormp"));
    assert!(file.to_string_lossy().contains("chain_family=evm"));
    assert!(file.to_string_lossy().contains("chain_id=1"));
    assert!(file.to_string_lossy().contains("dataset=evm.logs"));

    let batch = read_single_batch(file);
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .schema()
            .field_with_name("block_number")
            .unwrap()
            .data_type(),
        &arrow::datatypes::DataType::Int64
    );
    assert_eq!(
        batch
            .schema()
            .field_with_name("removed")
            .unwrap()
            .data_type(),
        &arrow::datatypes::DataType::Boolean
    );
    assert_eq!(string_value(&batch, "index_name", 0), "ormp");
    assert_eq!(string_value(&batch, "chain_family", 0), "evm");
    assert_eq!(string_value(&batch, "chain_name", 0), "ethereum");
    assert_eq!(int_value(&batch, "chain_id", 0), 1);
    assert_eq!(int_value(&batch, "block_number", 1), 101);
    assert_eq!(
        string_value(&batch, "selector", 1),
        "0x0000000000000000000000000000000000000002"
    );
    assert!(!bool_value(&batch, "removed", 0));
    assert!(string_value(&batch, "created_at", 0).ends_with('Z'));
}

#[test]
fn test_parquet_output_flushes_by_row_threshold_and_batches_small_writes() {
    let root = temp_path("threshold");
    let sink = OutputSinkConfig::Parquet {
        config: ParquetOutputConfig {
            path: root.clone(),
            max_rows_per_file: Some(2),
            max_bytes_per_file: None,
            partition_by: vec![],
            compression: None,
        },
    }
    .open_write_sink()
    .expect("open sink");

    let result = sink
        .write_records(&[
            record(100, 0, "0x0000000000000000000000000000000000000001"),
            record(101, 1, "0x0000000000000000000000000000000000000002"),
            record(102, 2, "0x0000000000000000000000000000000000000003"),
        ])
        .expect("write parquet");

    let receipt = result.receipt.expect("receipt");
    assert_eq!(receipt.accepted_rows, 3);
    assert_eq!(receipt.flushed_rows, 2);
    assert_eq!(receipt.files_written, 1);
    assert_eq!(parquet_files(&root).len(), 1);

    let flush = sink.flush().expect("flush remaining parquet");
    let flush_receipt = flush.receipt.expect("flush receipt");
    assert_eq!(flush_receipt.accepted_rows, 0);
    assert_eq!(flush_receipt.flushed_rows, 1);
    assert_eq!(flush_receipt.files_written, 1);

    let files = parquet_files(&root);
    assert_eq!(files.len(), 2);
    assert_eq!(read_single_batch(&files[0]).num_rows(), 2);
    assert_eq!(read_single_batch(&files[1]).num_rows(), 1);
}

#[test]
fn test_parquet_output_buffers_across_small_write_calls_until_explicit_flush() {
    let root = temp_path("multi-call-buffer");
    let sink = OutputSinkConfig::Parquet {
        config: ParquetOutputConfig {
            path: root.clone(),
            max_rows_per_file: Some(10),
            max_bytes_per_file: None,
            partition_by: vec![],
            compression: None,
        },
    }
    .open_write_sink()
    .expect("open sink");

    let first = sink
        .write_records(&[record(100, 0, "0x0000000000000000000000000000000000000001")])
        .expect("first write");
    let second = sink
        .write_records(&[record(101, 1, "0x0000000000000000000000000000000000000002")])
        .expect("second write");

    assert_eq!(first.receipt.expect("first receipt").flushed_rows, 0);
    assert_eq!(second.receipt.expect("second receipt").flushed_rows, 0);
    assert!(parquet_files(&root).is_empty());

    let flush = sink.flush().expect("final flush");
    let receipt = flush.receipt.expect("flush receipt");
    assert_eq!(receipt.accepted_rows, 0);
    assert_eq!(receipt.flushed_rows, 2);
    assert_eq!(receipt.files_written, 1);
    assert_eq!(
        receipt.highest_position,
        Some("ethereum:101:2:1".to_owned())
    );

    let files = parquet_files(&root);
    assert_eq!(files.len(), 1);
    assert_eq!(read_single_batch(&files[0]).num_rows(), 2);
}

#[test]
fn test_parquet_output_keeps_partition_buffers_separate() {
    let root = temp_path("partition-buffer");
    let sink = OutputSinkConfig::Parquet {
        config: ParquetOutputConfig {
            path: root.clone(),
            max_rows_per_file: Some(10),
            max_bytes_per_file: None,
            partition_by: vec!["chain".to_owned()],
            compression: None,
        },
    }
    .open_write_sink()
    .expect("open sink");
    let mut ethereum = record(100, 0, "0x0000000000000000000000000000000000000001");
    ethereum.chain = "ethereum".to_owned();
    let mut polygon = record(101, 0, "0x0000000000000000000000000000000000000002");
    polygon.chain = "polygon".to_owned();

    sink.write_records(&[ethereum]).expect("write ethereum");
    sink.write_records(&[polygon]).expect("write polygon");
    sink.flush().expect("flush partitions");

    let files = parquet_files(&root);
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .any(|file| file.to_string_lossy().contains("chain=ethereum"))
    );
    assert!(
        files
            .iter()
            .any(|file| file.to_string_lossy().contains("chain=polygon"))
    );
}

#[test]
fn test_parquet_output_flushes_by_byte_threshold_across_write_calls() {
    let root = temp_path("byte-threshold");
    let sink = OutputSinkConfig::Parquet {
        config: ParquetOutputConfig {
            path: root.clone(),
            max_rows_per_file: Some(10),
            max_bytes_per_file: Some(1),
            partition_by: vec![],
            compression: None,
        },
    }
    .open_write_sink()
    .expect("open sink");

    sink.write_records(&[
        record(100, 0, "0x0000000000000000000000000000000000000001"),
        record(101, 1, "0x0000000000000000000000000000000000000002"),
    ])
    .expect("write parquet");

    let files = parquet_files(&root);
    assert_eq!(files.len(), 1);
    assert_eq!(read_single_batch(&files[0]).num_rows(), 2);
}

fn read_single_batch(path: &Path) -> arrow::record_batch::RecordBatch {
    let file = std::fs::File::open(path).expect("open parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
    let reader = builder.build().expect("record batch reader");
    let batches = reader.collect::<Result<Vec<_>, _>>().expect("read batches");
    assert_eq!(batches.len(), 1);
    batches.into_iter().next().expect("batch")
}

fn parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_parquet_files(root, &mut files);
    files.sort();
    files
}

fn collect_parquet_files(path: &Path, files: &mut Vec<PathBuf>) {
    if !path.exists() {
        return;
    }
    for entry in fs::read_dir(path).expect("read output dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_parquet_files(&path, files);
        } else if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
            files.push(path);
        }
    }
}

fn string_value(batch: &arrow::record_batch::RecordBatch, name: &str, index: usize) -> String {
    let column = batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .expect("string column");
    column.value(index).to_owned()
}

fn int_value(batch: &arrow::record_batch::RecordBatch, name: &str, index: usize) -> i64 {
    let column = batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .expect("int column");
    column.value(index)
}

fn bool_value(batch: &arrow::record_batch::RecordBatch, name: &str, index: usize) -> bool {
    let column = batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
        .expect("bool column");
    column.value(index)
}

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    path.push(format!("datalens-indexer-parquet-{name}-{unique}"));
    let _ = fs::remove_dir_all(&path);
    path
}
