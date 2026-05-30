use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use datalens_indexer::{IndexedRecord, OutputSinkConfig, OutputWriteSink, ParquetOutputConfig};
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
    let sink = OutputSinkConfig::Parquet { config };
    let records = vec![
        record(100, 0, "0x0000000000000000000000000000000000000001"),
        record(101, 1, "0x0000000000000000000000000000000000000002"),
    ];

    let result = sink.write_records(&records).expect("write parquet");

    assert_eq!(result.written_rows, 2);
    let receipt = result.receipt.expect("receipt");
    assert_eq!(receipt.accepted_rows, 2);
    assert_eq!(receipt.inserted_rows, 2);
    assert_eq!(receipt.skipped_or_replaced_rows, 0);
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
    };

    sink.write_records(&[
        record(100, 0, "0x0000000000000000000000000000000000000001"),
        record(101, 1, "0x0000000000000000000000000000000000000002"),
        record(102, 2, "0x0000000000000000000000000000000000000003"),
    ])
    .expect("write parquet");

    let files = parquet_files(&root);
    assert_eq!(files.len(), 2);
    assert_eq!(read_single_batch(&files[0]).num_rows(), 2);
    assert_eq!(read_single_batch(&files[1]).num_rows(), 1);
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
