use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use datalens_indexer::{
    IndexedRecord, OutputWriteSink, QueryableStore, SqliteOutputStore, StoreQuery,
};

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
        }),
    }
}

#[test]
fn test_sqlite_output_initializes_parent_directory_and_schema() {
    let path = temp_path("init").join("nested").join("index.db");
    let store = SqliteOutputStore::connect(&format!("sqlite:{}", path.display()))
        .expect("sqlite store connects");

    assert!(path.exists());
    let rows = store
        .query(StoreQuery {
            dataset: "evm.logs".to_owned(),
            filter: serde_json::json!({ "chain": "ethereum" }),
        })
        .expect("query empty schema");

    assert!(rows.rows.is_empty());
}

#[test]
fn test_sqlite_output_batch_insert_is_idempotent() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    let records = vec![
        record(100, 0, "0x0000000000000000000000000000000000000001"),
        record(101, 1, "0x0000000000000000000000000000000000000002"),
    ];

    let first = store.write_records(&records).expect("first write");
    let second = store.write_records(&records).expect("second write");
    let rows = store
        .query(StoreQuery {
            dataset: "evm.logs".to_owned(),
            filter: serde_json::json!({ "chain": "ethereum", "from_block": 0, "to_block": 200 }),
        })
        .expect("range query");

    assert_eq!(first.written_rows, 2);
    assert_eq!(first.receipt.as_ref().expect("receipt").accepted_rows, 2);
    assert_eq!(first.receipt.as_ref().expect("receipt").inserted_rows, 2);
    assert_eq!(
        first
            .receipt
            .as_ref()
            .expect("receipt")
            .skipped_or_replaced_rows,
        0
    );
    assert_eq!(
        first.receipt.as_ref().expect("receipt").highest_position,
        Some("ethereum:101:2:1".to_owned())
    );
    assert_eq!(second.written_rows, 2);
    assert_eq!(second.receipt.as_ref().expect("receipt").inserted_rows, 0);
    assert_eq!(
        second
            .receipt
            .as_ref()
            .expect("receipt")
            .skipped_or_replaced_rows,
        2
    );
    assert_eq!(rows.rows.len(), 2);
}

#[test]
fn test_sqlite_output_queries_by_chain_dataset_address_and_block_range() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    let matching_address = "0x0000000000000000000000000000000000000001";
    store
        .write_records(&[
            record(10, 0, matching_address),
            record(20, 0, matching_address),
            record(30, 0, "0x0000000000000000000000000000000000000002"),
        ])
        .expect("write records");

    let rows = store
        .query(StoreQuery {
            dataset: "evm.logs".to_owned(),
            filter: serde_json::json!({
                "chain": "ethereum",
                "chain_id": 1,
                "address": matching_address,
                "from_block": 15,
                "to_block": 25,
            }),
        })
        .expect("filtered query");

    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["block_number"], 20);
    assert_eq!(rows.rows[0]["address"], matching_address);
}

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    path.push(format!("datalens-indexer-sqlite-{name}-{unique}"));
    let _ = fs::remove_dir_all(&path);
    path
}
