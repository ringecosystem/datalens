use std::time::{SystemTime, UNIX_EPOCH};

use datalens_indexer::{
    IndexedRecord, OutputWriteSink, PostgresOutputStore, QueryableStore, StoreQuery,
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
fn test_postgres_output_batch_insert_is_idempotent_when_url_is_configured() {
    let Ok(url) = std::env::var("DATALENS_POSTGRES_TEST_URL") else {
        return;
    };
    let store = PostgresOutputStore::connect(&url).expect("postgres store connects");
    let base_block = unique_block_base();
    let records = vec![
        record(base_block, 0, "0x0000000000000000000000000000000000000001"),
        record(
            base_block + 1,
            1,
            "0x0000000000000000000000000000000000000002",
        ),
    ];

    let first = store.write_records(&records).expect("first write");
    let second = store.write_records(&records).expect("second write");
    let rows = store
        .query(StoreQuery {
            dataset: "evm.logs".to_owned(),
            filter: serde_json::json!({
                "chain": "ethereum",
                "from_block": base_block,
                "to_block": base_block + 1,
                "signature": "0x0000000000000000000000000000000000000000000000000000000000000000",
            }),
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
        Some(format!("ethereum:{}:2:1", base_block + 1))
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

fn unique_block_base() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}
