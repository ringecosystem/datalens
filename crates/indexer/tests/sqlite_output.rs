use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use datalens_indexer::{
    IndexedRecord, OutputWriteSink, QueryableStore, SqliteOutputStore, StoreQuery,
};
use sqlx::{Row, SqlitePool};
use tokio::runtime::Runtime;

fn record(block_number: u64, log_index: u64, address: &str) -> IndexedRecord {
    record_with_event(block_number, log_index, address, None)
}

fn record_with_event(
    block_number: u64,
    log_index: u64,
    address: &str,
    event_name: Option<&str>,
) -> IndexedRecord {
    let topic0 = if event_name == Some("Approval") {
        "0x1111111111111111111111111111111111111111111111111111111111111111"
    } else {
        "0x0000000000000000000000000000000000000000000000000000000000000000"
    };
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
                topic0
            ],
            "data": "0x010203",
            "event_name": event_name,
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

#[test]
fn test_sqlite_output_filters_topic0_event_name_and_orders_with_limit() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    let matching_address = "0x0000000000000000000000000000000000000001";
    let topic0 = "0x0000000000000000000000000000000000000000000000000000000000000000";
    store
        .write_records(&[
            record_with_event(20, 1, matching_address, Some("Transfer")),
            record_with_event(10, 0, matching_address, Some("Transfer")),
            record_with_event(30, 2, matching_address, Some("Approval")),
            record_with_event(
                15,
                3,
                "0x0000000000000000000000000000000000000002",
                Some("Transfer"),
            ),
        ])
        .expect("write records");

    let rows = store
        .query(StoreQuery {
            dataset: "evm.logs".to_owned(),
            filter: serde_json::json!({
                "index": "ormp",
                "chain": "ethereum",
                "chain_id": 1,
                "address": matching_address,
                "event_name": "Transfer",
                "topic0": topic0,
                "from_block": 0,
                "to_block": 100,
                "limit": 2,
            }),
        })
        .expect("filtered query");

    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0]["block_number"], 10);
    assert_eq!(rows.rows[1]["block_number"], 20);
    assert_eq!(rows.rows[0]["event_name"], "Transfer");
    assert_eq!(rows.rows[1]["event_name"], "Transfer");
}

#[test]
fn test_sqlite_output_applies_default_limit_to_unbounded_queries() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    let records = (0..105)
        .map(|offset| {
            record(
                1_000 + offset,
                offset,
                "0x0000000000000000000000000000000000000001",
            )
        })
        .collect::<Vec<_>>();
    store.write_records(&records).expect("write records");

    let rows = store
        .query(StoreQuery {
            dataset: "evm.logs".to_owned(),
            filter: serde_json::json!({
                "index": "ormp",
                "chain": "ethereum",
                "chain_id": 1,
            }),
        })
        .expect("default-limited query");

    assert_eq!(rows.rows.len(), 100);
    assert_eq!(rows.rows[0]["block_number"], 1_000);
    assert_eq!(rows.rows[99]["block_number"], 1_099);
}

#[test]
fn test_sqlite_output_schema_matches_benchmark_query_paths() {
    let path = temp_path("schema").join("index.db");
    let url = format!("sqlite:{}", path.display());
    let _store = SqliteOutputStore::connect(&url).expect("sqlite store connects");
    let runtime = Runtime::new().expect("runtime");

    runtime.block_on(async {
        let pool = SqlitePool::connect(&url).await.expect("sqlite pool");
        let columns = sqlx::query("PRAGMA table_info(indexed_events)")
            .fetch_all(&pool)
            .await
            .expect("table info")
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        assert!(columns.iter().any(|column| column == "topic0"));

        let indexes = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'indexed_events'",
        )
        .fetch_all(&pool)
        .await
        .expect("sqlite indexes")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
        assert!(
            indexes
                .iter()
                .any(|name| name == "idx_indexed_events_query_page")
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "idx_indexed_events_selector_page")
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "idx_indexed_events_topic0_page")
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "idx_indexed_events_event_name_page")
        );

        let plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT * FROM indexed_events INDEXED BY idx_indexed_events_selector_page
            WHERE dataset = 'evm.logs'
              AND index_name = 'ormp'
              AND chain_name = 'ethereum'
              AND chain_id = 1
              AND selector = '0x0000000000000000000000000000000000000001'
              AND block_number >= 10
              AND block_number <= 20
            ORDER BY block_number, transaction_index, event_index, id
            LIMIT 100
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("query plan")
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            plan.contains("idx_indexed_events_selector_page"),
            "expected selector-page index in query plan:\n{plan}"
        );
    });
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
