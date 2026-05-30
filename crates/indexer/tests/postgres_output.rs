use std::{
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use async_graphql::{Request as GraphqlRequest, Variables};

use datalens_indexer::{
    IndexedRecord, OutputWriteSink, PostgresOutputStore, QueryableStore, StoreQuery,
    graphql::graphql_schema,
};
use tokio::runtime::Runtime;

fn record(index: &str, block_number: u64, log_index: u64, address: &str) -> IndexedRecord {
    IndexedRecord {
        index: index.to_owned(),
        chain: "ethereum".to_owned(),
        chain_id: 1,
        dataset: "evm.logs".to_owned(),
        payload: serde_json::json!({
            "block_number": block_number,
            "unique_key": format!("{index}:{block_number}:{log_index}"),
            "block_hash": format!("0xblock{block_number:064x}"),
            "transaction_hash": format!("0xtx{block_number:064x}"),
            "transaction_index": 2,
            "log_index": log_index,
            "address": address,
            "topics": [
                "0x0000000000000000000000000000000000000000000000000000000000000000"
            ],
            "signature": "Transfer(address,address,uint256)",
            "data": "0x010203",
            "removed": false,
        }),
    }
}

#[test]
fn test_postgres_output_batch_insert_is_idempotent_when_url_is_configured() {
    let Some(url) = postgres_test_url() else {
        return;
    };
    let store = PostgresOutputStore::connect(&url).expect("postgres store connects");
    let base_block = unique_block_base();
    let index = unique_index_name("postgres-output");
    let records = vec![
        record(
            &index,
            base_block,
            0,
            "0x0000000000000000000000000000000000000001",
        ),
        record(
            &index,
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
                "index": index,
                "chain": "ethereum",
                "from_block": base_block,
                "to_block": base_block + 1,
                "topic0": "0x0000000000000000000000000000000000000000000000000000000000000000",
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

#[test]
fn test_postgres_graphql_events_query_filters_rows_when_url_is_configured() {
    let Some(url) = postgres_test_url() else {
        return;
    };
    let store = PostgresOutputStore::connect(&url).expect("postgres store connects");
    let matching_address = "0x0000000000000000000000000000000000000001";
    let base_block = unique_block_base();
    let index = unique_index_name("postgres-graphql");
    store
        .write_records(&[
            record(&index, base_block, 0, matching_address),
            record(&index, base_block + 10, 1, matching_address),
            record(
                &index,
                base_block + 20,
                2,
                "0x0000000000000000000000000000000000000002",
            ),
        ])
        .expect("write records");
    let schema = graphql_schema(Arc::new(store));

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                GraphqlRequest::new(
                    r#"
                query Events($indexName: String!, $address: String!, $fromBlock: Int!, $toBlock: Int!) {
                  events(
                    indexName: $indexName
                    chain: "ethereum"
                    chainId: 1
                    dataset: "evm.logs"
                    address: $address
                    fromBlock: $fromBlock
                    toBlock: $toBlock
                    topic0: "0x0000000000000000000000000000000000000000000000000000000000000000"
                    limit: 5
                  ) {
                    indexName
                    chain
                    chainId
                    dataset
                    blockNumber
                    eventIndex
                    address
                    topic0
                    signature
                    payload
                    createdAt
                  }
                }
                "#,
                )
                .variables(Variables::from_json(serde_json::json!({
                    "indexName": index,
                    "address": matching_address,
                    "fromBlock": base_block + 5,
                    "toBlock": base_block + 15,
                }))),
            )
            .await
    });

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["indexName"], index);
    assert_eq!(events[0]["chain"], "ethereum");
    assert_eq!(events[0]["chainId"], 1);
    assert_eq!(events[0]["dataset"], "evm.logs");
    assert_eq!(events[0]["blockNumber"], base_block + 10);
    assert_eq!(events[0]["eventIndex"], 1);
    assert_eq!(events[0]["address"], matching_address);
    assert_eq!(events[0]["signature"], "Transfer(address,address,uint256)");
    assert_eq!(events[0]["payload"]["block_number"], base_block + 10);
    assert!(events[0]["createdAt"].as_str().is_some());
}

fn postgres_test_url() -> Option<String> {
    match std::env::var("DATALENS_POSTGRES_TEST_URL") {
        Ok(url) => Some(url),
        Err(_) if std::env::var("DATALENS_REQUIRE_POSTGRES_TEST_URL").is_ok() => {
            panic!(
                "DATALENS_POSTGRES_TEST_URL must be set for PostgreSQL integration tests; \
                 start PostgreSQL with docker compose or export a test database URL"
            );
        }
        Err(_) => None,
    }
}

fn unique_index_name(prefix: &str) -> String {
    format!("{prefix}-{}", unique_block_base())
}

fn unique_block_base() -> u64 {
    static NEXT_OFFSET: AtomicU64 = AtomicU64::new(0);

    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis();
    let millis = u64::try_from(millis).unwrap_or(u64::MAX);
    (millis % 1_000_000) * 1_000 + NEXT_OFFSET.fetch_add(100, Ordering::SeqCst)
}
