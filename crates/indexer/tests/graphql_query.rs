use std::sync::Arc;

use async_graphql::{Request as GraphqlRequest, Variables};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use datalens_indexer::{
    IndexedRecord, OutputWriteSink, SqliteOutputStore,
    graphql::{graphql_router, graphql_schema},
};
use tokio::runtime::Runtime;
use tower::ServiceExt;

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
            "signature": "Transfer(address,address,uint256)",
            "data": "0x010203",
            "removed": false,
        }),
    }
}

#[test]
fn test_graphql_events_query_filters_sqlite_store_and_returns_stable_fields() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    let matching_address = "0x0000000000000000000000000000000000000001";
    store
        .write_records(&[
            record(10, 0, matching_address),
            record(20, 1, matching_address),
            record(30, 2, "0x0000000000000000000000000000000000000002"),
        ])
        .expect("write records");
    let schema = graphql_schema(Arc::new(store));

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                GraphqlRequest::new(
                    r#"
                query Events($address: String!) {
                  events(
                    indexName: "ormp"
                    chain: "ethereum"
                    chainId: 1
                    dataset: "evm.logs"
                    address: $address
                    fromBlock: 15
                    toBlock: 25
                    topic0: "0x0000000000000000000000000000000000000000000000000000000000000000"
                    limit: 5
                  ) {
                    indexName
                    chain
                    chainId
                    dataset
                    blockNumber
                    blockHash
                    transactionHash
                    transactionIndex
                    eventIndex
                    address
                    selector
                    topics
                    topic0
                    signature
                    data
                    payload
                    createdAt
                  }
                }
                "#,
                )
                .variables(Variables::from_json(
                    serde_json::json!({ "address": matching_address }),
                )),
            )
            .await
    });

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["indexName"], "ormp");
    assert_eq!(events[0]["chain"], "ethereum");
    assert_eq!(events[0]["chainId"], 1);
    assert_eq!(events[0]["dataset"], "evm.logs");
    assert_eq!(events[0]["blockNumber"], 20);
    assert_eq!(events[0]["eventIndex"], 1);
    assert_eq!(events[0]["address"], matching_address);
    assert_eq!(events[0]["selector"], matching_address);
    assert_eq!(events[0]["topic0"], events[0]["topics"][0]);
    assert_eq!(events[0]["data"], "0x010203");
    assert_eq!(events[0]["payload"]["block_number"], 20);
    assert!(events[0]["createdAt"].as_str().is_some());
}

#[test]
fn test_graphql_events_query_rejects_limit_above_maximum() {
    let schema = graphql_schema(Arc::new(
        SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects"),
    ));

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                r#"
            {
              events(dataset: "evm.logs", limit: 1001) {
                blockNumber
              }
            }
            "#,
            )
            .await
    });

    assert_eq!(response.errors.len(), 1);
    assert!(
        response.errors[0]
            .message
            .contains("limit must be less than or equal to 1000"),
        "{:?}",
        response.errors
    );
}

#[test]
fn test_graphql_router_serves_playground_only_when_enabled() {
    let store =
        Arc::new(SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects"));
    let _store_keepalive = store.clone();
    let enabled = graphql_router(store.clone(), "/graphql", true);
    let disabled = graphql_router(store, "/graphql", false);

    Runtime::new().expect("runtime").block_on(async {
        let response = enabled
            .oneshot(
                Request::get("/graphql/playground")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("playground response");
        assert_eq!(response.status(), StatusCode::OK);
        let text = String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body")
                .to_vec(),
        )
        .expect("utf8 body");
        assert!(text.contains("/graphql"));

        let response = disabled
            .oneshot(
                Request::get("/graphql/playground")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("disabled response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    });
}
