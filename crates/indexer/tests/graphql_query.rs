use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_graphql::{Request as GraphqlRequest, Variables};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use datalens_indexer::{
    IndexedRecord, OutputWriteSink, PostgresOutputStore, SqliteOutputStore,
    graphql::{
        IndexerGraphqlMetricLabels, IndexerGraphqlMetrics, MetricsEndpointConfig, graphql_router,
        graphql_router_with_metrics, graphql_schema,
    },
};
use datalens_metrics::MetricsRecorder;
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

fn decoded_record(block_number: u64, log_index: u64, signature: &str) -> IndexedRecord {
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
            "address": "0x2cd1867fb8016f93710b6386f7f9f1d540a60812",
            "topics": [
                "0x9e6c1c44f7b2b36245897f9be35a5500f3a9e0d5b8f29f89dbf04b54053bb7d1"
            ],
            "signature": signature,
            "event_name": "MessageAccepted",
            "decoded": {
                "msgHash": "0xabc",
                "fromChainId": "1"
            },
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
fn test_graphql_events_query_filters_signature_and_returns_decoded_shape() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    store
        .write_records(&[
            decoded_record(10, 0, "MessageAccepted(bytes32,uint256,address,address)"),
            decoded_record(20, 1, "MessageDispatched(bytes32,uint256,address,address)"),
        ])
        .expect("write records");
    let schema = graphql_schema(Arc::new(store));

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                GraphqlRequest::new(
                    r#"
                query DecodedEvents($signature: String!) {
                  events(
                    indexName: "ormp"
                    chain: "ethereum"
                    chainId: 1
                    dataset: "evm.logs"
                    address: "0x2cd1867fb8016f93710b6386f7f9f1d540a60812"
                    eventName: "MessageAccepted"
                    signature: $signature
                    fromBlock: 1
                    toBlock: 15
                    limit: 5
                  ) {
                    blockNumber
                    address
                    signature
                    eventName
                    decoded
                  }
                }
                "#,
                )
                .variables(Variables::from_json(serde_json::json!({
                    "signature": "MessageAccepted(bytes32,uint256,address,address)",
                }))),
            )
            .await
    });

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["blockNumber"], 10);
    assert_eq!(
        events[0]["signature"],
        "MessageAccepted(bytes32,uint256,address,address)"
    );
    assert_eq!(events[0]["eventName"], "MessageAccepted");
    assert_eq!(events[0]["decoded"]["msgHash"], "0xabc");
    assert_eq!(events[0]["decoded"]["fromChainId"], "1");
}

#[test]
fn test_graphql_events_query_filters_postgres_store_when_url_is_configured() {
    let Ok(url) = std::env::var("DATALENS_POSTGRES_TEST_URL") else {
        return;
    };
    let store = PostgresOutputStore::connect(&url).expect("postgres store connects");
    let base_block = unique_block_base();
    let matching_address = "0x0000000000000000000000000000000000000001";
    store
        .write_records(&[
            record(base_block, 0, matching_address),
            record(base_block + 10, 1, matching_address),
            record(
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
                query Events($address: String!, $fromBlock: BigInt!, $toBlock: BigInt!) {
                  events(
                    indexName: "ormp"
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
                    selector
                    topics
                    topic0
                    data
                    payload
                    createdAt
                  }
                }
                "#,
                )
                .variables(Variables::from_json(serde_json::json!({
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
    assert_eq!(events[0]["indexName"], "ormp");
    assert_eq!(events[0]["chain"], "ethereum");
    assert_eq!(events[0]["chainId"], 1);
    assert_eq!(events[0]["dataset"], "evm.logs");
    assert_eq!(events[0]["blockNumber"], base_block + 10);
    assert_eq!(events[0]["eventIndex"], 1);
    assert_eq!(events[0]["address"], matching_address);
    assert_eq!(events[0]["selector"], matching_address);
    assert_eq!(events[0]["topic0"], events[0]["topics"][0]);
    assert_eq!(events[0]["data"], "0x010203");
    assert_eq!(events[0]["payload"]["block_number"], base_block + 10);
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

#[test]
fn test_graphql_router_records_success_error_and_bounded_labels() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    store
        .write_records(&[record(10, 0, "0x0000000000000000000000000000000000000001")])
        .expect("write records");
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let app = graphql_router_with_metrics(
        Arc::new(store),
        "/graphql",
        false,
        IndexerGraphqlMetrics {
            recorder: recorder.clone(),
            labels: metric_labels(),
            endpoint: Some(MetricsEndpointConfig {
                path: "/metrics".to_owned(),
                bearer_token: None,
            }),
        },
    );

    Runtime::new().expect("runtime").block_on(async {
        let success = app
            .clone()
            .oneshot(graphql_http_request(
                r#"{ events(dataset: "evm.logs", address: "0x0000000000000000000000000000000000000001") { blockNumber } }"#,
            ))
            .await
            .expect("success response");
        let error = app
            .clone()
            .oneshot(graphql_http_request(r#"{ events(dataset: "evm.logs", limit: 1001) { blockNumber } }"#))
            .await
            .expect("error response");
        let metrics = app
            .clone()
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");

        assert_eq!(success.status(), StatusCode::OK);
        assert_eq!(error.status(), StatusCode::OK);
        assert_eq!(metrics.status(), StatusCode::OK);
        assert_eq!(
            metrics.headers().get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok()),
            Some("text/plain; version=0.0.4")
        );
        let text = response_text(metrics).await;
        assert!(text.contains(
            r#"datalens_indexer_graphql_query_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",outcome="success",output="sqlite"} 1"#
        ));
        assert!(text.contains(
            r#"datalens_indexer_graphql_query_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",outcome="error",output="sqlite"} 1"#
        ));
        assert!(text.contains("datalens_indexer_graphql_query_duration_seconds"));
        assert!(!text.contains("0x0000000000000000000000000000000000000001"));
    });
}

#[test]
fn test_graphql_metrics_route_requires_operator_token_and_records_auth_failure() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let app = graphql_router_with_metrics(
        Arc::new(SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects")),
        "/graphql",
        false,
        IndexerGraphqlMetrics {
            recorder: recorder.clone(),
            labels: metric_labels(),
            endpoint: Some(MetricsEndpointConfig {
                path: "/metrics".to_owned(),
                bearer_token: Some("metrics-token".to_owned()),
            }),
        },
    );

    Runtime::new().expect("runtime").block_on(async {
        let rejected = app
            .clone()
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("rejected response");
        let accepted = app
            .clone()
            .oneshot(
                Request::get("/metrics")
                    .header("authorization", "Bearer metrics-token")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("accepted response");

        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(accepted.status(), StatusCode::OK);
        let text = response_text(accepted).await;
        assert!(text.contains(
            r#"datalens_indexer_graphql_auth_failure_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",output="sqlite"} 1"#
        ));
    });
}

#[test]
fn test_graphql_metrics_include_rate_limited_events() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = metric_labels();
    recorder.record_indexer_graphql_rate_limited(&labels);

    let text = recorder.encode().expect("metrics text");

    assert!(text.contains(
        r#"datalens_indexer_graphql_rate_limited_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",output="sqlite"} 1"#
    ));
}

fn metric_labels() -> IndexerGraphqlMetricLabels {
    IndexerGraphqlMetricLabels {
        application: "ormp".to_owned(),
        index: "ormp".to_owned(),
        chain: "ethereum".to_owned(),
        dataset: "evm.logs".to_owned(),
        output: "sqlite".to_owned(),
    }
}

fn graphql_http_request(query: &str) -> Request<Body> {
    Request::post("/graphql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({ "query": query }).to_string(),
        ))
        .expect("graphql request")
}

async fn response_text(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body")
            .to_vec(),
    )
    .expect("utf8 body")
}

fn unique_block_base() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}
