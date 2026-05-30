use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, Object as DynamicObject, Schema as DynamicSchema, TypeRef,
};
use async_graphql::{Request as GraphqlRequest, Variables};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use datalens_indexer::{
    ApplicationEntityQueryStore, ApplicationEntityReadQuery, ApplicationGraphqlSchemaContext,
    ApplicationGraphqlSchemaHook, GraphqlViewConfig, GraphqlViewFieldConfig,
    GraphqlViewFilterConfig, IndexedRecord, IndexerError, OutputWriteSink, PostgresOutputStore,
    QueryAuthApplicationConfig, QueryAuthConfig, QueryAuthQuotaConfig, QueryableStore,
    SqliteApplicationEntityStore, SqliteOutputStore, StoreQuery, StoreQueryResult,
    graphql::{
        IndexerGraphqlMetricLabels, IndexerGraphqlMetrics, MetricsEndpointConfig,
        graphql_application_router_with_auth, graphql_router, graphql_router_with_auth,
        graphql_router_with_metrics, graphql_schema, graphql_schema_with_views,
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
    decoded_record_with_status(block_number, log_index, signature, "decoded", None)
}

fn decoded_record_with_status(
    block_number: u64,
    log_index: u64,
    signature: &str,
    decode_status: &str,
    decode_error: Option<&str>,
) -> IndexedRecord {
    let mut payload = serde_json::json!({
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
        "decode_status": decode_status,
        "decode_error": decode_error,
        "data": "0x010203",
        "removed": false,
    });
    if decode_status != "failed"
        && let Some(object) = payload.as_object_mut()
    {
        object.insert(
            "decoded".to_owned(),
            serde_json::json!({
                "msgHash": "0xabc",
                "fromChainId": "1"
            }),
        );
    }
    IndexedRecord {
        index: "ormp".to_owned(),
        chain: "ethereum".to_owned(),
        chain_id: 1,
        dataset: "evm.logs".to_owned(),
        payload,
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
fn test_graphql_application_view_queries_decoded_event_fields() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    store
        .write_records(&[
            decoded_record(10, 0, "MessageAccepted(bytes32,uint256,address,address)"),
            decoded_record(20, 1, "MessageDispatched(bytes32,uint256,address,address)"),
        ])
        .expect("write records");
    let schema = graphql_schema_with_views(
        Arc::new(store),
        vec![GraphqlViewConfig {
            name: "messageAccepted".to_owned(),
            dataset: "evm.logs".to_owned(),
            event_name: Some("MessageAccepted".to_owned()),
            signature: Some("MessageAccepted(bytes32,uint256,address,address)".to_owned()),
            fields: vec![
                GraphqlViewFieldConfig {
                    name: "msgHash".to_owned(),
                    path: "decoded.msgHash".to_owned(),
                },
                GraphqlViewFieldConfig {
                    name: "sourceChain".to_owned(),
                    path: "decoded.fromChainId".to_owned(),
                },
                GraphqlViewFieldConfig {
                    name: "blockNumber".to_owned(),
                    path: "block_number".to_owned(),
                },
            ],
            filters: vec![GraphqlViewFilterConfig {
                field: "address".to_owned(),
                equals: "0x2cd1867fb8016f93710b6386f7f9f1d540a60812".to_owned(),
            }],
            default_limit: 10,
            max_limit: 25,
        }],
    )
    .expect("graphql schema");

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                r#"
            {
              messageAccepted(limit: 5) {
                msgHash
                sourceChain
                blockNumber
                payload
              }
              events(
                dataset: "evm.logs"
                signature: "MessageAccepted(bytes32,uint256,address,address)"
              ) {
                eventName
              }
            }
            "#,
            )
            .await
    });

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    let view_rows = body["messageAccepted"]
        .as_array()
        .expect("application view rows");
    assert_eq!(view_rows.len(), 1);
    assert_eq!(view_rows[0]["msgHash"], "0xabc");
    assert_eq!(view_rows[0]["sourceChain"], "1");
    assert_eq!(view_rows[0]["blockNumber"], 10);
    assert_eq!(view_rows[0]["payload"]["event_name"], "MessageAccepted");
    assert_eq!(body["events"].as_array().expect("generic events").len(), 1);
}

#[test]
fn test_application_graphql_hook_queries_application_entity_store() {
    let entity_store = Arc::new(
        SqliteApplicationEntityStore::connect(&sqlite_entity_test_url("graphql-hook"))
            .expect("sqlite entity store connects"),
    );
    Runtime::new().expect("runtime").block_on(async {
        let mut transaction = entity_store.begin().await.expect("begin transaction");
        sqlx::query(
            "CREATE TABLE payment_transfers (id TEXT PRIMARY KEY, account TEXT NOT NULL, amount INTEGER NOT NULL)",
        )
        .execute(transaction.sqlite())
        .await
        .expect("create application table");
        sqlx::query("INSERT INTO payment_transfers (id, account, amount) VALUES (?, ?, ?)")
            .bind("transfer-1")
            .bind("alice")
            .bind(42_i64)
            .execute(transaction.sqlite())
            .await
            .expect("insert first application row");
        sqlx::query("INSERT INTO payment_transfers (id, account, amount) VALUES (?, ?, ?)")
            .bind("transfer-2")
            .bind("bob")
            .bind(9_i64)
            .execute(transaction.sqlite())
            .await
            .expect("insert second application row");
        transaction.commit().await.expect("commit application rows");
    });
    let schema = PaymentTransferSchema
        .build_schema(ApplicationGraphqlSchemaContext::new(entity_store))
        .expect("application schema");

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                r#"
            {
              paymentTransfers(account: "alice") {
                id
                account
                amount
              }
            }
            "#,
            )
            .await
    });

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    let transfers = body["paymentTransfers"]
        .as_array()
        .expect("paymentTransfers array");
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0]["id"], "transfer-1");
    assert_eq!(transfers[0]["account"], "alice");
    assert_eq!(transfers[0]["amount"], 42);
}

#[test]
fn test_application_graphql_router_uses_existing_auth_rate_limit_and_metrics_boundaries() {
    let entity_store = Arc::new(
        SqliteApplicationEntityStore::connect(&sqlite_entity_test_url("graphql-auth"))
            .expect("sqlite entity store connects"),
    );
    Runtime::new().expect("runtime").block_on(async {
        let mut transaction = entity_store.begin().await.expect("begin transaction");
        sqlx::query(
            "CREATE TABLE payment_transfers (id TEXT PRIMARY KEY, account TEXT NOT NULL, amount INTEGER NOT NULL)",
        )
        .execute(transaction.sqlite())
        .await
        .expect("create application table");
        sqlx::query("INSERT INTO payment_transfers (id, account, amount) VALUES (?, ?, ?)")
            .bind("transfer-1")
            .bind("alice")
            .bind(42_i64)
            .execute(transaction.sqlite())
            .await
            .expect("insert application row");
        transaction.commit().await.expect("commit application rows");
    });
    let schema = PaymentTransferSchema
        .build_schema(ApplicationGraphqlSchemaContext::new(entity_store))
        .expect("application schema");
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let app = graphql_application_router_with_auth(
        schema,
        "/graphql",
        false,
        auth_config(Some(QueryAuthQuotaConfig {
            max_requests_per_minute: Some(1),
            max_concurrent_requests: None,
        })),
        Some(IndexerGraphqlMetrics {
            recorder: recorder.clone(),
            labels: metric_labels(),
            endpoint: Some(MetricsEndpointConfig {
                path: "/metrics".to_owned(),
                bearer_token: None,
            }),
        }),
    );

    Runtime::new().expect("runtime").block_on(async {
        let missing = app
            .clone()
            .oneshot(graphql_http_request(
                r#"{ paymentTransfers(account: "alice") { id } }"#,
            ))
            .await
            .expect("missing token response");
        let accepted = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ paymentTransfers(account: "alice") { id } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("accepted response");
        let rate_limited = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ paymentTransfers(account: "alice") { id } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("rate limited response");
        let metrics = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");

        assert_auth_error(missing, StatusCode::UNAUTHORIZED, "AuthenticationFailed").await;
        assert_eq!(accepted.status(), StatusCode::OK);
        assert_auth_error(rate_limited, StatusCode::TOO_MANY_REQUESTS, "RateLimited").await;
        let text = response_text(metrics).await;
        assert!(text.contains(
            r#"datalens_indexer_graphql_query_total{application="query_app",chain="ethereum",dataset="evm.logs",index="ormp",outcome="success",output="sqlite"} 1"#
        ));
        assert!(text.contains(
            r#"datalens_indexer_graphql_auth_failure_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",output="sqlite"} 1"#
        ));
        assert!(text.contains(
            r#"datalens_indexer_graphql_rate_limited_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",output="sqlite"} 1"#
        ));
    });
}

#[test]
fn test_graphql_decoded_events_query_returns_metadata_args_and_decode_status() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    store
        .write_records(&[
            record(5, 0, "0x0000000000000000000000000000000000000001"),
            decoded_record(10, 1, "MessageAccepted(bytes32,uint256,address,address)"),
        ])
        .expect("write records");
    let schema = graphql_schema(Arc::new(store));

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                r#"
            {
              decodedEvents(
                indexName: "ormp"
                chain: "ethereum"
                chainId: 1
                dataset: "evm.logs"
                eventName: "MessageAccepted"
                limit: 10
              ) {
                indexName
                chain
                chainId
                dataset
                blockNumber
                blockHash
                transactionHash
                transactionIndex
                logIndex
                address
                eventName
                signature
                topic0
                decodedArgs
                decodeStatus
                decodeError
                payload
                createdAt
              }
            }
            "#,
            )
            .await
    });

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    let events = body["decodedEvents"]
        .as_array()
        .expect("decodedEvents array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["indexName"], "ormp");
    assert_eq!(events[0]["chain"], "ethereum");
    assert_eq!(events[0]["chainId"], 1);
    assert_eq!(events[0]["dataset"], "evm.logs");
    assert_eq!(events[0]["blockNumber"], 10);
    assert_eq!(events[0]["logIndex"], 1);
    assert_eq!(
        events[0]["address"],
        "0x2cd1867fb8016f93710b6386f7f9f1d540a60812"
    );
    assert_eq!(events[0]["eventName"], "MessageAccepted");
    assert_eq!(
        events[0]["signature"],
        "MessageAccepted(bytes32,uint256,address,address)"
    );
    assert_eq!(
        events[0]["topic0"],
        "0x9e6c1c44f7b2b36245897f9be35a5500f3a9e0d5b8f29f89dbf04b54053bb7d1"
    );
    assert_eq!(events[0]["decodedArgs"]["msgHash"], "0xabc");
    assert_eq!(events[0]["decodedArgs"]["fromChainId"], "1");
    assert_eq!(events[0]["decodeStatus"], "decoded");
    assert!(events[0]["decodeError"].is_null());
    assert_eq!(events[0]["payload"]["decoded"]["msgHash"], "0xabc");
    assert!(events[0]["createdAt"].as_str().is_some());
}

#[test]
fn test_graphql_decoded_events_query_supports_empty_result_pagination_and_filters() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    store
        .write_records(&[
            decoded_record(10, 0, "MessageAccepted(bytes32,uint256,address,address)"),
            decoded_record_with_status(
                20,
                1,
                "MessageAccepted(bytes32,uint256,address,address)",
                "failed",
                Some("missing indexed argument"),
            ),
            decoded_record(30, 2, "MessageDispatched(bytes32,uint256,address,address)"),
        ])
        .expect("write records");
    let schema = graphql_schema(Arc::new(store));

    let response = Runtime::new().expect("runtime").block_on(async {
        schema
            .execute(
                GraphqlRequest::new(
                    r#"
                query DecodedEvents($address: String!, $topic0: String!) {
                  first: decodedEvents(dataset: "evm.logs", eventName: "Missing", limit: 10) {
                    blockNumber
                  }
                  secondPage: decodedEvents(
                    indexName: "ormp"
                    chain: "ethereum"
                    chainId: 1
                    dataset: "evm.logs"
                    address: $address
                    eventName: "MessageAccepted"
                    signature: "MessageAccepted(bytes32,uint256,address,address)"
                    topic0: $topic0
                    fromBlock: 1
                    toBlock: 25
                    limit: 1
                    after: "1"
                  ) {
                    blockNumber
                    logIndex
                    decodeStatus
                    decodeError
                  }
                }
                "#,
                )
                .variables(Variables::from_json(serde_json::json!({
                    "address": "0x2cd1867fb8016f93710b6386f7f9f1d540a60812",
                    "topic0": "0x9e6c1c44f7b2b36245897f9be35a5500f3a9e0d5b8f29f89dbf04b54053bb7d1",
                }))),
            )
            .await
    });

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    assert_eq!(body["first"].as_array().expect("first array").len(), 0);
    let second_page = body["secondPage"].as_array().expect("secondPage array");
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0]["blockNumber"], 20);
    assert_eq!(second_page[0]["logIndex"], 1);
    assert_eq!(second_page[0]["decodeStatus"], "failed");
    assert_eq!(second_page[0]["decodeError"], "missing indexed argument");
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

#[test]
fn test_graphql_auth_failures_and_rate_limits_are_exposed_in_metrics() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let store =
        Arc::new(SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects"));
    let _store_keepalive = store.clone();
    let app = graphql_router_with_auth(
        store,
        "/graphql",
        false,
        auth_config(Some(QueryAuthQuotaConfig {
            max_requests_per_minute: Some(1),
            max_concurrent_requests: None,
        })),
        Some(IndexerGraphqlMetrics {
            recorder: recorder.clone(),
            labels: metric_labels(),
            endpoint: Some(MetricsEndpointConfig {
                path: "/metrics".to_owned(),
                bearer_token: None,
            }),
        }),
    );

    Runtime::new().expect("runtime").block_on(async {
        let unauthorized = app
            .clone()
            .oneshot(graphql_http_request(
                r#"{ events(dataset: "evm.logs") { blockNumber } }"#,
            ))
            .await
            .expect("unauthorized response");
        let first = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("first response");
        let rate_limited = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("rate-limited response");
        let metrics = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");

        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(rate_limited.status(), StatusCode::TOO_MANY_REQUESTS);
        let text = response_text(metrics).await;
        assert!(text.contains(
            r#"datalens_indexer_graphql_auth_failure_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",output="sqlite"} 1"#
        ));
        assert!(text.contains(
            r#"datalens_indexer_graphql_rate_limited_total{application="ormp",chain="ethereum",dataset="evm.logs",index="ormp",output="sqlite"} 1"#
        ));
        assert!(!text.contains("query-token"));
    });
}

#[test]
fn test_graphql_auth_disabled_allows_query_without_token() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    store
        .write_records(&[record(10, 0, "0x0000000000000000000000000000000000000001")])
        .expect("write records");
    let store = Arc::new(store);
    let _store_keepalive = store.clone();
    let app = graphql_router_with_auth(store, "/graphql", false, QueryAuthConfig::default(), None);

    Runtime::new().expect("runtime").block_on(async {
        let response = app
            .oneshot(graphql_http_request(
                r#"{ events(dataset: "evm.logs") { blockNumber } }"#,
            ))
            .await
            .expect("graphql response");

        assert_eq!(response.status(), StatusCode::OK);
    });
}

#[test]
fn test_graphql_auth_rejects_missing_and_invalid_token() {
    let app = graphql_router_with_auth(
        Arc::new(SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects")),
        "/graphql",
        false,
        auth_config(None),
        None,
    );

    Runtime::new().expect("runtime").block_on(async {
        let missing = app
            .clone()
            .oneshot(graphql_http_request(
                r#"{ events(dataset: "evm.logs") { blockNumber } }"#,
            ))
            .await
            .expect("missing token response");
        let invalid = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .map(|body| body)
                    .tap_header(header::AUTHORIZATION, "Bearer wrong-token"),
            )
            .await
            .expect("invalid token response");

        assert_auth_error(missing, StatusCode::UNAUTHORIZED, "AuthenticationFailed").await;
        assert_auth_error(invalid, StatusCode::UNAUTHORIZED, "AuthenticationFailed").await;
    });
}

#[test]
fn test_graphql_auth_rejects_disabled_application() {
    let mut config = auth_config(None);
    config.applications[0].enabled = false;
    let store =
        Arc::new(SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects"));
    let _store_keepalive = store.clone();
    let app = graphql_router_with_auth(store, "/graphql", false, config, None);

    Runtime::new().expect("runtime").block_on(async {
        let response = app
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("disabled application response");

        assert_auth_error(response, StatusCode::FORBIDDEN, "Unauthorized").await;
    });
}

#[test]
fn test_graphql_auth_accepts_valid_token_and_records_application_metrics() {
    let store = SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects");
    store
        .write_records(&[record(10, 0, "0x0000000000000000000000000000000000000001")])
        .expect("write records");
    let store = Arc::new(store);
    let _store_keepalive = store.clone();
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let app = graphql_router_with_auth(
        store,
        "/graphql",
        false,
        auth_config(None),
        Some(IndexerGraphqlMetrics {
            recorder: recorder.clone(),
            labels: metric_labels(),
            endpoint: Some(MetricsEndpointConfig {
                path: "/metrics".to_owned(),
                bearer_token: None,
            }),
        }),
    );

    Runtime::new().expect("runtime").block_on(async {
        let response = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("graphql response");
        let metrics = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");

        assert_eq!(response.status(), StatusCode::OK);
        let text = response_text(metrics).await;
        assert!(text.contains(
            r#"datalens_indexer_graphql_query_total{application="query_app",chain="ethereum",dataset="evm.logs",index="ormp",outcome="success",output="sqlite"} 1"#
        ));
    });
}

#[test]
fn test_graphql_auth_rate_limit_exceeded_returns_stable_error() {
    let app = graphql_router_with_auth(
        Arc::new(SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects")),
        "/graphql",
        false,
        auth_config(Some(QueryAuthQuotaConfig {
            max_requests_per_minute: Some(1),
            max_concurrent_requests: None,
        })),
        None,
    );

    Runtime::new().expect("runtime").block_on(async {
        let first = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("first response");
        let second = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("second response");

        assert_eq!(first.status(), StatusCode::OK);
        assert_auth_error(second, StatusCode::TOO_MANY_REQUESTS, "RateLimited").await;
    });
}

#[test]
fn test_graphql_auth_concurrent_limit_exceeded_returns_stable_error() {
    let store = Arc::new(DelayedStore {
        delay: Duration::from_millis(150),
    });
    let app = graphql_router_with_auth(
        store,
        "/graphql",
        false,
        auth_config(Some(QueryAuthQuotaConfig {
            max_requests_per_minute: None,
            max_concurrent_requests: Some(1),
        })),
        None,
    );

    Runtime::new().expect("runtime").block_on(async {
        let first = tokio::spawn(
            app.clone().oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            ),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        let second = app
            .clone()
            .oneshot(
                graphql_http_request(r#"{ events(dataset: "evm.logs") { blockNumber } }"#)
                    .tap_header(header::AUTHORIZATION, "Bearer query-token"),
            )
            .await
            .expect("second response");
        let first = first.await.expect("first task").expect("first response");

        assert_eq!(first.status(), StatusCode::OK);
        assert_auth_error(second, StatusCode::TOO_MANY_REQUESTS, "RateLimited").await;
    });
}

#[test]
fn test_graphql_auth_protects_playground() {
    let app = graphql_router_with_auth(
        Arc::new(SqliteOutputStore::connect("sqlite::memory:").expect("sqlite store connects")),
        "/graphql",
        true,
        auth_config(None),
        None,
    );

    Runtime::new().expect("runtime").block_on(async {
        let missing = app
            .clone()
            .oneshot(
                Request::get("/graphql/playground")
                    .body(Body::empty())
                    .expect("playground request"),
            )
            .await
            .expect("missing token response");
        let accepted = app
            .clone()
            .oneshot(
                Request::get("/graphql/playground")
                    .header(header::AUTHORIZATION, "Bearer query-token")
                    .body(Body::empty())
                    .expect("playground request"),
            )
            .await
            .expect("accepted response");

        assert_auth_error(missing, StatusCode::UNAUTHORIZED, "AuthenticationFailed").await;
        assert_eq!(accepted.status(), StatusCode::OK);
    });
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

trait RequestHeaderExt {
    fn tap_header(self, name: header::HeaderName, value: &str) -> Self;
}

impl RequestHeaderExt for Request<Body> {
    fn tap_header(mut self, name: header::HeaderName, value: &str) -> Self {
        self.headers_mut()
            .insert(name, value.parse().expect("header value"));
        self
    }
}

fn auth_config(quota: Option<QueryAuthQuotaConfig>) -> QueryAuthConfig {
    QueryAuthConfig {
        enabled: true,
        applications: vec![QueryAuthApplicationConfig {
            id: "Query_App".to_owned(),
            enabled: true,
            token: "query-token".to_owned(),
            quota,
        }],
    }
}

async fn assert_auth_error(response: axum::response::Response, status: StatusCode, kind: &str) {
    assert_eq!(response.status(), status);
    let body: serde_json::Value =
        serde_json::from_str(&response_text(response).await).expect("error JSON");
    assert_eq!(body["error"]["kind"], kind);
    assert!(!body.to_string().contains("query-token"));
}

struct DelayedStore {
    delay: Duration,
}

impl QueryableStore for DelayedStore {
    fn query(&self, _query: StoreQuery) -> Result<StoreQueryResult, IndexerError> {
        std::thread::sleep(self.delay);
        Ok(StoreQueryResult {
            rows: vec![serde_json::json!({
                "index": "ormp",
                "chain": "ethereum",
                "chain_id": 1,
                "dataset": "evm.logs",
                "block_number": 10,
                "topics": [],
            })],
        })
    }
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

struct PaymentTransferSchema;

impl ApplicationGraphqlSchemaHook for PaymentTransferSchema {
    fn build_schema(
        &self,
        context: ApplicationGraphqlSchemaContext,
    ) -> Result<DynamicSchema, IndexerError> {
        let transfer = DynamicObject::new("PaymentTransfer")
            .field(Field::new(
                "id",
                TypeRef::named_nn(TypeRef::STRING),
                |ctx| {
                    FieldFuture::new(async move {
                        Ok(Some(FieldValue::value(json_string(
                            ctx.parent_value.try_downcast_ref::<serde_json::Value>()?,
                            "id",
                        ))))
                    })
                },
            ))
            .field(Field::new(
                "account",
                TypeRef::named_nn(TypeRef::STRING),
                |ctx| {
                    FieldFuture::new(async move {
                        Ok(Some(FieldValue::value(json_string(
                            ctx.parent_value.try_downcast_ref::<serde_json::Value>()?,
                            "account",
                        ))))
                    })
                },
            ))
            .field(Field::new(
                "amount",
                TypeRef::named_nn(TypeRef::INT),
                |ctx| {
                    FieldFuture::new(async move {
                        Ok(Some(FieldValue::value(json_i32(
                            ctx.parent_value.try_downcast_ref::<serde_json::Value>()?,
                            "amount",
                        ))))
                    })
                },
            ));
        let query = DynamicObject::new("Query").field(
            Field::new(
                "paymentTransfers",
                TypeRef::named_nn_list_nn("PaymentTransfer"),
                |ctx| {
                    let store = ctx
                        .data::<Arc<dyn ApplicationEntityQueryStore>>()
                        .expect("entity store")
                        .clone();
                    let account = ctx
                        .args
                        .try_get("account")
                        .and_then(|value| value.string())
                        .expect("account argument")
                        .to_owned();
                    FieldFuture::new(async move {
                        let rows = store
                            .query_json(
                                ApplicationEntityReadQuery::new(
                                    "SELECT id, account, amount FROM payment_transfers WHERE account = ? ORDER BY id",
                                )
                                .bind(account),
                            )
                            .await
                            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
                        Ok(Some(FieldValue::list(
                            rows.into_iter().map(FieldValue::owned_any),
                        )))
                    })
                },
            )
            .argument(async_graphql::dynamic::InputValue::new(
                "account",
                TypeRef::named_nn(TypeRef::STRING),
            )),
        );

        DynamicSchema::build("Query", None, None)
            .data(context.entity_store())
            .register(query)
            .register(transfer)
            .finish()
            .map_err(|error| IndexerError::Config(format!("application graphql schema: {error}")))
    }
}

fn sqlite_entity_test_url(name: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "datalens-application-graphql-{name}-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    format!("sqlite:{}", path.display())
}

fn json_string(value: &serde_json::Value, field: &str) -> String {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn json_i32(value: &serde_json::Value, field: &str) -> i32 {
    value
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}
