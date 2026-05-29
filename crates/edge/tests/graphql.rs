use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use datalens_core::BlockRange;
use datalens_edge::config::{EdgeConfig, GraphqlConfig};
use datalens_edge::{QueryService, QueryServiceRegistry, router, router_with_edge_config};
use datalens_solana::SolanaAdapter;
use datalens_storage::LocalStorage;
use datalens_tron::TronAdapter;
use tower::ServiceExt;

#[path = "graphql/support.rs"]
mod support;

use support::*;

#[tokio::test]
async fn test_graphql_discovery_lists_registered_chains() {
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("discovery-ethereum")),
            MockSource::default().with_chain(ethereum_identity()),
            "ethereum",
            chain_config(1),
        ))
        .expect("register ethereum")
        .with_service(QueryService::new_named(
            LocalStorage::new(temp_storage_root("discovery-solana")),
            SolanaAdapter::with_fixture_defaults(),
            planner_config(),
            writer_config(),
            "solana-mainnet-beta",
            non_evm_chain_config("solana"),
        ))
        .expect("register solana");
    let app = router(registry);

    let body = graphql_json(
        app.clone(),
        r#"
        query {
          discovery {
            chains {
              identity
              datasets
            }
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    let chains = body["data"]["discovery"]["chains"]
        .as_array()
        .expect("chains");
    assert_eq!(chains.len(), 2);
    assert!(chains.iter().any(|chain| {
        chain["identity"]["configured_name"] == "ethereum"
            && chain["datasets"] == serde_json::json!(["blocks", "logs"])
    }));
    assert!(chains.iter().any(|chain| {
        chain["identity"]["configured_name"] == "solana-mainnet-beta"
            && chain["datasets"] == serde_json::json!([])
    }));
}

#[tokio::test]
async fn test_graphql_native_evm_blocks_and_logs_query_match_rest_contract() {
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-evm")),
            source.clone(),
        ))
        .expect("register service");
    let app = router(registry);

    let body = graphql_json(
        app.clone(),
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
            range
            rows
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_chain_input(),
                "datasetKey": dataset_key_input("evm", "blocks"),
                "selector": { "kind": "all" },
                "range": { "kind": "block", "start": 10, "end": 10 },
                "finality": "durable_only",
                "fields": {}
            }
        }),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_eq!(body["data"]["query"]["datasetKey"], "evm.blocks");
    assert_eq!(
        body["data"]["query"]["range"],
        serde_json::json!({ "kind": "block", "start": 10, "end": 10 })
    );
    assert_eq!(
        body["data"]["query"]["rows"]["rows"]["rows"][0]["number"],
        10
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );

    let body = graphql_json(
        app,
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
            range
            rows
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_chain_input(),
                "datasetKey": dataset_key_input("evm", "logs"),
                "selector": {
                    "kind": "evm_logs",
                    "evmLogs": {
                        "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "topics": []
                    }
                },
                "range": { "kind": "block", "start": 20, "end": 21 },
                "finality": "durable_only",
                "fields": {}
            }
        }),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_eq!(body["data"]["query"]["datasetKey"], "evm.logs");
    assert_eq!(
        body["data"]["query"]["range"],
        serde_json::json!({ "kind": "block", "start": 20, "end": 21 })
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(10, 10)),
            SourceCall::Logs(BlockRange::expect_new(20, 21))
        ]
    );
}

#[tokio::test]
async fn test_graphql_native_solana_query_uses_native_contract() {
    let root = temp_storage_root("gql-solana");
    let solana = SolanaAdapter::with_fixture_defaults();
    let registry = QueryServiceRegistry::new()
        .with_service(QueryService::new_named(
            LocalStorage::new(&root),
            solana,
            planner_config(),
            writer_config(),
            "solana-mainnet-beta",
            non_evm_chain_config("solana"),
        ))
        .expect("register solana");
    let app = router(registry);

    let body = graphql_json(
        app,
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
            range
            rows
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": solana_chain_input(),
                "datasetKey": dataset_key_input("solana", "slots"),
                "selector": {
                    "kind": "other",
                    "other": {
                        "kind": "solana_all",
                        "fingerprint": "solana-all/all",
                        "canonicalKey": "all"
                    }
                },
                "range": { "kind": "slot", "start": 10, "end": 12 },
                "finality": "durable_only",
                "fields": {}
            }
        }),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_eq!(body["data"]["query"]["datasetKey"], "solana.slots");
    assert_eq!(
        body["data"]["query"]["range"],
        serde_json::json!({ "kind": "slot", "start": 10, "end": 12 })
    );
    assert_eq!(
        body["data"]["query"]["rows"]["rows"]["rows"]["rows"][0]["slot"],
        10
    );
}

#[tokio::test]
async fn test_graphql_native_tron_blocks_query_uses_typed_inputs() {
    let root = temp_storage_root("gql-tron");
    let tron = TronAdapter::with_fixture_defaults();
    let registry = QueryServiceRegistry::new()
        .with_service(QueryService::new_named(
            LocalStorage::new(&root),
            tron,
            planner_config(),
            writer_config(),
            "tron-mainnet",
            non_evm_chain_config("tron"),
        ))
        .expect("register tron");
    let app = router(registry);

    let body = graphql_json(
        app,
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
            range
            rows
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": tron_chain_input(),
                "datasetKey": dataset_key_input("tron", "blocks"),
                "selector": {
                    "kind": "other",
                    "other": {
                        "kind": "tron_all",
                        "fingerprint": "tron-all/all",
                        "canonicalKey": "all"
                    }
                },
                "range": { "kind": "block", "start": 10, "end": 12 },
                "finality": "durable_only",
                "fields": {}
            }
        }),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_eq!(body["data"]["query"]["datasetKey"], "tron.blocks");
    assert_eq!(
        body["data"]["query"]["range"],
        serde_json::json!({ "kind": "block", "start": 10, "end": 12 })
    );
    assert_eq!(
        body["data"]["query"]["rows"]["rows"]["rows"]["rows"][0]["number"],
        10
    );
}

#[tokio::test]
async fn test_graphql_warmup_submit_list_and_cancel_task() {
    let root = temp_storage_root("gql-warmup");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("register service");
    let app = router(registry);

    let submit = graphql_json(
        app.clone(),
        r#"
        mutation($input: WarmupSubmitInput!) {
          submitWarmupTask(input: $input) {
            taskId
            created
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_chain_input(),
                "datasetKey": dataset_key_input("evm", "logs"),
                "selector": {
                    "kind": "evm_logs",
                    "evmLogs": {
                        "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "topics": []
                    }
                },
                "rangeKind": { "kind": "block" },
                "start": 20,
                "end": 21,
                "mode": "fixed_range",
                "chunkPolicy": { "maxRangeLen": 2 }
            }
        }),
    )
    .await;
    assert_eq!(submit["errors"], serde_json::Value::Null);
    let task_id = submit["data"]["submitWarmupTask"]["taskId"]
        .as_str()
        .expect("task id")
        .to_owned();

    let listed = graphql_json(
        app.clone(),
        r#"
        query {
          warmupTasks {
            taskId
            state
            datasetKey
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(listed["errors"], serde_json::Value::Null);
    assert_eq!(
        listed["data"]["warmupTasks"]
            .as_array()
            .expect("tasks")
            .len(),
        1
    );
    assert_eq!(listed["data"]["warmupTasks"][0]["datasetKey"], "evm.logs");

    let cancelled = graphql_json(
        app,
        r#"
        mutation($id: ID!) {
          cancelWarmupTask(id: $id) {
            taskId
            state
          }
        }
        "#,
        serde_json::json!({ "id": task_id }),
    )
    .await;
    assert_eq!(cancelled["errors"], serde_json::Value::Null);
    assert_eq!(cancelled["data"]["cancelWarmupTask"]["state"], "cancelled");
}

#[tokio::test]
async fn test_graphql_playground_respects_config() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-playground")),
            MockSource::default(),
        ))
        .expect("register service");
    let enabled = router_with_edge_config(
        registry.clone(),
        EdgeConfig {
            graphql: GraphqlConfig {
                enabled: true,
                playground_enabled: true,
            },
        },
    );
    let disabled = router_with_edge_config(
        registry,
        EdgeConfig {
            graphql: GraphqlConfig {
                enabled: true,
                playground_enabled: false,
            },
        },
    );

    let response = enabled
        .oneshot(
            Request::get("/graphql/playground")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("enabled response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(body_text(response.into_body()).await.contains("/graphql"));

    let response = disabled
        .oneshot(
            Request::get("/graphql/playground")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("disabled response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_graphql_can_be_disabled_independently() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-disabled")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router_with_edge_config(
        registry,
        EdgeConfig {
            graphql: GraphqlConfig {
                enabled: false,
                playground_enabled: true,
            },
        },
    );

    let response = app
        .oneshot(
            Request::post("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"{ chains }"}"#))
                .expect("request"),
        )
        .await
        .expect("disabled response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_graphql_query_records_metrics_with_application_header() {
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-metrics")),
            source,
        ))
        .expect("register service");
    let app = router(registry);

    let response = app
        .clone()
        .oneshot(
            Request::post("/graphql")
                .header("content-type", "application/json")
                .header("x-datalens-application", "wallet-search")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "query": r#"
                            query($input: QueryInput!) {
                              query(input: $input) {
                                datasetKey
                              }
                            }
                        "#,
                        "variables": {
                            "input": {
                                "chain": ethereum_chain_input(),
                                "datasetKey": dataset_key_input("evm", "blocks"),
                                "selector": { "kind": "all" },
                                "range": { "kind": "block", "start": 10, "end": 10 }
                            }
                        }
                    }))
                    .expect("graphql request"),
                ))
                .expect("request"),
        )
        .await
        .expect("graphql response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response.into_body()).await;
    assert_eq!(body["errors"], serde_json::Value::Null);

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let text = body_text(metrics.into_body()).await;
    assert!(text.contains(
        r#"datalens_query_total{application="wallet-search",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="filled"} 1"#
    ));
}

#[tokio::test]
async fn test_graphql_errors_include_stable_extensions() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-errors")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router(registry);

    let body = graphql_json(
        app,
        r#"
        query($input: QueryInput!) {
          query(input: $input) {
            datasetKey
          }
        }
        "#,
        serde_json::json!({
            "input": {
                "chain": ethereum_chain_input(),
                "datasetKey": { "family": "bad", "name": "" },
                "selector": { "kind": "all" },
                "range": { "kind": "block", "start": 1, "end": 1 }
            }
        }),
    )
    .await;

    assert_eq!(body["errors"][0]["extensions"]["kind"], "invalid_input");
    assert_eq!(body["errors"][0]["extensions"]["status"], 400);
}

#[tokio::test]
async fn test_graphql_schema_exposes_typed_request_inputs() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-schema")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router(registry);

    let body = graphql_json(
        app,
        r#"
        query {
          __type(name: "QueryInput") {
            inputFields {
              name
              type {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
          selector: __type(name: "QuerySelectorInput") {
            inputFields {
              name
            }
          }
          warmup: __type(name: "WarmupSubmitInput") {
            inputFields {
              name
              type {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;

    assert_eq!(body["errors"], serde_json::Value::Null);
    assert_input_field_type(&body["data"]["__type"], "chain", "ChainIdentityInput");
    assert_input_field_type(&body["data"]["__type"], "datasetKey", "DatasetKeyInput");
    assert_input_field_type(&body["data"]["__type"], "selector", "QuerySelectorInput");
    assert_input_field_type(&body["data"]["__type"], "range", "QueryRangeInput");
    assert_input_field_type(&body["data"]["__type"], "fields", "FieldSelectionInput");
    assert!(
        body["data"]["selector"]["inputFields"]
            .as_array()
            .expect("selector input fields")
            .iter()
            .any(|field| field["name"] == "evmLogs")
    );
    assert_input_field_type(
        &body["data"]["warmup"],
        "datasetKey",
        "WarmupDatasetKeyInput",
    );
    assert_input_field_type(&body["data"]["warmup"], "selector", "WarmupSelectorInput");
    assert_input_field_type(&body["data"]["warmup"], "rangeKind", "RangeKindInput");
    assert_input_field_type(
        &body["data"]["warmup"],
        "chunkPolicy",
        "WarmupChunkPolicyInput",
    );
    assert_input_field_type(
        &body["data"]["warmup"],
        "retryPolicy",
        "WarmupRetryPolicyInput",
    );
}

async fn graphql_json(
    app: axum::Router,
    query: &str,
    variables: serde_json::Value,
) -> serde_json::Value {
    let response = app
        .oneshot(
            Request::post("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "query": query,
                        "variables": variables
                    }))
                    .expect("graphql request"),
                ))
                .expect("request"),
        )
        .await
        .expect("graphql response");
    assert_eq!(response.status(), StatusCode::OK);
    body_json(response.into_body()).await
}

async fn body_json(body: Body) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(body, usize::MAX).await.expect("body bytes"))
        .expect("json body")
}

async fn body_text(body: Body) -> String {
    String::from_utf8(
        to_bytes(body, usize::MAX)
            .await
            .expect("body bytes")
            .to_vec(),
    )
    .expect("utf8 body")
}

fn assert_input_field_type(parent: &serde_json::Value, field_name: &str, expected_type: &str) {
    let field = parent["inputFields"]
        .as_array()
        .expect("input fields")
        .iter()
        .find(|field| field["name"] == field_name)
        .unwrap_or_else(|| panic!("missing input field {field_name}"));
    let field_type = &field["type"];
    let type_name = field_type["name"]
        .as_str()
        .or_else(|| field_type["ofType"]["name"].as_str())
        .expect("field type name");
    assert_eq!(type_name, expected_type, "{field_name} type");
}
