mod support;

use support::graphql::*;

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
        .expect("register solana")
        .with_service(QueryService::new_named(
            LocalStorage::new(temp_storage_root("discovery-tron")),
            TronAdapter::with_fixture_defaults(),
            planner_config(),
            writer_config(),
            "tron-mainnet",
            non_evm_chain_config("tron"),
        ))
        .expect("register tron");
    let app = router(registry);

    let body = graphql_json(
        app.clone(),
        r#"
        query {
          discovery {
            chains {
              identity
              datasets {
                datasetKey
                rangeKinds
                selectors
                enabled
              }
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
    assert_eq!(chains.len(), 3);
    assert!(chains.iter().any(|chain| {
        chain["identity"]["configured_name"] == "ethereum"
            && chain["datasets"]
                == serde_json::json!([
                    {
                        "datasetKey": "evm.blocks",
                        "rangeKinds": [{"kind": "block"}],
                        "selectors": ["all"],
                        "enabled": true
                    },
                    {
                        "datasetKey": "evm.logs",
                        "rangeKinds": [{"kind": "block"}],
                        "selectors": ["evm_logs"],
                        "enabled": true
                    }
                ])
    }));
    assert!(chains.iter().any(|chain| {
        chain["identity"]["configured_name"] == "solana-mainnet-beta"
            && chain["datasets"]
                .as_array()
                .expect("solana datasets")
                .iter()
                .any(|dataset| {
                    dataset["datasetKey"] == "solana.slots"
                        && dataset["rangeKinds"] == serde_json::json!([{"kind": "slot"}])
                        && dataset["selectors"] == serde_json::json!(["solana_all", "all"])
                        && dataset["enabled"] == true
                })
    }));
    assert!(chains.iter().any(|chain| {
        chain["identity"]["configured_name"] == "tron-mainnet"
            && chain["datasets"]
                .as_array()
                .expect("tron datasets")
                .iter()
                .any(|dataset| {
                    dataset["datasetKey"] == "tron.blocks"
                        && dataset["rangeKinds"] == serde_json::json!([{"kind": "block"}])
                        && dataset["selectors"] == serde_json::json!(["tron_all"])
                        && dataset["enabled"] == true
                })
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
                "chain": ethereum_identity(),
                "datasetKey": "evm.blocks",
                "selector": { "kind": "all" },
                "range": { "kind": "block", "start": 10, "end": 10 },
                "finality": "durable_only",
                "fields": "all"
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
                "chain": ethereum_identity(),
                "datasetKey": "evm.logs",
                "selector": {
                    "kind": "evm_logs",
                    "value": {
                        "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "topics": []
                    }
                },
                "range": { "kind": "block", "start": 20, "end": 21 },
                "finality": "durable_only",
                "fields": "all"
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
                "chain": solana_identity(),
                "datasetKey": "solana.slots",
                "selector": {
                    "kind": "other",
                    "value": {
                        "kind": "solana_all",
                        "fingerprint": "solana-all/all",
                        "canonical_key": "all"
                    }
                },
                "range": { "kind": "slot", "start": 10, "end": 12 },
                "finality": "durable_only",
                "fields": "all"
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
                                "chain": ethereum_identity(),
                                "datasetKey": "evm.blocks",
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
        r#"datalens_query_total{application="wallet-search",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="filled"} 1"#
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
                "chain": ethereum_identity(),
                "datasetKey": "bad-key",
                "selector": { "kind": "all" },
                "range": { "kind": "block", "start": 1, "end": 1 }
            }
        }),
    )
    .await;

    assert_eq!(body["errors"][0]["extensions"]["kind"], "invalid_input");
    assert_eq!(body["errors"][0]["extensions"]["status"], 400);
}
