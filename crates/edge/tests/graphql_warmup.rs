mod support;

use support::graphql::*;

#[tokio::test]
async fn test_graphql_warmup_submit_list_and_cancel_task() {
    let root = temp_storage_root("gql-warmup");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("register service");
    let app = graphql_router(registry);

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
async fn test_graphql_warmup_follow_query_submit_reuses_identity_without_start() {
    let root = temp_storage_root("gql-warmup-follow-query-submit");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("register service");
    let app = graphql_router(registry);

    let mutation = r#"
        mutation($input: WarmupSubmitInput!) {
          submitWarmupTask(input: $input) {
            taskId
            created
          }
        }
        "#;
    let first = graphql_json(
        app.clone(),
        mutation,
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
                "end": null,
                "mode": "follow_query"
            }
        }),
    )
    .await;
    assert_eq!(first["errors"], serde_json::Value::Null);
    assert_eq!(first["data"]["submitWarmupTask"]["created"], true);
    let task_id = first["data"]["submitWarmupTask"]["taskId"]
        .as_str()
        .expect("task id")
        .to_owned();

    let second = graphql_json(
        app.clone(),
        mutation,
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
                "start": 500,
                "end": 550,
                "mode": "follow_query"
            }
        }),
    )
    .await;
    assert_eq!(second["errors"], serde_json::Value::Null);
    assert_eq!(second["data"]["submitWarmupTask"]["created"], false);
    assert_eq!(second["data"]["submitWarmupTask"]["taskId"], task_id);

    let listed = graphql_json(
        app,
        r#"
        query {
          warmupTasks {
            taskId
            start
            end
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(listed["errors"], serde_json::Value::Null);
    let tasks = listed["data"]["warmupTasks"].as_array().expect("tasks");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["taskId"], task_id);
    assert_eq!(tasks[0]["start"], 20);
    assert_eq!(tasks[0]["end"], serde_json::Value::Null);
}
