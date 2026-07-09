mod support;

use datalens_storage::{
    QueryWatermark, QueryWatermarkKey, QueryWatermarkRepository, QueryWatermarkStore,
};
use datalens_warmup::WarmupTaskFilter;
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

#[tokio::test]
async fn test_graphql_run_warmup_task_once_runs_only_requested_task() {
    let root = temp_storage_root("gql-warmup-run-task-once");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source.clone())
        .with_warmup_pool(warmup_pool_with_max_fetches(&root, 1));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("register service");
    let app = graphql_router(registry);

    let mutation = r#"
        mutation($input: WarmupSubmitInput!) {
          submitWarmupTask(input: $input) {
            taskId
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
                "end": 21,
                "mode": "fixed_range"
            }
        }),
    )
    .await;
    assert_eq!(first["errors"], serde_json::Value::Null);

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
                        "addresses": ["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
                        "topics": []
                    }
                },
                "rangeKind": { "kind": "block" },
                "start": 30,
                "end": 31,
                "mode": "fixed_range"
            }
        }),
    )
    .await;
    assert_eq!(second["errors"], serde_json::Value::Null);
    let second_task_id = second["data"]["submitWarmupTask"]["taskId"]
        .as_str()
        .expect("second task id")
        .to_owned();

    let run = graphql_json(
        app,
        r#"
        mutation($id: ID!) {
          runWarmupTaskOnce(id: $id) {
            results
          }
        }
        "#,
        serde_json::json!({ "id": second_task_id }),
    )
    .await;
    assert_eq!(run["errors"], serde_json::Value::Null);
    let results = run["data"]["runWarmupTaskOnce"]["results"]
        .as_array()
        .expect("results");
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_graphql_warmup_task_exposes_idle_follow_query_reason() {
    let root = temp_storage_root("gql-warmup-follow-query-idle");
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(root.join("watermarks")));
    let source = MockSource::default();
    let warmup_registry =
        LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry")));
    let service =
        service(LocalStorage::new(&root), source.clone()).with_warmup_pool(WarmupTaskPool::new(
            WarmupRuntime::new(
                source,
                LocalStorage::new(&root),
                warmup_registry.clone(),
                datalens_writer::DurableWriterConfig {
                    target_object_bytes: 1024,
                    min_object_rows: 1,
                    record_empty_coverage: true,
                    staging: Default::default(),
                },
            )
            .with_query_watermarks(watermarks.clone())
            .with_follow_query_idle_threshold_blocks(Some(10))
            .with_follow_query_resume_threshold_blocks(Some(20))
            .with_follow_query_start_offset_blocks(Some(1))
            .with_follow_query_lookahead_blocks(3)
            .with_runtime_config(WarmupRuntimeConfig {
                max_fetches_per_task_loop: 1,
                ..WarmupRuntimeConfig::default()
            }),
            WarmupSchedulerConfig {
                max_global_concurrent_tasks: 1,
                max_concurrent_tasks_per_chain: 1,
                fixed_range_min_tasks_per_tick: 0,
            },
        ));
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
                "start": 1,
                "end": null,
                "mode": "follow_query"
            }
        }),
    )
    .await;
    assert_eq!(submit["errors"], serde_json::Value::Null);
    let task = warmup_registry
        .list(WarmupTaskFilter::default())
        .expect("list warmup tasks")
        .into_iter()
        .next()
        .expect("warmup task");
    watermarks
        .update(&QueryWatermark {
            key: QueryWatermarkKey::new(
                task.application_id,
                task.chain,
                task.dataset_key,
                &task.selector,
                task.range_kind,
            ),
            latest_block: 990,
            updated_at_unix_seconds: 1,
        })
        .expect("save watermark");

    let run = graphql_json(
        app.clone(),
        r#"
        mutation {
          runWarmupOnce {
            results
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(run["errors"], serde_json::Value::Null);
    assert!(
        run["data"]["runWarmupOnce"]["results"]
            .as_array()
            .expect("results")
            .is_empty()
    );

    let listed = graphql_json(
        app,
        r#"
        query {
          warmupTasks {
            state
            noOpReason
          }
        }
        "#,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(listed["errors"], serde_json::Value::Null);
    let task = &listed["data"]["warmupTasks"].as_array().expect("tasks")[0];
    assert_eq!(task["state"], "idle");
    assert_eq!(task["noOpReason"], "near_safe_head");
}
