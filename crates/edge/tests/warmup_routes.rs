mod support;

use datalens_core::{DatasetKey, LedgerRangeKind};
use datalens_storage::{
    QueryWatermark, QueryWatermarkKey, QueryWatermarkRepository, QueryWatermarkStore,
};
use support::lifecycle::*;

#[tokio::test]
async fn test_api_warmup_routes_manage_application_scoped_tasks() {
    let root = temp_storage_root("api-warmup-routes");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let application_registry = datalens_edge::config::ApplicationRegistryConfig {
        required: true,
        applications: vec![
            application_config("app-a", "token-a"),
            application_config("app-b", "token-b"),
        ],
    };
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry)
        .expect("application registry")
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(10, 12))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 10,
                        "end": 12,
                        "mode": "fixed_range",
                        "chunk_policy": {
                            "max_range_len": 2
                        }
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);
    let submit_body = body_json(submit.into_body()).await;
    let task_id = submit_body["task_id"].as_str().expect("task id").to_owned();
    assert_eq!(submit_body["created"], true);

    let list = app
        .clone()
        .oneshot(
            Request::get("/v1/warmup/tasks")
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("list response");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = body_json(list.into_body()).await;
    assert_eq!(list_body["tasks"].as_array().expect("tasks").len(), 1);
    let listed_task = &list_body["tasks"][0];
    assert_eq!(listed_task["selector"]["kind"], "evm_logs");
    assert!(
        listed_task["selector"]["fingerprint"]
            .as_str()
            .expect("selector fingerprint")
            .starts_with("evm-logs/")
    );
    assert!(
        listed_task["selector"]["canonical_key"]
            .as_str()
            .expect("selector canonical key")
            .starts_with("evm-logs/addr=")
    );

    let read = app
        .clone()
        .oneshot(
            Request::get(format!("/v1/warmup/tasks/{task_id}"))
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("read response");
    assert_eq!(read.status(), StatusCode::OK);
    let read_body = body_json(read.into_body()).await;
    assert_eq!(read_body["task"]["selector"], listed_task["selector"]);

    let forbidden = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/warmup/tasks/{task_id}/cancel"))
                .header("x-datalens-application", "app-b")
                .header("authorization", "Bearer token-b")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("forbidden response");
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let forbidden_read = app
        .clone()
        .oneshot(
            Request::get(format!("/v1/warmup/tasks/{task_id}"))
                .header("x-datalens-application", "app-b")
                .header("authorization", "Bearer token-b")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("forbidden read response");
    assert_eq!(forbidden_read.status(), StatusCode::FORBIDDEN);

    let cancel = app
        .oneshot(
            Request::post(format!("/v1/warmup/tasks/{task_id}/cancel"))
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("cancel response");
    assert_eq!(cancel.status(), StatusCode::OK);
    let cancel_body = body_json(cancel.into_body()).await;
    assert_eq!(cancel_body["task"]["state"], "cancelled");
}

#[tokio::test]
async fn test_api_warmup_ensure_reuses_follow_query_task_without_start_in_identity() {
    let root = temp_storage_root("api-warmup-ensure-follow-query");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let first = app
        .clone()
        .oneshot(native_warmup_ensure_request(serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.logs",
            "selector": {
                "kind": "evm_logs",
                "value": evm_logs_selector_value(&logs_request(20, 21))
            },
            "range_kind": { "kind": "block" },
            "start": 20,
            "end": null,
            "mode": "follow_query"
        })))
        .await
        .expect("first ensure response");
    assert_eq!(first.status(), StatusCode::CREATED);
    let first_body = body_json(first.into_body()).await;
    let task_id = first_body["task_id"].as_str().expect("task id").to_owned();
    assert_eq!(first_body["created"], true);
    assert_eq!(first_body["state"], "queued");

    let second = app
        .clone()
        .oneshot(native_warmup_ensure_request(serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.logs",
            "selector": {
                "kind": "evm_logs",
                "value": evm_logs_selector_value(&logs_request(20, 21))
            },
            "range_kind": { "kind": "block" },
            "start": 500,
            "end": 550,
            "mode": "follow_query"
        })))
        .await
        .expect("second ensure response");
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = body_json(second.into_body()).await;
    assert_eq!(second_body["task_id"], task_id);
    assert_eq!(second_body["created"], false);
    assert_eq!(second_body["state"], "queued");

    let list = app
        .oneshot(
            Request::get("/v1/warmup/tasks")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("list response");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = body_json(list.into_body()).await;
    assert_eq!(list_body["tasks"].as_array().expect("tasks").len(), 1);
    assert_eq!(list_body["tasks"][0]["start"], 20);
    assert_eq!(list_body["tasks"][0]["end"], serde_json::Value::Null);
}

#[tokio::test]
async fn test_api_warmup_list_exposes_follow_query_status() {
    let root = temp_storage_root("api-warmup-follow-query-status");
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(root.join("watermarks")));
    let source = MockSource::default();
    let service =
        service(LocalStorage::new(&root), source.clone()).with_warmup_pool(WarmupTaskPool::new(
            WarmupRuntime::new(
                source,
                LocalStorage::new(&root),
                LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry"))),
                datalens_writer::DurableWriterConfig {
                    target_object_bytes: 1024,
                    min_object_rows: 1,
                    record_empty_coverage: true,
                    staging: Default::default(),
                },
            )
            .with_query_watermarks(watermarks.clone())
            .with_follow_query_start_offset_blocks(Some(1))
            .with_follow_query_lookahead_blocks(3)
            .with_runtime_config(WarmupRuntimeConfig {
                max_fetches_per_task_loop: 1,
                ..WarmupRuntimeConfig::default()
            }),
            WarmupSchedulerConfig {
                max_global_concurrent_tasks: 1,
                max_concurrent_tasks_per_chain: 1,
            },
        ));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    watermarks
        .update(&QueryWatermark {
            key: QueryWatermarkKey::new(
                "app-a",
                ethereum_identity(),
                DatasetKey::evm_logs(),
                &logs_request(10, 10).selector,
                LedgerRangeKind::Block,
            ),
            latest_block: 10,
            updated_at_unix_seconds: 1,
        })
        .expect("save watermark");

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks/ensure")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(10, 10))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 1,
                        "mode": "follow_query"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);

    let run_once = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .header("x-datalens-application", "app-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("run-once response");
    assert_eq!(run_once.status(), StatusCode::OK);

    let list = app
        .oneshot(
            Request::get("/v1/warmup/tasks")
                .header("x-datalens-application", "app-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("list response");

    assert_eq!(list.status(), StatusCode::OK);
    let list_body = body_json(list.into_body()).await;
    let task = &list_body["tasks"].as_array().expect("tasks")[0];
    assert_eq!(task["query_watermark"], 10);
    assert_eq!(task["cursor_next"], 14);
    assert_eq!(task["safe_head"], 100);
    assert_eq!(task["lookahead_blocks"], 3);
    assert_eq!(task["planned_start"], 60);
    assert_eq!(task["planned_end"], 62);
    assert_eq!(task["planned_query_distance"], 50);
    assert_eq!(task["no_op_reason"], serde_json::Value::Null);
}

#[tokio::test]
async fn test_api_warmup_list_exposes_idle_follow_query_reason() {
    let root = temp_storage_root("api-warmup-follow-query-idle");
    let watermarks = QueryWatermarkStore::new(LocalObjectStore::new(root.join("watermarks")));
    let source = MockSource::default();
    let service =
        service(LocalStorage::new(&root), source.clone()).with_warmup_pool(WarmupTaskPool::new(
            WarmupRuntime::new(
                source,
                LocalStorage::new(&root),
                LocalWarmupRegistry::new(LocalObjectStore::new(root.join("warmup-registry"))),
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
            },
        ));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    watermarks
        .update(&QueryWatermark {
            key: QueryWatermarkKey::new(
                "app-a",
                ethereum_identity(),
                DatasetKey::evm_logs(),
                &logs_request(10, 10).selector,
                LedgerRangeKind::Block,
            ),
            latest_block: 90,
            updated_at_unix_seconds: 1,
        })
        .expect("save watermark");

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks/ensure")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(10, 10))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 1,
                        "mode": "follow_query"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);

    let run_once = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .header("x-datalens-application", "app-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("run-once response");
    assert_eq!(run_once.status(), StatusCode::OK);
    let run_body = body_json(run_once.into_body()).await;
    assert!(run_body["results"].as_array().expect("results").is_empty());

    let list = app
        .oneshot(
            Request::get("/v1/warmup/tasks")
                .header("x-datalens-application", "app-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("list response");

    assert_eq!(list.status(), StatusCode::OK);
    let list_body = body_json(list.into_body()).await;
    let task = &list_body["tasks"].as_array().expect("tasks")[0];
    assert_eq!(task["state"], "idle");
    assert_eq!(task["no_op_reason"], "near_safe_head");
}

#[tokio::test]
async fn test_api_warmup_run_once_requires_warmup_run_operation() {
    let root = temp_storage_root("api-warmup-run-once-auth");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let registry = QueryServiceRegistry::new()
        .with_application_registry(datalens_edge::config::ApplicationRegistryConfig {
            required: true,
            applications: vec![datalens_edge::config::ApplicationConfig {
                id: "submitter".to_owned(),
                name: "submitter".to_owned(),
                enabled: true,
                display_name: None,
                token: "submit-token".to_owned(),
                chains: vec!["ethereum".to_owned()],
                datasets: vec!["evm.logs".to_owned()],
                operations: vec![datalens_edge::config::ApplicationOperationConfig::WarmupSubmit],
                quota: None,
            }],
        })
        .expect("application registry")
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let missing = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("missing auth response");
    let unauthorized = app
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .header("x-datalens-application", "submitter")
                .header("authorization", "Bearer submit-token")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("unauthorized response");

    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_api_warmup_run_task_once_runs_only_requested_task() {
    let root = temp_storage_root("api-warmup-run-task-once");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source.clone())
        .with_warmup_pool(warmup_pool_with_max_fetches(&root, 1));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let first = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(20, 21))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 20,
                        "end": 21,
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("first submit response");
    assert_eq!(first.status(), StatusCode::CREATED);

    let second = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(30, 31))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 30,
                        "end": 31,
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("second submit response");
    assert_eq!(second.status(), StatusCode::CREATED);
    let second_body = body_json(second.into_body()).await;
    let second_task_id = second_body["task_id"]
        .as_str()
        .expect("second task id")
        .to_owned();

    let run_once = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/warmup/tasks/{second_task_id}/run-once"))
                .header("x-datalens-application", "app-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("run task once response");
    assert_eq!(run_once.status(), StatusCode::OK);
    let run_body = body_json(run_once.into_body()).await;
    let results = run_body["results"].as_array().expect("results");
    assert_eq!(results.len(), 1);

    source.clear_calls();
    let first_query = app
        .clone()
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body(logs_request(20, 21))))
                .expect("request"),
        )
        .await
        .expect("first query response");
    assert_eq!(first_query.status(), StatusCode::OK);
    assert!(
        !source.calls().is_empty(),
        "untargeted task should not have produced durable coverage"
    );

    source.clear_calls();
    let second_query = app
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body(logs_request(30, 31))))
                .expect("request"),
        )
        .await
        .expect("second query response");
    assert_eq!(second_query.status(), StatusCode::OK);
    assert_eq!(
        source.calls(),
        Vec::<SourceCall>::new(),
        "targeted task should produce durable coverage"
    );
}

#[tokio::test]
async fn test_api_warmup_run_task_once_enforces_owner_and_reports_missing_task() {
    let root = temp_storage_root("api-warmup-run-task-once-auth");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let application_registry = datalens_edge::config::ApplicationRegistryConfig {
        required: true,
        applications: vec![
            application_config("app-a", "token-a"),
            application_config("app-b", "token-b"),
        ],
    };
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry)
        .expect("application registry")
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(20, 21))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 20,
                        "end": 21,
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);
    let submit_body = body_json(submit.into_body()).await;
    let task_id = submit_body["task_id"].as_str().expect("task id").to_owned();

    let forbidden = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/warmup/tasks/{task_id}/run-once"))
                .header("x-datalens-application", "app-b")
                .header("authorization", "Bearer token-b")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("forbidden response");
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let missing = app
        .oneshot(
            Request::post("/v1/warmup/tasks/warmup-missing/run-once")
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("missing response");
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    let missing_body = body_json(missing.into_body()).await;
    assert_eq!(missing_body["error"]["kind"], "invalid_input");
    assert_eq!(
        missing_body["error"]["message"],
        "warmup task warmup-missing not found"
    );
}

#[tokio::test]
async fn test_api_warmup_rejects_old_evm_logs_submit_shape() {
    let root = temp_storage_root("api-warmup-rejects-old-submit");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source).with_warmup_pool(warmup_pool(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let submit = app
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset": "logs",
                        "range": BlockRange::expect_new(20, 21),
                        "filter": evm_logs_selector_value(&logs_request(20, 21)),
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");

    assert_eq!(submit.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_api_warmup_run_once_writes_durable_coverage_that_query_hits() {
    let root = temp_storage_root("api-warmup-run-once");
    let source = MockSource::default();
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let service = service(LocalStorage::new(&root), source.clone())
        .with_metrics(recorder.clone())
        .with_warmup_pool(warmup_pool_with_metrics(&root, recorder));
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(20, 21))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 20,
                        "end": 21,
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);

    let run_once = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("run once response");
    assert_eq!(run_once.status(), StatusCode::OK);

    source.clear_calls();
    let query = app
        .clone()
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body(logs_request(20, 21))))
                .expect("request"),
        )
        .await
        .expect("query response");
    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(
        source.calls(),
        Vec::<SourceCall>::new(),
        "query should hit warmup-created durable coverage"
    );

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let body = body_text(metrics.into_body()).await;
    assert!(body.contains("datalens_warmup_task_total"));
    assert!(body.contains("datalens_warmup_fetch_total"));
    assert!(body.contains("datalens_warmup_write_total"));
}

#[tokio::test]
async fn test_api_warmup_shared_registry_runs_only_matching_chain_task() {
    let root = temp_storage_root("api-warmup-shared-registry");
    let storage = LocalStorage::new(root.join("storage"));
    let registry_store = LocalObjectStore::new(root.join("warmup-registry"));
    let ethereum_source = MockSource::default();
    let polygon_source = MockSource::default().with_chain(polygon_identity());
    let ethereum_service = service(
        LocalStorage::new(root.join("storage")),
        ethereum_source.clone(),
    )
    .with_warmup_pool(shared_registry_warmup_pool(
        storage.clone(),
        LocalWarmupRegistry::new(registry_store.clone()),
        ethereum_source.clone(),
    ));
    let polygon_service = service_named(
        LocalStorage::new(root.join("storage")),
        polygon_source.clone(),
        "polygon",
        chain_config(137),
    )
    .with_warmup_pool(shared_registry_warmup_pool(
        storage,
        LocalWarmupRegistry::new(registry_store),
        polygon_source.clone(),
    ));
    let registry = QueryServiceRegistry::new()
        .with_service(ethereum_service)
        .expect("register ethereum")
        .with_service(polygon_service)
        .expect("register polygon");
    let app = router(registry);

    let submit = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": ethereum_identity(),
                        "dataset_key": "evm.logs",
                        "selector": {
                            "kind": "evm_logs",
                            "value": evm_logs_selector_value(&logs_request(30, 31))
                        },
                        "range_kind": { "kind": "block" },
                        "start": 30,
                        "end": 31,
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");
    assert_eq!(submit.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::get("/v1/warmup/tasks")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("list response");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = body_json(list.into_body()).await;
    assert_eq!(list_body["tasks"].as_array().expect("tasks").len(), 1);

    let run_once = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("run once response");
    assert_eq!(run_once.status(), StatusCode::OK);
    assert_eq!(polygon_source.calls(), Vec::<SourceCall>::new());

    ethereum_source.clear_calls();
    let query = app
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body(logs_request(30, 31))))
                .expect("request"),
        )
        .await
        .expect("query response");
    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(ethereum_source.calls(), Vec::<SourceCall>::new());
}

#[tokio::test]
async fn test_api_warmup_native_submit_runs_solana_and_tron_tasks() {
    let root = temp_storage_root("api-warmup-native-multichain");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let solana_provider = CountingSolanaRpc::default();
    let tron_provider = CountingTronProvider::default();
    let solana = SolanaAdapter::with_provider(solana_identity(), solana_provider.clone())
        .with_max_slot_range_len(3);
    let tron = TronAdapter::with_provider(tron_identity(), tron_provider.clone())
        .with_max_block_range_len(3);
    let registry = QueryServiceRegistry::new()
        .with_service(
            QueryService::new_named(
                storage.clone(),
                solana.clone(),
                planner_config(),
                writer_config(),
                "solana-mainnet-beta",
                non_evm_chain_config("solana"),
            )
            .with_warmup_pool(warmup_pool_for(&root.join("solana"), solana)),
        )
        .expect("register solana")
        .with_service(
            QueryService::new_named(
                storage.clone(),
                tron.clone(),
                planner_config(),
                writer_config(),
                "tron",
                non_evm_chain_config("tron"),
            )
            .with_warmup_pool(warmup_pool_for(&root.join("tron"), tron)),
        )
        .expect("register tron");
    let app = router(registry.clone());

    let solana_submit = app
        .clone()
        .oneshot(native_warmup_request(serde_json::json!({
            "chain": solana_identity(),
            "dataset_key": "solana.slots",
            "selector": { "kind": "other", "value": {
                "kind": "solana_all",
                "fingerprint": "solana-all/all",
                "canonical_key": "all"
            }},
            "range_kind": { "kind": "slot" },
            "start": 10,
            "end": 12,
            "mode": "fixed_range",
            "chunk_policy": { "max_range_len": 3 }
        })))
        .await
        .expect("solana submit response");
    assert_eq!(solana_submit.status(), StatusCode::CREATED);

    let tron_submit = app
        .clone()
        .oneshot(native_warmup_request(serde_json::json!({
            "chain": tron_identity(),
            "dataset_key": "tron.blocks",
            "selector": { "kind": "other", "value": {
                "kind": "tron_all",
                "fingerprint": "tron-all/all",
                "canonical_key": "all"
            }},
            "range_kind": { "kind": "block" },
            "start": 10,
            "end": 12,
            "mode": "fixed_range"
        })))
        .await
        .expect("tron submit response");
    assert_eq!(tron_submit.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::get("/v1/warmup/tasks")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("list response");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = body_json(list.into_body()).await;
    let tasks = list_body["tasks"].as_array().expect("tasks");
    assert_eq!(tasks.len(), 2);
    assert!(tasks.iter().any(|task| {
        task["dataset_key"].as_str() == Some("solana.slots")
            && task["range_kind"] == serde_json::json!({ "kind": "slot" })
            && task["selector"]
                == serde_json::json!({
                    "kind": "solana_all",
                    "fingerprint": "solana-all/all",
                    "canonical_key": "all"
                })
            && task["start"] == 10
            && task["end"] == 12
    }));
    assert!(tasks.iter().any(|task| {
        task["dataset_key"].as_str() == Some("tron.blocks")
            && task["range_kind"] == serde_json::json!({ "kind": "block" })
            && task["selector"]
                == serde_json::json!({
                    "kind": "tron_all",
                    "fingerprint": "tron-all/all",
                    "canonical_key": "all"
                })
            && task["start"] == 10
            && task["end"] == 12
    }));

    let run_once = app
        .clone()
        .oneshot(
            Request::post("/v1/warmup/run-once")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("run once response");
    assert_eq!(run_once.status(), StatusCode::OK);

    solana_provider.fail_data_fetches();
    tron_provider.fail_data_fetches();
    registry
        .query_native(NativeQueryInput {
            chain: solana_identity(),
            dataset_key: DatasetKey::solana_slots(),
            ledger_range: LedgerRange::slots(10, 12).expect("valid range"),
            selector: solana_all_selector().expect("selector"),
            field_selection: FieldSelection::All,
            finality: QueryFinalityRequirement::DurableOnly,
        })
        .expect("solana query hits warmup cache");
    registry
        .query_native(NativeQueryInput {
            chain: tron_identity(),
            dataset_key: DatasetKey::tron_blocks(),
            ledger_range: LedgerRange::blocks(10, 12).expect("valid range"),
            selector: tron_all_selector().expect("selector"),
            field_selection: FieldSelection::All,
            finality: QueryFinalityRequirement::DurableOnly,
        })
        .expect("tron query hits warmup cache");
}

#[tokio::test]
async fn test_api_warmup_native_submit_uses_chain_neutral_application_allowlists() {
    let root = temp_storage_root("api-warmup-native-auth");
    let solana = SolanaAdapter::with_fixture_defaults();
    let registry = QueryServiceRegistry::new()
        .with_application_registry(datalens_edge::config::ApplicationRegistryConfig {
            required: true,
            applications: vec![datalens_edge::config::ApplicationConfig {
                id: "app-a".to_owned(),
                name: "app-a".to_owned(),
                enabled: true,
                display_name: None,
                token: "token-a".to_owned(),
                chains: vec!["solana-mainnet-beta".to_owned()],
                datasets: vec!["solana.slots".to_owned()],
                operations: vec![datalens_edge::config::ApplicationOperationConfig::WarmupSubmit],
                quota: None,
            }],
        })
        .expect("application registry")
        .with_service(
            QueryService::new_named(
                LocalStorage::new(&root),
                solana.clone(),
                planner_config(),
                writer_config(),
                "solana-mainnet-beta",
                non_evm_chain_config("solana"),
            )
            .with_warmup_pool(warmup_pool_for(&root.join("warmup"), solana)),
        )
        .expect("register solana");
    let app = router(registry);

    let submit = app
        .oneshot(
            Request::post("/v1/warmup/tasks")
                .header("content-type", "application/json")
                .header("x-datalens-application", "app-a")
                .header("authorization", "Bearer token-a")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "chain": solana_identity(),
                        "dataset_key": "solana.slots",
                        "selector": { "kind": "all" },
                        "range_kind": { "kind": "slot" },
                        "start": 10,
                        "end": 12,
                        "mode": "fixed_range"
                    }))
                    .expect("request json"),
                ))
                .expect("request"),
        )
        .await
        .expect("submit response");

    assert_eq!(submit.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_api_warmup_native_submit_rejects_unsupported_combinations_before_fetch() {
    let root = temp_storage_root("api-warmup-native-unsupported");
    let solana_provider = CountingSolanaRpc::default();
    let solana = SolanaAdapter::with_provider(solana_identity(), solana_provider.clone());
    let registry = QueryServiceRegistry::new()
        .with_service(
            QueryService::new_named(
                LocalStorage::new(&root),
                solana.clone(),
                planner_config(),
                writer_config(),
                "solana-mainnet-beta",
                non_evm_chain_config("solana"),
            )
            .with_warmup_pool(warmup_pool_for(&root.join("warmup"), solana)),
        )
        .expect("register solana");
    let app = router(registry);

    let submit = app
        .oneshot(native_warmup_request(serde_json::json!({
            "chain": solana_identity(),
            "dataset_key": "solana.slots",
            "selector": { "kind": "all" },
            "range_kind": { "kind": "block" },
            "start": 10,
            "end": 12,
            "mode": "fixed_range"
        })))
        .await
        .expect("submit response");

    assert_eq!(submit.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(solana_provider.data_fetch_count(), 0);
}

fn shared_registry_warmup_pool(
    storage: LocalStorage,
    registry: LocalWarmupRegistry<LocalObjectStore>,
    adapter: MockSource,
) -> WarmupTaskPool<MockSource, LocalStorage, LocalWarmupRegistry<LocalObjectStore>> {
    WarmupTaskPool::new(
        WarmupRuntime::new(
            adapter,
            storage,
            registry,
            datalens_writer::DurableWriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
        )
        .with_runtime_config(WarmupRuntimeConfig {
            max_fetches_per_task_loop: 4,
            ..WarmupRuntimeConfig::default()
        }),
        WarmupSchedulerConfig {
            max_global_concurrent_tasks: 1,
            max_concurrent_tasks_per_chain: 1,
        },
    )
}
