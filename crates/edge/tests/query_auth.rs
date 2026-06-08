mod support;

use datalens_edge::{
    auth::ApplicationRegistry,
    config::{EdgeConfig, MetricsEndpointConfig},
    router_with_edge_config,
};
use support::query::*;

#[tokio::test]
async fn test_registered_application_query_uses_native_dataset_metrics_label() {
    let storage = LocalStorage::new(temp_storage_root("app-auth-metrics"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![application(
            "Indexer_App",
            true,
            "secret-token",
            vec!["ethereum"],
            vec!["evm.blocks"],
            None,
        )]))
        .expect("application registry")
        .with_service(service(storage, source))
        .expect("register service");
    let app = router_with_edge_config(
        registry,
        EdgeConfig {
            metrics: MetricsEndpointConfig {
                public: true,
                bearer_token: None,
            },
            ..Default::default()
        },
    );

    let response = app
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some(" indexer_app "),
            Some("Bearer secret-token"),
        ))
        .await
        .expect("query response");

    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method(axum::http::Method::GET)
                .uri("/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let text = std::str::from_utf8(&body).expect("utf8 metrics");

    assert!(text.contains(
        r#"datalens_query_total{application="indexer_app",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="filled"} 1"#
    ));
    assert!(text.contains(
        r#"datalens_edge_request_total{application="indexer_app",outcome="accepted"} 1"#
    ));
    assert!(!text.contains("Indexer_App"));
}

#[tokio::test]
async fn test_operation_allowlist_does_not_let_query_credentials_read_discovery() {
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![ApplicationConfig {
            id: "query-only".to_owned(),
            name: "query-only".to_owned(),
            enabled: true,
            display_name: None,
            token: "secret-token".to_owned(),
            chains: vec!["ethereum".to_owned()],
            datasets: vec!["evm.blocks".to_owned()],
            operations: vec![ApplicationOperationConfig::Query],
            quota: None,
        }]))
        .expect("application registry")
        .with_service(service(
            LocalStorage::new(temp_storage_root("app-operation-discovery")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router(registry);

    let query = app
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("query-only"),
            Some("Bearer secret-token"),
        ))
        .await
        .expect("query response");
    let discovery = app
        .oneshot(
            Request::get("/v1/discovery")
                .header("x-datalens-application", "query-only")
                .header("authorization", "Bearer secret-token")
                .body(Body::empty())
                .expect("discovery request"),
        )
        .await
        .expect("discovery response");

    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(discovery.status(), StatusCode::FORBIDDEN);
}

#[test]
fn test_application_registry_rejects_empty_operations_when_required() {
    let error = ApplicationRegistry::from_config(ApplicationRegistryConfig {
        required: true,
        applications: vec![ApplicationConfig {
            id: "empty-ops".to_owned(),
            name: "empty-ops".to_owned(),
            enabled: true,
            display_name: None,
            token: "secret-token".to_owned(),
            chains: vec!["ethereum".to_owned()],
            datasets: vec!["evm.blocks".to_owned()],
            operations: Vec::new(),
            quota: None,
        }],
    })
    .expect_err("empty operation allowlist rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(
        error
            .message
            .contains("must declare at least one operation when application auth is required")
    );
}

#[tokio::test]
async fn test_max_requests_per_minute_is_enforced_before_provider_fetch() {
    let root = temp_storage_root("app-rate-limit");
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![application(
            "limited",
            true,
            "secret-token",
            vec!["ethereum"],
            vec!["evm.blocks"],
            Some(ApplicationQuotaConfig {
                max_query_range_blocks: None,
                max_hot_query_range_blocks: None,
                max_requests_per_minute: Some(1),
                max_concurrent_requests: None,
            }),
        )]))
        .expect("application registry")
        .with_service(service(LocalStorage::new(&root), source.clone()))
        .expect("register service");
    let app = router(registry);

    let first = app
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("limited"),
            Some("Bearer secret-token"),
        ))
        .await
        .expect("first response");
    let second = app
        .oneshot(query_http_request(
            blocks_request(11, 11),
            Some("limited"),
            Some("Bearer secret-token"),
        ))
        .await
        .expect("second response");

    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after = second
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .expect("retry after header")
        .to_str()
        .expect("retry after value")
        .to_owned();
    let body: serde_json::Value = serde_json::from_slice(
        &to_bytes(second.into_body(), usize::MAX)
            .await
            .expect("rate limit body"),
    )
    .expect("rate limit json");
    assert_eq!(body["error"]["kind"], "rate_limited");
    assert_eq!(
        body["error"]["message"],
        "application request rate quota exceeded"
    );
    assert_eq!(body["error"]["quota"]["kind"], "request_rate_limit");
    assert_eq!(body["error"]["quota"]["scope"], "application");
    assert_eq!(body["error"]["quota"]["limit"], 1);
    assert_eq!(body["error"]["quota"]["observed"], 1);
    let retry_after_seconds = body["error"]["quota"]["retry_after_seconds"]
        .as_u64()
        .expect("retry after seconds");
    assert!((1..=60).contains(&retry_after_seconds));
    assert_eq!(retry_after, retry_after_seconds.to_string());
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
}

#[tokio::test]
async fn test_max_concurrent_requests_is_enforced_while_request_is_in_flight() {
    let source = MockSource::default()
        .with_blocks(vec![block(10, "0x10"), block(11, "0x11")])
        .with_fetch_delay(std::time::Duration::from_millis(200));
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![application(
            "limited",
            true,
            "secret-token",
            vec!["ethereum"],
            vec!["evm.blocks"],
            Some(ApplicationQuotaConfig {
                max_query_range_blocks: None,
                max_hot_query_range_blocks: None,
                max_requests_per_minute: None,
                max_concurrent_requests: Some(1),
            }),
        )]))
        .expect("application registry")
        .with_service(service(
            LocalStorage::new(temp_storage_root("app-concurrency-limit")),
            source.clone(),
        ))
        .expect("register service");
    let app = router(registry);

    let first_app = app.clone();
    let first = tokio::spawn(async move {
        first_app
            .oneshot(query_http_request(
                blocks_request(10, 10),
                Some("limited"),
                Some("Bearer secret-token"),
            ))
            .await
            .expect("first response")
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let second = app
        .oneshot(query_http_request(
            blocks_request(11, 11),
            Some("limited"),
            Some("Bearer secret-token"),
        ))
        .await
        .expect("second response");
    let first = first.await.expect("first task");

    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        second
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none()
    );
    let body: serde_json::Value = serde_json::from_slice(
        &to_bytes(second.into_body(), usize::MAX)
            .await
            .expect("concurrent limit body"),
    )
    .expect("concurrent limit json");
    assert_eq!(body["error"]["quota"]["kind"], "concurrent_limit");
    assert_eq!(body["error"]["quota"]["scope"], "application");
    assert_eq!(body["error"]["quota"]["limit"], 1);
    assert_eq!(body["error"]["quota"]["observed"], 1);
    assert_eq!(
        body["error"]["quota"]["retry_after_seconds"],
        serde_json::Value::Null
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
}

#[tokio::test]
async fn test_metrics_route_requires_operator_token_when_application_auth_is_required() {
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![application(
            "query-app",
            true,
            "query-token",
            vec!["ethereum"],
            vec!["evm.blocks"],
            None,
        )]))
        .expect("application registry")
        .with_service(service(
            LocalStorage::new(temp_storage_root("metrics-policy")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router_with_edge_config(
        registry,
        EdgeConfig {
            metrics: MetricsEndpointConfig {
                public: false,
                bearer_token: Some("metrics-token".to_owned()),
            },
            ..Default::default()
        },
    );

    let application_credentials = app
        .clone()
        .oneshot(
            Request::get("/metrics")
                .header("x-datalens-application", "query-app")
                .header("authorization", "Bearer query-token")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("application metrics response");
    let operator_credentials = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", "Bearer metrics-token")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("operator metrics response");

    assert_eq!(application_credentials.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(operator_credentials.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_missing_invalid_and_disabled_application_are_rejected_before_fetch_or_cache_write() {
    let root = temp_storage_root("app-auth-rejects");
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![
            application(
                "indexer",
                true,
                "secret-token",
                vec!["ethereum"],
                vec!["evm.blocks"],
                None,
            ),
            application(
                "disabled",
                false,
                "disabled-token",
                vec!["ethereum"],
                vec!["evm.blocks"],
                None,
            ),
        ]))
        .expect("application registry")
        .with_service(service(LocalStorage::new(&root), source.clone()))
        .expect("register service");
    let app = router(registry);

    let missing = app
        .clone()
        .oneshot(query_http_request(blocks_request(10, 10), None, None))
        .await
        .expect("missing app response");
    let invalid = app
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("indexer"),
            Some("Bearer wrong-token"),
        ))
        .await
        .expect("invalid token response");
    let disabled = app
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("disabled"),
            Some("Bearer disabled-token"),
        ))
        .await
        .expect("disabled app response");

    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(disabled.status(), StatusCode::FORBIDDEN);
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );

    let invalid_body = to_bytes(invalid.into_body(), usize::MAX)
        .await
        .expect("invalid error body");
    let invalid_text = std::str::from_utf8(&invalid_body).expect("utf8 body");
    assert!(!invalid_text.contains("wrong-token"));
}

#[tokio::test]
async fn test_application_allowlist_and_quota_rejections_happen_before_fetch_or_cache_write() {
    let root = temp_storage_root("app-authz-quota");
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![
            application(
                "logs-only",
                true,
                "secret-token",
                vec!["ethereum"],
                vec!["evm.logs"],
                Some(ApplicationQuotaConfig {
                    max_query_range_blocks: Some(1),
                    max_hot_query_range_blocks: None,
                    max_requests_per_minute: Some(60),
                    max_concurrent_requests: Some(1),
                }),
            ),
            application(
                "hot-logs",
                true,
                "hot-token",
                vec!["ethereum"],
                vec!["evm.logs"],
                Some(ApplicationQuotaConfig {
                    max_query_range_blocks: Some(4),
                    max_hot_query_range_blocks: Some(1),
                    max_requests_per_minute: Some(60),
                    max_concurrent_requests: Some(1),
                }),
            ),
        ]))
        .expect("application registry")
        .with_service(service(LocalStorage::new(&root), source.clone()))
        .expect("register service");
    let app = router(registry);

    let unauthorized_dataset = app
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("logs-only"),
            Some("Bearer secret-token"),
        ))
        .await
        .expect("unauthorized dataset response");
    let quota_limited = app
        .clone()
        .oneshot(query_http_request(
            logs_request(10, 11, vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]),
            Some("logs-only"),
            Some("Bearer secret-token"),
        ))
        .await
        .expect("quota response");
    let mut hot_request = logs_request(10, 11, vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]);
    hot_request.finality = QueryFinalityRequirement::SafeToLatest;
    let hot_quota_limited = app
        .oneshot(query_http_request(
            hot_request,
            Some("hot-logs"),
            Some("Bearer hot-token"),
        ))
        .await
        .expect("hot quota response");

    assert_eq!(unauthorized_dataset.status(), StatusCode::FORBIDDEN);
    assert_eq!(quota_limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(hot_quota_limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        quota_limited
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none()
    );
    assert!(
        hot_quota_limited
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none()
    );
    let quota_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(quota_limited.into_body(), usize::MAX)
            .await
            .expect("quota body"),
    )
    .expect("quota json");
    let hot_quota_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(hot_quota_limited.into_body(), usize::MAX)
            .await
            .expect("hot quota body"),
    )
    .expect("hot quota json");
    assert_eq!(quota_body["error"]["kind"], "rate_limited");
    assert_eq!(quota_body["error"]["quota"]["kind"], "range_limit");
    assert_eq!(quota_body["error"]["quota"]["scope"], "application");
    assert_eq!(quota_body["error"]["quota"]["limit"], 1);
    assert_eq!(quota_body["error"]["quota"]["requested"], 2);
    assert_eq!(
        quota_body["error"]["quota"]["retry_after_seconds"],
        serde_json::Value::Null
    );
    assert_eq!(hot_quota_body["error"]["quota"]["kind"], "hot_range_limit");
    assert_eq!(hot_quota_body["error"]["quota"]["scope"], "application");
    assert_eq!(hot_quota_body["error"]["quota"]["limit"], 1);
    assert_eq!(hot_quota_body["error"]["quota"]["requested"], 2);
    assert_eq!(
        hot_quota_body["error"]["quota"]["retry_after_seconds"],
        serde_json::Value::Null
    );
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
}

#[tokio::test]
async fn test_application_identity_does_not_partition_durable_cache_key() {
    let root = temp_storage_root("app-shared-cache");
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![
            application(
                "app-a",
                true,
                "token-a",
                vec!["ethereum"],
                vec!["evm.blocks"],
                None,
            ),
            application(
                "app-b",
                true,
                "token-b",
                vec!["ethereum"],
                vec!["evm.blocks"],
                None,
            ),
        ]))
        .expect("application registry")
        .with_service(service(LocalStorage::new(&root), source.clone()))
        .expect("register service");
    let app = router(registry.clone());

    let first = app
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("app-a"),
            Some("Bearer token-a"),
        ))
        .await
        .expect("first response");
    assert_eq!(first.status(), StatusCode::OK);
    registry
        .wait_for_durable_promotions()
        .expect("promotion drain");
    let second = app
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("app-b"),
            Some("Bearer token-b"),
        ))
        .await
        .expect("second response");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("applications").exists());
}
