mod support;

use support::query::*;

#[tokio::test]
async fn test_registered_application_query_uses_normalized_metrics_label() {
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
    let app = router(registry);

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
        r#"datalens_query_total{application="indexer_app",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="filled"} 1"#
    ));
    assert!(!text.contains("Indexer_App"));
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
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
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
    let app = router(registry);

    let first = app
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("app-a"),
            Some("Bearer token-a"),
        ))
        .await
        .expect("first response");
    let second = app
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("app-b"),
            Some("Bearer token-b"),
        ))
        .await
        .expect("second response");

    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
    assert!(root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(!root.join("applications").exists());
}
