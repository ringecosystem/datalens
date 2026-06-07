mod support;

use support::query::*;

#[test]
fn test_provider_limit_error_is_classified() {
    let source = MockSource::default().with_error(DatalensErrorKind::ProviderLimit);
    let root = temp_storage_root("provider-limit");
    let service = service(LocalStorage::new(&root), source);
    let error = service
        .query_native(logs_request(
            1,
            2,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect_err("provider limit");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
}

#[tokio::test]
async fn test_metrics_route_returns_prometheus_text_for_query_path() {
    let storage = LocalStorage::new(temp_storage_root("metrics-route"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let service = service(storage, source);
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("register metrics service");
    let router = datalens_edge::router(registry);

    let response = router
        .clone()
        .oneshot(query_http_request(blocks_request(10, 10), None, None))
        .await
        .expect("query response");

    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .method(axum::http::Method::GET)
                .uri("/metrics")
                .body(axum::body::Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert_eq!(
        response.headers().get(axum::http::header::CONTENT_TYPE),
        Some(&axum::http::HeaderValue::from_static(
            "text/plain; version=0.0.4"
        ))
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let text = std::str::from_utf8(&body).expect("utf8 metrics");
    assert!(text.contains(
        r#"datalens_query_total{application="datalens",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="filled"} 1"#
    ));
    assert!(text.contains(
        r#"datalens_cache_coverage_total{application="datalens",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="miss"} 1"#
    ));
    assert!(text.contains(
        r#"datalens_application_chain_latest_requested_block{application="datalens",chain="ethereum",chain_kind="evm",dataset="evm.blocks"} 10"#
    ));
}

#[tokio::test]
async fn test_query_route_uses_application_identity_header_for_metrics() {
    let storage = LocalStorage::new(temp_storage_root("metrics-application-header"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let service = service(storage, source);
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("register metrics service");
    let router = datalens_edge::router(registry);

    let response = router
        .clone()
        .oneshot(query_http_request(
            blocks_request(10, 10),
            Some("wallet-search"),
            None,
        ))
        .await
        .expect("query response");

    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .method(axum::http::Method::GET)
                .uri("/metrics")
                .body(axum::body::Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let text = std::str::from_utf8(&body).expect("utf8 metrics");
    assert!(text.contains(
        r#"datalens_query_total{application="wallet-search",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="filled"} 1"#
    ));
}

#[test]
fn test_metrics_config_can_disable_recorder_initialization() {
    let storage = LocalStorage::new(temp_storage_root("metrics-disabled"));
    let source = MockSource::default().with_blocks(vec![block(1, "0x01")]);
    let service = QueryService::new_with_metrics_config(
        storage,
        source,
        PlannerConfig {
            max_query_range_blocks: 4,
            default_chunk_range_blocks: 2,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "ethereum",
        chain_config(1),
        MetricsConfig {
            enabled: false,
            default_application: "disabled".to_owned(),
        },
    )
    .expect("disabled metrics service builds");

    let response = service
        .query_native(blocks_request(1, 1))
        .expect("query succeeds");

    assert_eq!(block_numbers(&response), vec![1]);
    assert!(service.metrics_text().is_none());
}
