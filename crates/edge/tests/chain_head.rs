mod support;

use support::query::*;

#[tokio::test]
async fn test_chain_head_returns_latest_safe_and_finalized_heights() {
    let source = MockSource::default()
        .with_latest_height(123)
        .with_safe_height(120, FinalityKind::Safe)
        .with_finalized_height(118);
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("chain-head-finality")),
            source,
        ))
        .expect("register service");
    let app = router(registry);

    let latest = get_head(app.clone(), "/v1/chains/ethereum/head").await;
    let safe = get_head(app.clone(), "/v1/chains/ethereum/head?finality=safe").await;
    let finalized = get_head(app, "/v1/chains/ethereum/head?finality=finalized").await;

    assert_eq!(latest["chain"]["configured_name"], "ethereum");
    assert_eq!(latest["height"], 123);
    assert_eq!(latest["finality"], "latest");
    assert_eq!(latest["range_kind"], "block");
    assert!(latest.get("timestamp").is_none());

    assert_eq!(safe["height"], 120);
    assert_eq!(safe["finality"], "safe");
    assert_eq!(safe["range_kind"], "block");

    assert_eq!(finalized["height"], 118);
    assert_eq!(finalized["finality"], "finalized");
    assert_eq!(finalized["range_kind"], "block");
}

#[tokio::test]
async fn test_chain_head_resolves_numeric_evm_chain_id() {
    let source = MockSource::default().with_latest_height(456);
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("chain-head-chain-id")),
            source,
            "ethereum",
            1,
        ))
        .expect("register service");
    let app = router(registry);

    let body = get_head(app, "/v1/chains/1/head?finality=latest").await;

    assert_eq!(body["chain"]["configured_name"], "ethereum");
    assert_eq!(body["height"], 456);
    assert_eq!(body["finality"], "latest");
}

#[tokio::test]
async fn test_chain_head_requires_discovery_authorization_without_query_range_quota() {
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![ApplicationConfig {
            id: "head-reader".to_owned(),
            name: "head-reader".to_owned(),
            enabled: true,
            display_name: None,
            token: "secret-token".to_owned(),
            chains: vec!["ethereum".to_owned()],
            datasets: vec!["evm.logs".to_owned()],
            operations: vec![ApplicationOperationConfig::Discovery],
            quota: Some(ApplicationQuotaConfig {
                max_query_range_blocks: Some(1),
                max_hot_query_range_blocks: Some(1),
                max_requests_per_minute: None,
                max_concurrent_requests: None,
            }),
        }]))
        .expect("application registry")
        .with_service(service(
            LocalStorage::new(temp_storage_root("chain-head-auth")),
            MockSource::default().with_latest_height(789),
        ))
        .expect("register service");
    let app = router(registry);

    let missing_auth = app
        .clone()
        .oneshot(
            Request::get("/v1/chains/ethereum/head")
                .body(Body::empty())
                .expect("head request"),
        )
        .await
        .expect("missing auth response");
    let authorized = app
        .oneshot(
            Request::get("/v1/chains/ethereum/head")
                .header("x-datalens-application", "head-reader")
                .header("authorization", "Bearer secret-token")
                .body(Body::empty())
                .expect("head request"),
        )
        .await
        .expect("authorized response");

    assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(authorized.status(), StatusCode::OK);
    let body = response_json(authorized).await;
    assert_eq!(body["height"], 789);
}

#[tokio::test]
async fn test_chain_head_missing_auth_does_not_reveal_unconfigured_chain() {
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![application(
            "head-reader",
            true,
            "secret-token",
            vec!["ethereum"],
            vec!["evm.logs"],
            None,
        )]))
        .expect("application registry")
        .with_service(service(
            LocalStorage::new(temp_storage_root("chain-head-missing-auth-unknown")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router(registry);

    let response = app
        .oneshot(
            Request::get("/v1/chains/polygon/head")
                .body(Body::empty())
                .expect("head request"),
        )
        .await
        .expect("head response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(response).await["error"]["kind"],
        "authentication_failed"
    );
}

#[tokio::test]
async fn test_chain_head_authorizes_configured_chain_allowlist() {
    let registry = QueryServiceRegistry::new()
        .with_application_registry(application_registry(vec![ApplicationConfig {
            id: "ethereum-reader".to_owned(),
            name: "ethereum-reader".to_owned(),
            enabled: true,
            display_name: None,
            token: "secret-token".to_owned(),
            chains: vec!["ethereum".to_owned()],
            datasets: vec!["evm.logs".to_owned()],
            operations: vec![ApplicationOperationConfig::Discovery],
            quota: None,
        }]))
        .expect("application registry")
        .with_service(service(
            LocalStorage::new(temp_storage_root("chain-head-auth-ethereum")),
            MockSource::default(),
        ))
        .expect("register ethereum service")
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("chain-head-auth-polygon")),
            MockSource::default()
                .with_chain(polygon_identity())
                .with_latest_height(456),
            "polygon",
            137,
        ))
        .expect("register polygon service");
    let app = router(registry);

    let response = app
        .oneshot(
            Request::get("/v1/chains/polygon/head")
                .header("x-datalens-application", "ethereum-reader")
                .header("authorization", "Bearer secret-token")
                .body(Body::empty())
                .expect("head request"),
        )
        .await
        .expect("head response");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response_json(response).await["error"]["kind"],
        "unauthorized"
    );
}

#[tokio::test]
async fn test_chain_head_rejects_unsupported_chain_and_finality() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("chain-head-errors")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router(registry);

    let chain = app
        .clone()
        .oneshot(
            Request::get("/v1/chains/polygon/head")
                .body(Body::empty())
                .expect("head request"),
        )
        .await
        .expect("unsupported chain response");
    let finality = app
        .oneshot(
            Request::get("/v1/chains/ethereum/head?finality=archive")
                .body(Body::empty())
                .expect("head request"),
        )
        .await
        .expect("unsupported finality response");

    assert_eq!(chain.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(chain).await["error"]["kind"],
        "unsupported_dataset"
    );
    assert_eq!(finality.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_json(finality).await["error"]["kind"],
        "invalid_input"
    );
}

async fn get_head(app: axum::Router, uri: &str) -> serde_json::Value {
    let response = app
        .oneshot(Request::get(uri).body(Body::empty()).expect("head request"))
        .await
        .expect("head response");
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&body).expect("json body")
}
