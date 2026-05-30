mod support;

use support::graphql::*;

#[tokio::test]
async fn test_graphql_routes_are_disabled_by_default() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-default-disabled")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router(registry);

    let response = app
        .clone()
        .oneshot(
            Request::post("/native/graphql")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"{ chains }"}"#))
                .expect("request"),
        )
        .await
        .expect("graphql response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            Request::get("/native/graphiql")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("playground response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .oneshot(
            Request::get("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("health response");
    assert_eq!(response.status(), StatusCode::OK);
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
            query: QueryConfig {
                native: GraphqlSurfaceConfig {
                    graphql_enabled: true,
                    playground_enabled: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let disabled = router_with_edge_config(
        registry,
        EdgeConfig {
            query: QueryConfig {
                native: GraphqlSurfaceConfig {
                    graphql_enabled: true,
                    playground_enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let response = enabled
        .oneshot(
            Request::get("/native/graphiql")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("enabled response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        body_text(response.into_body())
            .await
            .contains("/native/graphql")
    );

    let response = disabled
        .oneshot(
            Request::get("/native/graphiql")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("disabled response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_native_graphql_uses_configured_paths() {
    let registry = QueryServiceRegistry::new()
        .with_service(service(
            LocalStorage::new(temp_storage_root("gql-native-paths")),
            MockSource::default(),
        ))
        .expect("register service");
    let app = router_with_edge_config(
        registry,
        EdgeConfig {
            query: QueryConfig {
                native: GraphqlSurfaceConfig {
                    graphql_enabled: true,
                    path: "/native/custom".to_owned(),
                    playground_enabled: true,
                    playground_path: "/native/custom-ui".to_owned(),
                },
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let response = app
        .clone()
        .oneshot(
            Request::post("/native/custom")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"{ chains }"}"#))
                .expect("request"),
        )
        .await
        .expect("graphql response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::get("/native/custom-ui")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("playground response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        body_text(response.into_body())
            .await
            .contains("/native/custom")
    );
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
            query: QueryConfig {
                native: GraphqlSurfaceConfig {
                    graphql_enabled: false,
                    playground_enabled: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let response = app
        .oneshot(
            Request::post("/native/graphql")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"{ chains }"}"#))
                .expect("request"),
        )
        .await
        .expect("disabled response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
