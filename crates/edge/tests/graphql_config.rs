mod support;

use support::graphql::*;

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
