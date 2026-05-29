use datalens_edge::{
    auth::application::{ApplicationRegistry, NoAuthentication},
    contract::{
        discovery::DiscoveryResponse,
        error::{ApiErrorBody, api_error_status},
        query::{QueryApiRequest, QueryApiResponse},
        warmup::{WarmupSubmitApiRequest, WarmupTaskView},
    },
    http::router::router,
    service::{
        lifecycle::ServiceLifecycle,
        query_service::{NativeQueryResponse, QueryService},
        registry::QueryServiceRegistry,
    },
};

#[test]
fn edge_public_boundaries_expose_stable_types() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<ApplicationRegistry>();
    assert_send_sync::<NoAuthentication>();
    assert_send_sync::<QueryServiceRegistry>();
    assert_send_sync::<ServiceLifecycle>();

    let _ = std::any::type_name::<QueryApiRequest>();
    let _ = std::any::type_name::<QueryApiResponse>();
    let _ = std::any::type_name::<NativeQueryResponse>();
    let _ = std::any::type_name::<QueryService<()>>();
    let _ = std::any::type_name::<DiscoveryResponse>();
    let _ = std::any::type_name::<WarmupSubmitApiRequest>();
    let _ = std::any::type_name::<WarmupTaskView>();
    let _ = std::any::type_name::<ApiErrorBody>();
    let _ = api_error_status;
    let _ = router;
}
