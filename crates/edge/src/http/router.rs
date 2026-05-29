use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    config,
    http::{
        AppState,
        handlers::{
            chains, discovery, health, metrics, query, warmup_cancel, warmup_get, warmup_list,
            warmup_pause, warmup_retry, warmup_run_once, warmup_submit,
        },
    },
    service::registry::QueryServiceRegistry,
};

pub fn router(registry: QueryServiceRegistry) -> Router {
    router_with_edge_config(registry, config::EdgeConfig::default())
}

pub fn router_with_edge_config(registry: QueryServiceRegistry, edge: config::EdgeConfig) -> Router {
    let graphql_schema = edge
        .graphql
        .enabled
        .then(|| crate::graphql::schema(registry.clone()));
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/chains", get(chains))
        .route("/v1/discovery", get(discovery))
        .route("/v1/query", post(query))
        .route("/v1/warmup/tasks", post(warmup_submit).get(warmup_list))
        .route("/v1/warmup/tasks/{task_id}", get(warmup_get))
        .route("/v1/warmup/tasks/{task_id}/pause", post(warmup_pause))
        .route("/v1/warmup/tasks/{task_id}/cancel", post(warmup_cancel))
        .route("/v1/warmup/tasks/{task_id}/retry", post(warmup_retry))
        .route("/v1/warmup/run-once", post(warmup_run_once));

    if graphql_schema.is_some() {
        router = router.route("/graphql", post(crate::graphql::graphql_handler));
    }
    if edge.graphql.enabled && edge.graphql.playground_enabled {
        router = router.route("/graphql/playground", get(crate::graphql::playground));
    }
    router.with_state(AppState {
        registry,
        graphql_schema,
        edge,
    })
}
