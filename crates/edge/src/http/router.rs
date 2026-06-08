use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    config,
    http::{
        AppState,
        handlers::{
            chain_head, chains, discovery, health, metrics, query, warmup_cancel, warmup_ensure,
            warmup_get, warmup_list, warmup_pause, warmup_retry, warmup_run_once, warmup_submit,
        },
    },
    service::registry::QueryServiceRegistry,
};

pub fn router(registry: QueryServiceRegistry) -> Router {
    router_with_edge_config(registry, config::EdgeConfig::default())
}

pub fn router_with_edge_config(registry: QueryServiceRegistry, edge: config::EdgeConfig) -> Router {
    let native_graphql_schema = edge
        .query
        .native
        .graphql_enabled
        .then(|| crate::graphql::schema(registry.clone()));
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/chains", get(chains))
        .route("/v1/chains/{chain}/head", get(chain_head))
        .route("/v1/discovery", get(discovery))
        .route("/v1/query", post(query))
        .route("/v1/warmup/tasks", post(warmup_submit).get(warmup_list))
        .route("/v1/warmup/tasks/ensure", post(warmup_ensure))
        .route("/v1/warmup/tasks/{task_id}", get(warmup_get))
        .route("/v1/warmup/tasks/{task_id}/pause", post(warmup_pause))
        .route("/v1/warmup/tasks/{task_id}/cancel", post(warmup_cancel))
        .route("/v1/warmup/tasks/{task_id}/retry", post(warmup_retry))
        .route("/v1/warmup/run-once", post(warmup_run_once));

    if native_graphql_schema.is_some() {
        router = router.route(
            &edge.query.native.path,
            post(crate::graphql::graphql_handler),
        );
    }
    if edge.query.native.graphql_enabled && edge.query.native.playground_enabled {
        router = router.route(
            &edge.query.native.playground_path,
            get(crate::graphql::playground),
        );
    }
    router.with_state(AppState {
        registry,
        native_graphql_schema,
        edge,
    })
}
