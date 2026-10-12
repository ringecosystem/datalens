//! HTTP API: REST and GraphQL queries, heads, subscriptions, admin, and metrics over
//! [`QueryEngine`].

mod admin;
mod auth;
mod error;
mod graphql;
mod rest;

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::routing::{get, post};
use datalens_core::Error;
use datalens_core::wire::{QueryRequest, QueryResponse};
use datalens_engine::QueryEngine;
use prometheus::Registry;

pub use auth::{APPLICATION_HEADER, Application, Quota};

use crate::auth::{Tenant, Tenants};
use crate::graphql::GqlSchema;

pub struct ApiConfig {
    pub applications: Vec<Application>,
    pub metrics_token: String,
}

pub(crate) struct Shared {
    engine: QueryEngine,
    tenants: Tenants,
    metrics_token: String,
    registry: Registry,
    schema: GqlSchema,
}

#[derive(Clone)]
pub(crate) struct AppState(Arc<Shared>);

impl AppState {
    fn new(engine: QueryEngine, registry: Registry, config: ApiConfig) -> Self {
        Self(Arc::new(Shared {
            engine,
            tenants: Tenants::new(config.applications),
            metrics_token: config.metrics_token,
            registry,
            schema: graphql::schema(),
        }))
    }
}

/// Query, GraphQL, heads, admin, and `/metrics` routes.
pub fn router(engine: QueryEngine, registry: Registry, config: ApiConfig) -> Router {
    Router::new()
        .route("/v1/query", post(rest::query))
        .route("/graphql", post(graphql::handle))
        .route("/v1/heads", get(rest::heads))
        .route("/v1/heads/{chain}", get(rest::chain_heads))
        .route("/v1/heads/{chain}/wait", get(rest::wait_heads))
        .route("/v1/watch", post(rest::watch).delete(rest::unwatch))
        .route("/v1/admin/backfill", post(admin::backfill))
        .route("/v1/admin/coverage", get(admin::coverage))
        .route("/v1/admin/jobs", get(admin::jobs))
        .route("/v1/admin/follow/resume", post(admin::resume_follow))
        .route("/metrics", get(rest::metrics))
        .with_state(AppState::new(engine, registry, config))
}

/// `/metrics` alone, for replicas without the api role.
pub fn metrics_router(engine: QueryEngine, registry: Registry, metrics_token: String) -> Router {
    let config = ApiConfig {
        applications: Vec::new(),
        metrics_token,
    };
    Router::new()
        .route("/metrics", get(rest::metrics))
        .with_state(AppState::new(engine, registry, config))
}

/// Admission, the engine query, and the query metrics. REST and GraphQL both land here.
pub(crate) async fn run_query(
    shared: &Shared,
    tenant: &Tenant,
    request: QueryRequest,
) -> Result<QueryResponse, Error> {
    let started = Instant::now();
    let chain = request.chain.to_string();
    let dataset = request.dataset.as_str();
    let result = async {
        let _permit = tenant.admit(&request)?;
        shared.engine.query(&tenant.caller(), request).await
    }
    .await;
    let outcome = match &result {
        Ok(response) if response.incomplete_reason.is_none() => "complete",
        Ok(_) => "partial",
        Err(error) => error.kind(),
    };
    shared.engine.metrics().record_query(
        &tenant.app.id,
        &chain,
        dataset,
        outcome,
        started.elapsed().as_secs_f64(),
    );
    result
}
