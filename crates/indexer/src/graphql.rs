use std::sync::Arc;

use async_graphql::{EmptyMutation, EmptySubscription, Schema, dynamic::Schema as DynamicSchema};
use axum::Router;

pub use datalens_metrics::IndexerGraphqlMetricLabels;

use crate::{GraphqlViewConfig, IndexerError, QueryAuthConfig, QueryableStore};

mod auth;
mod query;
mod router;
mod views;

pub use query::{
    DecodedEvent, DecodedEventConnection, DecodedEventEdge, EventPageInfo, IndexedEvent,
    IndexedEventConnection, IndexedEventEdge, QueryRoot,
};
pub use router::{IndexerGraphqlMetrics, MetricsEndpointConfig};

pub const DEFAULT_EVENT_LIMIT: u64 = 100;
pub const MAX_EVENT_LIMIT: u64 = 1000;

pub type IndexerGraphqlSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;
pub type IndexerGraphqlDynamicSchema = DynamicSchema;
pub(super) type SharedStore = Arc<dyn QueryableStore>;

pub fn graphql_schema(store: SharedStore) -> IndexerGraphqlSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(store)
        .finish()
}

pub fn index_graphql_schema_sdl() -> String {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .finish()
        .sdl()
}

pub fn graphql_schema_with_views(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
) -> Result<IndexerGraphqlDynamicSchema, IndexerError> {
    views::build_dynamic_schema(store, views)
}

pub fn graphql_router(store: SharedStore, path: &str, playground: bool) -> Router {
    router::graphql_router_internal(store, path, playground, None, None)
}

pub fn graphql_router_with_metrics(
    store: SharedStore,
    path: &str,
    playground: bool,
    metrics: IndexerGraphqlMetrics,
) -> Router {
    router::graphql_router_internal(store, path, playground, Some(metrics), None)
}

pub fn graphql_router_with_auth(
    store: SharedStore,
    path: &str,
    playground: bool,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Router {
    router::graphql_router_with_auth_path(store, path, playground, None, auth, metrics)
}

pub fn graphql_router_with_auth_path(
    store: SharedStore,
    path: &str,
    playground: bool,
    playground_path: Option<&str>,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Router {
    router::graphql_router_with_auth_path(store, path, playground, playground_path, auth, metrics)
}

pub fn graphql_router_with_views_auth(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
    path: &str,
    playground: bool,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Result<Router, IndexerError> {
    router::graphql_router_with_views_auth_path(store, views, path, playground, None, auth, metrics)
}

pub fn graphql_router_with_views_auth_path(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
    path: &str,
    playground: bool,
    playground_path: Option<&str>,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Result<Router, IndexerError> {
    router::graphql_router_with_views_auth_path(
        store,
        views,
        path,
        playground,
        playground_path,
        auth,
        metrics,
    )
}
