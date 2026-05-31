use std::time::Instant;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_metrics::{IndexerGraphqlMetricLabels, IndexerGraphqlQueryOutcome, MetricsRecorder};

use crate::{GraphqlViewConfig, IndexerError, QueryAuthConfig};

use super::{
    IndexerGraphqlDynamicSchema, IndexerGraphqlSchema, SharedStore,
    auth::{QueryApplicationPermit, QueryAuthRegistry, bearer_token},
    graphql_schema, graphql_schema_with_views,
    query::graphql_error_code,
};

pub(super) fn graphql_router_internal(
    store: SharedStore,
    path: &str,
    playground: bool,
    metrics: Option<IndexerGraphqlMetrics>,
    playground_path: Option<&str>,
) -> Router {
    graphql_router_with_auth_path(
        store,
        path,
        playground,
        playground_path,
        QueryAuthConfig::default(),
        metrics,
    )
}

pub(super) fn graphql_router_with_auth_path(
    store: SharedStore,
    path: &str,
    playground: bool,
    playground_path: Option<&str>,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Router {
    let schema = graphql_schema(store);
    let mut router = Router::new().route(path, post(graphql_handler));
    if playground {
        let path = playground_path
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{path}/playground"));
        router = router.route(&path, get(playground_handler));
    }
    if let Some(endpoint) = metrics
        .as_ref()
        .and_then(|metrics| metrics.endpoint.as_ref())
    {
        router = router.route(&endpoint.path, get(metrics_handler));
    }
    router.with_state(GraphqlState {
        schema,
        endpoint: path.to_owned(),
        metrics,
        auth: QueryAuthRegistry::new(auth),
    })
}

pub(super) fn graphql_router_with_views_auth_path(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
    path: &str,
    playground: bool,
    playground_path: Option<&str>,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Result<Router, IndexerError> {
    let schema = graphql_schema_with_views(store, views)?;
    let mut router = Router::new().route(path, post(graphql_dynamic_handler));
    if playground {
        let path = playground_path
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{path}/playground"));
        router = router.route(&path, get(playground_dynamic_handler));
    }
    if let Some(endpoint) = metrics
        .as_ref()
        .and_then(|metrics| metrics.endpoint.as_ref())
    {
        router = router.route(&endpoint.path, get(metrics_dynamic_handler));
    }
    Ok(router.with_state(DynamicGraphqlState {
        schema,
        endpoint: path.to_owned(),
        metrics,
        auth: QueryAuthRegistry::new(auth),
    }))
}

#[derive(Clone)]
struct GraphqlState {
    schema: IndexerGraphqlSchema,
    endpoint: String,
    metrics: Option<IndexerGraphqlMetrics>,
    auth: QueryAuthRegistry,
}

#[derive(Clone)]
struct DynamicGraphqlState {
    schema: IndexerGraphqlDynamicSchema,
    endpoint: String,
    metrics: Option<IndexerGraphqlMetrics>,
    auth: QueryAuthRegistry,
}

#[derive(Clone)]
pub struct IndexerGraphqlMetrics {
    pub recorder: MetricsRecorder,
    pub labels: IndexerGraphqlMetricLabels,
    pub endpoint: Option<MetricsEndpointConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsEndpointConfig {
    pub path: String,
    pub bearer_token: Option<String>,
}

async fn graphql_handler(
    State(state): State<GraphqlState>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> Response {
    let started = Instant::now();
    let application = match state.auth.authenticate(&headers) {
        Ok(application) => application,
        Err(error) => {
            record_auth_error(&state, &error);
            return auth_error_response(error);
        }
    };
    let response = state.schema.execute(request.into_inner()).await;
    if let Some(metrics) = &state.metrics {
        let outcome = if response.errors.is_empty() {
            IndexerGraphqlQueryOutcome::Success
        } else {
            IndexerGraphqlQueryOutcome::Error
        };
        let labels = request_labels(metrics, application.as_ref());
        metrics
            .recorder
            .record_indexer_graphql_query(&labels, outcome);
        metrics
            .recorder
            .observe_indexer_graphql_query_duration(&labels, started.elapsed().as_secs_f64());
    }
    GraphQLResponse::from(response).into_response()
}

async fn graphql_dynamic_handler(
    State(state): State<DynamicGraphqlState>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> Response {
    let started = Instant::now();
    let application = match state.auth.authenticate(&headers) {
        Ok(application) => application,
        Err(error) => {
            record_dynamic_auth_error(&state, &error);
            return auth_error_response(error);
        }
    };
    let response = state.schema.execute(request.into_inner()).await;
    if let Some(metrics) = &state.metrics {
        let outcome = if response.errors.is_empty() {
            IndexerGraphqlQueryOutcome::Success
        } else {
            IndexerGraphqlQueryOutcome::Error
        };
        let labels = request_labels(metrics, application.as_ref());
        metrics
            .recorder
            .record_indexer_graphql_query(&labels, outcome);
        metrics
            .recorder
            .observe_indexer_graphql_query_duration(&labels, started.elapsed().as_secs_f64());
    }
    GraphQLResponse::from(response).into_response()
}

async fn playground_handler(State(state): State<GraphqlState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.auth.authenticate(&headers) {
        record_auth_error(&state, &error);
        return auth_error_response(error);
    }
    Html(GraphiQLSource::build().endpoint(&state.endpoint).finish()).into_response()
}

async fn playground_dynamic_handler(
    State(state): State<DynamicGraphqlState>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = state.auth.authenticate(&headers) {
        record_dynamic_auth_error(&state, &error);
        return auth_error_response(error);
    }
    Html(GraphiQLSource::build().endpoint(&state.endpoint).finish()).into_response()
}

async fn metrics_handler(State(state): State<GraphqlState>, headers: HeaderMap) -> Response {
    let Some(metrics) = state.metrics else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if let Some(token) = metrics
        .endpoint
        .as_ref()
        .and_then(|endpoint| endpoint.bearer_token.as_deref())
        .filter(|token| !token.trim().is_empty())
        && bearer_token(&headers) != Some(token)
    {
        metrics
            .recorder
            .record_indexer_graphql_auth_failure(&metrics.labels);
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match metrics.recorder.encode() {
        Ok(text) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode metrics: {error}"),
        )
            .into_response(),
    }
}

async fn metrics_dynamic_handler(
    State(state): State<DynamicGraphqlState>,
    headers: HeaderMap,
) -> Response {
    let Some(metrics) = state.metrics else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if let Some(token) = metrics
        .endpoint
        .as_ref()
        .and_then(|endpoint| endpoint.bearer_token.as_deref())
        .filter(|token| !token.trim().is_empty())
        && bearer_token(&headers) != Some(token)
    {
        metrics
            .recorder
            .record_indexer_graphql_auth_failure(&metrics.labels);
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match metrics.recorder.encode() {
        Ok(text) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode metrics: {error}"),
        )
            .into_response(),
    }
}

fn request_labels(
    metrics: &IndexerGraphqlMetrics,
    application: Option<&QueryApplicationPermit>,
) -> IndexerGraphqlMetricLabels {
    let mut labels = metrics.labels.clone();
    if let Some(application) = application {
        labels.application = application.application_id().to_owned();
    }
    labels
}

fn record_auth_error(state: &GraphqlState, error: &DatalensError) {
    let Some(metrics) = &state.metrics else {
        return;
    };
    match error.kind {
        DatalensErrorKind::RateLimited => metrics
            .recorder
            .record_indexer_graphql_rate_limited(&metrics.labels),
        _ => metrics
            .recorder
            .record_indexer_graphql_auth_failure(&metrics.labels),
    }
}

fn record_dynamic_auth_error(state: &DynamicGraphqlState, error: &DatalensError) {
    let Some(metrics) = &state.metrics else {
        return;
    };
    match error.kind {
        DatalensErrorKind::RateLimited => metrics
            .recorder
            .record_indexer_graphql_rate_limited(&metrics.labels),
        _ => metrics
            .recorder
            .record_indexer_graphql_auth_failure(&metrics.labels),
    }
}

fn auth_error_response(error: DatalensError) -> Response {
    let status = match error.kind {
        DatalensErrorKind::AuthenticationFailed => StatusCode::UNAUTHORIZED,
        DatalensErrorKind::Unauthorized => StatusCode::FORBIDDEN,
        DatalensErrorKind::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::BAD_REQUEST,
    };
    (
        status,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        serde_json::json!({
            "error": {
                "code": graphql_error_code(&error.kind),
                "kind": format!("{:?}", error.kind),
                "message": error.message,
            }
        })
        .to_string(),
    )
        .into_response()
}
