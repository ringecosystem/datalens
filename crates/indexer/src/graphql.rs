use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, Json, Object, Schema, SimpleObject,
    Value as GraphqlValue,
    dynamic::{
        Field, FieldFuture, FieldValue, InputValue, Object as DynamicObject, Scalar,
        Schema as DynamicSchema, TypeRef,
    },
    http::GraphiQLSource,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use datalens_core::{DatalensError, DatalensErrorKind};
pub use datalens_metrics::IndexerGraphqlMetricLabels;
use datalens_metrics::{IndexerGraphqlQueryOutcome, MetricsRecorder};
use serde_json::{Map, Value};

use crate::{
    GraphqlViewConfig, GraphqlViewFieldConfig, IndexerError, QueryAuthApplicationConfig,
    QueryAuthConfig, QueryableStore, StoreQuery,
};

pub const DEFAULT_EVENT_LIMIT: u64 = 100;
pub const MAX_EVENT_LIMIT: u64 = 1000;

pub type IndexerGraphqlSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;
pub type IndexerGraphqlApplicationSchema = DynamicSchema;
type SharedStore = Arc<dyn QueryableStore>;

pub fn graphql_schema(store: SharedStore) -> IndexerGraphqlSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(store)
        .finish()
}

pub fn graphql_schema_with_views(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
) -> Result<IndexerGraphqlApplicationSchema, IndexerError> {
    build_dynamic_schema(store, views)
}

pub fn graphql_router(store: SharedStore, path: &str, playground: bool) -> Router {
    graphql_router_internal(store, path, playground, None)
}

pub fn graphql_router_with_metrics(
    store: SharedStore,
    path: &str,
    playground: bool,
    metrics: IndexerGraphqlMetrics,
) -> Router {
    graphql_router_internal(store, path, playground, Some(metrics))
}

fn graphql_router_internal(
    store: SharedStore,
    path: &str,
    playground: bool,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Router {
    graphql_router_with_auth(store, path, playground, QueryAuthConfig::default(), metrics)
}

pub fn graphql_router_with_auth(
    store: SharedStore,
    path: &str,
    playground: bool,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Router {
    let schema = graphql_schema(store);
    let mut router = Router::new().route(path, post(graphql_handler));
    if playground {
        let playground_path = format!("{path}/playground");
        router = router.route(&playground_path, get(playground_handler));
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

pub fn graphql_router_with_views_auth(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
    path: &str,
    playground: bool,
    auth: QueryAuthConfig,
    metrics: Option<IndexerGraphqlMetrics>,
) -> Result<Router, IndexerError> {
    let schema = graphql_schema_with_views(store, views)?;
    let mut router = Router::new().route(path, post(graphql_dynamic_handler));
    if playground {
        let playground_path = format!("{path}/playground");
        router = router.route(&playground_path, get(playground_dynamic_handler));
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
    schema: IndexerGraphqlApplicationSchema,
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

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value.strip_prefix("Bearer ")
}

#[derive(Clone, Debug)]
struct QueryAuthRegistry {
    enabled: bool,
    applications: Arc<BTreeMap<String, QueryAuthApplicationConfig>>,
    quota_state: Arc<Mutex<BTreeMap<String, QueryAuthState>>>,
}

impl QueryAuthRegistry {
    fn new(config: QueryAuthConfig) -> Self {
        Self {
            enabled: config.enabled,
            applications: Arc::new(
                config
                    .applications
                    .into_iter()
                    .map(|mut application| {
                        application.id = normalize_application_id(&application.id);
                        (application.id.clone(), application)
                    })
                    .collect(),
            ),
            quota_state: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<QueryApplicationPermit>, DatalensError> {
        if !self.enabled {
            return Ok(None);
        }
        let token = bearer_token(headers).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "application credentials are required",
            )
        })?;
        let application = self
            .applications
            .values()
            .find(|application| application.token == token)
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application credentials are invalid",
                )
            })?;
        if !application.enabled {
            return Err(DatalensError::new(
                DatalensErrorKind::Unauthorized,
                "application is disabled",
            ));
        }
        self.acquire_permit(application).map(Some)
    }

    fn acquire_permit(
        &self,
        application: &QueryAuthApplicationConfig,
    ) -> Result<QueryApplicationPermit, DatalensError> {
        let Some(quota) = &application.quota else {
            return Ok(QueryApplicationPermit::noop(application.id.clone()));
        };
        let mut states = self.quota_state.lock().expect("query auth quota state");
        let state = states.entry(application.id.clone()).or_default();
        let now = Instant::now();
        if now.duration_since(state.window_started_at) >= Duration::from_secs(60) {
            state.window_started_at = now;
            state.requests_in_window = 0;
        }
        if let Some(limit) = quota.max_requests_per_minute
            && state.requests_in_window >= limit
        {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application request rate quota exceeded",
            ));
        }
        if let Some(limit) = quota.max_concurrent_requests
            && state.in_flight >= limit
        {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application concurrent request quota exceeded",
            ));
        }
        if quota.max_requests_per_minute.is_some() {
            state.requests_in_window += 1;
        }
        let release = quota.max_concurrent_requests.is_some();
        if release {
            state.in_flight += 1;
        }
        Ok(QueryApplicationPermit {
            application_id: application.id.clone(),
            quota_state: self.quota_state.clone(),
            release,
        })
    }
}

#[derive(Debug)]
struct QueryApplicationPermit {
    application_id: String,
    quota_state: Arc<Mutex<BTreeMap<String, QueryAuthState>>>,
    release: bool,
}

impl QueryApplicationPermit {
    fn noop(application_id: String) -> Self {
        Self {
            application_id,
            quota_state: Arc::new(Mutex::new(BTreeMap::new())),
            release: false,
        }
    }

    fn application_id(&self) -> &str {
        &self.application_id
    }
}

impl Drop for QueryApplicationPermit {
    fn drop(&mut self) {
        if !self.release {
            return;
        }
        let mut states = self.quota_state.lock().expect("query auth quota state");
        if let Some(state) = states.get_mut(&self.application_id) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
}

#[derive(Debug)]
struct QueryAuthState {
    window_started_at: Instant,
    requests_in_window: u64,
    in_flight: u64,
}

impl Default for QueryAuthState {
    fn default() -> Self {
        Self {
            window_started_at: Instant::now(),
            requests_in_window: 0,
            in_flight: 0,
        }
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
                "kind": format!("{:?}", error.kind),
                "message": error.message,
            }
        })
        .to_string(),
    )
        .into_response()
}

fn normalize_application_id(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    #[allow(clippy::too_many_arguments)]
    async fn events(
        &self,
        ctx: &Context<'_>,
        index_name: Option<String>,
        chain: Option<String>,
        chain_id: Option<u64>,
        dataset: String,
        address: Option<String>,
        event_name: Option<String>,
        signature: Option<String>,
        from_block: Option<u64>,
        to_block: Option<u64>,
        topic0: Option<String>,
        limit: Option<u64>,
        after: Option<String>,
    ) -> async_graphql::Result<Vec<IndexedEvent>> {
        let limit = bounded_limit(limit)?;
        let after = parse_after(after)?;
        let filter = event_filter(EventFilter {
            index_name,
            chain,
            chain_id,
            address,
            event_name,
            signature,
            from_block,
            to_block,
            topic0,
            limit,
            after,
        });
        let store = store(ctx)?.clone();
        let result =
            tokio::task::spawn_blocking(move || store.query(StoreQuery { dataset, filter }))
                .await
                .map_err(|error| Error::new(format!("graphql query task failed: {error}")))?
                .map_err(graphql_error)?;
        result
            .rows
            .into_iter()
            .map(IndexedEvent::try_from)
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    async fn decoded_events(
        &self,
        ctx: &Context<'_>,
        index_name: Option<String>,
        chain: Option<String>,
        chain_id: Option<u64>,
        dataset: String,
        address: Option<String>,
        event_name: Option<String>,
        signature: Option<String>,
        from_block: Option<u64>,
        to_block: Option<u64>,
        topic0: Option<String>,
        limit: Option<u64>,
        after: Option<String>,
    ) -> async_graphql::Result<Vec<DecodedEvent>> {
        let limit = bounded_limit(limit)?;
        let after = parse_after(after)?;
        let filter = event_filter(EventFilter {
            index_name,
            chain,
            chain_id,
            address,
            event_name,
            signature,
            from_block,
            to_block,
            topic0,
            limit,
            after,
        });
        let store = store(ctx)?.clone();
        let result = tokio::task::spawn_blocking(move || {
            store.query_decoded_events(StoreQuery { dataset, filter })
        })
        .await
        .map_err(|error| Error::new(format!("graphql decoded query task failed: {error}")))?
        .map_err(graphql_error)?;
        result
            .rows
            .into_iter()
            .map(DecodedEvent::try_from)
            .collect()
    }
}

#[derive(SimpleObject)]
pub struct IndexedEvent {
    pub index_name: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<u64>,
    pub dataset: Option<String>,
    pub block_number: Option<u64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<u64>,
    pub event_index: Option<u64>,
    pub address: Option<String>,
    pub selector: Option<String>,
    pub topics: Vec<String>,
    pub topic0: Option<String>,
    pub signature: Option<String>,
    pub event_name: Option<String>,
    pub decoded: Json<Value>,
    pub data: Option<String>,
    pub payload: Json<Value>,
    pub created_at: Option<String>,
}

impl TryFrom<Value> for IndexedEvent {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("indexed event row must be a JSON object"))?;
        let topics = string_array(object, "topics");
        let selector = string_field(object, "address")
            .or_else(|| string_field(object, "selector"))
            .or_else(|| string_field(object, "program"))
            .or_else(|| string_field(object, "account"));
        let created_at = string_field(object, "created_at");
        Ok(Self {
            index_name: string_field(object, "index"),
            chain: string_field(object, "chain"),
            chain_id: u64_field(object, "chain_id"),
            dataset: string_field(object, "dataset"),
            block_number: u64_field(object, "block_number"),
            block_hash: string_field(object, "block_hash"),
            transaction_hash: string_field(object, "transaction_hash"),
            transaction_index: u64_field(object, "transaction_index"),
            event_index: u64_field(object, "log_index")
                .or_else(|| u64_field(object, "event_index")),
            address: string_field(object, "address"),
            selector,
            topic0: topics.first().cloned(),
            signature: string_field(object, "signature"),
            event_name: string_field(object, "event_name"),
            decoded: Json(object.get("decoded").cloned().unwrap_or(Value::Null)),
            data: string_field(object, "data"),
            payload: Json(value),
            created_at,
            topics,
        })
    }
}

#[derive(SimpleObject)]
pub struct DecodedEvent {
    pub index_name: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<u64>,
    pub dataset: Option<String>,
    pub block_number: Option<u64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<u64>,
    pub log_index: Option<u64>,
    pub address: Option<String>,
    pub event_name: Option<String>,
    pub signature: Option<String>,
    pub topic0: Option<String>,
    pub decoded_args: Json<Value>,
    pub decode_status: Option<String>,
    pub decode_error: Option<String>,
    pub payload: Json<Value>,
    pub created_at: Option<String>,
}

impl TryFrom<Value> for DecodedEvent {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("decoded event row must be a JSON object"))?;
        let topics = string_array(object, "topics");
        let decoded_args = object.get("decoded").cloned().unwrap_or(Value::Null);
        let created_at = string_field(object, "created_at");
        Ok(Self {
            index_name: string_field(object, "index"),
            chain: string_field(object, "chain"),
            chain_id: u64_field(object, "chain_id"),
            dataset: string_field(object, "dataset"),
            block_number: u64_field(object, "block_number"),
            block_hash: string_field(object, "block_hash"),
            transaction_hash: string_field(object, "transaction_hash"),
            transaction_index: u64_field(object, "transaction_index"),
            log_index: u64_field(object, "log_index").or_else(|| u64_field(object, "event_index")),
            address: string_field(object, "address"),
            event_name: string_field(object, "event_name"),
            signature: string_field(object, "signature"),
            topic0: string_field(object, "topic0").or_else(|| topics.first().cloned()),
            decoded_args: Json(decoded_args),
            decode_status: string_field(object, "decode_status")
                .or_else(|| Some("decoded".to_owned())),
            decode_error: string_field(object, "decode_error"),
            payload: Json(value),
            created_at,
        })
    }
}

struct EventFilter {
    index_name: Option<String>,
    chain: Option<String>,
    chain_id: Option<u64>,
    address: Option<String>,
    event_name: Option<String>,
    signature: Option<String>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    topic0: Option<String>,
    limit: u64,
    after: Option<u64>,
}

fn event_filter(input: EventFilter) -> Value {
    let EventFilter {
        index_name,
        chain,
        chain_id,
        address,
        event_name,
        signature,
        from_block,
        to_block,
        topic0,
        limit,
        after,
    } = input;
    let mut filter = Map::new();
    insert_string(&mut filter, "index", index_name);
    insert_string(&mut filter, "chain", chain);
    insert_u64(&mut filter, "chain_id", chain_id);
    insert_string(&mut filter, "address", address);
    insert_string(&mut filter, "event_name", event_name);
    insert_string(&mut filter, "signature", signature);
    insert_u64(&mut filter, "from_block", from_block);
    insert_u64(&mut filter, "to_block", to_block);
    insert_string(&mut filter, "topic0", topic0);
    insert_u64(&mut filter, "limit", Some(limit));
    insert_u64(&mut filter, "after", after);
    Value::Object(filter)
}

fn bounded_limit(limit: Option<u64>) -> async_graphql::Result<u64> {
    let limit = limit.unwrap_or(DEFAULT_EVENT_LIMIT);
    if limit == 0 {
        return Err(Error::new("limit must be greater than 0"));
    }
    if limit > MAX_EVENT_LIMIT {
        return Err(Error::new(format!(
            "limit must be less than or equal to {MAX_EVENT_LIMIT}"
        )));
    }
    Ok(limit)
}

fn parse_after(after: Option<String>) -> async_graphql::Result<Option<u64>> {
    after
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| Error::new("after must be a non-negative integer cursor"))
        })
        .transpose()
}

fn store<'a>(ctx: &'a Context<'_>) -> async_graphql::Result<&'a SharedStore> {
    ctx.data::<SharedStore>()
        .map_err(|_| Error::new("queryable store is not configured"))
}

fn graphql_error(error: IndexerError) -> Error {
    Error::new(error.to_string())
}

fn string_field(object: &Map<String, Value>, field: &str) -> Option<String> {
    object.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn u64_field(object: &Map<String, Value>, field: &str) -> Option<u64> {
    object.get(field).and_then(Value::as_u64)
}

fn string_array(object: &Map<String, Value>, field: &str) -> Vec<String> {
    object
        .get(field)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn insert_string(filter: &mut Map<String, Value>, field: &str, value: Option<String>) {
    if let Some(value) = value {
        filter.insert(field.to_owned(), Value::String(value));
    }
}

fn insert_u64(filter: &mut Map<String, Value>, field: &str, value: Option<u64>) {
    if let Some(value) = value {
        filter.insert(field.to_owned(), Value::from(value));
    }
}

#[derive(Clone, Debug)]
struct DynamicEventRow {
    payload: Value,
}

fn build_dynamic_schema(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
) -> Result<IndexerGraphqlApplicationSchema, IndexerError> {
    let mut query = DynamicObject::new("Query").field(dynamic_events_field());
    let mut builder = DynamicSchema::build("Query", None, None)
        .data(store)
        .register(Scalar::new("JSON"))
        .register(dynamic_indexed_event_object());

    for view in views {
        let type_name = format!("{}Row", view.name);
        query = query.field(dynamic_view_field(view.clone(), type_name.clone()));
        builder = builder.register(dynamic_view_object(&type_name, &view.fields));
    }

    builder.register(query).finish().map_err(|error| {
        IndexerError::Config(format!("query.views: build GraphQL schema: {error}"))
    })
}

fn dynamic_events_field() -> Field {
    Field::new("events", TypeRef::named_nn_list_nn("IndexedEvent"), |ctx| {
        let store = match ctx.data::<SharedStore>() {
            Ok(store) => store.clone(),
            Err(_) => {
                return FieldFuture::new(async move {
                    Err::<Option<FieldValue<'static>>, _>(Error::new(
                        "queryable store is not configured",
                    ))
                });
            }
        };
        let dataset = match ctx.args.try_get("dataset").and_then(|value| value.string()) {
            Ok(dataset) => dataset.to_owned(),
            Err(error) => {
                return FieldFuture::new(
                    async move { Err::<Option<FieldValue<'static>>, _>(error) },
                );
            }
        };
        let limit = match ctx
            .args
            .get("limit")
            .map(|value| value.u64())
            .transpose()
            .and_then(bounded_limit)
        {
            Ok(limit) => limit,
            Err(error) => {
                return FieldFuture::new(
                    async move { Err::<Option<FieldValue<'static>>, _>(error) },
                );
            }
        };
        let after = match ctx
            .args
            .get("after")
            .map(|value| value.string().map(str::to_owned))
            .transpose()
            .and_then(parse_after)
        {
            Ok(after) => after,
            Err(error) => {
                return FieldFuture::new(
                    async move { Err::<Option<FieldValue<'static>>, _>(error) },
                );
            }
        };
        let filter = event_filter(EventFilter {
            index_name: dynamic_string_arg(&ctx.args, "indexName"),
            chain: dynamic_string_arg(&ctx.args, "chain"),
            chain_id: dynamic_u64_arg(&ctx.args, "chainId"),
            address: dynamic_string_arg(&ctx.args, "address"),
            event_name: dynamic_string_arg(&ctx.args, "eventName"),
            signature: dynamic_string_arg(&ctx.args, "signature"),
            from_block: dynamic_u64_arg(&ctx.args, "fromBlock"),
            to_block: dynamic_u64_arg(&ctx.args, "toBlock"),
            topic0: dynamic_string_arg(&ctx.args, "topic0"),
            limit,
            after,
        });
        FieldFuture::new(async move {
            let result =
                tokio::task::spawn_blocking(move || store.query(StoreQuery { dataset, filter }))
                    .await
                    .map_err(|error| Error::new(format!("graphql query task failed: {error}")))?
                    .map_err(graphql_error)?;
            Ok(Some(dynamic_rows(result.rows)))
        })
    })
    .argument(InputValue::new(
        "indexName",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new("chain", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("chainId", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new(
        "dataset",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("address", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new(
        "eventName",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "signature",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new("fromBlock", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("toBlock", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("topic0", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
}

fn dynamic_view_field(view: GraphqlViewConfig, type_name: String) -> Field {
    Field::new(
        view.name.clone(),
        TypeRef::named_nn_list_nn(type_name),
        move |ctx| {
            let view = view.clone();
            let store = match ctx.data::<SharedStore>() {
                Ok(store) => store.clone(),
                Err(_) => {
                    return FieldFuture::new(async move {
                        Err::<Option<FieldValue<'static>>, _>(Error::new(
                            "queryable store is not configured",
                        ))
                    });
                }
            };
            let limit = match ctx
                .args
                .get("limit")
                .map(|value| value.u64())
                .transpose()
                .and_then(|limit| bounded_view_limit(limit, &view))
            {
                Ok(limit) => limit,
                Err(error) => {
                    return FieldFuture::new(async move {
                        Err::<Option<FieldValue<'static>>, _>(error)
                    });
                }
            };
            let after = match ctx
                .args
                .get("after")
                .map(|value| value.string().map(str::to_owned))
                .transpose()
                .and_then(parse_after)
            {
                Ok(after) => after,
                Err(error) => {
                    return FieldFuture::new(async move {
                        Err::<Option<FieldValue<'static>>, _>(error)
                    });
                }
            };
            let dataset = view.dataset.clone();
            let filter = view_filter(&view, limit, after);
            FieldFuture::new(async move {
                let result = tokio::task::spawn_blocking(move || {
                    store.query(StoreQuery { dataset, filter })
                })
                .await
                .map_err(|error| Error::new(format!("graphql query task failed: {error}")))?
                .map_err(graphql_error)?;
                Ok(Some(dynamic_rows(result.rows)))
            })
        },
    )
    .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
}

fn dynamic_indexed_event_object() -> DynamicObject {
    DynamicObject::new("IndexedEvent")
        .field(dynamic_row_field(
            "indexName",
            "index",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "chain",
            "chain",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "chainId",
            "chain_id",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "dataset",
            "dataset",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "blockNumber",
            "block_number",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "blockHash",
            "block_hash",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "transactionHash",
            "transaction_hash",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "transactionIndex",
            "transaction_index",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "eventIndex",
            "log_index",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "address",
            "address",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_selector_field())
        .field(dynamic_topics_field())
        .field(dynamic_topic0_field())
        .field(dynamic_row_field(
            "signature",
            "signature",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "eventName",
            "event_name",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_json_row_field("decoded", "decoded"))
        .field(dynamic_row_field(
            "data",
            "data",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_json_payload_field())
        .field(dynamic_row_field(
            "createdAt",
            "created_at",
            TypeRef::named(TypeRef::STRING),
        ))
}

fn dynamic_view_object(type_name: &str, fields: &[GraphqlViewFieldConfig]) -> DynamicObject {
    let mut object = DynamicObject::new(type_name).field(dynamic_json_payload_field());
    for field in fields {
        object = object.field(dynamic_json_row_field(&field.name, &field.path));
    }
    object
}

fn dynamic_row_field(name: &str, path: &str, ty: TypeRef) -> Field {
    let path = path.to_owned();
    Field::new(name, ty, move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .map(|row| value_at_path(&row.payload, &path).cloned())
            .and_then(|value| json_to_graphql(value.unwrap_or(Value::Null)));
        FieldFuture::from_value(value.ok())
    })
}

fn dynamic_json_row_field(name: &str, path: &str) -> Field {
    let path = path.to_owned();
    Field::new(name, TypeRef::named("JSON"), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .map(|row| {
                value_at_path(&row.payload, &path)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .and_then(json_to_graphql);
        FieldFuture::from_value(value.ok())
    })
}

fn dynamic_json_payload_field() -> Field {
    Field::new("payload", TypeRef::named("JSON"), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .map(|row| row.payload.clone())
            .and_then(json_to_graphql);
        FieldFuture::from_value(value.ok())
    })
}

fn dynamic_selector_field() -> Field {
    Field::new("selector", TypeRef::named(TypeRef::STRING), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .ok()
            .and_then(|row| {
                string_field(row.payload.as_object()?, "address")
                    .or_else(|| string_field(row.payload.as_object()?, "selector"))
                    .or_else(|| string_field(row.payload.as_object()?, "program"))
                    .or_else(|| string_field(row.payload.as_object()?, "account"))
            })
            .map(GraphqlValue::String);
        FieldFuture::from_value(value)
    })
}

fn dynamic_topics_field() -> Field {
    Field::new(
        "topics",
        TypeRef::named_list_nn(TypeRef::STRING),
        move |ctx| {
            let value = ctx
                .parent_value
                .try_downcast_ref::<DynamicEventRow>()
                .ok()
                .and_then(|row| {
                    row.payload
                        .as_object()
                        .map(|object| string_array(object, "topics"))
                })
                .map(|topics| {
                    GraphqlValue::List(topics.into_iter().map(GraphqlValue::String).collect())
                });
            FieldFuture::from_value(value)
        },
    )
}

fn dynamic_topic0_field() -> Field {
    Field::new("topic0", TypeRef::named(TypeRef::STRING), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .ok()
            .and_then(|row| {
                row.payload
                    .as_object()
                    .map(|object| string_array(object, "topics"))
            })
            .and_then(|topics| topics.into_iter().next())
            .map(GraphqlValue::String);
        FieldFuture::from_value(value)
    })
}

fn dynamic_rows(rows: Vec<Value>) -> FieldValue<'static> {
    FieldValue::list(
        rows.into_iter()
            .map(|payload| FieldValue::owned_any(DynamicEventRow { payload })),
    )
}

fn dynamic_string_arg(
    args: &async_graphql::dynamic::ObjectAccessor<'_>,
    name: &str,
) -> Option<String> {
    args.get(name)
        .and_then(|value| value.string().ok())
        .map(str::to_owned)
}

fn dynamic_u64_arg(args: &async_graphql::dynamic::ObjectAccessor<'_>, name: &str) -> Option<u64> {
    args.get(name).and_then(|value| value.u64().ok())
}

fn bounded_view_limit(limit: Option<u64>, view: &GraphqlViewConfig) -> async_graphql::Result<u64> {
    let limit = limit.unwrap_or(view.default_limit);
    if limit == 0 {
        return Err(Error::new("limit must be greater than 0"));
    }
    if limit > view.max_limit {
        return Err(Error::new(format!(
            "limit must be less than or equal to {}",
            view.max_limit
        )));
    }
    Ok(limit)
}

fn view_filter(view: &GraphqlViewConfig, limit: u64, after: Option<u64>) -> Value {
    let mut filter = Map::new();
    insert_string(&mut filter, "event_name", view.event_name.clone());
    insert_string(&mut filter, "signature", view.signature.clone());
    insert_u64(&mut filter, "limit", Some(limit));
    insert_u64(&mut filter, "after", after);
    for filter_config in &view.filters {
        filter.insert(
            filter_config.field.clone(),
            Value::String(filter_config.equals.clone()),
        );
    }
    Value::Object(filter)
}

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

fn json_to_graphql(value: Value) -> async_graphql::Result<GraphqlValue> {
    GraphqlValue::from_json(value).map_err(|error| Error::new(error.to_string()))
}
