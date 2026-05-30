use std::{sync::Arc, time::Instant};

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, Json, Object, Schema, SimpleObject,
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
pub use datalens_metrics::IndexerGraphqlMetricLabels;
use datalens_metrics::{IndexerGraphqlQueryOutcome, MetricsRecorder};
use serde_json::{Map, Value};

use crate::{IndexerError, QueryableStore, StoreQuery};

pub const DEFAULT_EVENT_LIMIT: u64 = 100;
pub const MAX_EVENT_LIMIT: u64 = 1000;

pub type IndexerGraphqlSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;
type SharedStore = Arc<dyn QueryableStore>;

pub fn graphql_schema(store: SharedStore) -> IndexerGraphqlSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(store)
        .finish()
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
    })
}

#[derive(Clone)]
struct GraphqlState {
    schema: IndexerGraphqlSchema,
    endpoint: String,
    metrics: Option<IndexerGraphqlMetrics>,
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
    request: GraphQLRequest,
) -> GraphQLResponse {
    let started = Instant::now();
    let response = state.schema.execute(request.into_inner()).await;
    if let Some(metrics) = &state.metrics {
        let outcome = if response.errors.is_empty() {
            IndexerGraphqlQueryOutcome::Success
        } else {
            IndexerGraphqlQueryOutcome::Error
        };
        metrics
            .recorder
            .record_indexer_graphql_query(&metrics.labels, outcome);
        metrics.recorder.observe_indexer_graphql_query_duration(
            &metrics.labels,
            started.elapsed().as_secs_f64(),
        );
    }
    response.into()
}

async fn playground_handler(State(state): State<GraphqlState>) -> Response {
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

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value.strip_prefix("Bearer ")
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
