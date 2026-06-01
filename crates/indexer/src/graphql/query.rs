use async_graphql::{Context, Error, ErrorExtensions, Json, Object, SimpleObject};
use datalens_core::DatalensErrorKind;
use serde_json::{Map, Value};

use crate::{IndexerError, StoreQuery};

use super::{DEFAULT_EVENT_LIMIT, MAX_EVENT_LIMIT, SharedStore};

const SUPPORTED_INDEX_DATASETS: &[&str] = &[
    "evm.logs",
    "solana.transactions",
    "solana.instructions",
    "solana.account_updates",
    "tron.events",
];

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    #[allow(clippy::too_many_arguments)]
    /// Raw indexed events ordered by blockNumber, transactionIndex, eventIndex, and storage id.
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
        validate_dataset(&dataset)?;
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
    /// Cursor-paginated raw indexed events with deterministic event ordering.
    async fn events_connection(
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
        first: Option<u64>,
        after: Option<String>,
    ) -> async_graphql::Result<IndexedEventConnection> {
        validate_dataset(&dataset)?;
        let first = bounded_first(first)?;
        let after = parse_after(after)?;
        let query_after = connection_query_after(after)?;
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
            limit: first.saturating_add(1),
            after: query_after,
        });
        let store = store(ctx)?.clone();
        let result =
            tokio::task::spawn_blocking(move || store.query(StoreQuery { dataset, filter }))
                .await
                .map_err(|error| internal_error(format!("graphql query task failed: {error}")))?
                .map_err(graphql_error)?;
        let events = result
            .rows
            .into_iter()
            .map(IndexedEvent::try_from)
            .collect::<async_graphql::Result<Vec<_>>>()?;
        Ok(indexed_connection(events, first, query_after.unwrap_or(0)))
    }

    #[allow(clippy::too_many_arguments)]
    /// Decoded indexed events ordered by blockNumber, transactionIndex, eventIndex, and storage id.
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
        validate_dataset(&dataset)?;
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

    #[allow(clippy::too_many_arguments)]
    /// Cursor-paginated decoded indexed events with deterministic event ordering.
    async fn decoded_events_connection(
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
        first: Option<u64>,
        after: Option<String>,
    ) -> async_graphql::Result<DecodedEventConnection> {
        validate_dataset(&dataset)?;
        let first = bounded_first(first)?;
        let after = parse_after(after)?;
        let query_after = connection_query_after(after)?;
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
            limit: first.saturating_add(1),
            after: query_after,
        });
        let store = store(ctx)?.clone();
        let result = tokio::task::spawn_blocking(move || {
            store.query_decoded_events(StoreQuery { dataset, filter })
        })
        .await
        .map_err(|error| internal_error(format!("graphql decoded query task failed: {error}")))?
        .map_err(graphql_error)?;
        let events = result
            .rows
            .into_iter()
            .map(DecodedEvent::try_from)
            .collect::<async_graphql::Result<Vec<_>>>()?;
        Ok(decoded_connection(events, first, query_after.unwrap_or(0)))
    }
}

#[derive(Clone, SimpleObject)]
pub struct IndexedEvent {
    pub index_name: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<u64>,
    pub dataset: Option<String>,
    pub block_number: Option<u64>,
    pub block_hash: Option<String>,
    pub parent_hash: Option<String>,
    pub block_timestamp: Option<u64>,
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
            parent_hash: string_field(object, "parent_hash"),
            block_timestamp: u64_field(object, "block_timestamp"),
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

#[derive(Clone, SimpleObject)]
pub struct DecodedEvent {
    pub index_name: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<u64>,
    pub dataset: Option<String>,
    pub block_number: Option<u64>,
    pub block_hash: Option<String>,
    pub parent_hash: Option<String>,
    pub block_timestamp: Option<u64>,
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
            parent_hash: string_field(object, "parent_hash"),
            block_timestamp: u64_field(object, "block_timestamp"),
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

#[derive(SimpleObject)]
pub struct EventPageInfo {
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(SimpleObject)]
pub struct IndexedEventEdge {
    pub cursor: String,
    pub node: IndexedEvent,
}

#[derive(SimpleObject)]
pub struct IndexedEventConnection {
    pub edges: Vec<IndexedEventEdge>,
    pub nodes: Vec<IndexedEvent>,
    pub page_info: EventPageInfo,
}

#[derive(SimpleObject)]
pub struct DecodedEventEdge {
    pub cursor: String,
    pub node: DecodedEvent,
}

#[derive(SimpleObject)]
pub struct DecodedEventConnection {
    pub edges: Vec<DecodedEventEdge>,
    pub nodes: Vec<DecodedEvent>,
    pub page_info: EventPageInfo,
}

fn indexed_connection(
    mut events: Vec<IndexedEvent>,
    first: u64,
    cursor_start: u64,
) -> IndexedEventConnection {
    let has_next_page = u64::try_from(events.len()).unwrap_or(u64::MAX) > first;
    events.truncate(usize::try_from(first).unwrap_or(usize::MAX));
    let nodes = events.clone();
    let edges = events
        .into_iter()
        .enumerate()
        .map(|(index, node)| {
            let cursor = cursor_start.saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
            IndexedEventEdge {
                cursor: cursor.to_string(),
                node,
            }
        })
        .collect::<Vec<_>>();
    let end_cursor = edges.last().map(|edge| edge.cursor.clone());
    IndexedEventConnection {
        edges,
        nodes,
        page_info: EventPageInfo {
            end_cursor,
            has_next_page,
        },
    }
}

fn decoded_connection(
    mut events: Vec<DecodedEvent>,
    first: u64,
    cursor_start: u64,
) -> DecodedEventConnection {
    let has_next_page = u64::try_from(events.len()).unwrap_or(u64::MAX) > first;
    events.truncate(usize::try_from(first).unwrap_or(usize::MAX));
    let nodes = events.clone();
    let edges = events
        .into_iter()
        .enumerate()
        .map(|(index, node)| {
            let cursor = cursor_start.saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
            DecodedEventEdge {
                cursor: cursor.to_string(),
                node,
            }
        })
        .collect::<Vec<_>>();
    let end_cursor = edges.last().map(|edge| edge.cursor.clone());
    DecodedEventConnection {
        edges,
        nodes,
        page_info: EventPageInfo {
            end_cursor,
            has_next_page,
        },
    }
}

pub(super) struct EventFilter {
    pub(super) index_name: Option<String>,
    pub(super) chain: Option<String>,
    pub(super) chain_id: Option<u64>,
    pub(super) address: Option<String>,
    pub(super) event_name: Option<String>,
    pub(super) signature: Option<String>,
    pub(super) from_block: Option<u64>,
    pub(super) to_block: Option<u64>,
    pub(super) topic0: Option<String>,
    pub(super) limit: u64,
    pub(super) after: Option<u64>,
}

pub(super) fn event_filter(input: EventFilter) -> Value {
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

pub(super) fn bounded_limit(limit: Option<u64>) -> async_graphql::Result<u64> {
    let limit = limit.unwrap_or(DEFAULT_EVENT_LIMIT);
    if limit == 0 {
        return Err(invalid_input_error("limit must be greater than 0"));
    }
    if limit > MAX_EVENT_LIMIT {
        return Err(invalid_input_error(format!(
            "limit must be less than or equal to {MAX_EVENT_LIMIT}"
        )));
    }
    Ok(limit)
}

fn bounded_first(first: Option<u64>) -> async_graphql::Result<u64> {
    match first {
        Some(first) => bounded_limit(Some(first)),
        None => bounded_limit(Some(DEFAULT_EVENT_LIMIT)),
    }
}

pub(super) fn parse_after(after: Option<String>) -> async_graphql::Result<Option<u64>> {
    after
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| invalid_input_error("after must be a non-negative integer cursor"))
        })
        .transpose()
}

fn connection_query_after(after: Option<u64>) -> async_graphql::Result<Option<u64>> {
    after
        .map(|cursor| {
            cursor
                .checked_add(1)
                .ok_or_else(|| invalid_input_error("after cursor is too large"))
        })
        .transpose()
}

fn validate_dataset(dataset: &str) -> async_graphql::Result<()> {
    if SUPPORTED_INDEX_DATASETS.contains(&dataset) {
        return Ok(());
    }
    Err(graphql_error_for_kind(
        DatalensErrorKind::UnsupportedDataset,
        format!(
            "unsupported dataset {dataset}; supported values are {}",
            SUPPORTED_INDEX_DATASETS.join(", ")
        ),
    ))
}

fn store<'a>(ctx: &'a Context<'_>) -> async_graphql::Result<&'a SharedStore> {
    ctx.data::<SharedStore>()
        .map_err(|_| internal_error("queryable store is not configured"))
}

pub(super) fn graphql_error(error: IndexerError) -> Error {
    match error {
        IndexerError::Config(message) | IndexerError::Plan(message) => invalid_input_error(message),
        IndexerError::Runner(message) => internal_error(message),
    }
}

fn invalid_input_error(message: impl Into<String>) -> Error {
    graphql_error_for_kind(DatalensErrorKind::InvalidInput, message)
}

fn internal_error(message: impl Into<String>) -> Error {
    graphql_error_for_kind(DatalensErrorKind::Internal, message)
}

fn graphql_error_for_kind(kind: DatalensErrorKind, message: impl Into<String>) -> Error {
    let code = graphql_error_code(&kind);
    let kind_name = format!("{kind:?}");
    Error::new(message.into()).extend_with(move |_, extensions| {
        extensions.set("code", code);
        extensions.set("kind", kind_name);
    })
}

pub(super) fn graphql_error_code(kind: &DatalensErrorKind) -> &'static str {
    match kind {
        DatalensErrorKind::AuthenticationFailed => "AUTHENTICATION_FAILED",
        DatalensErrorKind::InvalidInput | DatalensErrorKind::InvalidRequest => "INVALID_INPUT",
        DatalensErrorKind::Unauthorized => "AUTHORIZATION_FAILED",
        DatalensErrorKind::UnsupportedDataset => "UNSUPPORTED_DATASET",
        DatalensErrorKind::RateLimited => "RATE_LIMITED",
        _ => "INTERNAL",
    }
}

pub(super) fn string_field(object: &Map<String, Value>, field: &str) -> Option<String> {
    object.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn u64_field(object: &Map<String, Value>, field: &str) -> Option<u64> {
    object.get(field).and_then(Value::as_u64)
}

pub(super) fn string_array(object: &Map<String, Value>, field: &str) -> Vec<String> {
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

pub(super) fn insert_string(filter: &mut Map<String, Value>, field: &str, value: Option<String>) {
    if let Some(value) = value {
        filter.insert(field.to_owned(), Value::String(value));
    }
}

pub(super) fn insert_u64(filter: &mut Map<String, Value>, field: &str, value: Option<u64>) {
    if let Some(value) = value {
        filter.insert(field.to_owned(), Value::from(value));
    }
}
