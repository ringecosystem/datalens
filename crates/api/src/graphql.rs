use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, ErrorExtensions, InputValueError, InputValueResult,
    Json as GqlJson, Object, Scalar, ScalarType, Schema, SimpleObject, Value as GqlValue,
};
use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use datalens_core::Error;
use datalens_core::wire::{QueryRequest, QueryResponse, Segment};
use serde_json::Value;

use crate::auth::Tenant;
use crate::error::{ApiError, parse_json};
use crate::{AppState, Shared, run_query};

pub(crate) type GqlSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub(crate) fn schema() -> GqlSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish()
}

pub(crate) struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Same input and result as `POST /v1/query`. `input` is the REST request body.
    async fn query(
        &self,
        ctx: &Context<'_>,
        input: GqlJson<Value>,
    ) -> async_graphql::Result<QueryResult> {
        let shared = ctx.data::<Arc<Shared>>()?;
        let tenant = ctx.data::<Arc<Tenant>>()?;
        let request: QueryRequest = serde_json::from_value(input.0)
            .map_err(|error| gql_error(&Error::InvalidInput(error.to_string())))?;
        run_query(shared, tenant, request)
            .await
            .map(QueryResult::from)
            .map_err(|error| gql_error(&error))
    }
}

fn gql_error(error: &Error) -> async_graphql::Error {
    async_graphql::Error::new(error.to_string()).extend_with(|_, extensions| {
        extensions.set("kind", error.kind());
    })
}

pub(crate) async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    let request: async_graphql::Request = parse_json(&body)?;
    let response = state
        .0
        .schema
        .execute(request.data(Arc::clone(&state.0)).data(tenant))
        .await;
    Ok(Json(response).into_response())
}

/// A block number as a decimal string. GraphQL `Int` is 32-bit and block numbers are not.
struct BigInt(u64);

#[Scalar(name = "BigInt")]
impl ScalarType for BigInt {
    fn parse(value: GqlValue) -> InputValueResult<Self> {
        match &value {
            GqlValue::String(text) => text.parse().map(Self).map_err(InputValueError::custom),
            GqlValue::Number(number) => number
                .as_u64()
                .map(Self)
                .ok_or_else(|| InputValueError::expected_type(value.clone())),
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> GqlValue {
        GqlValue::String(self.0.to_string())
    }
}

#[derive(SimpleObject)]
#[graphql(name = "BlockRange")]
struct GqlRange {
    start: BigInt,
    end: BigInt,
}

#[derive(SimpleObject)]
#[graphql(name = "Anchor")]
struct GqlAnchor {
    number: BigInt,
    hash: String,
}

#[derive(SimpleObject)]
#[graphql(name = "Segment")]
struct GqlSegment {
    start: BigInt,
    end: BigInt,
    source: String,
    finality: String,
    anchor: Option<GqlAnchor>,
}

#[derive(SimpleObject)]
#[graphql(name = "Page")]
struct GqlPage {
    next_cursor: Option<String>,
}

#[derive(SimpleObject)]
pub(crate) struct QueryResult {
    chain: String,
    dataset: String,
    requested: GqlRange,
    served: Option<GqlRange>,
    next_start: Option<BigInt>,
    incomplete_reason: Option<String>,
    incomplete_detail: Option<String>,
    segments: Vec<GqlSegment>,
    rows: GqlJson<Vec<Value>>,
    page: GqlPage,
}

impl From<QueryResponse> for QueryResult {
    fn from(response: QueryResponse) -> Self {
        let range = |range: datalens_core::BlockRange| GqlRange {
            start: BigInt(range.start()),
            end: BigInt(range.end()),
        };
        Self {
            chain: response.chain.to_string(),
            dataset: response.dataset.to_string(),
            requested: range(response.requested),
            served: response.served.map(range),
            next_start: response.next_start.map(BigInt),
            incomplete_reason: response
                .incomplete_reason
                .map(|reason| reason.as_str().to_owned()),
            incomplete_detail: response.incomplete_detail,
            segments: response.segments.into_iter().map(segment).collect(),
            rows: GqlJson(response.rows),
            page: GqlPage {
                next_cursor: response.page.next_cursor,
            },
        }
    }
}

fn segment(segment: Segment) -> GqlSegment {
    GqlSegment {
        start: BigInt(segment.start),
        end: BigInt(segment.end),
        source: serde_name(&segment.source),
        finality: serde_name(&segment.finality),
        anchor: segment.anchor.map(|anchor| GqlAnchor {
            number: BigInt(anchor.number),
            hash: format!("{:#x}", anchor.hash),
        }),
    }
}

/// The wire name of a unit enum value, such as `durable` or `finalized`.
fn serde_name(value: &impl serde::Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(text)) => text,
        _ => String::new(),
    }
}
