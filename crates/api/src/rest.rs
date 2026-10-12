use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use datalens_core::wire::{ChainHeads, HeadsResponse, QueryRequest, QueryResponse};
use datalens_core::{ChainName, Error};
use prometheus::{Encoder, TextEncoder};

use crate::auth::{bearer, constant_time_eq};
use crate::error::{ApiError, parse_json};
use crate::{AppState, run_query};

pub(crate) async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<QueryResponse>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    let request: QueryRequest = parse_json(&body)?;
    Ok(Json(run_query(&state.0, &tenant, request).await?))
}

/// Heads of the chains this application may read.
pub(crate) async fn heads(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<HeadsResponse>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    let chains = state
        .0
        .engine
        .all_heads()
        .await?
        .into_iter()
        .filter(|heads| tenant.app.chains.contains(&heads.chain))
        .collect();
    Ok(Json(HeadsResponse { chains }))
}

pub(crate) async fn chain_heads(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
) -> Result<Json<ChainHeads>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    let chain = ChainName::new(chain)?;
    tenant.allow_chain(&chain)?;
    Ok(Json(state.0.engine.chain_heads(&chain).await?))
}

pub(crate) async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let allowed = bearer(&headers)
        .is_some_and(|token| constant_time_eq(token.as_bytes(), state.0.metrics_token.as_bytes()));
    if !allowed {
        return ApiError(Error::Unauthorized("metrics token required".to_owned())).into_response();
    }
    if let Err(error) = state.0.engine.refresh_job_gauge().await {
        tracing::warn!(%error, "fill job gauge refresh failed");
    }
    let mut body = Vec::new();
    let encoder = TextEncoder::new();
    if let Err(error) = encoder.encode(&state.0.registry.gather(), &mut body) {
        return ApiError(Error::Internal(error.to_string())).into_response();
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, encoder.format_type().to_owned())],
        body,
    )
        .into_response()
}
