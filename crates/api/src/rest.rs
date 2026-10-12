use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::time::Duration;

use datalens_core::wire::{
    ChainHeads, HeadsResponse, QueryRequest, QueryResponse, WatchRequest, WatchResponse,
};
use datalens_core::{ChainName, Error};
use datalens_store::HeadKind;
use prometheus::{Encoder, TextEncoder};
use serde::Deserialize;

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

/// Default and cap of a heads long-poll, below common proxy idle timeouts.
const WAIT_DEFAULT_MS: u64 = 30_000;
const WAIT_MAX_MS: u64 = 60_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WaitParams {
    after: u64,
    kind: Option<String>,
    timeout_ms: Option<u64>,
}

/// Long-polls until head `kind` (default `latest`) passes `after`. A timeout is not an error.
/// It returns the current heads with 200, so the caller compares and polls again.
pub(crate) async fn wait_heads(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    params: Result<Query<WaitParams>, QueryRejection>,
) -> Result<Json<ChainHeads>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    let chain = ChainName::new(chain)?;
    tenant.allow_chain(&chain)?;
    let Query(params) = params.map_err(|error| Error::InvalidInput(error.body_text()))?;
    let kind = match params.kind.as_deref() {
        None | Some("latest") => HeadKind::Latest,
        Some("safe") => HeadKind::Safe,
        Some("finalized") => HeadKind::Finalized,
        Some(other) => {
            return Err(Error::InvalidInput(format!(
                "kind must be latest, safe, or finalized, not {other:?}"
            ))
            .into());
        }
    };
    let timeout = params
        .timeout_ms
        .unwrap_or(WAIT_DEFAULT_MS)
        .min(WAIT_MAX_MS);
    Ok(Json(
        state
            .0
            .engine
            .wait_heads(&chain, params.after, kind, Duration::from_millis(timeout))
            .await?,
    ))
}

/// Subscribes the selector for the configured TTL, or renews it.
pub(crate) async fn watch(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<WatchResponse>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    let request: WatchRequest = parse_json(&body)?;
    tenant.allow_read(&request.chain, request.dataset, request.selector.as_ref())?;
    Ok(Json(state.0.engine.watch(&tenant.caller(), request).await?))
}

pub(crate) async fn unwatch(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    let request: WatchRequest = parse_json(&body)?;
    tenant.allow_read(&request.chain, request.dataset, request.selector.as_ref())?;
    state.0.engine.unwatch(&tenant.app.id, request).await?;
    Ok(StatusCode::NO_CONTENT)
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
