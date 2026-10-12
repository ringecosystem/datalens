use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use datalens_core::wire::{BackfillRequest, BackfillResponse, CoverageResponse, JobsResponse};
use datalens_core::{BlockRange, ChainName, Dataset, Error, LogSelector};
use datalens_store::JobStatus;
use serde::Deserialize;

use crate::AppState;
use crate::error::{ApiError, parse_json};

const MAX_JOBS: u32 = 1_000;

pub(crate) async fn backfill(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<BackfillResponse>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    tenant.require_admin()?;
    let request: BackfillRequest = parse_json(&body)?;
    Ok(Json(
        state.0.engine.backfill(&tenant.app.id, request).await?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageParams {
    chain: String,
    dataset: String,
    /// JSON `evm.logs` selector, as in a query body.
    selector: Option<String>,
    start: Option<u64>,
    end: Option<u64>,
}

pub(crate) async fn coverage(
    State(state): State<AppState>,
    headers: HeaderMap,
    params: Result<Query<CoverageParams>, QueryRejection>,
) -> Result<Json<CoverageResponse>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    tenant.require_admin()?;
    let Query(params) = params.map_err(|error| Error::InvalidInput(error.body_text()))?;
    let chain = ChainName::new(params.chain)?;
    let dataset: Dataset = params.dataset.parse()?;
    let selector: Option<LogSelector> = params
        .selector
        .map(|text| parse_json(text.as_bytes()))
        .transpose()?;
    let range = match (params.start, params.end) {
        (Some(start), Some(end)) => Some(BlockRange::new(start, end)?),
        (None, None) => None,
        _ => {
            return Err(Error::InvalidInput("start and end go together".to_owned()).into());
        }
    };
    Ok(Json(
        state
            .0
            .engine
            .coverage(&chain, dataset, selector, range)
            .await?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobsParams {
    status: Option<String>,
    limit: Option<u32>,
}

pub(crate) async fn jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    params: Result<Query<JobsParams>, QueryRejection>,
) -> Result<Json<JobsResponse>, ApiError> {
    let tenant = state.0.tenants.authenticate(&headers)?;
    tenant.require_admin()?;
    let Query(params) = params.map_err(|error| Error::InvalidInput(error.body_text()))?;
    let status = params
        .status
        .map(|text| {
            JobStatus::ALL
                .into_iter()
                .find(|status| status.as_str() == text)
                .ok_or_else(|| Error::InvalidInput(format!("unknown job status {text:?}")))
        })
        .transpose()?;
    let limit = params.limit.unwrap_or(100).clamp(1, MAX_JOBS);
    Ok(Json(state.0.engine.jobs(status, limit).await?))
}
