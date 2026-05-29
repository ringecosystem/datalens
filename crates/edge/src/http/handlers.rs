use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_metrics::ApplicationIdentity;
use datalens_warmup::{
    WarmupChunkPolicy, WarmupRetryPolicy, WarmupSubmitRequest, WarmupTask, WarmupTaskFilter,
    WarmupTaskId,
};

use crate::{
    APPLICATION_IDENTITY_HEADER, auth,
    config::ApplicationOperationConfig,
    contract::{
        discovery::DiscoveryResponse,
        error::{api_error_body, api_error_status},
        query::{QueryApiRequest, QueryApiResponse},
        warmup::{
            WarmupRunOnceApiResponse, WarmupSubmitApiRequest, WarmupSubmitApiResponse,
            WarmupTaskApiResponse, WarmupTaskListApiResponse, WarmupTaskListQuery,
            warmup_task_view,
        },
    },
    http::AppState,
    service::registry::{QueryServiceRegistry, WarmupMutation},
};

pub(crate) async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

pub(crate) async fn chains(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let _application_context = state
        .registry
        .authenticate_discovery_headers(&headers)
        .map_err(ApiError)?;
    Ok(Json(
        serde_json::json!({ "chains": state.registry.chain_names() }),
    ))
}

pub(crate) async fn discovery(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DiscoveryResponse>, ApiError> {
    let _application_context = state
        .registry
        .authenticate_discovery_headers(&headers)
        .map_err(ApiError)?;
    state.registry.discovery().map(Json).map_err(ApiError)
}

pub(crate) async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryApiRequest>,
) -> Result<Json<QueryApiResponse>, ApiError> {
    let registry = state.registry.clone();
    let dataset = request.dataset_for_auth().map_err(ApiError)?;
    let application_context = registry
        .authenticate_headers(
            &headers,
            request.chain.configured_name(),
            &dataset,
            request.range_len(),
            request.finality,
        )
        .map_err(ApiError)?;
    let application = application_context
        .as_ref()
        .map(|application| application.metrics_identity())
        .or_else(|| application_from_headers(&headers));
    let native_input = request.into_native_input().map_err(ApiError)?;
    tokio::task::spawn_blocking(move || {
        registry
            .query_native_with_application(native_input, application)
            .and_then(QueryApiResponse::try_from_native_response)
    })
    .await
    .map_err(|error| {
        ApiError(DatalensError::new(
            DatalensErrorKind::Internal,
            format!("query task failed: {error}"),
        ))
    })?
    .map(Json)
    .map_err(ApiError)
}

pub(crate) async fn warmup_submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WarmupSubmitApiRequest>,
) -> Result<Response, ApiError> {
    let registry = state.registry.clone();
    let dataset = request.dataset_for_auth().map_err(ApiError)?;
    let application_context = registry
        .authenticate_warmup_headers(
            &headers,
            request.chain().configured_name(),
            &dataset,
            ApplicationOperationConfig::WarmupSubmit,
        )
        .map_err(ApiError)?;
    let application_id = application_context
        .as_ref()
        .map(|application| application.id.clone())
        .or_else(|| application_id_from_headers(&headers))
        .unwrap_or_else(|| "unknown".to_owned());
    let request = warmup_submit_request(application_id, request).map_err(ApiError)?;
    let outcome = tokio::task::spawn_blocking(move || registry.submit_warmup_task(request))
        .await
        .map_err(|error| {
            ApiError(DatalensError::new(
                DatalensErrorKind::Internal,
                format!("warmup submit task failed: {error}"),
            ))
        })?
        .map_err(ApiError)?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(WarmupSubmitApiResponse {
            task_id: outcome.task_id,
            created: outcome.created,
        }),
    )
        .into_response())
}

pub(crate) async fn warmup_list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WarmupTaskListQuery>,
) -> Result<Json<WarmupTaskListApiResponse>, ApiError> {
    let registry = state.registry.clone();
    let application_context = registry
        .authenticate_task_headers(&headers, ApplicationOperationConfig::WarmupRead)
        .map_err(ApiError)?;
    let application_id = application_context
        .as_ref()
        .map(|application| application.id.clone())
        .or_else(|| application_id_from_headers(&headers));
    let filter = WarmupTaskFilter {
        application_id,
        chain_key: query.chain,
        state: query.state,
    };
    let tasks = tokio::task::spawn_blocking(move || registry.list_warmup_tasks(filter))
        .await
        .map_err(|error| {
            ApiError(DatalensError::new(
                DatalensErrorKind::Internal,
                format!("warmup list task failed: {error}"),
            ))
        })?
        .map_err(ApiError)?
        .into_iter()
        .map(warmup_task_view)
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError)?;
    Ok(Json(WarmupTaskListApiResponse { tasks }))
}

pub(crate) async fn warmup_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    let task_id = WarmupTaskId::new(task_id).map_err(ApiError)?;
    let task = load_authorized_warmup_task(state.registry.clone(), &headers, task_id).await?;
    Ok(Json(WarmupTaskApiResponse {
        task: warmup_task_view(task).map_err(ApiError)?,
    }))
}

pub(crate) async fn warmup_pause(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    mutate_authorized_warmup_task(state.registry, headers, task_id, WarmupMutation::Pause).await
}

pub(crate) async fn warmup_cancel(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    mutate_authorized_warmup_task(state.registry, headers, task_id, WarmupMutation::Cancel).await
}

pub(crate) async fn warmup_retry(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    mutate_authorized_warmup_task(state.registry, headers, task_id, WarmupMutation::Retry).await
}

pub(crate) async fn warmup_run_once(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<WarmupRunOnceApiResponse>, ApiError> {
    let registry = state.registry.clone();
    let _application_context = registry
        .authenticate_task_headers(&headers, ApplicationOperationConfig::WarmupRun)
        .map_err(ApiError)?;
    let results = tokio::task::spawn_blocking(move || registry.run_warmup_once())
        .await
        .map_err(|error| {
            ApiError(DatalensError::new(
                DatalensErrorKind::Internal,
                format!("warmup run-once task failed: {error}"),
            ))
        })?
        .map_err(ApiError)?;
    Ok(Json(WarmupRunOnceApiResponse { results }))
}

async fn load_authorized_warmup_task(
    registry: QueryServiceRegistry,
    headers: &HeaderMap,
    task_id: WarmupTaskId,
) -> Result<WarmupTask, ApiError> {
    let application_context = registry
        .authenticate_task_headers(headers, ApplicationOperationConfig::WarmupRead)
        .map_err(ApiError)?;
    let task = tokio::task::spawn_blocking(move || {
        registry.get_warmup_task(&task_id)?.ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("warmup task {} not found", task_id.as_str()),
            )
        })
    })
    .await
    .map_err(|error| {
        ApiError(DatalensError::new(
            DatalensErrorKind::Internal,
            format!("warmup get task failed: {error}"),
        ))
    })?
    .map_err(ApiError)?;
    authorize_warmup_task_application(
        &task,
        application_context
            .as_ref()
            .map(|application| application.id.clone())
            .or_else(|| application_id_from_headers(headers)),
    )?;
    Ok(task)
}

async fn mutate_authorized_warmup_task(
    registry: QueryServiceRegistry,
    headers: HeaderMap,
    task_id: String,
    mutation: WarmupMutation,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    let task_id = WarmupTaskId::new(task_id).map_err(ApiError)?;
    let task = load_authorized_warmup_task(registry.clone(), &headers, task_id.clone()).await?;
    let application_context = registry
        .authenticate_task_headers(&headers, ApplicationOperationConfig::WarmupMutate)
        .map_err(ApiError)?;
    authorize_warmup_task_application(
        &task,
        application_context
            .as_ref()
            .map(|application| application.id.clone())
            .or_else(|| application_id_from_headers(&headers)),
    )?;
    let task = tokio::task::spawn_blocking(move || match mutation {
        WarmupMutation::Pause => registry.pause_warmup_task(&task_id),
        WarmupMutation::Cancel => registry.cancel_warmup_task(&task_id),
        WarmupMutation::Retry => registry.retry_warmup_task(&task_id),
    })
    .await
    .map_err(|error| {
        ApiError(DatalensError::new(
            DatalensErrorKind::Internal,
            format!("warmup mutate task failed: {error}"),
        ))
    })?
    .map_err(ApiError)?;
    Ok(Json(WarmupTaskApiResponse {
        task: warmup_task_view(task).map_err(ApiError)?,
    }))
}

pub(crate) fn warmup_submit_request(
    application_id: String,
    request: WarmupSubmitApiRequest,
) -> Result<WarmupSubmitRequest, DatalensError> {
    Ok(WarmupSubmitRequest {
        application_id,
        chain: request.chain,
        dataset_key: request.dataset_key.into_dataset_key()?,
        selector: request.selector.into_selector()?,
        range_kind: request.range_kind,
        start: request.start,
        end: request.end,
        mode: request.mode,
        chunk_policy: WarmupChunkPolicy {
            max_range_len: request.chunk_policy.max_range_len.max(1),
            target_rows_hint: request.chunk_policy.target_rows_hint,
        },
        retry_policy: WarmupRetryPolicy {
            max_attempts: request.retry_policy.max_attempts.max(1),
            initial_backoff_ms: request.retry_policy.initial_backoff_ms,
            max_backoff_ms: request.retry_policy.max_backoff_ms,
        },
    })
}

fn authorize_warmup_task_application(
    task: &WarmupTask,
    application_id: Option<String>,
) -> Result<(), ApiError> {
    let Some(application_id) = application_id else {
        return Ok(());
    };
    if application_id != task.application_id {
        return Err(ApiError(DatalensError::new(
            DatalensErrorKind::Unauthorized,
            "application is not allowed to access another application's warmup task",
        )));
    }
    Ok(())
}

pub(crate) async fn metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize_metrics(&state, &headers)?;
    match state.registry.metrics_text() {
        Some(Ok(text)) => Ok((
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4",
            )],
            text,
        )
            .into_response()),
        Some(Err(error)) => Err(ApiError(error)),
        None => Ok(StatusCode::NOT_FOUND.into_response()),
    }
}

fn authorize_metrics(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state.edge.metrics.public || !state.registry.application_auth_required() {
        return Ok(());
    }
    let Some(token) = state
        .edge
        .metrics
        .bearer_token
        .as_deref()
        .filter(|token| !token.trim().is_empty())
    else {
        return Err(ApiError(DatalensError::new(
            DatalensErrorKind::Unauthorized,
            "metrics endpoint is not public",
        )));
    };
    if auth::bearer_token(headers) == Some(token) {
        return Ok(());
    }
    Err(ApiError(DatalensError::new(
        DatalensErrorKind::AuthenticationFailed,
        "metrics credentials are invalid",
    )))
}

pub(crate) struct ApiError(DatalensError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = api_error_status(&self.0.kind);
        log::warn!(
            "query response error status={} kind={:?}",
            status.as_u16(),
            self.0.kind
        );
        (status, Json(api_error_body(self.0))).into_response()
    }
}

pub(crate) fn application_from_headers(headers: &HeaderMap) -> Option<ApplicationIdentity> {
    headers
        .get(APPLICATION_IDENTITY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| ApplicationIdentity::from_optional(Some(value)))
}

pub(crate) fn application_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(APPLICATION_IDENTITY_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| auth::normalize_application_id(value).ok())
}
