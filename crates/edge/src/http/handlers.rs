use std::{collections::BTreeMap, time::Instant};

use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_executor::generate_query_id;
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
        error::{api_error_body, api_error_status, api_retry_after_seconds},
        head::{ChainHeadApiResponse, ChainHeadFinalityApi},
        query::{QueryApiRequest, QueryApiResponse, QueryRangeApi},
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

pub(crate) async fn chain_head(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(chain): AxumPath<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Result<Json<ChainHeadApiResponse>, ApiError> {
    let registry = state.registry.clone();
    let application_authentication = registry
        .authenticate_chain_head_headers(&headers)
        .map_err(ApiError)?;
    let finality =
        ChainHeadFinalityApi::parse(query.get("finality").map(String::as_str)).map_err(ApiError)?;
    let configured_chain = registry.configured_chain_name(&chain).map_err(ApiError)?;
    registry
        .authorize_chain_head_application(&headers, &application_authentication, &configured_chain)
        .map_err(ApiError)?;
    tokio::task::spawn_blocking(move || registry.chain_head(&chain, finality))
        .await
        .map_err(|error| {
            ApiError(DatalensError::new(
                DatalensErrorKind::Internal,
                format!("chain head task failed: {error}"),
            ))
        })?
        .map(Json)
        .map_err(ApiError)
}

pub(crate) async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryApiRequest>,
) -> Result<Json<QueryApiResponse>, ApiError> {
    let query_id = generate_query_id();
    let handler_start = Instant::now();
    let registry = state.registry.clone();
    let chain = request.chain.configured_name().to_owned();
    let requested_dataset = request.dataset_key.clone();
    let (range_start, range_end) = query_range_bounds(&request.range);
    log::info!(
        "query api request start query_id={} chain={} dataset={} range={}-{}",
        query_id,
        chain,
        requested_dataset,
        range_start,
        range_end
    );
    let dataset = match request.dataset_for_auth() {
        Ok(dataset) => dataset,
        Err(error) => {
            log::warn!(
                "query api request failed query_id={} chain={} dataset={} range={}-{} stage=dataset_validation duration_ms={} kind={:?} message={}",
                query_id,
                chain,
                requested_dataset,
                range_start,
                range_end,
                handler_start.elapsed().as_millis(),
                error.kind,
                error.message
            );
            return Err(ApiError(error));
        }
    };
    // Authentication identifies the caller; authorization and quota checks bind
    // that identity to the requested chain, dataset, range, and finality.
    let application_context = match registry.authenticate_headers(
        &headers,
        request.chain.configured_name(),
        &dataset,
        request.range_len(),
        request.finality,
    ) {
        Ok(application_context) => application_context,
        Err(error) => {
            log::warn!(
                "query api request failed query_id={} chain={} dataset={} range={}-{} stage=authentication duration_ms={} kind={:?} message={}",
                query_id,
                chain,
                dataset,
                range_start,
                range_end,
                handler_start.elapsed().as_millis(),
                error.kind,
                error.message
            );
            return Err(ApiError(error));
        }
    };
    let application = application_context
        .as_ref()
        .map(|application| application.metrics_identity())
        .or_else(|| application_from_headers(&headers));
    let native_input = match request.into_native_input() {
        Ok(native_input) => native_input,
        Err(error) => {
            log::warn!(
                "query api request failed query_id={} chain={} dataset={} range={}-{} stage=native_input duration_ms={} kind={:?} message={}",
                query_id,
                chain,
                dataset,
                range_start,
                range_end,
                handler_start.elapsed().as_millis(),
                error.kind,
                error.message
            );
            return Err(ApiError(error));
        }
    };
    // The blocking native executor is shared with GraphQL; both API surfaces
    // pass the same application identity into metrics and the usage ledger.
    let task_query_id = query_id.clone();
    let task_chain = chain.clone();
    let task_dataset = dataset.clone();
    let task_result = tokio::task::spawn_blocking(move || {
        let executor_start = Instant::now();
        let native_response = match registry.query_native_with_application_and_query_id(
            native_input,
            application,
            task_query_id.clone(),
        ) {
            Ok(response) => {
                log::info!(
                    "query api executor completed query_id={} chain={} dataset={} range={}-{} duration_ms={}",
                    task_query_id,
                    task_chain,
                    task_dataset,
                    range_start,
                    range_end,
                    executor_start.elapsed().as_millis()
                );
                response
            }
            Err(error) => {
                log::warn!(
                    "query api executor failed query_id={} chain={} dataset={} range={}-{} duration_ms={} kind={:?} message={}",
                    task_query_id,
                    task_chain,
                    task_dataset,
                    range_start,
                    range_end,
                    executor_start.elapsed().as_millis(),
                    error.kind,
                    error.message
                );
                return Err(error);
            }
        };
        let conversion_start = Instant::now();
        match QueryApiResponse::try_from_native_response(native_response) {
            Ok(response) => {
                log::info!(
                    "query api response conversion completed query_id={} chain={} dataset={} range={}-{} rows={} duration_ms={}",
                    task_query_id,
                    task_chain,
                    task_dataset,
                    range_start,
                    range_end,
                    response.rows.row_count(),
                    conversion_start.elapsed().as_millis()
                );
                Ok(response)
            }
            Err(error) => {
                log::warn!(
                    "query api response conversion failed query_id={} chain={} dataset={} range={}-{} duration_ms={} kind={:?} message={}",
                    task_query_id,
                    task_chain,
                    task_dataset,
                    range_start,
                    range_end,
                    conversion_start.elapsed().as_millis(),
                    error.kind,
                    error.message
                );
                Err(error)
            }
        }
    })
    .await
    .map_err(|error| {
        let error = DatalensError::new(
            DatalensErrorKind::Internal,
            format!("query task failed: {error}"),
        );
        log::warn!(
            "query api request failed query_id={} chain={} dataset={} range={}-{} stage=join duration_ms={} kind={:?} message={}",
            query_id,
            chain,
            dataset,
            range_start,
            range_end,
            handler_start.elapsed().as_millis(),
            error.kind,
            error.message
        );
        ApiError(error)
    })?;
    match task_result {
        Ok(response) => {
            log::info!(
                "query api request completed query_id={} chain={} dataset={} range={}-{} rows={} total_duration_ms={}",
                query_id,
                chain,
                dataset,
                range_start,
                range_end,
                response.rows.row_count(),
                handler_start.elapsed().as_millis()
            );
            Ok(Json(response))
        }
        Err(error) => {
            log::warn!(
                "query api request failed query_id={} chain={} dataset={} range={}-{} stage=executor duration_ms={} kind={:?} message={}",
                query_id,
                chain,
                dataset,
                range_start,
                range_end,
                handler_start.elapsed().as_millis(),
                error.kind,
                error.message
            );
            Err(ApiError(error))
        }
    }
}

fn query_range_bounds(range: &QueryRangeApi) -> (u64, u64) {
    match *range {
        QueryRangeApi::Block { start, end }
        | QueryRangeApi::Slot { start, end }
        | QueryRangeApi::Height { start, end } => (start, end),
    }
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
    // Warmup task ownership is stored as the normalized application id, not the
    // bearer subject, so later list/mutate checks can use the same route policy.
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
        let error = self.0;
        let status = api_error_status(&error.kind);
        let retry_after_seconds = api_retry_after_seconds(&error);
        log::warn!(
            "query response error status={} kind={:?}",
            status.as_u16(),
            error.kind
        );
        let mut response = (status, Json(api_error_body(error))).into_response();
        if let Some(seconds) = retry_after_seconds
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
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
