use async_graphql::{
    Context, EmptySubscription, Error, ErrorExtensions, ID, Json, Object, Schema, SimpleObject,
    Value as GraphqlValue, http::GraphiQLSource,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::State,
    http::HeaderMap,
    response::{Html, IntoResponse, Response},
};
use datalens_cache_repair::{CacheRepairRunResult, CacheRepairTaskFilter, CacheRepairTaskId};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetRows, LedgerRangeKind,
};
use datalens_warmup::{WarmupRunResult, WarmupTaskFilter, WarmupTaskId};
use serde::Serialize;

use crate::{
    config::ApplicationOperationConfig,
    contract::error::{api_error_kind, api_error_status},
    contract::{
        cache_repair::{
            CacheRepairRunOnceApiResponse, CacheRepairSubmitApiResponse, CacheRepairTaskView,
            cache_repair_task_view,
        },
        discovery::{ChainDiscovery, DatasetDiscovery, DiscoveryResponse},
        query::{QueryApiResponse, QueryCacheApi, QueryRangeApi},
        warmup::{
            WarmupRunOnceApiResponse, WarmupSubmitApiResponse, WarmupTaskView, warmup_task_view,
        },
    },
    http::{
        AppState,
        handlers::{
            application_from_headers, application_id_from_headers, cache_repair_submit_request,
            warmup_submit_request,
        },
    },
    service::registry::QueryServiceRegistry,
};

use super::input::{
    CacheRepairSubmitInput, CacheRepairTaskFilterInput, QueryInput, WarmupSubmitInput,
    WarmupTaskFilterInput,
};

pub(crate) type DatalensGraphqlSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub(crate) fn schema(registry: QueryServiceRegistry) -> DatalensGraphqlSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(registry)
        .finish()
}

pub fn native_graphql_schema_sdl() -> String {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .finish()
        .sdl()
}

pub(crate) async fn graphql_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> GraphQLResponse {
    let Some(schema) = state.native_graphql_schema else {
        return Schema::build(QueryRoot, MutationRoot, EmptySubscription)
            .finish()
            .execute(async_graphql::Request::new("{ __typename }"))
            .await
            .into();
    };
    schema
        .execute(request.into_inner().data(headers))
        .await
        .into()
}

pub(crate) async fn playground(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.registry.authenticate_discovery_headers(&headers) {
        return (
            api_error_status(&error.kind),
            axum::Json(crate::contract::error::api_error_body(error)),
        )
            .into_response();
    }
    Html(
        GraphiQLSource::build()
            .endpoint(&state.edge.query.native.path)
            .finish(),
    )
    .into_response()
}

pub(crate) struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn chains(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<String>> {
        let registry = registry(ctx)?;
        let _application_context = registry
            .authenticate_discovery_headers(&headers(ctx))
            .map_err(graphql_error)?;
        Ok(registry.chain_names())
    }

    async fn discovery(&self, ctx: &Context<'_>) -> async_graphql::Result<Discovery> {
        let registry = registry(ctx)?.clone();
        let _application_context = registry
            .authenticate_discovery_headers(&headers(ctx))
            .map_err(graphql_error)?;
        let response = spawn_graphql_blocking(move || registry.discovery()).await?;
        Ok(response.into())
    }

    async fn query(
        &self,
        ctx: &Context<'_>,
        input: QueryInput,
    ) -> async_graphql::Result<QueryResponse> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let request = input.into_request()?;
        let application_context = registry
            .authenticate_native_query_headers(&headers, &request)
            .map_err(graphql_error)?;
        // GraphQL uses the same native query request and application context as
        // REST so cache policy, quotas, and attribution do not diverge by API.
        let application = application_context
            .as_ref()
            .map(|application| application.metrics_identity())
            .or_else(|| application_from_headers(&headers));
        let native_input = request.clone().into_native_input().map_err(graphql_error)?;
        let response = spawn_graphql_blocking(move || {
            registry.query_native_with_application(native_input, application)
        })
        .await?;
        Ok(QueryApiResponse::try_from_native_response(response)
            .map_err(graphql_error)?
            .into())
    }

    async fn warmup_task(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<Option<WarmupTask>> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let task_id = WarmupTaskId::new(id.to_string()).map_err(graphql_error)?;
        let application_context = registry
            .authenticate_task_headers(&headers, ApplicationOperationConfig::WarmupRead)
            .map_err(graphql_error)?;
        let application_id = application_context
            .as_ref()
            .map(|application| application.id.clone())
            .or_else(|| application_id_from_headers(&headers));
        let task = spawn_graphql_blocking(move || registry.get_warmup_task(&task_id)).await?;
        task.map(|task| {
            authorize_warmup_task_application(&task, application_id)?;
            warmup_task_view(task).map(WarmupTask::from)
        })
        .transpose()
        .map_err(graphql_error)
    }

    async fn warmup_tasks(
        &self,
        ctx: &Context<'_>,
        filter: Option<WarmupTaskFilterInput>,
    ) -> async_graphql::Result<Vec<WarmupTask>> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let application_context = registry
            .authenticate_task_headers(&headers, ApplicationOperationConfig::WarmupRead)
            .map_err(graphql_error)?;
        let mut filter = match filter {
            Some(filter) => filter.into_filter()?,
            None => WarmupTaskFilter::default(),
        };
        if filter.application_id.is_none() {
            filter.application_id = application_context
                .as_ref()
                .map(|application| application.id.clone())
                .or_else(|| application_id_from_headers(&headers));
        }
        let tasks = spawn_graphql_blocking(move || registry.list_warmup_tasks(filter)).await?;
        tasks
            .into_iter()
            .map(warmup_task_view)
            .map(|result| result.map(WarmupTask::from))
            .collect::<Result<Vec<_>, _>>()
            .map_err(graphql_error)
    }

    async fn cache_repair_task(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<Option<CacheRepairTask>> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let task_id = CacheRepairTaskId::new(id.to_string()).map_err(graphql_error)?;
        let application_context = registry
            .authenticate_task_headers(&headers, ApplicationOperationConfig::CacheRepairRead)
            .map_err(graphql_error)?;
        let application_id = application_context
            .as_ref()
            .map(|application| application.id.clone())
            .or_else(|| application_id_from_headers(&headers));
        let task = spawn_graphql_blocking(move || registry.get_cache_repair_task(&task_id)).await?;
        task.map(|task| {
            authorize_cache_repair_task_application(&task, application_id)?;
            cache_repair_task_view(task).map(CacheRepairTask::from)
        })
        .transpose()
        .map_err(graphql_error)
    }

    async fn cache_repair_tasks(
        &self,
        ctx: &Context<'_>,
        filter: Option<CacheRepairTaskFilterInput>,
    ) -> async_graphql::Result<Vec<CacheRepairTask>> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let application_context = registry
            .authenticate_task_headers(&headers, ApplicationOperationConfig::CacheRepairRead)
            .map_err(graphql_error)?;
        let mut filter = match filter {
            Some(filter) => filter.into_filter()?,
            None => CacheRepairTaskFilter::default(),
        };
        if filter.application_id.is_none() {
            filter.application_id = application_context
                .as_ref()
                .map(|application| application.id.clone())
                .or_else(|| application_id_from_headers(&headers));
        }
        let tasks =
            spawn_graphql_blocking(move || registry.list_cache_repair_tasks(filter)).await?;
        tasks
            .into_iter()
            .map(cache_repair_task_view)
            .map(|result| result.map(CacheRepairTask::from))
            .collect::<Result<Vec<_>, _>>()
            .map_err(graphql_error)
    }
}

pub(crate) struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn submit_warmup_task(
        &self,
        ctx: &Context<'_>,
        input: WarmupSubmitInput,
    ) -> async_graphql::Result<WarmupSubmitPayload> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let request = input.into_request()?;
        let dataset = request.dataset_for_auth().map_err(graphql_error)?;
        let application_context = registry
            .authenticate_warmup_headers(
                &headers,
                request.chain().configured_name(),
                &dataset,
                ApplicationOperationConfig::WarmupSubmit,
            )
            .map_err(graphql_error)?;
        let application_id = application_context
            .as_ref()
            .map(|application| application.id.clone())
            .or_else(|| application_id_from_headers(&headers))
            .unwrap_or_else(|| "unknown".to_owned());
        let request = warmup_submit_request(application_id, request).map_err(graphql_error)?;
        let outcome = spawn_graphql_blocking(move || registry.submit_warmup_task(request)).await?;
        Ok(WarmupSubmitApiResponse {
            task_id: outcome.task_id,
            created: outcome.created,
        }
        .into())
    }

    async fn pause_warmup_task(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<WarmupTask> {
        mutate_warmup_task(ctx, id, WarmupMutation::Pause).await
    }

    async fn cancel_warmup_task(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<WarmupTask> {
        mutate_warmup_task(ctx, id, WarmupMutation::Cancel).await
    }

    async fn retry_warmup_task(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<WarmupTask> {
        mutate_warmup_task(ctx, id, WarmupMutation::Retry).await
    }

    async fn run_warmup_once(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<WarmupRunOncePayload> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let _application_context = registry
            .authenticate_task_headers(&headers, ApplicationOperationConfig::WarmupRun)
            .map_err(graphql_error)?;
        let results = spawn_graphql_blocking(move || registry.run_warmup_once()).await?;
        Ok(WarmupRunOnceApiResponse { results }.into())
    }

    async fn submit_cache_repair_task(
        &self,
        ctx: &Context<'_>,
        input: CacheRepairSubmitInput,
    ) -> async_graphql::Result<CacheRepairSubmitPayload> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let request = input.into_request()?;
        let dataset = request.dataset_for_auth().map_err(graphql_error)?;
        let application_context = registry
            .authenticate_warmup_headers(
                &headers,
                request.chain().configured_name(),
                &dataset,
                ApplicationOperationConfig::CacheRepairSubmit,
            )
            .map_err(graphql_error)?;
        let application_id = application_context
            .as_ref()
            .map(|application| application.id.clone())
            .or_else(|| application_id_from_headers(&headers))
            .unwrap_or_else(|| "unknown".to_owned());
        let request =
            cache_repair_submit_request(application_id, request).map_err(graphql_error)?;
        let outcome =
            spawn_graphql_blocking(move || registry.submit_cache_repair_task(request)).await?;
        Ok(CacheRepairSubmitApiResponse {
            task_id: outcome.task_id,
            created: outcome.created,
        }
        .into())
    }

    async fn cancel_cache_repair_task(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<CacheRepairTask> {
        mutate_cache_repair_task(ctx, id, CacheRepairMutation::Cancel).await
    }

    async fn retry_cache_repair_task(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<CacheRepairTask> {
        mutate_cache_repair_task(ctx, id, CacheRepairMutation::Retry).await
    }

    async fn run_cache_repair_once(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<CacheRepairRunOncePayload> {
        let registry = registry(ctx)?.clone();
        let headers = headers(ctx);
        let _application_context = registry
            .authenticate_task_headers(&headers, ApplicationOperationConfig::CacheRepairRun)
            .map_err(graphql_error)?;
        let results = spawn_graphql_blocking(move || registry.run_cache_repair_once()).await?;
        Ok(CacheRepairRunOnceApiResponse { results }.into())
    }
}

#[derive(SimpleObject)]
pub(crate) struct Discovery {
    chains: Vec<ChainDiscoveryGraphql>,
}

impl From<DiscoveryResponse> for Discovery {
    fn from(response: DiscoveryResponse) -> Self {
        Self {
            chains: response.chains.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct ChainDiscoveryGraphql {
    identity: Json<ChainIdentity>,
    datasets: Vec<DatasetDiscoveryGraphql>,
}

impl From<ChainDiscovery> for ChainDiscoveryGraphql {
    fn from(discovery: ChainDiscovery) -> Self {
        Self {
            identity: Json(discovery.identity),
            datasets: discovery.datasets.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct DatasetDiscoveryGraphql {
    dataset_key: String,
    range_kinds: Json<Vec<LedgerRangeKind>>,
    selectors: Vec<String>,
    enabled: bool,
}

impl From<DatasetDiscovery> for DatasetDiscoveryGraphql {
    fn from(discovery: DatasetDiscovery) -> Self {
        Self {
            dataset_key: discovery.dataset_key,
            range_kinds: Json(discovery.range_kinds),
            selectors: discovery.selectors,
            enabled: discovery.enabled,
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct QueryResponse {
    chain: Json<ChainIdentity>,
    dataset_key: String,
    range: Json<QueryRangeApi>,
    cache: Json<QueryCacheApi>,
    rows: Json<DatasetRows>,
}

impl From<QueryApiResponse> for QueryResponse {
    fn from(response: QueryApiResponse) -> Self {
        Self {
            chain: Json(response.chain),
            dataset_key: response.dataset_key,
            range: Json(response.range),
            cache: Json(response.cache),
            rows: Json(response.rows),
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct WarmupSubmitPayload {
    task_id: ID,
    created: bool,
}

impl From<WarmupSubmitApiResponse> for WarmupSubmitPayload {
    fn from(response: WarmupSubmitApiResponse) -> Self {
        Self {
            task_id: ID::from(response.task_id.as_str().to_owned()),
            created: response.created,
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct WarmupTask {
    task_id: ID,
    application_id: String,
    chain: Json<ChainIdentity>,
    dataset_key: String,
    selector: WarmupTaskSelector,
    range_kind: Json<LedgerRangeKind>,
    start: u64,
    end: Option<u64>,
    mode: String,
    state: String,
    created_at: u64,
    updated_at: u64,
    last_error: Option<String>,
    stats: Json<datalens_warmup::WarmupStats>,
    query_watermark: Option<u64>,
    cursor_next: Option<u64>,
    cursor_query_distance: Option<u64>,
    safe_head: Option<u64>,
    lookahead_blocks: Option<u64>,
    planned_start: Option<u64>,
    planned_end: Option<u64>,
    planned_query_distance: Option<u64>,
    no_op_reason: Option<String>,
    published_coverage_end: Option<u64>,
    published_query_distance: Option<u64>,
}

#[derive(SimpleObject)]
pub(crate) struct WarmupTaskSelector {
    kind: String,
    fingerprint: String,
    canonical_key: String,
}

impl From<WarmupTaskView> for WarmupTask {
    fn from(task: WarmupTaskView) -> Self {
        Self {
            task_id: ID::from(task.task_id.as_str().to_owned()),
            application_id: task.application_id,
            chain: Json(task.chain),
            dataset_key: task.dataset_key,
            selector: WarmupTaskSelector {
                kind: task.selector.kind,
                fingerprint: task.selector.fingerprint,
                canonical_key: task.selector.canonical_key,
            },
            range_kind: Json(task.range_kind),
            start: task.start,
            end: task.end,
            mode: serde_json_string(task.mode),
            state: serde_json_string(task.state),
            created_at: task.created_at,
            updated_at: task.updated_at,
            last_error: task.last_error,
            stats: Json(task.stats),
            query_watermark: task.query_watermark,
            cursor_next: task.cursor_next,
            cursor_query_distance: task.cursor_query_distance,
            safe_head: task.safe_head,
            lookahead_blocks: task.lookahead_blocks,
            planned_start: task.planned_start,
            planned_end: task.planned_end,
            planned_query_distance: task.planned_query_distance,
            no_op_reason: task.no_op_reason,
            published_coverage_end: task.published_coverage_end,
            published_query_distance: task.published_query_distance,
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct WarmupRunOncePayload {
    results: Json<Vec<WarmupRunResult>>,
}

#[derive(SimpleObject)]
pub(crate) struct CacheRepairSubmitPayload {
    task_id: ID,
    created: bool,
}

impl From<CacheRepairSubmitApiResponse> for CacheRepairSubmitPayload {
    fn from(response: CacheRepairSubmitApiResponse) -> Self {
        Self {
            task_id: ID::from(response.task_id.as_str().to_owned()),
            created: response.created,
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct CacheRepairTask {
    task_id: ID,
    application_id: String,
    chain: Json<ChainIdentity>,
    dataset_key: String,
    selector: WarmupTaskSelector,
    range_kind: Json<LedgerRangeKind>,
    start: u64,
    end: u64,
    finality: String,
    state: String,
    created_at: u64,
    updated_at: u64,
    last_error: Option<String>,
    stats: Json<datalens_cache_repair::CacheRepairStats>,
    reason: String,
}

impl From<CacheRepairTaskView> for CacheRepairTask {
    fn from(task: CacheRepairTaskView) -> Self {
        Self {
            task_id: ID::from(task.task_id.as_str().to_owned()),
            application_id: task.application_id,
            chain: Json(task.chain),
            dataset_key: task.dataset_key,
            selector: WarmupTaskSelector {
                kind: task.selector.kind,
                fingerprint: task.selector.fingerprint,
                canonical_key: task.selector.canonical_key,
            },
            range_kind: Json(task.range_kind),
            start: task.start,
            end: task.end,
            finality: serde_json_string(task.finality),
            state: serde_json_string(task.state),
            created_at: task.created_at,
            updated_at: task.updated_at,
            last_error: task.last_error,
            stats: Json(task.stats),
            reason: task.reason,
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct CacheRepairRunOncePayload {
    results: Json<Vec<CacheRepairRunResult>>,
}

impl From<CacheRepairRunOnceApiResponse> for CacheRepairRunOncePayload {
    fn from(response: CacheRepairRunOnceApiResponse) -> Self {
        Self {
            results: Json(response.results),
        }
    }
}

impl From<WarmupRunOnceApiResponse> for WarmupRunOncePayload {
    fn from(response: WarmupRunOnceApiResponse) -> Self {
        Self {
            results: Json(response.results),
        }
    }
}

#[derive(Clone, Copy)]
enum WarmupMutation {
    Pause,
    Cancel,
    Retry,
}

#[derive(Clone, Copy)]
enum CacheRepairMutation {
    Cancel,
    Retry,
}

async fn mutate_warmup_task(
    ctx: &Context<'_>,
    id: ID,
    mutation: WarmupMutation,
) -> async_graphql::Result<WarmupTask> {
    let registry = registry(ctx)?.clone();
    let headers = headers(ctx);
    let task_id = WarmupTaskId::new(id.to_string()).map_err(graphql_error)?;
    let application_context = registry
        .authenticate_task_headers(&headers, ApplicationOperationConfig::WarmupMutate)
        .map_err(graphql_error)?;
    let application_id = application_context
        .as_ref()
        .map(|application| application.id.clone())
        .or_else(|| application_id_from_headers(&headers));
    let current_task_id = task_id.clone();
    let current_registry = registry.clone();
    let current_task =
        spawn_graphql_blocking(move || current_registry.get_warmup_task(&current_task_id)).await?;
    let current_task = current_task.ok_or_else(|| {
        graphql_error(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("warmup task {} not found", task_id.as_str()),
        ))
    })?;
    // Mutations first load the current task, then authorize against its owner;
    // ownership is a warmup task boundary, not a GraphQL session property.
    authorize_warmup_task_application(&current_task, application_id).map_err(graphql_error)?;
    let task = spawn_graphql_blocking(move || match mutation {
        WarmupMutation::Pause => registry.pause_warmup_task(&task_id),
        WarmupMutation::Cancel => registry.cancel_warmup_task(&task_id),
        WarmupMutation::Retry => registry.retry_warmup_task(&task_id),
    })
    .await?;
    warmup_task_view(task)
        .map(WarmupTask::from)
        .map_err(graphql_error)
}

fn authorize_warmup_task_application(
    task: &datalens_warmup::WarmupTask,
    application_id: Option<String>,
) -> Result<(), DatalensError> {
    let Some(application_id) = application_id else {
        return Ok(());
    };
    if application_id != task.application_id {
        return Err(DatalensError::new(
            DatalensErrorKind::Unauthorized,
            "application is not allowed to access another application's warmup task",
        ));
    }
    Ok(())
}

async fn mutate_cache_repair_task(
    ctx: &Context<'_>,
    id: ID,
    mutation: CacheRepairMutation,
) -> async_graphql::Result<CacheRepairTask> {
    let registry = registry(ctx)?.clone();
    let headers = headers(ctx);
    let task_id = CacheRepairTaskId::new(id.to_string()).map_err(graphql_error)?;
    let application_context = registry
        .authenticate_task_headers(&headers, ApplicationOperationConfig::CacheRepairMutate)
        .map_err(graphql_error)?;
    let application_id = application_context
        .as_ref()
        .map(|application| application.id.clone())
        .or_else(|| application_id_from_headers(&headers));
    let current_task_id = task_id.clone();
    let current_registry = registry.clone();
    let current_task =
        spawn_graphql_blocking(move || current_registry.get_cache_repair_task(&current_task_id))
            .await?;
    let current_task = current_task.ok_or_else(|| {
        graphql_error(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("cache repair task {} not found", task_id.as_str()),
        ))
    })?;
    authorize_cache_repair_task_application(&current_task, application_id)
        .map_err(graphql_error)?;
    let task = spawn_graphql_blocking(move || match mutation {
        CacheRepairMutation::Cancel => registry.cancel_cache_repair_task(&task_id),
        CacheRepairMutation::Retry => registry.retry_cache_repair_task(&task_id),
    })
    .await?;
    cache_repair_task_view(task)
        .map(CacheRepairTask::from)
        .map_err(graphql_error)
}

fn authorize_cache_repair_task_application(
    task: &datalens_cache_repair::CacheRepairTask,
    application_id: Option<String>,
) -> Result<(), DatalensError> {
    let Some(application_id) = application_id else {
        return Ok(());
    };
    if application_id != task.application_id {
        return Err(DatalensError::new(
            DatalensErrorKind::Unauthorized,
            "application is not allowed to access another application's cache repair task",
        ));
    }
    Ok(())
}

async fn spawn_graphql_blocking<T>(
    operation: impl FnOnce() -> Result<T, DatalensError> + Send + 'static,
) -> async_graphql::Result<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            graphql_error(DatalensError::new(
                DatalensErrorKind::Internal,
                format!("graphql task failed: {error}"),
            ))
        })?
        .map_err(graphql_error)
}

fn registry<'a>(ctx: &'a Context<'_>) -> async_graphql::Result<&'a QueryServiceRegistry> {
    ctx.data::<QueryServiceRegistry>().map_err(|_| {
        graphql_error(DatalensError::new(
            DatalensErrorKind::Internal,
            "graphql registry is not configured",
        ))
    })
}

fn headers(ctx: &Context<'_>) -> HeaderMap {
    ctx.data_opt::<HeaderMap>().cloned().unwrap_or_default()
}

pub(crate) fn graphql_error(error: DatalensError) -> Error {
    let kind = error.kind;
    let status = api_error_status(&kind).as_u16();
    let quota = error.quota;
    Error::new(error.message).extend_with(move |_error, extension| {
        extension.set("kind", api_error_kind(&kind));
        extension.set("status", i32::from(status));
        if let Some(quota) = &quota
            && let Ok(value) = serde_json::to_value(quota).and_then(GraphqlValue::from_json)
        {
            extension.set("quota", value);
        }
    })
}

fn serde_json_string<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}
