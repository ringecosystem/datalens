//! Edge API boundary for datalens.

use std::{collections::BTreeMap, env, fs, net::SocketAddr, path::Path, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use datalens_chain::ChainAdapter;
use datalens_core::{
    BlockRange, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows,
    LedgerRange, LogFilter, QueryRows,
};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::{
    ApplicationIdentity, CacheCoverageOutcome, ErrorLabels, FillOutcome, MetricsLabels,
    MetricsRecorder, QueryOutcome,
};
use datalens_planner::{NativePlannerConfig, NativeQueryInput};
use datalens_storage::{S3ObjectStoreConfig, StorageRepository};
use datalens_writer::DurableWriterConfig;
use serde::{Deserialize, Serialize};

pub mod auth {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AuthContext {
        pub subject: Option<String>,
    }

    pub trait AuthenticationHook {
        fn authenticate(&self) -> AuthContext;
    }

    #[derive(Clone, Debug, Default)]
    pub struct NoAuthentication;

    impl AuthenticationHook for NoAuthentication {
        fn authenticate(&self) -> AuthContext {
            AuthContext { subject: None }
        }
    }
}

pub mod compatibility {
    pub trait CompatibilityAdapter {
        fn name(&self) -> &'static str;
    }

    #[derive(Clone, Debug, Default)]
    pub struct NativeCompatibility;

    impl CompatibilityAdapter for NativeCompatibility {
        fn name(&self) -> &'static str {
            "native"
        }
    }
}

pub mod http {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct HttpRoute {
        pub path: &'static str,
    }
}

pub mod native {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct NativeRoute {
        pub name: &'static str,
    }
}

pub mod streaming {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResponseStream {
        pub content_type: &'static str,
    }
}

pub mod config {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DatalensConfig {
        pub server: ServerConfig,
        pub storage: StorageConfig,
        pub planner: PlannerConfig,
        pub writer: WriterConfig,
        pub chains: BTreeMap<String, ChainConfig>,
    }

    impl DatalensConfig {
        pub fn from_file(path: impl AsRef<Path>) -> Result<Self, DatalensError> {
            let text = fs::read_to_string(path.as_ref()).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("read config {}: {error}", path.as_ref().display()),
                )
            })?;
            let expanded = expand_env_vars(&text)?;
            toml::from_str(&expanded).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("parse config: {error}"),
                )
            })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ServerConfig {
        pub bind: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub struct StorageConfig {
        pub backend: String,
        #[serde(default)]
        pub local: Option<LocalStorageConfig>,
        #[serde(default)]
        pub s3: Option<S3ObjectStoreConfig>,
    }

    #[derive(Deserialize)]
    struct RawStorageConfig {
        backend: String,
        #[serde(default)]
        root: Option<String>,
        #[serde(default)]
        local: Option<LocalStorageConfig>,
        #[serde(default)]
        s3: Option<S3ObjectStoreConfig>,
    }

    impl<'de> Deserialize<'de> for StorageConfig {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let raw = RawStorageConfig::deserialize(deserializer)?;
            let local = match (raw.local, raw.root) {
                (Some(local), _) => Some(local),
                (None, Some(root)) => Some(LocalStorageConfig { root }),
                (None, None) => None,
            };
            Ok(Self {
                backend: raw.backend,
                local,
                s3: raw.s3,
            })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct LocalStorageConfig {
        pub root: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PlannerConfig {
        pub max_query_range_blocks: u64,
        pub default_chunk_range_blocks: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct WriterConfig {
        pub target_object_bytes: u64,
        pub min_object_rows: usize,
        pub record_empty_coverage: bool,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ChainConfig {
        pub kind: String,
        pub chain_id: u64,
        pub rpc_urls: Vec<String>,
        #[serde(default)]
        pub finality: FinalityConfig,
        pub datasets: DatasetsConfig,
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "mode", rename_all = "snake_case")]
    pub enum FinalityConfig {
        #[default]
        Auto,
        Lag {
            #[serde(default)]
            safe_lag_blocks: Option<u64>,
            #[serde(default)]
            finalized_lag_blocks: Option<u64>,
        },
        RpcTags {
            safe_tag: String,
            finalized_tag: String,
        },
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DatasetsConfig {
        pub blocks: BlocksDatasetConfig,
        pub logs: LogsDatasetConfig,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct BlocksDatasetConfig {
        pub enabled: bool,
        pub max_batch_blocks: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct LogsDatasetConfig {
        pub enabled: bool,
        pub max_get_logs_range_blocks: u64,
        pub max_addresses_per_query: usize,
    }

    fn expand_env_vars(text: &str) -> Result<String, DatalensError> {
        let mut expanded = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(start) = rest.find("${") {
            expanded.push_str(&rest[..start]);
            let tail = &rest[start + 2..];
            let Some(end) = tail.find('}') else {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "unterminated environment variable placeholder",
                ));
            };
            let name = &tail[..end];
            let value = env::var(name).map_err(|_| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("missing environment variable {name}"),
                )
            })?;
            expanded.push_str(&value);
            rest = &tail[end + 1..];
        }
        expanded.push_str(rest);
        Ok(expanded)
    }
}

use config::{ChainConfig, PlannerConfig, WriterConfig};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LegacyEvmQueryRequest {
    pub chain: ChainIdentity,
    pub dataset: Dataset,
    pub range: BlockRange,
    pub filter: Option<LogFilter>,
    #[serde(default)]
    pub include_block: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheSummary {
    pub hit_ranges: Vec<BlockRange>,
    pub missing_ranges: Vec<BlockRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LegacyEvmQueryResponse {
    pub chain: ChainIdentity,
    pub range: BlockRange,
    pub cache: CacheSummary,
    pub rows: QueryRows,
}

pub fn legacy_evm_to_native_input(
    request: LegacyEvmQueryRequest,
) -> Result<NativeQueryInput, DatalensError> {
    let selector = match request.dataset {
        Dataset::Blocks => datalens_chain::DatasetSelector::all(),
        Dataset::Logs => {
            let filter = request.filter.ok_or_else(|| {
                DatalensError::new(DatalensErrorKind::InvalidInput, "logs require filter")
            })?;
            datalens_chain::DatasetSelector::try_evm_logs(filter)?
        }
    };
    let response_shape = match request.dataset {
        Dataset::Blocks => datalens_planner::ResponseShape::LegacyEvmBlocks,
        Dataset::Logs => datalens_planner::ResponseShape::LegacyEvmLogs,
    };

    Ok(NativeQueryInput {
        chain: request.chain,
        dataset_key: DatasetKey::from(request.dataset),
        ledger_range: LedgerRange::from_block_range(request.range),
        selector,
        response_shape,
        field_selection: datalens_planner::FieldSelection::All,
    })
}

#[derive(Clone)]
pub struct QueryService<S> {
    executor: NativeQueryExecutor<Arc<dyn StorageRepository>, S>,
    chain_name: String,
    chain: ChainConfig,
    metrics: Option<MetricsRecorder>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCacheSummary {
    pub hit_ranges: Vec<LedgerRange>,
    pub missing_ranges: Vec<LedgerRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeQueryResponse {
    pub chain: datalens_core::ChainIdentity,
    pub dataset_key: DatasetKey,
    pub ledger_range: LedgerRange,
    pub cache: NativeCacheSummary,
    pub rows: DatasetRows,
}

impl<S> QueryService<S>
where
    S: ChainAdapter,
{
    pub fn new(
        storage: impl StorageRepository + 'static,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain: ChainConfig,
    ) -> Self {
        Self::new_named(storage, source, planner, writer, "ethereum", chain)
    }

    pub fn new_named(
        storage: impl StorageRepository + 'static,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain_name: impl Into<String>,
        chain: ChainConfig,
    ) -> Self {
        let storage: Arc<dyn StorageRepository> = Arc::new(storage);
        let executor = NativeQueryExecutor::new(
            storage,
            source,
            NativeQueryExecutionConfig {
                planner: NativePlannerConfig {
                    max_query_range_len: planner.max_query_range_blocks,
                    default_chunk_range_len: planner.default_chunk_range_blocks,
                },
                writer: DurableWriterConfig {
                    target_object_bytes: writer.target_object_bytes,
                    min_object_rows: writer.min_object_rows,
                    record_empty_coverage: writer.record_empty_coverage,
                },
            },
        );

        Self {
            executor,
            chain_name: chain_name.into(),
            chain,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: MetricsRecorder) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn metrics_text(&self) -> Result<String, DatalensError> {
        let Some(metrics) = &self.metrics else {
            return Ok(String::new());
        };
        metrics.encode().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode metrics: {error}"),
            )
        })
    }

    pub fn chain_name(&self) -> &str {
        &self.chain_name
    }

    pub fn query(
        &self,
        request: LegacyEvmQueryRequest,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        let labels = metrics_labels(&request);
        log::info!(
            "legacy evm query start chain={} dataset={} range={}-{}",
            request.chain.configured_name(),
            request.dataset.as_str(),
            request.range.from_block,
            request.range.to_block
        );
        if let Err(error) = self.validate_legacy_evm_route(&request) {
            log::warn!("query validation failed kind={:?}", error.kind);
            self.record_query_error(&labels, &error.kind);
            return Err(error);
        }
        let response_range = request.range;
        self.record_requested(&labels, response_range.to_block);
        let response = match self.query_native(legacy_evm_to_native_input(request)?) {
            Ok(response) => response,
            Err(error) => {
                self.record_query_error(&labels, &error.kind);
                return Err(error);
            }
        };
        let hit_ranges = legacy_block_ranges(&response.cache.hit_ranges)?;
        let misses = legacy_block_ranges(&response.cache.missing_ranges)?;

        let response = LegacyEvmQueryResponse {
            chain: response.chain,
            range: response_range,
            cache: CacheSummary {
                hit_ranges,
                missing_ranges: misses,
            },
            rows: response.rows.into_rows(),
        };
        self.record_query_success(&labels, &response);
        Ok(response)
    }

    pub fn query_native(
        &self,
        native_input: NativeQueryInput,
    ) -> Result<NativeQueryResponse, DatalensError> {
        log::info!(
            "native query start chain={} dataset={} range={}-{}",
            native_input.chain.configured_name(),
            native_input.dataset_key.as_str(),
            native_input.ledger_range.start(),
            native_input.ledger_range.end()
        );
        let result = self.executor.execute(native_input)?;
        Ok(NativeQueryResponse {
            chain: result.chain,
            dataset_key: result.dataset_key,
            ledger_range: result.ledger_range,
            cache: NativeCacheSummary {
                hit_ranges: result.cache.hit_ranges,
                missing_ranges: result.cache.missing_ranges,
            },
            rows: result.rows,
        })
    }

    fn validate_legacy_evm_route(
        &self,
        request: &LegacyEvmQueryRequest,
    ) -> Result<(), DatalensError> {
        if request.chain.configured_name() != self.chain_name {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "chain is not configured",
            ));
        }
        if self.chain.kind != "evm" {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "only evm chains are supported",
            ));
        }

        match request.dataset {
            Dataset::Blocks if !self.chain.datasets.blocks.enabled => Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "blocks dataset is disabled",
            )),
            Dataset::Logs if !self.chain.datasets.logs.enabled => Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "logs dataset is disabled",
            )),
            Dataset::Logs => request.filter.as_ref().map(|_| ()).ok_or_else(|| {
                DatalensError::new(DatalensErrorKind::InvalidInput, "logs require filter")
            }),
            Dataset::Blocks => Ok(()),
        }
    }

    fn record_requested(&self, labels: &MetricsLabels, block: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.set_latest_requested_block(labels, block);
        }
    }

    fn record_query_success(&self, labels: &MetricsLabels, response: &LegacyEvmQueryResponse) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let coverage = match (
            response.cache.hit_ranges.is_empty(),
            response.cache.missing_ranges.is_empty(),
        ) {
            (_, true) => CacheCoverageOutcome::Hit,
            (true, false) => CacheCoverageOutcome::Miss,
            (false, false) => CacheCoverageOutcome::PartialHit,
        };
        metrics.record_cache_coverage(labels, coverage);

        if response.cache.missing_ranges.is_empty() {
            metrics.record_query(labels, QueryOutcome::Hit);
            return;
        }

        let latest_filled = response
            .cache
            .missing_ranges
            .iter()
            .map(|range| range.to_block)
            .max()
            .unwrap_or(response.range.to_block);
        metrics.set_latest_filled_block(labels, latest_filled);
        if response.rows.row_count() == 0 {
            metrics.record_query(labels, QueryOutcome::Empty);
            metrics.record_fill(labels, FillOutcome::Empty);
        } else {
            metrics.record_query(labels, QueryOutcome::Filled);
            metrics.record_fill(labels, FillOutcome::Filled);
        }
    }

    fn record_query_error(&self, labels: &MetricsLabels, kind: &DatalensErrorKind) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        metrics.record_query(labels, QueryOutcome::Error);
        match kind {
            DatalensErrorKind::ProviderFailure
            | DatalensErrorKind::ProviderLimit
            | DatalensErrorKind::ProviderTimeout
            | DatalensErrorKind::RateLimited => {
                metrics.record_provider_error(&ErrorLabels::from_labels(labels, kind.clone()));
            }
            DatalensErrorKind::StorageReadFailure
            | DatalensErrorKind::StorageWriteFailure
            | DatalensErrorKind::ManifestUpdateFailure => {
                metrics.record_storage_error(&ErrorLabels::from_labels(labels, kind.clone()));
            }
            DatalensErrorKind::InvalidInput
            | DatalensErrorKind::InvalidRequest
            | DatalensErrorKind::UnsupportedDataset
            | DatalensErrorKind::Internal => {}
        }
    }
}

trait RegisteredQueryService: Send + Sync {
    fn query(
        &self,
        request: LegacyEvmQueryRequest,
    ) -> Result<LegacyEvmQueryResponse, DatalensError>;

    fn metrics_text(&self) -> Result<String, DatalensError>;
}

impl<S> RegisteredQueryService for QueryService<S>
where
    S: ChainAdapter + 'static,
{
    fn query(
        &self,
        request: LegacyEvmQueryRequest,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        QueryService::query(self, request)
    }

    fn metrics_text(&self) -> Result<String, DatalensError> {
        QueryService::metrics_text(self)
    }
}

#[derive(Clone, Default)]
pub struct QueryServiceRegistry {
    services: BTreeMap<String, Arc<dyn RegisteredQueryService>>,
}

impl QueryServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_service<S>(mut self, service: QueryService<S>) -> Result<Self, DatalensError>
    where
        S: ChainAdapter + 'static,
    {
        let chain_name = service.chain_name().to_owned();
        if self.services.contains_key(&chain_name) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("chain {chain_name} is already registered"),
            ));
        }
        self.services.insert(chain_name, Arc::new(service));
        Ok(self)
    }

    pub fn chain_names(&self) -> Vec<String> {
        self.services.keys().cloned().collect()
    }

    pub fn query(
        &self,
        request: LegacyEvmQueryRequest,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        let chain_name = request.chain.configured_name();
        let service = self.services.get(chain_name).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("chain {chain_name} is not configured"),
            )
        })?;
        service.query(request)
    }

    pub fn metrics_text(&self) -> Result<String, DatalensError> {
        for service in self.services.values() {
            let metrics = service.metrics_text()?;
            if !metrics.is_empty() {
                return Ok(metrics);
            }
        }
        Ok(String::new())
    }
}

pub fn router(registry: QueryServiceRegistry) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/chains", get(chains))
        .route("/v1/query", post(query))
        .with_state(AppState { registry })
}

#[derive(Clone)]
struct AppState {
    registry: QueryServiceRegistry,
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn chains(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "chains": state.registry.chain_names() }))
}

async fn metrics(State(state): State<AppState>) -> Result<String, ApiError> {
    state.registry.metrics_text().map_err(ApiError)
}

async fn query(
    State(state): State<AppState>,
    Json(request): Json<LegacyEvmQueryRequest>,
) -> Result<Json<LegacyEvmQueryResponse>, ApiError> {
    let registry = state.registry.clone();
    tokio::task::spawn_blocking(move || registry.query(request))
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

fn metrics_labels(request: &LegacyEvmQueryRequest) -> MetricsLabels {
    MetricsLabels::new(
        ApplicationIdentity::unknown(),
        request.chain.clone(),
        request.dataset,
    )
}

struct ApiError(DatalensError);

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ApiErrorBody {
    pub error: ApiErrorDetail,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ApiErrorDetail {
    pub kind: &'static str,
    pub message: String,
}

pub fn api_error_status(kind: &DatalensErrorKind) -> StatusCode {
    match kind {
        DatalensErrorKind::InvalidInput | DatalensErrorKind::InvalidRequest => {
            StatusCode::BAD_REQUEST
        }
        DatalensErrorKind::UnsupportedDataset => StatusCode::UNPROCESSABLE_ENTITY,
        DatalensErrorKind::ProviderLimit | DatalensErrorKind::RateLimited => {
            StatusCode::TOO_MANY_REQUESTS
        }
        DatalensErrorKind::ProviderTimeout => StatusCode::GATEWAY_TIMEOUT,
        DatalensErrorKind::ProviderFailure => StatusCode::BAD_GATEWAY,
        DatalensErrorKind::StorageReadFailure
        | DatalensErrorKind::StorageWriteFailure
        | DatalensErrorKind::ManifestUpdateFailure
        | DatalensErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn api_error_body(error: DatalensError) -> ApiErrorBody {
    ApiErrorBody {
        error: ApiErrorDetail {
            kind: api_error_kind(&error.kind),
            message: error.message,
        },
    }
}

fn api_error_kind(kind: &DatalensErrorKind) -> &'static str {
    match kind {
        DatalensErrorKind::InvalidInput => "invalid_input",
        DatalensErrorKind::InvalidRequest => "invalid_request",
        DatalensErrorKind::UnsupportedDataset => "unsupported_dataset",
        DatalensErrorKind::ProviderFailure => "provider_failure",
        DatalensErrorKind::ProviderLimit => "provider_limit",
        DatalensErrorKind::ProviderTimeout => "provider_timeout",
        DatalensErrorKind::RateLimited => "rate_limited",
        DatalensErrorKind::StorageReadFailure => "storage_read_failure",
        DatalensErrorKind::StorageWriteFailure => "storage_write_failure",
        DatalensErrorKind::ManifestUpdateFailure => "manifest_update_failure",
        DatalensErrorKind::Internal => "internal",
    }
}

pub async fn serve(bind: SocketAddr, registry: QueryServiceRegistry) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    log::info!("api listener bound to {bind}");
    axum::serve(listener, router(registry)).await
}

fn legacy_block_ranges(ranges: &[LedgerRange]) -> Result<Vec<BlockRange>, DatalensError> {
    ranges
        .iter()
        .map(|range| {
            range.block_range().ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    "native plan returned non-block range for legacy evm response",
                )
            })
        })
        .collect()
}
