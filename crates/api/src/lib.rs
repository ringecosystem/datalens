//! Edge API boundary for datalens.

use std::{collections::BTreeMap, env, fs, net::SocketAddr, path::Path};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use datalens_core::{
    BlockHeader, BlockRange, CacheSummary, DatalensError, DatalensErrorKind, Dataset, EvmLogFilter,
    LogFilter, LogRecord, QueryRequest, QueryResponse, QueryRows,
};
use datalens_storage::{LocalStorage, missing_ranges};
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

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct StorageConfig {
        pub backend: String,
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
        pub safe_height_lag_blocks: u64,
        pub datasets: DatasetsConfig,
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

pub trait Source: Clone + Send + Sync + 'static {
    fn fetch_blocks(&self, range: BlockRange) -> Result<Vec<BlockHeader>, DatalensError>;

    fn fetch_logs(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError>;
}

#[derive(Clone)]
pub struct QueryService<S> {
    storage: LocalStorage,
    source: S,
    planner: PlannerConfig,
    writer: WriterConfig,
    chain_name: String,
    chain: ChainConfig,
}

impl<S> QueryService<S>
where
    S: Source,
{
    pub fn new(
        storage: LocalStorage,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain: ChainConfig,
    ) -> Self {
        Self::new_named(storage, source, planner, writer, "ethereum", chain)
    }

    pub fn new_named(
        storage: LocalStorage,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain_name: impl Into<String>,
        chain: ChainConfig,
    ) -> Self {
        Self {
            storage,
            source,
            planner,
            writer,
            chain_name: chain_name.into(),
            chain,
        }
    }

    pub fn query(&self, request: QueryRequest) -> Result<QueryResponse, DatalensError> {
        self.validate(&request)?;

        let filter = request.filter.as_ref();
        let hit_ranges = self
            .storage
            .covered_ranges(request.dataset, filter, request.range)?;
        let misses = missing_ranges(request.range, &hit_ranges);
        let mut rows = self
            .storage
            .read_rows(request.dataset, filter, request.range)?;

        for range in split_ranges(&misses, self.chunk_size(request.dataset)) {
            let fetched = match request.dataset {
                Dataset::Blocks => QueryRows::Blocks(self.source.fetch_blocks(range)?),
                Dataset::Logs => {
                    let filter = filter.ok_or_else(|| {
                        DatalensError::new(DatalensErrorKind::InvalidInput, "logs require filter")
                    })?;
                    QueryRows::Logs(self.source.fetch_logs(range, filter)?)
                }
            };
            self.storage.write_rows(
                request.dataset,
                filter,
                range,
                &fetched,
                self.writer.record_empty_coverage,
            )?;
            rows.try_append(fetched)?;
        }

        rows.sort();
        Ok(QueryResponse {
            chain: request.chain,
            range: request.range,
            cache: CacheSummary {
                hit_ranges,
                missing_ranges: misses,
            },
            rows,
        })
    }

    fn validate(&self, request: &QueryRequest) -> Result<(), DatalensError> {
        if request.range.from_block > request.range.to_block {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "from_block must be less than or equal to to_block",
            ));
        }
        if request.range.len() > u128::from(self.planner.max_query_range_blocks) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "query range exceeds planner.max_query_range_blocks",
            ));
        }
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
            Dataset::Logs => {
                let filter = request.filter.as_ref().ok_or_else(|| {
                    DatalensError::new(DatalensErrorKind::InvalidInput, "logs require filter")
                })?;
                EvmLogFilter::try_from(filter)?;
                if filter.addresses.len() > self.chain.datasets.logs.max_addresses_per_query {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "too many log addresses",
                    ));
                }
                Ok(())
            }
            Dataset::Blocks => Ok(()),
        }
    }

    fn chunk_size(&self, dataset: Dataset) -> u64 {
        match dataset {
            Dataset::Blocks => self
                .chain
                .datasets
                .blocks
                .max_batch_blocks
                .min(self.planner.default_chunk_range_blocks)
                .max(1),
            Dataset::Logs => self
                .chain
                .datasets
                .logs
                .max_get_logs_range_blocks
                .min(self.planner.default_chunk_range_blocks)
                .max(1),
        }
    }
}

pub fn router<S>(service: QueryService<S>, chain_names: Vec<String>) -> Router
where
    S: Source,
{
    Router::new()
        .route("/health", get(health))
        .route("/v1/chains", get(chains::<S>))
        .route("/v1/query", post(query::<S>))
        .with_state(AppState {
            service,
            chain_names,
        })
}

#[derive(Clone)]
struct AppState<S> {
    service: QueryService<S>,
    chain_names: Vec<String>,
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn chains<S>(State(state): State<AppState<S>>) -> Json<serde_json::Value>
where
    S: Source,
{
    Json(serde_json::json!({ "chains": state.chain_names }))
}

async fn query<S>(
    State(state): State<AppState<S>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError>
where
    S: Source,
{
    state.service.query(request).map(Json).map_err(ApiError)
}

struct ApiError(DatalensError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0.kind {
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
            | DatalensErrorKind::ManifestUpdateFailure => StatusCode::INTERNAL_SERVER_ERROR,
            DatalensErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({
                "error": {
                    "kind": self.0.kind,
                    "message": self.0.message,
                }
            })),
        )
            .into_response()
    }
}

pub async fn serve<S>(
    bind: SocketAddr,
    service: QueryService<S>,
    chain_names: Vec<String>,
) -> Result<(), std::io::Error>
where
    S: Source,
{
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router(service, chain_names)).await
}

fn split_ranges(ranges: &[BlockRange], chunk_size: u64) -> Vec<BlockRange> {
    ranges
        .iter()
        .flat_map(|range| range.split(chunk_size).expect("positive chunk size"))
        .collect()
}
