//! Edge API boundary for datalens.

use std::{collections::BTreeMap, env, fs, net::SocketAddr, path::Path};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use datalens_chain::{
    ChainAdapter, ChainFetchRequest, DatasetSelector, FetchContext, HeightRange, HeightRangeKind,
};
use datalens_core::{
    BlockRange, CacheSummary, DatalensError, DatalensErrorKind, Dataset, QueryRequest,
    QueryResponse,
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
    S: ChainAdapter,
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
        log::info!(
            "query start chain={} dataset={} range={}-{}",
            request.chain.configured_name(),
            request.dataset.as_str(),
            request.range.from_block,
            request.range.to_block
        );
        let selector = self.selector_for_request(&request)?;
        if let Err(error) = self.validate(&request, &selector) {
            log::warn!("query validation failed kind={:?}", error.kind);
            return Err(error);
        }

        let hit_ranges = self.storage.covered_ranges(
            &request.chain,
            request.dataset,
            &selector,
            request.range,
        )?;
        let misses = missing_ranges(request.range, &hit_ranges);
        log::info!(
            "cache summary dataset={} hit_ranges={} missing_ranges={}",
            request.dataset.as_str(),
            hit_ranges.len(),
            misses.len()
        );
        let mut rows =
            self.storage
                .read_rows(&request.chain, request.dataset, &selector, request.range)?;

        for range in split_ranges(&misses, self.chunk_size(request.dataset)) {
            let fetched = match self.source.fetch(
                ChainFetchRequest::new(
                    request.chain.clone(),
                    request.dataset,
                    HeightRange::Block(range),
                    selector.clone(),
                )
                .with_context(FetchContext {
                    request_id: None,
                    cache_write: true,
                }),
            ) {
                Ok(response) => response.rows,
                Err(error) => {
                    log::warn!(
                        "provider fetch failed dataset={} range={}-{} kind={:?}",
                        request.dataset.as_str(),
                        range.from_block,
                        range.to_block,
                        error.kind
                    );
                    return Err(error);
                }
            };
            if let Err(error) = self.storage.write_rows(
                &request.chain,
                request.dataset,
                &selector,
                range,
                &fetched,
                self.writer.record_empty_coverage,
            ) {
                log::error!(
                    "cache write failed dataset={} range={}-{} kind={:?}",
                    request.dataset.as_str(),
                    range.from_block,
                    range.to_block,
                    error.kind
                );
                return Err(error);
            }
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

    fn validate(
        &self,
        request: &QueryRequest,
        selector: &DatasetSelector,
    ) -> Result<(), DatalensError> {
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
        let capabilities = self.source.capabilities();
        if capabilities.chain() != &request.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "chain is not supported by adapter",
            ));
        }
        let dataset_capability = capabilities.dataset(request.dataset).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "dataset is not supported by adapter",
            )
        })?;
        if !dataset_capability.supports_selector(selector.kind()) {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "selector is not supported by adapter",
            ));
        }
        if !dataset_capability
            .ranges()
            .contains(&HeightRangeKind::Block)
        {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "block ranges are not supported by adapter",
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

    fn selector_for_request(
        &self,
        request: &QueryRequest,
    ) -> Result<DatasetSelector, DatalensError> {
        match request.dataset {
            Dataset::Blocks => Ok(DatasetSelector::all()),
            Dataset::Logs => {
                let filter = request.filter.clone().ok_or_else(|| {
                    DatalensError::new(DatalensErrorKind::InvalidInput, "logs require filter")
                })?;
                DatasetSelector::try_evm_logs(filter)
            }
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
    S: ChainAdapter,
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
    S: ChainAdapter,
{
    Json(serde_json::json!({ "chains": state.chain_names }))
}

async fn query<S>(
    State(state): State<AppState<S>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError>
where
    S: ChainAdapter,
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
        log::warn!(
            "query response error status={} kind={:?}",
            status.as_u16(),
            self.0.kind
        );
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
    S: ChainAdapter,
{
    let listener = tokio::net::TcpListener::bind(bind).await?;
    log::info!("api listener bound to {bind}");
    axum::serve(listener, router(service, chain_names)).await
}

fn split_ranges(ranges: &[BlockRange], chunk_size: u64) -> Vec<BlockRange> {
    ranges
        .iter()
        .flat_map(|range| range.split(chunk_size).expect("positive chunk size"))
        .collect()
}
