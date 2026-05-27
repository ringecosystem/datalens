//! Edge API boundary for datalens.

use std::{collections::BTreeMap, env, fs, net::SocketAddr, path::Path};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use datalens_chain::{ChainAdapter, ChainFetchRequest, FetchContext};
use datalens_core::{
    BlockRange, CacheSummary, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows,
    LedgerRange, LegacyEvmQueryRequest, LegacyEvmQueryResponse,
};
use datalens_planner::{NativePlanner, NativePlannerConfig, NativeQueryInput};
use datalens_storage::{LocalStorage, StorageWriteRequest, missing_ranges};
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

#[derive(Clone)]
pub struct QueryService<S> {
    storage: LocalStorage,
    source: S,
    planner: PlannerConfig,
    writer: WriterConfig,
    chain_name: String,
    chain: ChainConfig,
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

    pub fn query(
        &self,
        request: LegacyEvmQueryRequest,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        log::info!(
            "legacy evm query start chain={} dataset={} range={}-{}",
            request.chain.configured_name(),
            request.dataset.as_str(),
            request.range.from_block,
            request.range.to_block
        );
        if let Err(error) = self.validate_legacy_evm_route(&request) {
            log::warn!("query validation failed kind={:?}", error.kind);
            return Err(error);
        }
        let response_range = request.range;
        let response = self.query_native(NativeQueryInput::from_legacy_evm_query(request)?)?;
        let hit_ranges = legacy_block_ranges(&response.cache.hit_ranges)?;
        let misses = legacy_block_ranges(&response.cache.missing_ranges)?;

        Ok(LegacyEvmQueryResponse {
            chain: response.chain,
            range: response_range,
            cache: CacheSummary {
                hit_ranges,
                missing_ranges: misses,
            },
            rows: response.rows.into_rows(),
        })
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
        let safe_height = self.source.cache_safe_height()?;
        let default_chunk_range_len = self.chunk_size(native_input.dataset_key.clone());
        let plan = NativePlanner::new(NativePlannerConfig {
            max_query_range_len: self.planner.max_query_range_blocks,
            default_chunk_range_len,
        })
        .plan(native_input, &self.source.capabilities(), safe_height)?;

        let hit_ledger_ranges = self.storage.covered_ranges(
            &plan.chain,
            &plan.dataset_key,
            &plan.selector,
            plan.ledger_range.clone(),
        )?;
        let miss_ledger_ranges = missing_ranges(plan.ledger_range.clone(), &hit_ledger_ranges);
        log::info!(
            "cache summary dataset={} hit_ranges={} missing_ranges={}",
            plan.dataset_key.as_str(),
            hit_ledger_ranges.len(),
            miss_ledger_ranges.len()
        );
        let mut rows = self
            .storage
            .read_rows(
                &plan.chain,
                &plan.dataset_key,
                &plan.selector,
                plan.ledger_range.clone(),
            )?
            .into_rows();

        for range in plan.split_ranges(miss_ledger_ranges.clone())? {
            let fetch_request = ChainFetchRequest::new(
                plan.chain.clone(),
                plan.dataset_key.clone(),
                range.clone(),
                plan.selector.clone(),
            )
            .with_context(FetchContext {
                request_id: None,
                cache_write: true,
            });
            let fetched = match self.source.fetch(fetch_request.clone()) {
                Ok(response) => {
                    response.validate_for_request(&fetch_request)?;
                    response.rows
                }
                Err(error) => {
                    log::warn!(
                        "provider fetch failed dataset={} range={}-{} kind={:?}",
                        plan.dataset_key.as_str(),
                        range.start(),
                        range.end(),
                        error.kind
                    );
                    return Err(error);
                }
            };
            let finality_level = match &plan.finality_policy {
                datalens_planner::FinalityPolicy::DurableCache { boundary } => boundary.finality,
            };
            if let Err(error) = self.storage.write_rows(StorageWriteRequest {
                chain: &plan.chain,
                dataset_key: plan.dataset_key.clone(),
                selector: &plan.selector,
                range,
                rows: &fetched,
                finality_level,
                record_empty_coverage: self.writer.record_empty_coverage,
            }) {
                log::error!(
                    "cache write failed dataset={} range={}-{} kind={:?}",
                    plan.dataset_key.as_str(),
                    plan.ledger_range.start(),
                    plan.ledger_range.end(),
                    error.kind
                );
                return Err(error);
            }
            rows.try_append(fetched.into_rows())?;
        }

        rows.sort();
        Ok(NativeQueryResponse {
            chain: plan.chain,
            dataset_key: plan.dataset_key.clone(),
            ledger_range: plan.ledger_range,
            cache: NativeCacheSummary {
                hit_ranges: hit_ledger_ranges,
                missing_ranges: miss_ledger_ranges,
            },
            rows: DatasetRows::new(plan.dataset_key, rows)?,
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

    fn chunk_size(&self, dataset_key: DatasetKey) -> u64 {
        let configured_limit = match dataset_key.legacy_dataset() {
            Some(Dataset::Blocks) => self.chain.datasets.blocks.max_batch_blocks,
            Some(Dataset::Logs) => self.chain.datasets.logs.max_get_logs_range_blocks,
            None => self.planner.default_chunk_range_blocks,
        };
        configured_limit
            .min(self.planner.default_chunk_range_blocks)
            .max(1)
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
    Json(request): Json<LegacyEvmQueryRequest>,
) -> Result<Json<LegacyEvmQueryResponse>, ApiError>
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
