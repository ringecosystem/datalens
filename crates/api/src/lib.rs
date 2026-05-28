//! Edge API boundary for datalens.

use std::{collections::BTreeMap, env, fs, net::SocketAddr, path::Path, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use datalens_chain::ChainAdapter;
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey,
    DatasetRows, LedgerRange, LogFilter, NetworkId, QueryRows,
};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_planner::{NativePlannerConfig, NativeQueryInput};
use datalens_storage::{S3ObjectStoreConfig, StorageRepository, UsageLedgerRepository};
use datalens_writer::DurableWriterConfig;
use serde::{Deserialize, Serialize};

pub mod auth {
    use super::*;

    pub const APPLICATION_HEADER: &str = "x-datalens-application";

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AuthContext {
        pub subject: Option<String>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ApplicationContext {
        pub id: String,
        pub name: String,
        pub display_name: Option<String>,
        pub quota: Option<config::ApplicationQuotaConfig>,
    }

    impl ApplicationContext {
        pub fn metrics_identity(&self) -> ApplicationIdentity {
            ApplicationIdentity::named(self.id.clone())
        }
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

    #[derive(Clone, Debug, Default)]
    pub struct ApplicationRegistry {
        required: bool,
        applications: BTreeMap<String, config::ApplicationConfig>,
    }

    impl ApplicationRegistry {
        pub fn disabled() -> Self {
            Self::default()
        }

        pub fn from_config(
            config: config::ApplicationRegistryConfig,
        ) -> Result<Self, DatalensError> {
            let mut applications = BTreeMap::new();
            for mut application in config.applications {
                application.id = normalize_application_id(&application.id)?;
                application.name = normalize_application_id(&application.name)?;
                if application.token.trim().is_empty() {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        format!("application {} token must not be empty", application.id),
                    ));
                }
                if applications
                    .insert(application.id.clone(), application)
                    .is_some()
                {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "application id is registered more than once",
                    ));
                }
            }
            if config.required && applications.is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "applications registry must contain at least one application when required",
                ));
            }
            Ok(Self {
                required: config.required,
                applications,
            })
        }

        pub fn required(&self) -> bool {
            self.required
        }

        pub fn authenticate_headers(
            &self,
            headers: &HeaderMap,
            request: &LegacyEvmQueryRequest,
        ) -> Result<Option<ApplicationContext>, DatalensError> {
            if !self.required {
                return Ok(None);
            }
            let raw_application = headers
                .get(APPLICATION_HEADER)
                .ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::AuthenticationFailed,
                        "application identity is required",
                    )
                })?
                .to_str()
                .map_err(|_| {
                    DatalensError::new(
                        DatalensErrorKind::AuthenticationFailed,
                        "application identity is invalid",
                    )
                })?;
            let application_id = normalize_application_id(raw_application).map_err(|_| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application identity is invalid",
                )
            })?;
            let application = self.applications.get(&application_id).ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application credentials are invalid",
                )
            })?;
            let token = bearer_token(headers).ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application credentials are required",
                )
            })?;
            if token != application.token {
                return Err(DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application credentials are invalid",
                ));
            }
            if !application.enabled {
                return Err(DatalensError::new(
                    DatalensErrorKind::Unauthorized,
                    "application is disabled",
                ));
            }
            authorize_application(application, request)?;
            enforce_quota(application, request)?;
            Ok(Some(ApplicationContext {
                id: application.id.clone(),
                name: application.name.clone(),
                display_name: application.display_name.clone(),
                quota: application.quota.clone(),
            }))
        }
    }

    fn bearer_token(headers: &HeaderMap) -> Option<&str> {
        let value = headers
            .get(axum::http::header::AUTHORIZATION)?
            .to_str()
            .ok()?;
        value
            .strip_prefix("Bearer ")
            .filter(|token| !token.trim().is_empty())
    }

    fn authorize_application(
        application: &config::ApplicationConfig,
        request: &LegacyEvmQueryRequest,
    ) -> Result<(), DatalensError> {
        let chain = request.chain.configured_name();
        if !application.chains.iter().any(|allowed| allowed == chain) {
            return Err(DatalensError::new(
                DatalensErrorKind::Unauthorized,
                "application is not allowed to access this chain",
            ));
        }
        let dataset = request.dataset.as_str();
        if !application
            .datasets
            .iter()
            .any(|allowed| allowed == dataset)
        {
            return Err(DatalensError::new(
                DatalensErrorKind::Unauthorized,
                "application is not allowed to access this dataset",
            ));
        }
        Ok(())
    }

    fn enforce_quota(
        application: &config::ApplicationConfig,
        request: &LegacyEvmQueryRequest,
    ) -> Result<(), DatalensError> {
        let Some(quota) = &application.quota else {
            return Ok(());
        };
        if let Some(limit) = quota.max_query_range_blocks {
            let requested = request.range.len();
            if requested > u128::from(limit) {
                return Err(DatalensError::new(
                    DatalensErrorKind::RateLimited,
                    "application query range quota exceeded",
                ));
            }
        }
        Ok(())
    }

    pub fn normalize_application_id(value: &str) -> Result<String, DatalensError> {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized.is_empty()
            || normalized.starts_with('.')
            || normalized.ends_with('.')
            || normalized.contains('/')
            || normalized.contains('\\')
            || normalized.len() > 64
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "application id must be 1-64 characters using lowercase letters, digits, dot, underscore, or hyphen",
            ));
        }
        if !normalized.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        }) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "application id must be 1-64 characters using lowercase letters, digits, dot, underscore, or hyphen",
            ));
        }
        Ok(normalized)
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
        #[serde(default)]
        pub metrics: MetricsConfig,
        #[serde(default)]
        pub applications: ApplicationRegistryConfig,
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
    pub struct MetricsConfig {
        #[serde(default = "default_metrics_enabled")]
        pub enabled: bool,
        #[serde(default = "default_metrics_application")]
        pub default_application: String,
    }

    impl Default for MetricsConfig {
        fn default() -> Self {
            Self {
                enabled: default_metrics_enabled(),
                default_application: default_metrics_application(),
            }
        }
    }

    fn default_metrics_enabled() -> bool {
        true
    }

    fn default_metrics_application() -> String {
        "datalens".to_owned()
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ApplicationRegistryConfig {
        #[serde(default)]
        pub required: bool,
        #[serde(default)]
        pub applications: Vec<ApplicationConfig>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ApplicationConfig {
        pub id: String,
        pub name: String,
        #[serde(default = "default_application_enabled")]
        pub enabled: bool,
        #[serde(default)]
        pub display_name: Option<String>,
        pub token: String,
        #[serde(default)]
        pub chains: Vec<String>,
        #[serde(default)]
        pub datasets: Vec<String>,
        #[serde(default)]
        pub quota: Option<ApplicationQuotaConfig>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ApplicationQuotaConfig {
        #[serde(default)]
        pub max_query_range_blocks: Option<u64>,
        #[serde(default)]
        pub max_requests_per_minute: Option<u64>,
        #[serde(default)]
        pub max_concurrent_requests: Option<u64>,
    }

    fn default_application_enabled() -> bool {
        true
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

use auth::{ApplicationContext, ApplicationRegistry};
use config::{ChainConfig, MetricsConfig, PlannerConfig, WriterConfig};

pub const APPLICATION_IDENTITY_HEADER: &str = "x-datalens-application";

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryResponse {
    pub chains: Vec<ChainDiscovery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainDiscovery {
    pub identity: ChainIdentity,
    pub datasets: Vec<Dataset>,
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
        Self::new_with_metrics_config(
            storage,
            source,
            planner,
            writer,
            chain_name,
            chain,
            MetricsConfig::default(),
        )
        .expect("default metrics recorder initializes")
    }

    pub fn new_with_metrics_config(
        storage: impl StorageRepository + 'static,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain_name: impl Into<String>,
        chain: ChainConfig,
        metrics_config: MetricsConfig,
    ) -> Result<Self, DatalensError> {
        let storage: Arc<dyn StorageRepository> = Arc::new(storage);
        let recorder = if metrics_config.enabled {
            Some(MetricsRecorder::new().map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("initialize metrics recorder: {error}"),
                )
            })?)
        } else {
            None
        };
        let mut executor = NativeQueryExecutor::new(
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
        if let Some(recorder) = recorder.clone() {
            executor = executor.with_metrics(
                recorder,
                ApplicationIdentity::named(metrics_config.default_application),
            );
        }

        Ok(Self {
            executor,
            chain_name: chain_name.into(),
            chain,
            metrics: recorder,
        })
    }

    pub fn with_metrics(mut self, metrics: MetricsRecorder) -> Self {
        self.executor = self
            .executor
            .with_metrics(metrics.clone(), ApplicationIdentity::unknown());
        self.metrics = Some(metrics);
        self
    }

    pub fn with_usage_ledger(
        mut self,
        repository: impl UsageLedgerRepository + 'static,
        application: ApplicationIdentity,
    ) -> Self {
        self.executor = self.executor.with_usage_ledger(repository, application);
        self
    }

    pub fn chain_name(&self) -> &str {
        &self.chain_name
    }

    pub fn query(
        &self,
        request: LegacyEvmQueryRequest,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        self.query_with_application(request, None)
    }

    pub fn query_with_application(
        &self,
        request: LegacyEvmQueryRequest,
        application: Option<ApplicationIdentity>,
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
        let response =
            self.query_native_with_application(legacy_evm_to_native_input(request)?, application)?;
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
        self.query_native_with_application(native_input, None)
    }

    pub fn query_native_with_application(
        &self,
        native_input: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryResponse, DatalensError> {
        log::info!(
            "native query start chain={} dataset={} range={}-{}",
            native_input.chain.configured_name(),
            native_input.dataset_key.as_str(),
            native_input.ledger_range.start(),
            native_input.ledger_range.end()
        );
        let result = self
            .executor
            .execute_with_application(native_input, application)?;
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

    pub fn metrics_text(&self) -> Option<Result<String, DatalensError>> {
        self.metrics.as_ref().map(|recorder| {
            recorder.encode().map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode metrics: {error}"),
                )
            })
        })
    }

    pub fn discovery(&self) -> Result<ChainDiscovery, DatalensError> {
        Ok(ChainDiscovery {
            identity: ChainIdentity::try_new(
                chain_family(&self.chain.kind)?,
                self.chain_name.clone(),
                Some(NetworkId::numeric(self.chain.chain_id)),
            )?,
            datasets: enabled_datasets(&self.chain),
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
}

trait RegisteredQueryService: Send + Sync {
    fn query(
        &self,
        request: LegacyEvmQueryRequest,
        application: Option<ApplicationIdentity>,
    ) -> Result<LegacyEvmQueryResponse, DatalensError>;

    fn metrics_text(&self) -> Option<Result<String, DatalensError>>;

    fn discovery(&self) -> Result<ChainDiscovery, DatalensError>;
}

impl<S> RegisteredQueryService for QueryService<S>
where
    S: ChainAdapter + 'static,
{
    fn query(
        &self,
        request: LegacyEvmQueryRequest,
        application: Option<ApplicationIdentity>,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        QueryService::query_with_application(self, request, application)
    }

    fn metrics_text(&self) -> Option<Result<String, DatalensError>> {
        QueryService::metrics_text(self)
    }

    fn discovery(&self) -> Result<ChainDiscovery, DatalensError> {
        QueryService::discovery(self)
    }
}

#[derive(Clone, Default)]
pub struct QueryServiceRegistry {
    services: BTreeMap<String, Arc<dyn RegisteredQueryService>>,
    application_registry: ApplicationRegistry,
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

    pub fn with_application_registry(
        mut self,
        config: config::ApplicationRegistryConfig,
    ) -> Result<Self, DatalensError> {
        self.application_registry = ApplicationRegistry::from_config(config)?;
        Ok(self)
    }

    pub fn chain_names(&self) -> Vec<String> {
        self.services.keys().cloned().collect()
    }

    pub fn query(
        &self,
        request: LegacyEvmQueryRequest,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        self.query_with_application(request, None)
    }

    pub fn query_with_application(
        &self,
        request: LegacyEvmQueryRequest,
        application: Option<ApplicationIdentity>,
    ) -> Result<LegacyEvmQueryResponse, DatalensError> {
        let chain_name = request.chain.configured_name();
        let service = self.services.get(chain_name).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("chain {chain_name} is not configured"),
            )
        })?;
        service.query(request, application)
    }

    pub fn discovery(&self) -> Result<DiscoveryResponse, DatalensError> {
        let chains = self
            .services
            .values()
            .map(|service| service.discovery())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DiscoveryResponse { chains })
    }

    fn authenticate_headers(
        &self,
        headers: &HeaderMap,
        request: &LegacyEvmQueryRequest,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry
            .authenticate_headers(headers, request)
    }

    pub fn metrics_text(&self) -> Option<Result<String, DatalensError>> {
        let mut texts = Vec::new();
        for service in self.services.values() {
            match service.metrics_text() {
                Some(Ok(text)) => texts.push(text),
                Some(Err(error)) => return Some(Err(error)),
                None => {}
            }
        }
        if texts.is_empty() {
            None
        } else {
            Some(Ok(texts.join("\n")))
        }
    }
}

pub fn router(registry: QueryServiceRegistry) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/chains", get(chains))
        .route("/v1/discovery", get(discovery))
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

async fn discovery(State(state): State<AppState>) -> Result<Json<DiscoveryResponse>, ApiError> {
    state.registry.discovery().map(Json).map_err(ApiError)
}

async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<LegacyEvmQueryRequest>,
) -> Result<Json<LegacyEvmQueryResponse>, ApiError> {
    let registry = state.registry.clone();
    let application_context = registry
        .authenticate_headers(&headers, &request)
        .map_err(ApiError)?;
    let application = application_context
        .map(|application| application.metrics_identity())
        .or_else(|| application_from_headers(&headers));
    tokio::task::spawn_blocking(move || registry.query_with_application(request, application))
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

async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
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
        DatalensErrorKind::AuthenticationFailed => StatusCode::UNAUTHORIZED,
        DatalensErrorKind::InvalidInput | DatalensErrorKind::InvalidRequest => {
            StatusCode::BAD_REQUEST
        }
        DatalensErrorKind::Unauthorized => StatusCode::FORBIDDEN,
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
        DatalensErrorKind::AuthenticationFailed => "authentication_failed",
        DatalensErrorKind::InvalidInput => "invalid_input",
        DatalensErrorKind::InvalidRequest => "invalid_request",
        DatalensErrorKind::Unauthorized => "unauthorized",
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

fn application_from_headers(headers: &HeaderMap) -> Option<ApplicationIdentity> {
    headers
        .get(APPLICATION_IDENTITY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| ApplicationIdentity::from_optional(Some(value)))
}

fn chain_family(kind: &str) -> Result<ChainFamily, DatalensError> {
    match kind {
        "evm" => Ok(ChainFamily::Evm),
        value => ChainFamily::try_other(value.to_owned()),
    }
}

fn enabled_datasets(chain: &ChainConfig) -> Vec<Dataset> {
    let mut datasets = Vec::new();
    if chain.datasets.blocks.enabled {
        datasets.push(Dataset::Blocks);
    }
    if chain.datasets.logs.enabled {
        datasets.push(Dataset::Logs);
    }
    datasets
}
