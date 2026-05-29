//! Edge API boundary for datalens.

use std::{
    collections::BTreeMap,
    env, fs,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use datalens_chain::{AdapterKey, ChainAdapter, DatasetSelector};
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey,
    DatasetRows, LedgerRange, LedgerRangeKind, LogFilter, NetworkId, QueryDataFinality,
    QueryFinalityRequirement, QueryRows, QuerySegmentSource,
};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_planner::{NativePlannerConfig, NativeQueryInput};
use datalens_storage::{S3ObjectStoreConfig, StorageRepository, UsageLedgerRepository};
use datalens_warmup::{
    WarmupChunkPolicy, WarmupRegistry, WarmupRetryPolicy, WarmupRunResult, WarmupSubmitOutcome,
    WarmupSubmitRequest, WarmupTask, WarmupTaskFilter, WarmupTaskId, WarmupTaskMode,
    WarmupTaskPool, WarmupTaskState,
};
use datalens_writer::{DurableWriteResult, DurableWriterConfig};
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
            chain: &str,
            dataset: &str,
            range_len: u128,
            finality: QueryFinalityRequirement,
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
            authorize_application_dataset(application, chain, dataset)?;
            enforce_quota(application, range_len, finality)?;
            Ok(Some(ApplicationContext {
                id: application.id.clone(),
                name: application.name.clone(),
                display_name: application.display_name.clone(),
                quota: application.quota.clone(),
            }))
        }

        pub fn authenticate_warmup_headers(
            &self,
            headers: &HeaderMap,
            chain: &str,
            dataset: &str,
        ) -> Result<Option<ApplicationContext>, DatalensError> {
            if !self.required {
                return Ok(None);
            }
            let application = self.authenticate_application_headers(headers)?;
            authorize_application_dataset(application, chain, dataset)?;
            Ok(Some(ApplicationContext {
                id: application.id.clone(),
                name: application.name.clone(),
                display_name: application.display_name.clone(),
                quota: application.quota.clone(),
            }))
        }

        pub fn authenticate_task_headers(
            &self,
            headers: &HeaderMap,
        ) -> Result<Option<ApplicationContext>, DatalensError> {
            if !self.required {
                return Ok(None);
            }
            let application = self.authenticate_application_headers(headers)?;
            Ok(Some(ApplicationContext {
                id: application.id.clone(),
                name: application.name.clone(),
                display_name: application.display_name.clone(),
                quota: application.quota.clone(),
            }))
        }

        fn authenticate_application_headers(
            &self,
            headers: &HeaderMap,
        ) -> Result<&config::ApplicationConfig, DatalensError> {
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
            Ok(application)
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

    fn authorize_application_dataset(
        application: &config::ApplicationConfig,
        chain: &str,
        dataset: &str,
    ) -> Result<(), DatalensError> {
        if !application.chains.iter().any(|allowed| allowed == chain) {
            return Err(DatalensError::new(
                DatalensErrorKind::Unauthorized,
                "application is not allowed to access this chain",
            ));
        }
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
        range_len: u128,
        finality: QueryFinalityRequirement,
    ) -> Result<(), DatalensError> {
        let Some(quota) = &application.quota else {
            return Ok(());
        };
        if let Some(limit) = quota.max_query_range_blocks
            && range_len > u128::from(limit)
        {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application query range quota exceeded",
            ));
        }
        if finality.allows_hot()
            && let Some(limit) = quota.max_hot_query_range_blocks
            && range_len > u128::from(limit)
        {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application hot query range quota exceeded",
            ));
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
    pub use super::{
        ApiErrorBody, ApiErrorDetail, api_error_body, api_error_status, router, serve,
        serve_lifecycle,
    };

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

pub mod contract {
    pub use super::{
        CacheSummary, ChainDiscovery, DiscoveryResponse, LegacyEvmQueryRequest,
        LegacyEvmQueryResponse, NativeCacheSummary, NativeQueryResponse, QuerySegment,
        WarmupDatasetKeyApi, WarmupRunOnceApiResponse, WarmupSelectorApiRequest,
        WarmupSubmitApiRequest, WarmupSubmitApiResponse, WarmupTaskApiResponse,
        WarmupTaskListApiResponse, WarmupTaskListQuery, WarmupTaskView,
    };
}

pub mod service {
    pub use super::{
        LifecycleShutdown, NoopLifecycleShutdown, QueryService, QueryServiceRegistry,
        RegisteredWarmupService, ServiceLifecycle, WarmupSchedulerHandle,
        legacy_evm_to_native_input,
    };
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
        pub index: IndexConfig,
        #[serde(default)]
        pub warmup: WarmupConfig,
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
        #[serde(default)]
        pub staging: WriterStagingConfig,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct WriterStagingConfig {
        #[serde(default)]
        pub enabled: bool,
        #[serde(default)]
        pub min_rows: Option<usize>,
        #[serde(default)]
        pub target_object_bytes: Option<u64>,
        #[serde(default)]
        pub max_staged_ranges: Option<usize>,
        #[serde(default)]
        pub max_staged_rows: Option<usize>,
        #[serde(default)]
        pub max_staged_age_ms: Option<u64>,
        #[serde(default = "default_flush_on_shutdown")]
        pub flush_on_shutdown: bool,
        #[serde(default)]
        pub max_staged_bytes: Option<u64>,
    }

    impl Default for WriterStagingConfig {
        fn default() -> Self {
            Self {
                enabled: false,
                min_rows: None,
                target_object_bytes: None,
                max_staged_ranges: None,
                max_staged_rows: None,
                max_staged_age_ms: None,
                flush_on_shutdown: true,
                max_staged_bytes: None,
            }
        }
    }

    fn default_flush_on_shutdown() -> bool {
        true
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct MetricsConfig {
        #[serde(default = "default_metrics_enabled")]
        pub enabled: bool,
        #[serde(default = "default_metrics_application")]
        pub default_application: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct WarmupConfig {
        #[serde(default)]
        pub enabled: bool,
        #[serde(default = "default_warmup_registry_path")]
        pub registry_path: String,
        #[serde(default = "default_warmup_scheduler_interval_ms")]
        pub scheduler_interval_ms: u64,
        #[serde(default = "default_warmup_max_global_tasks")]
        pub max_global_tasks: usize,
        #[serde(default = "default_warmup_max_per_chain_tasks")]
        pub max_per_chain_tasks: usize,
        #[serde(default = "default_warmup_max_fetches_per_loop")]
        pub max_fetches_per_loop: u64,
        #[serde(default = "default_warmup_flush_on_shutdown")]
        pub flush_on_shutdown: bool,
    }

    impl Default for WarmupConfig {
        fn default() -> Self {
            Self {
                enabled: false,
                registry_path: default_warmup_registry_path(),
                scheduler_interval_ms: default_warmup_scheduler_interval_ms(),
                max_global_tasks: default_warmup_max_global_tasks(),
                max_per_chain_tasks: default_warmup_max_per_chain_tasks(),
                max_fetches_per_loop: default_warmup_max_fetches_per_loop(),
                flush_on_shutdown: default_warmup_flush_on_shutdown(),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct IndexConfig {
        #[serde(default = "default_index_chunk_range")]
        pub default_chunk_range: u64,
        #[serde(default = "default_index_concurrency")]
        pub max_concurrency: usize,
        #[serde(default)]
        pub retry: IndexRetryConfig,
        #[serde(default = "default_index_finality")]
        pub default_finality: String,
        #[serde(default = "default_index_cursor_path")]
        pub cursor_path: String,
    }

    impl Default for IndexConfig {
        fn default() -> Self {
            Self {
                default_chunk_range: default_index_chunk_range(),
                max_concurrency: default_index_concurrency(),
                retry: IndexRetryConfig::default(),
                default_finality: default_index_finality(),
                cursor_path: default_index_cursor_path(),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct IndexRetryConfig {
        #[serde(default = "default_index_retry_attempts")]
        pub max_attempts: u32,
        #[serde(default = "default_index_initial_backoff_ms")]
        pub initial_backoff_ms: u64,
        #[serde(default = "default_index_max_backoff_ms")]
        pub max_backoff_ms: u64,
    }

    impl Default for IndexRetryConfig {
        fn default() -> Self {
            Self {
                max_attempts: default_index_retry_attempts(),
                initial_backoff_ms: default_index_initial_backoff_ms(),
                max_backoff_ms: default_index_max_backoff_ms(),
            }
        }
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

    fn default_warmup_registry_path() -> String {
        ".datalens/warmup".to_owned()
    }

    fn default_warmup_scheduler_interval_ms() -> u64 {
        1_000
    }

    fn default_warmup_max_global_tasks() -> usize {
        1
    }

    fn default_warmup_max_per_chain_tasks() -> usize {
        1
    }

    fn default_warmup_max_fetches_per_loop() -> u64 {
        1
    }

    fn default_warmup_flush_on_shutdown() -> bool {
        true
    }

    fn default_index_chunk_range() -> u64 {
        1_000
    }

    fn default_index_concurrency() -> usize {
        1
    }

    fn default_index_retry_attempts() -> u32 {
        3
    }

    fn default_index_initial_backoff_ms() -> u64 {
        250
    }

    fn default_index_max_backoff_ms() -> u64 {
        30_000
    }

    fn default_index_finality() -> String {
        "finalized".to_owned()
    }

    fn default_index_cursor_path() -> String {
        ".datalens/index-cursors".to_owned()
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
        pub max_hot_query_range_blocks: Option<u64>,
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
    #[serde(default)]
    pub allow_hot: bool,
    #[serde(default)]
    pub finality: QueryFinalityRequirement,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheSummary {
    pub hit_ranges: Vec<BlockRange>,
    pub missing_ranges: Vec<BlockRange>,
    pub durable_hit_ranges: Vec<BlockRange>,
    pub hot_hit_ranges: Vec<BlockRange>,
    pub provider_fill_ranges: Vec<BlockRange>,
    pub promotion_pending_ranges: Vec<BlockRange>,
    pub segments: Vec<QuerySegment>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuerySegment {
    pub range: BlockRange,
    pub source: QuerySegmentSource,
    pub finality: QueryDataFinality,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LegacyEvmQueryResponse {
    pub chain: ChainIdentity,
    pub range: BlockRange,
    pub cache: CacheSummary,
    pub rows: QueryRows,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryApiRequest {
    pub chain: ChainIdentity,
    pub dataset_key: String,
    pub selector: QuerySelectorApi,
    pub range: QueryRangeApi,
    #[serde(default)]
    pub finality: QueryFinalityRequirement,
    #[serde(default)]
    pub fields: FieldSelectionApi,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryRangeApi {
    Block { start: u64, end: u64 },
    Slot { start: u64, end: u64 },
    Height { start: u64, end: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QuerySelectorApi {
    All,
    EvmLogs(LogFilter),
    Other {
        kind: String,
        fingerprint: String,
        canonical_key: String,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum FieldSelectionApi {
    #[default]
    All,
    Include(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryApiResponse {
    pub chain: ChainIdentity,
    pub dataset_key: String,
    pub range: QueryRangeApi,
    pub cache: QueryCacheApi,
    pub rows: DatasetRows,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryCacheApi {
    pub hit_ranges: Vec<QueryRangeApi>,
    pub missing_ranges: Vec<QueryRangeApi>,
    pub durable_hit_ranges: Vec<QueryRangeApi>,
    pub hot_hit_ranges: Vec<QueryRangeApi>,
    pub provider_fill_ranges: Vec<QueryRangeApi>,
    pub promotion_pending_ranges: Vec<QueryRangeApi>,
    pub segments: Vec<QuerySegmentApi>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QuerySegmentApi {
    pub range: QueryRangeApi,
    pub source: QuerySegmentSource,
    pub finality: QueryDataFinality,
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

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct WarmupSubmitApiRequest {
    pub chain: ChainIdentity,
    pub dataset_key: WarmupDatasetKeyApi,
    pub selector: WarmupSelectorApiRequest,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: Option<u64>,
    #[serde(default = "default_warmup_api_mode")]
    pub mode: WarmupTaskMode,
    #[serde(default)]
    pub chunk_policy: WarmupChunkPolicy,
    #[serde(default)]
    pub retry_policy: WarmupRetryPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum WarmupDatasetKeyApi {
    Key(String),
    Structured(DatasetKey),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum WarmupSelectorApiRequest {
    All,
    EvmLogs(LogFilter),
    Other {
        kind: String,
        fingerprint: String,
        canonical_key: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupSubmitApiResponse {
    pub task_id: WarmupTaskId,
    pub created: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupTaskApiResponse {
    pub task: WarmupTaskView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupTaskListApiResponse {
    pub tasks: Vec<WarmupTaskView>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupRunOnceApiResponse {
    pub results: Vec<WarmupRunResult>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupTaskView {
    pub task_id: WarmupTaskId,
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: String,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: Option<u64>,
    pub mode: WarmupTaskMode,
    pub state: WarmupTaskState,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_error: Option<String>,
    pub stats: datalens_warmup::WarmupStats,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct WarmupTaskListQuery {
    pub chain: Option<String>,
    pub state: Option<WarmupTaskState>,
}

fn default_warmup_api_mode() -> WarmupTaskMode {
    WarmupTaskMode::FixedRange
}

impl WarmupDatasetKeyApi {
    fn into_dataset_key(self) -> Result<DatasetKey, DatalensError> {
        match self {
            Self::Structured(dataset_key) => Ok(dataset_key),
            Self::Key(value) => parse_dataset_key(&value),
        }
    }

    fn dataset_key(&self) -> Result<DatasetKey, DatalensError> {
        match self {
            Self::Structured(dataset_key) => Ok(dataset_key.clone()),
            Self::Key(value) => parse_dataset_key(value),
        }
    }
}

impl WarmupSelectorApiRequest {
    fn into_selector(self) -> Result<DatasetSelector, DatalensError> {
        match self {
            Self::All => Ok(DatasetSelector::all()),
            Self::EvmLogs(filter) => DatasetSelector::try_evm_logs(filter),
            Self::Other {
                kind,
                fingerprint,
                canonical_key,
            } => DatasetSelector::try_other(AdapterKey::try_new(kind)?, fingerprint, canonical_key),
        }
    }
}

impl WarmupSubmitApiRequest {
    fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    fn dataset_for_auth(&self) -> Result<String, DatalensError> {
        Ok(self.dataset_key.dataset_key()?.as_str().to_owned())
    }
}

impl QueryApiRequest {
    pub fn into_native_input(self) -> Result<NativeQueryInput, DatalensError> {
        Ok(NativeQueryInput {
            chain: self.chain,
            dataset_key: parse_dataset_key(&self.dataset_key)?,
            ledger_range: self.range.into_ledger_range()?,
            selector: self.selector.into_selector()?,
            response_shape: datalens_planner::ResponseShape::NativeRows,
            field_selection: self.fields.into_field_selection(),
            finality: self.finality,
        })
    }

    fn dataset_for_auth(&self) -> Result<String, DatalensError> {
        Ok(parse_dataset_key(&self.dataset_key)?.as_str().to_owned())
    }

    fn range_len(&self) -> u128 {
        self.range.len()
    }
}

impl QueryRangeApi {
    fn len(&self) -> u128 {
        let (start, end) = match *self {
            Self::Block { start, end }
            | Self::Slot { start, end }
            | Self::Height { start, end } => (start, end),
        };
        u128::from(end.saturating_sub(start)) + 1
    }

    fn into_ledger_range(self) -> Result<LedgerRange, DatalensError> {
        match self {
            Self::Block { start, end } => LedgerRange::blocks(start, end),
            Self::Slot { start, end } => LedgerRange::slots(start, end),
            Self::Height { start, end } => LedgerRange::heights(start, end),
        }
    }

    fn from_ledger_range(range: LedgerRange) -> Result<Self, DatalensError> {
        let start = range.start();
        let end = range.end();
        match range.kind() {
            LedgerRangeKind::Block => Ok(Self::Block { start, end }),
            LedgerRangeKind::Slot => Ok(Self::Slot { start, end }),
            LedgerRangeKind::Height => Ok(Self::Height { start, end }),
            LedgerRangeKind::Other(kind) => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("ledger range kind {kind} is not supported by the query API"),
            )),
        }
    }
}

impl QuerySelectorApi {
    fn into_selector(self) -> Result<DatasetSelector, DatalensError> {
        match self {
            Self::All => Ok(DatasetSelector::all()),
            Self::EvmLogs(filter) => DatasetSelector::try_evm_logs(filter),
            Self::Other {
                kind,
                fingerprint,
                canonical_key,
            } => DatasetSelector::try_other(AdapterKey::try_new(kind)?, fingerprint, canonical_key),
        }
    }
}

impl FieldSelectionApi {
    fn into_field_selection(self) -> datalens_planner::FieldSelection {
        match self {
            Self::All => datalens_planner::FieldSelection::All,
            Self::Include(fields) => datalens_planner::FieldSelection::Include(fields),
        }
    }
}

impl Serialize for FieldSelectionApi {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::All => serializer.serialize_str("all"),
            Self::Include(fields) => {
                use serde::ser::SerializeMap;

                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("include", fields)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for FieldSelectionApi {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value == serde_json::Value::String("all".to_owned()) {
            return Ok(Self::All);
        }
        if let Some(include) = value.get("include") {
            let fields =
                Vec::<String>::deserialize(include.clone()).map_err(serde::de::Error::custom)?;
            return Ok(Self::Include(fields));
        }
        Err(serde::de::Error::custom(
            "fields must be \"all\" or an object with include",
        ))
    }
}

impl QueryApiResponse {
    pub fn try_from_native_response(response: NativeQueryResponse) -> Result<Self, DatalensError> {
        Ok(Self {
            chain: response.chain,
            dataset_key: response.dataset_key.as_str().to_owned(),
            range: QueryRangeApi::from_ledger_range(response.ledger_range)?,
            cache: QueryCacheApi::try_from(response.cache)?,
            rows: response.rows,
        })
    }
}

impl From<NativeQueryResponse> for QueryApiResponse {
    fn from(response: NativeQueryResponse) -> Self {
        Self::try_from_native_response(response)
            .expect("native response uses query API ledger range kinds")
    }
}

impl TryFrom<NativeCacheSummary> for QueryCacheApi {
    type Error = DatalensError;

    fn try_from(cache: NativeCacheSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            hit_ranges: query_api_ranges(cache.hit_ranges)?,
            missing_ranges: query_api_ranges(cache.missing_ranges)?,
            durable_hit_ranges: query_api_ranges(cache.durable_hit_ranges)?,
            hot_hit_ranges: query_api_ranges(cache.hot_hit_ranges)?,
            provider_fill_ranges: query_api_ranges(cache.provider_fill_ranges)?,
            promotion_pending_ranges: query_api_ranges(cache.promotion_pending_ranges)?,
            segments: cache
                .segments
                .into_iter()
                .map(QuerySegmentApi::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<datalens_core::QuerySegmentMetadata> for QuerySegmentApi {
    type Error = DatalensError;

    fn try_from(segment: datalens_core::QuerySegmentMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            range: QueryRangeApi::from_ledger_range(segment.range)?,
            source: segment.source,
            finality: segment.finality,
        })
    }
}

fn query_api_ranges(ranges: Vec<LedgerRange>) -> Result<Vec<QueryRangeApi>, DatalensError> {
    ranges
        .into_iter()
        .map(QueryRangeApi::from_ledger_range)
        .collect()
}

fn parse_dataset_key(value: &str) -> Result<DatasetKey, DatalensError> {
    let value = value.trim();
    let Some((family, name)) = value.split_once('.') else {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "dataset_key must use family.name form",
        ));
    };
    let family = if family == "evm" {
        ChainFamily::Evm
    } else {
        ChainFamily::Other(family.to_owned())
    };
    DatasetKey::try_new(family, name)
}

fn warmup_task_view(task: WarmupTask) -> Result<WarmupTaskView, DatalensError> {
    Ok(WarmupTaskView {
        task_id: task.task_id,
        application_id: task.application_id,
        chain: task.chain,
        dataset_key: task.dataset_key.as_str().to_owned(),
        range_kind: task.range_kind,
        start: task.start,
        end: task.end,
        mode: task.mode,
        state: task.state,
        created_at: task.created_at,
        updated_at: task.updated_at,
        last_error: task.last_error,
        stats: task.stats,
    })
}

pub fn legacy_evm_to_native_input(
    request: LegacyEvmQueryRequest,
) -> Result<NativeQueryInput, DatalensError> {
    if request.finality.allows_hot() && !request.allow_hot {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "allow_hot must be true when requesting hot or latest data",
        ));
    }
    let finality =
        if request.allow_hot && matches!(request.finality, QueryFinalityRequirement::DurableOnly) {
            QueryFinalityRequirement::SafeToLatest
        } else {
            request.finality
        };
    let selector = match request.dataset {
        Dataset::Blocks => datalens_chain::DatasetSelector::all(),
        Dataset::Transactions | Dataset::Receipts => {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "legacy EVM query route does not support transactions or receipts",
            ));
        }
        Dataset::Logs => {
            let filter = request.filter.ok_or_else(|| {
                DatalensError::new(DatalensErrorKind::InvalidInput, "logs require filter")
            })?;
            datalens_chain::DatasetSelector::try_evm_logs(filter)?
        }
    };
    let response_shape = match request.dataset {
        Dataset::Blocks => datalens_planner::ResponseShape::LegacyEvmBlocks,
        Dataset::Transactions | Dataset::Receipts => {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "legacy EVM query route does not support transactions or receipts",
            ));
        }
        Dataset::Logs => datalens_planner::ResponseShape::LegacyEvmLogs,
    };

    Ok(NativeQueryInput {
        chain: request.chain,
        dataset_key: DatasetKey::from(request.dataset),
        ledger_range: LedgerRange::from_block_range(request.range),
        selector,
        response_shape,
        field_selection: datalens_planner::FieldSelection::All,
        finality,
    })
}

#[derive(Clone)]
pub struct QueryService<S> {
    executor: NativeQueryExecutor<Arc<dyn StorageRepository>, S>,
    chain_name: String,
    chain: ChainConfig,
    metrics: Option<MetricsRecorder>,
    warmup: Option<Arc<dyn RegisteredWarmupService>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCacheSummary {
    pub hit_ranges: Vec<LedgerRange>,
    pub missing_ranges: Vec<LedgerRange>,
    pub durable_hit_ranges: Vec<LedgerRange>,
    pub hot_hit_ranges: Vec<LedgerRange>,
    pub provider_fill_ranges: Vec<LedgerRange>,
    pub promotion_pending_ranges: Vec<LedgerRange>,
    pub segments: Vec<datalens_core::QuerySegmentMetadata>,
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
                    staging: datalens_writer::WriteStagingConfig {
                        enabled: writer.staging.enabled,
                        min_rows: writer.staging.min_rows,
                        target_object_bytes: writer.staging.target_object_bytes,
                        max_staged_ranges: writer.staging.max_staged_ranges,
                        max_staged_rows: writer.staging.max_staged_rows,
                        max_staged_age_ms: writer.staging.max_staged_age_ms,
                        flush_on_shutdown: writer.staging.flush_on_shutdown,
                        max_staged_bytes: writer.staging.max_staged_bytes,
                    },
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
            warmup: None,
        })
    }

    pub fn with_metrics(mut self, metrics: MetricsRecorder) -> Self {
        self.executor = self
            .executor
            .with_metrics(metrics.clone(), ApplicationIdentity::unknown());
        self.metrics = Some(metrics);
        self
    }

    pub fn metrics_recorder(&self) -> Option<MetricsRecorder> {
        self.metrics.clone()
    }

    pub fn with_usage_ledger(
        mut self,
        repository: impl UsageLedgerRepository + 'static,
        application: ApplicationIdentity,
    ) -> Self {
        self.executor = self.executor.with_usage_ledger(repository, application);
        self
    }

    pub fn with_warmup_pool<P>(mut self, pool: P) -> Self
    where
        P: RegisteredWarmupService + 'static,
    {
        self.warmup = Some(Arc::new(pool));
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
        let durable_hit_ranges = legacy_block_ranges(&response.cache.durable_hit_ranges)?;
        let hot_hit_ranges = legacy_block_ranges(&response.cache.hot_hit_ranges)?;
        let provider_fill_ranges = legacy_block_ranges(&response.cache.provider_fill_ranges)?;
        let promotion_pending_ranges =
            legacy_block_ranges(&response.cache.promotion_pending_ranges)?;
        let segments = response
            .cache
            .segments
            .iter()
            .map(|segment| {
                Ok(QuerySegment {
                    range: segment.range.block_range().ok_or_else(|| {
                        DatalensError::new(
                            DatalensErrorKind::Internal,
                            "legacy response requires block segment ranges",
                        )
                    })?,
                    source: segment.source,
                    finality: segment.finality,
                })
            })
            .collect::<Result<Vec<_>, DatalensError>>()?;

        Ok(LegacyEvmQueryResponse {
            chain: response.chain,
            range: response_range,
            cache: CacheSummary {
                hit_ranges,
                missing_ranges: misses,
                durable_hit_ranges,
                hot_hit_ranges,
                provider_fill_ranges,
                promotion_pending_ranges,
                segments,
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
                durable_hit_ranges: result.cache.durable_hit_ranges,
                hot_hit_ranges: result.cache.hot_hit_ranges,
                provider_fill_ranges: result.cache.provider_fill_ranges,
                promotion_pending_ranges: result.cache.promotion_pending_ranges,
                segments: result.cache.segments,
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

    pub fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError> {
        self.executor.flush_staged_writes_for_shutdown()
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
            Dataset::Transactions | Dataset::Receipts => Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "legacy EVM query route does not support transactions or receipts",
            )),
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

    fn query_native(
        &self,
        request: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryResponse, DatalensError>;

    fn metrics_text(&self) -> Option<Result<String, DatalensError>>;

    fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError>;

    fn discovery(&self) -> Result<ChainDiscovery, DatalensError>;

    fn warmup(&self) -> Option<Arc<dyn RegisteredWarmupService>>;
}

pub trait RegisteredWarmupService: Send + Sync {
    fn submit(&self, request: WarmupSubmitRequest) -> Result<WarmupSubmitOutcome, DatalensError>;
    fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError>;
    fn list(&self, filter: WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError>;
    fn pause(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError>;
    fn cancel(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError>;
    fn retry_failed(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError>;
    fn run_available_once(&self) -> Result<Vec<WarmupRunResult>, DatalensError>;
}

impl<A, S, R> RegisteredWarmupService for WarmupTaskPool<A, S, R>
where
    A: ChainAdapter,
    S: StorageRepository + Clone + 'static,
    R: WarmupRegistry,
{
    fn submit(&self, request: WarmupSubmitRequest) -> Result<WarmupSubmitOutcome, DatalensError> {
        WarmupTaskPool::submit(self, request)
    }

    fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError> {
        WarmupTaskPool::get(self, task_id)
    }

    fn list(&self, filter: WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError> {
        WarmupTaskPool::list(self, filter)
    }

    fn pause(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        WarmupTaskPool::pause(self, task_id)
    }

    fn cancel(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        WarmupTaskPool::cancel(self, task_id)
    }

    fn retry_failed(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        WarmupTaskPool::retry_failed(self, task_id)
    }

    fn run_available_once(&self) -> Result<Vec<WarmupRunResult>, DatalensError> {
        WarmupTaskPool::run_available_once(self)
    }
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

    fn query_native(
        &self,
        request: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryResponse, DatalensError> {
        QueryService::query_native_with_application(self, request, application)
    }

    fn metrics_text(&self) -> Option<Result<String, DatalensError>> {
        QueryService::metrics_text(self)
    }

    fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError> {
        QueryService::flush_staged_writes_for_shutdown(self)
    }

    fn discovery(&self) -> Result<ChainDiscovery, DatalensError> {
        QueryService::discovery(self)
    }

    fn warmup(&self) -> Option<Arc<dyn RegisteredWarmupService>> {
        self.warmup.clone()
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

    pub fn query_native(
        &self,
        request: NativeQueryInput,
    ) -> Result<NativeQueryResponse, DatalensError> {
        self.query_native_with_application(request, None)
    }

    pub fn query_native_with_application(
        &self,
        request: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryResponse, DatalensError> {
        let chain_name = request.chain.configured_name();
        let service = self.services.get(chain_name).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("chain {chain_name} is not configured"),
            )
        })?;
        service.query_native(request, application)
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
        chain: &str,
        dataset: &str,
        range_len: u128,
        finality: QueryFinalityRequirement,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry
            .authenticate_headers(headers, chain, dataset, range_len, finality)
    }

    fn authenticate_warmup_headers(
        &self,
        headers: &HeaderMap,
        chain: &str,
        dataset: &str,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry
            .authenticate_warmup_headers(headers, chain, dataset)
    }

    fn authenticate_task_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry.authenticate_task_headers(headers)
    }

    pub fn submit_warmup_task(
        &self,
        request: WarmupSubmitRequest,
    ) -> Result<WarmupSubmitOutcome, DatalensError> {
        let service = self.warmup_service_for_chain(request.chain.configured_name())?;
        service.submit(request)
    }

    pub fn get_warmup_task(
        &self,
        task_id: &WarmupTaskId,
    ) -> Result<Option<WarmupTask>, DatalensError> {
        for service in self.services.values() {
            let Some(warmup) = service.warmup() else {
                continue;
            };
            if let Some(task) = warmup.get(task_id)? {
                return Ok(Some(task));
            }
        }
        Ok(None)
    }

    pub fn list_warmup_tasks(
        &self,
        filter: WarmupTaskFilter,
    ) -> Result<Vec<WarmupTask>, DatalensError> {
        let mut tasks = Vec::new();
        for service in self.services.values() {
            let Some(warmup) = service.warmup() else {
                continue;
            };
            tasks.extend(warmup.list(filter.clone())?);
        }
        tasks.sort_by(|left, right| left.task_id.as_str().cmp(right.task_id.as_str()));
        Ok(tasks)
    }

    pub fn pause_warmup_task(&self, task_id: &WarmupTaskId) -> Result<WarmupTask, DatalensError> {
        self.mutate_warmup_task(task_id, WarmupMutation::Pause)
    }

    pub fn cancel_warmup_task(&self, task_id: &WarmupTaskId) -> Result<WarmupTask, DatalensError> {
        self.mutate_warmup_task(task_id, WarmupMutation::Cancel)
    }

    pub fn retry_warmup_task(&self, task_id: &WarmupTaskId) -> Result<WarmupTask, DatalensError> {
        self.mutate_warmup_task(task_id, WarmupMutation::Retry)
    }

    pub fn run_warmup_once(&self) -> Result<Vec<WarmupRunResult>, DatalensError> {
        let mut results = Vec::new();
        for service in self.services.values() {
            let Some(warmup) = service.warmup() else {
                continue;
            };
            results.extend(warmup.run_available_once()?);
        }
        Ok(results)
    }

    pub fn start_warmup_scheduler(&self, interval: Duration) -> WarmupSchedulerHandle {
        let registry = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = stop.clone();
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                if let Err(error) = registry.run_warmup_once() {
                    log::warn!("warmup scheduler tick failed kind={:?}", error.kind);
                }
                thread::sleep(interval);
            }
        });
        WarmupSchedulerHandle {
            stop,
            handle: Some(handle),
        }
    }

    fn warmup_service_for_chain(
        &self,
        chain_name: &str,
    ) -> Result<Arc<dyn RegisteredWarmupService>, DatalensError> {
        self.services
            .get(chain_name)
            .and_then(|service| service.warmup())
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    format!("warmup is not configured for chain {chain_name}"),
                )
            })
    }

    fn mutate_warmup_task(
        &self,
        task_id: &WarmupTaskId,
        mutation: WarmupMutation,
    ) -> Result<WarmupTask, DatalensError> {
        let task = self.get_warmup_task(task_id)?.ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("warmup task {} not found", task_id.as_str()),
            )
        })?;
        let service = self.warmup_service_for_chain(task.chain.configured_name())?;
        match mutation {
            WarmupMutation::Pause => service.pause(task_id)?,
            WarmupMutation::Cancel => service.cancel(task_id)?,
            WarmupMutation::Retry => service.retry_failed(task_id)?,
        }
        service.get(task_id)?.ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!(
                    "warmup task {} disappeared after mutation",
                    task_id.as_str()
                ),
            )
        })
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

    pub fn flush_staged_writes_for_shutdown(
        &self,
    ) -> Result<Vec<DurableWriteResult>, DatalensError> {
        self.services
            .values()
            .map(|service| service.flush_staged_writes_for_shutdown())
            .collect()
    }
}

#[derive(Clone, Copy)]
enum WarmupMutation {
    Pause,
    Cancel,
    Retry,
}

pub struct WarmupSchedulerHandle {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

pub trait LifecycleShutdown: Send + 'static {
    fn shutdown(self);
}

impl WarmupSchedulerHandle {
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take()
            && let Err(error) = handle.join()
        {
            log::warn!("warmup scheduler thread join failed: {error:?}");
        }
    }
}

impl LifecycleShutdown for WarmupSchedulerHandle {
    fn shutdown(self) {
        WarmupSchedulerHandle::shutdown(self);
    }
}

impl Drop for WarmupSchedulerHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take()
            && let Err(error) = handle.join()
        {
            log::warn!("warmup scheduler thread join failed: {error:?}");
        }
    }
}

pub struct ServiceLifecycle<S = NoopLifecycleShutdown> {
    registry: QueryServiceRegistry,
    warmup_scheduler: Option<S>,
}

pub struct NoopLifecycleShutdown;

impl LifecycleShutdown for NoopLifecycleShutdown {
    fn shutdown(self) {}
}

impl ServiceLifecycle<NoopLifecycleShutdown> {
    pub fn new(registry: QueryServiceRegistry) -> Self {
        Self {
            registry,
            warmup_scheduler: None,
        }
    }
}

impl<S> ServiceLifecycle<S> {
    pub fn with_warmup_scheduler<T>(self, scheduler: T) -> ServiceLifecycle<T>
    where
        T: LifecycleShutdown,
    {
        ServiceLifecycle {
            registry: self.registry,
            warmup_scheduler: Some(scheduler),
        }
    }

    fn registry(&self) -> QueryServiceRegistry {
        self.registry.clone()
    }
}

impl<S> ServiceLifecycle<S>
where
    S: LifecycleShutdown,
{
    pub fn shutdown(self) -> Result<(), std::io::Error> {
        if let Some(scheduler) = self.warmup_scheduler {
            scheduler.shutdown();
        }
        flush_registry_staged_writes_for_shutdown(&self.registry)
    }
}

pub fn router(registry: QueryServiceRegistry) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/chains", get(chains))
        .route("/v1/discovery", get(discovery))
        .route("/v1/query", post(query))
        .route("/v1/warmup/tasks", post(warmup_submit).get(warmup_list))
        .route("/v1/warmup/tasks/{task_id}", get(warmup_get))
        .route("/v1/warmup/tasks/{task_id}/pause", post(warmup_pause))
        .route("/v1/warmup/tasks/{task_id}/cancel", post(warmup_cancel))
        .route("/v1/warmup/tasks/{task_id}/retry", post(warmup_retry))
        .route("/v1/warmup/run-once", post(warmup_run_once))
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

async fn warmup_submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WarmupSubmitApiRequest>,
) -> Result<Response, ApiError> {
    let registry = state.registry.clone();
    let dataset = request.dataset_for_auth().map_err(ApiError)?;
    let application_context = registry
        .authenticate_warmup_headers(&headers, request.chain().configured_name(), &dataset)
        .map_err(ApiError)?;
    let application_id = application_context
        .map(|application| application.id)
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

async fn warmup_list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WarmupTaskListQuery>,
) -> Result<Json<WarmupTaskListApiResponse>, ApiError> {
    let registry = state.registry.clone();
    let application_context = registry
        .authenticate_task_headers(&headers)
        .map_err(ApiError)?;
    let application_id = application_context
        .map(|application| application.id)
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

async fn warmup_get(
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

async fn warmup_pause(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    mutate_authorized_warmup_task(state.registry, headers, task_id, WarmupMutation::Pause).await
}

async fn warmup_cancel(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    mutate_authorized_warmup_task(state.registry, headers, task_id, WarmupMutation::Cancel).await
}

async fn warmup_retry(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<WarmupTaskApiResponse>, ApiError> {
    mutate_authorized_warmup_task(state.registry, headers, task_id, WarmupMutation::Retry).await
}

async fn warmup_run_once(
    State(state): State<AppState>,
) -> Result<Json<WarmupRunOnceApiResponse>, ApiError> {
    let registry = state.registry.clone();
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
        .authenticate_task_headers(headers)
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
            .map(|application| application.id)
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
        .authenticate_task_headers(&headers)
        .map_err(ApiError)?;
    authorize_warmup_task_application(
        &task,
        application_context
            .map(|application| application.id)
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

fn warmup_submit_request(
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
            "application is not allowed to mutate another application's warmup task",
        )));
    }
    Ok(())
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
        DatalensErrorKind::UnsupportedDataset | DatalensErrorKind::UnsupportedHotQuery => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
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
        DatalensErrorKind::UnsupportedHotQuery => "unsupported_hot_query",
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
    serve_lifecycle(bind, ServiceLifecycle::new(registry)).await
}

pub async fn serve_lifecycle<S>(
    bind: SocketAddr,
    lifecycle: ServiceLifecycle<S>,
) -> Result<(), std::io::Error>
where
    S: LifecycleShutdown,
{
    let listener = tokio::net::TcpListener::bind(bind).await?;
    log::info!("api listener bound to {bind}");
    let registry = lifecycle.registry();
    axum::serve(listener, router(registry))
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                log::error!("failed to listen for shutdown signal: {error}");
            }
        })
        .await?;
    lifecycle.shutdown()
}

fn flush_registry_staged_writes_for_shutdown(
    registry: &QueryServiceRegistry,
) -> Result<(), std::io::Error> {
    registry
        .flush_staged_writes_for_shutdown()
        .map(|results| {
            let flushed_objects = results
                .iter()
                .map(|result| result.data_objects.len())
                .sum::<usize>();
            if flushed_objects > 0 {
                log::info!("flushed {flushed_objects} staged durable objects during shutdown");
            }
        })
        .map_err(|error| std::io::Error::other(error.to_string()))
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

fn application_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(APPLICATION_IDENTITY_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| auth::normalize_application_id(value).ok())
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
