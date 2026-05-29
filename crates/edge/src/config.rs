use std::{collections::BTreeMap, env, fs, path::Path};

use datalens_core::{DatalensError, DatalensErrorKind};
use datalens_storage::S3ObjectStoreConfig;
use serde::{Deserialize, Serialize};

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
    pub api: ApiConfig,
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default)]
    pub graphql: GraphqlConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GraphqlConfig {
    #[serde(default = "default_graphql_enabled")]
    pub enabled: bool,
    #[serde(default = "default_graphql_playground_enabled")]
    pub playground_enabled: bool,
}

impl Default for GraphqlConfig {
    fn default() -> Self {
        Self {
            enabled: default_graphql_enabled(),
            playground_enabled: default_graphql_playground_enabled(),
        }
    }
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

fn default_graphql_enabled() -> bool {
    true
}

fn default_graphql_playground_enabled() -> bool {
    true
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
