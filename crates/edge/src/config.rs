use std::{collections::BTreeMap, env, fs, path::Path};

use datalens_cache_repair::{
    default_cache_repair_fetch_timeout_ms, default_cache_repair_lease_ttl_ms,
};
use datalens_core::{DatalensError, DatalensErrorKind, QueryStrategy};
use datalens_storage::{DurableStorageConfig, ParquetCompression, S3ObjectStoreConfig};
use datalens_warmup::DEFAULT_WARMUP_STALE_RUNNING_TTL_MS;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub query: QueryConfig,
    #[serde(default)]
    pub warmup: WarmupConfig,
    #[serde(default)]
    pub cache_repair: CacheRepairConfig,
    #[serde(default)]
    pub edge: EdgeConfig,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub backend: String,
    #[serde(default)]
    pub local: Option<LocalStorageConfig>,
    #[serde(default)]
    pub s3: Option<S3ObjectStoreConfig>,
    #[serde(default)]
    pub parquet: StorageParquetConfig,
    #[serde(default)]
    pub compaction: StorageCompactionConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalStorageConfig {
    pub root: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageParquetConfig {
    #[serde(default)]
    pub compression: ParquetCompression,
}

impl From<StorageParquetConfig> for DurableStorageConfig {
    fn from(config: StorageParquetConfig) -> Self {
        Self {
            parquet_compression: config.compression,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageCompactionConfig {
    #[serde(default = "default_storage_compaction_enabled")]
    pub enabled: bool,
    #[serde(default = "default_storage_compaction_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_storage_compaction_min_object_bytes")]
    pub min_object_bytes: u64,
    #[serde(default = "default_storage_compaction_max_merge_ranges")]
    pub max_merge_ranges: usize,
    #[serde(default = "default_storage_compaction_max_tick_duration_ms")]
    pub max_tick_duration_ms: u64,
    #[serde(default = "default_storage_compaction_max_candidates_per_tick")]
    pub max_candidates_per_tick: usize,
    #[serde(default = "default_storage_compaction_max_manifest_entries_per_tick")]
    pub max_manifest_entries_per_tick: usize,
    #[serde(default)]
    pub delete_source_objects: bool,
}

impl Default for StorageCompactionConfig {
    fn default() -> Self {
        let max_manifest_entries_per_tick =
            default_storage_compaction_max_manifest_entries_per_tick();
        Self {
            enabled: default_storage_compaction_enabled(),
            interval_ms: default_storage_compaction_interval_ms(),
            min_object_bytes: default_storage_compaction_min_object_bytes(),
            max_merge_ranges: default_storage_compaction_max_merge_ranges(),
            max_tick_duration_ms: default_storage_compaction_max_tick_duration_ms(),
            max_candidates_per_tick: default_storage_compaction_max_candidates_per_tick(),
            max_manifest_entries_per_tick,
            delete_source_objects: false,
        }
    }
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
#[serde(deny_unknown_fields)]
pub struct EdgeConfig {
    #[serde(default)]
    pub metrics: MetricsEndpointConfig,
    #[serde(skip)]
    pub query: QueryConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryConfig {
    #[serde(default = "default_native_query_surface")]
    pub native: GraphqlSurfaceConfig,
    #[serde(default = "default_index_query_surface")]
    pub index: GraphqlSurfaceConfig,
    #[serde(default)]
    pub metadata: QueryMetadataConfig,
    #[serde(default)]
    pub durable_intents: QueryDurableIntentConfig,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            native: default_native_query_surface(),
            index: default_index_query_surface(),
            metadata: QueryMetadataConfig::default(),
            durable_intents: QueryDurableIntentConfig::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryMetadataConfig {
    #[serde(default = "default_query_metadata_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_query_metadata_worker_threads")]
    pub worker_threads: usize,
    #[serde(default = "default_query_metadata_coalesced_capacity")]
    pub coalesced_capacity: usize,
}

impl Default for QueryMetadataConfig {
    fn default() -> Self {
        Self {
            queue_capacity: default_query_metadata_queue_capacity(),
            worker_threads: default_query_metadata_worker_threads(),
            coalesced_capacity: default_query_metadata_coalesced_capacity(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryDurableIntentConfig {
    #[serde(default = "default_query_durable_intent_enabled")]
    pub enabled: bool,
    #[serde(default = "default_query_durable_intent_worker_threads")]
    pub worker_threads: usize,
    #[serde(default = "default_query_durable_intent_claim_batch_size")]
    pub claim_batch_size: usize,
    #[serde(default)]
    pub terminal_retention_seconds: Option<u64>,
    #[serde(default = "default_query_durable_intent_cleanup_max_scan")]
    pub cleanup_max_scan: usize,
    #[serde(default = "default_query_durable_intent_cleanup_max_deletes")]
    pub cleanup_max_deletes: usize,
    #[serde(default = "default_query_durable_intent_cleanup_interval_seconds")]
    pub cleanup_interval_seconds: u64,
}

impl Default for QueryDurableIntentConfig {
    fn default() -> Self {
        Self {
            enabled: default_query_durable_intent_enabled(),
            worker_threads: default_query_durable_intent_worker_threads(),
            claim_batch_size: default_query_durable_intent_claim_batch_size(),
            terminal_retention_seconds: None,
            cleanup_max_scan: default_query_durable_intent_cleanup_max_scan(),
            cleanup_max_deletes: default_query_durable_intent_cleanup_max_deletes(),
            cleanup_interval_seconds: default_query_durable_intent_cleanup_interval_seconds(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GraphqlSurfaceConfig {
    #[serde(default = "default_graphql_enabled")]
    pub graphql_enabled: bool,
    #[serde(default = "default_native_graphql_path")]
    pub path: String,
    #[serde(default = "default_graphql_playground_enabled")]
    pub playground_enabled: bool,
    #[serde(default = "default_native_graphql_playground_path")]
    pub playground_path: String,
}

impl Default for GraphqlSurfaceConfig {
    fn default() -> Self {
        default_native_query_surface()
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
    #[serde(default = "default_warmup_follow_query_lookahead_blocks")]
    pub follow_query_lookahead_blocks: u64,
    #[serde(default)]
    pub follow_query_start_offset_blocks: Option<u64>,
    #[serde(default)]
    pub follow_query_start_offset_tiers_blocks: Option<Vec<u64>>,
    #[serde(default = "default_warmup_follow_query_catchup_threshold_blocks")]
    pub follow_query_catchup_threshold_blocks: u64,
    #[serde(default)]
    pub follow_query_idle_threshold_blocks: Option<u64>,
    #[serde(default)]
    pub follow_query_resume_threshold_blocks: Option<u64>,
    #[serde(default = "default_warmup_query_activity_ttl_seconds")]
    pub query_activity_ttl_seconds: u64,
    #[serde(default = "default_warmup_stale_running_ttl_ms")]
    pub stale_running_ttl_ms: u64,
    #[serde(default = "default_warmup_flush_on_shutdown")]
    pub flush_on_shutdown: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheRepairConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_cache_repair_registry_path")]
    pub registry_path: String,
    #[serde(default = "default_cache_repair_fetch_timeout_ms")]
    pub fetch_timeout_ms: u64,
    #[serde(default = "default_cache_repair_lease_ttl_ms")]
    pub lease_ttl_ms: u64,
}

impl Default for CacheRepairConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            registry_path: default_cache_repair_registry_path(),
            fetch_timeout_ms: default_cache_repair_fetch_timeout_ms(),
            lease_ttl_ms: default_cache_repair_lease_ttl_ms(),
        }
    }
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
            follow_query_lookahead_blocks: default_warmup_follow_query_lookahead_blocks(),
            follow_query_start_offset_blocks: None,
            follow_query_start_offset_tiers_blocks: None,
            follow_query_catchup_threshold_blocks:
                default_warmup_follow_query_catchup_threshold_blocks(),
            follow_query_idle_threshold_blocks: None,
            follow_query_resume_threshold_blocks: None,
            query_activity_ttl_seconds: default_warmup_query_activity_ttl_seconds(),
            stale_running_ttl_ms: default_warmup_stale_running_ttl_ms(),
            flush_on_shutdown: default_warmup_flush_on_shutdown(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    false
}

fn default_graphql_playground_enabled() -> bool {
    false
}

fn default_storage_compaction_enabled() -> bool {
    true
}

fn default_storage_compaction_interval_ms() -> u64 {
    60_000
}

fn default_storage_compaction_min_object_bytes() -> u64 {
    1_048_576
}

fn default_storage_compaction_max_merge_ranges() -> usize {
    32
}

fn default_storage_compaction_max_tick_duration_ms() -> u64 {
    30_000
}

fn default_storage_compaction_max_candidates_per_tick() -> usize {
    8
}

fn default_storage_compaction_max_manifest_entries_per_tick() -> usize {
    20_000
}

fn default_native_query_surface() -> GraphqlSurfaceConfig {
    GraphqlSurfaceConfig {
        graphql_enabled: default_graphql_enabled(),
        path: default_native_graphql_path(),
        playground_enabled: default_graphql_playground_enabled(),
        playground_path: default_native_graphql_playground_path(),
    }
}

fn default_index_query_surface() -> GraphqlSurfaceConfig {
    GraphqlSurfaceConfig {
        graphql_enabled: default_graphql_enabled(),
        path: default_index_graphql_path(),
        playground_enabled: default_graphql_playground_enabled(),
        playground_path: default_index_graphql_playground_path(),
    }
}

fn default_native_graphql_path() -> String {
    "/native/graphql".to_owned()
}

fn default_native_graphql_playground_path() -> String {
    "/native/graphiql".to_owned()
}

fn default_index_graphql_path() -> String {
    "/index/graphql".to_owned()
}

fn default_index_graphql_playground_path() -> String {
    "/index/graphiql".to_owned()
}

fn default_query_metadata_queue_capacity() -> usize {
    8192
}

fn default_query_metadata_worker_threads() -> usize {
    4
}

fn default_query_metadata_coalesced_capacity() -> usize {
    2048
}

fn default_query_durable_intent_worker_threads() -> usize {
    2
}

fn default_query_durable_intent_enabled() -> bool {
    true
}

fn default_query_durable_intent_claim_batch_size() -> usize {
    16
}

fn default_query_durable_intent_cleanup_max_scan() -> usize {
    1024
}

fn default_query_durable_intent_cleanup_max_deletes() -> usize {
    256
}

fn default_query_durable_intent_cleanup_interval_seconds() -> u64 {
    300
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetricsEndpointConfig {
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub bearer_token: Option<String>,
}

fn default_warmup_registry_path() -> String {
    ".datalens/warmup".to_owned()
}

fn default_cache_repair_registry_path() -> String {
    ".datalens/cache-repair".to_owned()
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

fn default_warmup_follow_query_lookahead_blocks() -> u64 {
    100
}

fn default_warmup_follow_query_catchup_threshold_blocks() -> u64 {
    200
}

fn default_warmup_query_activity_ttl_seconds() -> u64 {
    300
}

fn default_warmup_stale_running_ttl_ms() -> u64 {
    DEFAULT_WARMUP_STALE_RUNNING_TTL_MS
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
    pub operations: Vec<ApplicationOperationConfig>,
    #[serde(default)]
    pub quota: Option<ApplicationQuotaConfig>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationOperationConfig {
    Query,
    Discovery,
    WarmupSubmit,
    WarmupRead,
    WarmupMutate,
    WarmupRun,
    CacheRepairSubmit,
    CacheRepairRead,
    CacheRepairMutate,
    CacheRepairRun,
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
    #[serde(default)]
    pub rpc_url: Option<String>,
    #[serde(default)]
    pub rpc_urls: Vec<String>,
    #[serde(default)]
    pub rpc: Option<ChainRpcConfig>,
    #[serde(default)]
    pub warmup: ChainWarmupConfig,
    #[serde(default)]
    pub trongrid: TronGridConfig,
    #[serde(default)]
    pub finality: FinalityConfig,
    pub datasets: DatasetsConfig,
}

impl ChainConfig {
    pub fn primary_rpc_url(&self) -> Option<&str> {
        if let Some(rpc) = &self.rpc {
            return Some(rpc.primary_url.as_str());
        }
        self.rpc_url
            .as_deref()
            .or_else(|| self.rpc_urls.first().map(String::as_str))
    }

    pub fn secondary_rpc_urls(&self) -> &[String] {
        if let Some(rpc) = &self.rpc {
            return &rpc.secondary_urls;
        }
        if self.rpc_urls.len() > 1 {
            return &self.rpc_urls[1..];
        }
        &[]
    }

    pub fn rpc_provider_urls(&self) -> Vec<String> {
        let Some(primary_url) = self.primary_rpc_url() else {
            return Vec::new();
        };
        let mut urls = vec![primary_url.to_owned()];
        urls.extend(self.secondary_rpc_urls().iter().cloned());
        urls
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainRpcConfig {
    pub primary_url: String,
    #[serde(default)]
    pub secondary_urls: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainWarmupConfig {
    #[serde(default)]
    pub follow_query_start_offset_blocks: Option<u64>,
    #[serde(default)]
    pub follow_query_start_offset_tiers_blocks: Option<Vec<u64>>,
    #[serde(default)]
    pub follow_query_catchup_threshold_blocks: Option<u64>,
    #[serde(default)]
    pub follow_query_idle_threshold_blocks: Option<u64>,
    #[serde(default)]
    pub follow_query_resume_threshold_blocks: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TronGridConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_trongrid_contract_events_min_interval_ms")]
    pub contract_events_min_interval_ms: u64,
    #[serde(default = "default_trongrid_contract_events_backoff_ms")]
    pub contract_events_backoff_ms: u64,
    #[serde(default = "default_trongrid_contract_events_max_attempts")]
    pub contract_events_max_attempts: usize,
    #[serde(default = "default_trongrid_contract_events_max_range_blocks")]
    pub contract_events_max_range_blocks: u64,
}

impl Default for TronGridConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: None,
            api_key: None,
            contract_events_min_interval_ms: default_trongrid_contract_events_min_interval_ms(),
            contract_events_backoff_ms: default_trongrid_contract_events_backoff_ms(),
            contract_events_max_attempts: default_trongrid_contract_events_max_attempts(),
            contract_events_max_range_blocks: default_trongrid_contract_events_max_range_blocks(),
        }
    }
}

fn default_trongrid_contract_events_min_interval_ms() -> u64 {
    1_000
}

fn default_trongrid_contract_events_backoff_ms() -> u64 {
    1_000
}

fn default_trongrid_contract_events_max_attempts() -> usize {
    5
}

fn default_trongrid_contract_events_max_range_blocks() -> u64 {
    1_000
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
    #[serde(default = "default_true")]
    pub reliability_enabled: bool,
    #[serde(default = "default_true")]
    pub receipt_fallback_enabled: bool,
    #[serde(default)]
    pub query_strategy: QueryStrategy,
    pub max_get_logs_range_blocks: u64,
    #[serde(default = "default_max_block_scan_range_blocks")]
    pub max_block_scan_range_blocks: u64,
    pub max_addresses_per_query: usize,
    #[serde(default = "default_evm_log_header_fetch_mode")]
    pub header_fetch_mode: String,
    #[serde(default = "default_evm_log_header_fetch_concurrency")]
    pub header_fetch_concurrency: usize,
    #[serde(default = "default_evm_log_header_fetch_batch_size")]
    pub header_fetch_batch_size: usize,
    #[serde(default = "default_evm_log_header_cache_max_entries")]
    pub header_cache_max_entries: usize,
    #[serde(default = "default_evm_log_header_durable_chunk_size_blocks")]
    pub header_durable_chunk_size_blocks: u64,
}

fn default_max_block_scan_range_blocks() -> u64 {
    100
}

fn default_true() -> bool {
    true
}

fn default_evm_log_header_fetch_mode() -> String {
    "batch".to_owned()
}

fn default_evm_log_header_fetch_concurrency() -> usize {
    8
}

fn default_evm_log_header_fetch_batch_size() -> usize {
    20
}

fn default_evm_log_header_cache_max_entries() -> usize {
    50_000
}

fn default_evm_log_header_durable_chunk_size_blocks() -> u64 {
    1_000
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
        let value = env::var(name).unwrap_or_default();
        expanded.push_str(&value);
        rest = &tail[end + 1..];
    }
    expanded.push_str(rest);
    Ok(expanded)
}
