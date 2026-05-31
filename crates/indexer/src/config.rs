use std::{fmt, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{IndexerError, output::WebhookOutputConfig, webhook_config::RawWebhookOutputConfig};
use parse_config::parse_index_config;
use source_config::RawSourceConfig;
use validation::expand_env_vars;

mod parse_config;
mod source_config;
mod validation;
pub(crate) use parse_config::required_non_empty;
pub use source_config::{
    EvmSourceConfig, SolanaSelectorConfig, SolanaSourceConfig, SourceConfig, TronSourceConfig,
};
pub(crate) use validation::{parse_dataset, required_u64};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensIndexConfig {
    pub client: ClientConfig,
    pub index: IndexConfig,
    pub retry: IndexRetryConfig,
    pub sources: Vec<SourceConfig>,
    pub decode: DecodeConfig,
    pub output: OutputConfig,
    pub query: QueryServiceConfig,
    pub checkpoint: crate::CheckpointPolicy,
}

impl DatalensIndexConfig {
    pub fn from_toml_str(input: &str) -> Result<Self, IndexerError> {
        let expanded = expand_env_vars(input)?;
        let raw = toml::from_str::<RawDatalensIndexConfig>(&expanded)
            .map_err(|error| IndexerError::Config(error.to_string()))?;
        parse_index_config(raw)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    pub endpoint: String,
    pub application: String,
    pub token: ClientToken,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ClientToken {
    env: String,
    value: SecretString,
}

impl ClientToken {
    pub fn env(&self) -> &str {
        &self.env
    }

    pub fn secret(&self) -> &str {
        self.value.as_str()
    }
}

impl ClientConfig {
    pub fn to_datalens_client_config(&self) -> datalens_client::DatalensClientConfig {
        datalens_client::DatalensClientConfig {
            endpoint: self.endpoint.clone(),
            application: Some(self.application.clone()),
            bearer_token: Some(self.token.secret().to_owned()),
        }
    }
}

impl fmt::Debug for ClientToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientToken")
            .field("env", &self.env)
            .field("value", &self.value)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
struct SecretString(String);

impl SecretString {
    fn new(value: String) -> Self {
        Self(value)
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("\"<redacted>\"")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexConfig {
    pub name: String,
    pub dataset: IndexDataset,
    pub finality: FinalityRequirement,
    pub chunk_blocks: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRetryConfig {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for IndexRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 250,
            max_backoff_ms: 30_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexDataset {
    EvmLogs,
    SolanaTransactions,
    SolanaInstructions,
    SolanaAccountUpdates,
    TronEvents,
}

impl IndexDataset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EvmLogs => "evm.logs",
            Self::SolanaTransactions => "solana.transactions",
            Self::SolanaInstructions => "solana.instructions",
            Self::SolanaAccountUpdates => "solana.account_updates",
            Self::TronEvents => "tron.events",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityRequirement {
    Durable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutputConfig {
    Jsonl { path: PathBuf },
    Database { database: DatabaseOutputConfig },
    Parquet { parquet: ParquetOutputConfig },
    Webhook { webhook: WebhookOutputConfig },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParquetOutputConfig {
    pub path: PathBuf,
    pub max_rows_per_file: Option<usize>,
    pub max_bytes_per_file: Option<usize>,
    pub partition_by: Vec<String>,
    pub compression: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseOutputConfig {
    pub driver: DatabaseDriver,
    pub url: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseDriver {
    Sqlite,
    Postgres,
}

impl DatabaseDriver {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryServiceConfig {
    pub enabled: bool,
    pub protocol: QueryProtocol,
    pub bind: String,
    pub path: String,
    pub playground: bool,
    pub metrics: MetricsServiceConfig,
    pub auth: QueryAuthConfig,
    pub views: Vec<GraphqlViewConfig>,
}

impl Default for QueryServiceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            protocol: QueryProtocol::Graphql,
            bind: "127.0.0.1:9090".to_owned(),
            path: "/graphql".to_owned(),
            playground: false,
            metrics: MetricsServiceConfig::default(),
            auth: QueryAuthConfig::default(),
            views: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphqlViewConfig {
    pub name: String,
    pub dataset: String,
    pub event_name: Option<String>,
    pub signature: Option<String>,
    pub fields: Vec<GraphqlViewFieldConfig>,
    pub filters: Vec<GraphqlViewFilterConfig>,
    pub default_limit: u64,
    pub max_limit: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphqlViewFieldConfig {
    pub name: String,
    pub path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphqlViewFilterConfig {
    pub field: String,
    pub equals: String,
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct QueryAuthConfig {
    pub enabled: bool,
    pub applications: Vec<QueryAuthApplicationConfig>,
}

impl fmt::Debug for QueryAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryAuthConfig")
            .field("enabled", &self.enabled)
            .field("applications", &self.applications)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct QueryAuthApplicationConfig {
    pub id: String,
    pub enabled: bool,
    pub token: String,
    pub quota: Option<QueryAuthQuotaConfig>,
}

impl fmt::Debug for QueryAuthApplicationConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryAuthApplicationConfig")
            .field("id", &self.id)
            .field("enabled", &self.enabled)
            .field("token", &"<redacted>")
            .field("quota", &self.quota)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAuthQuotaConfig {
    pub max_requests_per_minute: Option<u64>,
    pub max_concurrent_requests: Option<u64>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct MetricsServiceConfig {
    pub enabled: bool,
    pub path: String,
    pub bearer_token: Option<String>,
}

impl Default for MetricsServiceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/metrics".to_owned(),
            bearer_token: None,
        }
    }
}

impl fmt::Debug for MetricsServiceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetricsServiceConfig")
            .field("enabled", &self.enabled)
            .field("path", &self.path)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QueryProtocol {
    #[default]
    Graphql,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DecodeConfig {
    pub enabled: bool,
    pub abis: Vec<DecodeAbiConfig>,
    pub events: Vec<DecodeEventConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeAbiConfig {
    pub chain: String,
    pub index: String,
    pub dataset: String,
    pub path: Option<PathBuf>,
    pub json: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeEventConfig {
    pub name: String,
    pub signature: String,
    pub topic0: String,
    pub chain: Option<String>,
    pub index: Option<String>,
    pub dataset: Option<String>,
    pub contract: Option<String>,
    pub inputs: Vec<DecodeEventInputConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeEventInputConfig {
    pub name: String,
    pub kind: String,
    pub indexed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDatalensIndexConfig {
    client: Option<RawClientConfig>,
    index: Option<RawIndexConfig>,
    #[serde(default)]
    sources: Vec<RawSourceConfig>,
    decode: Option<RawDecodeConfig>,
    output: Option<RawOutputConfig>,
    query: Option<RawQueryServiceConfig>,
    checkpoint: Option<RawCheckpointConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawClientConfig {
    endpoint: Option<String>,
    application: Option<String>,
    token_env: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIndexConfig {
    name: Option<String>,
    dataset: Option<String>,
    finality: Option<String>,
    chunk_blocks: Option<u64>,
    retry: Option<RawIndexRetryConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIndexRetryConfig {
    max_attempts: Option<u32>,
    initial_backoff_ms: Option<u64>,
    max_backoff_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOutputConfig {
    kind: Option<String>,
    jsonl: Option<RawJsonlOutputConfig>,
    database: Option<RawDatabaseOutputConfig>,
    parquet: Option<RawParquetOutputConfig>,
    webhook: Option<RawWebhookOutputConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawJsonlOutputConfig {
    path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDatabaseOutputConfig {
    driver: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawParquetOutputConfig {
    path: Option<PathBuf>,
    max_rows_per_file: Option<usize>,
    max_bytes_per_file: Option<usize>,
    #[serde(default)]
    partition_by: Vec<String>,
    compression: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQueryServiceConfig {
    #[serde(default)]
    enabled: bool,
    protocol: Option<String>,
    bind: Option<String>,
    path: Option<String>,
    #[serde(default)]
    playground: bool,
    metrics: Option<RawMetricsServiceConfig>,
    auth: Option<RawQueryAuthConfig>,
    #[serde(default)]
    views: Vec<RawGraphqlViewConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGraphqlViewConfig {
    name: Option<String>,
    dataset: Option<String>,
    event_name: Option<String>,
    signature: Option<String>,
    #[serde(default)]
    fields: Vec<RawGraphqlViewFieldConfig>,
    #[serde(default)]
    filters: Vec<RawGraphqlViewFilterConfig>,
    default_limit: Option<u64>,
    max_limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGraphqlViewFieldConfig {
    name: Option<String>,
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGraphqlViewFilterConfig {
    field: Option<String>,
    equals: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMetricsServiceConfig {
    #[serde(default)]
    enabled: bool,
    path: Option<String>,
    bearer_token: Option<String>,
    token_env: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQueryAuthConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    applications: Vec<RawQueryAuthApplicationConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQueryAuthApplicationConfig {
    id: Option<String>,
    #[serde(default = "default_enabled")]
    enabled: bool,
    token: Option<String>,
    token_env: Option<String>,
    max_requests_per_minute: Option<u64>,
    max_concurrent_requests: Option<u64>,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDecodeConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    abis: Vec<RawDecodeAbiConfig>,
    #[serde(default)]
    events: Vec<RawDecodeEventConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDecodeAbiConfig {
    chain: Option<String>,
    index: Option<String>,
    dataset: Option<String>,
    path: Option<PathBuf>,
    json: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDecodeEventConfig {
    name: Option<String>,
    signature: Option<String>,
    topic0: Option<String>,
    chain: Option<String>,
    index: Option<String>,
    dataset: Option<String>,
    contract: Option<String>,
    #[serde(default)]
    inputs: Vec<RawDecodeEventInputConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDecodeEventInputConfig {
    name: Option<String>,
    kind: Option<String>,
    #[serde(default)]
    indexed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCheckpointConfig {
    path: Option<PathBuf>,
}
