use std::collections::BTreeSet;
use std::{env, fmt, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    IndexerError,
    output::WebhookOutputConfig,
    webhook_config::{RawWebhookOutputConfig, parse_webhook_output},
};
use source_config::{RawSourceConfig, parse_sources};

mod source_config;
pub use source_config::{
    EvmSourceConfig, SolanaSelectorConfig, SolanaSourceConfig, SourceConfig, TronSourceConfig,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensIndexConfig {
    pub client: ClientConfig,
    pub index: IndexConfig,
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
        Self::try_from(raw)
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

impl TryFrom<RawDatalensIndexConfig> for DatalensIndexConfig {
    type Error = IndexerError;

    fn try_from(raw: RawDatalensIndexConfig) -> Result<Self, Self::Error> {
        let mut errors = Vec::new();

        let client = match raw.client {
            Some(raw) => parse_client(raw, &mut errors).unwrap_or_else(empty_client),
            None => {
                errors.push("client: missing required table".to_owned());
                empty_client()
            }
        };
        let index = match raw.index {
            Some(raw) => parse_index(raw, &mut errors).unwrap_or_else(empty_index),
            None => {
                errors.push("index: missing required table".to_owned());
                empty_index()
            }
        };
        let sources = parse_sources(raw.sources, &mut errors);
        let output = match raw.output {
            Some(raw) => parse_output(raw, &mut errors).unwrap_or_else(|| OutputConfig::Jsonl {
                path: PathBuf::new(),
            }),
            None => {
                errors.push("output: missing required output table".to_owned());
                OutputConfig::Jsonl {
                    path: PathBuf::new(),
                }
            }
        };
        let decode = parse_decode(raw.decode, &mut errors);
        let query = parse_query(raw.query, &mut errors);
        validate_output_query_capabilities(&output, &query, &mut errors);
        let checkpoint = match raw.checkpoint {
            Some(raw) => parse_checkpoint(raw, &mut errors).unwrap_or_else(|| {
                crate::CheckpointPolicy::File {
                    path: PathBuf::new(),
                }
            }),
            None => {
                errors.push("checkpoint.path: missing required field".to_owned());
                crate::CheckpointPolicy::File {
                    path: PathBuf::new(),
                }
            }
        };

        if !errors.is_empty() {
            return Err(IndexerError::Config(errors.join("; ")));
        }

        Ok(Self {
            client,
            index,
            sources,
            decode,
            output,
            query,
            checkpoint,
        })
    }
}

fn parse_client(raw: RawClientConfig, errors: &mut Vec<String>) -> Option<ClientConfig> {
    let endpoint = required_non_empty("client.endpoint", raw.endpoint, errors);
    let application = required_non_empty("client.application", raw.application, errors);
    let token_env = required_non_empty("client.token_env", raw.token_env, errors);
    let token = token_env.and_then(|token_env| {
        env::var(&token_env)
            .map(|value| ClientToken {
                env: token_env.clone(),
                value: SecretString::new(value),
            })
            .map_err(|_| {
                errors.push(format!(
                    "client.token_env: environment variable {token_env} is not set"
                ));
            })
            .ok()
    });

    Some(ClientConfig {
        endpoint: endpoint?,
        application: application?,
        token: token?,
    })
}

fn parse_index(raw: RawIndexConfig, errors: &mut Vec<String>) -> Option<IndexConfig> {
    let name = required_non_empty("index.name", raw.name, errors);
    let dataset = required_non_empty("index.dataset", raw.dataset, errors)
        .and_then(|value| parse_dataset("index.dataset", &value, errors));
    let finality = match required_non_empty("index.finality", raw.finality, errors).as_deref() {
        Some("durable") => Some(FinalityRequirement::Durable),
        Some(value) => {
            errors.push(format!(
                "index.finality: unsupported finality {value}; supported value is durable"
            ));
            None
        }
        None => None,
    };
    let chunk_blocks = match raw.chunk_blocks {
        Some(0) => {
            errors.push("index.chunk_blocks: must be greater than 0".to_owned());
            None
        }
        Some(value) => Some(value),
        None => {
            errors.push("index.chunk_blocks: missing required field".to_owned());
            None
        }
    };

    Some(IndexConfig {
        name: name?,
        dataset: dataset?,
        finality: finality?,
        chunk_blocks: chunk_blocks?,
    })
}

pub(super) fn parse_dataset(
    field: &str,
    value: &str,
    errors: &mut Vec<String>,
) -> Option<IndexDataset> {
    match value {
        "evm.logs" => Some(IndexDataset::EvmLogs),
        "solana.transactions" => Some(IndexDataset::SolanaTransactions),
        "solana.instructions" => Some(IndexDataset::SolanaInstructions),
        "solana.account_updates" => Some(IndexDataset::SolanaAccountUpdates),
        "tron.events" => Some(IndexDataset::TronEvents),
        value => {
            errors.push(format!(
                "{field}: unsupported dataset {value}; supported values are evm.logs, solana.transactions, solana.instructions, solana.account_updates, and tron.events"
            ));
            None
        }
    }
}

fn parse_output(raw: RawOutputConfig, errors: &mut Vec<String>) -> Option<OutputConfig> {
    match raw.kind.as_deref() {
        Some("database") => parse_database_output(raw.database, errors),
        Some("parquet") => parse_parquet_output(raw.parquet, errors),
        Some("webhook") => parse_webhook_output(raw.webhook, errors),
        Some("jsonl") | None => parse_jsonl_output(raw.jsonl, errors),
        Some(value) => {
            errors.push(format!(
                "output.kind: unsupported output kind {value}; supported values are jsonl, database, parquet, and webhook"
            ));
            None
        }
    }
}

fn parse_jsonl_output(
    raw: Option<RawJsonlOutputConfig>,
    errors: &mut Vec<String>,
) -> Option<OutputConfig> {
    let Some(jsonl) = raw else {
        errors.push("output: missing required jsonl output table".to_owned());
        return None;
    };
    let path = required_path("output.jsonl.path", jsonl.path, errors)?;
    Some(OutputConfig::Jsonl { path })
}

fn parse_database_output(
    raw: Option<RawDatabaseOutputConfig>,
    errors: &mut Vec<String>,
) -> Option<OutputConfig> {
    let Some(database) = raw else {
        errors.push("output.database: missing required table".to_owned());
        return None;
    };
    let driver = match required_non_empty("output.database.driver", database.driver, errors)
        .as_deref()
    {
        Some("sqlite") => Some(DatabaseDriver::Sqlite),
        Some("postgres") => Some(DatabaseDriver::Postgres),
        Some(value) => {
            errors.push(format!(
                    "output.database.driver: unsupported driver {value}; supported values are sqlite and postgres"
                ));
            None
        }
        None => None,
    };
    let url = required_non_empty("output.database.url", database.url, errors);

    Some(OutputConfig::Database {
        database: DatabaseOutputConfig {
            driver: driver?,
            url: url?,
        },
    })
}

fn parse_parquet_output(
    raw: Option<RawParquetOutputConfig>,
    errors: &mut Vec<String>,
) -> Option<OutputConfig> {
    let Some(parquet) = raw else {
        errors.push("output.parquet: missing required table".to_owned());
        return None;
    };
    let path = required_path("output.parquet.path", parquet.path, errors);
    validate_optional_positive_usize(
        "output.parquet.max_rows_per_file",
        parquet.max_rows_per_file,
        errors,
    );
    validate_optional_positive_usize(
        "output.parquet.max_bytes_per_file",
        parquet.max_bytes_per_file,
        errors,
    );
    validate_parquet_partitions(&parquet.partition_by, errors);
    validate_parquet_compression(parquet.compression.as_deref(), errors);

    Some(OutputConfig::Parquet {
        parquet: ParquetOutputConfig {
            path: path?,
            max_rows_per_file: parquet.max_rows_per_file,
            max_bytes_per_file: parquet.max_bytes_per_file,
            partition_by: parquet.partition_by,
            compression: parquet.compression,
        },
    })
}

fn parse_query(raw: Option<RawQueryServiceConfig>, errors: &mut Vec<String>) -> QueryServiceConfig {
    let Some(raw) = raw else {
        return QueryServiceConfig::default();
    };
    let default = QueryServiceConfig::default();
    let protocol = match raw.protocol.as_deref().unwrap_or("graphql") {
        "graphql" => QueryProtocol::Graphql,
        value => {
            errors.push(format!(
                "query.protocol: unsupported protocol {value}; supported value is graphql"
            ));
            QueryProtocol::Graphql
        }
    };
    let bind = raw
        .bind
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(default.bind);
    let path = raw
        .path
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(default.path);
    if !path.starts_with('/') {
        errors.push("query.path: must start with /".to_owned());
    }
    let metrics = parse_query_metrics(raw.metrics, &path, errors);
    let auth = parse_query_auth(raw.auth, errors);
    let views = parse_graphql_views(raw.views, errors);

    QueryServiceConfig {
        enabled: raw.enabled,
        protocol,
        bind,
        path,
        playground: raw.playground,
        metrics,
        auth,
        views,
    }
}

fn parse_graphql_views(
    raw: Vec<RawGraphqlViewConfig>,
    errors: &mut Vec<String>,
) -> Vec<GraphqlViewConfig> {
    let views = raw
        .into_iter()
        .enumerate()
        .filter_map(|(index, view)| parse_graphql_view(index, view, errors))
        .collect::<Vec<_>>();
    let mut names = BTreeSet::new();
    for view in &views {
        if !names.insert(view.name.clone()) {
            errors.push(format!(
                "query.views: GraphQL view name {} is registered more than once",
                view.name
            ));
        }
    }
    views
}

fn parse_graphql_view(
    index: usize,
    raw: RawGraphqlViewConfig,
    errors: &mut Vec<String>,
) -> Option<GraphqlViewConfig> {
    let field = format!("query.views[{index}]");
    let name = required_non_empty(&format!("{field}.name"), raw.name, errors)
        .and_then(|value| validate_graphql_identifier(&format!("{field}.name"), value, errors));
    let dataset = required_non_empty(&format!("{field}.dataset"), raw.dataset, errors);
    let event_name = optional_non_empty(&format!("{field}.event_name"), raw.event_name, errors);
    let signature = optional_non_empty(&format!("{field}.signature"), raw.signature, errors);
    let fields = raw
        .fields
        .into_iter()
        .enumerate()
        .filter_map(|(field_index, raw_field)| {
            parse_graphql_view_field(&format!("{field}.fields[{field_index}]"), raw_field, errors)
        })
        .collect::<Vec<_>>();
    if fields.is_empty() {
        errors.push(format!("{field}.fields: at least one field is required"));
    }
    let mut field_names = BTreeSet::new();
    for view_field in &fields {
        if !field_names.insert(view_field.name.clone()) {
            errors.push(format!(
                "{field}.fields: GraphQL field name {} is registered more than once",
                view_field.name
            ));
        }
    }
    let filters = raw
        .filters
        .into_iter()
        .enumerate()
        .filter_map(|(filter_index, raw_filter)| {
            parse_graphql_view_filter(
                &format!("{field}.filters[{filter_index}]"),
                raw_filter,
                errors,
            )
        })
        .collect::<Vec<_>>();
    let default_limit = raw
        .default_limit
        .unwrap_or(crate::graphql::DEFAULT_EVENT_LIMIT);
    let max_limit = raw.max_limit.unwrap_or(crate::graphql::MAX_EVENT_LIMIT);
    if default_limit == 0 {
        errors.push(format!("{field}.default_limit: must be greater than 0"));
    }
    if max_limit == 0 {
        errors.push(format!("{field}.max_limit: must be greater than 0"));
    }
    if default_limit > max_limit {
        errors.push(format!(
            "{field}.default_limit: must be less than or equal to max_limit"
        ));
    }

    Some(GraphqlViewConfig {
        name: name?,
        dataset: dataset?,
        event_name,
        signature,
        fields,
        filters,
        default_limit,
        max_limit,
    })
}

fn parse_graphql_view_field(
    field: &str,
    raw: RawGraphqlViewFieldConfig,
    errors: &mut Vec<String>,
) -> Option<GraphqlViewFieldConfig> {
    let name = required_non_empty(&format!("{field}.name"), raw.name, errors)
        .and_then(|value| validate_graphql_identifier(&format!("{field}.name"), value, errors));
    let path = required_non_empty(&format!("{field}.path"), raw.path, errors);
    Some(GraphqlViewFieldConfig {
        name: name?,
        path: path?,
    })
}

fn parse_graphql_view_filter(
    field: &str,
    raw: RawGraphqlViewFilterConfig,
    errors: &mut Vec<String>,
) -> Option<GraphqlViewFilterConfig> {
    Some(GraphqlViewFilterConfig {
        field: required_non_empty(&format!("{field}.field"), raw.field, errors)?,
        equals: required_non_empty(&format!("{field}.equals"), raw.equals, errors)?,
    })
}

fn parse_query_metrics(
    raw: Option<RawMetricsServiceConfig>,
    query_path: &str,
    errors: &mut Vec<String>,
) -> MetricsServiceConfig {
    let Some(raw) = raw else {
        return MetricsServiceConfig::default();
    };
    let default = MetricsServiceConfig::default();
    let path = raw
        .path
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(default.path);
    if !path.starts_with('/') {
        errors.push("query.metrics.path: must start with /".to_owned());
    }
    if path == query_path {
        errors.push("query.metrics.path: must not equal query.path".to_owned());
    }
    let bearer_token = match raw.token_env {
        Some(token_env) if !token_env.trim().is_empty() => env::var(&token_env)
            .map_err(|_| {
                errors.push(format!(
                    "query.metrics.token_env: environment variable {token_env} is not set"
                ));
            })
            .ok(),
        Some(_) => {
            errors.push("query.metrics.token_env: must not be empty".to_owned());
            None
        }
        None => raw.bearer_token.filter(|value| !value.trim().is_empty()),
    };

    MetricsServiceConfig {
        enabled: raw.enabled,
        path,
        bearer_token,
    }
}

fn parse_query_auth(raw: Option<RawQueryAuthConfig>, errors: &mut Vec<String>) -> QueryAuthConfig {
    let Some(raw) = raw else {
        return QueryAuthConfig::default();
    };
    let applications = raw
        .applications
        .into_iter()
        .enumerate()
        .filter_map(|(index, application)| parse_query_auth_application(index, application, errors))
        .collect::<Vec<_>>();
    let mut ids = BTreeSet::new();
    for application in &applications {
        if !ids.insert(application.id.clone()) {
            errors.push(format!(
                "query.auth.applications: application id {} is registered more than once",
                application.id
            ));
        }
    }
    if raw.enabled && applications.is_empty() {
        errors.push(
            "query.auth.applications: at least one application is required when query auth is enabled"
                .to_owned(),
        );
    }
    QueryAuthConfig {
        enabled: raw.enabled,
        applications,
    }
}

fn parse_query_auth_application(
    index: usize,
    raw: RawQueryAuthApplicationConfig,
    errors: &mut Vec<String>,
) -> Option<QueryAuthApplicationConfig> {
    let field = format!("query.auth.applications[{index}]");
    let id = required_non_empty(&format!("{field}.id"), raw.id, errors)
        .and_then(|value| normalize_application_id(&format!("{field}.id"), &value, errors));
    let token = match raw.token_env {
        Some(token_env) if !token_env.trim().is_empty() => env::var(&token_env)
            .map_err(|_| {
                errors.push(format!(
                    "{field}.token_env: environment variable {token_env} is not set"
                ));
            })
            .ok(),
        Some(_) => {
            errors.push(format!("{field}.token_env: must not be empty"));
            None
        }
        None => raw.token.filter(|value| !value.trim().is_empty()),
    };
    if token.is_none() {
        errors.push(format!("{field}.token: token or token_env is required"));
    }
    validate_optional_positive_u64(
        &format!("{field}.max_requests_per_minute"),
        raw.max_requests_per_minute,
        errors,
    );
    validate_optional_positive_u64(
        &format!("{field}.max_concurrent_requests"),
        raw.max_concurrent_requests,
        errors,
    );
    let quota = if raw.max_requests_per_minute.is_some() || raw.max_concurrent_requests.is_some() {
        Some(QueryAuthQuotaConfig {
            max_requests_per_minute: raw.max_requests_per_minute,
            max_concurrent_requests: raw.max_concurrent_requests,
        })
    } else {
        None
    };

    Some(QueryAuthApplicationConfig {
        id: id?,
        enabled: raw.enabled,
        token: token?,
        quota,
    })
}

fn parse_decode(raw: Option<RawDecodeConfig>, errors: &mut Vec<String>) -> DecodeConfig {
    let Some(raw) = raw else {
        return DecodeConfig::default();
    };
    let abis = raw
        .abis
        .into_iter()
        .enumerate()
        .filter_map(|(index, abi)| parse_decode_abi(index, abi, errors))
        .collect::<Vec<_>>();
    let events = raw
        .events
        .into_iter()
        .enumerate()
        .filter_map(|(index, event)| parse_decode_event(index, event, errors))
        .collect::<Vec<_>>();
    if raw.enabled && abis.is_empty() && events.is_empty() {
        errors.push(
            "decode: at least one ABI or event is required when decode is enabled".to_owned(),
        );
    }
    DecodeConfig {
        enabled: raw.enabled,
        abis,
        events,
    }
}

fn parse_decode_abi(
    abi_index: usize,
    raw: RawDecodeAbiConfig,
    errors: &mut Vec<String>,
) -> Option<DecodeAbiConfig> {
    let prefix = format!("decode.abis[{abi_index}]");
    let chain = required_non_empty(&format!("{prefix}.chain"), raw.chain, errors);
    let index = required_non_empty(&format!("{prefix}.index"), raw.index, errors);
    let dataset = required_non_empty(&format!("{prefix}.dataset"), raw.dataset, errors)
        .and_then(|value| parse_dataset(&format!("{prefix}.dataset"), &value, errors))
        .map(|dataset| dataset.as_str().to_owned());
    let path = raw.path.and_then(|path| {
        if path.as_os_str().is_empty() {
            errors.push(format!("{prefix}.path: must not be empty"));
            None
        } else {
            Some(path)
        }
    });
    let json = raw.json.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            errors.push(format!("{prefix}.json: must not be empty"));
            None
        } else {
            Some(value.to_owned())
        }
    });
    if path.is_none() && json.is_none() {
        errors.push(format!("{prefix}: path or json is required"));
    }
    if path.is_some() && json.is_some() {
        errors.push(format!("{prefix}: path and json are mutually exclusive"));
    }

    Some(DecodeAbiConfig {
        chain: chain?,
        index: index?,
        dataset: dataset?,
        path,
        json,
    })
}

fn parse_decode_event(
    event_index: usize,
    raw: RawDecodeEventConfig,
    errors: &mut Vec<String>,
) -> Option<DecodeEventConfig> {
    let prefix = format!("decode.events[{event_index}]");
    let name = required_non_empty(&format!("{prefix}.name"), raw.name, errors);
    let signature = required_non_empty(&format!("{prefix}.signature"), raw.signature, errors);
    let topic0 = required_non_empty(&format!("{prefix}.topic0"), raw.topic0, errors)
        .and_then(|value| validate_topic0(&format!("{prefix}.topic0"), value, errors));
    let chain = optional_non_empty(&format!("{prefix}.chain"), raw.chain, errors);
    let index = optional_non_empty(&format!("{prefix}.index"), raw.index, errors);
    let dataset = optional_non_empty(&format!("{prefix}.dataset"), raw.dataset, errors)
        .and_then(|value| parse_dataset(&format!("{prefix}.dataset"), &value, errors))
        .map(|dataset| dataset.as_str().to_owned());
    let contract = raw.contract.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            errors.push(format!("{prefix}.contract: must not be empty"));
            None
        } else {
            Some(value.to_owned())
        }
    });
    let inputs = raw
        .inputs
        .into_iter()
        .enumerate()
        .filter_map(|(input_index, input)| {
            parse_decode_event_input(&format!("{prefix}.inputs[{input_index}]"), input, errors)
        })
        .collect::<Vec<_>>();

    Some(DecodeEventConfig {
        name: name?,
        signature: signature?,
        topic0: topic0?,
        chain,
        index,
        dataset,
        contract,
        inputs,
    })
}

fn parse_decode_event_input(
    prefix: &str,
    raw: RawDecodeEventInputConfig,
    errors: &mut Vec<String>,
) -> Option<DecodeEventInputConfig> {
    Some(DecodeEventInputConfig {
        name: required_non_empty(&format!("{prefix}.name"), raw.name, errors)?,
        kind: required_non_empty(&format!("{prefix}.kind"), raw.kind, errors)?,
        indexed: raw.indexed,
    })
}

fn validate_topic0(field: &str, value: String, errors: &mut Vec<String>) -> Option<String> {
    let hex = value.strip_prefix("0x");
    if hex.is_none_or(|hex| hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())) {
        errors.push(format!("{field}: must be a 0x-prefixed 32-byte hex value"));
        None
    } else {
        Some(value.to_ascii_lowercase())
    }
}

fn validate_output_query_capabilities(
    output: &OutputConfig,
    query: &QueryServiceConfig,
    errors: &mut Vec<String>,
) {
    let capability = output.capability();
    let output_kind = capability.kind.as_str();
    if query.enabled && !capability.supports_query {
        errors.push(format!(
            "query.enabled: output kind {output_kind} does not support query service mode"
        ));
    }
    if query.enabled && query.protocol == QueryProtocol::Graphql && !capability.supports_graphql {
        errors.push(format!(
            "query.protocol: output kind {output_kind} does not support graphql query service"
        ));
    }
    if !query.views.is_empty() && !capability.supports_graphql {
        errors.push(format!(
            "query.views: output kind {output_kind} does not support graphql query service"
        ));
    }
}

fn parse_checkpoint(
    raw: RawCheckpointConfig,
    errors: &mut Vec<String>,
) -> Option<crate::CheckpointPolicy> {
    let path = required_path("checkpoint.path", raw.path, errors)?;
    Some(crate::CheckpointPolicy::File { path })
}

pub(super) fn required_non_empty(
    field: &str,
    value: Option<String>,
    errors: &mut Vec<String>,
) -> Option<String> {
    match value {
        Some(value) => {
            let value = value.trim();
            if value.is_empty() {
                errors.push(format!("{field}: must not be empty"));
                None
            } else {
                Some(value.to_owned())
            }
        }
        None => {
            errors.push(format!("{field}: missing required field"));
            None
        }
    }
}

fn optional_non_empty(
    field: &str,
    value: Option<String>,
    errors: &mut Vec<String>,
) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            errors.push(format!("{field}: must not be empty"));
            None
        } else {
            Some(value.to_owned())
        }
    })
}

pub(super) fn required_u64(
    field: &str,
    value: Option<u64>,
    errors: &mut Vec<String>,
) -> Option<u64> {
    match value {
        Some(value) => Some(value),
        None => {
            errors.push(format!("{field}: missing required field"));
            None
        }
    }
}

fn required_path(field: &str, value: Option<PathBuf>, errors: &mut Vec<String>) -> Option<PathBuf> {
    match value {
        Some(value) if !value.as_os_str().is_empty() => Some(value),
        Some(_) => {
            errors.push(format!("{field}: must not be empty"));
            None
        }
        None => {
            errors.push(format!("{field}: missing required field"));
            None
        }
    }
}

fn normalize_application_id(field: &str, value: &str, errors: &mut Vec<String>) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || normalized.contains('/')
        || normalized.contains('\\')
        || normalized.len() > 64
        || !normalized.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        errors.push(format!(
            "{field}: application id must be 1-64 characters using lowercase letters, digits, dot, underscore, or hyphen"
        ));
        return None;
    }
    Some(normalized)
}

fn validate_graphql_identifier(
    field: &str,
    value: String,
    errors: &mut Vec<String>,
) -> Option<String> {
    let mut bytes = value.bytes();
    let valid_start = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    let valid_rest = bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if !valid_start || !valid_rest || value.starts_with("__") {
        errors.push(format!(
            "{field}: must be a GraphQL identifier matching [_A-Za-z][_0-9A-Za-z]* and must not start with __"
        ));
        return None;
    }
    Some(value)
}

fn validate_optional_positive_u64(field: &str, value: Option<u64>, errors: &mut Vec<String>) {
    if value == Some(0) {
        errors.push(format!("{field}: must be greater than 0"));
    }
}

fn validate_optional_positive_usize(field: &str, value: Option<usize>, errors: &mut Vec<String>) {
    if value == Some(0) {
        errors.push(format!("{field}: must be greater than 0"));
    }
}

fn expand_env_vars(text: &str) -> Result<String, IndexerError> {
    let mut expanded = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        expanded.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let Some(end) = tail.find('}') else {
            return Err(IndexerError::Config(
                "unterminated environment variable placeholder".to_owned(),
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

fn validate_parquet_partitions(values: &[String], errors: &mut Vec<String>) {
    for (index, value) in values.iter().enumerate() {
        match value.as_str() {
            "index" | "chain_family" | "chain_id" | "chain" | "dataset" => {}
            _ => errors.push(format!(
                "output.parquet.partition_by[{index}]: unsupported partition field {value}; supported values are index, chain_family, chain_id, chain, and dataset"
            )),
        }
    }
}

fn validate_parquet_compression(value: Option<&str>, errors: &mut Vec<String>) {
    match value {
        Some("uncompressed" | "snappy" | "zstd") | None => {}
        Some(value) => errors.push(format!(
            "output.parquet.compression: unsupported compression {value}; supported values are uncompressed, snappy, and zstd"
        )),
    }
}

fn empty_client() -> ClientConfig {
    ClientConfig {
        endpoint: String::new(),
        application: String::new(),
        token: ClientToken {
            env: String::new(),
            value: SecretString::new(String::new()),
        },
    }
}

fn empty_index() -> IndexConfig {
    IndexConfig {
        name: String::new(),
        dataset: IndexDataset::EvmLogs,
        finality: FinalityRequirement::Durable,
        chunk_blocks: 1,
    }
}
