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
    pub output: OutputConfig,
    pub query: QueryServiceConfig,
    pub checkpoint: crate::CheckpointPolicy,
}

impl DatalensIndexConfig {
    pub fn from_toml_str(input: &str) -> Result<Self, IndexerError> {
        let raw = toml::from_str::<RawDatalensIndexConfig>(input)
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
}

impl Default for QueryServiceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            protocol: QueryProtocol::Graphql,
            bind: "127.0.0.1:9090".to_owned(),
            path: "/graphql".to_owned(),
            playground: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QueryProtocol {
    #[default]
    Graphql,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDatalensIndexConfig {
    client: Option<RawClientConfig>,
    index: Option<RawIndexConfig>,
    #[serde(default)]
    sources: Vec<RawSourceConfig>,
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

    QueryServiceConfig {
        enabled: raw.enabled,
        protocol,
        bind,
        path,
        playground: raw.playground,
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

fn validate_optional_positive_usize(field: &str, value: Option<usize>, errors: &mut Vec<String>) {
    if value == Some(0) {
        errors.push(format!("{field}: must be greater than 0"));
    }
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
