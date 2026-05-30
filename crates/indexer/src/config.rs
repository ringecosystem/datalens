use std::{env, fmt, path::PathBuf};

use serde::Deserialize;

use crate::IndexerError;

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
}

impl IndexDataset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EvmLogs => "evm.logs",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityRequirement {
    Durable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceConfig {
    Evm(EvmSourceConfig),
}

impl SourceConfig {
    pub fn chain(&self) -> &str {
        match self {
            Self::Evm(source) => &source.chain,
        }
    }

    pub fn from_block(&self) -> u64 {
        match self {
            Self::Evm(source) => source.from_block,
        }
    }

    pub fn to_block(&self) -> Option<u64> {
        match self {
            Self::Evm(source) => source.to_block,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmSourceConfig {
    pub chain: String,
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub addresses: Vec<String>,
    pub topics: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutputConfig {
    Jsonl { path: PathBuf },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryServiceConfig {
    pub enabled: bool,
    pub graphql: bool,
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
struct RawSourceConfig {
    chain: Option<String>,
    family: Option<String>,
    chain_id: Option<u64>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    #[serde(default)]
    addresses: Vec<String>,
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOutputConfig {
    jsonl: Option<RawJsonlOutputConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawJsonlOutputConfig {
    path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQueryServiceConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    graphql: bool,
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
                errors.push("output: missing required jsonl output table".to_owned());
                OutputConfig::Jsonl {
                    path: PathBuf::new(),
                }
            }
        };
        let query = parse_query(raw.query);
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
    let dataset = match required_non_empty("index.dataset", raw.dataset, errors).as_deref() {
        Some("evm.logs") => Some(IndexDataset::EvmLogs),
        Some(value) => {
            errors.push(format!(
                "index.dataset: unsupported dataset {value}; supported value is evm.logs"
            ));
            None
        }
        None => None,
    };
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

fn parse_sources(raw_sources: Vec<RawSourceConfig>, errors: &mut Vec<String>) -> Vec<SourceConfig> {
    if raw_sources.is_empty() {
        errors.push("sources: at least one source is required".to_owned());
        return Vec::new();
    }

    raw_sources
        .into_iter()
        .enumerate()
        .filter_map(|(index, raw)| parse_source(index, raw, errors))
        .collect()
}

fn parse_source(
    index: usize,
    raw: RawSourceConfig,
    errors: &mut Vec<String>,
) -> Option<SourceConfig> {
    let prefix = format!("sources[{index}]");
    let chain = required_non_empty(&format!("{prefix}.chain"), raw.chain, errors);
    let family = required_non_empty(&format!("{prefix}.family"), raw.family, errors);
    let chain_id = required_u64(&format!("{prefix}.chain_id"), raw.chain_id, errors);
    let from_block = required_u64(&format!("{prefix}.from_block"), raw.from_block, errors);
    if let (Some(from_block), Some(to_block)) = (from_block, raw.to_block)
        && from_block > to_block
    {
        errors.push(format!(
            "{prefix}.to_block: must be greater than or equal to from_block"
        ));
    }
    validate_hex_values(
        &format!("{prefix}.addresses"),
        &raw.addresses,
        HexKind::Address,
        errors,
    );
    validate_hex_values(
        &format!("{prefix}.topics"),
        &raw.topics,
        HexKind::Topic,
        errors,
    );

    match family.as_deref() {
        Some("evm") => Some(SourceConfig::Evm(EvmSourceConfig {
            chain: chain?,
            chain_id: chain_id?,
            from_block: from_block?,
            to_block: raw.to_block,
            addresses: raw.addresses,
            topics: raw.topics,
        })),
        Some(value) => {
            errors.push(format!(
                "{prefix}.family: unsupported family {value}; supported value is evm"
            ));
            None
        }
        None => None,
    }
}

fn parse_output(raw: RawOutputConfig, errors: &mut Vec<String>) -> Option<OutputConfig> {
    let Some(jsonl) = raw.jsonl else {
        errors.push("output: missing required jsonl output table".to_owned());
        return None;
    };
    let path = required_path("output.jsonl.path", jsonl.path, errors)?;
    Some(OutputConfig::Jsonl { path })
}

fn parse_query(raw: Option<RawQueryServiceConfig>) -> QueryServiceConfig {
    raw.map(|raw| QueryServiceConfig {
        enabled: raw.enabled,
        graphql: raw.graphql,
    })
    .unwrap_or_default()
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
    if query.graphql && !capability.supports_graphql {
        errors.push(format!(
            "query.graphql: output kind {output_kind} does not support GraphQL"
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

fn required_non_empty(
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

fn required_u64(field: &str, value: Option<u64>, errors: &mut Vec<String>) -> Option<u64> {
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

enum HexKind {
    Address,
    Topic,
}

fn validate_hex_values(field: &str, values: &[String], kind: HexKind, errors: &mut Vec<String>) {
    let expected_len = match kind {
        HexKind::Address => 42,
        HexKind::Topic => 66,
    };
    for (index, value) in values.iter().enumerate() {
        if value.len() != expected_len
            || !value.starts_with("0x")
            || !value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            errors.push(format!(
                "{field}[{index}]: must be a 0x-prefixed {}-byte hex value",
                (expected_len - 2) / 2
            ));
        }
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
